package postgre

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type BlockStatus string

func (s BlockStatus) ToString() string { return string(s) }

type ChainStatus string

func (s ChainStatus) ToString() string { return string(s) }

type ReorgStatus string

func (s ReorgStatus) ToString() string { return string(s) }

type Finality string

func (s Finality) ToString() string { return string(s) }

type TransferSource string

func (s TransferSource) ToString() string { return string(s) }

const (
	// TransferSourceEventLog 表示由原始日志产生的 Token 转账来源。
	TransferSourceEventLog TransferSource = "EVENT_LOG"
	// BlockStatusCanonical 表示区块属于规范链。
	BlockStatusCanonical BlockStatus = "CANONICAL"
	// BlockStatusOrphaned 表示区块已被重组淘汰。
	BlockStatusOrphaned BlockStatus = "ORPHANED"
	// ChainStatusActive 表示记录属于有效链。
	ChainStatusActive ChainStatus = "ACTIVE"
	// ChainStatusReverted 表示记录因重组被回滚。
	ChainStatusReverted ChainStatus = "REVERTED"
	// ReorgStatusCompleted 表示重组处理已完成。
	ReorgStatusCompleted ReorgStatus = "COMPLETED"
	// FinalityConfirmed 表示区块已达到确认数要求。
	FinalityConfirmed Finality = "CONFIRMED"
	// TransferSourceTransactionValue 表示 DAO-owned 原生转账来源。
	TransferSourceTransactionValue TransferSource = "TRANSACTION_VALUE"
)

type chainDAO struct{ db *gorm.DB }

func NewChainDAO(db *gorm.DB) ChainDAO { return &chainDAO{db: db} }
func (d *chainDAO) GetCheckpoint(ctx context.Context, chainID int64) (ChainCheckpoint, bool, error) {
	var row ChainCheckpoint
	err := d.db.WithContext(ctx).Where("chain_id = ?", chainID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ChainCheckpoint{}, false, nil
	}
	return row, err == nil, err
}
func (d *chainDAO) GetCanonicalBlock(ctx context.Context, chainID, height int64) (ChainBlock, bool, error) {
	var row ChainBlock
	err := d.db.WithContext(ctx).Where("chain_id = ? AND block_number = ? AND status = ?", chainID, height, BlockStatusCanonical.ToString()).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ChainBlock{}, false, nil
	}
	return row, err == nil, err
}
func (d *chainDAO) GetBlockByHash(ctx context.Context, chainID int64, hash string) (ChainBlock, bool, error) {
	var row ChainBlock
	err := d.db.WithContext(ctx).Where("chain_id = ? AND block_hash = ?", chainID, hash).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ChainBlock{}, false, nil
	}
	return row, err == nil, err
}

func conflictColumns(names ...string) clause.OnConflict {
	columns := make([]clause.Column, len(names))
	for i, name := range names {
		columns[i] = clause.Column{Name: name}
	}
	return clause.OnConflict{Columns: columns, DoNothing: true}
}

func saveCheckpoint(tx *gorm.DB, cp ChainCheckpoint) error {
	cp.UpdatedAt = time.Now().Unix()
	if cp.CreatedAt == 0 {
		cp.CreatedAt = cp.UpdatedAt
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "chain_id"}}, DoUpdates: clause.AssignmentColumns([]string{"last_processed_height", "last_processed_hash", "updated_at"})}).Create(&cp).Error
}

func (d *chainDAO) SaveCanonicalBatch(ctx context.Context, b CanonicalBatch) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if b.Block.ChainID != b.Checkpoint.ChainID || b.Block.BlockNumber != b.Checkpoint.LastProcessedHeight || b.Block.BlockHash != b.Checkpoint.LastProcessedHash {
			return fmt.Errorf("%w: canonical batch and checkpoint identity mismatch", ErrCanonicalIdentityMismatch)
		}
		var existing ChainBlock
		findErr := tx.Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Take(&existing).Error
		if findErr == nil {
			if existing.Status != BlockStatusCanonical.ToString() {
				return fmt.Errorf("%w: persisted block is not canonical", ErrStateFenceRejected)
			}
			return validateActiveReplay(tx, b)
		}
		if !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if err := tx.Create(&b.Block).Error; err != nil {
			return err
		}
		if len(b.Transactions) > 0 {
			if err := tx.Create(&b.Transactions).Error; err != nil {
				return err
			}
		}
		if len(b.RawEvents) > 0 {
			if err := tx.Omit("id").Create(&b.RawEvents).Error; err != nil {
				return err
			}
		}
		if len(b.NativeTransfers) > 0 {
			if err := tx.Create(&b.NativeTransfers).Error; err != nil {
				return err
			}
		}
		// Restore already-consumed token results as well as Native rows, without
		// inserting token transfers or touching their IDs and business contents.
		return advanceCanonicalCheckpoint(tx, b.Checkpoint)
	})
}

func advanceReactivationCheckpoint(tx *gorm.DB, target ChainCheckpoint) error {
	var current ChainCheckpoint
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("chain_id = ?", target.ChainID).Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		now := time.Now().Unix()
		target.CreatedAt, target.UpdatedAt = now, now
		res := tx.Create(&target)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("%w: reactivation checkpoint insert changed %d rows", ErrStateFenceRejected, res.RowsAffected)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if current.LastProcessedHeight == target.LastProcessedHeight && current.LastProcessedHash != target.LastProcessedHash {
		return fmt.Errorf("%w: canonical checkpoint hash conflict at height %d", ErrStateFenceRejected, target.LastProcessedHeight)
	}
	if current.LastProcessedHeight >= target.LastProcessedHeight {
		return nil
	}
	res := tx.Model(&ChainCheckpoint{}).Where("chain_id = ?", target.ChainID).Updates(map[string]any{"last_processed_height": target.LastProcessedHeight, "last_processed_hash": target.LastProcessedHash, "updated_at": time.Now().Unix()})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return fmt.Errorf("%w: reactivation checkpoint update changed %d rows", ErrStateFenceRejected, res.RowsAffected)
	}
	return nil
}

// ReactivateCanonicalBatch explicitly restores one fenced orphaned subtree.
func (d *chainDAO) ReactivateCanonicalBatch(ctx context.Context, b CanonicalBatch, expected string) error {
	if expected == "" {
		return fmt.Errorf("%w: expected reorg id is required", ErrCanonicalIdentityMismatch)
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var block ChainBlock
		if err := tx.Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Take(&block).Error; err != nil {
			return err
		}
		if block.Status != BlockStatusOrphaned.ToString() || block.ReorgID == nil || *block.ReorgID != expected || block.OrphanedAt == nil {
			return fmt.Errorf("%w: block reactivation fence mismatch", ErrStateFenceRejected)
		}
		var txs []ChainTransaction
		if err := tx.Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Find(&txs).Error; err != nil {
			return err
		}
		if !sameTransactionKeys(txs, b.Transactions) {
			return fmt.Errorf("%w: reactivation transaction set mismatch", ErrStateFenceRejected)
		}
		for _, row := range txs {
			if row.ChainStatus != ChainStatusReverted.ToString() || row.ReorgID == nil || *row.ReorgID != expected || row.RevertedAt == nil || *row.RevertedAt != *block.OrphanedAt {
				return fmt.Errorf("%w: reactivation transaction fence mismatch", ErrStateFenceRejected)
			}
		}
		var events []ChainRawEvent
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Order("id ASC").Find(&events).Error; err != nil {
			return err
		}
		if !sameEventKeys(events, b.RawEvents) {
			return fmt.Errorf("%w: reactivation raw event set mismatch", ErrStateFenceRejected)
		}
		for _, row := range events {
			if row.ChainStatus != ChainStatusReverted.ToString() || row.ReorgID == nil || *row.ReorgID != expected || row.RevertedAt == nil || *row.RevertedAt != *block.OrphanedAt {
				return fmt.Errorf("%w: reactivation raw event fence mismatch", ErrStateFenceRejected)
			}
		}
		var native []ChainTransfer
		if err := tx.Where("chain_id = ? AND block_hash = ? AND source_type = ?", b.Block.ChainID, b.Block.BlockHash, TransferSourceTransactionValue.ToString()).Find(&native).Error; err != nil {
			return err
		}
		if !sameTransferKeys(native, b.NativeTransfers) {
			return fmt.Errorf("%w: reactivation native set mismatch", ErrStateFenceRejected)
		}
		for _, row := range native {
			if row.ChainStatus != ChainStatusReverted.ToString() || row.ReorgID == nil || *row.ReorgID != expected || row.RevertedAt == nil || *row.RevertedAt != *block.OrphanedAt {
				return fmt.Errorf("%w: reactivation native fence mismatch", ErrStateFenceRejected)
			}
		}
		// Restore only pre-existing EVENT_LOG rows linked to the reverted raw events.
		// The integer is rendered as text by PostgreSQL; never cast dirty source_index.
		candidate := tx.Model(&ChainTransfer{}).Where("chain_id = ? AND block_hash = ? AND source_type = ?", b.Block.ChainID, b.Block.BlockHash, TransferSourceEventLog.ToString())
		var candidates int64
		if err := candidate.Count(&candidates).Error; err != nil {
			return err
		}
		matched := tx.Model(&ChainTransfer{}).Where(`chain_id = ? AND block_hash = ? AND source_type = ? AND chain_status = ? AND reorg_id = ? AND reverted_at = ? AND EXISTS (SELECT 1 FROM chain_raw_events e WHERE e.chain_id = chain_transfers.chain_id AND e.block_hash = chain_transfers.block_hash AND e.tx_hash = chain_transfers.tx_hash AND e.log_index::text = chain_transfers.source_index AND e.chain_status = ? AND e.reorg_id = ? AND e.reverted_at = ?)`, b.Block.ChainID, b.Block.BlockHash, TransferSourceEventLog.ToString(), ChainStatusReverted.ToString(), expected, *block.OrphanedAt, ChainStatusReverted.ToString(), expected, *block.OrphanedAt)
		var matchedCount int64
		if err := matched.Count(&matchedCount).Error; err != nil {
			return err
		}
		if matchedCount != candidates {
			return fmt.Errorf("%w: reactivation token relation mismatch", ErrStateFenceRejected)
		}
		if matchedCount > 0 {
			res := tx.Model(&ChainTransfer{}).Where(`chain_id = ? AND block_hash = ? AND source_type = ? AND chain_status = ? AND reorg_id = ? AND reverted_at = ? AND EXISTS (SELECT 1 FROM chain_raw_events e WHERE e.chain_id = chain_transfers.chain_id AND e.block_hash = chain_transfers.block_hash AND e.tx_hash = chain_transfers.tx_hash AND e.log_index::text = chain_transfers.source_index AND e.chain_status = ? AND e.reorg_id = ? AND e.reverted_at = ?)`, b.Block.ChainID, b.Block.BlockHash, TransferSourceEventLog.ToString(), ChainStatusReverted.ToString(), expected, *block.OrphanedAt, ChainStatusReverted.ToString(), expected, *block.OrphanedAt).Updates(map[string]any{"chain_status": ChainStatusActive.ToString(), "reorg_id": nil, "reverted_at": nil})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected != matchedCount {
				return fmt.Errorf("%w: reactivation token fence changed", ErrStateFenceRejected)
			}
		}
		res := tx.Model(&ChainBlock{}).Where("chain_id = ? AND block_hash = ? AND status = ? AND reorg_id = ? AND orphaned_at = ?", b.Block.ChainID, b.Block.BlockHash, BlockStatusOrphaned.ToString(), expected, *block.OrphanedAt).Updates(map[string]any{"status": BlockStatusCanonical.ToString(), "reorg_id": nil, "orphaned_at": nil})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("%w: reactivation block fence changed", ErrStateFenceRejected)
		}
		for _, model := range []struct {
			model     any
			predicate string
			count     int64
		}{{&ChainTransaction{}, "", int64(len(txs))}, {&ChainRawEvent{}, "", int64(len(events))}, {&ChainTransfer{}, "source_type = 'TRANSACTION_VALUE' AND ", int64(len(native))}} {
			q := "chain_id = ? AND block_hash = ? AND " + model.predicate + "chain_status = ? AND reorg_id = ? AND reverted_at = ?"
			res := tx.Model(model.model).Where(q, b.Block.ChainID, b.Block.BlockHash, ChainStatusReverted.ToString(), expected, *block.OrphanedAt).Updates(map[string]any{"chain_status": ChainStatusActive.ToString(), "reorg_id": nil, "reverted_at": nil})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected != model.count {
				return fmt.Errorf("%w: reactivation descendant fence changed", ErrStateFenceRejected)
			}
		}
		return advanceReactivationCheckpoint(tx, b.Checkpoint)
	})
}

type transactionKey struct {
	chainID           int64
	blockHash, txHash string
}

type eventKey struct {
	chainID   int64
	blockHash string
	logIndex  int
}

type transferKey struct {
	chainID                                    int64
	blockHash, txHash, sourceType, sourceIndex string
	batchIndex                                 int
}

func uniqueTransactionKeys(rows []ChainTransaction) map[transactionKey]struct{} {
	keys := make(map[transactionKey]struct{}, len(rows))
	for _, row := range rows {
		keys[transactionKey{row.ChainID, row.BlockHash, row.TxHash}] = struct{}{}
	}
	return keys
}
func uniqueEventKeys(rows []ChainRawEvent) map[eventKey]struct{} {
	keys := make(map[eventKey]struct{}, len(rows))
	for _, row := range rows {
		keys[eventKey{row.ChainID, row.BlockHash, row.LogIndex}] = struct{}{}
	}
	return keys
}
func uniqueTransferKeys(rows []ChainTransfer) map[transferKey]struct{} {
	keys := make(map[transferKey]struct{}, len(rows))
	for _, row := range rows {
		keys[transferKey{row.ChainID, row.BlockHash, row.TxHash, row.SourceType, row.SourceIndex, row.BatchIndex}] = struct{}{}
	}
	return keys
}
func sameTransactionKeys(a, b []ChainTransaction) bool {
	x, y := uniqueTransactionKeys(a), uniqueTransactionKeys(b)
	if len(x) != len(y) || len(a) != len(b) {
		return false
	}
	for k := range x {
		if _, ok := y[k]; !ok {
			return false
		}
	}
	return true
}
func sameEventKeys(a, b []ChainRawEvent) bool {
	x, y := uniqueEventKeys(a), uniqueEventKeys(b)
	if len(x) != len(y) || len(a) != len(b) {
		return false
	}
	for k := range x {
		if _, ok := y[k]; !ok {
			return false
		}
	}
	return true
}
func sameTransferKeys(a, b []ChainTransfer) bool {
	x, y := uniqueTransferKeys(a), uniqueTransferKeys(b)
	if len(x) != len(y) || len(a) != len(b) {
		return false
	}
	for k := range x {
		if _, ok := y[k]; !ok {
			return false
		}
	}
	return true
}

func validateActiveReplay(tx *gorm.DB, b CanonicalBatch) error {
	var current ChainCheckpoint
	if err := tx.Where("chain_id = ?", b.Checkpoint.ChainID).Take(&current).Error; err == nil {
		if current.LastProcessedHeight == b.Checkpoint.LastProcessedHeight && current.LastProcessedHash != b.Checkpoint.LastProcessedHash {
			return fmt.Errorf("%w: canonical checkpoint hash conflict at height %d", ErrStateFenceRejected, b.Checkpoint.LastProcessedHeight)
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	var txs []ChainTransaction
	if err := tx.Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Find(&txs).Error; err != nil {
		return err
	}
	var events []ChainRawEvent
	if err := tx.Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Find(&events).Error; err != nil {
		return err
	}
	var transfers []ChainTransfer
	if err := tx.Where("chain_id = ? AND block_hash = ? AND source_type = ?", b.Block.ChainID, b.Block.BlockHash, TransferSourceTransactionValue.ToString()).Find(&transfers).Error; err != nil {
		return err
	}
	if len(txs) != len(b.Transactions) || len(events) != len(b.RawEvents) || len(transfers) != len(b.NativeTransfers) {
		return fmt.Errorf("%w: canonical replay scanner-owned set mismatch", ErrStateFenceRejected)
	}
	if len(uniqueTransactionKeys(b.Transactions)) != len(b.Transactions) || len(uniqueEventKeys(b.RawEvents)) != len(b.RawEvents) || len(uniqueTransferKeys(b.NativeTransfers)) != len(b.NativeTransfers) {
		return fmt.Errorf("%w: canonical replay contains duplicate scanner-owned natural key", ErrStateFenceRejected)
	}
	txKeys := uniqueTransactionKeys(txs)
	for _, row := range b.Transactions {
		if _, ok := txKeys[transactionKey{row.ChainID, row.BlockHash, row.TxHash}]; !ok {
			return fmt.Errorf("%w: canonical replay transaction key mismatch", ErrStateFenceRejected)
		}
	}
	eventKeys := uniqueEventKeys(events)
	for _, row := range b.RawEvents {
		if _, ok := eventKeys[eventKey{row.ChainID, row.BlockHash, row.LogIndex}]; !ok {
			return fmt.Errorf("%w: canonical replay raw event key mismatch", ErrStateFenceRejected)
		}
	}
	transferKeys := uniqueTransferKeys(transfers)
	for _, row := range b.NativeTransfers {
		if _, ok := transferKeys[transferKey{row.ChainID, row.BlockHash, row.TxHash, row.SourceType, row.SourceIndex, row.BatchIndex}]; !ok {
			return fmt.Errorf("%w: canonical replay transfer key mismatch", ErrStateFenceRejected)
		}
	}
	for _, row := range txs {
		if row.ChainStatus != ChainStatusActive.ToString() {
			return fmt.Errorf("%w: canonical replay contains inactive transaction", ErrStateFenceRejected)
		}
	}
	for _, row := range events {
		if row.ChainStatus != ChainStatusActive.ToString() {
			return fmt.Errorf("%w: canonical replay contains inactive raw event", ErrStateFenceRejected)
		}
	}
	for _, row := range transfers {
		if row.ChainStatus != ChainStatusActive.ToString() {
			return fmt.Errorf("%w: canonical replay contains inactive transfer", ErrStateFenceRejected)
		}
	}
	// An exact active replay is validation-only. It must not touch scanner-owned
	// rows, token rows, or the checkpoint.
	return nil
}

func advanceCanonicalCheckpoint(tx *gorm.DB, target ChainCheckpoint) error {
	var current ChainCheckpoint
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("chain_id = ?", target.ChainID).Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		now := time.Now().Unix()
		target.CreatedAt, target.UpdatedAt = now, now
		return tx.Create(&target).Error
	}
	if err != nil {
		return err
	}
	if current.LastProcessedHeight == target.LastProcessedHeight && current.LastProcessedHash != target.LastProcessedHash {
		return fmt.Errorf("%w: canonical checkpoint hash conflict at height %d", ErrStateFenceRejected, target.LastProcessedHeight)
	}
	if current.LastProcessedHeight >= target.LastProcessedHeight {
		return nil
	}
	return tx.Model(&ChainCheckpoint{}).Where("chain_id = ?", target.ChainID).Updates(map[string]any{"last_processed_height": target.LastProcessedHeight, "last_processed_hash": target.LastProcessedHash, "updated_at": time.Now().Unix()}).Error
}

func (d *chainDAO) ApplyReorg(ctx context.Context, reorg ChainReorg, checkpoint ChainCheckpoint) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The unique key admits exactly one executor, including concurrent replay.
		admission := tx.Clauses(conflictColumns("reorg_id")).Create(&reorg)
		if admission.Error != nil {
			return admission.Error
		}
		if admission.RowsAffected == 0 {
			var existing ChainReorg
			if err := tx.Select("status").Where("reorg_id = ?", reorg.ReorgID).Take(&existing).Error; err != nil {
				return err
			}
			if existing.Status != ReorgStatusCompleted.ToString() {
				return fmt.Errorf("reorg record is not completed")
			}
			return nil
		}

		// Match the worker's global raw-event ID lock order before invalidation.
		const forkRange = "chain_id = ? AND block_number > ? AND block_number <= ?"
		var events []ChainRawEvent
		// FOR UPDATE 不是用于读取业务数据，events 的内容确实没有被使用；它的唯一作用是协调 reorg 与 worker 的并发事务。
		if err := tx.Select("id").Where(forkRange, reorg.ChainID, reorg.AncestorHeight, reorg.OldHeadHeight).Order("id ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&events).Error; err != nil {
			return err
		}
		now := time.Now().Unix()
		if err := tx.Model(&ChainBlock{}).Where(forkRange, reorg.ChainID, reorg.AncestorHeight, reorg.OldHeadHeight).Where("status = ?", BlockStatusCanonical.ToString()).Updates(map[string]any{"status": BlockStatusOrphaned.ToString(), "reorg_id": reorg.ReorgID, "orphaned_at": now}).Error; err != nil {
			return err
		}
		for _, model := range []any{&ChainTransaction{}, &ChainRawEvent{}, &ChainTransfer{}} {
			if err := tx.Model(model).Where(forkRange, reorg.ChainID, reorg.AncestorHeight, reorg.OldHeadHeight).Where("chain_status = ?", ChainStatusActive.ToString()).Updates(map[string]any{"chain_status": ChainStatusReverted.ToString(), "reorg_id": reorg.ReorgID, "reverted_at": now}).Error; err != nil {
				return err
			}
		}
		return saveCheckpoint(tx, checkpoint)
	})
}
