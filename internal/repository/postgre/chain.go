package postgre

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
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
	err := d.db.WithContext(ctx).Where("chain_id = ? AND block_number = ? AND status = ?", chainID, height, "CANONICAL").Take(&row).Error
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
			return fmt.Errorf("canonical batch and checkpoint identity mismatch")
		}
		var events []ChainRawEvent
		if err := tx.Select("id").Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Order("id ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&events).Error; err != nil {
			return err
		}
		if err := tx.Clauses(canonicalConflict("chain_blocks", "chain_id", "block_hash")).Create(&b.Block).Error; err != nil {
			return err
		}
		if len(b.Transactions) > 0 {
			if err := tx.Clauses(canonicalConflict("chain_transactions", "chain_id", "block_hash", "tx_hash")).Create(&b.Transactions).Error; err != nil {
				return err
			}
		}
		if len(b.RawEvents) > 0 {
			if err := tx.Omit("id").Clauses(canonicalConflict("chain_raw_events", "chain_id", "block_hash", "log_index")).Create(&b.RawEvents).Error; err != nil {
				return err
			}
		}
		if len(b.NativeTransfers) > 0 {
			if err := tx.Clauses(canonicalConflict("chain_transfers", "chain_id", "block_hash", "tx_hash", "source_type", "source_index", "batch_index")).Create(&b.NativeTransfers).Error; err != nil {
				return err
			}
		}
		// Restore already-consumed token results as well as Native rows, without
		// inserting token transfers or touching their IDs and business contents.
		if err := tx.Model(&ChainTransfer{}).Where("chain_id = ? AND block_hash = ? AND chain_status = ?", b.Block.ChainID, b.Block.BlockHash, "REVERTED").Updates(map[string]any{"chain_status": "ACTIVE", "reorg_id": gorm.Expr("NULL"), "reverted_at": gorm.Expr("NULL")}).Error; err != nil {
			return err
		}
		var stored ChainBlock
		if err := tx.Select("status").Where("chain_id = ? AND block_hash = ?", b.Block.ChainID, b.Block.BlockHash).Take(&stored).Error; err != nil {
			return err
		}
		if stored.Status != "CANONICAL" {
			return fmt.Errorf("persisted block is not canonical")
		}
		return advanceCanonicalCheckpoint(tx, b.Checkpoint)
	})
}

func canonicalConflict(table string, keys ...string) clause.OnConflict {
	conflict := conflictColumns(keys...)
	conflict.DoNothing = false
	status, invalid := "chain_status", "REVERTED"
	updates := map[string]any{"chain_status": "ACTIVE", "reorg_id": gorm.Expr("NULL"), "reverted_at": gorm.Expr("NULL")}
	if table == "chain_blocks" {
		status, invalid = "status", "ORPHANED"
		updates = map[string]any{"status": "CANONICAL", "finality": "CONFIRMED", "reorg_id": gorm.Expr("NULL"), "orphaned_at": gorm.Expr("NULL")}
	}
	conflict.DoUpdates = clause.Assignments(updates)
	// All identifiers and states are fixed internal schema names, never input.
	conflict.Where = clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: fmt.Sprintf(`"%s"."%s" = '%s'`, table, status, invalid)}}}
	return conflict
}

func advanceCanonicalCheckpoint(tx *gorm.DB, target ChainCheckpoint) error {
	var current ChainCheckpoint
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("chain_id = ?", target.ChainID).Take(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		target.CreatedAt, target.UpdatedAt = time.Now().Unix(), time.Now().Unix()
		return tx.Create(&target).Error
	}
	if err != nil {
		return err
	}
	if current.LastProcessedHeight == target.LastProcessedHeight && current.LastProcessedHash != target.LastProcessedHash {
		return fmt.Errorf("canonical checkpoint hash conflict at height %d", target.LastProcessedHeight)
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
			if existing.Status != "COMPLETED" {
				return fmt.Errorf("reorg record is not completed")
			}
			return nil
		}

		// Match the worker's global raw-event ID lock order before invalidation.
		const forkRange = "chain_id = ? AND block_number > ? AND block_number <= ?"
		var events []ChainRawEvent
		if err := tx.Select("id").Where(forkRange, reorg.ChainID, reorg.AncestorHeight, reorg.OldHeadHeight).Order("id ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&events).Error; err != nil {
			return err
		}
		now := time.Now().Unix()
		if err := tx.Model(&ChainBlock{}).Where(forkRange, reorg.ChainID, reorg.AncestorHeight, reorg.OldHeadHeight).Where("status = ?", "CANONICAL").Updates(map[string]any{"status": "ORPHANED", "reorg_id": reorg.ReorgID, "orphaned_at": now}).Error; err != nil {
			return err
		}
		for _, model := range []any{&ChainTransaction{}, &ChainRawEvent{}, &ChainTransfer{}} {
			if err := tx.Model(model).Where(forkRange, reorg.ChainID, reorg.AncestorHeight, reorg.OldHeadHeight).Where("chain_status = ?", "ACTIVE").Updates(map[string]any{"chain_status": "REVERTED", "reorg_id": reorg.ReorgID, "reverted_at": now}).Error; err != nil {
				return err
			}
		}
		return saveCheckpoint(tx, checkpoint)
	})
}
