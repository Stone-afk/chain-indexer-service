package postgre

import (
	"context"
	"errors"
	"fmt"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"sort"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type eventDAO struct{ db *gorm.DB }

func NewEventDAO(db *gorm.DB) EventDAO { return &eventDAO{db: db} }

func (dao *eventDAO) GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error) {
	query := dao.db.WithContext(ctx)
	var row chainEventBizCursor
	err := query.Where("biz = ? AND chain_family = ?", biz, chainFamily).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return row.Cursor, nil
}

func (dao *eventDAO) ListRawEvents(ctx context.Context, cursor int64, limit int) ([]ChainRawEvent, error) {
	query := dao.db.WithContext(ctx)
	rows := make([]ChainRawEvent, 0)
	err := query.
		Model(&ChainRawEvent{}).
		Select("chain_raw_events.*, chain_blocks.block_time AS block_time").
		Joins("INNER JOIN chain_blocks ON chain_blocks.chain_id = chain_raw_events.chain_id AND chain_blocks.block_hash = chain_raw_events.block_hash").
		Where("chain_raw_events.id > ?", cursor).
		Order("chain_raw_events.id ASC").
		Limit(limit).
		Find(&rows).Error
	return rows, err
}

func (dao *eventDAO) CommitTransfersAndAdvanceCursor(ctx context.Context, biz, chainFamily string, cursor int64, results []EventTransferResult) error {
	ids := make([]int64, 0, len(results))
	seen := make(map[int64]bool, len(results))
	for _, result := range results {
		if !seen[result.RawEventID] {
			ids = append(ids, result.RawEventID)
			seen[result.RawEventID] = true
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return dao.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		states := make(map[int64]ChainRawEvent, len(ids))
		if len(ids) > 0 {
			var events []ChainRawEvent
			// This lock order is shared with canonical recovery and reorg. Read
			// state in the locking SELECT so it is authoritative after any wait.
			if err := tx.Select("id", "chain_status", "reorg_id", "reverted_at").Where("id IN ?", ids).Order("id ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&events).Error; err != nil {
				return err
			}
			for _, event := range events {
				if event.ChainStatus != string(domain.ChainStatusActive) && event.ChainStatus != string(domain.ChainStatusReverted) {
					return fmt.Errorf("raw event %d has invalid chain status", event.ID)
				}
				states[event.ID] = event
			}
			for _, id := range ids {
				if _, ok := states[id]; !ok {
					return fmt.Errorf("raw event %d not found", id)
				}
			}
		}
		conflict := clause.OnConflict{
			Columns:   []clause.Column{{Name: "chain_id"}, {Name: "block_hash"}, {Name: "tx_hash"}, {Name: "source_type"}, {Name: "source_index"}, {Name: "batch_index"}},
			DoUpdates: clause.AssignmentColumns([]string{"chain_status", "reorg_id", "reverted_at"}),
		}
		for _, result := range results {
			state := states[result.RawEventID]
			for _, decoded := range result.Transfers {
				row := decoded
				row.ChainStatus, row.ReorgID, row.RevertedAt = state.ChainStatus, state.ReorgID, state.RevertedAt
				if state.ChainStatus == string(domain.ChainStatusActive) {
					row.ReorgID, row.RevertedAt = nil, nil
				}
				if err := tx.Clauses(conflict).Create(&row).Error; err != nil {
					return err
				}
			}
		}
		return tx.Exec(`INSERT INTO chain_event_biz_cursors (biz, chain_family, cursor) VALUES (?, ?, ?) ON CONFLICT (biz, chain_family) DO UPDATE SET cursor = GREATEST(chain_event_biz_cursors.cursor, EXCLUDED.cursor)`, biz, chainFamily, cursor).Error
	})
}
