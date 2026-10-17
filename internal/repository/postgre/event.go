package postgre

import (
	"context"
	"errors"

	"gorm.io/gorm"
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
	err := query.Where("id > ?", cursor).Order("id ASC").Limit(limit).Find(&rows).Error
	return rows, err
}

func (dao *eventDAO) AdvanceBizCursor(ctx context.Context, biz, chainFamily string, cursor int64) error {
	query := dao.db.WithContext(ctx)
	return query.Exec(`INSERT INTO chain_event_biz_cursors (biz, chain_family, cursor) VALUES (?, ?, ?) ON CONFLICT (biz, chain_family) DO UPDATE SET cursor = GREATEST(chain_event_biz_cursors.cursor, EXCLUDED.cursor)`, biz, chainFamily, cursor).Error
}
