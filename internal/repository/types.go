package repository

import (
	"context"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

type EventRepository interface {
	GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error)
	ListRawEvents(ctx context.Context, cursor int64, limit int) ([]domain.RawEvent, error)
	AdvanceBizCursor(ctx context.Context, biz, chainFamily string, cursor int64) error
}
