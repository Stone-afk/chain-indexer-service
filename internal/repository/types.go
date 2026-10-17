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

type ChainRepository interface {
	GetCheckpoint(context.Context, int64) (domain.Checkpoint, bool, error)
	GetCanonicalBlock(context.Context, int64, int64) (domain.StoredBlock, bool, error)
	SaveCanonicalBatch(context.Context, domain.CanonicalBatch) error
	ApplyReorg(context.Context, domain.Reorg, domain.Checkpoint) error
}
