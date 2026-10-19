package repository

import (
	"context"
	"errors"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

// ErrCanonicalIdentityMismatch 表示规范批次的准入身份不一致。
var ErrCanonicalIdentityMismatch = errors.New("canonical identity mismatch")

// ErrStateFenceRejected 表示持久化状态不满足规范写入的围栏条件。
var ErrStateFenceRejected = errors.New("canonical state fence rejected")

var ErrCanonicalHashConflict = errors.New("canonical block hash conflict")

type EventRepository interface {
	GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error)
	ListRawEvents(ctx context.Context, cursor int64, limit int) ([]domain.RawEvent, error)
	CommitTransfersAndAdvanceCursor(context.Context, string, string, int64, []domain.EventTransferResult) error
}

type ChainRepository interface {
	GetCheckpoint(context.Context, int64) (domain.Checkpoint, bool, error)
	GetCanonicalBlock(context.Context, int64, int64) (domain.StoredBlock, bool, error)
	GetBlockByHash(context.Context, int64, string) (domain.StoredBlock, bool, error)
	SaveCanonicalBatch(context.Context, domain.CanonicalBatch, domain.BlockHeader) error
	ReactivateCanonicalBatch(context.Context, domain.CanonicalBatch, domain.BlockHeader, string) error
	ApplyReorg(context.Context, domain.Reorg, domain.Checkpoint) error
}
