package postgre

import (
	"context"
	"errors"
)

// ErrCanonicalIdentityMismatch 表示规范批次的准入身份不一致。
var ErrCanonicalIdentityMismatch = errors.New("canonical identity mismatch")

// ErrStateFenceRejected 表示持久化状态不满足规范写入的围栏条件。
var ErrStateFenceRejected = errors.New("canonical state fence rejected")

type ChainDAO interface {
	GetCheckpoint(context.Context, int64) (ChainCheckpoint, bool, error)
	GetCanonicalBlock(context.Context, int64, int64) (ChainBlock, bool, error)
	GetBlockByHash(context.Context, int64, string) (ChainBlock, bool, error)
	SaveCanonicalBatch(context.Context, CanonicalBatch) error
	ReactivateCanonicalBatch(context.Context, CanonicalBatch, string) error
	ApplyReorg(context.Context, ChainReorg, ChainCheckpoint) error
}

type EventDAO interface {
	GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error)
	ListRawEvents(ctx context.Context, cursor int64, limit int) ([]ChainRawEvent, error)
	CommitTransfersAndAdvanceCursor(context.Context, string, string, int64, []EventTransferResult) error
}
