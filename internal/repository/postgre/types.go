package postgre

import "context"

type ChainDAO interface {
	GetCheckpoint(context.Context, int64) (ChainCheckpoint, bool, error)
	GetCanonicalBlock(context.Context, int64, int64) (ChainBlock, bool, error)
	SaveCanonicalBatch(context.Context, CanonicalBatch) error
	ApplyReorg(context.Context, ChainReorg, ChainCheckpoint) error
}

type EventDAO interface {
	GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error)
	ListRawEvents(ctx context.Context, cursor int64, limit int) ([]ChainRawEvent, error)
	CommitTransfersAndAdvanceCursor(context.Context, string, string, int64, []EventTransferResult) error
}
