package postgre

import "context"

type EventDAO interface {
	GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error)
	ListRawEvents(ctx context.Context, cursor int64, limit int) ([]ChainRawEvent, error)
	AdvanceBizCursor(ctx context.Context, biz, chainFamily string, cursor int64) error
}
