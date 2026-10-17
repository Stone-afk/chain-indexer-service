package provider

import (
	"context"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

type Reader interface {
	LatestBlockNumber(context.Context) (int64, error)
	HeaderByNumber(context.Context, int64) (domain.BlockHeader, error)
	BlockByNumber(context.Context, int64) (domain.Block, error)
	ReceiptByTransactionHash(context.Context, string) (domain.Receipt, error)
}

type HeadSubscriber interface {
	SubscribeNewHeads(context.Context, chan<- domain.BlockHeader) (Subscription, error)
}

type Subscription interface {
	Err() <-chan error
	Unsubscribe()
}
