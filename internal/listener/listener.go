package listener

import (
	"context"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
)

// Listener processes new-head triggers in receive order.
type Listener struct {
	subscriber provider.HeadSubscriber
	handler    HeadHandler
}

func New(subscriber provider.HeadSubscriber, handler HeadHandler) *Listener {
	return &Listener{subscriber: subscriber, handler: handler}
}

// Run owns one subscription until cancellation or the first failure.
func (l *Listener) Run(ctx context.Context) error {
	heads := make(chan domain.BlockHeader)
	subscription, err := l.subscriber.SubscribeNewHeads(ctx, heads)
	if err != nil {
		return err
	}
	defer subscription.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-subscription.Err():
			return err
		case <-heads:
			if err := l.handler.HandleNewHead(ctx); err != nil {
				return err
			}
		}
	}
}
