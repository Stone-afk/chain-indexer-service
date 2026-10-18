package ioc

import (
	"github.com/Stone-afk/chain-indexer-service/internal/listener"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
)

func InitListener(s provider.HeadSubscriber, h listener.HeadHandler) *listener.Listener {
	return listener.New(s, h)
}
