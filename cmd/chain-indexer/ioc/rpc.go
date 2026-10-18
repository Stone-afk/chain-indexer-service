package ioc

import (
	"context"
	"errors"
	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
)

func InitRPC(ctx context.Context, cfg config.ChainConfig) (provider.Reader, provider.HeadSubscriber, func() error, error) {
	r, rc, err := provider.NewHTTPReader(ctx, cfg.HTTPURL)
	if err != nil {
		return nil, nil, nil, err
	}
	s, sc, err := provider.NewWSSubscriber(ctx, cfg.WSURL)
	if err != nil {
		_ = rc()
		return nil, nil, nil, err
	}
	return r, s, func() error { return errors.Join(sc(), rc()) }, nil
}
