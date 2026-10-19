package ioc

import (
	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
	"github.com/Stone-afk/chain-indexer-service/internal/scanner"
)

func InitScanner(r provider.Reader, repo repository.ChainRepository, cfg config.ChainConfig) *scanner.Scanner {
	return scanner.New(r, repo, cfg.ChainID, cfg.StartBlock, cfg.ConfirmBlocks, cfg.MaxReorgDepth)
}
