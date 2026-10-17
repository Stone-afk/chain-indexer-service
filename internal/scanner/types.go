package scanner

import (
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
)

type Scanner struct {
	reader        provider.Reader
	repo          repository.ChainRepository
	chainID       int64
	startBlock    int64
	confirmBlocks int64
}

func New(reader provider.Reader, repo repository.ChainRepository, chainID, startBlock, confirmBlocks int64) *Scanner {
	return &Scanner{reader: reader, repo: repo, chainID: chainID, startBlock: startBlock, confirmBlocks: confirmBlocks}
}
