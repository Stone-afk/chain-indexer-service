package ioc

import (
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"gorm.io/gorm"
)

func InitChain(db *gorm.DB) repository.ChainRepository {
	return repository.NewChainRepository(postgre.NewChainDAO(db))
}
