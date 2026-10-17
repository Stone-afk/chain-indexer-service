package ioc

import (
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"gorm.io/gorm"
)

func InitEventDAO(db *gorm.DB) postgre.EventDAO { return postgre.NewEventDAO(db) }

func InitEventRepository(dao postgre.EventDAO) repository.EventRepository {
	return repository.NewEventRepository(dao)
}
