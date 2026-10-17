package ioc

import (
	"context"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"gorm.io/gorm"
)

func assertInitDBSignature() {
	var initDB func(context.Context, config.DatabaseConfig) (*gorm.DB, func() error, error)
	initDB = InitDB
	_ = initDB
}
