package ioc

import (
	"context"
	"errors"
	"fmt"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/pkg/postgre"
	"gorm.io/gorm"
)

func InitDB(ctx context.Context, cfg config.DatabaseConfig) (*gorm.DB, func() error, error) {
	database, err := postgre.Open(cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize PostgreSQL: %w", err)
	}
	cleanup := func() error { return postgre.Close(database) }
	if err := postgre.Ping(ctx, database); err != nil {
		return nil, nil, errors.Join(fmt.Errorf("initialize PostgreSQL: %w", err), cleanup())
	}
	if err := postgre.CheckSchemaVersion(ctx, database, 1); err != nil {
		return nil, nil, errors.Join(fmt.Errorf("initialize PostgreSQL: %w", err), cleanup())
	}
	return database, cleanup, nil
}
