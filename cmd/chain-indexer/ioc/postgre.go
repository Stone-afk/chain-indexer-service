package ioc

import (
	"context"
	"errors"
	"fmt"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
)

func InitPostgreSQL(ctx context.Context, cfg config.DatabaseConfig) (*postgre.Database, error) {
	database, err := postgre.Open(ctx, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("initialize PostgreSQL: %w", err)
	}
	if err := database.Ping(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("initialize PostgreSQL: %w", err), database.Close())
	}
	if err := postgre.CheckSchemaVersion(ctx, database.GORM(), 1); err != nil {
		return nil, errors.Join(fmt.Errorf("initialize PostgreSQL: %w", err), database.Close())
	}
	return database, nil
}
