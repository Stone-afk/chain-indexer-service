package ioc

import (
	"context"
	"errors"
	"fmt"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/pkg/postgre"
	"gorm.io/gorm"
)

type publicError struct {
	cause error
}

func (e publicError) Error() string       { return e.cause.Error() }
func (e publicError) Unwrap() error       { return e.cause }
func (e publicError) PublicError() string { return e.cause.Error() }

func public(err error) error {
	if err == nil {
		return nil
	}
	return publicError{cause: err}
}

func InitDB(ctx context.Context, cfg config.DatabaseConfig) (*gorm.DB, func() error, error) {
	database, err := postgre.Open(cfg.DSN)
	if err != nil {
		return nil, nil, public(fmt.Errorf("initialize PostgreSQL: %w", err))
	}
	cleanup := func() error { return postgre.Close(database) }
	if err := postgre.Ping(ctx, database); err != nil {
		return nil, nil, public(errors.Join(fmt.Errorf("initialize PostgreSQL: %w", err), cleanup()))
	}
	if err := postgre.CheckSchemaVersion(ctx, database, 3); err != nil {
		return nil, nil, public(errors.Join(fmt.Errorf("initialize PostgreSQL: %w", err), cleanup()))
	}
	return database, cleanup, nil
}
