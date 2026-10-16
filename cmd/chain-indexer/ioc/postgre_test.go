package ioc

import (
	"context"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
)

func assertInitPostgreSQLSignature() {
	var initPostgreSQL func(context.Context, config.DatabaseConfig) (*postgre.Database, error)
	initPostgreSQL = InitPostgreSQL
	_ = initPostgreSQL
}
