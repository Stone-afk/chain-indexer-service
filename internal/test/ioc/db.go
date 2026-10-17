//go:build e2e

package testioc

import (
	"context"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/pkg/postgre"
	"gorm.io/gorm"
)

func InitDB(t testing.TB, ctx context.Context, cfg config.DatabaseConfig) *gorm.DB {
	t.Helper()
	database, err := postgre.Open(cfg.DSN)
	if err != nil {
		t.Fatalf("open PostgreSQL e2e database: %v", err)
	}
	t.Cleanup(func() {
		if err := postgre.Close(database); err != nil {
			t.Errorf("close PostgreSQL e2e database: %v", err)
		}
	})
	if err := postgre.Ping(ctx, database); err != nil {
		t.Fatalf("ping PostgreSQL e2e database: %v", err)
	}
	return database
}

func InitMigrationRunner(t testing.TB, db *gorm.DB, dir string) *postgre.MigrationRunner {
	t.Helper()
	runner, err := postgre.NewMigrationRunner(db, dir)
	if err != nil {
		t.Fatalf("initialize migration runner: %v", err)
	}
	return runner
}

func ResetSchema(t testing.TB, db *gorm.DB) {
	t.Helper()
	const resetSQL = `
DROP TABLE IF EXISTS migration_rollback_probe;
DROP TABLE IF EXISTS chain_event_biz_cursors;
DROP TABLE IF EXISTS chain_reorgs;
DROP TABLE IF EXISTS chain_transfers;
DROP TABLE IF EXISTS chain_transactions;
DROP TABLE IF EXISTS chain_raw_events;
DROP TABLE IF EXISTS chain_blocks;
DROP TABLE IF EXISTS chain_checkpoints;
DROP TABLE IF EXISTS schema_migrations;`
	if err := db.Exec(resetSQL).Error; err != nil {
		t.Fatalf("reset PostgreSQL e2e database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Exec(resetSQL).Error; err != nil {
			t.Errorf("clean PostgreSQL e2e database: %v", err)
		}
	})
}
