//go:build e2e

package testioc

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/pkg/postgre"
	"gorm.io/gorm"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// NotifyBarrier observes committed changes to one of the fixed schema tables.
type NotifyBarrier struct {
	conn                              *pgx.Conn
	channel, trigger, function, table string
}

func NewNotifyBarrier(ctx context.Context, dsn, table string) (*NotifyBarrier, error) {
	allowed := map[string]bool{"chain_checkpoints": true, "chain_blocks": true, "chain_transactions": true, "chain_raw_events": true, "chain_transfers": true, "chain_event_biz_cursors": true}
	if !allowed[table] {
		return nil, fmt.Errorf("unsupported notification table %q", table)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	suffix := make([]byte, 8)
	if _, err = rand.Read(suffix); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	id := fmt.Sprintf("%x", suffix)
	b := &NotifyBarrier{conn: conn, channel: "t026_" + id, trigger: "t026_trg_" + id, function: "t026_fn_" + id, table: table}
	if _, err = conn.Exec(ctx, "LISTEN "+b.channel); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	q := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_notify('%s', TG_TABLE_NAME); RETURN NEW; END$$; CREATE TRIGGER %s AFTER INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION %s()`, b.function, b.channel, b.trigger, table, b.function)
	if _, err = conn.Exec(ctx, q); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return b, nil
}

func (b *NotifyBarrier) WaitForNotification(ctx context.Context) error {
	_, err := b.conn.WaitForNotification(ctx)
	return err
}
func (b *NotifyBarrier) Close(ctx context.Context) error {
	_, _ = b.conn.Exec(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s; DROP FUNCTION IF EXISTS %s()", b.trigger, b.table, b.function))
	return b.conn.Close(ctx)
}

// WithRowLock executes fn while a PostgreSQL row lock is held by the same
// transaction. It is intentionally test-only and bounded by ctx/release.
func WithRowLock(ctx context.Context, db *gorm.DB, query string, args []any, entered chan<- struct{}, backend chan<- int32, release <-chan struct{}, fn func(*gorm.DB) error) error {
	return WithFixedSession(ctx, db, func(session *gorm.DB) error {
		tx := session.WithContext(ctx).Begin()
		if tx.Error != nil {
			return tx.Error
		}
		defer tx.Rollback()
		var rows []struct{ ID int64 }
		if err := tx.Raw(query, args...).Scan(&rows).Error; err != nil {
			return err
		}
		var pid int32
		if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
			return err
		}
		select {
		case backend <- pid:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case entered <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit().Error
	})
}

// WithFixedSession pins all work in fn to one PostgreSQL backend.
func WithFixedSession(ctx context.Context, db *gorm.DB, fn func(*gorm.DB) error) error {
	return db.WithContext(ctx).Connection(fn)
}

// ObserveRowLockWait makes at most 100 catalog observations over two seconds,
// or until ctx ends sooner. Callers must take this snapshot before releasing
// the test-owned transaction.
func ObserveRowLockWait(ctx context.Context, db *gorm.DB, blockerPID, opponentPID int32) error {
	const query = `SELECT COALESCE(wait_event_type, ''), ? = ANY(pg_blocking_pids(pid)) FROM pg_stat_activity WHERE pid = ?`
	observeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var lastObservation string
	for attempt := 0; attempt < 100; attempt++ {
		var event string
		var blockedByExpected bool
		err := WithFixedSession(observeCtx, db, func(observer *gorm.DB) error {
			return observer.Raw(query, blockerPID, opponentPID).Row().Scan(&event, &blockedByExpected)
		})
		if err != nil {
			return fmt.Errorf("observe PostgreSQL row-lock wait: %w", err)
		}
		if event == "Lock" && blockedByExpected {
			return nil
		}
		lastObservation = fmt.Sprintf("opponent=%d wait_event_type=%q blocked_by_expected=%t blocker=%d", opponentPID, event, blockedByExpected, blockerPID)
		select {
		case <-ticker.C:
		case <-observeCtx.Done():
			return fmt.Errorf("row-lock wait observation ended (%s): %w", lastObservation, observeCtx.Err())
		}
	}
	return fmt.Errorf("row-lock wait observation ended after 100 attempts (%s)", lastObservation)
}

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
