//go:build e2e

package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"gorm.io/gorm"
)

func TestMigrationUpStatusDownUpRoundTrip(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	testioc.ResetSchema(t, db)
	runner := testioc.InitMigrationRunner(t, db, migrationDir(t))
	ctx := context.Background()

	status, err := runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status() before bootstrap error = %v", err)
	}
	assertMigrationStatus(t, status, false, false)

	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("Up() error = %v", err)
	}
	assertInitialSchema(t, db)
	assertAppliedVersionCount(t, db, 1, 1)
	assertAppliedVersionCount(t, db, 2, 1)

	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("second Up() error = %v", err)
	}
	assertAppliedVersionCount(t, db, 1, 1)
	assertAppliedVersionCount(t, db, 2, 1)
	status, err = runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status() after Up error = %v", err)
	}
	assertMigrationStatus(t, status, true, true)

	if _, err := runner.Down(ctx); err != nil {
		t.Fatalf("Down() error = %v", err)
	}
	assertInitialSchema(t, db)
	assertTableExists(t, db, "chain_event_biz_cursors", false)
	assertTableExists(t, db, "schema_migrations", true)
	assertAppliedVersionCount(t, db, 1, 1)
	assertAppliedVersionCount(t, db, 2, 0)
	status, err = runner.Status(ctx)
	if err != nil {
		t.Fatalf("Status() after Down error = %v", err)
	}
	assertMigrationStatus(t, status, true, false)

	if _, err := runner.Down(ctx); err != nil {
		t.Fatalf("second Down() error = %v", err)
	}
	assertBusinessTablesAbsent(t, db)
	assertAppliedVersionCount(t, db, 1, 0)

	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("Up() after Down error = %v", err)
	}
	assertInitialSchema(t, db)
	assertAppliedVersionCount(t, db, 1, 1)
	assertAppliedVersionCount(t, db, 2, 1)
}

func TestMigrationFailureRollsBackDDLAndVersion(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	testioc.ResetSchema(t, db)
	dir := t.TempDir()
	writeMigrationFile(t, dir, "000001_broken.up.sql", "CREATE TABLE migration_rollback_probe (id BIGINT PRIMARY KEY);\nTHIS IS NOT VALID SQL;")
	writeMigrationFile(t, dir, "000001_broken.down.sql", "DROP TABLE migration_rollback_probe;")
	runner := testioc.InitMigrationRunner(t, db, dir)

	if _, err := runner.Up(context.Background()); err == nil {
		t.Fatal("Up() succeeded for invalid migration SQL")
	}
	assertTableExists(t, db, "migration_rollback_probe", false)
	assertTableExists(t, db, "schema_migrations", true)
	assertAppliedVersionCount(t, db, 1, 0)
}

func TestMigrationRejectsUnknownAppliedVersion(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	testioc.ResetSchema(t, db)
	runner := testioc.InitMigrationRunner(t, db, migrationDir(t))
	ctx := context.Background()

	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("Up() setup error = %v", err)
	}
	if err := db.Exec("INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)", int64(999999), "unknown", int64(1)).Error; err != nil {
		t.Fatalf("insert unknown migration version: %v", err)
	}

	if _, err := runner.Status(ctx); err == nil || !strings.Contains(err.Error(), "999999") {
		t.Fatalf("Status() error = %v, want unknown version 999999", err)
	}
	if _, err := runner.Up(ctx); err == nil || !strings.Contains(err.Error(), "999999") {
		t.Fatalf("Up() error = %v, want unknown version 999999", err)
	}
	if _, err := runner.Down(ctx); err == nil || !strings.Contains(err.Error(), "999999") {
		t.Fatalf("Down() error = %v, want unknown version 999999", err)
	}
	assertInitialSchema(t, db)
	assertAppliedVersionCount(t, db, 1, 1)
}

func TestConcurrentMigrationUpAppliesOnce(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db1 := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	db2 := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	testioc.ResetSchema(t, db1)
	runner1 := testioc.InitMigrationRunner(t, db1, migrationDir(t))
	runner2 := testioc.InitMigrationRunner(t, db2, migrationDir(t))

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, runner := range []*postgre.MigrationRunner{runner1, runner2} {
		wg.Add(1)
		go func(runner *postgre.MigrationRunner) {
			defer wg.Done()
			<-start
			_, err := runner.Up(context.Background())
			errs <- err
		}(runner)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Up() error = %v", err)
		}
	}

	assertInitialSchema(t, db1)
	assertAppliedVersionCount(t, db1, 1, 1)
	assertAppliedVersionCount(t, db1, 2, 1)
}

func assertMigrationStatus(t *testing.T, got []postgre.MigrationStatus, v1Applied, v2Applied bool) {
	t.Helper()
	if len(got) != 2 {
		t.Fatalf("Status() returned %d migrations, want 2: %#v", len(got), got)
	}
	want := []struct {
		version int64
		name    string
		applied bool
	}{{1, "initial_schema", v1Applied}, {2, "event_biz_cursors", v2Applied}}
	for i := range want {
		if got[i].Version != want[i].version || got[i].Name != want[i].name || got[i].Applied != want[i].applied {
			t.Fatalf("Status()[%d] = %#v, want version=%d name=%s applied=%t", i, got[i], want[i].version, want[i].name, want[i].applied)
		}
	}
}

func assertInitialSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, table := range businessTables {
		assertTableExists(t, db, table, true)
	}

	requiredIndexes := []string{
		"uq_chain_blocks_canonical_height",
		"idx_chain_blocks_height", "idx_chain_blocks_parent_hash", "idx_chain_blocks_reorg",
		"idx_chain_raw_events_pending", "idx_chain_raw_events_contract_topic", "idx_chain_raw_events_tx", "idx_chain_raw_events_block", "idx_chain_raw_events_reorg",
		"idx_chain_transactions_hash", "idx_chain_transactions_from", "idx_chain_transactions_to", "idx_chain_transactions_block", "idx_chain_transactions_reorg",
		"idx_chain_transfers_tx", "idx_chain_transfers_from", "idx_chain_transfers_to", "idx_chain_transfers_token", "idx_chain_transfers_nft", "idx_chain_transfers_reorg",
		"idx_chain_reorgs_chain_time",
	}
	for _, index := range requiredIndexes {
		var exists bool
		if err := db.Raw("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?)", index).Scan(&exists).Error; err != nil {
			t.Fatalf("query index %q: %v", index, err)
		}
		if !exists {
			t.Errorf("required index %q does not exist", index)
		}
	}

	assertIndexDefinitionContains(t, db, "uq_chain_blocks_canonical_height", "unique index", "(chain_id, block_number)", "where ((status)::text = 'canonical'::text)")
	assertIndexDefinitionContains(t, db, "idx_chain_raw_events_pending", "(chain_id, block_number)", "parse_status", "pending", "failed")
	for _, index := range []string{"idx_chain_transactions_from", "idx_chain_transactions_to", "idx_chain_transfers_from", "idx_chain_transfers_to", "idx_chain_transfers_token", "idx_chain_reorgs_chain_time"} {
		assertIndexDefinitionContains(t, db, index, "desc")
	}

	var prohibitedConstraints int64
	if err := db.Raw(`SELECT COUNT(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace JOIN pg_class r ON r.oid = c.conrelid WHERE n.nspname = current_schema() AND c.contype IN ('f', 'c') AND r.relname IN ?`, businessTables).Scan(&prohibitedConstraints).Error; err != nil {
		t.Fatalf("query prohibited constraints: %v", err)
	}
	if prohibitedConstraints != 0 {
		t.Errorf("schema contains %d foreign-key or CHECK constraints, want 0", prohibitedConstraints)
	}
}

func assertIndexDefinitionContains(t *testing.T, db *gorm.DB, index string, fragments ...string) {
	t.Helper()
	var definition string
	if err := db.Raw("SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?", index).Scan(&definition).Error; err != nil {
		t.Fatalf("query index definition %q: %v", index, err)
	}
	lower := strings.ToLower(definition)
	for _, fragment := range fragments {
		if !strings.Contains(lower, strings.ToLower(fragment)) {
			t.Errorf("index %q definition = %q, want fragment %q", index, definition, fragment)
		}
	}
}

func assertBusinessTablesAbsent(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, table := range businessTables {
		assertTableExists(t, db, table, false)
	}
}

func assertAppliedVersionCount(t *testing.T, db *gorm.DB, version, want int64) {
	t.Helper()
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version).Scan(&count).Error; err != nil {
		t.Fatalf("count schema_migrations version %d: %v", version, err)
	}
	if count != want {
		t.Errorf("schema_migrations count for version %d = %d, want %d", version, count, want)
	}
}
