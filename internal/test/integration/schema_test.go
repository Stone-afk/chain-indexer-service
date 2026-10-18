//go:build e2e

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	productionioc "github.com/Stone-afk/chain-indexer-service/cmd/chain-indexer/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/pkg/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"gorm.io/gorm"
)

type expectedIndex struct {
	table     string
	columns   []string
	unique    bool
	predicate string
}

func TestDatabaseLifecycle(t *testing.T) {
	cfg := testioc.InitConfig(t)
	database := testioc.InitDB(t, context.Background(), cfg.Database)
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("GORM().DB() error = %v", err)
	}
	if err := postgre.Close(database); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := sqlDB.PingContext(context.Background()); err == nil {
		t.Fatal("connection pool still accepts Ping after Close")
	}
}

func TestDatabaseHandleOutlivesStartupContext(t *testing.T) {
	cfg := testioc.InitConfig(t)
	setupDB := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, setupDB)
	if _, err := testioc.InitMigrationRunner(t, setupDB, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	database, cleanup, err := productionioc.InitDB(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	cancel()
	var one int
	if err := database.Raw("SELECT 1").Scan(&one).Error; err != nil || one != 1 {
		t.Fatalf("query after startup context cancellation: one=%d err=%v", one, err)
	}
}

func TestSchemaVersionRequiresAppliedMigration(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	ctx := context.Background()
	if err := postgre.CheckSchemaVersion(ctx, db, 1); err == nil {
		t.Fatal("CheckSchemaVersion() succeeded without schema_migrations")
	}
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(ctx); err != nil {
		t.Fatalf("Up() error = %v", err)
	}
	if err := postgre.CheckSchemaVersion(ctx, db, 1); err != nil {
		t.Fatalf("CheckSchemaVersion() error = %v", err)
	}
	if err := postgre.CheckSchemaVersion(ctx, db, 2); err != nil {
		t.Fatalf("CheckSchemaVersion() error for complete V3 history = %v", err)
	}
}

func TestSchemaVersionRequiresExactMigrationIdentity(t *testing.T) {
	tests := []struct {
		name        string
		version     int64
		migration   string
		wantSuccess bool
	}{
		{name: "higher version does not replace version one", version: 2, migration: "bogus"},
		{name: "version one name must match", version: 1, migration: "wrong_name"},
		{name: "required identity is accepted", version: 1, migration: "initial_schema", wantSuccess: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testioc.InitConfig(t)
			db := testioc.InitDB(t, context.Background(), cfg.Database)
			testioc.ResetSchema(t, db)
			createSchemaMigrationHistory(t, db, test.version, test.migration)
			err := postgre.CheckSchemaVersion(context.Background(), db, 1)
			if test.wantSuccess && err != nil {
				t.Fatalf("CheckSchemaVersion() error = %v", err)
			}
			if !test.wantSuccess && err == nil {
				t.Fatal("CheckSchemaVersion() accepted invalid migration history")
			}
		})
	}
}

func TestSchemaV3RequiresExactMigrationIdentity(t *testing.T) {
	for _, test := range []struct {
		name, v3Name string
		history      []struct {
			version int64
			name    string
		}
		wantSuccess bool
	}{
		{name: "v2 history is insufficient", history: []struct {
			version int64
			name    string
		}{{1, "initial_schema"}, {2, "event_biz_cursors"}}},
		{name: "v3 identity is required", history: []struct {
			version int64
			name    string
		}{{1, "initial_schema"}, {2, "event_biz_cursors"}, {3, "drop_chain_transactions_tx_type"}}, wantSuccess: true},
		{name: "v3 name must match", history: []struct {
			version int64
			name    string
		}{{1, "initial_schema"}, {2, "event_biz_cursors"}, {3, "wrong_name"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testioc.InitConfig(t)
			db := testioc.InitDB(t, context.Background(), cfg.Database)
			testioc.ResetSchema(t, db)
			if err := db.Exec(`CREATE TABLE schema_migrations (version BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, applied_at BIGINT NOT NULL)`).Error; err != nil {
				t.Fatal(err)
			}
			for _, migration := range test.history {
				if err := db.Exec(`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`, migration.version, migration.name, int64(1)).Error; err != nil {
					t.Fatal(err)
				}
			}
			err := postgre.CheckSchemaVersion(context.Background(), db, 3)
			if (err == nil) != test.wantSuccess {
				t.Fatalf("CheckSchemaVersion() error = %v, wantSuccess=%t", err, test.wantSuccess)
			}
		})
	}
}

func TestSchemaRoundTripAndConstraints(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatalf("Up() error = %v", err)
	}

	maximum := "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	blockHash := hashOf('1')
	if err := db.Exec(`INSERT INTO chain_blocks (chain_id, block_number, block_hash, parent_hash, block_time, status, finality, gas_limit, raw_block) VALUES (?, ?, ?, ?, ?, ?, ?, ?::numeric, ?::jsonb)`, 1, 100, blockHash, hashOf('0'), 1, "CANONICAL", "CONFIRMED", maximum, `{"number":"0x64"}`).Error; err != nil {
		t.Fatalf("insert block: %v", err)
	}
	var got struct {
		GasLimit   string
		RawBlock   string
		StateIsNil bool
	}
	if err := db.Raw(`SELECT gas_limit::text AS gas_limit, raw_block::text AS raw_block, state_root IS NULL AS state_is_nil FROM chain_blocks WHERE chain_id = ? AND block_hash = ?`, 1, blockHash).Scan(&got).Error; err != nil {
		t.Fatalf("read block: %v", err)
	}
	var rawBlockValue map[string]string
	if err := json.Unmarshal([]byte(got.RawBlock), &rawBlockValue); err != nil {
		t.Fatalf("decode raw block: %v", err)
	}
	if got.GasLimit != maximum || !got.StateIsNil || rawBlockValue["number"] != "0x64" {
		t.Fatalf("block round trip mismatch: %#v", got)
	}

	if err := db.Exec(`INSERT INTO chain_blocks (chain_id, block_number, block_hash, parent_hash, block_time) VALUES (?, ?, ?, ?, ?)`, 1, 100, hashOf('2'), hashOf('0'), 1).Error; err == nil {
		t.Fatal("second canonical block at same height succeeded")
	}
	if err := db.Exec(`INSERT INTO chain_blocks (chain_id, block_number, block_hash, parent_hash, block_time, status) VALUES (?, ?, ?, ?, ?, ?)`, 1, 100, hashOf('2'), hashOf('0'), 1, "ORPHANED").Error; err != nil {
		t.Fatalf("orphaned block at same height failed: %v", err)
	}

	insertTransfer := `INSERT INTO chain_transfers (chain_id, block_number, block_hash, tx_hash, source_type, source_index, transfer_type, from_address, to_address, batch_index, block_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	args := []any{1, 100, blockHash, hashOf('3'), "EVENT_LOG", "0", "ERC20", addressOf('1'), addressOf('2'), 0, 1}
	if err := db.Exec(insertTransfer, args...).Error; err != nil {
		t.Fatalf("insert transfer: %v", err)
	}
	if err := db.Exec(insertTransfer, args...).Error; err == nil {
		t.Fatal("duplicate transfer idempotency key succeeded")
	}
}

func TestSchemaMigrationsAppliedAtIsExplicit(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatalf("Up() error = %v", err)
	}

	var columnDefault sql.NullString
	if err := db.Raw(`SELECT column_default FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'schema_migrations' AND column_name = 'applied_at'`).Scan(&columnDefault).Error; err != nil {
		t.Fatal(err)
	}
	if columnDefault.Valid {
		t.Fatalf("schema_migrations.applied_at default = %q, want NULL", columnDefault.String)
	}
	var appliedAt int64
	if err := db.Raw("SELECT applied_at FROM schema_migrations WHERE version = 1").Scan(&appliedAt).Error; err != nil {
		t.Fatal(err)
	}
	if appliedAt <= 0 {
		t.Fatalf("schema_migrations.applied_at = %d, want positive Unix seconds", appliedAt)
	}
}

func TestInitialSchemaIndexesMatchPDF(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatalf("Up() error = %v", err)
	}

	expected := map[string]expectedIndex{
		"uq_chain_blocks_canonical_height":    {table: "chain_blocks", columns: []string{"chain_id", "block_number"}, unique: true, predicate: "((status)::text = 'CANONICAL'::text)"},
		"idx_chain_blocks_height":             {table: "chain_blocks", columns: []string{"chain_id", "block_number"}},
		"idx_chain_blocks_parent_hash":        {table: "chain_blocks", columns: []string{"chain_id", "parent_hash"}},
		"idx_chain_blocks_reorg":              {table: "chain_blocks", columns: []string{"reorg_id"}},
		"idx_chain_raw_events_pending":        {table: "chain_raw_events", columns: []string{"chain_id", "block_number"}, predicate: "((parse_status)::text = ANY ((ARRAY['PENDING'::character varying, 'FAILED'::character varying])::text[]))"},
		"idx_chain_raw_events_contract_topic": {table: "chain_raw_events", columns: []string{"chain_id", "contract_address", "topic0"}},
		"idx_chain_raw_events_tx":             {table: "chain_raw_events", columns: []string{"chain_id", "tx_hash"}},
		"idx_chain_raw_events_block":          {table: "chain_raw_events", columns: []string{"chain_id", "block_number"}},
		"idx_chain_raw_events_reorg":          {table: "chain_raw_events", columns: []string{"reorg_id"}},
		"idx_chain_transactions_hash":         {table: "chain_transactions", columns: []string{"chain_id", "tx_hash"}},
		"idx_chain_transactions_from":         {table: "chain_transactions", columns: []string{"chain_id", "from_address", "block_number DESC"}},
		"idx_chain_transactions_to":           {table: "chain_transactions", columns: []string{"chain_id", "to_address", "block_number DESC"}},
		"idx_chain_transactions_block":        {table: "chain_transactions", columns: []string{"chain_id", "block_number", "tx_index"}},
		"idx_chain_transactions_reorg":        {table: "chain_transactions", columns: []string{"reorg_id"}},
		"idx_chain_transfers_tx":              {table: "chain_transfers", columns: []string{"chain_id", "tx_hash"}},
		"idx_chain_transfers_from":            {table: "chain_transfers", columns: []string{"chain_id", "from_address", "block_number DESC"}},
		"idx_chain_transfers_to":              {table: "chain_transfers", columns: []string{"chain_id", "to_address", "block_number DESC"}},
		"idx_chain_transfers_token":           {table: "chain_transfers", columns: []string{"chain_id", "token_address", "block_number DESC"}},
		"idx_chain_transfers_nft":             {table: "chain_transfers", columns: []string{"chain_id", "token_address", "token_id"}},
		"idx_chain_transfers_reorg":           {table: "chain_transfers", columns: []string{"reorg_id"}},
		"idx_chain_reorgs_chain_time":         {table: "chain_reorgs", columns: []string{"chain_id", "detected_at DESC"}},
	}

	expectedNames := make([]string, 0, len(expected))
	for name, want := range expected {
		expectedNames = append(expectedNames, name)
		got := readIndex(t, db, name)
		if got.table != want.table || !reflect.DeepEqual(got.columns, want.columns) || got.unique != want.unique || normalizeSQL(got.predicate) != normalizeSQL(want.predicate) {
			t.Errorf("index %s = table %s columns %v unique=%t predicate=%q, want table %s columns %v unique=%t predicate=%q", name, got.table, got.columns, got.unique, got.predicate, want.table, want.columns, want.unique, want.predicate)
		}
	}
	sort.Strings(expectedNames)
	if got := readStandaloneIndexNames(t, db); !reflect.DeepEqual(got, expectedNames) {
		t.Errorf("standalone indexes = %v, want %v", got, expectedNames)
	}
}

func readIndex(t *testing.T, db *gorm.DB, name string) expectedIndex {
	t.Helper()
	var metadata struct {
		IndexOID  int64 `gorm:"column:index_oid"`
		TableName string
		KeyCount  int
		Unique    bool
		Predicate string
	}
	if err := db.Raw(`
SELECT i.oid AS index_oid, t.relname AS table_name, x.indnkeyatts AS key_count, x.indisunique AS unique,
       COALESCE(pg_get_expr(x.indpred, x.indrelid), '') AS predicate
FROM pg_class i
JOIN pg_namespace n ON n.oid = i.relnamespace
JOIN pg_index x ON x.indexrelid = i.oid
JOIN pg_class t ON t.oid = x.indrelid
WHERE n.nspname = current_schema() AND i.relname = ?`, name).Scan(&metadata).Error; err != nil {
		t.Fatalf("read index %q: %v", name, err)
	}
	if metadata.IndexOID == 0 {
		t.Fatalf("index %q metadata = %#v", name, metadata)
	}
	columns := make([]string, 0, metadata.KeyCount)
	for position := 1; position <= metadata.KeyCount; position++ {
		var column string
		if err := db.Raw("SELECT pg_get_indexdef(?, ?, true)", metadata.IndexOID, position).Scan(&column).Error; err != nil {
			t.Fatalf("read index %q column %d: %v", name, position, err)
		}
		var descending bool
		if err := db.Raw("SELECT pg_index_column_has_property(?, ?, 'desc')", metadata.IndexOID, position).Scan(&descending).Error; err != nil {
			t.Fatalf("read index %q column %d direction: %v", name, position, err)
		}
		if descending {
			column += " DESC"
		}
		columns = append(columns, column)
	}
	return expectedIndex{table: metadata.TableName, columns: columns, unique: metadata.Unique, predicate: metadata.Predicate}
}

func readStandaloneIndexNames(t *testing.T, db *gorm.DB) []string {
	t.Helper()
	var names []string
	if err := db.Raw(`
SELECT i.relname
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class base ON base.oid = x.indrelid
JOIN pg_namespace n ON n.oid = base.relnamespace
LEFT JOIN pg_constraint c ON c.conindid = i.oid
WHERE n.nspname = current_schema() AND base.relname IN ? AND c.oid IS NULL
ORDER BY i.relname`, businessTables).Scan(&names).Error; err != nil {
		t.Fatal(err)
	}
	return names
}

func normalizeSQL(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func createSchemaMigrationHistory(t *testing.T, db *gorm.DB, version int64, name string) {
	t.Helper()
	if err := db.Exec(`CREATE TABLE schema_migrations (version BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, applied_at BIGINT NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`, version, name, int64(1)).Error; err != nil {
		t.Fatal(err)
	}
}
