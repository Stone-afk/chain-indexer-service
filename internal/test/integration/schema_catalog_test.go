//go:build e2e

package integration

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"gorm.io/gorm"
)

type expectedColumn struct {
	name        string
	typeName    string
	nullable    bool
	defaultExpr string
}

const unixSecondsDefault = "(floor(EXTRACT(epoch FROM CURRENT_TIMESTAMP)))::bigint"

var expectedBusinessColumns = map[string][]expectedColumn{
	"chain_checkpoints": {
		{name: "chain_id", typeName: "bigint"},
		{name: "last_processed_height", typeName: "bigint"},
		{name: "last_processed_hash", typeName: "character varying(66)"},
		{name: "created_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
		{name: "updated_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
	},
	"chain_blocks": {
		{name: "chain_id", typeName: "bigint"},
		{name: "block_number", typeName: "bigint"},
		{name: "block_hash", typeName: "character varying(66)"},
		{name: "parent_hash", typeName: "character varying(66)"},
		{name: "block_time", typeName: "bigint"},
		{name: "status", typeName: "character varying(16)", defaultExpr: "'CANONICAL'::character varying"},
		{name: "finality", typeName: "character varying(16)", defaultExpr: "'CONFIRMED'::character varying"},
		{name: "transaction_count", typeName: "integer", defaultExpr: "0"},
		{name: "state_root", typeName: "character varying(66)", nullable: true},
		{name: "transactions_root", typeName: "character varying(66)", nullable: true},
		{name: "receipts_root", typeName: "character varying(66)", nullable: true},
		{name: "gas_limit", typeName: "numeric(78,0)", nullable: true},
		{name: "gas_used", typeName: "numeric(78,0)", nullable: true},
		{name: "base_fee_per_gas", typeName: "numeric(78,0)", nullable: true},
		{name: "raw_block", typeName: "jsonb", nullable: true},
		{name: "raw_receipts", typeName: "jsonb", nullable: true},
		{name: "chain_version", typeName: "bigint", defaultExpr: "1"},
		{name: "reorg_id", typeName: "uuid", nullable: true},
		{name: "created_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
		{name: "orphaned_at", typeName: "bigint", nullable: true},
	},
	"chain_raw_events": {
		{name: "chain_id", typeName: "bigint"},
		{name: "block_number", typeName: "bigint"},
		{name: "block_hash", typeName: "character varying(66)"},
		{name: "tx_hash", typeName: "character varying(66)"},
		{name: "tx_index", typeName: "integer", nullable: true},
		{name: "log_index", typeName: "integer"},
		{name: "contract_address", typeName: "character varying(42)"},
		{name: "topic0", typeName: "character varying(66)", nullable: true},
		{name: "topic1", typeName: "character varying(66)", nullable: true},
		{name: "topic2", typeName: "character varying(66)", nullable: true},
		{name: "topic3", typeName: "character varying(66)", nullable: true},
		{name: "event_data", typeName: "text", nullable: true},
		{name: "raw_log", typeName: "jsonb"},
		{name: "parse_status", typeName: "character varying(16)", defaultExpr: "'PENDING'::character varying"},
		{name: "parser_version", typeName: "integer", nullable: true},
		{name: "parse_error", typeName: "text", nullable: true},
		{name: "chain_status", typeName: "character varying(16)", defaultExpr: "'ACTIVE'::character varying"},
		{name: "reorg_id", typeName: "uuid", nullable: true},
		{name: "created_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
		{name: "parsed_at", typeName: "bigint", nullable: true},
		{name: "reverted_at", typeName: "bigint", nullable: true},
	},
	"chain_transactions": {
		{name: "chain_id", typeName: "bigint"},
		{name: "block_number", typeName: "bigint"},
		{name: "block_hash", typeName: "character varying(66)"},
		{name: "tx_hash", typeName: "character varying(66)"},
		{name: "tx_index", typeName: "integer"},
		{name: "from_address", typeName: "character varying(42)"},
		{name: "to_address", typeName: "character varying(42)", nullable: true},
		{name: "nonce", typeName: "bigint"},
		{name: "value", typeName: "numeric(78,0)", defaultExpr: "0"},
		{name: "input", typeName: "text", nullable: true},
		{name: "method_id", typeName: "character varying(10)", nullable: true},
		{name: "method_name", typeName: "character varying(128)", nullable: true},
		{name: "tx_type", typeName: "smallint", nullable: true},
		{name: "gas_limit", typeName: "numeric(78,0)", nullable: true},
		{name: "gas_used", typeName: "numeric(78,0)", nullable: true},
		{name: "gas_price", typeName: "numeric(78,0)", nullable: true},
		{name: "effective_gas_price", typeName: "numeric(78,0)", nullable: true},
		{name: "transaction_fee", typeName: "numeric(78,0)", nullable: true},
		{name: "execution_status", typeName: "character varying(16)"},
		{name: "contract_address", typeName: "character varying(42)", nullable: true},
		{name: "chain_status", typeName: "character varying(16)", defaultExpr: "'ACTIVE'::character varying"},
		{name: "reorg_id", typeName: "uuid", nullable: true},
		{name: "raw_transaction", typeName: "jsonb", nullable: true},
		{name: "raw_receipt", typeName: "jsonb", nullable: true},
		{name: "block_time", typeName: "bigint"},
		{name: "created_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
		{name: "reverted_at", typeName: "bigint", nullable: true},
	},
	"chain_transfers": {
		{name: "id", typeName: "bigint", defaultExpr: "nextval('chain_transfers_id_seq'::regclass)"},
		{name: "chain_id", typeName: "bigint"},
		{name: "block_number", typeName: "bigint"},
		{name: "block_hash", typeName: "character varying(66)"},
		{name: "tx_hash", typeName: "character varying(66)"},
		{name: "tx_index", typeName: "integer", nullable: true},
		{name: "source_type", typeName: "character varying(24)"},
		{name: "source_index", typeName: "character varying(128)"},
		{name: "transfer_type", typeName: "character varying(16)"},
		{name: "token_address", typeName: "character varying(42)", nullable: true},
		{name: "from_address", typeName: "character varying(42)"},
		{name: "to_address", typeName: "character varying(42)"},
		{name: "amount", typeName: "numeric(78,0)", nullable: true},
		{name: "token_id", typeName: "numeric(78,0)", nullable: true},
		{name: "batch_index", typeName: "integer", defaultExpr: "0"},
		{name: "chain_status", typeName: "character varying(16)", defaultExpr: "'ACTIVE'::character varying"},
		{name: "reorg_id", typeName: "uuid", nullable: true},
		{name: "raw_data", typeName: "jsonb", nullable: true},
		{name: "block_time", typeName: "bigint"},
		{name: "created_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
		{name: "reverted_at", typeName: "bigint", nullable: true},
	},
	"chain_reorgs": {
		{name: "reorg_id", typeName: "uuid"},
		{name: "chain_id", typeName: "bigint"},
		{name: "ancestor_height", typeName: "bigint"},
		{name: "ancestor_hash", typeName: "character varying(66)"},
		{name: "old_head_height", typeName: "bigint"},
		{name: "old_head_hash", typeName: "character varying(66)"},
		{name: "new_head_height", typeName: "bigint"},
		{name: "new_head_hash", typeName: "character varying(66)"},
		{name: "reorg_depth", typeName: "integer"},
		{name: "status", typeName: "character varying(16)"},
		{name: "detected_at", typeName: "bigint", defaultExpr: unixSecondsDefault},
		{name: "completed_at", typeName: "bigint", nullable: true},
		{name: "error_message", typeName: "text", nullable: true},
	},
}

var expectedBusinessKeys = []string{
	"chain_blocks:p:chain_id,block_hash",
	"chain_checkpoints:p:chain_id",
	"chain_raw_events:p:chain_id,block_hash,log_index",
	"chain_reorgs:p:reorg_id",
	"chain_transactions:p:chain_id,block_hash,tx_hash",
	"chain_transfers:p:id",
	"chain_transfers:u:chain_id,block_hash,tx_hash,source_type,source_index,batch_index",
}

func TestInitialSchemaCatalogMatchesPDF(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, initialMigrationDir(t)).Up(context.Background()); err != nil {
		t.Fatalf("Up() error = %v", err)
	}

	if err := compareBusinessSchemaCatalog(db); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaV3CatalogExcludesTransactionType(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatalf("Up() error = %v", err)
	}
	var exists bool
	if err := db.Raw("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'chain_transactions' AND column_name = 'tx_type')").Scan(&exists).Error; err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("V3 schema still contains chain_transactions.tx_type")
	}
}

func TestSchemaCatalogComparisonDetectsDrift(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, initialMigrationDir(t)).Up(context.Background()); err != nil {
		t.Fatalf("Up() error = %v", err)
	}
	if err := db.Exec("ALTER TABLE chain_raw_events ALTER COLUMN raw_log DROP NOT NULL").Error; err != nil {
		t.Fatal(err)
	}
	if err := compareBusinessSchemaCatalog(db); err == nil || !strings.Contains(err.Error(), "chain_raw_events columns") {
		t.Fatalf("catalog comparison error = %v, want raw-events column drift", err)
	}
}

func compareBusinessSchemaCatalog(db *gorm.DB) error {
	for _, table := range businessTables {
		got, err := readColumns(db, table)
		if err != nil {
			return err
		}
		want := expectedBusinessColumns[table]
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("%s columns = %#v, want %#v", table, got, want)
		}
	}

	gotKeys, err := readBusinessKeys(db)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(gotKeys, expectedBusinessKeys) {
		return fmt.Errorf("business keys = %v, want %v", gotKeys, expectedBusinessKeys)
	}
	return nil
}

func readColumns(db *gorm.DB, table string) ([]expectedColumn, error) {
	var rows []struct {
		Name        string `gorm:"column:name"`
		TypeName    string `gorm:"column:type_name"`
		Nullable    bool   `gorm:"column:nullable"`
		DefaultExpr string `gorm:"column:default_expr"`
	}
	err := db.Raw(`
SELECT a.attname AS name,
       format_type(a.atttypid, a.atttypmod) AS type_name,
       NOT a.attnotnull AS nullable,
       COALESCE(pg_get_expr(d.adbin, d.adrelid), '') AS default_expr
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = current_schema() AND c.relname = ?
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, table).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("read %s columns: %w", table, err)
	}

	columns := make([]expectedColumn, 0, len(rows))
	for _, row := range rows {
		columns = append(columns, expectedColumn{
			name:        row.Name,
			typeName:    row.TypeName,
			nullable:    row.Nullable,
			defaultExpr: normalizeSQL(row.DefaultExpr),
		})
	}
	return columns, nil
}

func readBusinessKeys(db *gorm.DB) ([]string, error) {
	var rows []struct {
		TableName string `gorm:"column:table_name"`
		KeyType   string `gorm:"column:key_type"`
		Columns   string `gorm:"column:columns"`
	}
	err := db.Raw(`
SELECT c.relname AS table_name,
       x.contype::text AS key_type,
       string_agg(a.attname, ',' ORDER BY k.ordinality) AS columns
FROM pg_constraint x
JOIN pg_class c ON c.oid = x.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN LATERAL unnest(x.conkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE n.nspname = current_schema() AND c.relname IN ? AND x.contype IN ('p', 'u')
GROUP BY c.relname, x.oid, x.contype`, businessTables).Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("read business keys: %w", err)
	}

	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.TableName+":"+row.KeyType+":"+row.Columns)
	}
	sort.Strings(keys)
	return keys, nil
}
