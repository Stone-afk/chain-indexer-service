package postgre

import (
	"reflect"
	"sync"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
	"gorm.io/gorm/schema"
)

func TestRowModelMetadataMatchesSchemaContract(t *testing.T) {
	tests := []struct {
		name       string
		model      any
		table      string
		primaryKey []string
	}{
		{name: "checkpoint", model: &chainCheckpointRow{}, table: "chain_checkpoints", primaryKey: []string{"chain_id"}},
		{name: "block", model: &chainBlockRow{}, table: "chain_blocks", primaryKey: []string{"chain_id", "block_hash"}},
		{name: "raw event", model: &ChainRawEvent{}, table: "chain_raw_events", primaryKey: []string{"chain_id", "block_hash", "log_index"}},
		{name: "biz cursor", model: &chainEventBizCursor{}, table: "chain_event_biz_cursors", primaryKey: []string{"id"}},
		{name: "transaction", model: &chainTransactionRow{}, table: "chain_transactions", primaryKey: []string{"chain_id", "block_hash", "tx_hash"}},
		{name: "transfer", model: &chainTransferRow{}, table: "chain_transfers", primaryKey: []string{"id"}},
		{name: "reorg", model: &chainReorgRow{}, table: "chain_reorgs", primaryKey: []string{"reorg_id"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := schema.Parse(test.model, &sync.Map{}, schema.NamingStrategy{})
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Table != test.table {
				t.Fatalf("table = %q, want %q", parsed.Table, test.table)
			}
			primaryKeys := make([]string, 0, len(parsed.PrimaryFields))
			for _, field := range parsed.PrimaryFields {
				primaryKeys = append(primaryKeys, field.DBName)
			}
			if !reflect.DeepEqual(primaryKeys, test.primaryKey) {
				t.Fatalf("primary keys = %v, want %v", primaryKeys, test.primaryKey)
			}
		})
	}
}

func TestRowModelsMapNumericJSONBAndNullableFields(t *testing.T) {
	tests := []struct {
		name     string
		model    any
		column   string
		goType   any
		nullable bool
		dataType schema.DataType
	}{
		{name: "block numeric", model: &chainBlockRow{}, column: "gas_limit", goType: (*string)(nil), nullable: true, dataType: "numeric(78,0)"},
		{name: "block JSONB", model: &chainBlockRow{}, column: "raw_block", goType: (*xsql.JSONField)(nil), nullable: true, dataType: "jsonb"},
		{name: "raw event ID", model: &ChainRawEvent{}, column: "id", goType: int64(0), nullable: false, dataType: "int"},
		{name: "raw event JSONB", model: &ChainRawEvent{}, column: "raw_log", goType: xsql.JSONField(nil), nullable: false, dataType: "jsonb"},
		{name: "raw event nullable", model: &ChainRawEvent{}, column: "topic0", goType: (*string)(nil), nullable: true, dataType: "string"},
		{name: "biz", model: &chainEventBizCursor{}, column: "biz", goType: "", nullable: false, dataType: "string"},
		{name: "chain family", model: &chainEventBizCursor{}, column: "chain_family", goType: "", nullable: false, dataType: "string"},
		{name: "cursor", model: &chainEventBizCursor{}, column: "cursor", goType: int64(0), nullable: false, dataType: "int"},
		{name: "transaction numeric", model: &chainTransactionRow{}, column: "value", goType: "", nullable: false, dataType: "numeric(78,0)"},
		{name: "transaction JSONB", model: &chainTransactionRow{}, column: "raw_receipt", goType: (*xsql.JSONField)(nil), nullable: true, dataType: "jsonb"},
		{name: "transfer numeric", model: &chainTransferRow{}, column: "amount", goType: (*string)(nil), nullable: true, dataType: "numeric(78,0)"},
		{name: "transfer JSONB", model: &chainTransferRow{}, column: "raw_data", goType: (*xsql.JSONField)(nil), nullable: true, dataType: "jsonb"},
		{name: "reorg nullable", model: &chainReorgRow{}, column: "completed_at", goType: (*int64)(nil), nullable: true, dataType: "int"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := schema.Parse(test.model, &sync.Map{}, schema.NamingStrategy{})
			if err != nil {
				t.Fatal(err)
			}
			field := parsed.FieldsByDBName[test.column]
			if field == nil {
				t.Fatalf("column %q is not mapped", test.column)
			}
			if field.FieldType != reflect.TypeOf(test.goType) {
				t.Fatalf("column %q Go type = %v, want %v", test.column, field.FieldType, reflect.TypeOf(test.goType))
			}
			if nullable := field.FieldType.Kind() == reflect.Ptr; nullable != test.nullable {
				t.Fatalf("column %q nullable = %t, want %t", test.column, nullable, test.nullable)
			}
			if field.DataType != test.dataType {
				t.Fatalf("column %q GORM data type = %q, want %q", test.column, field.DataType, test.dataType)
			}
		})
	}
}

func TestChainTransactionRowExcludesTxTypeColumn(t *testing.T) {
	parsed, err := schema.Parse(&ChainTransaction{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := parsed.FieldsByDBName["tx_type"]; ok {
		t.Fatal("chain transaction must not map tx_type")
	}
}

func TestChainRawEventBlockTimeProjectionIsReadOnly(t *testing.T) {
	parsed, err := schema.Parse(&ChainRawEvent{}, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	field := parsed.FieldsByDBName["block_time"]
	if field == nil {
		t.Fatal("block_time is not mapped")
	}
	if field.Creatable || field.Updatable || field.Readable == false {
		t.Fatalf("block_time permissions: creatable=%v updatable=%v readable=%v", field.Creatable, field.Updatable, field.Readable)
	}
}
