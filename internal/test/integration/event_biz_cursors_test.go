//go:build e2e

package integration

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"gorm.io/gorm"
)

func TestEventBizCursorSchemaContract(t *testing.T) {
	db := eventCursorDB(t)
	type column struct {
		Name, DataType, Nullable string
		Default                  sql.NullString
	}
	var columns []column
	if err := db.Raw(`SELECT column_name AS name,data_type,is_nullable AS nullable,column_default AS "default" FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='chain_event_biz_cursors' ORDER BY ordinal_position`).Scan(&columns).Error; err != nil {
		t.Fatal(err)
	}
	wantColumns := []column{{"id", "bigint", "NO", sql.NullString{String: "nextval('chain_event_biz_cursors_id_seq'::regclass)", Valid: true}}, {"biz", "text", "NO", sql.NullString{}}, {"chain_family", "text", "NO", sql.NullString{}}, {"cursor", "bigint", "NO", sql.NullString{String: "0", Valid: true}}}
	if !reflect.DeepEqual(columns, wantColumns) {
		t.Fatalf("cursor columns = %#v, want %#v", columns, wantColumns)
	}
	columns = nil
	if err := db.Raw(`SELECT column_name AS name,data_type,is_nullable AS nullable,column_default AS "default" FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='chain_raw_events' AND column_name='id'`).Scan(&columns).Error; err != nil {
		t.Fatal(err)
	}
	wantRawID := []column{{"id", "bigint", "NO", sql.NullString{String: "nextval('chain_raw_events_id_seq'::regclass)", Valid: true}}}
	if !reflect.DeepEqual(columns, wantRawID) {
		t.Fatalf("raw id column = %#v, want %#v", columns, wantRawID)
	}
	var keys []string
	if err := db.Raw(`SELECT r.relname || ':' || c.conname || ':' || c.contype || ':' || COALESCE(string_agg(a.attname, ',' ORDER BY u.ord),'') FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid LEFT JOIN LATERAL unnest(c.conkey) WITH ORDINALITY u(attnum,ord) ON true LEFT JOIN pg_attribute a ON a.attrelid=r.oid AND a.attnum=u.attnum WHERE r.relname IN ('chain_event_biz_cursors','chain_raw_events') GROUP BY r.relname,c.conname,c.contype,c.oid ORDER BY 1`).Scan(&keys).Error; err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"chain_event_biz_cursors:chain_event_biz_cursors_pkey:p:id", "chain_event_biz_cursors:uq_chain_event_biz_cursors_biz_family:u:biz,chain_family", "chain_raw_events:chain_raw_events_pkey:p:chain_id,block_hash,log_index", "chain_raw_events:uq_chain_raw_events_id:u:id"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("schema constraints = %v, want exactly %v", keys, wantKeys)
	}
	if err := db.Exec(`INSERT INTO chain_event_biz_cursors (biz,chain_family) VALUES ('tx','evm'),('tx','solana'),('transfer','evm')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO chain_event_biz_cursors (biz,chain_family) VALUES ('tx','evm')`).Error; err == nil {
		t.Fatal("duplicate cursor identity succeeded")
	}
	if err := db.Exec(`UPDATE chain_event_biz_cursors SET cursor=7 WHERE biz='tx' AND chain_family='evm'`).Error; err != nil {
		t.Fatal(err)
	}
	var cursors []int64
	if err := db.Raw(`SELECT cursor FROM chain_event_biz_cursors ORDER BY biz,chain_family`).Scan(&cursors).Error; err != nil {
		t.Fatal(err)
	}
	if len(cursors) != 3 || cursors[0] != 0 || cursors[1] != 7 || cursors[2] != 0 {
		t.Fatalf("independent cursors = %v", cursors)
	}
}

func TestEventBizCursorUpgradeAndRoundTrip(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, initialMigrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	insertRawEvent(t, db, 1, 'a', 0)
	insertRawEvent(t, db, 2, 'b', 1)
	before := rawEventSnapshots(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := rawEventSnapshots(t, db); !reflect.DeepEqual(after, before) {
		t.Fatalf("raw events changed by Up: before=%v after=%v", before, after)
	}
	var ids []int64
	if err := db.Raw(`SELECT id FROM chain_raw_events ORDER BY chain_id`).Scan(&ids).Error; err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] <= 0 || ids[1] <= 0 || ids[0] == ids[1] {
		t.Fatalf("upgraded ids = %v", ids)
	}
	insertRawEvent(t, db, 3, 'c', 2)
	var thirdID int64
	if err := db.Raw(`SELECT id FROM chain_raw_events WHERE chain_id=3`).Scan(&thirdID).Error; err != nil || thirdID <= ids[1] {
		t.Fatalf("new default id = %d, existing ids=%v, err=%v", thirdID, ids, err)
	}
	if err := db.Exec(`INSERT INTO chain_raw_events (id,chain_id,block_number,block_hash,tx_hash,log_index,contract_address,raw_log) VALUES (?,?,?,?,?,?,?,?)`, ids[0], 4, 4, hashOf('d'), hashOf('d'), 3, addressOf('d'), `{}`).Error; err == nil {
		t.Fatal("duplicate event id succeeded")
	}
	if err := db.Exec(`INSERT INTO chain_raw_events (chain_id,block_number,block_hash,tx_hash,log_index,contract_address,raw_log) VALUES (?,?,?,?,?,?,?)`, 1, 9, hashOf('a'), hashOf('z'), 0, addressOf('z'), `{}`).Error; err == nil {
		t.Fatal("duplicate natural identity succeeded")
	}
	runner := testioc.InitMigrationRunner(t, db, migrationDir(t))
	if _, err := runner.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTableExists(t, db, "chain_event_biz_cursors", false)
	if after := rawEventSnapshots(t, db); !reflect.DeepEqual(after[:2], before) {
		t.Fatalf("raw events changed by Down: before=%v after=%v", before, after)
	}
	var rows int64
	if err := db.Raw(`SELECT count(*) FROM chain_raw_events`).Scan(&rows).Error; err != nil || rows != 3 {
		t.Fatalf("raw rows after Down = %d, err=%v", rows, err)
	}
	assertAppliedVersionCount(t, db, 1, 1)
	assertAppliedVersionCount(t, db, 2, 0)
	var leftovers int64
	if err := db.Raw(`SELECT (SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='chain_raw_events' AND column_name='id') + (SELECT count(*) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname IN ('chain_raw_events_id_seq','chain_event_biz_cursors_id_seq'))`).Scan(&leftovers).Error; err != nil || leftovers != 0 {
		t.Fatalf("v2 columns/sequences remaining = %d, err=%v", leftovers, err)
	}
	if _, err := runner.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTableExists(t, db, "chain_event_biz_cursors", true)
	insertRawEvent(t, db, 4, 'd', 3)
	var regeneratedID int64
	if err := db.Raw(`SELECT id FROM chain_raw_events WHERE chain_id=4`).Scan(&regeneratedID).Error; err != nil || regeneratedID <= 0 {
		t.Fatalf("re-Up default id = %d, err=%v", regeneratedID, err)
	}
}

func TestEventBizCursorIDPagination(t *testing.T) {
	db := eventCursorDB(t)
	insertRawEvent(t, db, 1, 'a', 0)
	if err := db.Exec(`INSERT INTO chain_raw_events (chain_id,block_number,block_hash,tx_hash,log_index,contract_address,raw_log) VALUES (?,?,?,?,?,?,?)`, 1, 9, hashOf('a'), hashOf('z'), 0, addressOf('z'), `{}`).Error; err == nil {
		t.Fatal("natural-key conflict succeeded")
	}
	insertRawEvent(t, db, 2, 'b', 1)
	insertRawEvent(t, db, 3, 'c', 2)
	var got []int64
	var cursor int64
	for {
		var page []int64
		if err := db.Raw(`SELECT id FROM chain_raw_events WHERE id > ? ORDER BY id ASC LIMIT 2`, cursor).Scan(&page).Error; err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		got = append(got, page...)
		cursor = page[len(page)-1]
	}
	if len(got) != 3 || got[0]+1 == got[1] || !(got[0] < got[1] && got[1] < got[2]) {
		t.Fatalf("paginated ids = %v, want ordered three with a gap", got)
	}
}

func eventCursorDB(t *testing.T) *gorm.DB {
	t.Helper()
	cfg := testioc.InitConfig(t)
	db := testioc.InitPostgreSQL(t, context.Background(), cfg.Database).GORM()
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func insertRawEvent(t *testing.T, db *gorm.DB, chainID int64, marker byte, logIndex int) {
	t.Helper()
	if err := db.Exec(`INSERT INTO chain_raw_events (chain_id,block_number,block_hash,tx_hash,log_index,contract_address,raw_log) VALUES (?,?,?,?,?,?,?)`, chainID, chainID, hashOf(marker), hashOf(marker), logIndex, addressOf(marker), `{}`).Error; err != nil {
		t.Fatal(err)
	}
}

func rawEventSnapshots(t *testing.T, db *gorm.DB) []string {
	t.Helper()
	var snapshots []string
	if err := db.Raw(`SELECT (to_jsonb(e)-'id')::text FROM chain_raw_events e ORDER BY chain_id`).Scan(&snapshots).Error; err != nil {
		t.Fatal(err)
	}
	return snapshots
}
