package postgre

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io"
	"strings"
	"testing"
)

// This driver observes SQL and transaction boundaries only. It supplies no
// PostgreSQL locking/isolation evidence; both interleavings belong to FA04.
type chainSQL struct {
	statements        []string
	args              [][]driver.NamedValue
	fail              string
	err               error
	columns           []string
	values            [][]driver.Value
	reorgRowsAffected *int64
	reorgStatus       *string
	canonical         *canonicalSQLState
}

type canonicalSQLState struct {
	checkpoint *ChainCheckpoint
	status     string
}
type chainConnector struct{ trace *chainSQL }

func (c chainConnector) Connect(context.Context) (driver.Conn, error) {
	return &chainConn{trace: c.trace}, nil
}
func (c chainConnector) Driver() driver.Driver { return chainDriver{} }

type chainDriver struct{}

func (chainDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type chainConn struct{ trace *chainSQL }

func (*chainConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (*chainConn) Close() error                        { return nil }
func (c *chainConn) Begin() (driver.Tx, error) {
	c.trace.statements = append(c.trace.statements, "BEGIN")
	return c, nil
}
func (c *chainConn) Commit() error {
	c.trace.statements = append(c.trace.statements, "COMMIT")
	return nil
}
func (c *chainConn) Rollback() error {
	c.trace.statements = append(c.trace.statements, "ROLLBACK")
	return nil
}
func (c *chainConn) record(q string, a []driver.NamedValue) error {
	c.trace.statements = append(c.trace.statements, q)
	c.trace.args = append(c.trace.args, a)
	if c.trace.fail != "" && strings.Contains(q, c.trace.fail) {
		return c.trace.err
	}
	return nil
}
func (c *chainConn) ExecContext(_ context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	err := c.record(q, a)
	if strings.Contains(q, `INSERT INTO "chain_reorgs"`) && c.trace.reorgRowsAffected != nil {
		return driver.RowsAffected(*c.trace.reorgRowsAffected), err
	}
	return driver.RowsAffected(1), err
}
func (c *chainConn) QueryContext(_ context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if err := c.record(q, a); err != nil {
		return nil, err
	}
	if strings.Contains(q, `FROM "chain_reorgs"`) {
		rows := &chainRows{columns: []string{"status"}}
		if c.trace.reorgStatus != nil {
			rows.values = [][]driver.Value{{*c.trace.reorgStatus}}
		}
		return rows, nil
	}
	if state := c.trace.canonical; state != nil {
		switch {
		case strings.Contains(q, `FROM "chain_raw_events"`):
			return &chainRows{columns: []string{"id"}, values: [][]driver.Value{{int64(3)}, {int64(9)}}}, nil
		case strings.Contains(q, `FROM "chain_blocks"`):
			return &chainRows{columns: []string{"status"}, values: [][]driver.Value{{state.status}}}, nil
		case strings.Contains(q, `FROM "chain_checkpoints"`):
			rows := &chainRows{columns: []string{"chain_id", "last_processed_height", "last_processed_hash"}}
			if cp := state.checkpoint; cp != nil {
				rows.values = [][]driver.Value{{cp.ChainID, cp.LastProcessedHeight, cp.LastProcessedHash}}
			}
			return rows, nil
		}
	}
	return &chainRows{columns: c.trace.columns, values: c.trace.values}, nil
}

type chainRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *chainRows) Columns() []string { return r.columns }
func (*chainRows) Close() error        { return nil }
func (r *chainRows) Next(dst []driver.Value) error {
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dst, r.values[r.index])
	r.index++
	return nil
}
func chainTestDB(t *testing.T, trace *chainSQL) *gorm.DB {
	t.Helper()
	conn := sql.OpenDB(chainConnector{trace})
	t.Cleanup(func() { _ = conn.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: conn}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestCheckpointAndCanonicalBlockQueries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []string
		values  [][]driver.Value
		found   bool
	}{
		{"found", []string{"chain_id", "last_processed_height", "last_processed_hash", "block_number", "block_hash", "status"}, [][]driver.Value{{int64(137), int64(8), "hash", int64(8), "hash", "CANONICAL"}}, true},
		{"absent", []string{"chain_id"}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := &chainSQL{columns: tc.columns, values: tc.values}
			dao := NewChainDAO(chainTestDB(t, trace))
			cp, found, err := dao.GetCheckpoint(context.Background(), 137)
			if err != nil || found != tc.found {
				t.Fatalf("cp=%+v found=%v err=%v", cp, found, err)
			}
			if found && (cp.LastProcessedHeight != 8 || cp.LastProcessedHash != "hash") {
				t.Fatalf("cp=%+v", cp)
			}
			b, found, err := dao.GetCanonicalBlock(context.Background(), 137, 8)
			if err != nil || found != tc.found {
				t.Fatalf("block=%+v err=%v", b, err)
			}
			q := trace.statements[1]
			if !strings.Contains(q, "chain_id = $1") || !strings.Contains(q, "block_number = $2") || !strings.Contains(q, "status = $3") || trace.args[1][2].Value != "CANONICAL" {
				t.Fatalf("query=%s args=%v", q, trace.args)
			}
		})
	}
}

func TestCanonicalBatchTransactionAndPreciseConflict(t *testing.T) {
	for _, failure := range []string{"", "FOR UPDATE", `INSERT INTO "chain_blocks"`, `INSERT INTO "chain_transactions"`, `INSERT INTO "chain_raw_events"`, `INSERT INTO "chain_transfers"`, `UPDATE "chain_transfers"`, `FROM "chain_blocks"`, `FROM "chain_checkpoints"`, `INSERT INTO "chain_checkpoints"`} {
		t.Run(failure, func(t *testing.T) {
			sentinel := errors.New("write failed")
			trace := &chainSQL{fail: failure, err: sentinel, canonical: &canonicalSQLState{status: "CANONICAL"}}
			dao := NewChainDAO(chainTestDB(t, trace))
			batch := canonicalFixture()
			err := dao.SaveCanonicalBatch(context.Background(), batch)
			if failure != "" {
				if !errors.Is(err, sentinel) {
					t.Fatalf("err=%v", err)
				}
				if trace.statements[len(trace.statements)-1] != "ROLLBACK" {
					t.Fatalf("statements=%v", trace.statements)
				}
				for _, q := range trace.statements[:len(trace.statements)-2] {
					if strings.Contains(q, `INSERT INTO "chain_checkpoints"`) {
						t.Fatal("checkpoint written before failed source write")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(trace.statements) != 11 || trace.statements[0] != "BEGIN" || trace.statements[10] != "COMMIT" {
				t.Fatalf("statements=%v", trace.statements)
			}
			for i, want := range []string{`FROM "chain_raw_events"`, `INSERT INTO "chain_blocks"`, `INSERT INTO "chain_transactions"`, `INSERT INTO "chain_raw_events"`, `INSERT INTO "chain_transfers"`, `UPDATE "chain_transfers"`, `FROM "chain_blocks"`, `FROM "chain_checkpoints"`, `INSERT INTO "chain_checkpoints"`} {
				if !strings.Contains(trace.statements[i+1], want) {
					t.Fatalf("statement %d: %s", i, trace.statements[i+1])
				}
			}
			lock := trace.statements[1]
			if !strings.Contains(lock, "chain_id = $1 AND block_hash = $2") || !strings.Contains(lock, "ORDER BY id ASC FOR UPDATE") || trace.args[0][0].Value != int64(1) || trace.args[0][1].Value != "b" {
				t.Fatalf("lock=%s args=%v", lock, trace.args[0])
			}
			for i, key := range []string{`("chain_id","block_hash")`, `("chain_id","block_hash","tx_hash")`, `("chain_id","block_hash","log_index")`, `("chain_id","block_hash","tx_hash","source_type","source_index","batch_index")`} {
				q := trace.statements[i+2]
				if !strings.Contains(q, "ON CONFLICT "+key+" DO UPDATE SET") {
					t.Fatalf("conflict=%s", q)
				}
				assertCanonicalStateOnly(t, q, i == 0)
			}
			restore := trace.statements[6]
			if !strings.Contains(restore, "chain_id =") || !strings.Contains(restore, "block_hash =") || !strings.Contains(restore, "chain_status =") {
				t.Fatalf("token restoration=%s", restore)
			}
			for _, forbidden := range []string{"amount", "token_id", "raw_data", "source_type", "created_at"} {
				if strings.Contains(restore, forbidden) {
					t.Fatalf("token recovery changes/restricts business fields: %s", restore)
				}
			}
			cp := trace.statements[9]
			if !strings.Contains(cp, "last_processed_height") || !strings.Contains(cp, "last_processed_hash") {
				t.Fatalf("checkpoint SQL=%s", cp)
			}
		})
	}
}

func canonicalFixture() CanonicalBatch {
	return CanonicalBatch{Block: ChainBlock{ChainID: 1, BlockNumber: 8, BlockHash: "b", Status: "CANONICAL", Finality: "CONFIRMED"}, Transactions: []ChainTransaction{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", ChainStatus: "ACTIVE"}}, RawEvents: []ChainRawEvent{{ChainID: 1, BlockNumber: 8, BlockHash: "b", LogIndex: 1, RawLog: []byte(`{}`), ChainStatus: "ACTIVE"}}, NativeTransfers: []ChainTransfer{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", ChainStatus: "ACTIVE"}}, Checkpoint: ChainCheckpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "b"}}
}

func assertCanonicalStateOnly(t *testing.T, q string, block bool) {
	t.Helper()
	update := strings.Split(q, " DO UPDATE SET ")[1]
	assignments := strings.Split(update, " WHERE ")[0]
	for _, forbidden := range []string{`"id"`, "block_number", "block_hash", "tx_hash", "log_index", "raw_log", "raw_block", "raw_receipt", "raw_transaction", "raw_data", "created_at", "amount", "token_id", "value", "parse_status", "chain_version"} {
		if strings.Contains(assignments, forbidden) {
			t.Fatalf("immutable field in update: %s", update)
		}
	}
	if !strings.Contains(update, `"reorg_id"=NULL`) {
		t.Fatalf("missing explicit NULL: %s", update)
	}
	if block {
		if !strings.Contains(update, `"orphaned_at"=NULL`) || !strings.Contains(update, `"finality"=`) || !strings.Contains(update, `"chain_blocks"."status" = 'ORPHANED'`) {
			t.Fatalf("block restore=%s", update)
		}
	} else if !strings.Contains(update, `"reverted_at"=NULL`) || !strings.Contains(update, `"chain_status"=`) || !strings.Contains(update, " = 'REVERTED'") {
		t.Fatalf("row restore=%s", update)
	}
}

func TestCanonicalBatchCheckpointBranches(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cp             *ChainCheckpoint
		status         string
		write, wantErr bool
	}{
		{"absent", nil, "CANONICAL", true, false}, {"advance", &ChainCheckpoint{ChainID: 1, LastProcessedHeight: 7, LastProcessedHash: "parent"}, "CANONICAL", true, false},
		{"same replay", &ChainCheckpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "b"}, "CANONICAL", false, false}, {"older replay", &ChainCheckpoint{ChainID: 1, LastProcessedHeight: 9, LastProcessedHash: "new"}, "CANONICAL", false, false},
		{"same height different hash", &ChainCheckpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "other"}, "CANONICAL", false, true}, {"not canonical", nil, "ORPHANED", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := &chainSQL{canonical: &canonicalSQLState{checkpoint: tc.cp, status: tc.status}}
			err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), canonicalFixture())
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
			writes := 0
			for _, q := range trace.statements {
				if strings.HasPrefix(q, `INSERT INTO "chain_checkpoints"`) || strings.HasPrefix(q, `UPDATE "chain_checkpoints"`) {
					writes++
				}
			}
			if (writes > 0) != tc.write {
				t.Fatalf("checkpoint writes=%d SQL=%v", writes, trace.statements)
			}
			end := "COMMIT"
			if tc.wantErr {
				end = "ROLLBACK"
			}
			if trace.statements[len(trace.statements)-1] != end {
				t.Fatalf("SQL=%v", trace.statements)
			}
		})
	}
}

func TestCanonicalBatchRejectsCheckpointIdentityMismatch(t *testing.T) {
	for _, field := range []string{"chain", "height", "hash"} {
		t.Run(field, func(t *testing.T) {
			batch := canonicalFixture()
			switch field {
			case "chain":
				batch.Checkpoint.ChainID = 137
			case "height":
				batch.Checkpoint.LastProcessedHeight = 9
			case "hash":
				batch.Checkpoint.LastProcessedHash = "other"
			}
			trace := &chainSQL{}
			err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), batch)
			if err == nil {
				t.Fatal("accepted checkpoint identity mismatch")
			}
			for _, q := range trace.statements {
				if q != "BEGIN" && q != "ROLLBACK" {
					t.Fatalf("SQL before validation=%v", trace.statements)
				}
			}
		})
	}
}

func TestCanonicalBatchRecoveryDoesNotGenerateTokenTransfers(t *testing.T) {
	// These fixtures prove the SQL recovery protocol, not actual A→B→A database
	// state transitions, raw ID preservation or lock waits (all deferred to FA04).
	for _, name := range []string{"same ACTIVE replay", "A restored after A-B-A"} {
		t.Run(name, func(t *testing.T) {
			batch := canonicalFixture()
			batch.NativeTransfers = nil
			trace := &chainSQL{canonical: &canonicalSQLState{status: "CANONICAL", checkpoint: &batch.Checkpoint}}
			if err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			restores := 0
			for _, q := range trace.statements {
				if strings.HasPrefix(q, `INSERT INTO "chain_transfers"`) {
					t.Fatalf("generated token: %s", q)
				}
				if strings.HasPrefix(q, `UPDATE "chain_transfers"`) {
					restores++
					if !strings.Contains(q, `"reorg_id"=`) || !strings.Contains(q, `"reverted_at"=`) {
						t.Fatalf("restore=%s", q)
					}
				}
			}
			if restores != 1 {
				t.Fatalf("restores=%d", restores)
			}
		})
	}
}

func TestReorgLockOrderAndAtomicUpdates(t *testing.T) {
	for _, failure := range []string{"", "FOR UPDATE", "UPDATE \"chain_blocks\"", "UPDATE \"chain_transactions\"", "UPDATE \"chain_raw_events\"", "UPDATE \"chain_transfers\"", "INSERT INTO \"chain_reorgs\"", "INSERT INTO \"chain_checkpoints\""} {
		t.Run(failure, func(t *testing.T) {
			sentinel := errors.New("reorg failed")
			trace := &chainSQL{fail: failure, err: sentinel, columns: []string{"id"}, values: [][]driver.Value{{int64(3)}, {int64(9)}}}
			dao := NewChainDAO(chainTestDB(t, trace))
			completed := int64(99)
			err := dao.ApplyReorg(context.Background(), ChainReorg{ReorgID: "r", ChainID: 137, AncestorHeight: 8, OldHeadHeight: 10, Status: "COMPLETED", CompletedAt: &completed}, ChainCheckpoint{ChainID: 137, LastProcessedHeight: 8, LastProcessedHash: "a"})
			if failure != "" {
				if !errors.Is(err, sentinel) || trace.statements[len(trace.statements)-1] != "ROLLBACK" {
					t.Fatalf("err=%v statements=%v", err, trace.statements)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(trace.statements) != 9 || trace.statements[0] != "BEGIN" || trace.statements[8] != "COMMIT" {
				t.Fatalf("statements=%v", trace.statements)
			}
			admission := trace.statements[1]
			if !strings.Contains(admission, `INSERT INTO "chain_reorgs"`) || !strings.Contains(admission, `ON CONFLICT ("reorg_id") DO NOTHING`) || !strings.Contains(admission, `"completed_at"`) {
				t.Fatalf("first business SQL=%s", admission)
			}
			completedRecord := false
			for _, arg := range trace.args[0] {
				if arg.Value == "COMPLETED" {
					completedRecord = true
				}
			}
			if !completedRecord {
				t.Fatalf("admission args=%v", trace.args[0])
			}
			lock := trace.statements[2]
			if !strings.Contains(lock, `FROM "chain_raw_events"`) || !strings.Contains(lock, "ORDER BY id ASC FOR UPDATE") || !strings.Contains(lock, "chain_id = $1") || !strings.Contains(lock, "block_number > $2") || !strings.Contains(lock, "block_number <= $3") {
				t.Fatalf("lock=%s", lock)
			}
			for i, table := range []string{"chain_blocks", "chain_transactions", "chain_raw_events", "chain_transfers"} {
				q := trace.statements[i+3]
				if !strings.Contains(q, `UPDATE "`+table+`"`) || !strings.Contains(q, "reorg_id") || !strings.Contains(q, "chain_id =") || !strings.Contains(q, "block_number >") || !strings.Contains(q, "block_number <=") {
					t.Fatalf("update=%s", q)
				}
				want := "REVERTED"
				if i == 0 {
					want = "ORPHANED"
				}
				seen := false
				for _, arg := range trace.args[i+2] {
					if arg.Value == want {
						seen = true
					}
				}
				if !seen {
					t.Fatalf("missing %s args=%v", want, trace.args[i+2])
				}
			}
			if !strings.Contains(trace.statements[7], "last_processed_height") {
				t.Fatalf("tail=%v", trace.statements[6:])
			}
		})
	}
}

func TestReorgDuplicateAdmissionHasNoBusinessWrites(t *testing.T) {
	completed, invalid := "COMPLETED", "FAILED"
	for _, tc := range []struct {
		name    string
		status  *string
		fail    string
		wantErr bool
	}{
		{"completed replay after rescan", &completed, "", false},
		{"missing conflict record", nil, "", true},
		{"unexpected status", &invalid, "", true},
		{"read failure", &completed, `FROM "chain_reorgs"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zero := int64(0)
			sentinel := errors.New("read failed")
			trace := &chainSQL{reorgRowsAffected: &zero, reorgStatus: tc.status, fail: tc.fail, err: sentinel}
			dao := NewChainDAO(chainTestDB(t, trace))
			err := dao.ApplyReorg(context.Background(), ChainReorg{ReorgID: "r", Status: "COMPLETED"}, ChainCheckpoint{ChainID: 137, LastProcessedHeight: 8, LastProcessedHash: "ancestor"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if tc.fail != "" && !errors.Is(err, sentinel) {
				t.Fatalf("read cause lost: %v", err)
			}
			end := "COMMIT"
			if tc.wantErr {
				end = "ROLLBACK"
			}
			if len(trace.statements) != 4 || trace.statements[0] != "BEGIN" || trace.statements[3] != end {
				t.Fatalf("replay touched business data: %v", trace.statements)
			}
			if !strings.Contains(trace.statements[1], `INSERT INTO "chain_reorgs"`) || !strings.Contains(trace.statements[1], `ON CONFLICT ("reorg_id") DO NOTHING`) || !strings.Contains(trace.statements[2], `FROM "chain_reorgs"`) || !strings.Contains(trace.statements[2], "reorg_id = $1") || trace.args[1][0].Value != "r" {
				t.Fatalf("admission protocol=%v args=%v", trace.statements, trace.args)
			}
		})
	}
}
