package postgre

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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
	statements             []string
	args                   [][]driver.NamedValue
	fail                   string
	err                    error
	columns                []string
	values                 [][]driver.Value
	reorgRowsAffected      *int64
	checkpointRowsAffected *int64
	reorgStatus            *string
	canonical              *canonicalSQLState
	descStatus             string
	replayRows             map[string]*chainRows
	reactivationBlock      struct {
		status, reorgID string
		orphanedAt      *int64
	}
	reactivationRows map[string]struct {
		chainStatus, reorgID string
		revertedAt           *int64
	}
	reactivationTokenRows map[string]struct {
		chainID                                              int64
		blockHash, txHash, sourceIndex, chainStatus, reorgID string
		revertedAt                                           *int64
	}
	updateFailures map[string]error
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
	for stage, failure := range c.trace.updateFailures {
		if strings.Contains(q, stage) {
			return driver.RowsAffected(0), failure
		}
	}
	if strings.Contains(q, `INSERT INTO "chain_reorgs"`) && c.trace.reorgRowsAffected != nil {
		return driver.RowsAffected(*c.trace.reorgRowsAffected), err
	}
	if strings.Contains(q, `chain_checkpoints`) && c.trace.checkpointRowsAffected != nil {
		return driver.RowsAffected(*c.trace.checkpointRowsAffected), err
	}
	return driver.RowsAffected(1), err
}
func (c *chainConn) QueryContext(_ context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if err := c.record(q, a); err != nil {
		return nil, err
	}
	if c.trace.reactivationBlock.status != "" && strings.Contains(q, `FROM "chain_blocks"`) {
		b := c.trace.reactivationBlock
		return &chainRows{columns: []string{"id", "chain_id", "block_number", "block_hash", "status", "reorg_id", "orphaned_at"}, values: [][]driver.Value{{int64(1), int64(1), int64(8), "b", b.status, b.reorgID, b.orphanedAt}}}, nil
	}
	if strings.Contains(q, "count(") || strings.Contains(q, "COUNT(") {
		count := int64(len(c.trace.reactivationTokenRows))
		if strings.Contains(q, "EXISTS") {
			count = 0
			for _, row := range c.trace.reactivationTokenRows {
				if row.txHash == "tx" && row.sourceIndex == "1" && row.chainStatus == "REVERTED" && row.reorgID == "r1" && row.revertedAt != nil && *row.revertedAt == 100 {
					count++
				}
			}
		}
		return &chainRows{columns: []string{"count"}, values: [][]driver.Value{{count}}}, nil
	}
	for key, row := range c.trace.reactivationRows {
		if c.trace.reactivationBlock.status == "" {
			break
		}
		if strings.Contains(q, `FROM "chain_transactions"`) || strings.Contains(q, `FROM "chain_raw_events"`) {
			return &chainRows{columns: []string{"id", "chain_id", "block_number", "block_hash", "tx_hash", "log_index", "source_type", "source_index", "chain_status", "reorg_id", "reverted_at"}, values: [][]driver.Value{{int64(1), int64(1), int64(8), "b", key, int64(1), "TRANSACTION_VALUE", "tx-value", row.chainStatus, row.reorgID, row.revertedAt}}}, nil
		}
	}
	for _, row := range c.trace.reactivationTokenRows {
		if strings.Contains(q, `FROM "chain_transfers"`) && strings.Contains(q, "EVENT_LOG") {
			return &chainRows{columns: []string{"id", "chain_id", "block_hash", "tx_hash", "source_type", "source_index", "chain_status", "reorg_id", "reverted_at"}, values: [][]driver.Value{{int64(1), row.chainID, row.blockHash, row.txHash, "EVENT_LOG", row.sourceIndex, row.chainStatus, row.reorgID, row.revertedAt}}}, nil
		}
	}
	if c.trace.reactivationBlock.status != "" && strings.Contains(q, `FROM "chain_transfers"`) {
		return &chainRows{columns: []string{"id", "chain_id", "block_hash", "tx_hash", "source_type", "source_index", "chain_status", "reorg_id", "reverted_at"}, values: [][]driver.Value{{int64(1), int64(1), "b", "tx", "TRANSACTION_VALUE", "tx-value", "REVERTED", "r1", int64(100)}}}, nil
	}
	for table, rows := range c.trace.replayRows {
		if strings.Contains(q, `FROM "`+table+`"`) {
			return &chainRows{columns: rows.columns, values: rows.values}, nil
		}
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
			return &chainRows{columns: []string{"chain_id", "block_hash", "log_index", "chain_status"}, values: [][]driver.Value{{int64(1), "b", int64(1), "ACTIVE"}}}, nil
		case strings.Contains(q, `FROM "chain_transactions"`):
			status := c.trace.descStatus
			if status == "" {
				status = "ACTIVE"
			}
			return &chainRows{columns: []string{"chain_id", "block_hash", "tx_hash", "chain_status"}, values: [][]driver.Value{{int64(1), "b", "tx", status}}}, nil
		case strings.Contains(q, `FROM "chain_transfers"`):
			return &chainRows{columns: []string{"chain_id", "block_hash", "tx_hash", "source_type", "source_index", "batch_index", "chain_status"}, values: [][]driver.Value{{int64(1), "b", "tx", "TRANSACTION_VALUE", "tx-value", int64(0), "ACTIVE"}}}, nil
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
	if len(c.trace.columns) > 0 {
		return &chainRows{columns: c.trace.columns, values: c.trace.values}, nil
	}
	if strings.Contains(q, `FROM "chain_blocks"`) {
		return &chainRows{columns: []string{"status"}}, nil
	}
	if strings.Contains(q, `FROM "chain_checkpoints"`) {
		return &chainRows{columns: []string{"chain_id", "last_processed_height", "last_processed_hash"}}, nil
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
	for _, failure := range []string{"", `INSERT INTO "chain_blocks"`, `INSERT INTO "chain_transactions"`, `INSERT INTO "chain_raw_events"`, `INSERT INTO "chain_transfers"`, `FROM "chain_checkpoints"`, `INSERT INTO "chain_checkpoints"`} {
		t.Run(failure, func(t *testing.T) {
			sentinel := errors.New("write failed")
			trace := &chainSQL{fail: failure, err: sentinel}
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
			if len(trace.statements) != 9 || trace.statements[0] != "BEGIN" || trace.statements[8] != "COMMIT" {
				t.Fatalf("statements=%v", trace.statements)
			}
			for i, want := range []string{`FROM "chain_blocks"`, `INSERT INTO "chain_blocks"`, `INSERT INTO "chain_transactions"`, `INSERT INTO "chain_raw_events"`, `INSERT INTO "chain_transfers"`, `FROM "chain_checkpoints"`, `INSERT INTO "chain_checkpoints"`} {
				if !strings.Contains(trace.statements[i+1], want) {
					t.Fatalf("statement %d: %s", i, trace.statements[i+1])
				}
			}
			_ = trace.statements[1]
			for _, q := range trace.statements {
				if strings.HasPrefix(q, `INSERT INTO "chain_`) && strings.Contains(q, "ON CONFLICT") {
					t.Fatalf("ordinary insert unexpectedly used recovery conflict: %s", q)
				}
			}
			cp := trace.statements[7]
			if !strings.Contains(cp, "last_processed_height") || !strings.Contains(cp, "last_processed_hash") {
				t.Fatalf("checkpoint SQL=%s", cp)
			}
		})
	}
}

func canonicalFixture() CanonicalBatch {
	return CanonicalBatch{Block: ChainBlock{ChainID: 1, BlockNumber: 8, BlockHash: "b", Status: "CANONICAL", Finality: "CONFIRMED"}, Transactions: []ChainTransaction{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", ChainStatus: "ACTIVE"}}, RawEvents: []ChainRawEvent{{ChainID: 1, BlockNumber: 8, BlockHash: "b", LogIndex: 1, RawLog: []byte(`{}`), ChainStatus: "ACTIVE"}}, NativeTransfers: []ChainTransfer{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", ChainStatus: "ACTIVE"}}, Checkpoint: ChainCheckpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "b"}}
}

// T003 RED SQL-contract: implementation must fence every row and recover only
// the exact subtree. In particular EVENT_LOG joins must use integer-to-text.
func TestReactivateCanonicalBatchSQLContract(t *testing.T) {
	b := canonicalFixture()
	for _, tc := range []struct {
		name, reorgID, blockStatus string
		rowsAffected               int64
		wantRollback               bool
	}{
		{name: "empty expected reorg", reorgID: "", blockStatus: "ORPHANED", rowsAffected: 1, wantRollback: true},
		{name: "stale reorg", reorgID: "stale-reorg", blockStatus: "ORPHANED", rowsAffected: 0, wantRollback: true},
		{name: "block not orphaned", reorgID: "r1", blockStatus: "CANONICAL", rowsAffected: 1, wantRollback: true},
		{name: "missing orphan timestamp", reorgID: "r1", blockStatus: "ORPHANED", rowsAffected: 1, wantRollback: true},
		{name: "valid fenced subtree", reorgID: "r1", blockStatus: "ORPHANED", rowsAffected: 1, wantRollback: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := int64(100)
			trace := &chainSQL{reorgRowsAffected: &tc.rowsAffected, canonical: &canonicalSQLState{status: tc.blockStatus}, reactivationBlock: struct {
				status, reorgID string
				orphanedAt      *int64
			}{tc.blockStatus, tc.reorgID, func() *int64 {
				if tc.name == "missing orphan timestamp" {
					return nil
				}
				return &now
			}()}, reactivationRows: map[string]struct {
				chainStatus, reorgID string
				revertedAt           *int64
			}{"tx": {"REVERTED", tc.reorgID, &now}}}
			dao := NewChainDAO(chainTestDB(t, trace))
			// Deliberate RED: T002 has no explicit reactivation API.
			if err := dao.ReactivateCanonicalBatch(context.Background(), b, tc.reorgID); (err == nil) == tc.wantRollback {
				t.Fatalf("error=%v wantRollback=%v", err, tc.wantRollback)
			}
			if tc.wantRollback {
				if len(trace.statements) > 0 && trace.statements[len(trace.statements)-1] != "ROLLBACK" {
					t.Fatalf("statements=%v", trace.statements)
				}
				for _, q := range trace.statements {
					if strings.Contains(q, `INSERT INTO "chain_checkpoints"`) || strings.HasPrefix(q, `UPDATE "chain_`) {
						t.Fatalf("failure changed state: %s", q)
					}
				}
			}
			if !tc.wantRollback {
				var joined bool
				for _, q := range trace.statements {
					joined = joined || strings.Contains(q, `log_index::text`)
					if strings.Contains(q, `source_index::int`) {
						t.Fatalf("unsafe cast: %s", q)
					}
				}
				if !joined {
					t.Fatal("missing integer-to-text EVENT_LOG relation")
				}
			}
		})
	}
}

func TestReactivateCanonicalBatchExplicitPersistedFences(t *testing.T) {
	now := int64(100)
	for _, tc := range []struct {
		name, blockStatus, blockReorg string
		blockOrphanedAt               *int64
		descStatus, descReorg         string
		descRevertedAt                *int64
		wantErr                       bool
	}{
		{"valid", "ORPHANED", "r1", &now, "REVERTED", "r1", &now, false},
		{"block canonical", "CANONICAL", "r1", &now, "REVERTED", "r1", &now, true},
		{"wrong block reorg", "ORPHANED", "r2", &now, "REVERTED", "r1", &now, true},
		{"nil orphan timestamp", "ORPHANED", "r1", nil, "REVERTED", "r1", &now, true},
		{"descendant active", "ORPHANED", "r1", &now, "ACTIVE", "", nil, true},
		{"descendant stale reorg", "ORPHANED", "r1", &now, "REVERTED", "r2", &now, true},
		{"descendant nil reverted timestamp", "ORPHANED", "r1", &now, "REVERTED", "r1", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := &chainSQL{reactivationBlock: struct {
				status, reorgID string
				orphanedAt      *int64
			}{tc.blockStatus, tc.blockReorg, tc.blockOrphanedAt}, reactivationRows: map[string]struct {
				chainStatus, reorgID string
				revertedAt           *int64
			}{"tx": {tc.descStatus, tc.descReorg, tc.descRevertedAt}}}
			b := canonicalFixture()
			err := NewChainDAO(chainTestDB(t, trace)).ReactivateCanonicalBatch(context.Background(), b, "r1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrStateFenceRejected) {
				t.Fatalf("want state fence, got %v", err)
			}
			if tc.wantErr && trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("not rolled back: %v", trace.statements)
			}
		})
	}
}

func TestReactivateCanonicalBatchExactSubtreeAndTokenContracts(t *testing.T) {
	now := int64(100)
	for _, tc := range []struct {
		name, table string
		mutate      func(*CanonicalBatch)
	}{
		{"missing transaction", "chain_transactions", func(b *CanonicalBatch) { b.Transactions = nil }},
		{"extra transaction", "chain_transactions", func(b *CanonicalBatch) { b.Transactions = append(b.Transactions, ChainTransaction{TxHash: "extra"}) }},
		{"missing raw event", "chain_raw_events", func(b *CanonicalBatch) { b.RawEvents = nil }},
		{"extra raw event", "chain_raw_events", func(b *CanonicalBatch) { b.RawEvents = append(b.RawEvents, ChainRawEvent{LogIndex: 9}) }},
		{"missing native", "chain_transfers", func(b *CanonicalBatch) { b.NativeTransfers = nil }},
		{"extra native", "chain_transfers", func(b *CanonicalBatch) {
			b.NativeTransfers = append(b.NativeTransfers, ChainTransfer{SourceIndex: "extra"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := &chainSQL{reactivationBlock: struct {
				status, reorgID string
				orphanedAt      *int64
			}{"ORPHANED", "r1", &now}, reactivationRows: map[string]struct {
				chainStatus, reorgID string
				revertedAt           *int64
			}{"tx": {"REVERTED", "r1", &now}}}
			b := canonicalFixture()
			tc.mutate(&b)
			err := NewChainDAO(chainTestDB(t, trace)).ReactivateCanonicalBatch(context.Background(), b, "r1")
			if !errors.Is(err, ErrStateFenceRejected) || trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("err=%v statements=%v", err, trace.statements)
			}
		})
	}

	wrongTime := int64(101)
	for _, tc := range []struct {
		name, sourceIndex, txHash, status, reorgID string
		revertedAt                                 *int64
		wantErr                                    bool
	}{
		{"valid EVENT_LOG", "1", "tx", "REVERTED", "r1", &now, false},
		{"active token", "1", "tx", "ACTIVE", "r1", &now, true},
		{"wrong token reorg", "1", "tx", "REVERTED", "r2", &now, true},
		{"nil token timestamp", "1", "tx", "REVERTED", "r1", nil, true},
		{"wrong token timestamp", "1", "tx", "REVERTED", "r1", &wrongTime, true},
		{"dirty source index", "not-a-number", "tx", "REVERTED", "r1", &now, true},
		{"wrong tx", "1", "other-tx", "REVERTED", "r1", &now, true},
		{"wrong log", "2", "tx", "REVERTED", "r1", &now, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := &chainSQL{reactivationBlock: struct {
				status, reorgID string
				orphanedAt      *int64
			}{"ORPHANED", "r1", &now}, reactivationRows: map[string]struct {
				chainStatus, reorgID string
				revertedAt           *int64
			}{"tx": {"REVERTED", "r1", &now}}, reactivationTokenRows: map[string]struct {
				chainID                                              int64
				blockHash, txHash, sourceIndex, chainStatus, reorgID string
				revertedAt                                           *int64
			}{"token": {1, "b", tc.txHash, tc.sourceIndex, tc.status, tc.reorgID, tc.revertedAt}}}
			err := NewChainDAO(chainTestDB(t, trace)).ReactivateCanonicalBatch(context.Background(), canonicalFixture(), "r1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrStateFenceRejected) {
				t.Fatalf("want state fence, got %v", err)
			}
			if tc.wantErr && trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("invalid token relation did not roll back: %v", trace.statements)
			}
			lockedEvents := false
			for _, q := range trace.statements {
				if strings.Contains(q, `INSERT INTO "chain_transfers"`) {
					t.Fatalf("token synthesized: %s", q)
				}
				if strings.Contains(q, `source_index::int`) {
					t.Fatalf("unsafe cast: %s", q)
				}
				lockedEvents = lockedEvents || strings.Contains(q, `FROM "chain_raw_events"`) && strings.Contains(q, "ORDER BY id ASC FOR UPDATE")
			}
			if !tc.wantErr && !lockedEvents {
				t.Fatalf("successful reactivation did not lock raw events: %v", trace.statements)
			}
		})
	}
}

func TestReactivateCheckpointRowsAffectedZeroRollsBack(t *testing.T) {
	for _, tc := range []struct {
		name, wantSQL string
		checkpoint    *ChainCheckpoint
	}{
		{"insert", `INSERT INTO "chain_checkpoints"`, nil},
		{"update", `UPDATE "chain_checkpoints"`, &ChainCheckpoint{ChainID: 1, LastProcessedHeight: 7, LastProcessedHash: "parent"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zero, now := int64(0), int64(100)
			trace := &chainSQL{checkpointRowsAffected: &zero, canonical: &canonicalSQLState{checkpoint: tc.checkpoint}, reactivationBlock: struct {
				status, reorgID string
				orphanedAt      *int64
			}{"ORPHANED", "r1", &now}, reactivationRows: map[string]struct {
				chainStatus, reorgID string
				revertedAt           *int64
			}{"tx": {"REVERTED", "r1", &now}}}
			err := NewChainDAO(chainTestDB(t, trace)).ReactivateCanonicalBatch(context.Background(), canonicalFixture(), "r1")
			if !errors.Is(err, ErrStateFenceRejected) || trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("err=%v statements=%v", err, trace.statements)
			}
			seen := false
			for _, q := range trace.statements {
				seen = seen || strings.Contains(q, tc.wantSQL)
			}
			if !seen {
				t.Fatalf("missing checkpoint %s: %v", tc.name, trace.statements)
			}
			if tc.name == "insert" {
				for i, q := range trace.statements {
					if !strings.Contains(q, tc.wantSQL) {
						continue
					}
					var timestamps []int64
					for _, arg := range trace.args[i-1] {
						if value, ok := arg.Value.(int64); ok && value > 8 {
							timestamps = append(timestamps, value)
						}
					}
					if len(timestamps) < 2 || timestamps[len(timestamps)-1] == 0 || timestamps[len(timestamps)-1] != timestamps[len(timestamps)-2] {
						t.Fatalf("checkpoint timestamps were not initialized equally: args=%v", trace.args[i-1])
					}
				}
			}
		})
	}
}

func TestReactivateCanonicalBatchStageFailuresRollback(t *testing.T) {
	for _, stage := range []string{`chain_blocks`, `chain_transactions`, `chain_raw_events`, `chain_transfers`, `chain_checkpoints`} {
		t.Run(stage, func(t *testing.T) {
			sentinel := errors.New("stage failure")
			now := int64(100)
			trace := &chainSQL{reactivationBlock: struct {
				status, reorgID string
				orphanedAt      *int64
			}{"ORPHANED", "r1", &now}, reactivationRows: map[string]struct {
				chainStatus, reorgID string
				revertedAt           *int64
			}{"tx": {"REVERTED", "r1", &now}}, updateFailures: map[string]error{stage: sentinel}}
			err := NewChainDAO(chainTestDB(t, trace)).ReactivateCanonicalBatch(context.Background(), canonicalFixture(), "r1")
			if !errors.Is(err, sentinel) || errors.Is(err, ErrStateFenceRejected) || errors.Is(err, ErrCanonicalIdentityMismatch) {
				t.Fatalf("err=%v", err)
			}
			if trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("statements=%v", trace.statements)
			}
			if len(trace.statements) == 0 || trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("statements=%v", trace.statements)
			}
		})
	}
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
			var state *canonicalSQLState
			if tc.name == "same replay" || tc.name == "older replay" || tc.name == "same height different hash" || tc.name == "not canonical" {
				state = &canonicalSQLState{checkpoint: tc.cp, status: tc.status}
			}
			trace := &chainSQL{canonical: state}
			err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), canonicalFixture())
			if errors.Is(err, ErrStateFenceRejected) != tc.wantErr || (err != nil) != tc.wantErr {
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
			if !errors.Is(err, ErrCanonicalIdentityMismatch) {
				t.Fatalf("want identity mismatch, got %v", err)
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
	batch := canonicalFixture()
	trace := &chainSQL{canonical: &canonicalSQLState{status: "CANONICAL", checkpoint: &batch.Checkpoint}}
	if err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	for _, q := range trace.statements {
		if strings.HasPrefix(q, `INSERT INTO "chain_transfers"`) || strings.HasPrefix(q, `UPDATE "chain_transfers"`) {
			t.Fatalf("active replay touched transfers: %s", q)
		}
	}
	for _, q := range trace.statements {
		if strings.Contains(q, `FROM "chain_transfers"`) && !strings.Contains(q, "source_type =") {
			t.Fatalf("native replay query not source-owned: %s", q)
		}
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

func TestGetBlockByHashUsesExactNaturalKey(t *testing.T) {
	trace := &chainSQL{columns: []string{"chain_id", "block_number", "block_hash", "status"}, values: [][]driver.Value{{int64(1), int64(8), "A", "CANONICAL"}}}
	_, found, err := NewChainDAO(chainTestDB(t, trace)).GetBlockByHash(context.Background(), 1, "A")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if !strings.Contains(trace.statements[0], "chain_id = $1 AND block_hash = $2") {
		t.Fatalf("query=%s", trace.statements[0])
	}
}

func TestOrdinarySaveRejectsOrphanedAndDoesNotAdvanceCheckpoint(t *testing.T) {
	trace := &chainSQL{canonical: &canonicalSQLState{status: "ORPHANED"}}
	err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), canonicalFixture())
	if !errors.Is(err, ErrStateFenceRejected) {
		t.Fatal("ordinary save implicitly reactivated orphaned block")
	}
	for _, q := range trace.statements {
		if strings.Contains(q, "chain_checkpoints") && strings.HasPrefix(q, "INSERT") {
			t.Fatalf("checkpoint advanced on rejected orphan: %s", q)
		}
	}
}

func TestOrdinarySaveRejectsInactiveDescendantsAndDoesNotWrite(t *testing.T) {
	batch := canonicalFixture()
	batch.Transactions[0].ChainStatus = "REVERTED"
	trace := &chainSQL{canonical: &canonicalSQLState{status: "CANONICAL"}, descStatus: "REVERTED"}
	err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), batch)
	if !errors.Is(err, ErrStateFenceRejected) {
		t.Fatal("accepted replay with reverted scanner-owned descendant")
	}
	for _, q := range trace.statements {
		if strings.HasPrefix(q, "INSERT") || strings.HasPrefix(q, "UPDATE") {
			t.Fatalf("replay wrote data before rejecting inactive descendant: %s", q)
		}
	}
}

func TestActiveReplayIsValidationOnlyAndRejectsNaturalKeySetMismatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*CanonicalBatch)
	}{
		{"missing transaction", func(b *CanonicalBatch) { b.Transactions = nil }},
		{"extra raw event", func(b *CanonicalBatch) {
			b.RawEvents = append(b.RawEvents, ChainRawEvent{ChainID: 1, BlockNumber: 8, BlockHash: "b", LogIndex: 99})
		}},
		{"missing native transfer", func(b *CanonicalBatch) { b.NativeTransfers = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := canonicalFixture()
			tc.mut(&batch)
			trace := &chainSQL{canonical: &canonicalSQLState{status: "CANONICAL"}}
			if err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), batch); !errors.Is(err, ErrStateFenceRejected) {
				t.Fatal("accepted scanner-owned natural-key set mismatch")
			}
			for _, q := range trace.statements {
				if strings.HasPrefix(q, "INSERT") || strings.HasPrefix(q, "UPDATE") {
					t.Fatalf("strict replay wrote/backfilled data: %s", q)
				}
			}
		})
	}
}

func TestActiveReplayRejectsDuplicateIncomingNaturalKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*CanonicalBatch)
	}{
		{"transaction", func(b *CanonicalBatch) { b.Transactions = append(b.Transactions, b.Transactions[0]) }},
		{"raw event", func(b *CanonicalBatch) { b.RawEvents = append(b.RawEvents, b.RawEvents[0]) }},
		{"native transfer", func(b *CanonicalBatch) { b.NativeTransfers = append(b.NativeTransfers, b.NativeTransfers[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := canonicalFixture()
			tc.mutate(&b)
			trace := &chainSQL{canonical: &canonicalSQLState{status: "CANONICAL"}}
			if err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), b); !errors.Is(err, ErrStateFenceRejected) {
				t.Fatal("accepted duplicate incoming natural key")
			}
		})
	}
}

func TestCanonicalHeightConflictRollsBackWithoutCheckpoint(t *testing.T) {
	trace := &chainSQL{fail: `INSERT INTO "chain_blocks"`, err: errors.New("canonical height conflict")}
	if err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), canonicalFixture()); err == nil {
		t.Fatal("accepted canonical height conflict")
	}
	if trace.statements[len(trace.statements)-1] != "ROLLBACK" {
		t.Fatalf("statements=%v", trace.statements)
	}
	for _, q := range trace.statements {
		if strings.Contains(q, "chain_checkpoints") {
			t.Fatalf("checkpoint changed after canonical conflict: %s", q)
		}
	}
}

func TestActiveReplayExactNaturalKeys(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*CanonicalBatch)
		wantErr bool
	}{
		{"transaction replacement", func(b *CanonicalBatch) { b.Transactions[0].TxHash = "other" }, true},
		{"raw event replacement", func(b *CanonicalBatch) { b.RawEvents[0].LogIndex = 2 }, true},
		{"native transfer replacement", func(b *CanonicalBatch) { b.NativeTransfers[0].SourceIndex = "other" }, true},
		{"transaction delimiter collision", func(b *CanonicalBatch) { b.Transactions[0].BlockHash = "b/tx"; b.Transactions[0].TxHash = "x" }, true},
		{"native transfer delimiter collision", func(b *CanonicalBatch) {
			b.NativeTransfers[0].TxHash = "tx/TRANSACTION_VALUE"
			b.NativeTransfers[0].SourceIndex = "x"
		}, true},
		{"exact delimiter keys", func(b *CanonicalBatch) {
			b.Transactions[0].TxHash = "tx/x"
			b.NativeTransfers[0].SourceIndex = "TRANSACTION_VALUE/x"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := canonicalFixture()
			trace := &chainSQL{canonical: &canonicalSQLState{status: "CANONICAL"}}
			if strings.Contains(tc.name, "delimiter") {
				trace.replayRows = map[string]*chainRows{
					"chain_transactions": {columns: []string{"chain_id", "block_hash", "tx_hash", "chain_status"}, values: [][]driver.Value{{int64(1), "b", "tx/x", "ACTIVE"}}},
					"chain_transfers":    {columns: []string{"chain_id", "block_hash", "tx_hash", "source_type", "source_index", "batch_index", "chain_status"}, values: [][]driver.Value{{int64(1), "b", "tx", "TRANSACTION_VALUE", "TRANSACTION_VALUE/x", int64(0), "ACTIVE"}}},
				}
				b.Transactions[0].TxHash = "tx/x"
				b.NativeTransfers[0].SourceIndex = "TRANSACTION_VALUE/x"
			}
			tc.mutate(&b)
			err := NewChainDAO(chainTestDB(t, trace)).SaveCanonicalBatch(context.Background(), b)
			if errors.Is(err, ErrStateFenceRejected) != tc.wantErr || (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			for _, q := range trace.statements {
				if strings.HasPrefix(q, "INSERT") || strings.HasPrefix(q, "UPDATE") {
					t.Fatalf("replay wrote data: %s", q)
				}
			}
		})
	}
}

func TestDistinctNaturalKeysWithDelimitersRemainDistinct(t *testing.T) {
	transactions := []ChainTransaction{{ChainID: 1, BlockHash: "b/tx", TxHash: "x"}, {ChainID: 1, BlockHash: "b", TxHash: "tx/x"}}
	events := []ChainRawEvent{{ChainID: 1, BlockHash: "b/1", LogIndex: 2}, {ChainID: 1, BlockHash: "b", LogIndex: 12}}
	transfers := []ChainTransfer{
		{ChainID: 1, BlockHash: "b", TxHash: "tx/TRANSACTION_VALUE", SourceType: "TRANSACTION_VALUE", SourceIndex: "x"},
		{ChainID: 1, BlockHash: "b", TxHash: "tx", SourceType: "TRANSACTION_VALUE", SourceIndex: "TRANSACTION_VALUE/x"},
	}
	for _, tc := range []struct {
		name  string
		count int
	}{
		{"transaction", len(uniqueTransactionKeys(transactions))},
		{"raw event", len(uniqueEventKeys(events))},
		{"native transfer", len(uniqueTransferKeys(transfers))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.count != 2 {
				t.Fatalf("distinct keys collapsed: got %d, want 2", tc.count)
			}
		})
	}
}

func TestReactivateEmptyExpectedReorgIdentity(t *testing.T) {
	trace := &chainSQL{}
	err := NewChainDAO(chainTestDB(t, trace)).ReactivateCanonicalBatch(context.Background(), canonicalFixture(), "")
	if !errors.Is(err, ErrCanonicalIdentityMismatch) || len(trace.statements) != 0 {
		t.Fatalf("err=%v SQL=%v", err, trace.statements)
	}
}

func TestCanonicalQueryErrorsRemainUnclassified(t *testing.T) {
	for _, reactivate := range []bool{false, true} {
		t.Run(fmt.Sprintf("reactivate=%v", reactivate), func(t *testing.T) {
			cause := errors.New("query failed")
			trace := &chainSQL{fail: "SELECT", err: cause}
			dao := NewChainDAO(chainTestDB(t, trace))
			var err error
			if reactivate {
				err = dao.ReactivateCanonicalBatch(context.Background(), canonicalFixture(), "r1")
			} else {
				err = dao.SaveCanonicalBatch(context.Background(), canonicalFixture())
			}
			if !errors.Is(err, cause) || errors.Is(err, ErrCanonicalIdentityMismatch) || errors.Is(err, ErrStateFenceRejected) {
				t.Fatalf("misclassified query error: %v", err)
			}
			if trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("SQL=%v", trace.statements)
			}
		})
	}
}
