package postgre

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// The driver checks the SQL protocol, never real PostgreSQL interleavings.
// Recovery/worker and reorg/worker locking and convergence remain FA04 evidence.
func eventTrace(status string) *chainSQL {
	var reorg, reverted driver.Value
	if status == "REVERTED" {
		reorg, reverted = "reorg-1", int64(123)
	}
	return &chainSQL{columns: []string{"id", "chain_status", "reorg_id", "reverted_at"}, values: [][]driver.Value{{int64(3), status, reorg, reverted}, {int64(9), status, reorg, reverted}}}
}

func eventTransfer(index int) ChainTransfer {
	amount := "42"
	return ChainTransfer{ChainID: 137, BlockNumber: 8, BlockHash: "fork", TxHash: "tx", SourceType: "EVENT_LOG", SourceIndex: "4", TransferType: "ERC1155", FromAddress: "from", ToAddress: "to", Amount: &amount, BatchIndex: index, ChainStatus: "STALE"}
}

func TestCommitTransfersLocksDeduplicatedIDsAndUsesLockedState(t *testing.T) {
	for _, status := range []string{"ACTIVE", "REVERTED"} {
		t.Run(status, func(t *testing.T) {
			trace := eventTrace(status)
			input := []EventTransferResult{{RawEventID: 9, Transfers: []ChainTransfer{eventTransfer(0), eventTransfer(1)}}, {RawEventID: 3}, {RawEventID: 9}}
			if err := NewEventDAO(chainTestDB(t, trace)).CommitTransfersAndAdvanceCursor(context.Background(), "transaction", "evm", 9, input); err != nil {
				t.Fatal(err)
			}
			if trace.statements[0] != "BEGIN" || trace.statements[len(trace.statements)-1] != "COMMIT" {
				t.Fatalf("SQL=%v", trace.statements)
			}
			lock := trace.statements[1]
			if !strings.Contains(lock, `FROM "chain_raw_events"`) || !strings.Contains(lock, "ORDER BY id ASC FOR UPDATE") || !strings.Contains(lock, "id IN ($1,$2)") {
				t.Fatalf("lock=%s", lock)
			}
			if len(trace.args[0]) != 2 || trace.args[0][0].Value != int64(3) || trace.args[0][1].Value != int64(9) {
				t.Fatalf("IDs=%v", trace.args[0])
			}
			// SELECT must reread the metadata while holding the row lock, without a
			// chain_status filter that would silently discard REVERTED events.
			if strings.Contains(lock, "chain_status =") {
				t.Fatalf("filtered lock=%s", lock)
			}
			writes := 0
			for i, q := range trace.statements {
				if strings.Contains(q, "parse_status") {
					t.Fatalf("parse status used: %s", q)
				}
				if !strings.HasPrefix(q, `INSERT INTO "chain_transfers"`) {
					continue
				}
				writes++
				if !strings.Contains(q, `ON CONFLICT ("chain_id","block_hash","tx_hash","source_type","source_index","batch_index") DO UPDATE SET`) {
					t.Fatalf("conflict=%s", q)
				}
				update := strings.Split(q, " DO UPDATE SET ")[1]
				assignments := strings.Split(update, " RETURNING ")[0]
				for _, field := range []string{"amount", "token_id", "raw_data", "from_address", "to_address", "created_at", "block_hash", "source_index", "batch_index"} {
					if strings.Contains(assignments, field) {
						t.Fatalf("business field overwritten: %s", update)
					}
				}
				for _, field := range []string{"chain_status", "reorg_id", "reverted_at"} {
					if !strings.Contains(assignments, field) {
						t.Fatalf("missing state sync: %s", update)
					}
				}
				args := trace.args[i-1]
				foundStatus, foundReorg, foundTime := false, false, false
				for _, arg := range args {
					if arg.Value == status {
						foundStatus = true
					}
					if arg.Value == "reorg-1" {
						foundReorg = true
					}
					if arg.Value == int64(123) {
						foundTime = true
					}
					if arg.Value == "STALE" {
						t.Fatal("used pre-lock state")
					}
				}
				if !foundStatus || (status == "REVERTED" && (!foundReorg || !foundTime)) || (status == "ACTIVE" && (foundReorg || foundTime)) {
					t.Fatalf("status=%s args=%v", status, args)
				}
			}
			if writes == 0 {
				t.Fatal("results were discarded")
			}
			cursor := trace.statements[len(trace.statements)-2]
			if !strings.Contains(cursor, "GREATEST(chain_event_biz_cursors.cursor, EXCLUDED.cursor)") {
				t.Fatalf("cursor=%s", cursor)
			}
			last := trace.args[len(trace.args)-1]
			if len(last) != 3 || last[0].Value != "transaction" || last[1].Value != "evm" || last[2].Value != int64(9) {
				t.Fatalf("cursor args=%v", last)
			}
			if input[0].Transfers[0].ChainStatus != "STALE" {
				t.Fatal("mutated decoded input")
			}
		})
	}
}

func TestCommitTransfersRollsBackAndDoesNotAdvanceOnFailure(t *testing.T) {
	for _, failure := range []string{"FOR UPDATE", `INSERT INTO "chain_transfers"`, "INSERT INTO chain_event_biz_cursors"} {
		t.Run(failure, func(t *testing.T) {
			trace := eventTrace("ACTIVE")
			sentinel := errors.New("database failure")
			trace.fail, trace.err = failure, sentinel
			err := NewEventDAO(chainTestDB(t, trace)).CommitTransfersAndAdvanceCursor(context.Background(), "transaction", "evm", 9, []EventTransferResult{{RawEventID: 3, Transfers: []ChainTransfer{eventTransfer(0)}}, {RawEventID: 9, Transfers: []ChainTransfer{eventTransfer(1)}}})
			if !errors.Is(err, sentinel) || trace.statements[len(trace.statements)-1] != "ROLLBACK" {
				t.Fatalf("err=%v SQL=%v", err, trace.statements)
			}
			if failure != "INSERT INTO chain_event_biz_cursors" {
				for _, q := range trace.statements {
					if strings.Contains(q, "chain_event_biz_cursors") {
						t.Fatalf("cursor advanced after failure: %v", trace.statements)
					}
				}
			}
		})
	}
}

func TestCommitTransfersEmptyResultAndMissingLockedEvent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  [][]driver.Value
		wantErr bool
	}{
		{"empty decode still consumed", [][]driver.Value{{int64(3), "REVERTED", "r", int64(4)}}, false},
		{"missing raw event", nil, true},
		{"unexpected state", [][]driver.Value{{int64(3), "INVALID", nil, nil}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := eventTrace("ACTIVE")
			trace.values = tc.values
			err := NewEventDAO(chainTestDB(t, trace)).CommitTransfersAndAdvanceCursor(context.Background(), "transaction", "evm", 3, []EventTransferResult{{RawEventID: 3}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
			end := "COMMIT"
			if tc.wantErr {
				end = "ROLLBACK"
			}
			if trace.statements[len(trace.statements)-1] != end {
				t.Fatalf("SQL=%v", trace.statements)
			}
			for _, q := range trace.statements {
				if strings.HasPrefix(q, `INSERT INTO "chain_transfers"`) || (tc.wantErr && strings.Contains(q, "chain_event_biz_cursors")) {
					t.Fatalf("unexpected write: %s", q)
				}
			}
		})
	}
}

func TestCommitTransfersPartialDuplicateKeepsOtherBatchItems(t *testing.T) {
	trace := eventTrace("ACTIVE")
	input := []EventTransferResult{{RawEventID: 3, Transfers: []ChainTransfer{eventTransfer(0), eventTransfer(1)}}}
	if err := NewEventDAO(chainTestDB(t, trace)).CommitTransfersAndAdvanceCursor(context.Background(), "transaction", "evm", 9, input); err != nil {
		t.Fatal(err)
	}
	// PostgreSQL conflict handling must be per row. The emitted records include
	// both batch items; FA04 verifies the existing row and new row in real PG.
	var indices []int64
	for i, q := range trace.statements {
		if !strings.HasPrefix(q, `INSERT INTO "chain_transfers"`) {
			continue
		}
		columns := strings.Split(strings.Split(strings.Split(q, " (")[1], ") VALUES")[0], ",")
		batchColumn := -1
		for n, column := range columns {
			if column == `"batch_index"` {
				batchColumn = n
			}
		}
		if batchColumn < 0 {
			t.Fatalf("missing batch column: %s", q)
		}
		args := trace.args[i-1]
		for offset := batchColumn; offset < len(args); offset += len(columns) {
			indices = append(indices, args[offset].Value.(int64))
		}
	}
	if !reflect.DeepEqual(indices, []int64{0, 1}) {
		t.Fatalf("missing expanded items: %v SQL=%v", indices, trace.statements)
	}
}
