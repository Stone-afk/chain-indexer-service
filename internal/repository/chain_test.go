package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
	"github.com/jackc/pgx/v5/pgconn"
	"reflect"
	"testing"
)

type chainDAOFake struct {
	checkpoint      postgre.ChainCheckpoint
	block           postgre.ChainBlock
	found           bool
	err             error
	batch           postgre.CanonicalBatch
	reorg           postgre.ChainReorg
	chain, height   int64
	reactivateCalls int
	reactivateReorg string
}

func (f *chainDAOFake) GetCheckpoint(_ context.Context, chain int64) (postgre.ChainCheckpoint, bool, error) {
	f.chain = chain
	return f.checkpoint, f.found, f.err
}
func (f *chainDAOFake) GetCanonicalBlock(_ context.Context, chain, height int64) (postgre.ChainBlock, bool, error) {
	f.chain, f.height = chain, height
	return f.block, f.found, f.err
}
func (f *chainDAOFake) GetBlockByHash(_ context.Context, chain int64, hash string) (postgre.ChainBlock, bool, error) {
	f.chain = chain
	f.block.BlockHash = hash
	return f.block, f.found, f.err
}
func (f *chainDAOFake) SaveCanonicalBatch(_ context.Context, b postgre.CanonicalBatch) error {
	f.batch = b
	return f.err
}
func (f *chainDAOFake) ApplyReorg(_ context.Context, r postgre.ChainReorg, c postgre.ChainCheckpoint) error {
	f.reorg, f.checkpoint = r, c
	return f.err
}

// T003 fixture hook; the production interface method is deliberately absent
// until T004, so this helper makes the expected mapping executable afterward.
func (f *chainDAOFake) ReactivateCanonicalBatch(_ context.Context, _ postgre.CanonicalBatch, expected string) error {
	f.reactivateCalls++
	f.reactivateReorg = expected
	return f.err
}
func testObserved(b domain.CanonicalBatch) domain.BlockHeader {
	return domain.BlockHeader{Number: b.Block.BlockNumber, Hash: b.Block.BlockHash, ParentHash: b.Block.ParentHash}
}

func TestCheckpointAndCanonicalBlockMapping(t *testing.T) {
	raw := xsql.JSONField(`{"n":8}`)
	state := "state"
	f := &chainDAOFake{found: true, checkpoint: postgre.ChainCheckpoint{ChainID: 137, LastProcessedHeight: 8, LastProcessedHash: "hash"}, block: postgre.ChainBlock{ChainID: 137, BlockNumber: 8, BlockHash: "hash", ParentHash: "parent", BlockTime: 55, Status: "CANONICAL", Finality: "CONFIRMED", StateRoot: &state, RawBlock: &raw, ChainVersion: 2}}
	var r ChainRepository = NewChainRepository(f)
	cp, found, err := r.GetCheckpoint(context.Background(), 137)
	if err != nil || !found || cp != (domain.Checkpoint{ChainID: 137, LastProcessedHeight: 8, LastProcessedHash: "hash"}) || f.chain != 137 {
		t.Fatalf("cp=%+v found=%v err=%v", cp, found, err)
	}
	block, found, err := r.GetCanonicalBlock(context.Background(), 137, 8)
	want := domain.StoredBlock{ChainID: 137, BlockNumber: 8, BlockHash: "hash", ParentHash: "parent", BlockTime: 55, Status: "CANONICAL", Finality: "CONFIRMED", StateRoot: &state, RawBlock: []byte(raw), ChainVersion: 2}
	if err != nil || !found || !reflect.DeepEqual(block, want) || f.height != 8 {
		t.Fatalf("block=%+v err=%v", block, err)
	}
	block.RawBlock[0] = 'x'
	if raw[0] == 'x' {
		t.Fatal("raw block aliases entity")
	}
	f.found = false
	_, found, err = r.GetCheckpoint(context.Background(), 1)
	if found || err != nil {
		t.Fatalf("found=%v err=%v", found, err)
	}
	f.err = errors.New("read")
	_, _, err = r.GetCanonicalBlock(context.Background(), 1, 8)
	if !errors.Is(err, f.err) {
		t.Fatalf("err=%v", err)
	}
}

func TestCanonicalBatchNativeTransferMapping(t *testing.T) {
	amount, to, input, price := "999999999999999999999999999999", "created", "0xab", "3"
	txIndex := 2
	raw := []byte(`{"ok":true}`)
	batch := domain.CanonicalBatch{Block: domain.StoredBlock{ChainID: 1, BlockNumber: 8, BlockHash: "b", RawBlock: raw, RawReceipts: raw, GasUsed: &price}, Transactions: []domain.StoredTransaction{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", TxIndex: 2, FromAddress: "from", ToAddress: nil, Value: amount, Input: &input, ExecutionStatus: "SUCCESS", ContractAddress: &to, TransactionFee: &price, RawTransaction: raw, RawReceipt: raw, BlockTime: 55}}, RawEvents: []domain.RawEvent{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", LogIndex: 4, RawLog: raw, TxIndex: &txIndex}}, NativeTransfers: []domain.Transfer{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", TxIndex: &txIndex, SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", TransferType: "NATIVE", FromAddress: "from", ToAddress: to, Amount: &amount, BatchIndex: 0, ChainStatus: "ACTIVE", RawData: raw, BlockTime: 55}}, Checkpoint: domain.Checkpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "b"}}
	f := &chainDAOFake{}
	r := NewChainRepository(f)
	if err := r.SaveCanonicalBatch(context.Background(), batch, testObserved(batch)); err != nil {
		t.Fatal(err)
	}
	got := f.batch
	if got.Checkpoint.ChainID != 1 || got.Checkpoint.LastProcessedHeight != 8 || got.Checkpoint.LastProcessedHash != "b" || string(*got.Block.RawBlock) != string(raw) || string(*got.Block.RawReceipts) != string(raw) {
		t.Fatalf("batch=%+v", got)
	}
	tx := got.Transactions[0]
	if tx.Value != amount || tx.ToAddress != nil || *tx.ContractAddress != "created" || *tx.TransactionFee != "3" || string(*tx.RawReceipt) != string(raw) {
		t.Fatalf("tx=%+v", tx)
	}
	ev := got.RawEvents[0]
	if ev.LogIndex != 4 || ev.TxHash != "tx" || string(ev.RawLog) != string(raw) {
		t.Fatalf("event=%+v", ev)
	}
	transfer := got.NativeTransfers[0]
	if transfer.SourceType != "TRANSACTION_VALUE" || transfer.SourceIndex != "tx-value" || transfer.BatchIndex != 0 || *transfer.Amount != amount || transfer.ToAddress != "created" || transfer.ChainStatus != "ACTIVE" {
		t.Fatalf("transfer=%+v", transfer)
	}
	raw[0] = 'x'
	if (*got.Block.RawBlock)[0] == 'x' || got.RawEvents[0].RawLog[0] == 'x' || (*transfer.RawData)[0] == 'x' {
		t.Fatal("write mapping aliases input JSON")
	}
	f.err = errors.New("batch failed")
	if err := r.SaveCanonicalBatch(context.Background(), batch, testObserved(batch)); !errors.Is(err, f.err) {
		t.Fatalf("err=%v", err)
	}
}

func TestCanonicalBatchMapsCanonicalHeightConstraint(t *testing.T) {
	constraintErr := &pgconn.PgError{Code: "23505", ConstraintName: "uq_chain_blocks_canonical_height"}
	f := &chainDAOFake{err: fmt.Errorf("insert canonical block: %w", constraintErr)}

	empty := domain.CanonicalBatch{}
	err := NewChainRepository(f).SaveCanonicalBatch(context.Background(), empty, testObserved(empty))
	if !errors.Is(err, ErrCanonicalHashConflict) {
		t.Fatalf("err=%v, want ErrCanonicalHashConflict", err)
	}
	if !errors.Is(err, constraintErr) {
		t.Fatalf("err=%v, want original PostgreSQL cause", err)
	}
}

func TestCanonicalBatchPreservesOtherWriteErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "other constraint", err: &pgconn.PgError{Code: "23505", ConstraintName: "chain_blocks_pkey"}},
		{name: "other PostgreSQL code", err: &pgconn.PgError{Code: "23503", ConstraintName: "uq_chain_blocks_canonical_height"}},
		{name: "non PostgreSQL error", err: errors.New("write failed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &chainDAOFake{err: tt.err}
			empty := domain.CanonicalBatch{}
			err := NewChainRepository(f).SaveCanonicalBatch(context.Background(), empty, testObserved(empty))
			if !errors.Is(err, tt.err) {
				t.Fatalf("err=%v, want original cause %v", err, tt.err)
			}
			if errors.Is(err, ErrCanonicalHashConflict) {
				t.Fatalf("err=%v unexpectedly maps to ErrCanonicalHashConflict", err)
			}
		})
	}
}

func TestReorgMapping(t *testing.T) {
	f := &chainDAOFake{}
	r := NewChainRepository(f)
	reorg := domain.Reorg{ReorgID: "r", ChainID: 1, AncestorHeight: 8, AncestorHash: "a", OldHeadHeight: 10, OldHeadHash: "old", NewHeadHeight: 11, NewHeadHash: "new", ReorgDepth: 2, DetectedAt: 99}
	cp := domain.Checkpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "a"}
	if err := r.ApplyReorg(context.Background(), reorg, cp); err != nil {
		t.Fatal(err)
	}
	if f.reorg.ReorgID != "r" || f.reorg.Status != "COMPLETED" || f.reorg.CompletedAt == nil || f.reorg.AncestorHeight != 8 || f.reorg.NewHeadHash != "new" || f.checkpoint.LastProcessedHeight != 8 {
		t.Fatalf("reorg=%+v cp=%+v", f.reorg, f.checkpoint)
	}
	f.err = errors.New("reorg failed")
	if err := r.ApplyReorg(context.Background(), reorg, cp); !errors.Is(err, f.err) {
		t.Fatalf("err=%v", err)
	}
}

func TestCanonicalBatchRejectsObservedIdentityBeforeDAO(t *testing.T) {
	for _, tc := range []struct {
		name string
		obs  domain.BlockHeader
	}{
		{"number", domain.BlockHeader{Number: 9, Hash: "A", ParentHash: "P"}},
		{"hash", domain.BlockHeader{Number: 8, Hash: "B", ParentHash: "P"}},
		{"parent", domain.BlockHeader{Number: 8, Hash: "A", ParentHash: "Q"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &chainDAOFake{}
			b := domain.CanonicalBatch{Block: domain.StoredBlock{ChainID: 1, BlockNumber: 8, BlockHash: "A", ParentHash: "P"}, Checkpoint: domain.Checkpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "A"}}
			if err := NewChainRepository(f).SaveCanonicalBatch(context.Background(), b, tc.obs); !errors.Is(err, ErrCanonicalIdentityMismatch) {
				t.Fatalf("want identity mismatch, got %v", err)
			}
			if f.batch.Block.BlockHash != "" {
				t.Fatal("DAO was called after observation mismatch")
			}
		})
	}
}

func TestCanonicalBatchRejectsCheckpointIdentityBeforeDAO(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Checkpoint)
	}{
		{"chain", func(c *domain.Checkpoint) { c.ChainID = 2 }},
		{"height", func(c *domain.Checkpoint) { c.LastProcessedHeight++ }},
		{"hash", func(c *domain.Checkpoint) { c.LastProcessedHash = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &chainDAOFake{}
			b := domain.CanonicalBatch{Block: domain.StoredBlock{ChainID: 1, BlockNumber: 8, BlockHash: "A", ParentHash: "P"}, Checkpoint: domain.Checkpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "A"}}
			tc.mutate(&b.Checkpoint)
			if err := NewChainRepository(f).SaveCanonicalBatch(context.Background(), b, testObserved(b)); !errors.Is(err, ErrCanonicalIdentityMismatch) {
				t.Fatalf("want identity mismatch, got %v", err)
			}
			if f.batch.Block.BlockHash != "" {
				t.Fatal("DAO was called after checkpoint mismatch")
			}
		})
	}
}

func TestGetBlockByHashMapsStoredBlock(t *testing.T) {
	raw := xsql.JSONField(`{"ok":true}`)
	f := &chainDAOFake{found: true, block: postgre.ChainBlock{ChainID: 1, BlockNumber: 8, BlockHash: "A", ParentHash: "P", RawBlock: &raw}}
	got, found, err := NewChainRepository(f).GetBlockByHash(context.Background(), 1, "A")
	if err != nil || !found || got.BlockHash != "A" || string(got.RawBlock) != string(raw) {
		t.Fatalf("got=%+v found=%v err=%v", got, found, err)
	}
}

// T003 RED contract: reactivation is intentionally absent until T004.
func TestReactivateCanonicalBatchContract(t *testing.T) {
	b := domain.CanonicalBatch{Block: domain.StoredBlock{ChainID: 1, BlockNumber: 8, BlockHash: "A", ParentHash: "P"}, Checkpoint: domain.Checkpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "A"}}
	// Keep all admission guards visible in the contract: no empty/stale reorg
	// context, and no write after an observation mismatch.
	for _, tc := range []struct {
		name  string
		reorg string
		obs   domain.BlockHeader
	}{
		{name: "empty reorg id", obs: testObserved(b)},
		{name: "observed mismatch", reorg: "r1", obs: domain.BlockHeader{Number: 8, Hash: "B", ParentHash: "P"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &chainDAOFake{}
			// This call is the deliberate RED: T002 has no reactivation API.
			if err := NewChainRepository(f).ReactivateCanonicalBatch(context.Background(), b, tc.obs, tc.reorg); !errors.Is(err, ErrCanonicalIdentityMismatch) {
				t.Fatalf("want identity mismatch, got %v", err)
			}
			if f.reactivateCalls != 0 {
				t.Fatal("DAO called after invalid admission")
			}
		})
	}
	// A valid request must map the batch and pass the exact expected ID once
	// T004 supplies the API; this is intentionally compile-RED today.
	f := &chainDAOFake{}
	if err := NewChainRepository(f).ReactivateCanonicalBatch(context.Background(), b, testObserved(b), "r1"); err != nil {
		t.Fatalf("valid reactivation: %v", err)
	}
	if f.reactivateCalls != 1 || f.reactivateReorg != "r1" {
		t.Fatalf("calls=%d reorg=%q", f.reactivateCalls, f.reactivateReorg)
	}
}

func TestCanonicalWriteErrorCategories(t *testing.T) {
	for _, reactivate := range []bool{false, true} {
		for _, tc := range []struct {
			name            string
			cause, category error
		}{
			{"identity", fmt.Errorf("DAO identity: %w", postgre.ErrCanonicalIdentityMismatch), ErrCanonicalIdentityMismatch},
			{"state", fmt.Errorf("DAO state: %w", postgre.ErrStateFenceRejected), ErrStateFenceRejected},
			{"height conflict", &pgconn.PgError{Code: "23505", ConstraintName: "uq_chain_blocks_canonical_height"}, ErrCanonicalHashConflict},
			{"other constraint", &pgconn.PgError{Code: "23505", ConstraintName: "chain_blocks_pkey"}, nil},
			{"other code", &pgconn.PgError{Code: "23503", ConstraintName: "uq_chain_blocks_canonical_height"}, nil},
			{"query error", errors.New("query failed"), nil},
		} {
			t.Run(fmt.Sprintf("reactivate=%v/%s", reactivate, tc.name), func(t *testing.T) {
				f := &chainDAOFake{err: tc.cause}
				r := NewChainRepository(f)
				b := domain.CanonicalBatch{}
				var err error
				if reactivate {
					err = r.ReactivateCanonicalBatch(context.Background(), b, testObserved(b), "r1")
				} else {
					err = r.SaveCanonicalBatch(context.Background(), b, testObserved(b))
				}
				if !errors.Is(err, tc.cause) {
					t.Fatalf("lost cause: %v", err)
				}
				for _, category := range []error{ErrCanonicalIdentityMismatch, ErrStateFenceRejected, ErrCanonicalHashConflict} {
					if errors.Is(err, category) != (category == tc.category) {
						t.Fatalf("category %v: %v", category, err)
					}
				}
				for _, category := range []error{postgre.ErrCanonicalIdentityMismatch, postgre.ErrStateFenceRejected} {
					if errors.Is(err, category) != errors.Is(tc.cause, category) {
						t.Fatalf("DAO category lost or invented: %v", err)
					}
				}
				if original, ok := tc.cause.(*pgconn.PgError); ok {
					var got *pgconn.PgError
					if !errors.As(err, &got) || got != original {
						t.Fatalf("lost PostgreSQL cause: %v", err)
					}
				}
			})
		}
	}
}
