package repository

import (
	"context"
	"errors"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
	"reflect"
	"testing"
)

type chainDAOFake struct {
	checkpoint    postgre.ChainCheckpoint
	block         postgre.ChainBlock
	found         bool
	err           error
	batch         postgre.CanonicalBatch
	reorg         postgre.ChainReorg
	chain, height int64
}

func (f *chainDAOFake) GetCheckpoint(_ context.Context, chain int64) (postgre.ChainCheckpoint, bool, error) {
	f.chain = chain
	return f.checkpoint, f.found, f.err
}
func (f *chainDAOFake) GetCanonicalBlock(_ context.Context, chain, height int64) (postgre.ChainBlock, bool, error) {
	f.chain, f.height = chain, height
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
	typ := int16(2)
	raw := []byte(`{"ok":true}`)
	batch := domain.CanonicalBatch{Block: domain.StoredBlock{ChainID: 1, BlockNumber: 8, BlockHash: "b", RawBlock: raw, RawReceipts: raw, GasUsed: &price}, Transactions: []domain.StoredTransaction{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", TxIndex: 2, FromAddress: "from", ToAddress: nil, Value: amount, Input: &input, TxType: &typ, ExecutionStatus: "SUCCESS", ContractAddress: &to, TransactionFee: &price, RawTransaction: raw, RawReceipt: raw, BlockTime: 55}}, RawEvents: []domain.RawEvent{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", LogIndex: 4, RawLog: raw, TxIndex: &txIndex}}, NativeTransfers: []domain.Transfer{{ChainID: 1, BlockNumber: 8, BlockHash: "b", TxHash: "tx", TxIndex: &txIndex, SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", TransferType: "NATIVE", FromAddress: "from", ToAddress: to, Amount: &amount, BatchIndex: 0, ChainStatus: "ACTIVE", RawData: raw, BlockTime: 55}}, Checkpoint: domain.Checkpoint{ChainID: 1, LastProcessedHeight: 8, LastProcessedHash: "b"}}
	f := &chainDAOFake{}
	r := NewChainRepository(f)
	if err := r.SaveCanonicalBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	got := f.batch
	if got.Checkpoint.ChainID != 1 || got.Checkpoint.LastProcessedHeight != 8 || got.Checkpoint.LastProcessedHash != "b" || string(*got.Block.RawBlock) != string(raw) || string(*got.Block.RawReceipts) != string(raw) {
		t.Fatalf("batch=%+v", got)
	}
	tx := got.Transactions[0]
	if tx.Value != amount || tx.ToAddress != nil || *tx.ContractAddress != "created" || *tx.TransactionFee != "3" || *tx.TxType != 2 || string(*tx.RawReceipt) != string(raw) {
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
	if err := r.SaveCanonicalBatch(context.Background(), batch); !errors.Is(err, f.err) {
		t.Fatalf("err=%v", err)
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
