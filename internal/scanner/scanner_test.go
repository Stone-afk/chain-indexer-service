package scanner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
)

var errStage = errors.New("stage failed")

type readerFake struct {
	t        *testing.T
	latest   int64
	headers  map[int64]domain.BlockHeader
	blocks   map[int64]domain.Block
	receipts map[string]domain.Receipt
	fail     string
	calls    *[]string
}

var _ provider.Reader = (*readerFake)(nil)

func (r *readerFake) call(s string) error {
	*r.calls = append(*r.calls, s)
	if r.fail == s {
		return errStage
	}
	return nil
}
func (r *readerFake) LatestBlockNumber(context.Context) (int64, error) {
	return r.latest, r.call("latest")
}
func (r *readerFake) HeaderByNumber(_ context.Context, n int64) (domain.BlockHeader, error) {
	if err := r.call(fmt.Sprintf("header:%d", n)); err != nil {
		return domain.BlockHeader{}, err
	}
	h, ok := r.headers[n]
	if !ok {
		r.t.Fatalf("unexpected header %d", n)
	}
	return h, nil
}
func (r *readerFake) BlockByNumber(_ context.Context, n int64) (domain.Block, error) {
	if err := r.call(fmt.Sprintf("block:%d", n)); err != nil {
		return domain.Block{}, err
	}
	b, ok := r.blocks[n]
	if !ok {
		r.t.Fatalf("unexpected block %d", n)
	}
	return b, nil
}
func (r *readerFake) ReceiptByTransactionHash(_ context.Context, h string) (domain.Receipt, error) {
	if err := r.call("receipt:" + h); err != nil {
		return domain.Receipt{}, err
	}
	v, ok := r.receipts[h]
	if !ok {
		r.t.Fatalf("unexpected receipt %s", h)
	}
	return v, nil
}

type chainRepoFake struct {
	t       *testing.T
	cp      domain.Checkpoint
	found   bool
	stored  map[int64]domain.StoredBlock
	batches []domain.CanonicalBatch
	reorgs  []domain.Reorg
	rewinds []domain.Checkpoint
	fail    string
	calls   *[]string
}

var _ repository.ChainRepository = (*chainRepoFake)(nil)

func (r *chainRepoFake) call(s string) error {
	*r.calls = append(*r.calls, s)
	if r.fail == s {
		return errStage
	}
	return nil
}
func (r *chainRepoFake) GetCheckpoint(_ context.Context, id int64) (domain.Checkpoint, bool, error) {
	if id != 1 {
		r.t.Fatalf("chain %d", id)
	}
	return r.cp, r.found, r.call("checkpoint")
}
func (r *chainRepoFake) GetCanonicalBlock(_ context.Context, id, n int64) (domain.StoredBlock, bool, error) {
	if id != 1 {
		r.t.Fatalf("chain %d", id)
	}
	err := r.call(fmt.Sprintf("stored:%d", n))
	b, ok := r.stored[n]
	return b, ok, err
}
func (r *chainRepoFake) SaveCanonicalBatch(_ context.Context, b domain.CanonicalBatch) error {
	if err := r.call(fmt.Sprintf("save:%d", b.Block.BlockNumber)); err != nil {
		return err
	}
	r.batches = append(r.batches, b)
	r.cp = b.Checkpoint
	r.found = true
	return nil
}
func (r *chainRepoFake) ApplyReorg(_ context.Context, v domain.Reorg, cp domain.Checkpoint) error {
	if err := r.call("apply"); err != nil {
		return err
	}
	r.reorgs = append(r.reorgs, v)
	r.rewinds = append(r.rewinds, cp)
	r.cp = cp
	return nil
}
func fixture(t *testing.T) (*readerFake, *chainRepoFake, *[]string) {
	t.Helper()
	calls := []string{}
	r := &readerFake{t: t, latest: 5, headers: map[int64]domain.BlockHeader{}, blocks: map[int64]domain.Block{}, receipts: map[string]domain.Receipt{}, calls: &calls}
	p := &chainRepoFake{t: t, stored: map[int64]domain.StoredBlock{}, calls: &calls}
	for n := int64(0); n <= 6; n++ {
		b, rs := blockFixture(n)
		r.headers[n] = b.Header
		r.blocks[n] = b
		for _, v := range rs {
			r.receipts[v.TxHash] = v
		}
		p.stored[n] = domain.StoredBlock{ChainID: 1, BlockNumber: n, BlockHash: b.Header.Hash, ParentHash: b.Header.ParentHash}
	}
	return r, p, &calls
}
func blockFixture(n int64) (domain.Block, []domain.Receipt) {
	to := "recipient"
	h := fmt.Sprintf("hash-%d", n)
	tx := fmt.Sprintf("tx-%d", n)
	b := domain.Block{Header: domain.BlockHeader{Number: n, Hash: h, ParentHash: fmt.Sprintf("hash-%d", n-1), Timestamp: 100 + n}, StateRoot: "state", TransactionsRoot: "txroot", ReceiptsRoot: "receiptroot", GasLimit: 30000, GasUsed: 21000, BaseFeePerGas: "7", RawBlock: []byte(`{"block":true}`), Transactions: []domain.Transaction{{Hash: tx, Index: 0, From: "sender", To: &to, Nonce: 4, Value: "10", Input: "0x12345678abcd", Type: 2, GasLimit: 22000, GasPrice: "11", RawTransaction: []byte(`{"tx":true}`)}}}
	rs := []domain.Receipt{{TxHash: tx, BlockNumber: n, BlockHash: h, TxIndex: 0, Status: 1, GasUsed: 21000, EffectiveGasPrice: "9", RawReceipt: []byte(`{"receipt":true}`), Logs: []domain.Log{{BlockNumber: n, BlockHash: h, TxHash: tx, TxIndex: 0, Index: 3, Address: "token", Topics: []string{"a", "b", "c", "d"}, Data: "0xbeef", RawLog: []byte(`{"log":true}`)}}}}
	return b, rs
}
func heights(bs []domain.CanonicalBatch) []int64 {
	v := []int64{}
	for _, b := range bs {
		v = append(v, b.Block.BlockNumber)
	}
	return v
}
func require(t *testing.T, ok bool, detail ...any) {
	t.Helper()
	if !ok {
		t.Fatal(detail...)
	}
}
func TestSafeStartBlockResumeGap(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		latest, start, confirm int64
		found                  bool
		cp                     int64
		want                   []int64
	}{
		{"underflow", 1, 0, 2, false, 0, []int64{}}, {"below start", 3, 4, 0, false, 0, []int64{}}, {"genesis", 0, 0, 0, false, 0, []int64{0}},
		{"start", 5, 3, 1, false, 0, []int64{3, 4}}, {"resume gap", 5, 0, 1, true, 1, []int64{2, 3, 4}}, {"duplicate head", 5, 0, 1, true, 4, []int64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, p, _ := fixture(t)
			r.latest = tc.latest
			p.found = tc.found
			p.cp = domain.Checkpoint{ChainID: 1, LastProcessedHeight: tc.cp, LastProcessedHash: fmt.Sprintf("hash-%d", tc.cp)}
			err := New(r, p, 1, tc.start, tc.confirm).HandleNewHead(context.Background())
			require(t, !(err != nil), err)
			if !reflect.DeepEqual(heights(p.batches), tc.want) {
				t.Fatalf("heights %v want %v", heights(p.batches), tc.want)
			}
			require(t, !(len(p.reorgs) != 0), "unexpected rollback")
		})
	}
}
func TestLatestReadOnEveryHead(t *testing.T) {
	r, p, calls := fixture(t)
	r.latest = 3
	s := New(r, p, 1, 3, 0)
	for _, latest := range []int64{3, 3, 2, 5} {
		r.latest = latest
		err := s.HandleNewHead(context.Background())
		require(t, !(err != nil), err)
	}
	require(t, !(!reflect.DeepEqual(heights(p.batches), []int64{3, 4, 5})), heights(p.batches))
	count := 0
	for _, c := range *calls {
		if c == "latest" {
			count++
		}
	}
	if count != 4 {
		t.Fatalf("latest reads %d", count)
	}
}
func TestFailureStopsBeforeNextHeight(t *testing.T) {
	for _, stage := range []string{"latest", "checkpoint", "header:2", "block:3", "receipt:tx-3", "save:3", "mapping"} {
		t.Run(stage, func(t *testing.T) {
			r, p, calls := fixture(t)
			p.found = true
			p.cp = domain.Checkpoint{ChainID: 1, LastProcessedHeight: 2, LastProcessedHash: "hash-2"}
			if stage == "mapping" {
				v := r.receipts["tx-3"]
				v.TxHash = "wrong"
				r.receipts["tx-3"] = v
			} else {
				r.fail = stage
				p.fail = stage
			}
			err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
			require(t, !(err == nil), "expected failure")
			if stage != "mapping" && !errors.Is(err, errStage) {
				t.Fatalf("lost cause %v", err)
			}
			require(t, !(len(p.batches) != 0 || p.cp.LastProcessedHeight != 2 || len(p.reorgs) != 0), "failure changed persisted state")
			for _, c := range *calls {
				require(t, !(c == "block:4"), "crossed failed height")
			}
		})
	}
}
