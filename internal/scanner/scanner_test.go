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
	t         *testing.T
	latest    int64
	headers   map[int64]domain.BlockHeader
	blocks    map[int64]domain.Block
	receipts  map[string]domain.Receipt
	fail      string
	calls     *[]string
	headerSeq map[int64][]domain.BlockHeader
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
	if seq := r.headerSeq[n]; len(seq) > 0 {
		h := seq[0]
		r.headerSeq[n] = seq[1:]
		return h, nil
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
	t             *testing.T
	cp            domain.Checkpoint
	found         bool
	stored        map[int64]domain.StoredBlock
	batches       []domain.CanonicalBatch
	reorgs        []domain.Reorg
	rewinds       []domain.Checkpoint
	fail          string
	calls         *[]string
	observed      domain.BlockHeader
	expectedReorg string
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
func (r *chainRepoFake) GetBlockByHash(_ context.Context, id int64, hash string) (domain.StoredBlock, bool, error) {
	if id != 1 {
		r.t.Fatalf("chain %d", id)
	}
	if err := r.call("stored-hash:" + hash); err != nil {
		return domain.StoredBlock{}, false, err
	}
	for _, b := range r.stored {
		if b.BlockHash == hash {
			return b, true, nil
		}
	}
	return domain.StoredBlock{}, false, nil
}

func (r *chainRepoFake) ReactivateCanonicalBatch(_ context.Context, b domain.CanonicalBatch, observed domain.BlockHeader, expected string) error {
	if err := r.call("reactivate:" + expected); err != nil {
		return err
	}
	r.batches = append(r.batches, b)
	r.observed = observed
	r.expectedReorg = expected
	r.cp = b.Checkpoint
	r.found = true
	return nil
}
func (r *chainRepoFake) SaveCanonicalBatch(_ context.Context, b domain.CanonicalBatch, observed domain.BlockHeader) error {
	if err := r.call(fmt.Sprintf("save:%d", b.Block.BlockNumber)); err != nil {
		return err
	}
	r.batches = append(r.batches, b)
	r.observed = observed
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
	r := &readerFake{t: t, latest: 5, headers: map[int64]domain.BlockHeader{}, blocks: map[int64]domain.Block{}, receipts: map[string]domain.Receipt{}, calls: &calls, headerSeq: map[int64][]domain.BlockHeader{}}
	p := &chainRepoFake{t: t, stored: map[int64]domain.StoredBlock{}, calls: &calls}
	for n := int64(0); n <= 6; n++ {
		b, rs := blockFixture(n)
		r.headers[n] = b.Header
		r.blocks[n] = b
		for _, v := range rs {
			r.receipts[v.TxHash] = v
		}
		p.stored[n] = domain.StoredBlock{ChainID: 1, BlockNumber: n, BlockHash: b.Header.Hash, ParentHash: b.Header.ParentHash, Status: domain.BlockStatusCanonical.ToString()}
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
			require(t, err == nil, err)
			if !reflect.DeepEqual(heights(p.batches), tc.want) {
				t.Fatalf("heights %v want %v", heights(p.batches), tc.want)
			}
			require(t, len(p.reorgs) == 0, "unexpected rollback")
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
		require(t, err == nil, err)
	}
	require(t, reflect.DeepEqual(heights(p.batches), []int64{3, 4, 5}), heights(p.batches))
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

func TestFinalObservationRejectsInitialCanonicalBlockBeforeWrite(t *testing.T) {
	r, p, calls := fixture(t)
	r.latest = 0
	p.stored[0] = domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", ParentHash: "hash--1", Status: domain.BlockStatusCanonical.ToString()}
	// The block body is observed as A, but the same-height final observation is B.
	// A correct scanner must classify A, re-read the final header, and perform no write.
	r.headerSeq[0] = []domain.BlockHeader{{Number: 0, Hash: "hash-0-B", ParentHash: "hash--1"}}
	if err := New(r, p, 1, 0, 0).HandleNewHead(context.Background()); err == nil {
		t.Fatal("expected final-observation mismatch")
	}
	if len(p.batches) != 0 || p.found || p.cp.LastProcessedHeight != 0 {
		t.Fatalf("mismatch wrote state: batches=%d found=%v checkpoint=%+v", len(p.batches), p.found, p.cp)
	}
	wantPrefix := []string{"latest", "checkpoint", "block:0", "receipt:tx-0", "stored-hash:hash-0", "header:0"}
	if len(*calls) < len(wantPrefix) || !reflect.DeepEqual((*calls)[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("call order=%v want prefix=%v", *calls, wantPrefix)
	}
	for _, call := range (*calls)[len(wantPrefix):] {
		if call == "save:0" || call == "reactivate:reorg-1" {
			t.Fatalf("write after final observation: %v", *calls)
		}
	}
}

func TestFinalObservationBranchesAndFencesWrites(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stored    *domain.StoredBlock
		wantWrite string
		wantReorg string
	}{
		{name: "absent", wantWrite: "save:0"},
		{name: "canonical", stored: &domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", Status: "CANONICAL"}, wantWrite: "save:0"},
		{name: "orphaned", stored: &domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", Status: "ORPHANED", ReorgID: strptr("reorg-1")}, wantWrite: "reactivate:reorg-1", wantReorg: "reorg-1"},
		{name: "orphaned without reorg", stored: &domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", Status: "ORPHANED"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, p, calls := fixture(t)
			r.latest = 0
			if tc.stored == nil {
				delete(p.stored, 0)
			} else {
				p.stored[0] = *tc.stored
			}
			r.headerSeq[0] = []domain.BlockHeader{{Number: 0, Hash: "hash-0", ParentHash: "hash--1"}}
			err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
			if tc.wantWrite == "" {
				if err == nil || len(p.batches) != 0 || p.found {
					t.Fatalf("orphaned without reorg wrote state: err=%v calls=%v", err, *calls)
				}
				return
			}
			if err != nil || len(p.batches) != 1 {
				t.Fatalf("err=%v batches=%d calls=%v", err, len(p.batches), *calls)
			}
			if p.expectedReorg != tc.wantReorg || p.observed.Hash != "hash-0" {
				t.Fatalf("observed=%+v expectedReorg=%q", p.observed, p.expectedReorg)
			}
			if !reflect.DeepEqual(*calls, []string{"latest", "checkpoint", "block:0", "receipt:tx-0", "stored-hash:hash-0", "header:0", tc.wantWrite}) {
				t.Fatalf("calls=%v", *calls)
			}
		})
	}
}

func TestFinalObservationRejectsEachHeaderIdentityMismatch(t *testing.T) {
	for _, field := range []string{"number", "hash", "parent"} {
		t.Run(field, func(t *testing.T) {
			r, p, _ := fixture(t)
			r.latest = 0
			p.stored[0] = domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", ParentHash: "hash--1", Status: domain.BlockStatusCanonical.ToString()}
			h := r.headers[0]
			switch field {
			case "number":
				h.Number++
			case "hash":
				h.Hash = "different"
			case "parent":
				h.ParentHash = "different"
			}
			r.headerSeq[0] = []domain.BlockHeader{h}
			if err := New(r, p, 1, 0, 0).HandleNewHead(context.Background()); err == nil || !errors.Is(err, ErrFinalIdentityMismatch) || len(p.batches) != 0 || p.found {
				t.Fatalf("mismatch accepted or wrote state: err=%v batches=%d", err, len(p.batches))
			}
			calls := *r.calls
			seenHeader := false
			for _, call := range calls {
				if call == "header:0" {
					seenHeader = true
				}
				if call == "save:0" || call == "reactivate:reorg-1" {
					t.Fatalf("write on mismatch: %v", calls)
				}
			}
			if !seenHeader {
				t.Fatalf("did not reach final header: %v", calls)
			}
		})
	}
}

func TestStateFenceRejectionsAreClassified(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stored domain.StoredBlock
	}{
		{"orphaned without reorg", domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", Status: domain.BlockStatusOrphaned.ToString()}},
		{"unknown status", domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", Status: "UNKNOWN"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, p, _ := fixture(t)
			r.latest = 0
			p.stored[0] = tc.stored
			r.headerSeq[0] = []domain.BlockHeader{{Number: 0, Hash: "hash-0", ParentHash: "hash--1"}}
			err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
			if !errors.Is(err, ErrStateFenceRejected) || len(p.batches) != 0 || p.found {
				t.Fatalf("err=%v batches=%d found=%v", err, len(p.batches), p.found)
			}
		})
	}
}

func TestScannerExternalReadAndReactivationErrorsPropagate(t *testing.T) {
	for _, tc := range []struct {
		name, fail string
		orphan     bool
	}{
		{"classify", "stored-hash:hash-0", false},
		{"final header", "header:0", false},
		{"reactivate", "reactivate:reorg-1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, p, _ := fixture(t)
			r.latest = 0
			if tc.orphan {
				p.stored[0] = domain.StoredBlock{ChainID: 1, BlockNumber: 0, BlockHash: "hash-0", Status: domain.BlockStatusOrphaned.ToString(), ReorgID: strptr("reorg-1")}
			}
			r.headerSeq[0] = []domain.BlockHeader{{Number: 0, Hash: "hash-0", ParentHash: "hash--1"}}
			r.fail, p.fail = tc.fail, tc.fail
			err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
			if !errors.Is(err, errStage) || len(p.batches) != 0 || p.cp.LastProcessedHeight != 0 {
				t.Fatalf("err=%v batches=%d cp=%+v", err, len(p.batches), p.cp)
			}
		})
	}
}

func strptr(s string) *string { return &s }
