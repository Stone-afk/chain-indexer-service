package transaction

import (
	"context"
	"errors"
	"fmt"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"testing"
	"time"
)

type fakeEventRepo struct {
	cursor                     int64
	events                     []domain.RawEvent
	getErr, listErr, commitErr error
	gets, lists, commits       int
	listedCursor               int64
	listedLimit                int
	committedCursor            int64
	results                    []domain.EventTransferResult
	trace                      []string
	listCalls                  chan int
	releaseFirstList           chan struct{}
}

func (f *fakeEventRepo) GetBizCursor(_ context.Context, b, fam string) (int64, error) {
	f.gets++
	f.trace = append(f.trace, "get:"+b+":"+fam)
	return f.cursor, f.getErr
}
func (f *fakeEventRepo) ListRawEvents(_ context.Context, c int64, l int) ([]domain.RawEvent, error) {
	f.lists++
	if f.listCalls != nil {
		f.listCalls <- f.lists
	}
	if f.lists == 1 && f.releaseFirstList != nil {
		<-f.releaseFirstList
	}
	f.listedCursor = c
	f.listedLimit = l
	f.trace = append(f.trace, "list")
	return f.events, f.listErr
}
func (f *fakeEventRepo) CommitTransfersAndAdvanceCursor(_ context.Context, b, fam string, c int64, r []domain.EventTransferResult) error {
	f.commits++
	f.committedCursor = c
	f.results = r
	f.trace = append(f.trace, "commit")
	if b != "transaction" || fam != "evm" {
		return errors.New("scope")
	}
	return f.commitErr
}
func TestProcessBatchContract(t *testing.T) {
	c := mustCatalog(t)
	p := mustParser(t)
	f := &fakeEventRepo{cursor: 4, events: []domain.RawEvent{
		{ID: 5, ChainID: 1, BlockNumber: 10, BlockHash: "b5", TxHash: "t5", LogIndex: 1, ContractAddress: "token", Topic0: &c.ERC20Transfer.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), EventData: pptr("0x" + fmt.Sprintf("%064x", 42)), RawLog: []byte(`{"i":5}`), ChainStatus: "ACTIVE"},
		{ID: 6, ChainID: 1, BlockNumber: 10, BlockHash: "b6", TxHash: "t6", LogIndex: 2, ContractAddress: "token", Topic0: &c.ERC721Transfer.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), Topic3: pptr(pword(7)), EventData: pptr("0x"), RawLog: []byte(`{"i":6}`), ChainStatus: "REVERTED", ReorgID: pptr("r6"), RevertedAt: pptrInt64(99)},
		{ID: 7, Topic0: pptr("0xdead"), EventData: pptr("0x")},
		{ID: 8, ChainID: 1, BlockNumber: 10, BlockHash: "b8", TxHash: "t8", LogIndex: 4, ContractAddress: "token", Topic0: &c.ERC20Transfer.Topic0, Topic1: pptr(pword(1)), Topic2: pptr(pword(2)), EventData: pptr("0x" + fmt.Sprintf("%064x", 8)), RawLog: []byte(`{"i":8}`), ChainStatus: "ACTIVE"},
	}}
	n, e := NewWorker(f, p, 100, time.Second).ProcessBatch(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if n != 4 || f.listedCursor != 4 || f.listedLimit != 100 || f.commits != 1 || f.committedCursor != 8 || len(f.results) != 4 || f.results[0].RawEventID != 5 || f.results[1].RawEventID != 6 || f.results[2].RawEventID != 7 || f.results[3].RawEventID != 8 || len(f.results[0].Transfers) != 1 || f.results[0].Transfers[0].ChainStatus != "ACTIVE" || len(f.results[1].Transfers) != 1 || f.results[1].Transfers[0].ChainStatus != "REVERTED" || f.results[1].Transfers[0].ReorgID == nil || *f.results[1].Transfers[0].ReorgID != "r6" || len(f.results[2].Transfers) != 0 || len(f.results[3].Transfers) != 1 {
		t.Fatalf("%+v", f)
	}
	f3 := &fakeEventRepo{cursor: 4, events: f.events[:1]}
	if _, err := NewWorker(f3, p, 3, time.Second).ProcessBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f3.listedLimit != 3 {
		t.Fatalf("list limit=%d", f3.listedLimit)
	}
}
func TestProcessBatchErrorsAndEmpty(t *testing.T) {
	p := mustParser(t)
	for _, x := range []struct {
		n       string
		g, l, c error
	}{{"g", errors.New("g"), nil, nil}, {"l", nil, errors.New("l"), nil}, {"c", nil, nil, errors.New("c")}, {"e", nil, nil, nil}} {
		t.Run(x.n, func(t *testing.T) {
			f := &fakeEventRepo{events: []domain.RawEvent{{ID: 1}}, getErr: x.g, listErr: x.l, commitErr: x.c}
			if x.n == "e" {
				f.events = nil
			}
			n, e := NewWorker(f, p, 100, time.Second).ProcessBatch(context.Background())
			if x.n == "e" && (e != nil || f.commits != 0) {
				t.Fatal(e)
			}
			if x.n == "e" && n != 0 {
				t.Fatalf("count=%d", n)
			}
			if x.n != "e" && !errors.Is(e, map[string]error{"g": x.g, "l": x.l, "c": x.c}[x.n]) {
				t.Fatal("no error")
			}
			if x.n == "g" && (len(f.trace) != 1 || f.trace[0] != "get:transaction:evm") || x.n == "l" && (len(f.trace) != 2 || f.trace[0] != "get:transaction:evm" || f.trace[1] != "list") || x.n == "c" && (f.commits != 1 || len(f.trace) != 3) {
				t.Fatalf("trace=%v", f.trace)
			}
		})
	}
}
func TestRunCancellationAndError(t *testing.T) {
	p := mustParser(t)
	ctx, c := context.WithCancel(context.Background())
	d := make(chan error, 1)
	go func() { d <- NewWorker(&fakeEventRepo{}, p, 100, time.Second).Run(ctx) }()
	c()
	select {
	case e := <-d:
		if e != nil && e != context.Canceled {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	if e := NewWorker(&fakeEventRepo{getErr: errors.New("stop")}, p, 100, time.Second).Run(context.Background()); e == nil {
		t.Fatal("missing")
	}
}

func TestRunEmptyPageIdlesAndThenCancels(t *testing.T) {
	p := mustParser(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan int)
	release := make(chan struct{})
	f := &fakeEventRepo{listCalls: calls, releaseFirstList: release}
	done := make(chan error, 1)
	go func() { done <- NewWorker(f, p, 100, 200*time.Millisecond).Run(ctx) }()
	select {
	case n := <-calls:
		if n != 1 {
			t.Fatalf("first list=%d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("first batch timeout")
	}
	release <- struct{}{}
	select {
	case n := <-calls:
		t.Fatalf("busy loop list=%d", n)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case n := <-calls:
		if n != 2 {
			t.Fatalf("second list=%d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle boundary timeout")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel timeout")
	}
}
