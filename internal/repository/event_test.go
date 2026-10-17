package repository

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
)

type eventDAOFake struct {
	rows       []postgre.ChainRawEvent
	err        error
	calls      int
	cursor     int64
	listCursor int64
	listLimit  int
}

func (f *eventDAOFake) GetBizCursor(_ context.Context, _, _ string) (int64, error) {
	f.calls++
	return f.cursor, f.err
}
func (f *eventDAOFake) ListRawEvents(_ context.Context, cursor int64, limit int) ([]postgre.ChainRawEvent, error) {
	f.calls++
	f.listCursor, f.listLimit = cursor, limit
	return f.rows, f.err
}
func (f *eventDAOFake) AdvanceBizCursor(_ context.Context, _, _ string, _ int64) error {
	f.calls++
	return f.err
}

func TestEventRepositoryMapsRawEvents(t *testing.T) {
	raw := xsql.JSONField(`{"a":1}`)
	txIndex, topic0, topic1, topic2, topic3 := 3, "t0", "t1", "t2", "t3"
	eventData, reorgID, revertedAt := "data", "reorg", int64(42)
	f := &eventDAOFake{rows: []postgre.ChainRawEvent{{
		ID: 9, ChainID: 1, BlockNumber: 2, BlockHash: "block", TxHash: "tx",
		TxIndex: &txIndex, LogIndex: 4, ContractAddress: "contract",
		Topic0: &topic0, Topic1: &topic1, Topic2: &topic2, Topic3: &topic3,
		EventData: &eventData, RawLog: raw, ChainStatus: "REVERTED",
		ReorgID: &reorgID, RevertedAt: &revertedAt,
	}}}
	rows, err := NewEventRepository(f).ListRawEvents(context.Background(), 1, 2)
	want := domain.RawEvent{
		ID: 9, ChainID: 1, BlockNumber: 2, BlockHash: "block", TxHash: "tx",
		TxIndex: &txIndex, LogIndex: 4, ContractAddress: "contract",
		Topic0: &topic0, Topic1: &topic1, Topic2: &topic2, Topic3: &topic3,
		EventData: &eventData, RawLog: []byte(raw), ChainStatus: "REVERTED",
		ReorgID: &reorgID, RevertedAt: &revertedAt,
	}
	if err != nil || !reflect.DeepEqual(rows, []domain.RawEvent{want}) || f.listCursor != 1 || f.listLimit != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	rows[0].RawLog[0] = 'x'
	if raw[0] == 'x' {
		t.Fatal("RawLog aliases DAO storage")
	}
}

func TestEventRepositoryDelegatesCursorOperations(t *testing.T) {
	f := &eventDAOFake{cursor: 7}
	r := NewEventRepository(f)
	got, err := r.GetBizCursor(context.Background(), "biz", "evm")
	if err != nil || got != 7 {
		t.Fatalf("cursor=%d err=%v", got, err)
	}
	if err := r.AdvanceBizCursor(context.Background(), "biz", "evm", 8); err != nil {
		t.Fatal(err)
	}
	if f.calls != 2 {
		t.Fatalf("calls=%d", f.calls)
	}
	f.err = errors.New("db")
	if _, err := r.GetBizCursor(context.Background(), "biz", "evm"); !errors.Is(err, f.err) {
		t.Fatalf("err=%v", err)
	}
	if err := r.AdvanceBizCursor(context.Background(), "biz", "evm", 9); !errors.Is(err, f.err) {
		t.Fatalf("advance err=%v", err)
	}
}

func TestEventRepositoryRejectsInvalidArguments(t *testing.T) {
	f := &eventDAOFake{}
	r := NewEventRepository(f)
	for _, pair := range [][2]string{{"", "evm"}, {"biz", ""}} {
		if _, err := r.GetBizCursor(context.Background(), pair[0], pair[1]); err == nil {
			t.Fatal("accepted invalid cursor key")
		}
		if err := r.AdvanceBizCursor(context.Background(), pair[0], pair[1], 1); err == nil {
			t.Fatal("accepted invalid cursor key")
		}
	}
	if err := r.AdvanceBizCursor(context.Background(), "biz", "evm", -1); err == nil {
		t.Fatal("accepted negative cursor")
	}
	for _, args := range [][2]int{{-1, 1}, {0, 0}} {
		if _, err := r.ListRawEvents(context.Background(), int64(args[0]), args[1]); err == nil {
			t.Fatal("accepted invalid page")
		}
	}
	if f.calls != 0 {
		t.Fatalf("DAO called %d times", f.calls)
	}
}
