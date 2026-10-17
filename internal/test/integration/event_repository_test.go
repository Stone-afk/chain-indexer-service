//go:build e2e

package integration

import (
	"context"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/cmd/chain-indexer/ioc"
)

func TestEventRepositoryPostgreContract(t *testing.T) {
	db := eventCursorDB(t)
	r := ioc.InitEventRepository(ioc.InitEventDAO(db))
	ctx := context.Background()
	got, err := r.GetBizCursor(ctx, "tx", "evm")
	if err != nil || got != 0 {
		t.Fatalf("missing cursor=%d err=%v", got, err)
	}
	var count int64
	if err := db.Table("chain_event_biz_cursors").Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("read inserted rows=%d err=%v", count, err)
	}
	for _, n := range []int64{7, 5} {
		if err := r.AdvanceBizCursor(ctx, "tx", "evm", n); err != nil {
			t.Fatal(err)
		}
	}
	got, err = r.GetBizCursor(ctx, "tx", "evm")
	if err != nil || got != 7 {
		t.Fatalf("cursor=%d err=%v", got, err)
	}
	got, err = r.GetBizCursor(ctx, "tx", "solana")
	if err != nil || got != 0 {
		t.Fatalf("other namespace=%d err=%v", got, err)
	}
	got, err = r.GetBizCursor(ctx, "transfer", "evm")
	if err != nil || got != 0 {
		t.Fatalf("other biz=%d err=%v", got, err)
	}
	insertRawEvent(t, db, 1, 'a', 0)
	if err := db.Exec(`INSERT INTO chain_raw_events (chain_id,block_number,block_hash,tx_hash,log_index,contract_address,raw_log) VALUES (?,?,?,?,?,?,?)`, 1, 9, hashOf('a'), hashOf('z'), 0, addressOf('z'), `{}`).Error; err == nil {
		t.Fatal("expected uniqueness gap")
	}
	insertRawEvent(t, db, 2, 'b', 1)
	insertRawEvent(t, db, 3, 'c', 2)
	if err := db.Exec(`UPDATE chain_raw_events SET parse_status='FAILED',chain_status='REVERTED' WHERE chain_id=2`).Error; err != nil {
		t.Fatal(err)
	}
	page, err := r.ListRawEvents(ctx, 0, 2)
	if err != nil || len(page) != 2 || page[0].ID >= page[1].ID || page[1].ID <= page[0].ID+1 || page[1].ChainStatus != "REVERTED" {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	next, err := r.ListRawEvents(ctx, page[1].ID, 2)
	if err != nil || len(next) != 1 || next[0].ID <= page[1].ID {
		t.Fatalf("next=%+v err=%v", next, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.ListRawEvents(canceled, 0, 1); err == nil {
		t.Fatal("canceled query succeeded")
	}
	if _, err := r.GetBizCursor(canceled, "tx", "evm"); err == nil {
		t.Fatal("failed cursor lookup returned missing cursor")
	}
}
