//go:build e2e

package integration

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/worker/transaction"
	"github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
	"gorm.io/gorm"
)

func reorgFailureDB(t *testing.T) (*gorm.DB, context.Context) {
	t.Helper()
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db, context.Background()
}

func failureBatch(tb testing.TB, hash byte, height int64) postgre.CanonicalBatch {
	tb.Helper()
	bh, th := hashOf(hash), hashOf(hash+1)
	from, to := addressOf('1'), addressOf('2')
	idx := 0
	raw := postgre.ChainRawEvent{ChainID: 1, BlockNumber: height, BlockHash: bh, TxHash: th, TxIndex: &idx, LogIndex: 0, ContractAddress: addressOf('3'), RawLog: xsql.JSONField(`{"address":"` + addressOf('3') + `"}`), ChainStatus: "ACTIVE"}
	events, err := abi.NewTransferEvents()
	if err != nil {
		tb.Fatalf("new transfer events: %v", err)
	}
	topic := func(a string) string { return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(a, "0x") }
	t1, t2 := topic(from), topic(to)
	raw.Topic0, raw.Topic1, raw.Topic2, raw.EventData = &events.ERC20Transfer.Topic0, &t1, &t2, func() *string { v := "0x" + strings.Repeat("0", 63) + "1"; return &v }()
	native := postgre.ChainTransfer{ChainID: 1, BlockNumber: height, BlockHash: bh, TxHash: th, SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", TransferType: "NATIVE", FromAddress: from, ToAddress: to, Amount: func() *string { v := "1"; return &v }(), ChainStatus: "ACTIVE"}
	return postgre.CanonicalBatch{Block: postgre.ChainBlock{ChainID: 1, BlockNumber: height, BlockHash: bh, ParentHash: hashOf('p'), BlockTime: 1, Status: "CANONICAL", Finality: "CONFIRMED", TransactionCount: 1}, Transactions: []postgre.ChainTransaction{{ChainID: 1, BlockNumber: height, BlockHash: bh, TxHash: th, TxIndex: 0, FromAddress: from, ToAddress: &to, Value: "1", GasLimit: ptr("21000"), GasUsed: ptr("21000"), GasPrice: ptr("1"), EffectiveGasPrice: ptr("1"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 1}}, RawEvents: []postgre.ChainRawEvent{raw}, NativeTransfers: []postgre.ChainTransfer{native}, Checkpoint: postgre.ChainCheckpoint{ChainID: 1, LastProcessedHeight: height, LastProcessedHash: bh}}
}

type reorgSnapshot struct {
	Blocks     []postgre.ChainBlock
	Txs        []postgre.ChainTransaction
	Raw        []postgre.ChainRawEvent
	Native     []postgre.ChainTransfer
	Checkpoint postgre.ChainCheckpoint
}

func takeSnapshot(t *testing.T, db *gorm.DB, hash string) reorgSnapshot {
	t.Helper()
	s := reorgSnapshot{}
	for _, q := range []struct {
		name string
		dst  any
	}{{"chain_blocks", &s.Blocks}, {"chain_transactions", &s.Txs}, {"chain_raw_events", &s.Raw}, {"chain_transfers", &s.Native}} {
		query := db.Where("block_hash = ?", hash)
		if q.name == "chain_transfers" {
			query = query.Where("source_type = ?", "TRANSACTION_VALUE")
		}
		if err := query.Find(q.dst).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Where("chain_id = 1").First(&s.Checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCanonicalAndWorkerFailureRollback(t *testing.T) {
	t.Run("canonical", func(t *testing.T) {
		db, ctx := reorgFailureDB(t)
		if err := db.Exec(`CREATE FUNCTION test_fail_canonical() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test canonical failure'; END $$`).Error; err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Exec(`DROP FUNCTION test_fail_canonical()`).Error; err != nil {
				t.Errorf("drop function: %v", err)
			}
		}()
		if err := db.Exec(`CREATE TRIGGER test_fail_canonical AFTER INSERT ON chain_transactions FOR EACH ROW EXECUTE FUNCTION test_fail_canonical()`).Error; err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Exec(`DROP TRIGGER test_fail_canonical ON chain_transactions`).Error; err != nil {
				t.Errorf("drop trigger: %v", err)
			}
		}()
		if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, failureBatch(t, 'a', 1)); err == nil {
			t.Fatal("expected canonical failure")
		}
		for _, table := range []string{"chain_blocks", "chain_transactions", "chain_raw_events", "chain_transfers", "chain_checkpoints"} {
			var n int64
			if err := db.Raw("SELECT COUNT(*) FROM " + table).Scan(&n).Error; err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("%s rows=%d", table, n)
			}
		}
	})
	t.Run("worker", func(t *testing.T) {
		db, ctx := reorgFailureDB(t)
		batch := failureBatch(t, 'b', 1)
		if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if err := db.Exec(`CREATE FUNCTION test_fail_worker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test worker failure'; END $$`).Error; err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Exec(`DROP FUNCTION test_fail_worker()`).Error; err != nil {
				t.Errorf("drop function: %v", err)
			}
		}()
		if err := db.Exec(`CREATE TRIGGER test_fail_worker BEFORE INSERT ON chain_transfers FOR EACH ROW EXECUTE FUNCTION test_fail_worker()`).Error; err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Exec(`DROP TRIGGER test_fail_worker ON chain_transfers`).Error; err != nil {
				t.Errorf("drop trigger: %v", err)
			}
		}()
		var rawID int64
		if err := db.Raw("SELECT id FROM chain_raw_events LIMIT 1").Scan(&rawID).Error; err != nil {
			t.Fatal(err)
		}
		parser, err := transaction.NewParser()
		if err != nil {
			t.Fatal(err)
		}
		_, err = transaction.NewWorker(repository.NewEventRepository(postgre.NewEventDAO(db)), parser, 10, 0).ProcessBatch(ctx)
		if err == nil {
			t.Fatal("expected worker failure")
		}
		var cursor, tokenTransfers, nativeTransfers int64
		if err := db.Raw("SELECT COALESCE(MAX(cursor),0) FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&cursor).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG'", batch.Block.BlockHash).Scan(&tokenTransfers).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE source_type='TRANSACTION_VALUE'").Scan(&nativeTransfers).Error; err != nil {
			t.Fatal(err)
		}
		if cursor != 0 || tokenTransfers != 0 || nativeTransfers != 1 || rawID == 0 {
			t.Fatalf("cursor=%d tokenTransfers=%d nativeTransfers=%d raw=%d", cursor, tokenTransfers, nativeTransfers, rawID)
		}
	})
}

func TestSameHeightDifferentHashRollsBack(t *testing.T) {
	db, ctx := reorgFailureDB(t)
	first := failureBatch(t, 'c', 3)
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, first); err != nil {
		t.Fatal(err)
	}
	before := takeSnapshot(t, db, first.Block.BlockHash)
	second := failureBatch(t, 'd', 3)
	secondDomain := domain.CanonicalBatch{
		Block: domain.StoredBlock{
			ChainID: second.Block.ChainID, BlockNumber: second.Block.BlockNumber,
			BlockHash: second.Block.BlockHash, ParentHash: second.Block.ParentHash,
			BlockTime: second.Block.BlockTime, Status: second.Block.Status, Finality: second.Block.Finality,
		},
		Checkpoint: domain.Checkpoint{
			ChainID: second.Checkpoint.ChainID, LastProcessedHeight: second.Checkpoint.LastProcessedHeight,
			LastProcessedHash: second.Checkpoint.LastProcessedHash,
		},
	}
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).SaveCanonicalBatch(ctx, secondDomain); !errors.Is(err, repository.ErrCanonicalHashConflict) {
		t.Fatalf("err=%v", err)
	}
	var cp postgre.ChainCheckpoint
	if err := db.Where("chain_id=1").First(&cp).Error; err != nil {
		t.Fatal(err)
	}
	if cp.LastProcessedHash != first.Block.BlockHash || cp.LastProcessedHeight != 3 {
		t.Fatalf("checkpoint=%+v", cp)
	}
	for _, q := range []struct{ table, hash string }{{"chain_blocks", first.Block.BlockHash}, {"chain_transactions", first.Block.BlockHash}, {"chain_raw_events", first.Block.BlockHash}, {"chain_transfers", first.Block.BlockHash}, {"chain_blocks", second.Block.BlockHash}, {"chain_transactions", second.Block.BlockHash}, {"chain_raw_events", second.Block.BlockHash}} {
		var n int64
		if err := db.Raw("SELECT COUNT(*) FROM "+q.table+" WHERE block_hash=?", q.hash).Scan(&n).Error; err != nil {
			t.Fatal(err)
		}
		want := int64(1)
		if q.hash == second.Block.BlockHash {
			want = 0
		}
		if n != want {
			t.Fatalf("%s %s=%d", q.table, q.hash, n)
		}
	}
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", first.Block.BlockHash, first.Transactions[0].TxHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("transaction count=%d", n)
	}
	after := takeSnapshot(t, db, first.Block.BlockHash)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("A block/tx/raw/native/checkpoint snapshot changed")
	}
	var secondRows int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_blocks WHERE block_hash=?", second.Block.BlockHash).Scan(&secondRows).Error; err != nil {
		t.Fatal(err)
	}
	if secondRows != 0 {
		t.Fatalf("B block rows=%d", secondRows)
	}
}

func TestOldBatchReplayDoesNotMoveCheckpointBackward(t *testing.T) {
	db, ctx := reorgFailureDB(t)
	high := failureBatch(t, 'e', 5)
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, high); err != nil {
		t.Fatal(err)
	}
	before := takeSnapshot(t, db, high.Block.BlockHash)
	old := failureBatch(t, 'f', 4)
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, old); err != nil {
		t.Fatal(err)
	}
	var cp postgre.ChainCheckpoint
	if err := db.Where("chain_id=1").First(&cp).Error; err != nil {
		t.Fatal(err)
	}
	if cp.LastProcessedHeight != 5 || cp.LastProcessedHash != high.Block.BlockHash {
		t.Fatalf("checkpoint=%+v", cp)
	}
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", old.Block.BlockHash, old.Transactions[0].TxHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("old transaction count=%d", n)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_blocks WHERE block_number=5 AND block_hash=?", high.Block.BlockHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("high block count=%d", n)
	}
	after := takeSnapshot(t, db, high.Block.BlockHash)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("high block/tx/raw/native snapshot changed")
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", old.Block.BlockHash, old.Transactions[0].TxHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("replayed old transaction count=%d", n)
	}
}
