//go:build e2e

package integration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/worker/transaction"
	"github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
	"gorm.io/gorm"
)

func reactivationBatch(b postgre.CanonicalBatch) domain.CanonicalBatch {
	d := domain.CanonicalBatch{Block: domain.StoredBlock{ChainID: b.Block.ChainID, BlockNumber: b.Block.BlockNumber, BlockHash: b.Block.BlockHash, ParentHash: b.Block.ParentHash, BlockTime: b.Block.BlockTime, Status: b.Block.Status, Finality: b.Block.Finality, TransactionCount: b.Block.TransactionCount}, Checkpoint: domain.Checkpoint{ChainID: b.Checkpoint.ChainID, LastProcessedHeight: b.Checkpoint.LastProcessedHeight, LastProcessedHash: b.Checkpoint.LastProcessedHash}}
	for _, v := range b.Transactions {
		d.Transactions = append(d.Transactions, domain.StoredTransaction{ChainID: v.ChainID, BlockNumber: v.BlockNumber, BlockHash: v.BlockHash, TxHash: v.TxHash, TxIndex: v.TxIndex})
	}
	for _, v := range b.RawEvents {
		d.RawEvents = append(d.RawEvents, domain.RawEvent{ID: v.ID, ChainID: v.ChainID, BlockNumber: v.BlockNumber, BlockHash: v.BlockHash, TxHash: v.TxHash, TxIndex: v.TxIndex, LogIndex: v.LogIndex})
	}
	for _, v := range b.NativeTransfers {
		d.NativeTransfers = append(d.NativeTransfers, domain.Transfer{ID: v.ID, ChainID: v.ChainID, BlockNumber: v.BlockNumber, BlockHash: v.BlockHash, TxHash: v.TxHash, TxIndex: v.TxIndex, SourceType: v.SourceType, SourceIndex: v.SourceIndex})
	}
	return d
}

func runtimeBlockFixture(t *testing.T, f *testioc.RPCFixture, height int64, parentHash, blockHash, txHash string) map[string]any {
	t.Helper()
	events, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	from, to, token := addressOf('1'), addressOf('2'), addressOf('3')
	word := func(a string) string { return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(a, "0x") }
	log := map[string]any{"address": token, "topics": []string{events.ERC20Transfer.Topic0, word(from), word(to)}, "data": "0x" + strings.Repeat("0", 63) + "1", "blockNumber": fmt.Sprintf("0x%x", height), "blockHash": blockHash, "transactionHash": txHash, "transactionIndex": "0x0", "logIndex": "0x0"}
	tx := map[string]any{"hash": txHash, "from": from, "to": to, "transactionIndex": "0x0", "nonce": "0x0", "type": "0x2", "gas": "0x5208", "gasPrice": "0x1", "value": "0x1", "input": "0x"}
	block := map[string]any{"number": fmt.Sprintf("0x%x", height), "hash": blockHash, "parentHash": parentHash, "timestamp": "0x1", "gasUsed": "0x5208", "gasLimit": "0x5208", "transactions": []any{tx}}
	f.PutHeader(height, block)
	f.PutBlock(height, block)
	f.PutReceipt(txHash, map[string]any{"transactionHash": txHash, "blockNumber": fmt.Sprintf("0x%x", height), "blockHash": blockHash, "transactionIndex": "0x0", "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x1", "logs": []any{log}})
	return block
}

func TestWorkerConsumesAlreadyRevertedEventAsReverted(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	bh, th := hashOf('a'), hashOf('b')
	from, to, token := addressOf('1'), addressOf('2'), addressOf('3')
	events, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	topic1, topic2 := "0x"+strings.Repeat("0", 24)+strings.TrimPrefix(from, "0x"), "0x"+strings.Repeat("0", 24)+strings.TrimPrefix(to, "0x")
	amount := "0x" + strings.Repeat("0", 63) + "1"
	idx := 0
	raw := postgre.ChainRawEvent{ChainID: 1, BlockNumber: 0, BlockHash: bh, TxHash: th, TxIndex: &idx, LogIndex: 0, ContractAddress: token, Topic0: &events.ERC20Transfer.Topic0, Topic1: &topic1, Topic2: &topic2, EventData: &amount, RawLog: xsql.JSONField(`{"address":"` + token + `"}`), ChainStatus: "ACTIVE"}
	batch := postgre.CanonicalBatch{Block: postgre.ChainBlock{ChainID: 1, BlockNumber: 0, BlockHash: bh, ParentHash: hashOf('c'), BlockTime: 1, Status: "CANONICAL", Finality: "CONFIRMED", TransactionCount: 1}, Transactions: []postgre.ChainTransaction{{ChainID: 1, BlockNumber: 0, BlockHash: bh, TxHash: th, TxIndex: 0, FromAddress: from, ToAddress: &to, Nonce: 0, Value: "0", GasLimit: ptr("21000"), GasUsed: ptr("21000"), GasPrice: ptr("1"), EffectiveGasPrice: ptr("1"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 1}}, RawEvents: []postgre.ChainRawEvent{raw}, Checkpoint: postgre.ChainCheckpoint{ChainID: 1, LastProcessedHeight: 0, LastProcessedHash: bh}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	rid := "22222222-2222-2222-2222-222222222222"
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).ApplyReorg(ctx, domain.Reorg{ReorgID: rid, ChainID: 1, AncestorHeight: -1, AncestorHash: hashOf('c'), OldHeadHeight: 0, OldHeadHash: bh, NewHeadHeight: 0, NewHeadHash: hashOf('d'), ReorgDepth: 1, DetectedAt: 1}, domain.Checkpoint{ChainID: 1, LastProcessedHeight: -1, LastProcessedHash: hashOf('c')}); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewEventRepository(postgre.NewEventDAO(db))
	parser, err := transaction.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	processed, err := transaction.NewWorker(repo, parser, 100, time.Millisecond).ProcessBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed=%d", processed)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='EVENT_LOG' AND chain_status='REVERTED'", bh).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reverted token count=%d", count)
	}
	var gotRID string
	var gotRev int64
	var rawStatus, rawRID string
	var rawRev int64
	if err := db.Raw("SELECT chain_status,reorg_id::text,reverted_at FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", bh).Row().Scan(&rawStatus, &rawRID, &rawRev); err != nil {
		t.Fatal(err)
	}
	var transferBlock, transferTx, transferSource, transferToken, transferFrom, transferTo, transferAmount, transferStatus string
	if err := db.Raw("SELECT block_hash,tx_hash,source_index,token_address,from_address,to_address,amount::text,chain_status,reorg_id::text,reverted_at FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='EVENT_LOG'", bh).Row().Scan(&transferBlock, &transferTx, &transferSource, &transferToken, &transferFrom, &transferTo, &transferAmount, &transferStatus, &gotRID, &gotRev); err != nil {
		t.Fatal(err)
	}
	if rawStatus != "REVERTED" || rawRID != rid || rawRev == 0 || gotRID != rawRID || gotRev != rawRev || transferBlock != bh || transferTx != th || transferSource != "0" || transferToken != token || transferFrom != from || transferTo != to || transferAmount != "1" || transferStatus != rawStatus {
		t.Fatalf("already-reverted transfer identity/status block=%s tx=%s source=%s token=%s from=%s to=%s amount=%s status=%s reorg=%s reverted=%d", transferBlock, transferTx, transferSource, transferToken, transferFrom, transferTo, transferAmount, transferStatus, gotRID, gotRev)
	}
	var rawID, cursor int64
	if err := db.Raw("SELECT id FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", bh).Scan(&rawID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	if cursor != rawID {
		t.Fatalf("cursor=%d raw=%d", cursor, rawID)
	}
}

func TestCanonicalAtoBtoARestoresGeneratedToken(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(12)
	blockHash, txHash := hashOf('a'), hashOf('b')
	token := addressOf('3')
	block := runtimeBlockFixture(t, f, 0, hashOf('c'), blockHash, txHash)
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	p := runRuntime(t, f, cfg.Database.DSN)
	subscriberCtx, subscriberCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subscriberCancel()
	if err := f.WaitForSubscribers(subscriberCtx); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := notice.WaitForNotification(ctx); err != nil {
		t.Fatal(err)
	}
	p.Stop(t)
	var b postgre.ChainBlock
	var txs []postgre.ChainTransaction
	var raws []postgre.ChainRawEvent
	var native []postgre.ChainTransfer
	var cp postgre.ChainCheckpoint
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).First(&b).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).Find(&txs).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).Find(&raws).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "TRANSACTION_VALUE").Find(&native).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=?", 1).First(&cp).Error; err != nil {
		t.Fatal(err)
	}
	var tokenBefore postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "EVENT_LOG").First(&tokenBefore).Error; err != nil {
		t.Fatal(err)
	}
	if tokenBefore.ID == 0 || tokenBefore.Amount == nil || *tokenBefore.Amount != "1" || tokenBefore.TokenAddress == nil || *tokenBefore.TokenAddress != token || tokenBefore.FromAddress != addressOf('1') || tokenBefore.ToAddress != addressOf('2') {
		t.Fatalf("token before=%+v", tokenBefore)
	}
	beforeBlock, beforeTxs, beforeRaws, beforeNative, beforeToken := b, append([]postgre.ChainTransaction(nil), txs...), append([]postgre.ChainRawEvent(nil), raws...), append([]postgre.ChainTransfer(nil), native...), tokenBefore
	rid := "11111111-1111-1111-1111-111111111111"
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).ApplyReorg(ctx, domain.Reorg{ReorgID: rid, ChainID: 1, AncestorHeight: -1, AncestorHash: hashOf('c'), OldHeadHeight: 0, OldHeadHash: blockHash, NewHeadHeight: 0, NewHeadHash: hashOf('d'), ReorgDepth: 1, DetectedAt: 1}, domain.Checkpoint{ChainID: 1, LastProcessedHeight: -1, LastProcessedHash: hashOf('c')}); err != nil {
		t.Fatal(err)
	}
	var status, reorg string
	var reverted int64
	if err := db.Raw("SELECT status,reorg_id::text,orphaned_at FROM chain_blocks WHERE chain_id=1 AND block_hash=?", blockHash).Row().Scan(&status, &reorg, &reverted); err != nil {
		t.Fatal(err)
	}
	if status != "ORPHANED" || reorg != rid || reverted == 0 {
		t.Fatalf("block status=%s reorg=%s reverted=%d", status, reorg, reverted)
	}
	var rawStatus, rawReorg string
	var rawRev int64
	if err := db.Raw("SELECT chain_status,reorg_id::text,reverted_at FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", blockHash).Row().Scan(&rawStatus, &rawReorg, &rawRev); err != nil {
		t.Fatal(err)
	}
	if rawStatus != "REVERTED" || rawReorg != rid || rawRev == 0 {
		t.Fatalf("raw status=%s reorg=%s reverted=%d", rawStatus, rawReorg, rawRev)
	}
	var txStatus, txReorg string
	var txRev int64
	if err := db.Raw("SELECT chain_status,reorg_id::text,reverted_at FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Row().Scan(&txStatus, &txReorg, &txRev); err != nil {
		t.Fatal(err)
	}
	if txStatus != "REVERTED" || txReorg != rid || txRev == 0 {
		t.Fatalf("transaction status=%s reorg=%s reverted=%d", txStatus, txReorg, txRev)
	}
	var nativeStatus, nativeReorg string
	var nativeRev int64
	if err := db.Raw("SELECT chain_status,reorg_id::text,reverted_at FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='TRANSACTION_VALUE'", blockHash).Row().Scan(&nativeStatus, &nativeReorg, &nativeRev); err != nil {
		t.Fatal(err)
	}
	if nativeStatus != "REVERTED" || nativeReorg != rid || nativeRev == 0 {
		t.Fatalf("native status=%s reorg=%s reverted=%d", nativeStatus, nativeReorg, nativeRev)
	}
	var tokenReorg struct {
		ChainStatus, ReorgID string
		RevertedAt           int64
	}
	if err := db.Raw("SELECT chain_status,reorg_id::text,reverted_at FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='EVENT_LOG'", blockHash).Scan(&tokenReorg).Error; err != nil {
		t.Fatal(err)
	}
	if tokenReorg.ChainStatus != "REVERTED" || tokenReorg.ReorgID != rid || tokenReorg.RevertedAt == 0 {
		t.Fatalf("token status=%+v", tokenReorg)
	}
	bHash, bTxHash := hashOf('d'), hashOf('e')
	bBlock := b
	bBlock.BlockHash, bBlock.ParentHash, bBlock.Status = bHash, hashOf('c'), "CANONICAL"
	bTx := txs[0]
	bTx.BlockHash, bTx.TxHash, bTx.ChainStatus, bTx.ReorgID, bTx.RevertedAt = bHash, bTxHash, "ACTIVE", nil, nil
	bRaw := raws[0]
	bRaw.ID, bRaw.BlockHash, bRaw.TxHash, bRaw.ChainStatus, bRaw.ReorgID, bRaw.RevertedAt = 0, bHash, bTxHash, "ACTIVE", nil, nil
	bNative := native[0]
	bNative.ID, bNative.BlockHash, bNative.TxHash, bNative.ChainStatus, bNative.ReorgID, bNative.RevertedAt = 0, bHash, bTxHash, "ACTIVE", nil, nil
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, postgre.CanonicalBatch{Block: bBlock, Transactions: []postgre.ChainTransaction{bTx}, RawEvents: []postgre.ChainRawEvent{bRaw}, NativeTransfers: []postgre.ChainTransfer{bNative}, Checkpoint: postgre.ChainCheckpoint{ChainID: 1, LastProcessedHeight: 0, LastProcessedHash: bHash}}); err != nil {
		t.Fatal(err)
	}
	var checkpointAfterB postgre.ChainCheckpoint
	if err := db.Where("chain_id=?", 1).First(&checkpointAfterB).Error; err != nil || checkpointAfterB.LastProcessedHash != bHash {
		t.Fatalf("checkpoint after B=%+v err=%v", checkpointAfterB, err)
	}
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).ApplyReorg(ctx, domain.Reorg{ReorgID: "22222222-2222-2222-2222-222222222222", ChainID: 1, AncestorHeight: -1, AncestorHash: hashOf('c'), OldHeadHeight: 0, OldHeadHash: bHash, NewHeadHeight: 0, NewHeadHash: blockHash, ReorgDepth: 1, DetectedAt: 2}, domain.Checkpoint{ChainID: 1, LastProcessedHeight: -1, LastProcessedHash: hashOf('c')}); err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=?", 1).First(&cp).Error; err != nil || cp.LastProcessedHeight != -1 {
		t.Fatalf("checkpoint after B reorg=%+v err=%v", cp, err)
	}
	b.Status = "CANONICAL"
	b.ReorgID = nil
	b.OrphanedAt = nil
	if err := db.Where("chain_id=?", 1).First(&cp).Error; err != nil {
		t.Fatal(err)
	}
	cp.LastProcessedHeight, cp.LastProcessedHash = 0, blockHash
	for i := range raws {
		raws[i].ID = 0
		raws[i].ChainStatus = "ACTIVE"
		raws[i].ReorgID = nil
		raws[i].RevertedAt = nil
	}
	for i := range txs {
		txs[i].ChainStatus = "ACTIVE"
		txs[i].ReorgID = nil
		txs[i].RevertedAt = nil
	}
	for i := range native {
		native[i].ChainStatus = "ACTIVE"
		native[i].ReorgID = nil
		native[i].RevertedAt = nil
	}
	reactivate := postgre.CanonicalBatch{Block: b, Transactions: txs, RawEvents: raws, NativeTransfers: native, Checkpoint: cp}
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).ReactivateCanonicalBatch(ctx, reactivationBatch(reactivate), domain.BlockHeader{Number: b.BlockNumber, Hash: b.BlockHash, ParentHash: b.ParentHash}, rid); err != nil {
		t.Fatal(err)
	}
	var afterBlock postgre.ChainBlock
	var afterTx []postgre.ChainTransaction
	var afterRaw []postgre.ChainRawEvent
	var afterNative []postgre.ChainTransfer
	var tokenAfter postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).First(&afterBlock).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=? AND tx_hash=?", 1, blockHash, txHash).Find(&afterTx).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).Find(&afterRaw).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "TRANSACTION_VALUE").Find(&afterNative).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "EVENT_LOG").First(&tokenAfter).Error; err != nil {
		t.Fatal(err)
	}
	beforeBlock.Status, beforeBlock.ReorgID, beforeBlock.OrphanedAt = "CANONICAL", nil, nil
	for i := range beforeTxs {
		beforeTxs[i].ChainStatus, beforeTxs[i].ReorgID, beforeTxs[i].RevertedAt = "ACTIVE", nil, nil
	}
	for i := range beforeRaws {
		beforeRaws[i].ChainStatus, beforeRaws[i].ReorgID, beforeRaws[i].RevertedAt = "ACTIVE", nil, nil
	}
	for i := range beforeNative {
		beforeNative[i].ChainStatus, beforeNative[i].ReorgID, beforeNative[i].RevertedAt = "ACTIVE", nil, nil
	}
	beforeToken.ChainStatus, beforeToken.ReorgID, beforeToken.RevertedAt = "ACTIVE", nil, nil
	if !reflect.DeepEqual(beforeBlock, afterBlock) || !reflect.DeepEqual(beforeTxs, afterTx) || !reflect.DeepEqual(beforeRaws, afterRaw) || !reflect.DeepEqual(beforeNative, afterNative) || !reflect.DeepEqual(beforeToken, tokenAfter) {
		t.Fatalf("A replay did not restore complete snapshots")
	}
	var checkpointFinal postgre.ChainCheckpoint
	if err := db.Where("chain_id=?", 1).First(&checkpointFinal).Error; err != nil {
		t.Fatal(err)
	}
	if checkpointFinal.LastProcessedHeight != 0 || checkpointFinal.LastProcessedHash != blockHash {
		t.Fatalf("checkpoint=%+v", checkpointFinal)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("transaction count=%d", count)
	}
}
func runtimeChains(f *testioc.RPCFixture) map[string]config.ChainConfig {
	ids := map[string]int64{"ethereum": 1, "bsc": 56, "polygon": 137, "arbitrum": 42161, "optimism": 10, "base": 8453, "avalanche": 43114}
	confirms := map[string]int64{"ethereum": 12, "bsc": 15, "polygon": 100, "arbitrum": 20, "optimism": 20, "base": 20, "avalanche": 15}
	chains := make(map[string]config.ChainConfig, len(ids))
	for name, id := range ids {
		chains[name] = config.ChainConfig{Enabled: name == "ethereum", ChainID: id, StartBlock: 0, HTTPURL: f.HTTPURL(), WSURL: f.WSURL(), ConfirmBlocks: confirms[name]}
	}
	return chains
}

type runtimeBuffer struct {
	mu    sync.Mutex
	value strings.Builder
}

func (b *runtimeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.value.Write(p)
}
func (b *runtimeBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.value.String() }

type runtimeProcess struct {
	cmd            *exec.Cmd
	stdout, stderr *runtimeBuffer
	wait, readers  chan error
	waitDone       chan struct{}
	waitErr        error
	stopOnce       sync.Once
	stopOutput     string
	stopErr        error
}

func (p *runtimeProcess) drain(ctx context.Context, signal bool) (string, error) {
	var errs []error
	p.stopOnce.Do(func() {
		if signal {
			if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
				errs = append(errs, fmt.Errorf("signal service: %w", err))
			}
		}
		waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		select {
		case <-p.waitDone:
		case <-waitCtx.Done():
			if err := p.cmd.Process.Kill(); err != nil {
				errs = append(errs, fmt.Errorf("kill service: %w", err))
			}
			killCtx, cancelKill := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelKill()
			select {
			case <-p.waitDone:
			case <-killCtx.Done():
				errs = append(errs, errors.New("service wait did not join"))
			}
		}
		if p.waitErr != nil {
			errs = append(errs, fmt.Errorf("service wait: %w", p.waitErr))
		}
		readerCtx, cancelReader := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelReader()
		select {
		case err := <-p.readers:
			if err != nil {
				errs = append(errs, fmt.Errorf("service reader: %w", err))
			}
		case <-readerCtx.Done():
			errs = append(errs, errors.New("service reader did not join"))
		}
		p.stopOutput = p.stdout.String() + p.stderr.String()
		p.stopErr = errors.Join(errs...)
	})
	return p.stopOutput, p.stopErr
}
func (p *runtimeProcess) Stop(t *testing.T) string {
	t.Helper()
	output, err := p.drain(context.Background(), true)
	if err != nil {
		t.Error(err)
	}
	return output
}
func runRuntime(t *testing.T, f *testioc.RPCFixture, dsn string) *runtimeProcess {
	t.Helper()
	path := testioc.WriteServiceConfig(t, config.DatabaseConfig{DSN: dsn}, runtimeChains(f))
	binary := buildBinary(t, "./cmd/chain-indexer")
	cmd := exec.Command(binary, "--config", path)
	cmd.Dir = repositoryRoot(t)
	cmd.Env = commandEnvironment()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutBuffer, stderrBuffer := &runtimeBuffer{}, &runtimeBuffer{}
	cmd.Stderr = stderrBuffer
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &runtimeProcess{cmd: cmd, stdout: stdoutBuffer, stderr: stderrBuffer, readers: make(chan error, 1), waitDone: make(chan struct{})}
	t.Cleanup(func() { p.Stop(t) })
	ready := make(chan struct{})
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			_, _ = stdoutBuffer.Write([]byte(s.Text() + "\n"))
			if strings.Contains(s.Text(), "service ready") {
				select {
				case <-ready:
				default:
					close(ready)
				}
			}
		}
		p.readers <- s.Err()
	}()
	go func() { p.waitErr = cmd.Wait(); close(p.waitDone) }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	select {
	case <-ready:
	case <-p.waitDone:
		output, err := p.drain(context.Background(), false)
		t.Fatalf("service exited before ready: %s (%v)", output, err)
	case <-ctx.Done():
		output, err := p.drain(context.Background(), false)
		t.Fatalf("service did not become ready: %s (%v)", output, err)
	}
	cancel()
	return p
}

func TestChainIndexerRuntimeEndToEnd(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(12)
	blockHash := hashOf('a')
	txHash := hashOf('b')
	block := runtimeBlockFixture(t, f, 0, hashOf('c'), blockHash, txHash)
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	cursorNotice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer cursorNotice.Close(context.Background())
	process := runRuntime(t, f, cfg.Database.DSN)
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(waitCtx); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	if err := cursorNotice.WaitForNotification(waitCtx); err != nil {
		t.Fatal(err)
	}
	var checkpoint struct {
		Height int64  `gorm:"column:last_processed_height"`
		Hash   string `gorm:"column:last_processed_hash"`
	}
	if err := db.Raw("SELECT last_processed_height,last_processed_hash FROM chain_checkpoints WHERE chain_id=1").Scan(&checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.Height != 0 || checkpoint.Hash != blockHash {
		t.Fatalf("checkpoint=%+v", checkpoint)
	}
	var blockRow struct {
		Number int64  `gorm:"column:block_number"`
		Hash   string `gorm:"column:block_hash"`
		Parent string `gorm:"column:parent_hash"`
		Status string `gorm:"column:status"`
		Time   int64  `gorm:"column:block_time"`
	}
	if err := db.Raw("SELECT block_number,block_hash,parent_hash,status,block_time FROM chain_blocks WHERE chain_id=1 AND block_hash=?", blockHash).Scan(&blockRow).Error; err != nil {
		t.Fatal(err)
	}
	if blockRow.Number != 0 || blockRow.Hash != blockHash || blockRow.Parent != hashOf('c') || blockRow.Status != "CANONICAL" || blockRow.Time != 1 {
		t.Fatalf("block=%+v", blockRow)
	}
	var txRow struct {
		Hash   string `gorm:"column:tx_hash"`
		From   string `gorm:"column:from_address"`
		To     string `gorm:"column:to_address"`
		Status string `gorm:"column:chain_status"`
		Value  string
		Number int64 `gorm:"column:block_number"`
		Time   int64 `gorm:"column:block_time"`
	}
	if err := db.Raw("SELECT tx_hash,from_address,to_address,value::text,chain_status,block_number,block_time FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Scan(&txRow).Error; err != nil {
		t.Fatal(err)
	}
	if txRow.Hash != txHash || txRow.From != addressOf('1') || txRow.To != addressOf('2') || txRow.Value != "1" || txRow.Status != "ACTIVE" || txRow.Number != 0 || txRow.Time != 1 {
		t.Fatalf("transaction=%+v", txRow)
	}
	var rawRow struct {
		ID     int64
		Hash   string `gorm:"column:block_hash"`
		Tx     string `gorm:"column:tx_hash"`
		Log    int    `gorm:"column:log_index"`
		Status string `gorm:"column:chain_status"`
	}
	if err := db.Raw("SELECT id,block_hash,tx_hash,log_index,chain_status FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", blockHash).Scan(&rawRow).Error; err != nil {
		t.Fatal(err)
	}
	if rawRow.ID == 0 || rawRow.Hash != blockHash || rawRow.Tx != txHash || rawRow.Log != 0 || rawRow.Status != "ACTIVE" {
		t.Fatalf("raw=%+v", rawRow)
	}
	var transfers []struct {
		ChainID           int64
		BlockHash, TxHash string
		ID                int64
		Source            string `gorm:"column:source_type"`
		Index             string `gorm:"column:source_index"`
		Type              string `gorm:"column:transfer_type"`
		Token             string `gorm:"column:token_address"`
		From              string `gorm:"column:from_address"`
		To                string `gorm:"column:to_address"`
		Amount            string
		TokenID           string
		Status            string `gorm:"column:chain_status"`
		Batch             int
		Time              int64 `gorm:"column:block_time"`
	}
	if err := db.Raw("SELECT chain_id,block_hash,tx_hash,id,source_type,source_index,transfer_type,COALESCE(token_address,'') AS token_address,from_address,to_address,COALESCE(amount::text,'') AS amount,COALESCE(token_id::text,'') AS token_id,chain_status,batch_index,block_time FROM chain_transfers WHERE chain_id=1 AND block_hash=? ORDER BY source_type,source_index", blockHash).Scan(&transfers).Error; err != nil {
		t.Fatal(err)
	}
	if len(transfers) != 2 {
		t.Fatalf("transfers=%d", len(transfers))
	}
	for _, tr := range transfers {
		if tr.ChainID != 1 || tr.BlockHash != blockHash || tr.TxHash != txHash || tr.ID == 0 || tr.From != addressOf('1') || tr.To != addressOf('2') || tr.Amount != "1" || tr.Status != "ACTIVE" || tr.Batch != 0 || tr.Time != 1 {
			t.Fatalf("transfer=%+v", tr)
		}
		if tr.Source == "TRANSACTION_VALUE" {
			if tr.Index != "tx-value" || tr.Type != "NATIVE" || tr.Token != "" {
				t.Fatalf("native=%+v", tr)
			}
		} else if tr.Source == "EVENT_LOG" {
			if tr.Index != "0" || tr.Type != "ERC20" || tr.Token != addressOf('3') {
				t.Fatalf("erc20=%+v", tr)
			}
		} else {
			t.Fatalf("unexpected transfer=%+v", tr)
		}
	}
	var cursor int64
	if err := db.Raw("SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	if cursor != rawRow.ID {
		t.Fatalf("cursor=%d raw_id=%d", cursor, rawRow.ID)
	}
	var txCount, txTypeColumns int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Scan(&txCount).Error; err != nil {
		t.Fatal(err)
	}
	if txCount != 1 {
		t.Fatalf("transaction natural key count=%d", txCount)
	}
	if err := db.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='chain_transactions' AND column_name='tx_type'").Scan(&txTypeColumns).Error; err != nil {
		t.Fatal(err)
	}
	if txTypeColumns != 0 {
		t.Fatalf("tx_type columns=%d", txTypeColumns)
	}
	output := process.Stop(t)
	if !strings.Contains(output, "service ready") || !strings.Contains(output, "schema_version=3") || !strings.Contains(output, "enabled_chains=1") || !strings.Contains(output, "service stopping") || !strings.Contains(output, "service stopped") {
		t.Fatalf("lifecycle output=%q", output)
	}
	assertOutputOmitsSecrets(t, output, cfg.Database.DSN)
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM schema_migrations WHERE version = 3").Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("schema version count = %d", n)
	}
}

func TestCanonicalActiveReplayIsIdempotent(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(12)
	blockHash, txHash := hashOf('a'), hashOf('b')
	block := runtimeBlockFixture(t, f, 0, hashOf('c'), blockHash, txHash)
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	_, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	process := runRuntime(t, f, cfg.Database.DSN)
	cursorNotice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer cursorNotice.Close(context.Background())
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(waitCtx); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	if err := cursorNotice.WaitForNotification(waitCtx); err != nil {
		t.Fatal(err)
	}
	var beforeTransfer postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "EVENT_LOG").First(&beforeTransfer).Error; err != nil {
		t.Fatal(err)
	}
	var before struct{ RawID, Cursor int64 }
	if err := db.Raw("SELECT r.id AS raw_id, c.cursor FROM chain_raw_events r CROSS JOIN chain_event_biz_cursors c WHERE r.chain_id=1 AND r.block_hash=? AND c.biz='transaction' AND c.chain_family='evm'", blockHash).Scan(&before).Error; err != nil {
		t.Fatal(err)
	}
	if before.RawID == 0 || beforeTransfer.ID == 0 || beforeTransfer.ChainStatus != "ACTIVE" {
		t.Fatalf("before=%+v transfer=%+v", before, beforeTransfer)
	}
	var storedBlock postgre.ChainBlock
	var storedTx []postgre.ChainTransaction
	var storedRaw []postgre.ChainRawEvent
	var storedNative []postgre.ChainTransfer
	var storedCheckpoint postgre.ChainCheckpoint
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).First(&storedBlock).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=? AND tx_hash=?", 1, blockHash, txHash).Find(&storedTx).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=?", 1, blockHash).Find(&storedRaw).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "TRANSACTION_VALUE").Find(&storedNative).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("chain_id=?", 1).First(&storedCheckpoint).Error; err != nil {
		t.Fatal(err)
	}
	for i := range storedRaw {
		storedRaw[i].ID = 0
	}
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(waitCtx, postgre.CanonicalBatch{Block: storedBlock, Transactions: storedTx, RawEvents: storedRaw, NativeTransfers: storedNative, Checkpoint: storedCheckpoint}); err != nil {
		t.Fatal(err)
	}
	var afterTransfer postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "EVENT_LOG").First(&afterTransfer).Error; err != nil {
		t.Fatal(err)
	}
	var after struct{ RawID, Cursor int64 }
	if err := db.Raw("SELECT r.id AS raw_id, c.cursor FROM chain_raw_events r CROSS JOIN chain_event_biz_cursors c WHERE r.chain_id=1 AND r.block_hash=? AND c.biz='transaction' AND c.chain_family='evm'", blockHash).Scan(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeTransfer, afterTransfer) || after != before {
		t.Fatalf("replay changed identity/business fields: before=%+v after=%+v", beforeTransfer, afterTransfer)
	}
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("transaction natural key count=%d", n)
	}
	if output := process.Stop(t); !strings.Contains(output, "service stopped") || !strings.Contains(output, "service stopping") {
		t.Fatalf("lifecycle output=%q", output)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG'", blockHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("token natural key count=%d", n)
	}
	for name, query := range map[string]string{"block": "SELECT COUNT(*) FROM chain_blocks WHERE chain_id=1 AND block_hash=?", "transaction": "SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", "raw": "SELECT COUNT(*) FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", "native": "SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='TRANSACTION_VALUE'"} {
		var count int64
		args := []any{blockHash}
		if name == "transaction" {
			args = []any{blockHash, txHash}
		}
		if err := db.Raw(query, args...).Scan(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s natural key count=%d", name, count)
		}
	}
	if before.Cursor != after.Cursor {
		t.Fatalf("cursor changed from %d to %d", before.Cursor, after.Cursor)
	}
	if before.RawID != after.RawID {
		t.Fatalf("raw id changed")
	}
}

func TestWorkerFirstThenCanonicalRecoveryConverges(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	blockHash, txHash := hashOf('a'), hashOf('b')
	from, to, token := addressOf('1'), addressOf('2'), addressOf('3')
	events, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	topic1 := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(from, "0x")
	topic2 := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(to, "0x")
	amount := "0x" + strings.Repeat("0", 63) + "1"
	idx := 0
	raw := postgre.ChainRawEvent{ChainID: 1, BlockNumber: 0, BlockHash: blockHash, TxHash: txHash, TxIndex: &idx, LogIndex: 0, ContractAddress: token, Topic0: &events.ERC20Transfer.Topic0, Topic1: &topic1, Topic2: &topic2, EventData: &amount, RawLog: xsql.JSONField(`{"address":"` + token + `"}`), ChainStatus: "ACTIVE"}
	batch := postgre.CanonicalBatch{
		Block:        postgre.ChainBlock{ChainID: 1, BlockNumber: 0, BlockHash: blockHash, ParentHash: hashOf('c'), BlockTime: 1, Status: "CANONICAL", Finality: "CONFIRMED", TransactionCount: 1},
		Transactions: []postgre.ChainTransaction{{ChainID: 1, BlockNumber: 0, BlockHash: blockHash, TxHash: txHash, TxIndex: 0, FromAddress: from, ToAddress: &to, Nonce: 0, Value: "0", GasLimit: ptr("21000"), GasUsed: ptr("21000"), GasPrice: ptr("1"), EffectiveGasPrice: ptr("1"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 1}},
		RawEvents:    []postgre.ChainRawEvent{raw}, Checkpoint: postgre.ChainCheckpoint{ChainID: 1, LastProcessedHeight: 0, LastProcessedHash: blockHash},
	}
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.NewWorker(repository.NewEventRepository(postgre.NewEventDAO(db)), mustParser(t), 100, time.Millisecond).ProcessBatch(ctx); err != nil {
		t.Fatal(err)
	}
	var beforeRawID, beforeCursor int64
	var beforeTransfer postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "EVENT_LOG").First(&beforeTransfer).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT r.id,c.cursor FROM chain_raw_events r JOIN chain_event_biz_cursors c ON c.biz='transaction' AND c.chain_family='evm' WHERE r.block_hash=?", blockHash).Row().Scan(&beforeRawID, &beforeCursor); err != nil {
		t.Fatal(err)
	}
	rid := "33333333-3333-3333-3333-333333333333"
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).ApplyReorg(ctx, domain.Reorg{ReorgID: rid, ChainID: 1, AncestorHeight: -1, AncestorHash: hashOf('c'), OldHeadHeight: 0, OldHeadHash: blockHash, NewHeadHeight: 0, NewHeadHash: hashOf('d'), ReorgDepth: 1, DetectedAt: 1}, domain.Checkpoint{ChainID: 1, LastProcessedHeight: -1, LastProcessedHash: hashOf('c')}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	backend := make(chan int32, 1)
	opponentPID := make(chan int32, 1)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- testioc.WithRowLock(ctx, db, "SELECT id FROM chain_raw_events WHERE id=? FOR UPDATE", []any{beforeRawID}, entered, backend, release, func(tx *gorm.DB) error {
			parser, err := transaction.NewParser()
			if err != nil {
				return err
			}
			_, err = transaction.NewWorker(repository.NewEventRepository(postgre.NewEventDAO(tx)), parser, 100, time.Millisecond).ProcessBatch(ctx)
			return err
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
		}
		t.Fatal(ctx.Err())
	}
	recoveryDone := make(chan error, 1)
	var blockerPID int32
	select {
	case blockerPID = <-backend:
	case err := <-workerDone:
		close(release)
		t.Fatal(err)
	case <-ctx.Done():
		close(release)
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
		}
		t.Fatal(ctx.Err())
	}
	go func() {
		recoveryDone <- testioc.WithFixedSession(ctx, db, func(session *gorm.DB) error {
			var pid int32
			if err := session.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				return err
			}
			opponentPID <- pid
			return repository.NewChainRepository(postgre.NewChainDAO(session)).ReactivateCanonicalBatch(ctx, reactivationBatch(batch), domain.BlockHeader{Number: batch.Block.BlockNumber, Hash: batch.Block.BlockHash, ParentHash: batch.Block.ParentHash}, "33333333-3333-3333-3333-333333333333")
		})
	}()
	var waiterPID int32
	select {
	case waiterPID = <-opponentPID:
	case err := <-recoveryDone:
		close(release)
		joinDone(workerDone)
		t.Fatal(err)
	case <-ctx.Done():
		close(release)
		joinDone(workerDone)
		joinDone(recoveryDone)
		t.Fatal(ctx.Err())
	}
	observeErr := testioc.ObserveRowLockWait(ctx, db, blockerPID, waiterPID)
	close(release)
	workerErr, recoveryErr := <-workerDone, <-recoveryDone
	if observeErr != nil {
		t.Fatalf("expected designated PostgreSQL row-lock wait, blocker=%d waiter=%d err=%v", blockerPID, waiterPID, observeErr)
	}
	if workerErr != nil {
		t.Fatal(workerErr)
	}
	if recoveryErr != nil {
		t.Fatal(recoveryErr)
	}
	var afterRawID, afterCursor int64
	if err := db.Raw("SELECT r.id,c.cursor FROM chain_raw_events r JOIN chain_event_biz_cursors c ON c.biz='transaction' AND c.chain_family='evm' WHERE r.block_hash=?", blockHash).Row().Scan(&afterRawID, &afterCursor); err != nil {
		t.Fatal(err)
	}
	if afterRawID == 0 || beforeCursor != beforeRawID || afterRawID != beforeRawID || afterCursor != afterRawID {
		t.Fatalf("recovery changed raw/cursor identity: before=%d/%d after=%d/%d", beforeRawID, beforeCursor, afterRawID, afterCursor)
	}
	var afterTransfer postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=? AND source_type=?", 1, blockHash, "EVENT_LOG").First(&afterTransfer).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeTransfer, afterTransfer) {
		t.Fatalf("token recovery changed full identity: before=%+v after=%+v", beforeTransfer, afterTransfer)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("transaction natural key count=%d", count)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_raw_events WHERE chain_id=1 AND block_hash=? AND chain_status='ACTIVE'", blockHash).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("active raw event count=%d", count)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='EVENT_LOG' AND chain_status='ACTIVE'", blockHash).Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("active token count=%d", count)
	}
}

func TestCanonicalRecoveryFirstThenWorkerConverges(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	blockHash, txHash := hashOf('e'), hashOf('f')
	from, to, token := addressOf('1'), addressOf('2'), addressOf('3')
	events, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	topic1 := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(from, "0x")
	topic2 := "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(to, "0x")
	amount := "0x" + strings.Repeat("0", 63) + "1"
	idx := 0
	raw := postgre.ChainRawEvent{ChainID: 1, BlockNumber: 0, BlockHash: blockHash, TxHash: txHash, TxIndex: &idx, LogIndex: 0, ContractAddress: token, Topic0: &events.ERC20Transfer.Topic0, Topic1: &topic1, Topic2: &topic2, EventData: &amount, RawLog: xsql.JSONField(`{"address":"` + token + `"}`), ChainStatus: "ACTIVE"}
	batch := postgre.CanonicalBatch{Block: postgre.ChainBlock{ChainID: 1, BlockNumber: 0, BlockHash: blockHash, ParentHash: hashOf('d'), BlockTime: 1, Status: "CANONICAL", Finality: "CONFIRMED", TransactionCount: 1}, Transactions: []postgre.ChainTransaction{{ChainID: 1, BlockNumber: 0, BlockHash: blockHash, TxHash: txHash, TxIndex: 0, FromAddress: from, ToAddress: &to, GasLimit: ptr("21000"), GasUsed: ptr("21000"), GasPrice: ptr("1"), EffectiveGasPrice: ptr("1"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 1}}, RawEvents: []postgre.ChainRawEvent{raw}, Checkpoint: postgre.ChainCheckpoint{ChainID: 1, LastProcessedHeight: 0, LastProcessedHash: blockHash}}
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := repository.NewChainRepository(postgre.NewChainDAO(db)).ApplyReorg(ctx, domain.Reorg{ReorgID: "44444444-4444-4444-4444-444444444444", ChainID: 1, AncestorHeight: -1, AncestorHash: hashOf('d'), OldHeadHeight: 0, OldHeadHash: blockHash, NewHeadHeight: 0, NewHeadHash: hashOf('g'), ReorgDepth: 1, DetectedAt: 1}, domain.Checkpoint{ChainID: 1, LastProcessedHeight: -1, LastProcessedHash: hashOf('d')}); err != nil {
		t.Fatal(err)
	}
	var rawID int64
	if err := db.Raw("SELECT id FROM chain_raw_events WHERE block_hash=?", blockHash).Scan(&rawID).Error; err != nil {
		t.Fatal(err)
	}
	var before struct {
		RawID, BlockNumber, LogIndex                                      int64
		TxIndex                                                           *int
		BlockHash, TxHash, Contract, Topic0, Topic1, Topic2, Data, Status string
		ReorgID, RevertedAt                                               *string
	}
	if err := db.Raw("SELECT id,block_number,log_index,tx_index,block_hash,tx_hash,contract_address,topic0,topic1,topic2,event_data,chain_status,reorg_id::text,reverted_at::text FROM chain_raw_events WHERE id=?", rawID).Row().Scan(&before.RawID, &before.BlockNumber, &before.LogIndex, &before.TxIndex, &before.BlockHash, &before.TxHash, &before.Contract, &before.Topic0, &before.Topic1, &before.Topic2, &before.Data, &before.Status, &before.ReorgID, &before.RevertedAt); err != nil {
		t.Fatal(err)
	}
	if before.Status != "REVERTED" || before.ReorgID == nil || before.RevertedAt == nil {
		t.Fatalf("raw before=%+v", before)
	}
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND source_type='EVENT_LOG'", blockHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pre-worker token count=%d", n)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pre-worker cursor rows=%d", n)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?", blockHash, txHash).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("transaction natural key count=%d", n)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	backend := make(chan int32, 1)
	recoveryDone := make(chan error, 1)
	go func() {
		recoveryDone <- testioc.WithRowLock(ctx, db, "SELECT id FROM chain_raw_events WHERE id=? FOR UPDATE", []any{rawID}, entered, backend, release, func(tx *gorm.DB) error {
			b := batch
			b.RawEvents = []postgre.ChainRawEvent{raw}
			b.RawEvents[0].ID = rawID
			b.RawEvents[0].ChainStatus = "ACTIVE"
			return repository.NewChainRepository(postgre.NewChainDAO(tx)).ReactivateCanonicalBatch(ctx, reactivationBatch(b), domain.BlockHeader{Number: b.Block.BlockNumber, Hash: b.Block.BlockHash, ParentHash: b.Block.ParentHash}, "44444444-4444-4444-4444-444444444444")
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		select {
		case <-recoveryDone:
		case <-time.After(5 * time.Second):
		}
		t.Fatal(ctx.Err())
	}
	var blockerPID int32
	select {
	case blockerPID = <-backend:
	case err := <-recoveryDone:
		close(release)
		t.Fatal(err)
	case <-ctx.Done():
		close(release)
		select {
		case <-recoveryDone:
		case <-time.After(5 * time.Second):
		}
		t.Fatal(ctx.Err())
	}
	opponentPID := make(chan int32, 1)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- testioc.WithFixedSession(ctx, db, func(session *gorm.DB) error {
			var pid int32
			if err := session.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				return err
			}
			opponentPID <- pid
			parser, err := transaction.NewParser()
			if err != nil {
				return err
			}
			_, err = transaction.NewWorker(repository.NewEventRepository(postgre.NewEventDAO(session)), parser, 100, time.Millisecond).ProcessBatch(ctx)
			return err
		})
	}()
	var waiterPID int32
	select {
	case waiterPID = <-opponentPID:
	case err := <-workerDone:
		close(release)
		joinDone(recoveryDone)
		t.Fatal(err)
	case <-ctx.Done():
		close(release)
		joinDone(recoveryDone)
		joinDone(workerDone)
		t.Fatal(ctx.Err())
	}
	observeErr := testioc.ObserveRowLockWait(ctx, db, blockerPID, waiterPID)
	close(release)
	recoveryErr, workerErr := <-recoveryDone, <-workerDone
	if observeErr != nil {
		t.Fatalf("expected designated PostgreSQL row-lock wait, blocker=%d waiter=%d err=%v", blockerPID, waiterPID, observeErr)
	}
	if recoveryErr != nil {
		t.Fatal(recoveryErr)
	}
	if workerErr != nil {
		t.Fatal(workerErr)
	}
	afterRaw := before
	if err := db.Raw("SELECT id,block_number,log_index,tx_index,block_hash,tx_hash,contract_address,topic0,topic1,topic2,event_data,chain_status,reorg_id::text,reverted_at::text FROM chain_raw_events WHERE id=?", rawID).Row().Scan(&afterRaw.RawID, &afterRaw.BlockNumber, &afterRaw.LogIndex, &afterRaw.TxIndex, &afterRaw.BlockHash, &afterRaw.TxHash, &afterRaw.Contract, &afterRaw.Topic0, &afterRaw.Topic1, &afterRaw.Topic2, &afterRaw.Data, &afterRaw.Status, &afterRaw.ReorgID, &afterRaw.RevertedAt); err != nil {
		t.Fatal(err)
	}
	if afterRaw.RawID != before.RawID || !reflect.DeepEqual(afterRaw.TxIndex, before.TxIndex) || afterRaw.BlockNumber != before.BlockNumber || afterRaw.BlockHash != before.BlockHash || afterRaw.TxHash != before.TxHash || afterRaw.LogIndex != before.LogIndex || afterRaw.Contract != before.Contract || afterRaw.Topic0 != before.Topic0 || afterRaw.Topic1 != before.Topic1 || afterRaw.Topic2 != before.Topic2 || afterRaw.Data != before.Data || afterRaw.Status != "ACTIVE" || afterRaw.ReorgID != nil || afterRaw.RevertedAt != nil {
		t.Fatalf("recovery changed raw: before=%+v after=%+v", before, afterRaw)
	}
	var tokenAfter struct {
		ID, BlockNumber, BatchIndex                                                                                         int64
		ChainID                                                                                                             int64
		BlockHash, TxHash, SourceIndex, SourceType, TransferType, TokenAddress, FromAddress, ToAddress, Amount, ChainStatus string
		ReorgID, RevertedAt                                                                                                 *string
	}
	if err := db.Raw("SELECT id,chain_id,block_number,block_hash,tx_hash,source_index,source_type,transfer_type,token_address,from_address,to_address,amount::text,batch_index,chain_status,reorg_id::text,reverted_at::text FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG'", blockHash).Row().Scan(&tokenAfter.ID, &tokenAfter.ChainID, &tokenAfter.BlockNumber, &tokenAfter.BlockHash, &tokenAfter.TxHash, &tokenAfter.SourceIndex, &tokenAfter.SourceType, &tokenAfter.TransferType, &tokenAfter.TokenAddress, &tokenAfter.FromAddress, &tokenAfter.ToAddress, &tokenAfter.Amount, &tokenAfter.BatchIndex, &tokenAfter.ChainStatus, &tokenAfter.ReorgID, &tokenAfter.RevertedAt); err != nil {
		t.Fatal(err)
	}
	if tokenAfter.ID == 0 || tokenAfter.ChainID != 1 || tokenAfter.BlockNumber != 0 || tokenAfter.BlockHash != blockHash || tokenAfter.TxHash != txHash || tokenAfter.SourceIndex != "0" || tokenAfter.SourceType != "EVENT_LOG" || tokenAfter.TransferType != "ERC20" || tokenAfter.TokenAddress != token || tokenAfter.FromAddress != from || tokenAfter.ToAddress != to || tokenAfter.Amount != "1" || tokenAfter.BatchIndex != 0 || tokenAfter.ChainStatus != "ACTIVE" || tokenAfter.ReorgID != nil || tokenAfter.RevertedAt != nil {
		t.Fatalf("token after=%+v", tokenAfter)
	}
	var cursor int64
	if err := db.Raw("SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	if cursor != rawID {
		t.Fatalf("cursor=%d raw=%d", cursor, rawID)
	}
	var tokenCount, txCount int64
	if err := db.Raw("SELECT (SELECT COUNT(*) FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG' AND chain_status='ACTIVE'), (SELECT COUNT(*) FROM chain_transactions WHERE chain_id=1 AND block_hash=? AND tx_hash=?)", blockHash, blockHash, txHash).Row().Scan(&tokenCount, &txCount); err != nil {
		t.Fatal(err)
	}
	if tokenCount != 1 || txCount != 1 {
		t.Fatalf("active token count=%d transaction count=%d", tokenCount, txCount)
	}
}

func mustParser(t *testing.T) *transaction.Parser {
	t.Helper()
	p, err := transaction.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func ptr(s string) *string { return &s }
func joinDone(ch <-chan error) {
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
	}
}

func TestWorkerBatchAndUnknownEvent(t *testing.T) {
	cfg := testioc.InitConfig(t)
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	events, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	from, to, token := addressOf('1'), addressOf('2'), addressOf('3')
	word := func(a string) string { return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(a, "0x") }
	batchData := "0x" + strings.Repeat("0", 62) + "40" + strings.Repeat("0", 62) + "a0" + strings.Repeat("0", 63) + "2" + strings.Repeat("0", 63) + "7" + strings.Repeat("0", 63) + "7" + strings.Repeat("0", 63) + "2" + strings.Repeat("0", 63) + "9" + strings.Repeat("0", 63) + "b"
	mk := func(i int, topic, data string) postgre.ChainRawEvent {
		ix := i
		h := hashOf(byte('a' + i))
		th := hashOf(byte('k' + i))
		operator := word(addressOf('4'))
		topic1, topic2 := word(from), word(to)
		var topic3 *string
		if topic == events.ERC1155TransferBatch.Topic0 {
			topic1, topic2, topic3 = operator, word(from), func() *string { s := word(to); return &s }()
		}
		return postgre.ChainRawEvent{ChainID: 1, BlockNumber: 1, BlockHash: h, TxHash: th, TxIndex: &ix, LogIndex: i, ContractAddress: token, Topic0: &topic, Topic1: &topic1, Topic2: &topic2, Topic3: topic3, EventData: &data, RawLog: xsql.JSONField(`{"i":1}`), ChainStatus: "ACTIVE"}
	}
	raws := []postgre.ChainRawEvent{mk(0, events.ERC1155TransferBatch.Topic0, batchData), mk(1, "0x"+strings.Repeat("f", 64), "0x"), mk(2, events.ERC20Transfer.Topic0, "0x"+strings.Repeat("0", 63)+"3")}
	for i := range raws {
		raws[i].BlockHash = hashOf('z')
		raws[i].TxHash = hashOf(byte('k' + i))
		raws[i].TxIndex = func() *int { v := i; return &v }()
	}
	transactions := make([]postgre.ChainTransaction, 3)
	for i := range transactions {
		ix := i
		transactions[i] = postgre.ChainTransaction{ChainID: 1, BlockNumber: 1, BlockHash: hashOf('z'), TxHash: hashOf(byte('k' + i)), TxIndex: ix, FromAddress: from, ToAddress: &to, GasLimit: ptr("21000"), GasUsed: ptr("21000"), GasPrice: ptr("1"), EffectiveGasPrice: ptr("1"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 99}
	}
	for i := range raws {
		raws[i].BlockNumber = 1
		raws[i].BlockHash = hashOf('z')
	}
	batch := postgre.CanonicalBatch{Block: postgre.ChainBlock{ChainID: 1, BlockNumber: 1, BlockHash: hashOf('z'), ParentHash: hashOf('y'), BlockTime: 99, Status: "CANONICAL", Finality: "CONFIRMED", TransactionCount: 3}, Transactions: transactions, RawEvents: raws, Checkpoint: postgre.ChainCheckpoint{ChainID: 1, LastProcessedHeight: 1, LastProcessedHash: hashOf('z')}}
	if err := postgre.NewChainDAO(db).SaveCanonicalBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if n, err := transaction.NewWorker(repository.NewEventRepository(postgre.NewEventDAO(db)), mustParser(t), 10, time.Millisecond).ProcessBatch(ctx); err != nil || n != 3 {
		t.Fatalf("processed=%d err=%v", n, err)
	}
	var rows []postgre.ChainTransfer
	if err := db.Raw("SELECT * FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG' ORDER BY source_index,batch_index", hashOf('z')).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("transfers=%d", len(rows))
	}
	var unknown, erc20 int64
	if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG' AND source_index='1'", hashOf('z')).Scan(&unknown).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE block_hash=? AND source_type='EVENT_LOG' AND source_index='2'", hashOf('z')).Scan(&erc20).Error; err != nil {
		t.Fatal(err)
	}
	if unknown != 0 || erc20 != 1 {
		t.Fatalf("unknown=%d erc20=%d", unknown, erc20)
	}
	if rows[0].ChainID != 1 || rows[0].BlockHash != hashOf('z') || rows[0].TxHash != hashOf('k') || rows[0].FromAddress != from || rows[0].ToAddress != to || rows[0].TransferType != "ERC1155" || rows[0].SourceType != "EVENT_LOG" || rows[0].SourceIndex != "0" || rows[0].TokenAddress == nil || *rows[0].TokenAddress != token || rows[0].TokenID == nil || *rows[0].TokenID != "7" || rows[0].Amount == nil || *rows[0].Amount != "9" || rows[0].BatchIndex != 0 || rows[1].ChainID != 1 || rows[1].BlockHash != hashOf('z') || rows[1].TxHash != hashOf('k') || rows[1].FromAddress != from || rows[1].ToAddress != to || rows[1].TransferType != "ERC1155" || rows[1].SourceType != "EVENT_LOG" || rows[1].SourceIndex != "0" || rows[1].TokenAddress == nil || *rows[1].TokenAddress != token || rows[1].BatchIndex != 1 || rows[1].TokenID == nil || *rows[1].TokenID != "7" || rows[1].Amount == nil || *rows[1].Amount != "11" || rows[2].ChainID != 1 || rows[2].BlockHash != hashOf('z') || rows[2].TxHash != hashOf('k'+byte(2)) || rows[2].FromAddress != from || rows[2].ToAddress != to || rows[2].TransferType != "ERC20" || rows[2].SourceType != "EVENT_LOG" || rows[2].SourceIndex != "2" || rows[2].TokenAddress == nil || *rows[2].TokenAddress != token || rows[2].Amount == nil || *rows[2].Amount != "3" {
		t.Fatalf("rows=%+v", rows)
	}
	var cursor int64
	if err := db.Raw("SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	var last int64
	if err := db.Raw("SELECT id FROM chain_raw_events ORDER BY id DESC LIMIT 1").Scan(&last).Error; err != nil {
		t.Fatal(err)
	}
	if cursor != last {
		t.Fatalf("cursor=%d last=%d", cursor, last)
	}
}
