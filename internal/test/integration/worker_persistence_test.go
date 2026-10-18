//go:build e2e

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
	"gorm.io/gorm"
)

// These helpers only encode wire words/objects or inspect committed rows. Each
// LE test below owns its inputs, database, process and timeline independently.
// The fixed DSN belongs solely to scripts/test_docker_compose.yml. Local tests
// never load the ignored live configuration or its RPC endpoints.
const workerComposeDSN = "host=127.0.0.1 port=15432 user=postgres password=postgres dbname=chain_indexer_e2e sslmode=disable"

func workerPtr[T any](v T) *T   { return &v }
func workerWord(n int64) string { return fmt.Sprintf("%064x", n) }
func workerAddressWord(a string) string {
	return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(a, "0x")
}
func workerLog(height int64, bh, th string, index int, contract string, topics []string, data string) map[string]any {
	return map[string]any{"address": contract, "topics": topics, "data": data, "blockNumber": fmt.Sprintf("0x%x", height), "blockHash": bh, "transactionHash": th, "transactionIndex": "0x0", "logIndex": fmt.Sprintf("0x%x", index), "removed": false}
}
func workerTx(th, from string, to any, index int, typ, value, input string) map[string]any {
	return map[string]any{"hash": th, "from": from, "to": to, "transactionIndex": fmt.Sprintf("0x%x", index), "nonce": fmt.Sprintf("0x%x", 7+index), "type": typ, "gas": "0x7530", "gasPrice": "0x2", "value": value, "input": input}
}
func workerBlock(height int64, bh, parent string, txs []any) map[string]any {
	return map[string]any{"number": fmt.Sprintf("0x%x", height), "hash": bh, "parentHash": parent, "timestamp": "0x99", "gasUsed": fmt.Sprintf("0x%x", 21000*len(txs)), "gasLimit": "0x989680", "transactions": txs}
}
func workerReceipt(height int64, bh, th, status string, index int, logs []any) map[string]any {
	return map[string]any{"transactionHash": th, "blockNumber": fmt.Sprintf("0x%x", height), "blockHash": bh, "transactionIndex": fmt.Sprintf("0x%x", index), "status": status, "gasUsed": "0x5208", "effectiveGasPrice": "0x3", "contractAddress": nil, "logs": logs}
}

// runWorkerRuntime is process plumbing, not a scenario runner: it never creates
// RPC input, migrates PG, publishes a head, waits for a cursor or asserts data.
func runWorkerRuntime(t *testing.T, f *testioc.RPCFixture, dsn string) *runtimeProcess {
	t.Helper()
	chains := runtimeChains(f)
	e := chains["ethereum"]
	e.StartBlock = 32
	chains["ethereum"] = e
	path := testioc.WriteServiceConfig(t, config.DatabaseConfig{DSN: dsn}, chains)
	binary := buildBinary(t, "./cmd/chain-indexer")
	cmd := exec.Command(binary, "--config", path)
	cmd.Dir, cmd.Env = repositoryRoot(t), commandEnvironment()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, stderr := &runtimeBuffer{}, &runtimeBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &runtimeProcess{cmd: cmd, stdout: out, stderr: stderr, readers: make(chan error, 1), waitDone: make(chan struct{})}
	// drain(false) is also safe after an expected component failure and always
	// joins the child/readers; normal paths explicitly Stop after cursor commit.
	t.Cleanup(func() { _, _ = p.drain(context.Background(), true) })
	ready := make(chan struct{})
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			_, _ = out.Write([]byte(s.Text() + "\n"))
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
	defer cancel()
	select {
	case <-ready:
	case <-p.waitDone:
		_, _ = p.drain(context.Background(), false)
		t.Fatal("worker service exited before ready")
	case <-ctx.Done():
		_, _ = p.drain(context.Background(), true)
		t.Fatal("worker service readiness deadline")
	}
	return p
}

func workerPGTime(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(`SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::bigint`).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

// A notification only wakes the observer. The barrier succeeds exclusively on
// the exact committed raw IDs and cursor read from PG, even across 100-row pages.
func workerCursorCommitted(t *testing.T, db *gorm.DB, notice *testioc.NotifyBarrier, ctx context.Context, rawCount int) int64 {
	t.Helper()
	for {
		var ids []int64
		if err := db.Raw(`SELECT id FROM chain_raw_events ORDER BY id`).Scan(&ids).Error; err != nil {
			t.Fatal(err)
		}
		var cursor int64
		if err := db.Raw(`SELECT COALESCE(MAX(cursor),0) FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'`).Scan(&cursor).Error; err != nil {
			t.Fatal(err)
		}
		if len(ids) == rawCount && rawCount > 0 && cursor == ids[len(ids)-1] {
			return cursor
		}
		if len(ids) > rawCount {
			t.Fatalf("raw identities=%v, want %d", ids, rawCount)
		}
		if err := notice.WaitForNotification(ctx); err != nil {
			t.Fatalf("cursor commit: raw=%v cursor=%d: %v", ids, cursor, err)
		}
	}
}

func workerJSONEqual(t *testing.T, got []byte, want any) {
	t.Helper()
	w, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(b []byte) any {
		d := json.NewDecoder(strings.NewReader(string(b)))
		d.UseNumber()
		var v any
		if err := d.Decode(&v); err != nil {
			t.Fatalf("invalid persisted JSON: %v", err)
		}
		return v
	}
	if !reflect.DeepEqual(decode(got), decode(w)) {
		t.Fatalf("JSONB mismatch\ngot=%s\nwant=%s", got, w)
	}
}

func workerAssertTransaction(t *testing.T, db *gorm.DB, want postgre.ChainTransaction, tx, receipt any, before, after int64, blockTransactionCount ...int) {
	t.Helper()
	expectedCount := 1
	if len(blockTransactionCount) == 1 {
		expectedCount = blockTransactionCount[0]
	}
	var keys []string
	if err := db.Raw(`SELECT tx_hash FROM chain_transactions WHERE chain_id=? AND block_hash=? ORDER BY tx_index`, want.ChainID, want.BlockHash).Scan(&keys).Error; err != nil {
		t.Fatal(err)
	}
	if len(keys) != expectedCount {
		t.Fatalf("block transaction identities=%v want %d", keys, expectedCount)
	}
	var rows []postgre.ChainTransaction
	if err := db.Where("chain_id=? AND block_hash=? AND tx_hash=?", want.ChainID, want.BlockHash, want.TxHash).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("transaction natural key %s: %+v", want.TxHash, rows)
	}
	got := rows[0]
	if got.CreatedAt <= 0 || got.CreatedAt < before || got.CreatedAt > after {
		t.Fatalf("transaction created_at=%d outside PG [%d,%d]", got.CreatedAt, before, after)
	}
	if got.RawTransaction == nil || got.RawReceipt == nil {
		t.Fatal("transaction JSONB unexpectedly NULL")
	}
	workerJSONEqual(t, []byte(*got.RawTransaction), tx)
	workerJSONEqual(t, []byte(*got.RawReceipt), receipt)
	got.CreatedAt, got.RawTransaction, got.RawReceipt = 0, nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transaction all V3 columns\ngot=%+v\nwant=%+v", got, want)
	}
}

func workerAssertTransfer(t *testing.T, db *gorm.DB, want postgre.ChainTransfer, raw any, before, after int64) int64 {
	t.Helper()
	var rows []postgre.ChainTransfer
	if err := db.Where("chain_id=? AND block_hash=? AND tx_hash=? AND source_type=? AND source_index=? AND batch_index=?", want.ChainID, want.BlockHash, want.TxHash, want.SourceType, want.SourceIndex, want.BatchIndex).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("transfer natural key %s/%s/%d: %+v", want.TxHash, want.SourceIndex, want.BatchIndex, rows)
	}
	got := rows[0]
	if got.ID <= 0 || got.CreatedAt <= 0 || got.CreatedAt < before || got.CreatedAt > after {
		t.Fatalf("transfer ID/time=%d/%d outside PG [%d,%d]", got.ID, got.CreatedAt, before, after)
	}
	if raw == nil {
		if got.RawData != nil {
			t.Fatal("Native raw_data must be NULL")
		}
	} else {
		if got.RawData == nil {
			t.Fatal("event raw_data is NULL")
		}
		workerJSONEqual(t, []byte(*got.RawData), raw)
	}
	id := got.ID
	got.ID, got.CreatedAt, got.RawData = 0, 0, nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transfer all V3 columns\ngot=%+v\nwant=%+v", got, want)
	}
	return id
}

func workerAssertRaw(t *testing.T, db *gorm.DB, log map[string]any, before, after int64) int64 {
	t.Helper()
	var rows []postgre.ChainRawEvent
	if err := db.Raw(`SELECT re.*,b.block_time FROM chain_raw_events re JOIN chain_blocks b ON b.chain_id=re.chain_id AND b.block_hash=re.block_hash WHERE re.chain_id=1 AND re.block_hash=? AND re.tx_hash=? AND re.log_index=?`, log["blockHash"], log["transactionHash"], hexInt(log["logIndex"].(string))).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("raw event natural key: %+v", rows)
	}
	g := rows[0]
	topics := log["topics"].([]string)
	w := postgre.ChainRawEvent{ChainID: 1, BlockNumber: hexInt(log["blockNumber"].(string)), BlockTime: 153, BlockHash: log["blockHash"].(string), TxHash: log["transactionHash"].(string), TxIndex: workerPtr(int(hexInt(log["transactionIndex"].(string)))), LogIndex: int(hexInt(log["logIndex"].(string))), ContractAddress: log["address"].(string), EventData: workerPtr(log["data"].(string)), ParseStatus: "PENDING", ChainStatus: "ACTIVE"}
	for i, p := range []**string{&w.Topic0, &w.Topic1, &w.Topic2, &w.Topic3} {
		if i < len(topics) {
			*p = workerPtr(topics[i])
		}
	}
	if g.ID <= 0 || g.CreatedAt <= 0 || g.CreatedAt < before || g.CreatedAt > after {
		t.Fatalf("raw ID/time=%d/%d outside PG [%d,%d]", g.ID, g.CreatedAt, before, after)
	}
	workerJSONEqual(t, []byte(g.RawLog), log)
	id := g.ID
	g.ID, g.CreatedAt, g.RawLog = 0, 0, nil
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("raw fields\ngot=%+v\nwant=%+v", g, w)
	}
	return id
}
func hexInt(s string) int64 {
	n, err := strconv.ParseInt(strings.TrimPrefix(s, "0x"), 16, 64)
	if err != nil {
		panic(err)
	}
	return n
}

func workerAssertV3(t *testing.T, db *gorm.DB) {
	t.Helper()
	var columns []struct{ TableName, ColumnName string }
	if err := db.Raw(`SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='chain_transactions' AND column_name IN ('tx_type','status')) OR (table_name='chain_transfers' AND column_name='raw_event_id'))`).Scan(&columns).Error; err != nil {
		t.Fatal(err)
	}
	if len(columns) != 0 {
		t.Fatalf("non-V3 persistence columns=%+v", columns)
	}
}

// LE-30: success call, failed value>0, success creation and envelopes 0/1/2.
func TestWorkerPersistenceTransactionAndNative(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(44)
	cfg := config.Config{Database: config.DatabaseConfig{DSN: workerComposeDSN}}
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	bh := hashOf('a')
	from, to, created := addressOf('A'), addressOf('B'), addressOf('C')
	cases := []struct {
		hash, typ, value, input, status string
		to, contract                    *string
		method                          *string
	}{
		{hashOf('1'), "0x2", "0x3", "0x12345678abcd", "0x1", &to, nil, workerPtr("0x12345678")},
		{hashOf('2'), "0x0", "0x4", "0x", "0x0", &to, nil, nil},
		{hashOf('3'), "0x1", "0x5", "0x6000", "0x1", nil, &created, nil},
	}
	txs := make([]any, len(cases))
	receipts := make([]map[string]any, len(cases))
	for i, c := range cases {
		var recipient any
		if c.to != nil {
			recipient = *c.to
		}
		txs[i] = workerTx(c.hash, from, recipient, i, c.typ, c.value, c.input)
		logs := []any{}
		if i == len(cases)-1 { // unknown log gives this transaction-only case a real worker commit.
			l := workerLog(32, bh, c.hash, 0, addressOf('D'), []string{hashOf('f')}, "0x")
			l["transactionIndex"] = "0x2"
			logs = append(logs, l)
		}
		receipts[i] = workerReceipt(32, bh, c.hash, c.status, i, logs)
		if i == 1 {
			delete(receipts[i], "effectiveGasPrice") // legacy receipt exercises gas-price fallback + NULL.
		}
		if i == 2 {
			delete(txs[i].(map[string]any), "gasPrice") // typed creation preserves an absent price as NULL.
		}
		if c.contract != nil {
			receipts[i]["contractAddress"] = *c.contract
		}
		f.PutReceipt(c.hash, receipts[i])
	}
	block := workerBlock(32, bh, hashOf('0'), txs)
	f.PutHeader(32, block)
	f.PutBlock(32, block)
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	before := workerPGTime(t, db)
	p := runWorkerRuntime(t, f, cfg.Database.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	workerCursorCommitted(t, db, notice, ctx, 1)
	p.Stop(t)
	after := workerPGTime(t, db)
	workerAssertV3(t, db)
	for i, c := range cases {
		status := "SUCCESS"
		if c.status == "0x0" {
			status = "FAILED"
		}
		gasPrice, effective, fee := workerPtr("2"), workerPtr("3"), workerPtr("63000")
		if i == 1 {
			effective = nil
			fee = workerPtr("42000")
		}
		if i == 2 {
			gasPrice = nil
		}
		workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: c.hash, TxIndex: i, FromAddress: from, ToAddress: c.to, Nonce: int64(7 + i), Value: []string{"3", "4", "5"}[i], Input: &c.input, MethodID: c.method, GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: gasPrice, EffectiveGasPrice: effective, TransactionFee: fee, ExecutionStatus: status, ContractAddress: c.contract, ChainStatus: "ACTIVE", BlockTime: 153}, txs[i], receipts[i], before, after, len(cases))
	}
	for _, c := range []struct {
		hash, to, amount string
		index            int
	}{{cases[0].hash, to, "3", 0}, {cases[2].hash, created, "5", 2}} {
		workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: c.hash, TxIndex: &c.index, SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", TransferType: "NATIVE", FromAddress: from, ToAddress: c.to, Amount: &c.amount, ChainStatus: "ACTIVE", BlockTime: 153}, nil, before, after)
	}
	var all []postgre.ChainTransfer
	if err := db.Order("id").Find(&all).Error; err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].TxHash == cases[1].hash || all[1].TxHash == cases[1].hash {
		t.Fatalf("failed transaction produced Native or unexpected transfer: %+v", all)
	}
}

// LE-31 owns a shared-topic pair; three topics mean ERC20, four mean ERC721.
func TestWorkerPersistenceERC20AndERC721(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(44)
	cfg := config.Config{Database: config.DatabaseConfig{DSN: workerComposeDSN}}
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	bh, th := hashOf('b'), hashOf('4')
	from, to, token := addressOf('a'), addressOf('b'), addressOf('C')
	logs := []map[string]any{
		workerLog(32, bh, th, 3, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(17)),
		workerLog(32, bh, th, 8, token, []string{e.ERC721Transfer.Topic0, workerAddressWord(from), workerAddressWord(to), "0x" + workerWord(29)}, "0x"),
	}
	tx := workerTx(th, addressOf('D'), addressOf('E'), 0, "0x0", "0x0", "0x")
	receipt := workerReceipt(32, bh, th, "0x1", 0, []any{logs[0], logs[1]})
	block := workerBlock(32, bh, hashOf('0'), []any{tx})
	f.PutHeader(32, block)
	f.PutBlock(32, block)
	f.PutReceipt(th, receipt)
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	before := workerPGTime(t, db)
	p := runWorkerRuntime(t, f, cfg.Database.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	workerCursorCommitted(t, db, notice, ctx, 2)
	p.Stop(t)
	after := workerPGTime(t, db)
	workerAssertV3(t, db)
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, FromAddress: addressOf('D'), ToAddress: workerPtr(addressOf('E')), Nonce: 7, Value: "0", Input: workerPtr("0x"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, tx, receipt, before, after)
	for i, w := range []struct {
		source, typ, amount string
		id                  *string
	}{{"3", "ERC20", "17", nil}, {"8", "ERC721", "1", workerPtr("29")}} {
		workerAssertRaw(t, db, logs[i], before, after)
		workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: w.source, TransferType: w.typ, TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: &w.amount, TokenID: w.id, ChainStatus: "ACTIVE", BlockTime: 153}, logs[i], before, after)
	}
	var rows []postgre.ChainTransfer
	if err := db.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("shared-topic pair extra rows=%+v", rows)
	}
}

// LE-32 independently encodes canonical dynamic offsets and duplicate token IDs.
func TestWorkerPersistenceERC1155(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(44)
	cfg := config.Config{Database: config.DatabaseConfig{DSN: workerComposeDSN}}
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	bh, th := hashOf('c'), hashOf('5')
	operator, from, to, token := addressOf('D'), addressOf('a'), addressOf('b'), addressOf('C')
	topics := []string{e.ERC1155TransferSingle.Topic0, workerAddressWord(operator), workerAddressWord(from), workerAddressWord(to)}
	batchTopics := append([]string(nil), topics...)
	batchTopics[0] = e.ERC1155TransferBatch.Topic0
	// ids offset=64, values offset=160, ids=[7,7], values=[9,11].
	batch := "0x" + workerWord(64) + workerWord(160) + workerWord(2) + workerWord(7) + workerWord(7) + workerWord(2) + workerWord(9) + workerWord(11)
	logs := []map[string]any{workerLog(32, bh, th, 2, token, topics, "0x"+workerWord(19)+workerWord(23)), workerLog(32, bh, th, 6, token, batchTopics, batch)}
	tx := workerTx(th, addressOf('1'), addressOf('2'), 0, "0x1", "0x0", "0x")
	receipt := workerReceipt(32, bh, th, "0x1", 0, []any{logs[0], logs[1]})
	block := workerBlock(32, bh, hashOf('0'), []any{tx})
	f.PutHeader(32, block)
	f.PutBlock(32, block)
	f.PutReceipt(th, receipt)
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	before := workerPGTime(t, db)
	p := runWorkerRuntime(t, f, cfg.Database.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	workerCursorCommitted(t, db, notice, ctx, 2)
	p.Stop(t)
	after := workerPGTime(t, db)
	workerAssertV3(t, db)
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, FromAddress: addressOf('1'), ToAddress: workerPtr(addressOf('2')), Nonce: 7, Value: "0", Input: workerPtr("0x"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, tx, receipt, before, after)
	for _, l := range logs {
		workerAssertRaw(t, db, l, before, after)
	} // includes exact operator topics + dynamic payload.
	for _, w := range []struct {
		source, id, amount string
		batch, log         int
	}{{"2", "19", "23", 0, 0}, {"6", "7", "9", 0, 1}, {"6", "7", "11", 1, 1}} {
		workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: w.source, TransferType: "ERC1155", TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: &w.amount, TokenID: &w.id, BatchIndex: w.batch, ChainStatus: "ACTIVE", BlockTime: 153}, logs[w.log], before, after)
	}
	var rows []postgre.ChainTransfer
	if err := db.Order("source_index,batch_index").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("1155 expansion rows=%+v", rows)
	}
}

// LE-33 is a table of independent wire inputs in one worker page. Each negative
// source is checked as an empty row set, followed by a legal sentinel source.
func TestWorkerPersistenceNumericAddressAndMalformed(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(44)
	cfg := config.Config{Database: config.DatabaseConfig{DSN: workerComposeDSN}}
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	bh, th := hashOf('d'), hashOf('6')
	from, to, token := addressOf('a'), addressOf('b'), addressOf('C')
	zero := addressOf('0')
	mixedFrom := "0x" + strings.Repeat("aB", 20)
	mixedTo := "0x" + strings.Repeat("cD", 20)
	t20 := []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}
	t1155 := []string{e.ERC1155TransferBatch.Topic0, workerAddressWord(addressOf('D')), workerAddressWord(from), workerAddressWord(to)}
	canonical := "0x" + workerWord(64) + workerWord(160) + workerWord(2) + workerWord(7) + workerWord(7) + workerWord(2) + workerWord(9) + workerWord(11)
	max := "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	inputs := []struct {
		name                   string
		topics                 []string
		data, amount, from, to string
	}{
		{"zero-uint256", t20, "0x" + workerWord(0), "0", from, to},
		{"max-uint256", t20, "0x" + strings.Repeat("f", 64), max, from, to},
		{"leading-zero-word", t20, "0x" + workerWord(15), "15", from, to},
		{"zero-address-mint", []string{e.ERC20Transfer.Topic0, workerAddressWord(zero), workerAddressWord(to)}, "0x" + workerWord(2), "2", zero, to},
		{"zero-address-burn", []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(zero)}, "0x" + workerWord(3), "3", from, zero},
		{"mixed-case-ABI-provider-preserved", []string{e.ERC20Transfer.Topic0, workerAddressWord(mixedFrom), workerAddressWord(mixedTo)}, "0x" + workerWord(4), "4", strings.ToLower(mixedFrom), strings.ToLower(mixedTo)},
		{"nonzero-address-padding", []string{e.ERC20Transfer.Topic0, "0x01" + strings.Repeat("0", 22) + strings.TrimPrefix(from, "0x"), workerAddressWord(to)}, "0x" + workerWord(1), "", "", ""},
		{"short-indexed-address", []string{e.ERC20Transfer.Topic0, from, workerAddressWord(to)}, "0x" + workerWord(1), "", "", ""},
		{"illegal-indexed-hex", []string{e.ERC20Transfer.Topic0, "0x" + strings.Repeat("g", 64), workerAddressWord(to)}, "0x" + workerWord(1), "", "", ""},
		{"missing-indexed-topic", []string{e.ERC20Transfer.Topic0, workerAddressWord(from)}, "0x" + workerWord(1), "", "", ""},
		{"illegal-data-hex", t20, "0x" + strings.Repeat("z", 64), "", "", ""},
		{"uint256-overflow-data", t20, "0x01" + strings.Repeat("0", 64), "", "", ""},
		{"noncanonical-offset", t1155, "0x" + workerWord(64) + workerWord(64) + workerWord(2) + workerWord(7) + workerWord(7) + workerWord(2) + workerWord(9) + workerWord(11), "", "", ""},
		{"trailing-data", t20, "0x" + workerWord(1) + workerWord(0), "", "", ""},
		{"truncated-single", []string{e.ERC1155TransferSingle.Topic0, workerAddressWord(addressOf('D')), workerAddressWord(from), workerAddressWord(to)}, "0x" + workerWord(7), "", "", ""},
		{"empty-ERC20-data", t20, "0x", "", "", ""},
		{"empty-batch", t1155, "0x" + workerWord(64) + workerWord(96) + workerWord(0) + workerWord(0), "", "", ""},
		{"mismatched-batch-no-prefix", t1155, "0x" + workerWord(64) + workerWord(160) + workerWord(2) + workerWord(7) + workerWord(7) + workerWord(1) + workerWord(9), "", "", ""},
		{"truncated-batch-no-prefix", t1155, canonical[:len(canonical)-64], "", "", ""},
		{"malicious-batch-length", t1155, "0x" + workerWord(64) + workerWord(160) + strings.Repeat("f", 64) + workerWord(7) + workerWord(7) + workerWord(2) + workerWord(9) + workerWord(11), "", "", ""},
		{"unknown", []string{hashOf('f'), workerAddressWord(from), workerAddressWord(to)}, "0x" + workerWord(1), "", "", ""},
		{"valid-after-all-malformed", t20, "0x" + workerWord(31), "31", from, to},
	}
	logs := make([]any, len(inputs))
	for i, c := range inputs {
		logs[i] = workerLog(32, bh, th, i, token, c.topics, c.data)
	}
	tx := workerTx(th, mixedFrom, mixedTo, 0, "0x2", "0x0", "0x")
	receipt := workerReceipt(32, bh, th, "0x1", 0, logs)
	block := workerBlock(32, bh, hashOf('0'), []any{tx})
	f.PutHeader(32, block)
	f.PutBlock(32, block)
	f.PutReceipt(th, receipt)
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	before := workerPGTime(t, db)
	p := runWorkerRuntime(t, f, cfg.Database.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	workerCursorCommitted(t, db, notice, ctx, len(logs))
	p.Stop(t)
	after := workerPGTime(t, db)
	workerAssertV3(t, db)
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, FromAddress: mixedFrom, ToAddress: &mixedTo, Nonce: 7, Value: "0", Input: workerPtr("0x"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, tx, receipt, before, after)
	for i, c := range inputs {
		t.Run(c.name, func(t *testing.T) {
			workerAssertRaw(t, db, logs[i].(map[string]any), before, after)
			var rows []postgre.ChainTransfer
			if err := db.Where("chain_id=1 AND block_hash=? AND tx_hash=? AND source_type='EVENT_LOG' AND source_index=?", bh, th, strconv.Itoa(i)).Find(&rows).Error; err != nil {
				t.Fatal(err)
			}
			if c.amount == "" {
				if len(rows) != 0 {
					t.Fatalf("malformed/unknown event produced partial results=%+v", rows)
				}
				return
			}
			workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: strconv.Itoa(i), TransferType: "ERC20", TokenAddress: &token, FromAddress: c.from, ToAddress: c.to, Amount: &c.amount, ChainStatus: "ACTIVE", BlockTime: 153}, logs[i], before, after)
		})
	}
	var rows []postgre.ChainTransfer
	if err := db.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 7 {
		t.Fatalf("boundary page extra/missing identities=%+v", rows)
	}
}

// LE-34 uses an independently constructed mixed receipt, including unknown
// first, non-contiguous log indices, and a Batch that expands one raw identity.
func TestWorkerPersistenceMixedLogsAndJSONLinkage(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(44)
	cfg := config.Config{Database: config.DatabaseConfig{DSN: workerComposeDSN}}
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	bh, th := hashOf('e'), hashOf('7')
	from, to, operator, token := addressOf('a'), addressOf('b'), addressOf('d'), addressOf('C')
	logs := []map[string]any{
		workerLog(32, bh, th, 0, token, []string{hashOf('f')}, "0x1234"),
		workerLog(32, bh, th, 2, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(43)),
		workerLog(32, bh, th, 5, token, []string{e.ERC721Transfer.Topic0, workerAddressWord(from), workerAddressWord(to), "0x" + workerWord(47)}, "0x"),
		workerLog(32, bh, th, 7, token, []string{e.ERC1155TransferSingle.Topic0, workerAddressWord(operator), workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(53)+workerWord(59)),
		workerLog(32, bh, th, 10, token, []string{e.ERC1155TransferBatch.Topic0, workerAddressWord(operator), workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(64)+workerWord(160)+workerWord(2)+workerWord(61)+workerWord(61)+workerWord(2)+workerWord(67)+workerWord(71)),
	}
	wireLogs := make([]any, len(logs))
	for i, l := range logs {
		l["fixtureExtra"] = map[string]any{"label": fmt.Sprintf("log-%d", i), "largeDecimal": "115792089237316195423570985008687907853269984665640564039457584007913129639935", "list": []any{true, nil, "x"}}
		wireLogs[i] = l
	}
	tx := workerTx(th, addressOf('D'), addressOf('E'), 0, "0x2", "0x0", "0xaabbccdd00")
	tx["accessList"] = []any{map[string]any{"address": token, "storageKeys": []string{hashOf('1')}}}
	receipt := workerReceipt(32, bh, th, "0x1", 0, wireLogs)
	receipt["cumulativeGasUsed"] = "0xdead"
	receipt["fixtureBoolean"] = true
	block := workerBlock(32, bh, hashOf('0'), []any{tx})
	f.PutHeader(32, block)
	f.PutBlock(32, block)
	f.PutReceipt(th, receipt)
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	before := workerPGTime(t, db)
	p := runWorkerRuntime(t, f, cfg.Database.DSN)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(block)
	last := workerCursorCommitted(t, db, notice, ctx, len(logs))
	p.Stop(t)
	after := workerPGTime(t, db)
	workerAssertV3(t, db)
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, FromAddress: addressOf('D'), ToAddress: workerPtr(addressOf('E')), Nonce: 7, Value: "0", Input: workerPtr("0xaabbccdd00"), MethodID: workerPtr("0xaabbccdd"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, tx, receipt, before, after)
	var rawIDs []int64
	for _, l := range logs {
		rawIDs = append(rawIDs, workerAssertRaw(t, db, l, before, after))
	}
	if rawIDs[len(rawIDs)-1] != last {
		t.Fatalf("raw order=%v cursor=%d", rawIDs, last)
	}
	expected := []struct {
		log, batch          int
		source, typ, amount string
		id                  *string
	}{
		{1, 0, "2", "ERC20", "43", nil}, {2, 0, "5", "ERC721", "1", workerPtr("47")}, {3, 0, "7", "ERC1155", "59", workerPtr("53")}, {4, 0, "10", "ERC1155", "67", workerPtr("61")}, {4, 1, "10", "ERC1155", "71", workerPtr("61")},
	}
	for _, w := range expected {
		id := workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 32, BlockHash: bh, TxHash: th, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: w.source, TransferType: w.typ, TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: &w.amount, TokenID: w.id, BatchIndex: w.batch, ChainStatus: "ACTIVE", BlockTime: 153}, logs[w.log], before, after)
		var linked []struct {
			RawID, TransferID       int64
			SourceType, SourceIndex string
			BatchIndex              int
		}
		if err := db.Raw(`SELECT re.id AS raw_id,tr.id AS transfer_id,tr.source_type,tr.source_index,tr.batch_index FROM chain_transfers tr JOIN chain_raw_events re ON re.chain_id=tr.chain_id AND re.block_hash=tr.block_hash AND re.tx_hash=tr.tx_hash AND re.log_index::text=tr.source_index WHERE tr.source_type='EVENT_LOG' AND tr.id=?`, id).Scan(&linked).Error; err != nil {
			t.Fatal(err)
		}
		want := []struct {
			RawID, TransferID       int64
			SourceType, SourceIndex string
			BatchIndex              int
		}{{rawIDs[w.log], id, "EVENT_LOG", w.source, w.batch}}
		if !reflect.DeepEqual(linked, want) {
			t.Fatalf("natural-key linkage=%+v want=%+v", linked, want)
		}
	}
	var rows []postgre.ChainTransfer
	if err := db.Order("id").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(expected) {
		t.Fatalf("unknown/source/batch rows=%+v", rows)
	}
	for _, r := range rows {
		if r.SourceIndex == "0" {
			t.Fatalf("unknown log produced source=%+v", r)
		}
	}
}

// LE-35 owns a three-stage timeline: commit A, replace it with fork B, then
// reject the second item in C's worker transaction. All replay is performed by
// a new service process against scanner-produced raw rows, never by calling a
// mapper, parser, worker method or repository from the test.
func TestWorkerPersistenceStateReplayAndPageAtomicity(t *testing.T) {
	f := testioc.InitRPCFixture(t)
	f.SetLatest(45)
	cfg := config.Config{Database: config.DatabaseConfig{DSN: workerComposeDSN}}
	db := testioc.InitDB(t, context.Background(), cfg.Database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	commonHash, aHash, bHash, cHash := hashOf('0'), hashOf('a'), hashOf('b'), hashOf('c')
	aTx, bTx, cTx := hashOf('1'), hashOf('2'), hashOf('3')
	from, to, token := addressOf('a'), addressOf('b'), addressOf('C')
	common := workerBlock(32, commonHash, hashOf('f'), []any{})
	f.PutHeader(32, common)
	f.PutBlock(32, common)
	aLogs := make([]any, 100)
	for i := range aLogs {
		aLogs[i] = workerLog(33, aHash, aTx, i, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(int64(i+1)))
	}
	aWireTx := workerTx(aTx, from, to, 0, "0x2", "0x3", "0x")
	aReceipt := workerReceipt(33, aHash, aTx, "0x1", 0, aLogs)
	a := workerBlock(33, aHash, commonHash, []any{aWireTx})
	f.PutHeader(33, a)
	f.PutBlock(33, a)
	f.PutReceipt(aTx, aReceipt)
	headA := workerBlock(45, hashOf('d'), aHash, []any{})
	f.PutHeader(45, headA)
	notice, err := testioc.NewNotifyBarrier(context.Background(), cfg.Database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	defer notice.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	before := workerPGTime(t, db)
	p := runWorkerRuntime(t, f, cfg.Database.DSN)
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(headA)
	firstCursor := workerCursorCommitted(t, db, notice, ctx, 100)
	p.Stop(t)
	if err := f.WaitForNoSubscribers(ctx); err != nil {
		t.Fatal(err)
	}
	afterA := workerPGTime(t, db)
	workerAssertV3(t, db)
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 33, BlockHash: aHash, TxHash: aTx, FromAddress: from, ToAddress: &to, Nonce: 7, Value: "3", Input: workerPtr("0x"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, aWireTx, aReceipt, before, afterA)
	workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 33, BlockHash: aHash, TxHash: aTx, TxIndex: workerPtr(0), SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", TransferType: "NATIVE", FromAddress: from, ToAddress: to, Amount: workerPtr("3"), ChainStatus: "ACTIVE", BlockTime: 153}, nil, before, afterA)
	for i, l := range aLogs {
		workerAssertRaw(t, db, l.(map[string]any), before, afterA)
		workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 33, BlockHash: aHash, TxHash: aTx, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: strconv.Itoa(i), TransferType: "ERC20", TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: workerPtr(strconv.Itoa(i + 1)), ChainStatus: "ACTIVE", BlockTime: 153}, l, before, afterA)
	}
	// Snapshot complete persisted rows and IDs before any fork exists.
	var activeTransfers []postgre.ChainTransfer
	var activeRaw []postgre.ChainRawEvent
	var activeTxs []postgre.ChainTransaction
	if err := db.Order("id").Find(&activeTransfers).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Order("id").Find(&activeRaw).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Order("tx_hash").Find(&activeTxs).Error; err != nil {
		t.Fatal(err)
	}
	if len(activeTransfers) != 101 || len(activeRaw) != 100 || len(activeTxs) != 1 || firstCursor != activeRaw[99].ID {
		t.Fatalf("ACTIVE snapshot sizes transfers=%d raw=%d tx=%d cursor=%d", len(activeTransfers), len(activeRaw), len(activeTxs), firstCursor)
	}

	// B replaces height 33, with the persisted height-32 block as common ancestor.
	bLogs := []any{workerLog(33, bHash, bTx, 0, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(103)), workerLog(33, bHash, bTx, 1, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(107))}
	bWireTx := workerTx(bTx, from, to, 0, "0x1", "0x4", "0x")
	bReceipt := workerReceipt(33, bHash, bTx, "0x1", 0, bLogs)
	b := workerBlock(33, bHash, commonHash, []any{bWireTx})
	f.PutHeader(33, b)
	f.PutBlock(33, b)
	f.PutReceipt(bTx, bReceipt)
	headB := workerBlock(45, hashOf('e'), bHash, []any{})
	f.PutHeader(45, headB)
	rollbackBefore := workerPGTime(t, db)
	p = runWorkerRuntime(t, f, cfg.Database.DSN)
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(headB)
	secondCursor := workerCursorCommitted(t, db, notice, ctx, 102)
	p.Stop(t)
	if err := f.WaitForNoSubscribers(ctx); err != nil {
		t.Fatal(err)
	}
	rollbackAfter := workerPGTime(t, db)
	if secondCursor <= firstCursor {
		t.Fatalf("fork cursor did not advance %d -> %d", firstCursor, secondCursor)
	}
	var reorgs []postgre.ChainReorg
	if err := db.Find(&reorgs).Error; err != nil {
		t.Fatal(err)
	}
	if len(reorgs) != 1 {
		t.Fatalf("reorg provenance=%+v", reorgs)
	}
	r := reorgs[0]
	if r.ReorgID == "" || r.ChainID != 1 || r.AncestorHeight != 32 || r.AncestorHash != commonHash || r.OldHeadHeight != 33 || r.OldHeadHash != aHash || r.NewHeadHeight != 45 || r.NewHeadHash != headB["hash"] || r.ReorgDepth != 1 || r.Status != "COMPLETED" || r.ErrorMessage != nil || r.DetectedAt < rollbackBefore || r.DetectedAt > rollbackAfter || r.CompletedAt == nil || *r.CompletedAt < rollbackBefore || *r.CompletedAt > rollbackAfter {
		t.Fatalf("fork reorg fields=%+v", r)
	}
	var revertedTransfers []postgre.ChainTransfer
	var revertedRaw []postgre.ChainRawEvent
	var revertedTxs []postgre.ChainTransaction
	if err := db.Where("block_hash=?", aHash).Order("id").Find(&revertedTransfers).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("block_hash=?", aHash).Order("id").Find(&revertedRaw).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("block_hash=?", aHash).Order("tx_hash").Find(&revertedTxs).Error; err != nil {
		t.Fatal(err)
	}
	if len(revertedTransfers) != len(activeTransfers) || len(revertedRaw) != len(activeRaw) || len(revertedTxs) != 1 {
		t.Fatal("fork lost original identities")
	}
	var revertedAt int64
	for i, g := range revertedRaw {
		if g.ChainStatus != "REVERTED" || g.ReorgID == nil || *g.ReorgID != r.ReorgID || g.RevertedAt == nil || *g.RevertedAt < rollbackBefore || *g.RevertedAt > rollbackAfter {
			t.Fatalf("raw rollback metadata=%+v", g)
		}
		if i == 0 {
			revertedAt = *g.RevertedAt
		}
		if *g.RevertedAt != revertedAt {
			t.Fatal("one rollback has differing raw timestamps")
		}
		g.ChainStatus, g.ReorgID, g.RevertedAt = "ACTIVE", nil, nil
		if !reflect.DeepEqual(g, activeRaw[i]) {
			t.Fatalf("fork changed raw identity/business fields index=%d", i)
		}
	}
	for i, g := range revertedTransfers {
		if g.ChainStatus != "REVERTED" || g.ReorgID == nil || *g.ReorgID != r.ReorgID || g.RevertedAt == nil || *g.RevertedAt != revertedAt {
			t.Fatalf("transfer rollback metadata=%+v", g)
		}
		g.ChainStatus, g.ReorgID, g.RevertedAt = "ACTIVE", nil, nil
		if !reflect.DeepEqual(g, activeTransfers[i]) {
			t.Fatalf("fork changed transfer row ID/business fields index=%d", i)
		}
	}
	gTx := revertedTxs[0]
	if gTx.ChainStatus != "REVERTED" || gTx.ReorgID == nil || *gTx.ReorgID != r.ReorgID || gTx.RevertedAt == nil || *gTx.RevertedAt != revertedAt {
		t.Fatalf("transaction rollback metadata=%+v", gTx)
	}
	gTx.ChainStatus, gTx.ReorgID, gTx.RevertedAt = "ACTIVE", nil, nil
	if !reflect.DeepEqual(gTx, activeTxs[0]) {
		t.Fatal("fork changed transaction business/JSON fields")
	}
	var blocks []postgre.ChainBlock
	if err := db.Order("block_number,block_hash").Find(&blocks).Error; err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 {
		t.Fatalf("fork block identities=%+v", blocks)
	}
	for _, block := range blocks {
		if block.BlockHash == aHash {
			if block.Status != "ORPHANED" || block.ReorgID == nil || *block.ReorgID != r.ReorgID || block.OrphanedAt == nil || *block.OrphanedAt != revertedAt {
				t.Fatalf("orphan metadata=%+v", block)
			}
		} else if block.Status != "CANONICAL" || block.ReorgID != nil || block.OrphanedAt != nil {
			t.Fatalf("canonical block metadata=%+v", block)
		}
	}
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 33, BlockHash: bHash, TxHash: bTx, FromAddress: from, ToAddress: &to, Nonce: 7, Value: "4", Input: workerPtr("0x"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, bWireTx, bReceipt, rollbackBefore, rollbackAfter)
	workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 33, BlockHash: bHash, TxHash: bTx, TxIndex: workerPtr(0), SourceType: "TRANSACTION_VALUE", SourceIndex: "tx-value", TransferType: "NATIVE", FromAddress: from, ToAddress: to, Amount: workerPtr("4"), ChainStatus: "ACTIVE", BlockTime: 153}, nil, rollbackBefore, rollbackAfter)
	for i, l := range bLogs {
		workerAssertRaw(t, db, l.(map[string]any), rollbackBefore, rollbackAfter)
		workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 33, BlockHash: bHash, TxHash: bTx, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: strconv.Itoa(i), TransferType: "ERC20", TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: workerPtr([]string{"103", "107"}[i]), ChainStatus: "ACTIVE", BlockTime: 153}, l, rollbackBefore, rollbackAfter)
	}

	// Model one missing derived result while stopped, without changing raw input.
	// Rebuilding it from already-REVERTED raw data proves that worker consumption
	// cannot pass merely because the reorg updated an old ACTIVE transfer in PG.
	var beforeMissing []postgre.ChainTransfer
	if err := db.Order("id").Find(&beforeMissing).Error; err != nil {
		t.Fatal(err)
	}
	var missing postgre.ChainTransfer
	if err := db.Where("chain_id=1 AND block_hash=? AND tx_hash=? AND source_type='EVENT_LOG' AND source_index='99' AND batch_index=0", aHash, aTx).First(&missing).Error; err != nil {
		t.Fatal(err)
	}
	removed := db.Exec(`DELETE FROM chain_transfers WHERE id=? AND chain_id=1 AND block_hash=? AND tx_hash=? AND source_type='EVENT_LOG' AND source_index='99' AND batch_index=0`, missing.ID, aHash, aTx)
	if removed.Error != nil {
		t.Fatal(removed.Error)
	}
	if removed.RowsAffected != 1 {
		t.Fatal("missing-result input did not remove the exact test-owned row")
	}
	if err := db.Exec(`UPDATE chain_event_biz_cursors SET cursor=0 WHERE biz='transaction' AND chain_family='evm'`).Error; err != nil {
		t.Fatal(err)
	}
	rebuildBefore := workerPGTime(t, db)
	p = runWorkerRuntime(t, f, cfg.Database.DSN)
	workerCursorCommitted(t, db, notice, ctx, 102)
	p.Stop(t)
	if err := f.WaitForNoSubscribers(ctx); err != nil {
		t.Fatal(err)
	}
	rebuildAfter := workerPGTime(t, db)
	rebuiltID := workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 33, BlockHash: aHash, TxHash: aTx, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: "99", TransferType: "ERC20", TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: workerPtr("100"), ChainStatus: "REVERTED", ReorgID: &r.ReorgID, RevertedAt: &revertedAt, BlockTime: 153}, aLogs[99], rebuildBefore, rebuildAfter)
	if rebuiltID == missing.ID {
		t.Fatal("missing reverted result was not rebuilt with a new row identity")
	}
	var recovered []postgre.ChainTransfer
	if err := db.Order("id").Find(&recovered).Error; err != nil {
		t.Fatal(err)
	}
	if len(recovered) != len(beforeMissing) {
		t.Fatal("rebuilding one missing result changed unique cardinality")
	}
	byID := make(map[int64]postgre.ChainTransfer, len(recovered))
	for _, row := range recovered {
		byID[row.ID] = row
	}
	if _, exists := byID[missing.ID]; exists {
		t.Fatal("deleted derived row ID reappeared")
	}
	for _, original := range beforeMissing {
		if original.ID != missing.ID && !reflect.DeepEqual(byID[original.ID], original) {
			t.Fatalf("rebuild mutated surviving row ID=%d", original.ID)
		}
	}

	// Capture all rows after the fork, then supply restored consumer checkpoints
	// while the service is stopped. Partial replay crosses the A/B boundary;
	// full replay consumes exactly a 100-REVERTED page then a 2-ACTIVE page.
	var beforeReplay []postgre.ChainTransfer
	if err := db.Order("id").Find(&beforeReplay).Error; err != nil {
		t.Fatal(err)
	}
	if len(beforeReplay) != 104 {
		t.Fatalf("post-fork transfer identities=%+v", beforeReplay)
	}
	for _, restore := range []struct {
		name   string
		cursor int64
	}{{"partial-boundary-replay", activeRaw[98].ID}, {"whole-two-page-replay", 0}} {
		t.Run(restore.name, func(t *testing.T) {
			if err := db.Exec(`UPDATE chain_event_biz_cursors SET cursor=? WHERE biz='transaction' AND chain_family='evm'`, restore.cursor).Error; err != nil {
				t.Fatal(err)
			}
			p := runWorkerRuntime(t, f, cfg.Database.DSN)
			cursor := workerCursorCommitted(t, db, notice, ctx, 102)
			p.Stop(t)
			if err := f.WaitForNoSubscribers(ctx); err != nil {
				t.Fatal(err)
			}
			var replay []postgre.ChainTransfer
			if err := db.Order("id").Find(&replay).Error; err != nil {
				t.Fatal(err)
			}
			if cursor != secondCursor || !reflect.DeepEqual(replay, beforeReplay) {
				t.Fatalf("replay changed IDs/business/metadata: cursor=%d want=%d", cursor, secondCursor)
			}
		})
	}

	// A test-only CHECK fails the second upsert in a real PG worker transaction.
	// It does not intercept the scanner, RPC, parser or process lifecycle.
	if err := db.Exec(`ALTER TABLE chain_transfers ADD CONSTRAINT le35_reject_second_item CHECK (NOT (block_hash='` + cHash + `' AND source_type='EVENT_LOG' AND source_index='1')) NOT VALID`).Error; err != nil {
		t.Fatal(err)
	}
	gateInstalled := true
	defer func() {
		if gateInstalled {
			if err := db.Exec(`ALTER TABLE chain_transfers DROP CONSTRAINT IF EXISTS le35_reject_second_item`).Error; err != nil {
				t.Error(err)
			}
		}
	}()
	cLogs := []any{workerLog(34, cHash, cTx, 0, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(109)), workerLog(34, cHash, cTx, 1, token, []string{e.ERC20Transfer.Topic0, workerAddressWord(from), workerAddressWord(to)}, "0x"+workerWord(113))}
	cWireTx := workerTx(cTx, from, to, 0, "0x0", "0x0", "0x")
	cReceipt := workerReceipt(34, cHash, cTx, "0x1", 0, cLogs)
	c := workerBlock(34, cHash, bHash, []any{cWireTx})
	f.PutBlock(34, c)
	f.PutHeader(34, c)
	f.PutReceipt(cTx, cReceipt)
	f.SetLatest(46)
	headC := workerBlock(46, hashOf('f'), cHash, []any{})
	f.PutHeader(46, headC)
	failureBefore := workerPGTime(t, db)
	p = runWorkerRuntime(t, f, cfg.Database.DSN)
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.PublishHead(headC)
	select {
	case <-p.waitDone:
	case <-ctx.Done():
		_, _ = p.drain(context.Background(), true)
		t.Fatal("PG failure did not terminate worker/service")
	}
	if _, err := p.drain(context.Background(), false); err == nil {
		t.Fatal("PG transaction failure unexpectedly exited successfully")
	}
	if err := f.WaitForNoSubscribers(ctx); err != nil {
		t.Fatal(err)
	}
	failureAfter := workerPGTime(t, db)
	var failedCursor int64
	if err := db.Raw(`SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'`).Scan(&failedCursor).Error; err != nil {
		t.Fatal(err)
	}
	var afterFailure []postgre.ChainTransfer
	if err := db.Order("id").Find(&afterFailure).Error; err != nil {
		t.Fatal(err)
	}
	if failedCursor != secondCursor || !reflect.DeepEqual(afterFailure, beforeReplay) {
		t.Fatal("failed second upsert committed part of page or cursor")
	}
	var cp postgre.ChainCheckpoint
	if err := db.Where("chain_id=1").First(&cp).Error; err != nil {
		t.Fatal(err)
	}
	if cp.LastProcessedHeight != 34 || cp.LastProcessedHash != cHash {
		t.Fatalf("failure was not in worker after scanner commit: %+v", cp)
	}
	var cRawIDs []int64
	for _, l := range cLogs {
		cRawIDs = append(cRawIDs, workerAssertRaw(t, db, l.(map[string]any), failureBefore, failureAfter))
	}
	if cRawIDs[0] <= failedCursor || cRawIDs[1] <= cRawIDs[0] {
		t.Fatalf("failed page raw frontier=%v cursor=%d", cRawIDs, failedCursor)
	}
	workerAssertTransaction(t, db, postgre.ChainTransaction{ChainID: 1, BlockNumber: 34, BlockHash: cHash, TxHash: cTx, FromAddress: from, ToAddress: &to, Nonce: 7, Value: "0", Input: workerPtr("0x"), GasLimit: workerPtr("30000"), GasUsed: workerPtr("21000"), GasPrice: workerPtr("2"), EffectiveGasPrice: workerPtr("3"), TransactionFee: workerPtr("63000"), ExecutionStatus: "SUCCESS", ChainStatus: "ACTIVE", BlockTime: 153}, cWireTx, cReceipt, failureBefore, failureAfter)
	if err := db.Exec(`ALTER TABLE chain_transfers DROP CONSTRAINT le35_reject_second_item`).Error; err != nil {
		t.Fatal(err)
	}
	gateInstalled = false
	retryBefore := workerPGTime(t, db)
	p = runWorkerRuntime(t, f, cfg.Database.DSN)
	finalCursor := workerCursorCommitted(t, db, notice, ctx, 104)
	p.Stop(t)
	if err := f.WaitForNoSubscribers(ctx); err != nil {
		t.Fatal(err)
	}
	retryAfter := workerPGTime(t, db)
	if finalCursor != cRawIDs[1] || finalCursor <= secondCursor {
		t.Fatalf("retry cursor=%d raw=%v", finalCursor, cRawIDs)
	}
	for i, l := range cLogs {
		workerAssertTransfer(t, db, postgre.ChainTransfer{ChainID: 1, BlockNumber: 34, BlockHash: cHash, TxHash: cTx, TxIndex: workerPtr(0), SourceType: "EVENT_LOG", SourceIndex: strconv.Itoa(i), TransferType: "ERC20", TokenAddress: &token, FromAddress: from, ToAddress: to, Amount: workerPtr([]string{"109", "113"}[i]), ChainStatus: "ACTIVE", BlockTime: 153}, l, retryBefore, retryAfter)
	}
	var committed []postgre.ChainTransfer
	if err := db.Order("id").Find(&committed).Error; err != nil {
		t.Fatal(err)
	}
	if len(committed) != 106 || !reflect.DeepEqual(committed[:104], beforeReplay) {
		t.Fatal("retry changed prior rows or duplicated failure page")
	}
	// Restart once more with a partial duplicate page; the successful commit's
	// row IDs, JSON, times and business fields must remain byte-for-byte stable.
	if err := db.Exec(`UPDATE chain_event_biz_cursors SET cursor=? WHERE biz='transaction' AND chain_family='evm'`, cRawIDs[0]-1).Error; err != nil {
		t.Fatal(err)
	}
	p = runWorkerRuntime(t, f, cfg.Database.DSN)
	workerCursorCommitted(t, db, notice, ctx, 104)
	p.Stop(t)
	if err := f.WaitForNoSubscribers(ctx); err != nil {
		t.Fatal(err)
	}
	var final []postgre.ChainTransfer
	if err := db.Order("id").Find(&final).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(final, committed) {
		t.Fatal("restart after recovery duplicated or mutated committed rows")
	}
}
