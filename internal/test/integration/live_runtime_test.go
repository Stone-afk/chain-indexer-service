//go:build e2e

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
	"gorm.io/gorm"
)

func liveBoundedContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// This assembly deliberately uses no helpers from protected runtime_test.go,
// and never reads ignored live endpoint configuration.
type liveLocal struct {
	f           *testioc.RPCFixture
	db          *gorm.DB
	dsn, binary string
	chains      map[string]config.ChainConfig
	ctx         context.Context
	notice      *testioc.NotifyBarrier
}

func newLiveLocal(t *testing.T, enabled ...string) *liveLocal {
	t.Helper()
	ctx := liveBoundedContext(t, 90*time.Second)
	f := testioc.InitRPCFixture(t)
	database := config.DatabaseConfig{DSN: "postgres://postgres:postgres@127.0.0.1:15432/chain_indexer_e2e?sslmode=disable"}
	db := testioc.InitDB(t, ctx, database)
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(ctx); err != nil {
		t.Fatal(err)
	}
	chains := map[string]config.ChainConfig{}
	ids := map[string]int64{"ethereum": 1, "bsc": 56, "polygon": 137, "arbitrum": 42161, "optimism": 10, "base": 8453, "avalanche": 43114}
	confirms := map[string]int64{"ethereum": 12, "bsc": 15, "polygon": 100, "arbitrum": 20, "optimism": 20, "base": 20, "avalanche": 15}
	for name, id := range ids {
		c := config.ChainConfig{ChainID: id, ConfirmBlocks: confirms[name]}
		c.Enabled = false
		c.StartBlock = 7
		c.HTTPURL = f.HTTPURL()
		c.WSURL = f.WSURL()
		for _, n := range enabled {
			if name == n {
				c.Enabled = true
			}
		}
		chains[name] = c
	}
	notice, err := testioc.NewNotifyBarrier(ctx, database.DSN, "chain_event_biz_cursors")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := notice.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return &liveLocal{f, db, database.DSN, buildBinary(t, "./cmd/chain-indexer"), chains, ctx, notice}
}

type liveOutput struct {
	sync.Mutex
	text strings.Builder
}

func (b *liveOutput) Write(p []byte) (int, error) { b.Lock(); defer b.Unlock(); return b.text.Write(p) }
func (b *liveOutput) String() string              { b.Lock(); defer b.Unlock(); return b.text.String() }

type liveProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	output  *liveOutput
	stopped bool
}

func (l *liveLocal) start(t *testing.T, subscribers int) *liveProcess {
	t.Helper()
	p := &liveProcess{done: make(chan struct{}), output: &liveOutput{}}
	p.cmd = exec.Command(l.binary, "--config", testioc.WriteServiceConfig(t, config.DatabaseConfig{DSN: l.dsn}, l.chains))
	p.cmd.Dir = repoRoot(t)
	p.cmd.Env = commandEnvironment()
	p.cmd.Stdout = p.output
	p.cmd.Stderr = p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.stop(t) })
	if err := l.f.WaitForSubscribers(l.ctx, subscribers); err != nil {
		t.Fatalf("subscriber barrier: %v; process=%s", err, p.output.String())
	}
	return p
}
func (p *liveProcess) join(t *testing.T, ctx context.Context, success bool) string {
	t.Helper()
	select {
	case <-p.done:
	case <-ctx.Done():
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Fatal("killed process did not join")
		}
		p.stopped = true
		t.Fatal("process join deadline")
	}
	p.stopped = true
	if (p.err == nil) != success {
		t.Fatalf("process outcome=%v output=%s", p.err, p.output.String())
	}
	return p.output.String()
}
func (p *liveProcess) stop(t *testing.T) string {
	t.Helper()
	if p.stopped {
		return p.output.String()
	}
	select {
	case <-p.done:
		p.stopped = true
		if p.err != nil {
			t.Errorf("service exited unexpectedly: %v output=%s", p.err, p.output.String())
		}
		return p.output.String()
	default:
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Error(err)
	}
	return p.join(t, liveBoundedContext(t, 15*time.Second), true)
}
func liveHash(n int64) string    { return fmt.Sprintf("0x%064x", n) }
func liveAddress(n int64) string { return fmt.Sprintf("0x%040x", n) }
func (l *liveLocal) block(t *testing.T, height int64, logs int) map[string]any {
	t.Helper()
	ev, err := abi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	bh, th := liveHash(height+100), liveHash(height+200)
	from, to, token := liveAddress(1), liveAddress(2), liveAddress(3)
	word := func(s string) string { return "0x" + strings.Repeat("0", 24) + strings.TrimPrefix(s, "0x") }
	var ls []any
	for i := 0; i < logs; i++ {
		ls = append(ls, map[string]any{"address": token, "topics": []string{ev.ERC20Transfer.Topic0, word(from), word(to)}, "data": "0x" + strings.Repeat("0", 63) + "1", "blockNumber": fmt.Sprintf("0x%x", height), "blockHash": bh, "transactionHash": th, "transactionIndex": "0x0", "logIndex": fmt.Sprintf("0x%x", i)})
	}
	tx := map[string]any{"hash": th, "from": from, "to": to, "transactionIndex": "0x0", "nonce": "0x0", "type": "0x2", "gas": "0x5208", "gasPrice": "0x1", "value": "0x1", "input": "0x"}
	b := map[string]any{"number": fmt.Sprintf("0x%x", height), "hash": bh, "parentHash": liveHash(height + 99), "timestamp": "0x1", "gasUsed": "0x5208", "gasLimit": "0x5208", "transactions": []any{tx}}
	l.f.PutHeader(height, b)
	l.f.PutBlock(height, b)
	l.f.PutReceipt(th, map[string]any{"transactionHash": th, "blockNumber": fmt.Sprintf("0x%x", height), "blockHash": bh, "transactionIndex": "0x0", "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x1", "logs": ls})
	return b
}
func (l *liveLocal) waitCursor(t *testing.T, rawCount int64) {
	t.Helper()
	for {
		var r struct{ Count, Max, Cursor int64 }
		if err := l.db.WithContext(l.ctx).Raw("SELECT COUNT(*) AS count, COALESCE(MAX(id),0) AS max, COALESCE((SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'),0) AS cursor FROM chain_raw_events").Scan(&r).Error; err != nil {
			t.Fatal(err)
		}
		if r.Count == rawCount && r.Max > 0 && r.Cursor == r.Max {
			return
		}
		if err := l.notice.WaitForNotification(l.ctx); err != nil {
			t.Fatalf("cursor barrier: %v facts=%+v", err, r)
		}
	}
}
func (l *liveLocal) checkpoint(t *testing.T, chain, height int64) {
	t.Helper()
	var n int64
	if err := l.db.WithContext(l.ctx).Raw("SELECT COUNT(*) FROM chain_checkpoints WHERE chain_id=? AND last_processed_height=? AND last_processed_hash=?", chain, height, liveHash(height+100)).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("checkpoint chain=%d height=%d count=%d", chain, height, n)
	}
}
func (l *liveLocal) count(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := l.db.WithContext(l.ctx).Raw(sql, args...).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}
func (l *liveLocal) snapshot(t *testing.T) map[string]string {
	t.Helper()
	facts := map[string]string{}
	for _, table := range []string{"chain_blocks", "chain_transactions", "chain_raw_events", "chain_transfers"} {
		var value string
		if err := l.db.WithContext(l.ctx).Raw("SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb)::text FROM " + table + " r").Scan(&value).Error; err != nil {
			t.Fatal(err)
		}
		facts[table] = value
	}
	return facts
}
func (l *liveLocal) replayLE18(t *testing.T) {
	t.Helper()
	var batch postgre.CanonicalBatch
	if err := l.db.WithContext(l.ctx).Where("chain_id = ? AND block_hash = ?", 1, liveHash(107)).Take(&batch.Block).Error; err != nil {
		t.Fatal(err)
	}
	if err := l.db.WithContext(l.ctx).Where("chain_id = ? AND block_hash = ?", 1, liveHash(107)).Find(&batch.Transactions).Error; err != nil {
		t.Fatal(err)
	}
	if err := l.db.WithContext(l.ctx).Where("chain_id = ? AND block_hash = ?", 1, liveHash(107)).Find(&batch.RawEvents).Error; err != nil {
		t.Fatal(err)
	}
	if err := l.db.WithContext(l.ctx).Where("chain_id = ? AND block_hash = ? AND source_type = ?", 1, liveHash(107), postgre.TransferSourceTransactionValue.ToString()).Find(&batch.NativeTransfers).Error; err != nil {
		t.Fatal(err)
	}
	if err := l.db.WithContext(l.ctx).Where("chain_id = ?", 1).Take(&batch.Checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	if err := postgre.NewChainDAO(l.db).SaveCanonicalBatch(l.ctx, batch); err != nil {
		t.Fatal(err)
	}
}

func TestLiveLE14StartBlockBoundedScan(t *testing.T) {
	l := newLiveLocal(t, "ethereum")
	l.f.SetLatest(20) // safe=8; explicit start=7.
	l.block(t, 6, 1)
	l.block(t, 7, 1)
	head := l.block(t, 8, 1)
	l.block(t, 9, 1)
	p := l.start(t, 1)
	l.f.PublishHead(head)
	l.waitCursor(t, 2)
	p.stop(t)
	l.checkpoint(t, 1, 8)
	if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE chain_id=1 AND block_number BETWEEN 7 AND 8 AND block_hash=CASE block_number WHEN 7 THEN ? ELSE ? END AND status='CANONICAL'", liveHash(107), liveHash(108)); n != 2 {
		t.Fatalf("window rows=%d", n)
	}
	if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE block_number<7 OR block_number>8"); n != 0 {
		t.Fatalf("outside window=%d", n)
	}
}
func TestLiveLE15CheckpointResume(t *testing.T) {
	l := newLiveLocal(t, "ethereum")
	l.f.SetLatest(19)
	a := l.block(t, 7, 1)
	p := l.start(t, 1)
	l.f.PublishHead(a)
	l.waitCursor(t, 1)
	p.stop(t)
	l.checkpoint(t, 1, 7)
	if err := l.f.WaitForNoSubscribers(l.ctx); err != nil {
		t.Fatal(err)
	}
	l.f.SetLatest(20)
	b := l.block(t, 8, 1)
	q := l.start(t, 1)
	before := l.f.RequestCount("eth_getTransactionReceipt")
	l.f.PublishHead(b)
	l.waitCursor(t, 2)
	q.stop(t)
	l.checkpoint(t, 1, 8)
	if n := l.f.RequestCount("eth_getTransactionReceipt") - before; n != 1 {
		t.Fatalf("resume receipt requests=%d", n)
	}
	if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE chain_id=1"); n != 2 {
		t.Fatalf("resume rows=%d", n)
	}
}
func TestLiveLE16CursorPaginationRestartIdempotency(t *testing.T) {
	l := newLiveLocal(t, "ethereum")
	l.f.SetLatest(19)
	b := l.block(t, 7, 101)
	var cursors []int64
	// 101 events force the real binary worker's 100-row page into two commits.
	if err := l.db.Exec("CREATE TABLE live_cursor_history(sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,cursor bigint); CREATE FUNCTION live_cursor_history_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO live_cursor_history(cursor) VALUES(NEW.cursor); RETURN NEW; END$$; CREATE TRIGGER live_cursor_history_tr AFTER INSERT OR UPDATE ON chain_event_biz_cursors FOR EACH ROW EXECUTE FUNCTION live_cursor_history_fn()").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		l.db.Exec("DROP TABLE IF EXISTS live_cursor_history; DROP FUNCTION IF EXISTS live_cursor_history_fn() CASCADE")
	})
	p := l.start(t, 1)
	l.f.PublishHead(b)
	l.waitCursor(t, 101)
	p.stop(t)
	if err := l.db.Raw("SELECT cursor FROM live_cursor_history ORDER BY sequence").Scan(&cursors).Error; err != nil {
		t.Fatal(err)
	}
	if len(cursors) != 2 || cursors[0] >= cursors[1] {
		t.Fatalf("pagination commits=%v", cursors)
	}
	var before, after []struct {
		ID                int64
		BlockHash, TxHash string
		LogIndex          int
	}
	if err := l.db.Raw("SELECT id,block_hash,tx_hash,log_index FROM chain_raw_events ORDER BY id").Scan(&before).Error; err != nil {
		t.Fatal(err)
	}
	if len(before) != 101 || cursors[0] != before[99].ID || cursors[1] != before[100].ID {
		t.Fatalf("page cursor mismatch=%v", cursors)
	}
	businessBefore := l.snapshot(t)
	if err := l.f.WaitForNoSubscribers(l.ctx); err != nil {
		t.Fatal(err)
	}
	firstPageID, finalRawID := before[99].ID, before[100].ID
	// Keep every persisted raw event and derived transfer. Rewind only this
	// consumer after the first process has joined so a fresh worker must parse
	// and commit the already-consumed final page against existing natural keys.
	reset := l.db.WithContext(l.ctx).Exec("UPDATE chain_event_biz_cursors SET cursor=? WHERE biz='transaction' AND chain_family='evm' AND cursor=?", firstPageID, finalRawID)
	if reset.Error != nil {
		t.Fatal(reset.Error)
	}
	if reset.RowsAffected != 1 {
		t.Fatalf("cursor rewind affected=%d", reset.RowsAffected)
	}
	if n := l.count(t, "SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'"); n != firstPageID {
		t.Fatalf("rewound cursor=%d want=%d", n, firstPageID)
	}
	resetSequence := l.count(t, "SELECT MAX(sequence) FROM live_cursor_history")
	if n := l.count(t, "SELECT COUNT(*) FROM live_cursor_history"); n != 3 {
		t.Fatalf("first two pages plus rewind commits=%d", n)
	}
	q := l.start(t, 1)
	if q.cmd.Process.Pid == p.cmd.Process.Pid {
		t.Fatal("restart reused first process")
	}
	l.f.PublishHead(b)
	l.waitCursor(t, 101)
	q.stop(t)
	l.checkpoint(t, 1, 7)
	var replayCursors []int64
	if err := l.db.WithContext(l.ctx).Raw("SELECT cursor FROM live_cursor_history WHERE sequence>? ORDER BY sequence", resetSequence).Scan(&replayCursors).Error; err != nil {
		t.Fatal(err)
	}
	if len(replayCursors) != 1 {
		t.Fatalf("replayed final page commits=%v", replayCursors)
	}
	previous := firstPageID
	for _, cursor := range replayCursors {
		if cursor <= previous || cursor > finalRawID {
			t.Fatalf("restart cursor not monotonic: previous=%d cursor=%d final=%d", previous, cursor, finalRawID)
		}
		previous = cursor
	}
	if previous != finalRawID {
		t.Fatalf("replayed cursor=%d final raw=%d", previous, finalRawID)
	}
	if err := l.db.Raw("SELECT id,block_hash,tx_hash,log_index FROM chain_raw_events ORDER BY id").Scan(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("replay changed raw identity")
	}
	if !reflect.DeepEqual(businessBefore, l.snapshot(t)) {
		t.Fatal("replay changed persisted IDs or business fields")
	}
	if n := l.count(t, "SELECT COUNT(*) FROM live_cursor_history"); n != 4 {
		t.Fatalf("expected two initial pages, one rewind and one replay page; commits=%d", n)
	}
	if n := l.count(t, "SELECT COUNT(*) FROM chain_transfers WHERE source_type='EVENT_LOG'"); n != 101 {
		t.Fatalf("token replay rows=%d", n)
	}
	if n := l.count(t, "SELECT COUNT(DISTINCT (chain_id,block_hash,tx_hash,source_type,source_index,batch_index)) FROM chain_transfers WHERE source_type='EVENT_LOG'"); n != 101 {
		t.Fatalf("replayed token natural keys=%d", n)
	}
	if n := l.count(t, "SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'"); n != before[100].ID {
		t.Fatalf("restart cursor=%d", n)
	}
}
func TestLiveLE17MultiChainIsolation(t *testing.T) {
	l := newLiveLocal(t, "ethereum", "bsc")
	l.f.SetLatest(22) // Ethereum safe=10; BSC safe=7.
	for h := int64(7); h <= 10; h++ {
		l.block(t, h, 1)
	}
	p := l.start(t, 2)
	l.f.PublishHead(map[string]any{"number": "0x16"})
	l.waitCursor(t, 5)
	p.stop(t)
	l.checkpoint(t, 1, 10)
	l.checkpoint(t, 56, 7)
	for _, id := range []int64{1, 56} {
		if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE chain_id=? AND block_hash=? AND block_number=7 AND status='CANONICAL'", id, liveHash(107)); n != 1 {
			t.Fatalf("chain=%d shared block=%d", id, n)
		}
		for _, table := range []string{"chain_transactions", "chain_raw_events"} {
			if n := l.count(t, "SELECT COUNT(*) FROM "+table+" WHERE chain_id=? AND block_hash=? AND tx_hash=? AND chain_status='ACTIVE'", id, liveHash(107), liveHash(207)); n != 1 {
				t.Fatalf("chain=%d table=%s identity rows=%d", id, table, n)
			}
		}
		if n := l.count(t, "SELECT COUNT(*) FROM chain_transfers WHERE chain_id=? AND block_hash=? AND tx_hash=?", id, liveHash(107), liveHash(207)); n != 2 {
			t.Fatalf("chain=%d transfer rows=%d", id, n)
		}
		if n := l.count(t, "SELECT COUNT(*) FROM chain_raw_events WHERE chain_id=? AND block_hash=? AND tx_hash=? AND log_index=0 AND contract_address=?", id, liveHash(107), liveHash(207), liveAddress(3)); n != 1 {
			t.Fatalf("chain=%d shared log identity=%d", id, n)
		}
	}
	for _, table := range []string{"chain_checkpoints", "chain_blocks", "chain_transactions", "chain_raw_events", "chain_transfers"} {
		if n := l.count(t, "SELECT COUNT(*) FROM "+table+" WHERE chain_id NOT IN (1,56)"); n != 0 {
			t.Fatalf("disabled chain %s=%d", table, n)
		}
	}
}
func TestLiveLE18DatabaseUniquenessSingleRowFact(t *testing.T) {
	l := newLiveLocal(t, "ethereum")
	l.f.SetLatest(19)
	b := l.block(t, 7, 2)
	p := l.start(t, 1)
	l.f.PublishHead(b)
	l.waitCursor(t, 2)
	p.stop(t)
	if err := l.f.WaitForNoSubscribers(l.ctx); err != nil {
		t.Fatal(err)
	}
	before := l.snapshot(t)
	var checkpointBefore postgre.ChainCheckpoint
	if err := l.db.WithContext(l.ctx).Where("chain_id = ?", 1).Take(&checkpointBefore).Error; err != nil {
		t.Fatal(err)
	}
	if checkpointBefore.ChainID != 1 || checkpointBefore.LastProcessedHeight != 7 || checkpointBefore.LastProcessedHash != liveHash(107) {
		t.Fatalf("checkpoint before replay=%+v", checkpointBefore)
	}
	cursorBefore := l.count(t, "SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'")
	l.replayLE18(t)
	if !reflect.DeepEqual(before, l.snapshot(t)) {
		t.Fatal("canonical replay changed natural-key row IDs or contents")
	}
	var checkpointAfter postgre.ChainCheckpoint
	if err := l.db.WithContext(l.ctx).Where("chain_id = ?", 1).Take(&checkpointAfter).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(checkpointBefore, checkpointAfter) {
		t.Fatalf("canonical replay changed checkpoint: before=%+v after=%+v", checkpointBefore, checkpointAfter)
	}
	if cursorAfter := l.count(t, "SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'"); cursorAfter != cursorBefore {
		t.Fatalf("canonical replay changed cursor: before=%d after=%d", cursorBefore, cursorAfter)
	}
	for _, tc := range []struct {
		table string
		want  int64
	}{{"chain_blocks", 1}, {"chain_transactions", 1}, {"chain_raw_events", 2}, {"chain_transfers", 3}} {
		if n := l.count(t, "SELECT COUNT(*) FROM "+tc.table+" WHERE chain_id=1 AND block_hash=?", liveHash(107)); n != tc.want {
			t.Fatalf("replayed %s=%d want=%d", tc.table, n, tc.want)
		}
	}
	if n := l.count(t, "SELECT COUNT(DISTINCT log_index) FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", liveHash(107)); n != 2 {
		t.Fatalf("log identities=%d", n)
	}
	if n := l.count(t, "SELECT COUNT(DISTINCT (source_type,source_index,batch_index)) FROM chain_transfers WHERE chain_id=1 AND block_hash=?", liveHash(107)); n != 3 {
		t.Fatalf("transfer natural identities=%d", n)
	}
}
func TestLiveLE21ReorgEvidenceBoundary(t *testing.T) {
	l := newLiveLocal(t, "ethereum")
	l.f.SetLatest(20)
	l.block(t, 7, 1)
	b := l.block(t, 8, 1)
	p := l.start(t, 1)
	l.f.PublishHead(b)
	l.waitCursor(t, 2)
	p.stop(t)
	l.checkpoint(t, 1, 8)
	if n := l.count(t, "SELECT COUNT(*) FROM chain_reorgs"); n != 0 {
		t.Fatalf("canonical run reorg records=%d", n)
	}
	if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE chain_id=1 AND status='CANONICAL' AND reorg_id IS NULL AND orphaned_at IS NULL AND block_hash IN (?,?)", liveHash(107), liveHash(108)); n != 2 {
		t.Fatalf("canonical block provenance=%d", n)
	}
	for _, table := range []string{"chain_transactions", "chain_raw_events", "chain_transfers"} {
		want := int64(2)
		if table == "chain_transfers" {
			want = 4
		}
		if n := l.count(t, "SELECT COUNT(*) FROM "+table+" WHERE chain_id=1 AND chain_status='ACTIVE' AND reorg_id IS NULL AND reverted_at IS NULL AND block_hash IN (?,?)", liveHash(107), liveHash(108)); n != want {
			t.Fatalf("canonical %s provenance=%d", table, n)
		}
	}
}
