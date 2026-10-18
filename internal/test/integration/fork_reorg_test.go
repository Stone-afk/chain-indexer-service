//go:build e2e

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"github.com/jackc/pgx/v5"
	"gorm.io/gorm"
)

// Only process lifecycle, committed progress and database reads are shared.
// Branch timelines and all business assertions remain in their owning tests.
type forkService struct {
	process *runtimeProcess
	app     string
	joined  bool
}

func startForkService(t *testing.T, ctx context.Context, f *testioc.ForkRPCFixture, dsn string) *forkService {
	t.Helper()
	app := fmt.Sprintf("fork-%d", time.Now().UnixNano())
	serviceDSN := dsn + " application_name=" + app
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("parse test database URL")
		}
		q := u.Query()
		q.Set("application_name", app)
		u.RawQuery = q.Encode()
		serviceDSN = u.String()
	}
	chains := map[string]config.ChainConfig{}
	for _, c := range []struct {
		name        string
		id, confirm int64
	}{
		{"ethereum", 1, 12}, {"bsc", 56, 15}, {"polygon", 137, 100}, {"arbitrum", 42161, 20}, {"optimism", 10, 20}, {"base", 8453, 20}, {"avalanche", 43114, 15},
	} {
		chains[c.name] = config.ChainConfig{Enabled: c.name == "ethereum", ChainID: c.id, ConfirmBlocks: c.confirm, StartBlock: 0, HTTPURL: f.HTTPURL(), WSURL: f.WSURL()}
	}
	path := testioc.WriteServiceConfig(t, config.DatabaseConfig{DSN: serviceDSN}, chains)
	cmd := exec.Command(buildBinary(t, "./cmd/chain-indexer"), "--config", path)
	cmd.Dir = repositoryRoot(t)
	cmd.Env = commandEnvironment()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &runtimeProcess{cmd: cmd, stdout: &runtimeBuffer{}, stderr: &runtimeBuffer{}, readers: make(chan error, 1), waitDone: make(chan struct{})}
	cmd.Stderr = p.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal("start fork service")
	}
	service := &forkService{process: p, app: app}
	t.Cleanup(func() {
		if !service.joined {
			service.Stop(t)
		}
	})
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		announced := false
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = p.stdout.Write([]byte(line + "\n"))
			if !announced && strings.Contains(line, "service ready") {
				announced = true
				close(ready)
			}
		}
		p.readers <- scanner.Err()
	}()
	go func() { p.waitErr = cmd.Wait(); close(p.waitDone) }()
	select {
	case <-ready:
	case <-p.waitDone:
		_, _ = p.drain(context.Background(), false)
		service.joined = true
		t.Fatal("fork service exited before ready")
	case <-ctx.Done():
		service.Stop(t)
		t.Fatal("fork service readiness deadline")
	}
	if err := f.WaitForSubscribers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	return service
}
func (s *forkService) Stop(t *testing.T) {
	t.Helper()
	if err := s.stop(); err != nil {
		t.Errorf("join fork service: %v", err)
	}
}
func (s *forkService) stop() error {
	if s.joined {
		return nil
	}
	_, err := s.process.drain(context.Background(), true)
	s.joined = true
	return err
}
func (s *forkService) Failed(t *testing.T, ctx context.Context) string {
	t.Helper()
	select {
	case <-s.process.waitDone:
	case <-ctx.Done():
		s.Stop(t)
		t.Fatal("fork service failure deadline")
	}
	output, err := s.process.drain(context.Background(), false)
	s.joined = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("expected service failure, got %v", err)
	}
	return output
}

type forkProgress struct{ conn *pgx.Conn }

// Dedicated credentials and port from scripts/test_docker_compose.yml.
const forkComposeDSN = "host=127.0.0.1 port=15432 user=postgres password=postgres dbname=chain_indexer_e2e sslmode=disable"

func startForkDatabase(t *testing.T, ctx context.Context) (*gorm.DB, string, *forkProgress) {
	t.Helper()
	db := testioc.InitDB(t, ctx, config.DatabaseConfig{DSN: forkComposeDSN})
	testioc.ResetSchema(t, db)
	if _, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, forkComposeDSN)
	if err != nil {
		t.Fatal("connect fork progress observer")
	}
	if _, err := conn.Exec(ctx, `LISTEN fork_progress;
 CREATE FUNCTION test_fork_progress() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE h bigint; bh text;
 BEGIN
 SELECT last_processed_height,last_processed_hash INTO h,bh FROM chain_checkpoints WHERE chain_id=1;
 PERFORM pg_notify('fork_progress',json_build_object('height',h,'hash',bh)::text);
 RETURN NEW;
 END $$;
 CREATE TRIGGER test_fork_checkpoint AFTER INSERT OR UPDATE ON chain_checkpoints FOR EACH ROW EXECUTE FUNCTION test_fork_progress();
 CREATE TRIGGER test_fork_cursor AFTER INSERT OR UPDATE ON chain_event_biz_cursors FOR EACH ROW EXECUTE FUNCTION test_fork_progress()`); err != nil {
		_ = conn.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := conn.Exec(cleanup, `DROP TRIGGER test_fork_checkpoint ON chain_checkpoints; DROP TRIGGER test_fork_cursor ON chain_event_biz_cursors; DROP FUNCTION test_fork_progress()`)
		if err != nil {
			t.Error("remove fork progress triggers")
		}
		if err := conn.Close(cleanup); err != nil {
			t.Error("close fork progress observer")
		}
	})
	return db.WithContext(ctx), forkComposeDSN, &forkProgress{conn: conn}
}

// Notifications carry the committed checkpoint identity. They only wake this
// query: neither an unrelated height/hash nor an earlier scanner commit can
// satisfy a wait for the worker's final cursor and token rows.
func waitForkState(t *testing.T, ctx context.Context, db *gorm.DB, p *forkProgress, height int64, hash string, consumed bool) {
	t.Helper()
	for {
		var cp []postgre.ChainCheckpoint
		if err := db.WithContext(ctx).Where("chain_id=1").Find(&cp).Error; err != nil {
			t.Fatal(err)
		}
		if len(cp) == 1 && cp[0].LastProcessedHeight == height && cp[0].LastProcessedHash == hash {
			if !consumed {
				return
			}
			var state struct{ RawID, Cursor, RawCount, TokenCount int64 }
			if err := db.WithContext(ctx).Raw(`SELECT COALESCE((SELECT MAX(id) FROM chain_raw_events),0) raw_id,
    COALESCE((SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'),0) cursor,
    (SELECT COUNT(*) FROM chain_raw_events) raw_count,
    (SELECT COUNT(*) FROM chain_transfers WHERE source_type='EVENT_LOG') token_count`).Scan(&state).Error; err != nil {
				t.Fatal(err)
			}
			if state.RawID > 0 && state.Cursor == state.RawID && state.RawCount == state.TokenCount {
				return
			}
		}
		for {
			notification, err := p.conn.WaitForNotification(ctx)
			if err != nil {
				t.Fatalf("wait checkpoint (%d,%s): %v", height, hash, err)
			}
			var identity struct {
				Height *int64
				Hash   *string
			}
			if err := json.Unmarshal([]byte(notification.Payload), &identity); err != nil {
				t.Fatal("invalid progress identity")
			}
			if identity.Height != nil && identity.Hash != nil && *identity.Height == height && *identity.Hash == hash {
				break
			}
		}
	}
}
func waitForkRawLock(ctx context.Context, db *gorm.DB, app string, blocker int32) (int32, error) {
	// SG-02 bounds this catalog poll to 100 observations and two seconds; the
	// scenario context may stop it sooner.
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var lastPIDs []int32
	for attempt := 0; attempt < 100; attempt++ {
		var pids []int32
		if err := db.WithContext(waitCtx).Raw(`SELECT pid FROM pg_stat_activity WHERE application_name=? AND state='active'
   AND wait_event_type='Lock' AND ?=ANY(pg_blocking_pids(pid))
   AND query LIKE '%chain_raw_events%' AND query LIKE '%FOR UPDATE%'`, app, blocker).Scan(&pids).Error; err != nil {
			return 0, fmt.Errorf("observe fork row-lock waiter: %w", err)
		}
		lastPIDs = pids
		if len(pids) == 1 {
			if err := testioc.ObserveRowLockWait(waitCtx, db, blocker, pids[0]); err != nil {
				return 0, err
			}
			return pids[0], nil
		}
		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			return 0, fmt.Errorf("fork row-lock wait ended (matching backends=%v): %w", pids, waitCtx.Err())
		}
	}
	return 0, fmt.Errorf("fork row-lock wait ended after 100 attempts (matching backends=%v)", lastPIDs)
}

type forkCursor struct {
	ID               int64
	Biz, ChainFamily string
	Cursor           int64
}
type forkState struct {
	Checkpoints   []postgre.ChainCheckpoint
	Blocks        []postgre.ChainBlock
	Transactions  []postgre.ChainTransaction
	Raw           []postgre.ChainRawEvent
	Native, Token []postgre.ChainTransfer
	Reorgs        []postgre.ChainReorg
	Cursors       []forkCursor
}

func queryForkState(t *testing.T, db *gorm.DB) forkState {
	t.Helper()
	s := forkState{}
	for _, q := range []struct {
		table, order, filter string
		dst                  any
	}{
		{"chain_checkpoints", "chain_id", "", &s.Checkpoints},
		{"chain_blocks", "block_number, block_hash", "", &s.Blocks},
		{"chain_transactions", "block_number, block_hash, tx_hash", "", &s.Transactions},
		{"chain_raw_events", "id", "", &s.Raw},
		{"chain_transfers", "id", "source_type='TRANSACTION_VALUE'", &s.Native},
		{"chain_transfers", "id", "source_type='EVENT_LOG'", &s.Token},
		{"chain_reorgs", "reorg_id", "", &s.Reorgs},
		{"chain_event_biz_cursors", "id", "", &s.Cursors},
	} {
		query := db.Table(q.table).Order(q.order)
		if q.filter != "" {
			query = query.Where(q.filter)
		}
		if err := query.Find(q.dst).Error; err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// LE-22 catches rollback of an unconfirmed suffix across the confirmation boundary.
func TestLocalForkConfirmationBoundary(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, dsn, progress := startForkDatabase(t, ctx)
	f := testioc.InitForkRPCFixture(t)
	h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

	a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
	for n := int64(0); n <= 13; n++ {
		parent := h(0)
		if n > 0 {
			parent = h(0x2200 + n - 1)
		}
		a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2200 + n), Parent: parent, Tx: h(0x2210 + n)})
		block := a[n]
		if n > 1 {
			block.Hash, block.Tx = h(0x2240+n), h(0x2260+n)
			if n > 2 {
				block.Parent = h(0x2240 + n - 1)
			}
		}
		b = append(b, block)
	}
	f.SetBranch("confirmed-A", a)
	f.SetBranch("shallow-B", b)
	f.Select("confirmed-A", 0)
	service := startForkService(t, ctx, f, dsn)
	f.Switch("confirmed-A", 13)
	waitForkState(t, ctx, db, progress, 1, a[1].Hash, true)
	before := queryForkState(t, db)
	checkpointRead := f.Gate("shallow-B", 1, false)
	t.Cleanup(checkpointRead.Release)
	f.Switch("shallow-B", 13)
	if err := checkpointRead.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	checkpointRead.Release()
	// A second serial checkpoint request proves the previous head handler returned.
	next := f.Gate("shallow-B", 1, false)
	t.Cleanup(next.Release)
	f.Publish("shallow-B", 12)
	if err := next.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	next.Release()
	service.Stop(t)
	got := queryForkState(t, db)
	if !reflect.DeepEqual(before, got) {
		t.Fatal("unconfirmed A/B suffix changed confirmed facts/checkpoint/cursor")
	}
	want := []struct {
		block    testioc.ForkBlock
		reverted bool
	}{{a[0], false}, {a[1], false}}

	// LE-22: compare every persisted identity to this scenario's frozen manifest.
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 1 || got.Checkpoints[0].LastProcessedHash != a[1].Hash {
		t.Fatalf("checkpoint: %+v", got.Checkpoints)
	}
	if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
		t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
	}
	if len(got.Reorgs) != 0 {
		t.Fatalf("unexpected reorg: %+v", got.Reorgs)
	}
	reorgID := ""
	blocks := map[string]postgre.ChainBlock{}
	txs := map[string]postgre.ChainTransaction{}
	raws := map[string]postgre.ChainRawEvent{}
	natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
	for _, v := range got.Blocks {
		if _, ok := blocks[v.BlockHash]; ok {
			t.Fatal("duplicate block natural key")
		}
		blocks[v.BlockHash] = v
	}
	for _, v := range got.Transactions {
		if _, ok := txs[v.BlockHash]; ok {
			t.Fatal("duplicate transaction natural key")
		}
		txs[v.BlockHash] = v
	}
	var lastID int64
	for _, v := range got.Raw {
		if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
			t.Fatal("raw identity/ID ordering")
		}
		raws[v.BlockHash] = v
		lastID = v.ID
	}
	for _, v := range got.Native {
		if _, ok := natives[v.BlockHash]; ok {
			t.Fatal("duplicate native natural key")
		}
		natives[v.BlockHash] = v
	}
	for _, v := range got.Token {
		if _, ok := tokens[v.BlockHash]; ok {
			t.Fatal("duplicate token natural key")
		}
		tokens[v.BlockHash] = v
	}
	for _, w := range want {
		b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
		blockStatus, chainStatus := "CANONICAL", "ACTIVE"
		if w.reverted {
			blockStatus, chainStatus = "ORPHANED", "REVERTED"
		}
		if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
			t.Fatalf("block identity/status: %+v", b)
		}
		if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
			t.Fatalf("transaction identity/business: %+v", tx)
		}
		if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
			t.Fatalf("raw identity/business: %+v", raw)
		}
		for _, tr := range []postgre.ChainTransfer{native, token} {
			if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
				t.Fatalf("transfer identity/business: %+v", tr)
			}
		}
		if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
			t.Fatalf("native/token source linkage: %+v %+v", native, token)
		}
		if w.reverted {
			if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
				t.Fatalf("orphan metadata: %+v", b)
			}
			for _, m := range []struct {
				ID *string
				At *int64
			}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
				if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
					t.Fatal("old fork metadata is not atomic/shared")
				}
			}
		} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
			t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
		}
	}
	if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
		t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
	}
	for _, cursor := range before.Cursors {
		if got.Cursors[0].Cursor < cursor.Cursor {
			t.Fatal("cursor regressed")
		}
	}
	// Compare full immutable snapshots, allowing only already-asserted state metadata.
	for _, old := range before.Blocks {
		v := blocks[old.BlockHash]
		old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("block immutable data changed")
		}
	}
	for _, old := range before.Transactions {
		v := txs[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("transaction immutable data changed")
		}
	}
	for _, old := range before.Raw {
		v := raws[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("raw row ID/business data changed")
		}
	}
	for _, old := range before.Native {
		v := natives[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("native row ID/business data changed")
		}
	}
	for _, old := range before.Token {
		v := tokens[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("token row ID/business data changed")
		}
	}

	for _, block := range b[2:] {
		if _, ok := blocks[block.Hash]; ok {
			t.Fatal("unconfirmed B block persisted")
		}
	}
}

// LE-23 catches same-height hash replacement without atomic rollback and replay.
func TestLocalForkSameHeightRollbackReplay(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, dsn, progress := startForkDatabase(t, ctx)
	f := testioc.InitForkRPCFixture(t)
	h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

	a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
	for n := int64(0); n <= 14; n++ {
		parent := h(0)
		if n > 0 {
			parent = h(0x2300 + n - 1)
		}
		a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2300 + n), Parent: parent, Tx: h(0x2320 + n)})
		block := a[n]
		if n > 0 {
			block.Hash, block.Tx = h(0x2340+n), h(0x2360+n)
			if n > 1 {
				block.Parent = h(0x2340 + n - 1)
			}
		}
		b = append(b, block)
	}
	f.SetBranch("same-A", a)
	f.SetBranch("same-B", b)
	f.Select("same-A", 0)
	service := startForkService(t, ctx, f, dsn)
	f.Switch("same-A", 14)
	waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
	before := queryForkState(t, db)
	replay := f.Gate("same-B", 1, true)
	t.Cleanup(replay.Release)
	f.Switch("same-B", 14)
	if err := replay.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	rolled := queryForkState(t, db)
	if len(rolled.Checkpoints) != 1 || rolled.Checkpoints[0].LastProcessedHeight != 0 || rolled.Checkpoints[0].LastProcessedHash != a[0].Hash || len(rolled.Reorgs) != 1 || rolled.Reorgs[0].Status != "COMPLETED" || len(rolled.Blocks) != 3 {
		t.Fatalf("rollback commit before replay: %+v", rolled)
	}
	for _, block := range rolled.Blocks {
		if block.BlockNumber > 0 && (block.Status != "ORPHANED" || block.ReorgID == nil || block.OrphanedAt == nil) {
			t.Fatal("A suffix not invalidated before B replay")
		}
	}
	replay.Release()
	waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
	service.Stop(t)
	got := queryForkState(t, db)

	want := []struct {
		block    testioc.ForkBlock
		reverted bool
	}{}
	for _, block := range a[:1] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, false})
	}
	for _, block := range a[1:3] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, true})
	}
	for _, block := range b[1:3] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, false})
	}

	// LE-23: compare every persisted identity to this scenario's frozen manifest.
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
		t.Fatalf("checkpoint: %+v", got.Checkpoints)
	}
	if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
		t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
	}

	if len(got.Reorgs) != 1 {
		t.Fatalf("reorg natural count=%d", len(got.Reorgs))
	}
	r := got.Reorgs[0]
	if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
		t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
	}
	reorgID := r.ReorgID

	blocks := map[string]postgre.ChainBlock{}
	txs := map[string]postgre.ChainTransaction{}
	raws := map[string]postgre.ChainRawEvent{}
	natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
	for _, v := range got.Blocks {
		if _, ok := blocks[v.BlockHash]; ok {
			t.Fatal("duplicate block natural key")
		}
		blocks[v.BlockHash] = v
	}
	for _, v := range got.Transactions {
		if _, ok := txs[v.BlockHash]; ok {
			t.Fatal("duplicate transaction natural key")
		}
		txs[v.BlockHash] = v
	}
	var lastID int64
	for _, v := range got.Raw {
		if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
			t.Fatal("raw identity/ID ordering")
		}
		raws[v.BlockHash] = v
		lastID = v.ID
	}
	for _, v := range got.Native {
		if _, ok := natives[v.BlockHash]; ok {
			t.Fatal("duplicate native natural key")
		}
		natives[v.BlockHash] = v
	}
	for _, v := range got.Token {
		if _, ok := tokens[v.BlockHash]; ok {
			t.Fatal("duplicate token natural key")
		}
		tokens[v.BlockHash] = v
	}
	for _, w := range want {
		b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
		blockStatus, chainStatus := "CANONICAL", "ACTIVE"
		if w.reverted {
			blockStatus, chainStatus = "ORPHANED", "REVERTED"
		}
		if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
			t.Fatalf("block identity/status: %+v", b)
		}
		if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
			t.Fatalf("transaction identity/business: %+v", tx)
		}
		if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
			t.Fatalf("raw identity/business: %+v", raw)
		}
		for _, tr := range []postgre.ChainTransfer{native, token} {
			if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
				t.Fatalf("transfer identity/business: %+v", tr)
			}
		}
		if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
			t.Fatalf("native/token source linkage: %+v %+v", native, token)
		}
		if w.reverted {
			if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
				t.Fatalf("orphan metadata: %+v", b)
			}
			for _, m := range []struct {
				ID *string
				At *int64
			}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
				if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
					t.Fatal("old fork metadata is not atomic/shared")
				}
			}
		} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
			t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
		}
	}
	if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
		t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
	}
	for _, cursor := range before.Cursors {
		if got.Cursors[0].Cursor < cursor.Cursor {
			t.Fatal("cursor regressed")
		}
	}
	// Compare full immutable snapshots, allowing only already-asserted state metadata.
	for _, old := range before.Blocks {
		v := blocks[old.BlockHash]
		old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("block immutable data changed")
		}
	}
	for _, old := range before.Transactions {
		v := txs[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("transaction immutable data changed")
		}
	}
	for _, old := range before.Raw {
		v := raws[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("raw row ID/business data changed")
		}
	}
	for _, old := range before.Native {
		v := natives[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("native row ID/business data changed")
		}
	}
	for _, old := range before.Token {
		v := tokens[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("token row ID/business data changed")
		}
	}

}

// LE-24 catches ancestry searches that stop before the real, older common block.
func TestLocalForkDeepAncestorBeforeCheckpoint(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, dsn, progress := startForkDatabase(t, ctx)
	f := testioc.InitForkRPCFixture(t)
	h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

	a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
	for n := int64(0); n <= 16; n++ {
		parent := h(0)
		if n > 0 {
			parent = h(0x2400 + n - 1)
		}
		a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2400 + n), Parent: parent, Tx: h(0x2420 + n)})
	}
	for n := int64(0); n <= 18; n++ {
		if n <= 1 {
			b = append(b, a[n])
			continue
		}
		parent := a[1].Hash
		if n > 2 {
			parent = h(0x2440 + n - 1)
		}
		b = append(b, testioc.ForkBlock{Height: n, Hash: h(0x2440 + n), Parent: parent, Tx: h(0x2460 + n)})
	}
	f.SetBranch("deep-A", a)
	f.SetBranch("longer-B", b)
	f.Select("deep-A", 0)
	service := startForkService(t, ctx, f, dsn)
	f.Switch("deep-A", 16)
	waitForkState(t, ctx, db, progress, 4, a[4].Hash, true)
	before := queryForkState(t, db)
	replay := f.Gate("longer-B", 2, true)
	t.Cleanup(replay.Release)
	f.Switch("longer-B", 18)
	if err := replay.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	rolled := queryForkState(t, db)
	if len(rolled.Checkpoints) != 1 || rolled.Checkpoints[0].LastProcessedHeight != 1 || rolled.Checkpoints[0].LastProcessedHash != a[1].Hash || len(rolled.Reorgs) != 1 || rolled.Reorgs[0].AncestorHeight != 1 || rolled.Reorgs[0].ReorgDepth != 3 {
		t.Fatalf("deep ancestor commit: %+v", rolled)
	}
	if f.Requests("longer-B", 3, false) != 1 || f.Requests("longer-B", 2, false) != 1 || f.Requests("longer-B", 1, false) != 1 {
		t.Fatal("did not search every ancestor height")
	}
	replay.Release()
	waitForkState(t, ctx, db, progress, 6, b[6].Hash, true)
	service.Stop(t)
	got := queryForkState(t, db)

	want := []struct {
		block    testioc.ForkBlock
		reverted bool
	}{}
	for _, block := range a[:2] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, false})
	}
	for _, block := range a[2:5] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, true})
	}
	for _, block := range b[2:7] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, false})
	}

	// LE-24: compare every persisted identity to this scenario's frozen manifest.
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 6 || got.Checkpoints[0].LastProcessedHash != b[6].Hash {
		t.Fatalf("checkpoint: %+v", got.Checkpoints)
	}
	if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
		t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
	}

	if len(got.Reorgs) != 1 {
		t.Fatalf("reorg natural count=%d", len(got.Reorgs))
	}
	r := got.Reorgs[0]
	if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 1 || r.AncestorHash != a[1].Hash || r.OldHeadHeight != 4 || r.OldHeadHash != a[4].Hash || r.NewHeadHeight != 18 || r.NewHeadHash != b[18].Hash || r.ReorgDepth != 3 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
		t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
	}
	reorgID := r.ReorgID

	blocks := map[string]postgre.ChainBlock{}
	txs := map[string]postgre.ChainTransaction{}
	raws := map[string]postgre.ChainRawEvent{}
	natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
	for _, v := range got.Blocks {
		if _, ok := blocks[v.BlockHash]; ok {
			t.Fatal("duplicate block natural key")
		}
		blocks[v.BlockHash] = v
	}
	for _, v := range got.Transactions {
		if _, ok := txs[v.BlockHash]; ok {
			t.Fatal("duplicate transaction natural key")
		}
		txs[v.BlockHash] = v
	}
	var lastID int64
	for _, v := range got.Raw {
		if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
			t.Fatal("raw identity/ID ordering")
		}
		raws[v.BlockHash] = v
		lastID = v.ID
	}
	for _, v := range got.Native {
		if _, ok := natives[v.BlockHash]; ok {
			t.Fatal("duplicate native natural key")
		}
		natives[v.BlockHash] = v
	}
	for _, v := range got.Token {
		if _, ok := tokens[v.BlockHash]; ok {
			t.Fatal("duplicate token natural key")
		}
		tokens[v.BlockHash] = v
	}
	for _, w := range want {
		b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
		blockStatus, chainStatus := "CANONICAL", "ACTIVE"
		if w.reverted {
			blockStatus, chainStatus = "ORPHANED", "REVERTED"
		}
		if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
			t.Fatalf("block identity/status: %+v", b)
		}
		if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
			t.Fatalf("transaction identity/business: %+v", tx)
		}
		if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
			t.Fatalf("raw identity/business: %+v", raw)
		}
		for _, tr := range []postgre.ChainTransfer{native, token} {
			if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
				t.Fatalf("transfer identity/business: %+v", tr)
			}
		}
		if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
			t.Fatalf("native/token source linkage: %+v %+v", native, token)
		}
		if w.reverted {
			if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
				t.Fatalf("orphan metadata: %+v", b)
			}
			for _, m := range []struct {
				ID *string
				At *int64
			}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
				if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
					t.Fatal("old fork metadata is not atomic/shared")
				}
			}
		} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
			t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
		}
	}
	if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
		t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
	}
	for _, cursor := range before.Cursors {
		if got.Cursors[0].Cursor < cursor.Cursor {
			t.Fatal("cursor regressed")
		}
	}
	// Compare full immutable snapshots, allowing only already-asserted state metadata.
	for _, old := range before.Blocks {
		v := blocks[old.BlockHash]
		old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("block immutable data changed")
		}
	}
	for _, old := range before.Transactions {
		v := txs[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("transaction immutable data changed")
		}
	}
	for _, old := range before.Raw {
		v := raws[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("raw row ID/business data changed")
		}
	}
	for _, old := range before.Native {
		v := natives[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("native row ID/business data changed")
		}
	}
	for _, old := range before.Token {
		v := tokens[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("token row ID/business data changed")
		}
	}

}

// LE-25 catches missing old-fork tokens when rollback races the real worker.
func TestLocalForkWorkerInterleavings(t *testing.T) {
	t.Run("worker-before-rollback", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, dsn, progress := startForkDatabase(t, ctx)
		f := testioc.InitForkRPCFixture(t)
		h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

		a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
		for n := int64(0); n <= 14; n++ {
			parent := h(0)
			if n > 0 {
				parent = h(0x2500 + n - 1)
			}
			a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2500 + n), Parent: parent, Tx: h(0x2520 + n)})
			block := a[n]
			if n > 0 {
				block.Hash, block.Tx = h(0x2540+n), h(0x2560+n)
				if n > 1 {
					block.Parent = h(0x2540 + n - 1)
				}
			}
			b = append(b, block)
		}
		f.SetBranch("consumed-A", a)
		f.SetBranch("consumed-B", b)
		f.Select("consumed-A", 0)
		service := startForkService(t, ctx, f, dsn)
		f.Switch("consumed-A", 14)
		waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
		before := queryForkState(t, db)
		if len(before.Token) != 3 || len(before.Cursors) != 1 || before.Cursors[0].Cursor != before.Raw[2].ID {
			t.Fatal("worker-before precondition requires committed A token/cursor")
		}
		f.Switch("consumed-B", 14)
		waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
		service.Stop(t)
		got := queryForkState(t, db)

		want := []struct {
			block    testioc.ForkBlock
			reverted bool
		}{}
		for _, block := range a[:1] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}
		for _, block := range a[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, true})
		}
		for _, block := range b[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}

		// LE-25 worker-before: compare every persisted identity to this scenario's frozen manifest.
		if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
			t.Fatalf("checkpoint: %+v", got.Checkpoints)
		}
		if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
			t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
		}

		if len(got.Reorgs) != 1 {
			t.Fatalf("reorg natural count=%d", len(got.Reorgs))
		}
		r := got.Reorgs[0]
		if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
			t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
		}
		reorgID := r.ReorgID

		blocks := map[string]postgre.ChainBlock{}
		txs := map[string]postgre.ChainTransaction{}
		raws := map[string]postgre.ChainRawEvent{}
		natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
		for _, v := range got.Blocks {
			if _, ok := blocks[v.BlockHash]; ok {
				t.Fatal("duplicate block natural key")
			}
			blocks[v.BlockHash] = v
		}
		for _, v := range got.Transactions {
			if _, ok := txs[v.BlockHash]; ok {
				t.Fatal("duplicate transaction natural key")
			}
			txs[v.BlockHash] = v
		}
		var lastID int64
		for _, v := range got.Raw {
			if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
				t.Fatal("raw identity/ID ordering")
			}
			raws[v.BlockHash] = v
			lastID = v.ID
		}
		for _, v := range got.Native {
			if _, ok := natives[v.BlockHash]; ok {
				t.Fatal("duplicate native natural key")
			}
			natives[v.BlockHash] = v
		}
		for _, v := range got.Token {
			if _, ok := tokens[v.BlockHash]; ok {
				t.Fatal("duplicate token natural key")
			}
			tokens[v.BlockHash] = v
		}
		for _, w := range want {
			b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
			blockStatus, chainStatus := "CANONICAL", "ACTIVE"
			if w.reverted {
				blockStatus, chainStatus = "ORPHANED", "REVERTED"
			}
			if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
				t.Fatalf("block identity/status: %+v", b)
			}
			if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
				t.Fatalf("transaction identity/business: %+v", tx)
			}
			if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
				t.Fatalf("raw identity/business: %+v", raw)
			}
			for _, tr := range []postgre.ChainTransfer{native, token} {
				if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
					t.Fatalf("transfer identity/business: %+v", tr)
				}
			}
			if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
				t.Fatalf("native/token source linkage: %+v %+v", native, token)
			}
			if w.reverted {
				if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
					t.Fatalf("orphan metadata: %+v", b)
				}
				for _, m := range []struct {
					ID *string
					At *int64
				}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
					if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
						t.Fatal("old fork metadata is not atomic/shared")
					}
				}
			} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
				t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
			}
		}
		if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
			t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
		}
		for _, cursor := range before.Cursors {
			if got.Cursors[0].Cursor < cursor.Cursor {
				t.Fatal("cursor regressed")
			}
		}
		// Compare full immutable snapshots, allowing only already-asserted state metadata.
		for _, old := range before.Blocks {
			v := blocks[old.BlockHash]
			old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("block immutable data changed")
			}
		}
		for _, old := range before.Transactions {
			v := txs[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("transaction immutable data changed")
			}
		}
		for _, old := range before.Raw {
			v := raws[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("raw row ID/business data changed")
			}
		}
		for _, old := range before.Native {
			v := natives[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("native row ID/business data changed")
			}
		}
		for _, old := range before.Token {
			v := tokens[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("token row ID/business data changed")
			}
		}

	})
	t.Run("worker-after-rollback", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, dsn, progress := startForkDatabase(t, ctx)
		f := testioc.InitForkRPCFixture(t)
		h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

		a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
		for n := int64(0); n <= 14; n++ {
			parent := h(0)
			if n > 0 {
				parent = h(0x2580 + n - 1)
			}
			a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2580 + n), Parent: parent, Tx: h(0x25a0 + n)})
			block := a[n]
			if n > 0 {
				block.Hash, block.Tx = h(0x25c0+n), h(0x25e0+n)
				if n > 1 {
					block.Parent = h(0x25c0 + n - 1)
				}
			}
			b = append(b, block)
		}
		f.SetBranch("pending-A", a)
		f.SetBranch("pending-B", b)
		f.Select("pending-A", 0)
		// This table lock stops the real worker before its initial cursor read.
		// Scanner canonical batches and reorg never touch this cursor table.
		workerGate := db.WithContext(ctx).Begin()
		if workerGate.Error != nil {
			t.Fatal(workerGate.Error)
		}
		t.Cleanup(func() { workerGate.Rollback() })
		if err := workerGate.Exec("LOCK TABLE chain_event_biz_cursors IN ACCESS EXCLUSIVE MODE").Error; err != nil {
			t.Fatal(err)
		}
		service := startForkService(t, ctx, f, dsn)
		f.Switch("pending-A", 14)
		waitForkState(t, ctx, db, progress, 2, a[2].Hash, false)
		// The blocker session can read its own locked cursor table. Capture all
		// immutable A facts here, while the worker still cannot read its cursor.
		before := queryForkState(t, workerGate)
		pending := before.Raw
		if len(before.Cursors) != 0 || len(before.Token) != 0 {
			t.Fatal("worker-after precondition requires no cursor or token")
		}
		if len(pending) != 3 {
			t.Fatalf("pending A raw=%d", len(pending))
		}
		for _, raw := range pending {
			if raw.ChainStatus != "ACTIVE" || raw.ReorgID != nil || raw.RevertedAt != nil {
				t.Fatal("A must be active before rollback")
			}
		}
		var tokenCount int64
		if err := db.Model(&postgre.ChainTransfer{}).Where("source_type='EVENT_LOG'").Count(&tokenCount).Error; err != nil || tokenCount != 0 {
			t.Fatal("A token must not exist before worker")
		}
		f.Switch("pending-B", 14)
		waitForkState(t, ctx, db, progress, 2, b[2].Hash, false)
		var invalid []postgre.ChainRawEvent
		if err := db.Where("block_hash IN ?", []string{a[1].Hash, a[2].Hash}).Order("id").Find(&invalid).Error; err != nil {
			t.Fatal(err)
		}
		if len(invalid) != 2 {
			t.Fatal("old A raw disappeared")
		}
		for i, raw := range invalid {
			if raw.ID != pending[i+1].ID || raw.ChainStatus != "REVERTED" || raw.ReorgID == nil || raw.RevertedAt == nil || *raw.RevertedAt <= 0 {
				t.Fatal("rollback must retain unconsumed raw with invalidation metadata")
			}
		}
		if err := workerGate.Table("chain_event_biz_cursors").Count(&tokenCount).Error; err != nil || tokenCount != 0 {
			t.Fatal("cursor advanced while worker gated")
		}
		if err := db.Model(&postgre.ChainTransfer{}).Where("source_type='EVENT_LOG'").Count(&tokenCount).Error; err != nil || tokenCount != 0 {
			t.Fatal("token existed before worker release")
		}
		if err := workerGate.Commit().Error; err != nil {
			t.Fatal(err)
		}
		waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
		service.Stop(t)
		got := queryForkState(t, db)
		want := []struct {
			block    testioc.ForkBlock
			reverted bool
		}{}
		for _, block := range a[:1] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}
		for _, block := range a[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, true})
		}
		for _, block := range b[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}

		// LE-25 worker-after: compare every persisted identity to this scenario's frozen manifest.
		if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
			t.Fatalf("checkpoint: %+v", got.Checkpoints)
		}
		if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
			t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
		}

		if len(got.Reorgs) != 1 {
			t.Fatalf("reorg natural count=%d", len(got.Reorgs))
		}
		r := got.Reorgs[0]
		if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
			t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
		}
		reorgID := r.ReorgID

		blocks := map[string]postgre.ChainBlock{}
		txs := map[string]postgre.ChainTransaction{}
		raws := map[string]postgre.ChainRawEvent{}
		natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
		for _, v := range got.Blocks {
			if _, ok := blocks[v.BlockHash]; ok {
				t.Fatal("duplicate block natural key")
			}
			blocks[v.BlockHash] = v
		}
		for _, v := range got.Transactions {
			if _, ok := txs[v.BlockHash]; ok {
				t.Fatal("duplicate transaction natural key")
			}
			txs[v.BlockHash] = v
		}
		var lastID int64
		for _, v := range got.Raw {
			if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
				t.Fatal("raw identity/ID ordering")
			}
			raws[v.BlockHash] = v
			lastID = v.ID
		}
		for _, v := range got.Native {
			if _, ok := natives[v.BlockHash]; ok {
				t.Fatal("duplicate native natural key")
			}
			natives[v.BlockHash] = v
		}
		for _, v := range got.Token {
			if _, ok := tokens[v.BlockHash]; ok {
				t.Fatal("duplicate token natural key")
			}
			tokens[v.BlockHash] = v
		}
		for _, w := range want {
			b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
			blockStatus, chainStatus := "CANONICAL", "ACTIVE"
			if w.reverted {
				blockStatus, chainStatus = "ORPHANED", "REVERTED"
			}
			if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
				t.Fatalf("block identity/status: %+v", b)
			}
			if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
				t.Fatalf("transaction identity/business: %+v", tx)
			}
			if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
				t.Fatalf("raw identity/business: %+v", raw)
			}
			for _, tr := range []postgre.ChainTransfer{native, token} {
				if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
					t.Fatalf("transfer identity/business: %+v", tr)
				}
			}
			if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
				t.Fatalf("native/token source linkage: %+v %+v", native, token)
			}
			if w.reverted {
				if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
					t.Fatalf("orphan metadata: %+v", b)
				}
				for _, m := range []struct {
					ID *string
					At *int64
				}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
					if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
						t.Fatal("old fork metadata is not atomic/shared")
					}
				}
			} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
				t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
			}
		}
		if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
			t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
		}
		for _, cursor := range before.Cursors {
			if got.Cursors[0].Cursor < cursor.Cursor {
				t.Fatal("cursor regressed")
			}
		}
		// Compare full immutable snapshots, allowing only already-asserted state metadata.
		for _, old := range before.Blocks {
			v := blocks[old.BlockHash]
			old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("block immutable data changed")
			}
		}
		for _, old := range before.Transactions {
			v := txs[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("transaction immutable data changed")
			}
		}
		for _, old := range before.Raw {
			v := raws[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("raw row ID/business data changed")
			}
		}
		for _, old := range before.Native {
			v := natives[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("native row ID/business data changed")
			}
		}
		for _, old := range before.Token {
			v := tokens[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("token row ID/business data changed")
			}
		}

	})
}

// LE-26 catches partial visibility and failed convergence at three real exit boundaries.
func TestLocalForkRestartRecovery(t *testing.T) {
	t.Run("detected", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, dsn, progress := startForkDatabase(t, ctx)
		f := testioc.InitForkRPCFixture(t)
		h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

		a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
		for n := int64(0); n <= 14; n++ {
			parent := h(0)
			if n > 0 {
				parent = h(0x2600 + n - 1)
			}
			a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2600 + n), Parent: parent, Tx: h(0x2600 + 0x20 + n)})
			block := a[n]
			if n > 0 {
				block.Hash, block.Tx = h(0x2600+0x40+n), h(0x2600+0x60+n)
				if n > 1 {
					block.Parent = h(0x2600 + 0x40 + n - 1)
				}
			}
			b = append(b, block)
		}
		f.SetBranch("detected-A", a)
		f.SetBranch("detected-B", b)
		f.Select("detected-A", 0)
		service := startForkService(t, ctx, f, dsn)
		f.Switch("detected-A", 14)
		waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
		before := queryForkState(t, db)

		// The mismatching checkpoint header at 2 has returned; ancestor request 1
		// has entered the RPC boundary but has not returned to the scanner.
		ancestor := f.Gate("detected-B", 1, false)
		t.Cleanup(ancestor.Release)
		f.Switch("detected-B", 14)
		if err := ancestor.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		if f.Requests("detected-B", 2, false) != 1 {
			t.Fatal("checkpoint mismatch was not read")
		}
		service.Stop(t)
		ancestor.Release()
		stopped := queryForkState(t, db)
		if !reflect.DeepEqual(before, stopped) || len(stopped.Reorgs) != 0 {
			t.Fatal("DETECTED exit exposed partial rollback or changed A/cursor")
		}

		if err := f.WaitForSubscribers(ctx, 0); err != nil {
			t.Fatal(err)
		}
		restarted := startForkService(t, ctx, f, dsn)
		f.Publish("detected-B", 14)
		waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
		restarted.Stop(t)
		got := queryForkState(t, db)

		want := []struct {
			block    testioc.ForkBlock
			reverted bool
		}{}
		for _, block := range a[:1] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}
		for _, block := range a[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, true})
		}
		for _, block := range b[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}

		// LE-26 detected: compare every persisted identity to this scenario's frozen manifest.
		if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
			t.Fatalf("checkpoint: %+v", got.Checkpoints)
		}
		if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
			t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
		}

		if len(got.Reorgs) != 1 {
			t.Fatalf("reorg natural count=%d", len(got.Reorgs))
		}
		r := got.Reorgs[0]
		if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
			t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
		}
		reorgID := r.ReorgID

		blocks := map[string]postgre.ChainBlock{}
		txs := map[string]postgre.ChainTransaction{}
		raws := map[string]postgre.ChainRawEvent{}
		natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
		for _, v := range got.Blocks {
			if _, ok := blocks[v.BlockHash]; ok {
				t.Fatal("duplicate block natural key")
			}
			blocks[v.BlockHash] = v
		}
		for _, v := range got.Transactions {
			if _, ok := txs[v.BlockHash]; ok {
				t.Fatal("duplicate transaction natural key")
			}
			txs[v.BlockHash] = v
		}
		var lastID int64
		for _, v := range got.Raw {
			if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
				t.Fatal("raw identity/ID ordering")
			}
			raws[v.BlockHash] = v
			lastID = v.ID
		}
		for _, v := range got.Native {
			if _, ok := natives[v.BlockHash]; ok {
				t.Fatal("duplicate native natural key")
			}
			natives[v.BlockHash] = v
		}
		for _, v := range got.Token {
			if _, ok := tokens[v.BlockHash]; ok {
				t.Fatal("duplicate token natural key")
			}
			tokens[v.BlockHash] = v
		}
		for _, w := range want {
			b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
			blockStatus, chainStatus := "CANONICAL", "ACTIVE"
			if w.reverted {
				blockStatus, chainStatus = "ORPHANED", "REVERTED"
			}
			if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
				t.Fatalf("block identity/status: %+v", b)
			}
			if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
				t.Fatalf("transaction identity/business: %+v", tx)
			}
			if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
				t.Fatalf("raw identity/business: %+v", raw)
			}
			for _, tr := range []postgre.ChainTransfer{native, token} {
				if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
					t.Fatalf("transfer identity/business: %+v", tr)
				}
			}
			if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
				t.Fatalf("native/token source linkage: %+v %+v", native, token)
			}
			if w.reverted {
				if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
					t.Fatalf("orphan metadata: %+v", b)
				}
				for _, m := range []struct {
					ID *string
					At *int64
				}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
					if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
						t.Fatal("old fork metadata is not atomic/shared")
					}
				}
			} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
				t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
			}
		}
		if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
			t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
		}
		for _, cursor := range before.Cursors {
			if got.Cursors[0].Cursor < cursor.Cursor {
				t.Fatal("cursor regressed")
			}
		}
		// Compare full immutable snapshots, allowing only already-asserted state metadata.
		for _, old := range before.Blocks {
			v := blocks[old.BlockHash]
			old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("block immutable data changed")
			}
		}
		for _, old := range before.Transactions {
			v := txs[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("transaction immutable data changed")
			}
		}
		for _, old := range before.Raw {
			v := raws[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("raw row ID/business data changed")
			}
		}
		for _, old := range before.Native {
			v := natives[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("native row ID/business data changed")
			}
		}
		for _, old := range before.Token {
			v := tokens[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("token row ID/business data changed")
			}
		}

	})
	t.Run("processing", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, dsn, progress := startForkDatabase(t, ctx)
		f := testioc.InitForkRPCFixture(t)
		h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

		a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
		for n := int64(0); n <= 14; n++ {
			parent := h(0)
			if n > 0 {
				parent = h(0x2680 + n - 1)
			}
			a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2680 + n), Parent: parent, Tx: h(0x2680 + 0x20 + n)})
			block := a[n]
			if n > 0 {
				block.Hash, block.Tx = h(0x2680+0x40+n), h(0x2680+0x60+n)
				if n > 1 {
					block.Parent = h(0x2680 + 0x40 + n - 1)
				}
			}
			b = append(b, block)
		}
		f.SetBranch("processing-A", a)
		f.SetBranch("processing-B", b)
		f.Select("processing-A", 0)
		service := startForkService(t, ctx, f, dsn)
		f.Switch("processing-A", 14)
		waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
		before := queryForkState(t, db)

		blocker := db.WithContext(ctx).Begin()
		if blocker.Error != nil {
			t.Fatal(blocker.Error)
		}
		t.Cleanup(func() { blocker.Rollback() })
		var blockerPID int32
		if err := blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error; err != nil {
			t.Fatal(err)
		}
		var lockedID int64
		if err := blocker.Raw("SELECT id FROM chain_raw_events WHERE chain_id=1 AND block_hash=? FOR UPDATE", a[1].Hash).Scan(&lockedID).Error; err != nil {
			t.Fatal(err)
		}
		if lockedID != before.Raw[1].ID {
			t.Fatal("blocker did not lock designated A raw")
		}
		f.Switch("processing-B", 14)
		// application_name binds this backend to this service instance. The catalog
		// observer additionally proves the exact blocker -> waiter relation.
		waiterPID, waitErr := waitForkRawLock(ctx, db, service.app, blockerPID)
		if waitErr != nil {
			var cleanupErrs []error
			if err := blocker.Rollback().Error; err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("rollback blocker: %w", err))
			}
			if err := service.stop(); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("join fork service: %w", err))
			}
			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 5*time.Second)
			if err := f.WaitForSubscribers(cleanupCtx, 0); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("wait for subscriber cleanup: %w", err))
			}
			cancelCleanup()
			if cleanupErr := errors.Join(cleanupErrs...); cleanupErr != nil {
				t.Fatalf("wait for designated fork row lock: %v; cleanup: %v", waitErr, cleanupErr)
			}
			t.Fatalf("wait for designated fork row lock: %v", waitErr)
		}
		if waiterPID == blockerPID || waiterPID <= 0 {
			t.Fatal("invalid service/blocker backend identities")
		}
		during := queryForkState(t, db)
		if !reflect.DeepEqual(before, during) || len(during.Reorgs) != 0 {
			t.Fatal("PROCESSING transaction became partly visible")
		}
		service.Stop(t)
		if err := blocker.Rollback().Error; err != nil {
			t.Fatal(err)
		}
		stopped := queryForkState(t, db)
		if !reflect.DeepEqual(before, stopped) || len(stopped.Reorgs) != 0 {
			t.Fatal("canceled PROCESSING transaction changed A/checkpoint/cursor")
		}

		if err := f.WaitForSubscribers(ctx, 0); err != nil {
			t.Fatal(err)
		}
		restarted := startForkService(t, ctx, f, dsn)
		f.Publish("processing-B", 14)
		waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
		restarted.Stop(t)
		got := queryForkState(t, db)

		want := []struct {
			block    testioc.ForkBlock
			reverted bool
		}{}
		for _, block := range a[:1] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}
		for _, block := range a[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, true})
		}
		for _, block := range b[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}

		// LE-26 processing: compare every persisted identity to this scenario's frozen manifest.
		if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
			t.Fatalf("checkpoint: %+v", got.Checkpoints)
		}
		if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
			t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
		}

		if len(got.Reorgs) != 1 {
			t.Fatalf("reorg natural count=%d", len(got.Reorgs))
		}
		r := got.Reorgs[0]
		if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
			t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
		}
		reorgID := r.ReorgID

		blocks := map[string]postgre.ChainBlock{}
		txs := map[string]postgre.ChainTransaction{}
		raws := map[string]postgre.ChainRawEvent{}
		natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
		for _, v := range got.Blocks {
			if _, ok := blocks[v.BlockHash]; ok {
				t.Fatal("duplicate block natural key")
			}
			blocks[v.BlockHash] = v
		}
		for _, v := range got.Transactions {
			if _, ok := txs[v.BlockHash]; ok {
				t.Fatal("duplicate transaction natural key")
			}
			txs[v.BlockHash] = v
		}
		var lastID int64
		for _, v := range got.Raw {
			if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
				t.Fatal("raw identity/ID ordering")
			}
			raws[v.BlockHash] = v
			lastID = v.ID
		}
		for _, v := range got.Native {
			if _, ok := natives[v.BlockHash]; ok {
				t.Fatal("duplicate native natural key")
			}
			natives[v.BlockHash] = v
		}
		for _, v := range got.Token {
			if _, ok := tokens[v.BlockHash]; ok {
				t.Fatal("duplicate token natural key")
			}
			tokens[v.BlockHash] = v
		}
		for _, w := range want {
			b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
			blockStatus, chainStatus := "CANONICAL", "ACTIVE"
			if w.reverted {
				blockStatus, chainStatus = "ORPHANED", "REVERTED"
			}
			if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
				t.Fatalf("block identity/status: %+v", b)
			}
			if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
				t.Fatalf("transaction identity/business: %+v", tx)
			}
			if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
				t.Fatalf("raw identity/business: %+v", raw)
			}
			for _, tr := range []postgre.ChainTransfer{native, token} {
				if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
					t.Fatalf("transfer identity/business: %+v", tr)
				}
			}
			if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
				t.Fatalf("native/token source linkage: %+v %+v", native, token)
			}
			if w.reverted {
				if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
					t.Fatalf("orphan metadata: %+v", b)
				}
				for _, m := range []struct {
					ID *string
					At *int64
				}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
					if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
						t.Fatal("old fork metadata is not atomic/shared")
					}
				}
			} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
				t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
			}
		}
		if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
			t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
		}
		for _, cursor := range before.Cursors {
			if got.Cursors[0].Cursor < cursor.Cursor {
				t.Fatal("cursor regressed")
			}
		}
		// Compare full immutable snapshots, allowing only already-asserted state metadata.
		for _, old := range before.Blocks {
			v := blocks[old.BlockHash]
			old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("block immutable data changed")
			}
		}
		for _, old := range before.Transactions {
			v := txs[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("transaction immutable data changed")
			}
		}
		for _, old := range before.Raw {
			v := raws[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("raw row ID/business data changed")
			}
		}
		for _, old := range before.Native {
			v := natives[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("native row ID/business data changed")
			}
		}
		for _, old := range before.Token {
			v := tokens[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("token row ID/business data changed")
			}
		}

	})
	t.Run("rollback-after-commit", func(t *testing.T) {

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, dsn, progress := startForkDatabase(t, ctx)
		f := testioc.InitForkRPCFixture(t)
		h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

		a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
		for n := int64(0); n <= 14; n++ {
			parent := h(0)
			if n > 0 {
				parent = h(0x2700 + n - 1)
			}
			a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2700 + n), Parent: parent, Tx: h(0x2700 + 0x20 + n)})
			block := a[n]
			if n > 0 {
				block.Hash, block.Tx = h(0x2700+0x40+n), h(0x2700+0x60+n)
				if n > 1 {
					block.Parent = h(0x2700 + 0x40 + n - 1)
				}
			}
			b = append(b, block)
		}
		f.SetBranch("committed-A", a)
		f.SetBranch("committed-B", b)
		f.Select("committed-A", 0)
		service := startForkService(t, ctx, f, dsn)
		f.Switch("committed-A", 14)
		waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
		before := queryForkState(t, db)

		replay := f.Gate("committed-B", 1, true)
		t.Cleanup(replay.Release)
		f.Switch("committed-B", 14)
		if err := replay.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		committed := queryForkState(t, db)
		if len(committed.Reorgs) != 1 || committed.Reorgs[0].Status != "COMPLETED" || committed.Reorgs[0].CompletedAt == nil || committed.Reorgs[0].AncestorHeight != 0 || committed.Reorgs[0].AncestorHash != a[0].Hash || committed.Reorgs[0].OldHeadHash != a[2].Hash || committed.Reorgs[0].NewHeadHash != b[14].Hash || committed.Reorgs[0].ReorgDepth != 2 {
			t.Fatalf("committed rollback record: %+v", committed.Reorgs)
		}
		if len(committed.Checkpoints) != 1 || committed.Checkpoints[0].LastProcessedHeight != 0 || committed.Checkpoints[0].LastProcessedHash != a[0].Hash || len(committed.Blocks) != 3 || len(committed.Transactions) != 3 || len(committed.Raw) != 3 || len(committed.Native) != 3 || len(committed.Token) != 3 || !reflect.DeepEqual(committed.Cursors, before.Cursors) {
			t.Fatal("rollback boundary checkpoint/facts/cursor")
		}
		rid := committed.Reorgs[0].ReorgID
		for _, block := range committed.Blocks {
			if block.BlockNumber > 0 && (block.Status != "ORPHANED" || block.ReorgID == nil || *block.ReorgID != rid || block.OrphanedAt == nil) {
				t.Fatal("committed A block state")
			}
		}
		for _, raw := range committed.Raw {
			if raw.BlockNumber > 0 && (raw.ChainStatus != "REVERTED" || raw.ReorgID == nil || *raw.ReorgID != rid || raw.RevertedAt == nil) {
				t.Fatal("committed A raw state")
			}
		}
		for _, tx := range committed.Transactions {
			if tx.BlockNumber > 0 && (tx.ChainStatus != "REVERTED" || tx.ReorgID == nil || *tx.ReorgID != rid || tx.RevertedAt == nil) {
				t.Fatal("committed A transaction state")
			}
		}
		for _, tr := range append(append([]postgre.ChainTransfer{}, committed.Native...), committed.Token...) {
			if tr.BlockNumber > 0 && (tr.ChainStatus != "REVERTED" || tr.ReorgID == nil || *tr.ReorgID != rid || tr.RevertedAt == nil) {
				t.Fatal("committed A transfer state")
			}
		}
		service.Stop(t)
		replay.Release()
		stopped := queryForkState(t, db)
		if !reflect.DeepEqual(committed, stopped) {
			t.Fatal("exit changed committed rollback boundary")
		}

		if err := f.WaitForSubscribers(ctx, 0); err != nil {
			t.Fatal(err)
		}
		restarted := startForkService(t, ctx, f, dsn)
		f.Publish("committed-B", 14)
		waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
		restarted.Stop(t)
		got := queryForkState(t, db)

		want := []struct {
			block    testioc.ForkBlock
			reverted bool
		}{}
		for _, block := range a[:1] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}
		for _, block := range a[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, true})
		}
		for _, block := range b[1:3] {
			want = append(want, struct {
				block    testioc.ForkBlock
				reverted bool
			}{block, false})
		}

		// LE-26 committed: compare every persisted identity to this scenario's frozen manifest.
		if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
			t.Fatalf("checkpoint: %+v", got.Checkpoints)
		}
		if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
			t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
		}

		if len(got.Reorgs) != 1 {
			t.Fatalf("reorg natural count=%d", len(got.Reorgs))
		}
		r := got.Reorgs[0]
		if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
			t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
		}
		reorgID := r.ReorgID

		blocks := map[string]postgre.ChainBlock{}
		txs := map[string]postgre.ChainTransaction{}
		raws := map[string]postgre.ChainRawEvent{}
		natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
		for _, v := range got.Blocks {
			if _, ok := blocks[v.BlockHash]; ok {
				t.Fatal("duplicate block natural key")
			}
			blocks[v.BlockHash] = v
		}
		for _, v := range got.Transactions {
			if _, ok := txs[v.BlockHash]; ok {
				t.Fatal("duplicate transaction natural key")
			}
			txs[v.BlockHash] = v
		}
		var lastID int64
		for _, v := range got.Raw {
			if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
				t.Fatal("raw identity/ID ordering")
			}
			raws[v.BlockHash] = v
			lastID = v.ID
		}
		for _, v := range got.Native {
			if _, ok := natives[v.BlockHash]; ok {
				t.Fatal("duplicate native natural key")
			}
			natives[v.BlockHash] = v
		}
		for _, v := range got.Token {
			if _, ok := tokens[v.BlockHash]; ok {
				t.Fatal("duplicate token natural key")
			}
			tokens[v.BlockHash] = v
		}
		for _, w := range want {
			b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
			blockStatus, chainStatus := "CANONICAL", "ACTIVE"
			if w.reverted {
				blockStatus, chainStatus = "ORPHANED", "REVERTED"
			}
			if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
				t.Fatalf("block identity/status: %+v", b)
			}
			if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
				t.Fatalf("transaction identity/business: %+v", tx)
			}
			if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
				t.Fatalf("raw identity/business: %+v", raw)
			}
			for _, tr := range []postgre.ChainTransfer{native, token} {
				if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
					t.Fatalf("transfer identity/business: %+v", tr)
				}
			}
			if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
				t.Fatalf("native/token source linkage: %+v %+v", native, token)
			}
			if w.reverted {
				if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
					t.Fatalf("orphan metadata: %+v", b)
				}
				for _, m := range []struct {
					ID *string
					At *int64
				}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
					if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
						t.Fatal("old fork metadata is not atomic/shared")
					}
				}
			} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
				t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
			}
		}
		if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
			t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
		}
		for _, cursor := range before.Cursors {
			if got.Cursors[0].Cursor < cursor.Cursor {
				t.Fatal("cursor regressed")
			}
		}
		// Compare full immutable snapshots, allowing only already-asserted state metadata.
		for _, old := range before.Blocks {
			v := blocks[old.BlockHash]
			old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("block immutable data changed")
			}
		}
		for _, old := range before.Transactions {
			v := txs[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("transaction immutable data changed")
			}
		}
		for _, old := range before.Raw {
			v := raws[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("raw row ID/business data changed")
			}
		}
		for _, old := range before.Native {
			v := natives[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("native row ID/business data changed")
			}
		}
		for _, old := range before.Token {
			v := tokens[old.BlockHash]
			old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
			if !reflect.DeepEqual(old, v) {
				t.Fatal("token row ID/business data changed")
			}
		}

		if got.Reorgs[0].ReorgID != rid || !reflect.DeepEqual(got.Reorgs, committed.Reorgs) {
			t.Fatal("restart duplicated/replaced completed reorg")
		}
	})
}

// LE-27 catches accidental rollback/write admission when no ancestor exists, including genesis.
func TestLocalForkNoCommonAncestorFailClosed(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, dsn, progress := startForkDatabase(t, ctx)
	f := testioc.InitForkRPCFixture(t)
	h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

	a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
	for n := int64(0); n <= 14; n++ {
		pa, pb := h(0), h(0)
		if n > 0 {
			pa, pb = h(0x2780+n-1), h(0x27c0+n-1)
		}
		a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2780 + n), Parent: pa, Tx: h(0x27a0 + n)})
		b = append(b, testioc.ForkBlock{Height: n, Hash: h(0x27c0 + n), Parent: pb, Tx: h(0x27e0 + n)})
	}
	f.SetBranch("isolated-A", a)
	f.SetBranch("alien-genesis-B", b)
	f.Select("isolated-A", 0)
	service := startForkService(t, ctx, f, dsn)
	f.Switch("isolated-A", 14)
	waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
	before := queryForkState(t, db)
	f.Switch("alien-genesis-B", 14)
	output := service.Failed(t, ctx)
	if !strings.Contains(output, "component chain ethereum listener") {
		t.Fatal("missing stable service component error")
	}
	for _, secret := range []string{dsn, f.HTTPURL(), f.WSURL()} {
		if strings.Contains(output, secret) {
			t.Fatal("service failure disclosed private connection data")
		}
	}
	if f.Requests("alien-genesis-B", 2, false) != 1 || f.Requests("alien-genesis-B", 1, false) != 1 || f.Requests("alien-genesis-B", 0, false) != 1 {
		t.Fatal("no-ancestor case did not search through genesis")
	}
	got := queryForkState(t, db)
	if !reflect.DeepEqual(before, got) {
		t.Fatal("fail-closed changed A checkpoint/facts/metadata/cursor")
	}
	want := []struct {
		block    testioc.ForkBlock
		reverted bool
	}{{a[0], false}, {a[1], false}, {a[2], false}}

	// LE-27: compare every persisted identity to this scenario's frozen manifest.
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != a[2].Hash {
		t.Fatalf("checkpoint: %+v", got.Checkpoints)
	}
	if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
		t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
	}
	if len(got.Reorgs) != 0 {
		t.Fatalf("unexpected reorg: %+v", got.Reorgs)
	}
	reorgID := ""
	blocks := map[string]postgre.ChainBlock{}
	txs := map[string]postgre.ChainTransaction{}
	raws := map[string]postgre.ChainRawEvent{}
	natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
	for _, v := range got.Blocks {
		if _, ok := blocks[v.BlockHash]; ok {
			t.Fatal("duplicate block natural key")
		}
		blocks[v.BlockHash] = v
	}
	for _, v := range got.Transactions {
		if _, ok := txs[v.BlockHash]; ok {
			t.Fatal("duplicate transaction natural key")
		}
		txs[v.BlockHash] = v
	}
	var lastID int64
	for _, v := range got.Raw {
		if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
			t.Fatal("raw identity/ID ordering")
		}
		raws[v.BlockHash] = v
		lastID = v.ID
	}
	for _, v := range got.Native {
		if _, ok := natives[v.BlockHash]; ok {
			t.Fatal("duplicate native natural key")
		}
		natives[v.BlockHash] = v
	}
	for _, v := range got.Token {
		if _, ok := tokens[v.BlockHash]; ok {
			t.Fatal("duplicate token natural key")
		}
		tokens[v.BlockHash] = v
	}
	for _, w := range want {
		b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
		blockStatus, chainStatus := "CANONICAL", "ACTIVE"
		if w.reverted {
			blockStatus, chainStatus = "ORPHANED", "REVERTED"
		}
		if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
			t.Fatalf("block identity/status: %+v", b)
		}
		if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
			t.Fatalf("transaction identity/business: %+v", tx)
		}
		if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
			t.Fatalf("raw identity/business: %+v", raw)
		}
		for _, tr := range []postgre.ChainTransfer{native, token} {
			if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
				t.Fatalf("transfer identity/business: %+v", tr)
			}
		}
		if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
			t.Fatalf("native/token source linkage: %+v %+v", native, token)
		}
		if w.reverted {
			if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
				t.Fatalf("orphan metadata: %+v", b)
			}
			for _, m := range []struct {
				ID *string
				At *int64
			}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
				if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
					t.Fatal("old fork metadata is not atomic/shared")
				}
			}
		} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
			t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
		}
	}
	if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
		t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
	}
	for _, cursor := range before.Cursors {
		if got.Cursors[0].Cursor < cursor.Cursor {
			t.Fatal("cursor regressed")
		}
	}
	// Compare full immutable snapshots, allowing only already-asserted state metadata.
	for _, old := range before.Blocks {
		v := blocks[old.BlockHash]
		old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("block immutable data changed")
		}
	}
	for _, old := range before.Transactions {
		v := txs[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("transaction immutable data changed")
		}
	}
	for _, old := range before.Raw {
		v := raws[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("raw row ID/business data changed")
		}
	}
	for _, old := range before.Native {
		v := natives[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("native row ID/business data changed")
		}
	}
	for _, old := range before.Token {
		v := tokens[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("token row ID/business data changed")
		}
	}

	for _, block := range b {
		if _, ok := blocks[block.Hash]; ok {
			t.Fatal("B block persisted despite missing ancestor")
		}
		if _, ok := txs[block.Hash]; ok {
			t.Fatal("B transaction persisted despite missing ancestor")
		}
		if _, ok := raws[block.Hash]; ok {
			t.Fatal("B raw persisted despite missing ancestor")
		}
		if _, ok := natives[block.Hash]; ok {
			t.Fatal("B native persisted despite missing ancestor")
		}
		if _, ok := tokens[block.Hash]; ok {
			t.Fatal("B token persisted despite missing ancestor")
		}
	}
}

// LE-28 catches reorg/fact duplication and row-ID/business mutation across replay/restart.
func TestLocalForkReplayIdempotent(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, dsn, progress := startForkDatabase(t, ctx)
	f := testioc.InitForkRPCFixture(t)
	h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

	a, b := []testioc.ForkBlock{}, []testioc.ForkBlock{}
	for n := int64(0); n <= 14; n++ {
		parent := h(0)
		if n > 0 {
			parent = h(0x2800 + n - 1)
		}
		a = append(a, testioc.ForkBlock{Height: n, Hash: h(0x2800 + n), Parent: parent, Tx: h(0x2820 + n)})
		block := a[n]
		if n > 0 {
			block.Hash, block.Tx = h(0x2840+n), h(0x2860+n)
			if n > 1 {
				block.Parent = h(0x2840 + n - 1)
			}
		}
		b = append(b, block)
	}
	f.SetBranch("replay-A", a)
	f.SetBranch("replay-B", b)
	f.Select("replay-A", 0)
	service := startForkService(t, ctx, f, dsn)
	f.Switch("replay-A", 14)
	waitForkState(t, ctx, db, progress, 2, a[2].Hash, true)
	before := queryForkState(t, db)
	f.Switch("replay-B", 14)
	waitForkState(t, ctx, db, progress, 2, b[2].Hash, true)
	service.Stop(t)
	converged := queryForkState(t, db)
	if err := f.WaitForSubscribers(ctx, 0); err != nil {
		t.Fatal(err)
	}
	restarted := startForkService(t, ctx, f, dsn)
	// Same branch objects and same head identities are reused after restart.
	first := f.Gate("replay-B", 2, false)
	t.Cleanup(first.Release)
	f.Publish("replay-B", 14)
	if err := first.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()
	second := f.Gate("replay-B", 2, false)
	t.Cleanup(second.Release)
	f.Publish("replay-B", 14)
	if err := second.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	second.Release()
	third := f.Gate("replay-B", 2, false)
	t.Cleanup(third.Release)
	f.Publish("replay-B", 13)
	if err := third.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	third.Release()
	restarted.Stop(t)
	got := queryForkState(t, db)
	if !reflect.DeepEqual(converged, got) {
		t.Fatal("same fork replay/restart changed row IDs, immutable data, reorg, checkpoint or cursor")
	}

	want := []struct {
		block    testioc.ForkBlock
		reverted bool
	}{}
	for _, block := range a[:1] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, false})
	}
	for _, block := range a[1:3] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, true})
	}
	for _, block := range b[1:3] {
		want = append(want, struct {
			block    testioc.ForkBlock
			reverted bool
		}{block, false})
	}

	// LE-28: compare every persisted identity to this scenario's frozen manifest.
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 2 || got.Checkpoints[0].LastProcessedHash != b[2].Hash {
		t.Fatalf("checkpoint: %+v", got.Checkpoints)
	}
	if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
		t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
	}

	if len(got.Reorgs) != 1 {
		t.Fatalf("reorg natural count=%d", len(got.Reorgs))
	}
	r := got.Reorgs[0]
	if r.ChainID != 1 || r.ReorgID == "" || r.AncestorHeight != 0 || r.AncestorHash != a[0].Hash || r.OldHeadHeight != 2 || r.OldHeadHash != a[2].Hash || r.NewHeadHeight != 14 || r.NewHeadHash != b[14].Hash || r.ReorgDepth != 2 || r.Status != "COMPLETED" || r.DetectedAt <= 0 || r.CompletedAt == nil || *r.CompletedAt < r.DetectedAt || r.ErrorMessage != nil {
		t.Fatalf("reorg identity/heads/depth/timestamps: %+v", r)
	}
	reorgID := r.ReorgID

	blocks := map[string]postgre.ChainBlock{}
	txs := map[string]postgre.ChainTransaction{}
	raws := map[string]postgre.ChainRawEvent{}
	natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
	for _, v := range got.Blocks {
		if _, ok := blocks[v.BlockHash]; ok {
			t.Fatal("duplicate block natural key")
		}
		blocks[v.BlockHash] = v
	}
	for _, v := range got.Transactions {
		if _, ok := txs[v.BlockHash]; ok {
			t.Fatal("duplicate transaction natural key")
		}
		txs[v.BlockHash] = v
	}
	var lastID int64
	for _, v := range got.Raw {
		if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
			t.Fatal("raw identity/ID ordering")
		}
		raws[v.BlockHash] = v
		lastID = v.ID
	}
	for _, v := range got.Native {
		if _, ok := natives[v.BlockHash]; ok {
			t.Fatal("duplicate native natural key")
		}
		natives[v.BlockHash] = v
	}
	for _, v := range got.Token {
		if _, ok := tokens[v.BlockHash]; ok {
			t.Fatal("duplicate token natural key")
		}
		tokens[v.BlockHash] = v
	}
	for _, w := range want {
		b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
		blockStatus, chainStatus := "CANONICAL", "ACTIVE"
		if w.reverted {
			blockStatus, chainStatus = "ORPHANED", "REVERTED"
		}
		if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
			t.Fatalf("block identity/status: %+v", b)
		}
		if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
			t.Fatalf("transaction identity/business: %+v", tx)
		}
		if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
			t.Fatalf("raw identity/business: %+v", raw)
		}
		for _, tr := range []postgre.ChainTransfer{native, token} {
			if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
				t.Fatalf("transfer identity/business: %+v", tr)
			}
		}
		if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
			t.Fatalf("native/token source linkage: %+v %+v", native, token)
		}
		if w.reverted {
			if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
				t.Fatalf("orphan metadata: %+v", b)
			}
			for _, m := range []struct {
				ID *string
				At *int64
			}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
				if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
					t.Fatal("old fork metadata is not atomic/shared")
				}
			}
		} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
			t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
		}
	}
	if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
		t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
	}
	for _, cursor := range before.Cursors {
		if got.Cursors[0].Cursor < cursor.Cursor {
			t.Fatal("cursor regressed")
		}
	}
	// Compare full immutable snapshots, allowing only already-asserted state metadata.
	for _, old := range before.Blocks {
		v := blocks[old.BlockHash]
		old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("block immutable data changed")
		}
	}
	for _, old := range before.Transactions {
		v := txs[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("transaction immutable data changed")
		}
	}
	for _, old := range before.Raw {
		v := raws[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("raw row ID/business data changed")
		}
	}
	for _, old := range before.Native {
		v := natives[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("native row ID/business data changed")
		}
	}
	for _, old := range before.Token {
		v := tokens[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("token row ID/business data changed")
		}
	}

}

// LE-29 catches treating WS head height/order as authoritative over HTTP safe height.
func TestLocalForkDuplicateOutOfOrderHeads(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, dsn, progress := startForkDatabase(t, ctx)
	f := testioc.InitForkRPCFixture(t)
	h := func(n int64) string { return fmt.Sprintf("0x%064x", n) }

	canonical := []testioc.ForkBlock{}
	for n := int64(0); n <= 100; n++ {
		parent := h(0)
		if n > 0 {
			parent = h(0x2900 + n - 1)
		}
		canonical = append(canonical, testioc.ForkBlock{Height: n, Hash: h(0x2900 + n), Parent: parent, Tx: h(0x2a00 + n)})
	}
	f.SetBranch("ordered-http", canonical)
	f.Select("ordered-http", 0)
	service := startForkService(t, ctx, f, dsn)
	f.Switch("ordered-http", 13)
	waitForkState(t, ctx, db, progress, 1, canonical[1].Hash, true)
	before := queryForkState(t, db)
	// Each signal has its own request barrier. HTTP remains at 13 while
	// duplicate and descending WS numbers are handled serially.
	duplicate := f.Gate("ordered-http", 1, false)
	t.Cleanup(duplicate.Release)
	f.Publish("ordered-http", 13)
	if err := duplicate.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	duplicate.Release()
	descending := f.Gate("ordered-http", 1, false)
	t.Cleanup(descending.Release)
	f.Publish("ordered-http", 0)
	if err := descending.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	descending.Release()
	atOne := queryForkState(t, db)
	if !reflect.DeepEqual(before, atOne) {
		t.Fatal("duplicate/descending heads changed HTTP safe checkpoint 1")
	}
	// A low WS head triggers the jump to HTTP safe=3 without skipping 2.
	f.Select("ordered-http", 15)
	f.Publish("ordered-http", 2)
	waitForkState(t, ctx, db, progress, 3, canonical[3].Hash, true)
	atThree := queryForkState(t, db)
	if len(atThree.Checkpoints) != 1 || atThree.Checkpoints[0].LastProcessedHeight != 3 || len(atThree.Blocks) != 4 || len(atThree.Raw) != 4 || len(atThree.Token) != 4 || len(atThree.Reorgs) != 0 || len(atThree.Cursors) != 1 || atThree.Cursors[0].Cursor < before.Cursors[0].Cursor {
		t.Fatal("first HTTP jump skipped/regressed facts or cursor")
	}
	// A future WS head still only permits HTTP safe=5.
	f.Select("ordered-http", 17)
	f.Publish("ordered-http", 100)
	waitForkState(t, ctx, db, progress, 5, canonical[5].Hash, true)
	orderedBaseline := queryForkState(t, db)
	highDuplicate := f.Gate("ordered-http", 5, false)
	t.Cleanup(highDuplicate.Release)
	f.Publish("ordered-http", 100)
	if err := highDuplicate.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	highDuplicate.Release()
	lowAfterHigh := f.Gate("ordered-http", 5, false)
	t.Cleanup(lowAfterHigh.Release)
	f.Publish("ordered-http", 1)
	if err := lowAfterHigh.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	lowAfterHigh.Release()
	finalSignal := f.Gate("ordered-http", 5, false)
	t.Cleanup(finalSignal.Release)
	f.Publish("ordered-http", 17)
	if err := finalSignal.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	finalSignal.Release()
	service.Stop(t)
	got := queryForkState(t, db)
	if !reflect.DeepEqual(orderedBaseline, got) {
		t.Fatal("final duplicate/out-of-order heads changed ordered canonical row IDs/business/metadata")
	}
	want := []struct {
		block    testioc.ForkBlock
		reverted bool
	}{{canonical[0], false}, {canonical[1], false}, {canonical[2], false}, {canonical[3], false}, {canonical[4], false}, {canonical[5], false}}

	// LE-29: compare every persisted identity to this scenario's frozen manifest.
	if len(got.Checkpoints) != 1 || got.Checkpoints[0].ChainID != 1 || got.Checkpoints[0].LastProcessedHeight != 5 || got.Checkpoints[0].LastProcessedHash != canonical[5].Hash {
		t.Fatalf("checkpoint: %+v", got.Checkpoints)
	}
	if len(got.Blocks) != len(want) || len(got.Transactions) != len(want) || len(got.Raw) != len(want) || len(got.Native) != len(want) || len(got.Token) != len(want) {
		t.Fatalf("natural-key counts block=%d tx=%d raw=%d native=%d token=%d want=%d", len(got.Blocks), len(got.Transactions), len(got.Raw), len(got.Native), len(got.Token), len(want))
	}
	if len(got.Reorgs) != 0 {
		t.Fatalf("unexpected reorg: %+v", got.Reorgs)
	}
	reorgID := ""
	blocks := map[string]postgre.ChainBlock{}
	txs := map[string]postgre.ChainTransaction{}
	raws := map[string]postgre.ChainRawEvent{}
	natives, tokens := map[string]postgre.ChainTransfer{}, map[string]postgre.ChainTransfer{}
	for _, v := range got.Blocks {
		if _, ok := blocks[v.BlockHash]; ok {
			t.Fatal("duplicate block natural key")
		}
		blocks[v.BlockHash] = v
	}
	for _, v := range got.Transactions {
		if _, ok := txs[v.BlockHash]; ok {
			t.Fatal("duplicate transaction natural key")
		}
		txs[v.BlockHash] = v
	}
	var lastID int64
	for _, v := range got.Raw {
		if _, ok := raws[v.BlockHash]; ok || v.ID <= lastID {
			t.Fatal("raw identity/ID ordering")
		}
		raws[v.BlockHash] = v
		lastID = v.ID
	}
	for _, v := range got.Native {
		if _, ok := natives[v.BlockHash]; ok {
			t.Fatal("duplicate native natural key")
		}
		natives[v.BlockHash] = v
	}
	for _, v := range got.Token {
		if _, ok := tokens[v.BlockHash]; ok {
			t.Fatal("duplicate token natural key")
		}
		tokens[v.BlockHash] = v
	}
	for _, w := range want {
		b, tx, raw, native, token := blocks[w.block.Hash], txs[w.block.Hash], raws[w.block.Hash], natives[w.block.Hash], tokens[w.block.Hash]
		blockStatus, chainStatus := "CANONICAL", "ACTIVE"
		if w.reverted {
			blockStatus, chainStatus = "ORPHANED", "REVERTED"
		}
		if b.ChainID != 1 || b.BlockNumber != w.block.Height || b.BlockHash != w.block.Hash || b.ParentHash != w.block.Parent || b.BlockTime != 1 || b.Status != blockStatus || b.Finality != "CONFIRMED" || b.TransactionCount != 1 || b.ChainVersion != 1 || b.CreatedAt <= 0 || b.RawBlock == nil || b.RawReceipts == nil {
			t.Fatalf("block identity/status: %+v", b)
		}
		if tx.ChainID != 1 || tx.BlockNumber != w.block.Height || tx.BlockHash != w.block.Hash || tx.TxHash != w.block.Tx || tx.TxIndex != 0 || tx.FromAddress != addressOf('1') || tx.ToAddress == nil || *tx.ToAddress != addressOf('2') || tx.Nonce != 0 || tx.Value != "1" || tx.ExecutionStatus != "SUCCESS" || tx.BlockTime != 1 || tx.ChainStatus != chainStatus || tx.RawTransaction == nil || tx.RawReceipt == nil {
			t.Fatalf("transaction identity/business: %+v", tx)
		}
		if raw.ChainID != 1 || raw.BlockNumber != w.block.Height || raw.BlockHash != w.block.Hash || raw.TxHash != w.block.Tx || raw.TxIndex == nil || *raw.TxIndex != 0 || raw.LogIndex != 0 || raw.ID <= 0 || raw.ContractAddress != addressOf('3') || raw.ChainStatus != chainStatus || raw.Topic0 == nil || *raw.Topic0 != "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" || raw.EventData == nil || *raw.EventData != "0x"+strings.Repeat("0", 63)+"1" {
			t.Fatalf("raw identity/business: %+v", raw)
		}
		for _, tr := range []postgre.ChainTransfer{native, token} {
			if tr.ID <= 0 || tr.ChainID != 1 || tr.BlockNumber != w.block.Height || tr.BlockHash != w.block.Hash || tr.TxHash != w.block.Tx || tr.TxIndex == nil || *tr.TxIndex != 0 || tr.FromAddress != addressOf('1') || tr.ToAddress != addressOf('2') || tr.Amount == nil || *tr.Amount != "1" || tr.BatchIndex != 0 || tr.BlockTime != 1 || tr.ChainStatus != chainStatus {
				t.Fatalf("transfer identity/business: %+v", tr)
			}
		}
		if native.SourceType != "TRANSACTION_VALUE" || native.SourceIndex != "tx-value" || native.TransferType != "NATIVE" || native.TokenAddress != nil || native.TokenID != nil || token.SourceType != "EVENT_LOG" || token.SourceIndex != "0" || token.TransferType != "ERC20" || token.TokenAddress == nil || *token.TokenAddress != addressOf('3') || token.TokenID != nil {
			t.Fatalf("native/token source linkage: %+v %+v", native, token)
		}
		if w.reverted {
			if b.ReorgID == nil || *b.ReorgID != reorgID || b.OrphanedAt == nil || *b.OrphanedAt <= 0 {
				t.Fatalf("orphan metadata: %+v", b)
			}
			for _, m := range []struct {
				ID *string
				At *int64
			}{{tx.ReorgID, tx.RevertedAt}, {raw.ReorgID, raw.RevertedAt}, {native.ReorgID, native.RevertedAt}, {token.ReorgID, token.RevertedAt}} {
				if m.ID == nil || *m.ID != reorgID || m.At == nil || *m.At != *b.OrphanedAt {
					t.Fatal("old fork metadata is not atomic/shared")
				}
			}
		} else if b.ReorgID != nil || b.OrphanedAt != nil || tx.ReorgID != nil || tx.RevertedAt != nil || raw.ReorgID != nil || raw.RevertedAt != nil || native.ReorgID != nil || native.RevertedAt != nil || token.ReorgID != nil || token.RevertedAt != nil {
			t.Fatalf("canonical branch retained invalidation metadata: %s", w.block.Hash)
		}
	}
	if len(got.Cursors) != 1 || got.Cursors[0].Biz != "transaction" || got.Cursors[0].ChainFamily != "evm" || got.Cursors[0].Cursor != lastID {
		t.Fatalf("cursor=%+v last raw ID=%d", got.Cursors, lastID)
	}
	for _, cursor := range before.Cursors {
		if got.Cursors[0].Cursor < cursor.Cursor {
			t.Fatal("cursor regressed")
		}
	}
	// Compare full immutable snapshots, allowing only already-asserted state metadata.
	for _, old := range before.Blocks {
		v := blocks[old.BlockHash]
		old.Status, old.ReorgID, old.OrphanedAt = v.Status, v.ReorgID, v.OrphanedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("block immutable data changed")
		}
	}
	for _, old := range before.Transactions {
		v := txs[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("transaction immutable data changed")
		}
	}
	for _, old := range before.Raw {
		v := raws[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("raw row ID/business data changed")
		}
	}
	for _, old := range before.Native {
		v := natives[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("native row ID/business data changed")
		}
	}
	for _, old := range before.Token {
		v := tokens[old.BlockHash]
		old.ChainStatus, old.ReorgID, old.RevertedAt = v.ChainStatus, v.ReorgID, v.RevertedAt
		if !reflect.DeepEqual(old, v) {
			t.Fatal("token row ID/business data changed")
		}
	}

}
