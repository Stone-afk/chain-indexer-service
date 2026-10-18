//go:build e2e

package integration

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveLE19HTTP429TimeoutErrorPropagation(t *testing.T) {
	for _, kind := range []string{"429", "timeout", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			l := newLiveLocal(t, "ethereum")
			l.f.SetLatest(19)
			a := l.block(t, 7, 1)
			p := l.start(t, 1)
			l.f.PublishHead(a)
			l.waitCursor(t, 1)
			l.checkpoint(t, 1, 7)
			l.f.SetLatest(20)
			b := l.block(t, 8, 1)
			before := l.f.RequestCount("eth_blockNumber")
			l.f.FaultNextHTTP(kind)
			l.f.PublishHead(b)
			if err := l.f.WaitForRequests(l.ctx, "fault:"+kind, 1); err != nil {
				t.Fatal(err)
			}
			if l.f.RequestCount("eth_blockNumber") != before+1 {
				t.Fatal("published head did not reach actual RPC request")
			}
			var output string
			if kind == "timeout" {
				// A stalled real HTTP request must be canceled by the process signal;
				// cancellation must join both the service and the boundary handler.
				output = p.stop(t)
				if err := l.f.WaitForRequests(l.ctx, "fault:timeout-canceled", 1); err != nil {
					t.Fatal(err)
				}
			} else {
				output = p.join(t, liveBoundedContext(t, 15*time.Second), false)
				if !strings.Contains(output, "component chain ethereum listener") {
					t.Fatalf("missing stable component error: %s", output)
				}
			}
			if strings.Contains(output, "fixture-private-provider-payload") || strings.Contains(output, l.f.HTTPURL()) || strings.Contains(output, l.dsn) {
				t.Fatal("provider fault exposed private details")
			}
			l.checkpoint(t, 1, 7)
			if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE block_number=8"); n != 0 {
				t.Fatalf("fault advanced block rows=%d", n)
			}
			if n := l.count(t, "SELECT COUNT(*) FROM chain_raw_events WHERE block_hash=?", liveHash(107)); n != 1 {
				t.Fatalf("fault damaged committed raw rows=%d", n)
			}
			if err := l.f.WaitForNoSubscribers(l.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLiveLE20WSDisconnectCancelCleanup(t *testing.T) {
	t.Run("disconnect-error", func(t *testing.T) {
		l := newLiveLocal(t, "ethereum")
		l.f.SetLatest(19)
		a := l.block(t, 7, 1)
		p := l.start(t, 1)
		l.f.PublishHead(a)
		l.waitCursor(t, 1)
		l.checkpoint(t, 1, 7)
		l.f.DisconnectWS()
		output := p.join(t, liveBoundedContext(t, 15*time.Second), false)
		if err := l.f.WaitForNoSubscribers(l.ctx); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output, "component chain ethereum listener") {
			t.Fatalf("disconnect did not propagate: %s", output)
		}
		if strings.Contains(output, l.f.WSURL()) || strings.Contains(output, l.dsn) {
			t.Fatal("disconnect output exposed endpoint/DSN")
		}
		l.checkpoint(t, 1, 7)
		if n := l.count(t, "SELECT COUNT(*) FROM chain_blocks WHERE chain_id=1 AND block_hash=? AND status='CANONICAL'", liveHash(107)); n != 1 {
			t.Fatalf("disconnect damaged canonical block=%d", n)
		}
		if n := l.count(t, "SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND chain_status='ACTIVE'", liveHash(107)); n != 2 {
			t.Fatalf("disconnect damaged transfers=%d", n)
		}
	})

	t.Run("explicit-cancel", func(t *testing.T) {
		l := newLiveLocal(t, "ethereum")
		l.f.SetLatest(19)
		a := l.block(t, 7, 1)
		baselineConnections := l.count(t, "SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database()")
		p := l.start(t, 1)
		l.f.PublishHead(a)
		l.waitCursor(t, 1)
		l.checkpoint(t, 1, 7)
		if n := l.count(t, "SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database()"); n <= baselineConnections {
			t.Fatalf("service database peer was not connected: before=%d during=%d", baselineConnections, n)
		}
		if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		output := p.join(t, liveBoundedContext(t, 15*time.Second), true)
		if !strings.Contains(output, "service stopped") || strings.Contains(output, "component chain") {
			t.Fatalf("explicit cancel did not join peers cleanly: %s", output)
		}
		if err := l.f.WaitForNoSubscribers(l.ctx); err != nil {
			t.Fatal(err)
		}
		cleanup := liveBoundedContext(t, 5*time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			connections := l.count(t, "SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database()")
			if connections == baselineConnections {
				break
			}
			select {
			case <-ticker.C:
			case <-cleanup.Done():
				t.Fatalf("service database connection leaked: before=%d after=%d", baselineConnections, connections)
			}
		}
		l.checkpoint(t, 1, 7)
		if n := l.count(t, "SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=? AND chain_status='ACTIVE'", liveHash(107)); n != 2 {
			t.Fatalf("cancel damaged transfers=%d", n)
		}
	})
}
