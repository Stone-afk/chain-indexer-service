//go:build e2e

package testioc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

// ForkBlock freezes wire identities; the fixture only encodes standard RPC
// payloads. Branch membership, ancestry and every expected result live in tests.
type ForkBlock struct {
	Height           int64
	Hash, Parent, Tx string
}

type ForkRequestGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *ForkRequestGate) Wait(ctx context.Context) error {
	select {
	case <-g.entered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (g *ForkRequestGate) Release() { g.once.Do(func() { close(g.release) }) }

type forkRequestKey struct {
	branch string
	height int64
	full   bool
}

// ForkRPCFixture serves branch-aware HTTP and independent WS subscriptions.
// No notifier, request gate or network callback executes while mu is held.
type ForkRPCFixture struct {
	mu        sync.Mutex
	server    *rpc.Server
	http, ws  *httptest.Server
	branches  map[string]map[int64]json.RawMessage
	receipts  map[string]json.RawMessage
	branch    string
	latest    int64
	notifiers map[*rpc.Subscription]*rpc.Notifier
	changed   chan struct{}
	gates     map[forkRequestKey]*ForkRequestGate
	allGates  []*ForkRequestGate
	requests  map[forkRequestKey]int
}

func InitForkRPCFixture(t testing.TB) *ForkRPCFixture {
	t.Helper()
	f := &ForkRPCFixture{server: rpc.NewServer(), branches: make(map[string]map[int64]json.RawMessage), receipts: make(map[string]json.RawMessage), notifiers: make(map[*rpc.Subscription]*rpc.Notifier), changed: make(chan struct{}), gates: make(map[forkRequestKey]*ForkRequestGate), requests: make(map[forkRequestKey]int)}
	if err := f.server.RegisterName("eth", f); err != nil {
		t.Fatal(err)
	}
	f.http = httptest.NewServer(f.server)
	f.ws = httptest.NewServer(f.server.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		f.mu.Lock()
		gates := append([]*ForkRequestGate(nil), f.allGates...)
		f.mu.Unlock()
		for _, g := range gates {
			g.Release()
		}
		f.server.Stop()
		f.http.Close()
		f.ws.Close()
	})
	return f
}
func (f *ForkRPCFixture) HTTPURL() string { return f.http.URL }
func (f *ForkRPCFixture) WSURL() string   { return "ws" + strings.TrimPrefix(f.ws.URL, "http") }
func (f *ForkRPCFixture) changedLocked()  { close(f.changed); f.changed = make(chan struct{}) }

func (f *ForkRPCFixture) SetBranch(name string, manifest []ForkBlock) {
	blocks := make(map[int64]json.RawMessage, len(manifest))
	receipts := make(map[string]json.RawMessage, len(manifest))
	for _, b := range manifest {
		number := fmt.Sprintf("0x%x", b.Height)
		from, to, token := "0x"+strings.Repeat("1", 40), "0x"+strings.Repeat("2", 40), "0x"+strings.Repeat("3", 40)
		tx := map[string]any{"hash": b.Tx, "from": from, "to": to, "transactionIndex": "0x0", "nonce": "0x0", "type": "0x2", "gas": "0x5208", "gasPrice": "0x1", "value": "0x1", "input": "0x"}
		blocks[b.Height] = forkRaw(map[string]any{"number": number, "hash": b.Hash, "parentHash": b.Parent, "timestamp": "0x1", "gasUsed": "0x5208", "gasLimit": "0x5208", "transactions": []any{tx}})
		log := map[string]any{"address": token, "topics": []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", "0x" + strings.Repeat("0", 24) + strings.Repeat("1", 40), "0x" + strings.Repeat("0", 24) + strings.Repeat("2", 40)}, "data": "0x" + strings.Repeat("0", 63) + "1", "blockNumber": number, "blockHash": b.Hash, "transactionHash": b.Tx, "transactionIndex": "0x0", "logIndex": "0x0"}
		receipts[b.Tx] = forkRaw(map[string]any{"transactionHash": b.Tx, "blockNumber": number, "blockHash": b.Hash, "transactionIndex": "0x0", "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x1", "logs": []any{log}})
	}
	f.mu.Lock()
	f.branches[name] = blocks
	for h, r := range receipts {
		f.receipts[h] = r
	}
	f.mu.Unlock()
}
func (f *ForkRPCFixture) Select(name string, latest int64) {
	f.mu.Lock()
	f.branch, f.latest = name, latest
	f.mu.Unlock()
}
func (f *ForkRPCFixture) Switch(name string, latest int64) {
	f.Select(name, latest)
	f.Publish(name, latest)
}

// Publish deliberately leaves the HTTP view unchanged.
func (f *ForkRPCFixture) Publish(name string, height int64) {
	f.mu.Lock()
	payload, ok := f.branches[name][height]
	peers := make(map[*rpc.Subscription]*rpc.Notifier, len(f.notifiers))
	for s, n := range f.notifiers {
		peers[s] = n
	}
	f.mu.Unlock()
	if !ok {
		panic("fork head not in manifest")
	}
	for s, n := range peers {
		_ = n.Notify(s.ID, payload)
	}
}

// Waiting observes the live subscription set, including removals on restart.
func (f *ForkRPCFixture) WaitForSubscribers(ctx context.Context, want int) error {
	for {
		f.mu.Lock()
		count, changed := len(f.notifiers), f.changed
		f.mu.Unlock()
		if count == want {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (f *ForkRPCFixture) Gate(branch string, height int64, full bool) *ForkRequestGate {
	g := &ForkRequestGate{entered: make(chan struct{}), release: make(chan struct{})}
	f.mu.Lock()
	key := forkRequestKey{branch, height, full}
	if _, ok := f.gates[key]; ok {
		f.mu.Unlock()
		panic("fork gate already armed")
	}
	f.gates[key] = g
	f.allGates = append(f.allGates, g)
	f.mu.Unlock()
	return g
}
func (f *ForkRPCFixture) Requests(branch string, height int64, full bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[forkRequestKey{branch, height, full}]
}
func (f *ForkRPCFixture) WaitForRequests(ctx context.Context, branch string, height int64, full bool, want int) error {
	for {
		f.mu.Lock()
		count, changed := f.requests[forkRequestKey{branch, height, full}], f.changed
		f.mu.Unlock()
		if count >= want {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (f *ForkRPCFixture) BlockNumber() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fmt.Sprintf("0x%x", f.latest)
}
func (f *ForkRPCFixture) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	n, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, rpc.ErrNotificationsUnsupported
	}
	s := n.CreateSubscription()
	f.mu.Lock()
	f.notifiers[s] = n
	f.changedLocked()
	f.mu.Unlock()
	go func() {
		<-s.Err()
		f.mu.Lock()
		delete(f.notifiers, s)
		f.changedLocked()
		f.mu.Unlock()
	}()
	return s, nil
}
func (f *ForkRPCFixture) GetBlockByNumber(ctx context.Context, number string, full bool) (json.RawMessage, error) {
	height, err := strconv.ParseInt(strings.TrimPrefix(number, "0x"), 16, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid fork height")
	}
	f.mu.Lock()
	key := forkRequestKey{f.branch, height, full}
	value, ok := f.branches[f.branch][height]
	gate := f.gates[key]
	if gate != nil {
		delete(f.gates, key)
	}
	f.requests[key]++
	f.changedLocked()
	f.mu.Unlock()
	if gate != nil {
		close(gate.entered)
		select {
		case <-gate.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !ok {
		return nil, fmt.Errorf("fork block missing")
	}
	if full {
		return value, nil
	}
	var block struct{ Number, Hash, ParentHash, Timestamp json.RawMessage }
	if err := json.Unmarshal(value, &block); err != nil {
		return nil, err
	}
	return forkRaw(map[string]json.RawMessage{"number": block.Number, "hash": block.Hash, "parentHash": block.ParentHash, "timestamp": block.Timestamp}), nil
}
func (f *ForkRPCFixture) GetTransactionReceipt(hash string) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.receipts[hash]; ok {
		return r, nil
	}
	return nil, fmt.Errorf("fork receipt missing")
}
func forkRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
