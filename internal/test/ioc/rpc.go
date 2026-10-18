//go:build e2e

package testioc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

// RPCFixture is a small, standard JSON-RPC server used by local E2E tests.
// It deliberately binds both transports to OS-selected ports.
type RPCFixture struct {
	mu        sync.Mutex
	server    *rpc.Server
	http, ws  *httptest.Server
	latest    int64
	objects   map[string]json.RawMessage
	notifiers map[*rpc.Subscription]*rpc.Notifier
	wait      chan struct{}
	fail      error
	fault     string
	hits      map[string]int
	changed   chan struct{}
}

func InitRPCFixture(t testing.TB) *RPCFixture {
	t.Helper()
	f := &RPCFixture{server: rpc.NewServer(), objects: map[string]json.RawMessage{}, notifiers: map[*rpc.Subscription]*rpc.Notifier{}, wait: make(chan struct{}), hits: map[string]int{}, changed: make(chan struct{})}
	if err := f.server.RegisterName("eth", f); err != nil {
		t.Fatalf("register RPC fixture: %v", err)
	}
	f.http = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	f.ws = httptest.NewServer(f.server.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { f.http.Close(); f.ws.Close(); f.server.Stop() })
	return f
}

// FaultNextHTTP affects one actual HTTP request, after the caller publishes a head.
func (f *RPCFixture) FaultNextHTTP(kind string) { f.mu.Lock(); f.fault = kind; f.mu.Unlock() }
func (f *RPCFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request", 400)
		return
	}
	var call struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(b, &call)
	f.mu.Lock()
	kind := f.fault
	f.fault = ""
	f.hits[call.Method]++
	if kind != "" {
		f.hits["fault:"+kind]++
	}
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
	switch kind {
	case "429":
		http.Error(w, "fixture-private-provider-payload", http.StatusTooManyRequests)
		return
	case "timeout":
		<-r.Context().Done()
		f.mu.Lock()
		f.hits["fault:timeout-canceled"]++
		close(f.changed)
		f.changed = make(chan struct{})
		f.mu.Unlock()
		return
	case "malformed":
		_, _ = io.WriteString(w, "fixture-private-provider-payload {")
		return
	}
	r.Body = io.NopCloser(strings.NewReader(string(b)))
	f.server.ServeHTTP(w, r)
}
func (f *RPCFixture) RequestCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[method]
}
func (f *RPCFixture) WaitForRequests(ctx context.Context, method string, want int) error {
	for {
		f.mu.Lock()
		n, ch := f.hits[method], f.changed
		f.mu.Unlock()
		if n >= want {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DisconnectWS closes active transports, making the subscriber observe EOF.
func (f *RPCFixture) DisconnectWS() { f.server.Stop() }
func (f *RPCFixture) WaitForNoSubscribers(ctx context.Context) error {
	for {
		f.mu.Lock()
		n, ch := len(f.notifiers), f.wait
		f.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (f *RPCFixture) HTTPURL() string          { return f.http.URL }
func (f *RPCFixture) WSURL() string            { return "ws" + strings.TrimPrefix(f.ws.URL, "http") }
func (f *RPCFixture) SetLatest(n int64)        { f.mu.Lock(); f.latest = n; f.mu.Unlock() }
func (f *RPCFixture) PutHeader(n int64, v any) { f.put("header", n, v) }
func (f *RPCFixture) PutBlock(n int64, v any)  { f.put("block", n, v) }
func (f *RPCFixture) PutReceipt(hash string, v any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects["receipt:"+hash] = raw(v)
}
func (f *RPCFixture) put(kind string, n int64, v any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[kind+":"+strconv.FormatInt(n, 10)] = raw(v)
}
func raw(v any) json.RawMessage {
	if b, ok := v.(json.RawMessage); ok {
		return b
	}
	b, _ := json.Marshal(v)
	return b
}
func (f *RPCFixture) FailNext(err error) { f.mu.Lock(); f.fail = err; f.mu.Unlock() }
func (f *RPCFixture) WaitForSubscribers(ctx context.Context, expected ...int) error {
	want := 1
	if len(expected) > 1 {
		return fmt.Errorf("expected at most one subscriber count")
	}
	if len(expected) == 1 {
		want = expected[0]
	}
	if want < 1 {
		return fmt.Errorf("subscriber count must be positive")
	}
	for {
		f.mu.Lock()
		if len(f.notifiers) >= want {
			f.mu.Unlock()
			return nil
		}
		wait := f.wait
		f.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (f *RPCFixture) PublishHead(v any) {
	f.mu.Lock()
	peers := make(map[*rpc.Subscription]*rpc.Notifier, len(f.notifiers))
	for s, n := range f.notifiers {
		peers[s] = n
	}
	f.mu.Unlock()
	for s, n := range peers {
		_ = n.Notify(s.ID, raw(v))
	}
}

// RPC methods are intentionally permissive; tests may provide raw JSON or structs.
func (f *RPCFixture) BlockNumber() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return "0x" + strconv.FormatInt(f.latest, 16)
}

func (f *RPCFixture) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	n, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, rpc.ErrNotificationsUnsupported
	}
	s := n.CreateSubscription()
	f.mu.Lock()
	f.notifiers[s] = n
	close(f.wait)
	f.wait = make(chan struct{})
	f.mu.Unlock()
	go func() {
		<-s.Err()
		f.mu.Lock()
		delete(f.notifiers, s)
		close(f.wait)
		f.wait = make(chan struct{})
		f.mu.Unlock()
	}()
	return s, nil
}
func (f *RPCFixture) GetBlockByNumber(n string, full bool) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		e := f.fail
		f.fail = nil
		return nil, e
	}
	k := "header:" + strconv.FormatInt(parseHex(n), 10)
	if full {
		k = "block:" + strconv.FormatInt(parseHex(n), 10)
	}
	if v, ok := f.objects[k]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("fixture block object %q not found", k)
}
func (f *RPCFixture) GetTransactionReceipt(h string) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.objects["receipt:"+h]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("fixture receipt object %q not found", h)
}
func parseHex(s string) int64 {
	n, _ := strconv.ParseInt(strings.TrimPrefix(s, "0x"), 16, 64)
	return n
}
