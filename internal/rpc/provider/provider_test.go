package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
	"github.com/ethereum/go-ethereum/rpc"
)

var _ func(context.Context, string) (provider.Reader, func() error, error) = provider.NewHTTPReader
var _ func(context.Context, string) (provider.HeadSubscriber, func() error, error) = provider.NewWSSubscriber

const header = `{"number":"0x2a","hash":"0xabc","parentHash":"0xdef","timestamp":"0x64"}`
const tx = `{"hash":"0x123","transactionIndex":"0x2","from":"0x111","to":"0x222","nonce":"0x3","value":"0x10","input":"0xab","type":"0x2","gas":"0x5208","gasPrice":"0x5","accessList":[]}`
const logJSON = `{"blockNumber":"0x2a","blockHash":"0xabc","transactionHash":"0x123","transactionIndex":"0x2","logIndex":"0x4","address":"0x333","topics":["0x444"],"data":"0xab","removed":false}`
const receipt = `{"transactionHash":"0x123","status":"0x1","gasUsed":"0x5208","effectiveGasPrice":"0x5","contractAddress":null,"cumulativeGasUsed":"0x6000","logs":[` + logJSON + `]}`

func TestProviderInterfacesAreNarrow(t *testing.T) {
	for name, pair := range map[string][2]reflect.Type{
		"Reader": {reflect.TypeOf((*provider.Reader)(nil)).Elem(), reflect.TypeOf((*interface {
			LatestBlockNumber(context.Context) (int64, error)
			HeaderByNumber(context.Context, int64) (domain.BlockHeader, error)
			BlockByNumber(context.Context, int64) (domain.Block, error)
			ReceiptByTransactionHash(context.Context, string) (domain.Receipt, error)
		})(nil)).Elem()},
		"HeadSubscriber": {reflect.TypeOf((*provider.HeadSubscriber)(nil)).Elem(), reflect.TypeOf((*interface {
			SubscribeNewHeads(context.Context, chan<- domain.BlockHeader) (provider.Subscription, error)
		})(nil)).Elem()},
		"Subscription": {reflect.TypeOf((*provider.Subscription)(nil)).Elem(), reflect.TypeOf((*interface {
			Err() <-chan error
			Unsubscribe()
		})(nil)).Elem()},
	} {
		got, want := pair[0], pair[1]
		if got.Kind() != reflect.Interface || got.NumMethod() != want.NumMethod() {
			t.Fatalf("%s method set = %v", name, got)
		}
		for i := 0; i < want.NumMethod(); i++ {
			m := want.Method(i)
			actual, ok := got.MethodByName(m.Name)
			if !ok || actual.Type != m.Type {
				t.Errorf("%s.%s = %v; want %v", name, m.Name, actual.Type, m.Type)
			}
		}
	}
}

func assertJSONEqual(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var actual, expected any
	if err := json.Unmarshal(got, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("raw payload = %s; want %s", got, want)
	}
}

func TestHTTPReaderMapsStandardRPC(t *testing.T) {
	block := strings.TrimSuffix(header, "}") + `,"transactionsRoot":"0xaaa","receiptsRoot":"0xbbb","stateRoot":"0xccc","gasUsed":"0x5208","gasLimit":"0x10000","baseFeePerGas":"0x4","extraData":"0xbeef","transactions":[` + tx + `]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		result := "null"
		switch req.Method {
		case "eth_blockNumber":
			result = `"0x2a"`
		case "eth_getBlockByNumber":
			if len(req.Params) != 2 || string(req.Params[0]) != `"0x2a"` {
				t.Errorf("block params: %s", req.Params)
			}
			if string(req.Params[1]) == "true" {
				result = block
			} else {
				result = header
			}
		case "eth_getTransactionReceipt":
			if len(req.Params) != 1 || string(req.Params[0]) != `"0x123"` {
				t.Errorf("receipt params: %s", req.Params)
			}
			result = receipt
		default:
			t.Errorf("unexpected method %s", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": json.RawMessage(result)})
	}))
	defer srv.Close()
	r, cleanup, err := provider.NewHTTPReader(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if n, err := r.LatestBlockNumber(context.Background()); err != nil || n != 42 {
		t.Fatalf("latest = %d, %v", n, err)
	}
	h, err := r.HeaderByNumber(context.Background(), 42)
	if err != nil || !reflect.DeepEqual(h, domain.BlockHeader{Number: 42, Hash: "0xabc", ParentHash: "0xdef", Timestamp: 100}) {
		t.Fatalf("header = %+v, %v", h, err)
	}
	b, err := r.BlockByNumber(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if b.Header != h || b.TransactionsRoot != "0xaaa" || b.ReceiptsRoot != "0xbbb" || b.StateRoot != "0xccc" || b.GasUsed != 21000 || b.GasLimit != 65536 || b.BaseFeePerGas != "4" || len(b.Transactions) != 1 || !json.Valid(b.RawBlock) {
		t.Fatalf("block = %+v", b)
	}
	x := b.Transactions[0]
	if x.Hash != "0x123" || x.Index != 2 || x.From != "0x111" || x.To == nil || *x.To != "0x222" || x.Nonce != 3 || x.Value != "16" || x.Input != "0xab" || x.Type != 2 || x.GasLimit != 21000 || x.GasPrice != "5" || !json.Valid(x.RawTransaction) {
		t.Fatalf("transaction = %+v", x)
	}
	rc, err := r.ReceiptByTransactionHash(context.Background(), "0x123")
	if err != nil {
		t.Fatal(err)
	}
	if rc.TxHash != "0x123" || rc.Status != 1 || rc.GasUsed != 21000 || rc.EffectiveGasPrice != "5" || rc.ContractAddress != nil || len(rc.Logs) != 1 || !json.Valid(rc.RawReceipt) {
		t.Fatalf("receipt = %+v", rc)
	}
	l := rc.Logs[0]
	if l.BlockNumber != 42 || l.BlockHash != "0xabc" || l.TxHash != "0x123" || l.TxIndex != 2 || l.Index != 4 || l.Address != "0x333" || !reflect.DeepEqual(l.Topics, []string{"0x444"}) || l.Data != "0xab" || !json.Valid(l.RawLog) {
		t.Fatalf("log = %+v", l)
	}
	assertJSONEqual(t, b.RawBlock, block)
	assertJSONEqual(t, x.RawTransaction, tx)
	assertJSONEqual(t, rc.RawReceipt, receipt)
	assertJSONEqual(t, l.RawLog, logJSON)
}

type headsAPI struct{ closed chan struct{} }

func (a *headsAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	n, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, rpc.ErrNotificationsUnsupported
	}
	s := n.CreateSubscription()
	go func() { _ = n.Notify(s.ID, json.RawMessage(header)); <-s.Err(); close(a.closed) }()
	return s, nil
}

func TestWSHeadsAndLifecycle(t *testing.T) {
	for _, mode := range []string{"unsubscribe", "disconnect", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			api := &headsAPI{closed: make(chan struct{})}
			rpcServer := rpc.NewServer()
			defer rpcServer.Stop()
			if err := rpcServer.RegisterName("eth", api); err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(rpcServer.WebsocketHandler([]string{"*"}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			endpoint := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api-secret?key=query-secret"
			s, cleanup, err := provider.NewWSSubscriber(ctx, endpoint)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			heads := make(chan domain.BlockHeader, 1)
			sub, err := s.SubscribeNewHeads(ctx, heads)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-heads:
				if got != (domain.BlockHeader{Number: 42, Hash: "0xabc", ParentHash: "0xdef", Timestamp: 100}) {
					t.Fatalf("head = %+v", got)
				}
			case <-ctx.Done():
				t.Fatal("head not delivered")
			}
			switch mode {
			case "unsubscribe":
				sub.Unsubscribe()
			case "cleanup":
				if err := cleanup(); err != nil {
					t.Fatal(err)
				}
			case "disconnect":
				rpcServer.Stop()
				select {
				case err := <-sub.Err():
					if err == nil {
						t.Fatal("disconnect error missing")
					}
					assertErrorChainRedacted(t, err, endpoint, "api-secret", "query-secret")
				case <-ctx.Done():
					t.Fatal("subscription error not exposed")
				}
			}
			select {
			case <-api.closed:
			case <-ctx.Done():
				t.Fatal("subscription or connection leaked")
			}
		})
	}
}

func TestProviderErrorsRedactEndpoint(t *testing.T) {
	for _, scheme := range []string{"http", "ws"} {
		t.Run(scheme, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, r.URL.String(), http.StatusUnauthorized) }))
			defer srv.Close()
			endpoint := scheme + strings.TrimPrefix(srv.URL, "http") + "/api-secret?key=query-secret"
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var err error
			if scheme == "http" {
				var r provider.Reader
				var cleanup func() error
				r, cleanup, err = provider.NewHTTPReader(ctx, endpoint)
				if err == nil {
					defer cleanup()
					_, err = r.LatestBlockNumber(ctx)
				}
			} else {
				_, _, err = provider.NewWSSubscriber(ctx, endpoint)
			}
			if err == nil {
				t.Fatal("expected endpoint error")
			}
			assertErrorChainRedacted(t, err, endpoint, "api-secret", "query-secret")
		})
	}
}

func assertErrorChainRedacted(t *testing.T, err error, secrets ...string) {
	t.Helper()
	for depth := 0; err != nil; depth, err = depth+1, errors.Unwrap(err) {
		for _, secret := range secrets {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error chain depth %d exposes secret %q: %v", depth, secret, err)
			}
		}
	}
}

func TestProviderConstructorErrorChainRedactsSecrets(t *testing.T) {
	for _, scheme := range []string{"http", "ws"} {
		t.Run(scheme, func(t *testing.T) {
			endpoint := scheme + "://user:password@example.invalid/%api-secret?key=query-secret"
			var err error
			if scheme == "http" {
				_, _, err = provider.NewHTTPReader(context.Background(), endpoint)
			} else {
				_, _, err = provider.NewWSSubscriber(context.Background(), endpoint)
			}
			if err == nil {
				t.Fatal("expected constructor error")
			}
			assertErrorChainRedacted(t, err, endpoint, "user", "password", "api-secret", "query-secret")
		})
	}
}
