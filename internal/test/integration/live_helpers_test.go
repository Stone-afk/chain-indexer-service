//go:build e2e

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/config"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"github.com/Stone-afk/chain-indexer-service/internal/rpc/provider"
	"github.com/Stone-afk/chain-indexer-service/internal/scanner"
	testioc "github.com/Stone-afk/chain-indexer-service/internal/test/ioc"
	"github.com/Stone-afk/chain-indexer-service/internal/worker/transaction"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestLiveInfuraParentByHashContract(t *testing.T) {
	const height = int64(79731593)
	const hash = "0x25c31df6e505ab5377935fd7d7eb1cdb06b1672054e636d2a7ce410e865f4c96"
	var mu sync.Mutex
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error("fixture_request_decode")
			return
		}
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if request.Method == "eth_getBlockByNumber" {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32603, "message": "unreadable by number"}})
			return
		}
		if request.Method != "eth_getBlockByHash" || len(request.Params) != 2 || string(request.Params[0]) != `"`+hash+`"` || string(request.Params[1]) != "false" {
			t.Error("fixture_requires_fixed_parent_hash")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"number": fmt.Sprintf("0x%x", height), "hash": hash, "parentHash": hashOf('1'), "timestamp": "0x64"}})
	}))
	defer server.Close()
	s := &infuraSuite{c: &config.Config{Chains: map[string]config.ChainConfig{"polygon": {HTTPURL: server.URL}}}, deadline: time.Now().Add(10 * time.Second), maximum: 200, parents: map[string]domain.BlockHeader{}}
	h := s.header(t, "polygon", height, hash)
	infuraEqual(t, "fixed_parent_header", h, domain.BlockHeader{Number: height, Hash: hash, ParentHash: hashOf('1'), Timestamp: 100})
	infuraEqual(t, "fixed_parent_cache", s.header(t, "polygon", height, hash), h)
	mu.Lock()
	got := append([]string(nil), methods...)
	mu.Unlock()
	infuraEqual(t, "fixed_parent_exact_double_read", got, []string{"eth_getBlockByHash", "eth_getBlockByHash"})
	infuraEqual(t, "fixed_parent_action_count", s.requests, 2)
}

func TestLiveInfuraParentShapeValidation(t *testing.T) {
	hash := hashOf('2')
	valid := fmt.Sprintf(`{"number":"0x2a","hash":%q,"parentHash":%q,"timestamp":"0x64"}`, hash, hashOf('1'))
	for _, tc := range []struct {
		name, raw string
		height    int64
		hash      string
		fail      bool
	}{
		{"valid", valid, 42, hash, false}, {"wrong_height", valid, 43, hash, true}, {"wrong_hash", valid, 42, hashOf('3'), true},
		{"null", "null", 42, hash, true}, {"malformed", `{"timestamp":"private-provider-detail"}`, 42, hash, true},
		{"zero_timestamp", fmt.Sprintf(`{"number":"0x2a","hash":%q,"parentHash":%q,"timestamp":"0x0"}`, hash, hashOf('1')), 42, hash, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := infuraDecodeParent([]byte(tc.raw), tc.height, tc.hash)
			if (err != nil) != tc.fail {
				t.Fatal("parent_shape_classification")
			}
			if err != nil && err.Error() != "fixed_parent_shape" && err.Error() != "fixed_parent_decode" {
				t.Fatal("parent_error_not_redacted")
			}
		})
	}
}

func TestLiveInfuraRPCDuplicateObjectKeys(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     any
		duplicate bool
	}{
		{"root", json.RawMessage(`{"hash":"first","hash":"last"}`), true},
		{"nested_array", domain.Block{RawBlock: json.RawMessage(`{"transactions":[{"unknown":{"a":1,"a":2}}]}`)}, true},
		{"transaction", domain.Transaction{RawTransaction: json.RawMessage(`{"value":"0x1","value":"0x1"}`)}, true},
		{"receipt", domain.Receipt{RawReceipt: json.RawMessage(`{"logs":[{"data":"0x1","data":"0x2"}]}`)}, true},
		{"log_escaped_key", domain.Log{RawLog: json.RawMessage(`{"data":"0x1","\u0064ata":"0x2"}`)}, true},
		{"independent_objects", json.RawMessage(`[{"a":1},{"a":2}]`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := infuraRPCSnapshot(tc.value)
			if (err != nil) != tc.duplicate {
				t.Fatal("duplicate_object_key_not_rejected")
			}
		})
	}
}

func TestLiveInfuraRPCDoubleReadSemantics(t *testing.T) {
	first := domain.Block{
		Header:       domain.BlockHeader{Number: 52766047, Hash: hashOf('1'), Timestamp: 100},
		Transactions: []domain.Transaction{{Hash: hashOf('2'), Value: "10", RawTransaction: json.RawMessage(`{"value":"0xa","type":"0x2"}`)}},
		RawBlock:     json.RawMessage(`{"transactions":[{"hash":"a"},{"hash":"b"}],"optional":null,"precision":9007199254740993}`),
	}
	reordered := first
	reordered.RawBlock = json.RawMessage(`{ "precision":9007199254740993,"optional":null,"transactions":[{"hash":"a"},{"hash":"b"}] }`)
	reordered.Transactions = append([]domain.Transaction(nil), first.Transactions...)
	reordered.Transactions[0].RawTransaction = json.RawMessage(`{"type":"0x2", "value":"0xa"}`)
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Block)
		same   bool
	}{
		{"object_order", func(*domain.Block) {}, true},
		{"mapped_identity", func(b *domain.Block) { b.Header.Hash = hashOf('3') }, false},
		{"mapped_value", func(b *domain.Block) { b.Transactions[0].Value = "11" }, false},
		{"raw_transaction_value", func(b *domain.Block) {
			b.Transactions[0].RawTransaction = json.RawMessage(`{"type":"0x2","value":"0xb"}`)
		}, false},
		{"optional_missing", func(b *domain.Block) {
			b.RawBlock = json.RawMessage(strings.ReplaceAll(string(b.RawBlock), `"optional":null,`, ""))
		}, false},
		{"optional_value", func(b *domain.Block) {
			b.RawBlock = json.RawMessage(strings.ReplaceAll(string(b.RawBlock), `"optional":null`, `"optional":0`))
		}, false},
		{"large_integer", func(b *domain.Block) {
			b.RawBlock = json.RawMessage(strings.ReplaceAll(string(b.RawBlock), "9007199254740993", "9007199254740992"))
		}, false},
		{"transaction_order", func(b *domain.Block) {
			b.RawBlock = json.RawMessage(strings.ReplaceAll(string(b.RawBlock), `[{"hash":"a"},{"hash":"b"}]`, `[{"hash":"b"},{"hash":"a"}]`))
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := reordered
			candidate.Transactions = append([]domain.Transaction(nil), reordered.Transactions...)
			tc.mutate(&candidate)
			a, err := infuraRPCSnapshot(first)
			if err != nil {
				t.Fatal(err)
			}
			b, err := infuraRPCSnapshot(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.DeepEqual(a, b) != tc.same {
				t.Fatal("rpc_semantic_comparison")
			}
		})
	}
	if !bytes.Equal(first.RawBlock, []byte(`{"transactions":[{"hash":"a"},{"hash":"b"}],"optional":null,"precision":9007199254740993}`)) {
		t.Fatal("source_payload_mutated")
	}
	// Receipt/log and parent RawMessages use the identical comparison path.
	a := domain.Receipt{RawReceipt: json.RawMessage(`{"status":"0x1","logs":[{"data":"0x","removed":false}]}`), Logs: []domain.Log{{RawLog: json.RawMessage(`{"data":"0x","removed":false}`)}}}
	b := domain.Receipt{RawReceipt: json.RawMessage(`{"logs":[{"removed":false,"data":"0x"}],"status":"0x1"}`), Logs: []domain.Log{{RawLog: json.RawMessage(`{"removed":false,"data":"0x"}`)}}}
	av, err := infuraRPCSnapshot(a)
	if err != nil {
		t.Fatal(err)
	}
	bv, err := infuraRPCSnapshot(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(av, bv) {
		t.Fatal("receipt_log_object_order")
	}
}

var infuraNames = []string{"ethereum", "bsc", "polygon", "arbitrum", "optimism", "base", "avalanche"}
var infuraLock sync.Mutex
var infuraRun *infuraSuite

type infuraSuite struct {
	m                           manifest
	c                           *config.Config
	deadline, next              time.Time
	requests, estimate, maximum int
	failed                      bool
	blocks                      map[string]domain.Block
	parents                     map[string]domain.BlockHeader
	receipts                    map[string]domain.Receipt
}

// Only T046 may execute this gate. In ordinary make e2e the skip occurs before
// opening either the ignored configuration or a network connection.
func infuraSuiteFor(t *testing.T) *infuraSuite {
	t.Helper()
	if os.Getenv("CHAIN_INDEXER_LIVE_E2E") != "1" {
		t.Skip("live opt-in required")
	}
	infuraLock.Lock()
	t.Cleanup(infuraLock.Unlock)
	t.Cleanup(func() {
		if t.Failed() && infuraRun != nil {
			infuraRun.failed = true
		}
	})
	if infuraRun != nil {
		if infuraRun.failed {
			t.Fatal("live_suite_previous_failure")
		}
		if !time.Now().Before(infuraRun.deadline) {
			t.Fatal("suite_deadline")
		}
		return infuraRun
	}
	s := &infuraSuite{deadline: time.Now().Add(10 * time.Minute), blocks: map[string]domain.Block{}, parents: map[string]domain.BlockHeader{}, receipts: map[string]domain.Receipt{}}
	b, err := os.ReadFile(filepath.Join(repositoryRoot(t), "internal/test/integration/testdata/infura_samples.json"))
	infuraOK(t, err, "manifest_read")
	digest, err := manifestIdentityDigest(b)
	infuraOK(t, err, "manifest_identity")
	if fmt.Sprintf("%x", digest) != approvedManifestIdentitySHA256 {
		t.Fatal("manifest_identity_drift")
	}
	infuraOK(t, json.Unmarshal(b, &s.m), "manifest_decode")
	if !s.m.Preflight || s.m.Acceptance != "deferred_to_T046" || len(s.m.Chains) != 7 {
		t.Fatal("manifest_unverified")
	}
	p := os.Getenv("CHAIN_INDEXER_LIVE_CONFIG")
	if p == "" {
		p = localConfigPath(t)
	}
	s.c, err = config.LoadForService(p, func(n string) (string, bool) {
		if n == "POSTGRES_DSN" {
			return "postgres://dummy:dummy@127.0.0.1:5432/dummy", true
		}
		return os.LookupEnv(n)
	})
	infuraOK(t, err, "config_invalid")
	var key string
	for _, name := range infuraNames {
		c, ok := s.c.Chains[name]
		m := s.m.Chains[name]
		if !ok || !c.Enabled || c.ChainID != m.ChainID || c.StartBlock != m.StartBlock || c.StartBlock <= 0 {
			t.Fatal("config_manifest_mismatch: " + name)
		}
		for _, endpoint := range []struct{ value, scheme, host, pattern string }{{c.HTTPURL, "https", m.HTTPHost, `^/v3/([0-9a-fA-F]{32})$`}, {c.WSURL, "wss", m.WSHost, `^/ws/v3/([0-9a-fA-F]{32})$`}} {
			u, e := url.Parse(endpoint.value)
			if e != nil || u.Scheme != endpoint.scheme || u.Host != endpoint.host || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
				t.Fatal("endpoint_invalid: " + name)
			}
			parts := regexp.MustCompile(endpoint.pattern).FindStringSubmatch(u.Path)
			if len(parts) != 2 || (key != "" && parts[1] != key) {
				t.Fatal("endpoint_key_invalid: " + name)
			}
			key = parts[1]
		}
	}
	// Conservative suite estimate, not a statement of Infura's billing prices:
	// 100 per JSON-RPC action, plus 1000 reserved per single-head WSS session.
	// Unique sample blocks, parent blocks and receipts are each read
	// twice. Cache hits add no RPCs; chainId and subscribe/unsubscribe add 4/chain.
	blocks, parents, receipts := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, name := range infuraNames {
		c := s.m.Chains[name]
		samples := append([]manifestSample(nil), c.Samples...)
		for _, block := range c.ScannerBlocks {
			samples = append(samples, block.Transactions...)
		}
		for _, x := range samples {
			blocks[infuraKey(name, x.BlockNumber)] = true
			parents[name+"/"+x.ParentHash] = true
			receipts[name+"/"+x.TxHash] = true
		}
		if c.ConfigStartBlock.BlockNumber != c.StartBlock {
			t.Fatal("config_start_manifest_mismatch")
		}
	}
	s.maximum = (2*(len(blocks)+len(parents)+len(receipts))+4*len(infuraNames))*100 + len(infuraNames)*1000
	if s.maximum > 25000 {
		t.Fatal("request_credit_estimate_exceeds_25000")
	}
	infuraRun = s
	t.Logf("conservative_suite_request_credit_estimate=%d limit=25000", s.maximum)
	return s
}

func infuraOK(t *testing.T, err error, class string) {
	t.Helper()
	if err != nil {
		if infuraRun != nil {
			infuraRun.failed = true
		}
		t.Fatal(class)
	}
}
func infuraEqual(t *testing.T, label string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		if infuraRun != nil {
			infuraRun.failed = true
		}
		t.Fatal("fixed_field_mismatch: " + label)
	}
}

// DTO fields and every embedded raw RPC field remain part of the comparison.
// Only JSON object member order/whitespace are immaterial. UseNumber preserves
// exact numeric text; missing/null, optional values and array order stay strict.
// Marshal/decode creates a comparison snapshot without changing cached payloads.
func infuraRPCSnapshot(value any) (any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	// Marshal retains embedded RawMessage object members. Reject duplicates in
	// every nested object before a map decoder can discard an earlier value.
	check := json.NewDecoder(bytes.NewReader(raw))
	check.UseNumber()
	if err := infuraRejectDuplicateKeys(check); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var snapshot any
	err = decoder.Decode(&snapshot)
	return snapshot, err
}

func infuraRejectDuplicateKeys(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	opening, container := token.(json.Delim)
	if !container {
		return nil
	}
	var closing json.Delim
	switch opening {
	case '{':
		closing = '}'
		seen := map[string]bool{}
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return fmt.Errorf("invalid_rpc_object_key")
			}
			if seen[key] {
				return fmt.Errorf("duplicate_rpc_object_key")
			}
			seen[key] = true
			if err := infuraRejectDuplicateKeys(decoder); err != nil {
				return err
			}
		}
	case '[':
		closing = ']'
		for decoder.More() {
			if err := infuraRejectDuplicateKeys(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid_rpc_json_delimiter")
	}
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if token != closing {
		return fmt.Errorf("invalid_rpc_json_delimiter")
	}
	return nil
}

func infuraRPCEqual(t *testing.T, label string, a, b any) {
	t.Helper()
	left, err := infuraRPCSnapshot(a)
	infuraOK(t, err, "rpc_comparison_json")
	right, err := infuraRPCSnapshot(b)
	infuraOK(t, err, "rpc_comparison_json")
	infuraEqual(t, label, left, right)
}
func infuraKey(name string, height int64) string { return name + "/" + strconv.FormatInt(height, 10) }
func infuraText(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (s *infuraSuite) context(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	if s.deadline.Before(deadline) {
		deadline = s.deadline
	}
	if !time.Now().Before(deadline) {
		t.Fatal("suite_deadline")
	}
	return context.WithDeadline(context.Background(), deadline)
}

// The suite lock and absence of t.Parallel serialize all paid actions, including
// WSS subscribe/unsubscribe. There is no RPC retry or sample replacement.
func (s *infuraSuite) action(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	if s.failed {
		t.Fatal("live_suite_previous_failure")
	}
	t.Cleanup(func() {
		if t.Failed() {
			s.failed = true
		}
	})
	ctx, cancel := s.context(t)
	if d := time.Until(s.next); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			cancel()
			t.Fatal("rate_wait_deadline")
		}
	}
	if s.estimate+100 > s.maximum {
		cancel()
		t.Fatal("request_credit_estimate_exceeded")
	}
	s.next = time.Now().Add(time.Second)
	s.requests++
	s.estimate += 100
	return ctx, cancel
}

func (s *infuraSuite) reader(t *testing.T, name string) provider.Reader {
	t.Helper()
	ctx, cancel := s.context(t)
	defer cancel()
	r, closeReader, err := provider.NewHTTPReader(ctx, s.c.Chains[name].HTTPURL)
	infuraOK(t, err, "http_connect: "+name)
	t.Cleanup(func() { infuraOK(t, closeReader(), "http_cleanup: "+name) })
	return r
}

func (s *infuraSuite) block(t *testing.T, name string, height int64) domain.Block {
	t.Helper()
	key := infuraKey(name, height)
	if b, ok := s.blocks[key]; ok {
		return b
	}
	r := s.reader(t, name)
	var pair [2]domain.Block
	for i := range pair {
		ctx, cancel := s.action(t)
		b, err := r.BlockByNumber(ctx, height)
		cancel()
		infuraOK(t, err, "fixed_block_rpc: "+name)
		pair[i] = b
	}
	infuraRPCEqual(t, "complete_block_double_read", pair[0], pair[1])
	if pair[0].Header.Number != height || pair[0].Header.Timestamp <= 0 {
		t.Fatal("fixed_block_shape")
	}
	s.blocks[key] = pair[0]
	return pair[0]
}

func (s *infuraSuite) header(t *testing.T, name string, height int64, hash string) domain.BlockHeader {
	t.Helper()
	key := name + "/" + hash
	if h, ok := s.parents[key]; ok {
		infuraEqual(t, "cached_parent_height", h.Number, height)
		return h
	}
	ctx, cancel := s.context(t)
	client, err := rpc.DialContext(ctx, s.c.Chains[name].HTTPURL)
	cancel()
	infuraOK(t, err, "fixed_parent_connect: "+name)
	defer client.Close()
	// The method and identifier are fixed before any request. Polygon's frozen
	// parent can be read by hash while a by-number shape returns -32603; there
	// is no by-number attempt, fallback, retry, or dynamic sample discovery.
	var pair [2]json.RawMessage
	for i := range pair {
		ctx, cancel := s.action(t)
		err := client.CallContext(ctx, &pair[i], "eth_getBlockByHash", hash, false)
		cancel()
		infuraOK(t, err, fmt.Sprintf("fixed_parent_rpc: %s height=%d", name, height))
	}
	infuraRPCEqual(t, "complete_parent_header_double_read", pair[0], pair[1])
	h, err := infuraDecodeParent(pair[0], height, hash)
	infuraOK(t, err, "fixed_parent_decode_or_identity: "+name)
	s.parents[key] = h
	return h
}

func infuraDecodeParent(raw []byte, height int64, hash string) (domain.BlockHeader, error) {
	var wire struct {
		Number, Timestamp hexutil.Uint64
		Hash, ParentHash  string
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return domain.BlockHeader{}, fmt.Errorf("fixed_parent_decode")
	}
	h := domain.BlockHeader{Number: int64(wire.Number), Timestamp: int64(wire.Timestamp), Hash: wire.Hash, ParentHash: wire.ParentHash}
	hashShape := regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
	if h.Number <= 0 || h.Number != height || h.Hash != hash || h.Timestamp <= 0 || !hashShape.MatchString(h.Hash) || !hashShape.MatchString(h.ParentHash) {
		return domain.BlockHeader{}, fmt.Errorf("fixed_parent_shape")
	}
	return h, nil
}

func (s *infuraSuite) receipt(t *testing.T, name string, sample manifestSample) domain.Receipt {
	t.Helper()
	key := name + "/" + sample.TxHash
	if r, ok := s.receipts[key]; ok {
		return r
	}
	r := s.reader(t, name)
	var pair [2]domain.Receipt
	for i := range pair {
		ctx, cancel := s.action(t)
		receipt, err := r.ReceiptByTransactionHash(ctx, sample.TxHash)
		cancel()
		infuraOK(t, err, "fixed_receipt_rpc: "+name)
		pair[i] = receipt
	}
	infuraRPCEqual(t, "complete_receipt_double_read", pair[0], pair[1])
	s.receipts[key] = pair[0]
	return pair[0]
}

func (s *infuraSuite) sample(t *testing.T, name string, x manifestSample) (domain.Block, domain.Transaction, domain.Receipt) {
	t.Helper()
	b := s.block(t, name, x.BlockNumber)
	r := s.receipt(t, name, x)
	infuraEqual(t, "block_number", b.Header.Number, x.BlockNumber)
	infuraEqual(t, "block_hash", b.Header.Hash, x.BlockHash)
	infuraEqual(t, "parent_hash", b.Header.ParentHash, x.ParentHash)
	infuraEqual(t, "block_timestamp", b.Header.Timestamp, x.BlockTimestamp)
	infuraEqual(t, "full_block_transaction_count", len(b.Transactions), x.BlockTransactionCount)
	p := s.header(t, name, x.ParentNumber, x.ParentHash)
	infuraEqual(t, "parent_number", p.Number, x.ParentNumber)
	infuraEqual(t, "parent_hash_identity", p.Hash, x.ParentHash)
	if p.Timestamp <= 0 || p.Timestamp > b.Header.Timestamp {
		t.Fatal("parent_timestamp")
	}
	if x.TxIndex < 0 || x.TxIndex >= int64(len(b.Transactions)) {
		t.Fatal("transaction_position")
	}
	tx := b.Transactions[x.TxIndex]
	want := domain.Transaction{Hash: x.TxHash, Index: int(x.TxIndex), From: x.From, To: x.To, Nonce: x.Nonce, Value: x.Value, Input: x.Input, Type: x.TxType, GasLimit: infuraDecimal(t, x.GasLimit), GasPrice: infuraText(x.GasPrice), RawTransaction: tx.RawTransaction}
	infuraEqual(t, "transaction_fields", tx, want)
	infuraEqual(t, "receipt_block_number", r.BlockNumber, x.BlockNumber)
	infuraEqual(t, "receipt_block_hash", r.BlockHash, x.BlockHash)
	infuraEqual(t, "receipt_tx_hash", r.TxHash, x.TxHash)
	infuraEqual(t, "receipt_tx_index", r.TxIndex, int(x.TxIndex))
	infuraEqual(t, "receipt_status", r.Status, x.ReceiptStatus)
	infuraEqual(t, "receipt_gas", r.GasUsed, infuraDecimal(t, x.GasUsed))
	infuraEqual(t, "receipt_price", r.EffectiveGasPrice, infuraText(x.EffectiveGasPrice))
	infuraEqual(t, "receipt_contract", r.ContractAddress, x.ContractAddress)
	infuraEqual(t, "receipt_log_count", len(r.Logs), len(x.Logs))
	for i, l := range r.Logs {
		w := x.Logs[i]
		infuraEqual(t, "log_fields", l, domain.Log{BlockNumber: w.BlockNumber, BlockHash: w.BlockHash, TxHash: w.TxHash, TxIndex: int(w.TxIndex), Index: int(w.LogIndex), Address: w.Contract, Topics: w.Topics, Data: w.Data, RawLog: l.RawLog})
		infuraEqual(t, "expected_log_index", int64(l.Index), x.ExpectedLogIndices[i])
		var raw struct {
			Removed bool `json:"removed"`
		}
		infuraOK(t, json.Unmarshal(l.RawLog, &raw), "raw_log_decode")
		infuraEqual(t, "removed", raw.Removed, w.Removed)
	}
	return b, tx, r
}

func infuraDecimal(t *testing.T, text string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(text, 10, 64)
	infuraOK(t, err, "manifest_decimal")
	return n
}

// This one-height reader replays only double-read frozen provider responses.
// Sample-scoped callers explicitly select one transaction; LE12 supplies the
// complete block and every receipt. Neither mode changes any identity/raw field.
type infuraFrozenWindow struct {
	block    domain.Block
	receipts map[string]domain.Receipt
}

func (r infuraFrozenWindow) LatestBlockNumber(context.Context) (int64, error) {
	return r.block.Header.Number, nil
}
func (r infuraFrozenWindow) HeaderByNumber(_ context.Context, n int64) (domain.BlockHeader, error) {
	if n != r.block.Header.Number {
		return domain.BlockHeader{}, fmt.Errorf("projection_height")
	}
	return r.block.Header, nil
}
func (r infuraFrozenWindow) BlockByNumber(_ context.Context, n int64) (domain.Block, error) {
	if n != r.block.Header.Number {
		return domain.Block{}, fmt.Errorf("projection_height")
	}
	return r.block, nil
}
func (r infuraFrozenWindow) ReceiptByTransactionHash(_ context.Context, h string) (domain.Receipt, error) {
	receipt, ok := r.receipts[h]
	if !ok {
		return domain.Receipt{}, fmt.Errorf("projection_transaction")
	}
	return receipt, nil
}

// LE08–11/13 verify sample-scoped ingestion, not complete-block ingestion.
// The original full provider block remains in raw_block and is independently
// verified by LE06/07. The persisted transaction_count=1 describes this scope.
func (s *infuraSuite) ingest(t *testing.T, name string, x manifestSample) *gorm.DB {
	t.Helper()
	b, tx, r := s.sample(t, name, x)
	b.Transactions = []domain.Transaction{tx}
	db := s.ingestWindow(t, b, []domain.Receipt{r}, []manifestSample{x})
	var count int
	infuraOK(t, db.Raw("SELECT transaction_count FROM chain_blocks WHERE chain_id=? AND block_hash=?", x.ChainID, x.BlockHash).Scan(&count).Error, "sample_scoped_block_query")
	infuraEqual(t, "sample_scoped_transaction_count", count, 1)
	return db
}

func (s *infuraSuite) ingestComplete(t *testing.T, name string, frozen manifestScannerBlock) *gorm.DB {
	t.Helper()
	b := s.block(t, name, frozen.Block.BlockNumber)
	infuraEqual(t, "complete_frozen_transaction_count", len(b.Transactions), frozen.Block.TransactionCount)
	infuraEqual(t, "complete_manifest_transaction_set", len(frozen.Transactions), len(b.Transactions))
	receipts := make([]domain.Receipt, len(frozen.Transactions))
	for i, x := range frozen.Transactions {
		_, tx, receipt := s.sample(t, name, x)
		infuraEqual(t, "complete_transaction_position", tx.Index, i)
		infuraEqual(t, "complete_transaction_hash", b.Transactions[i].Hash, x.TxHash)
		receipts[i] = receipt
	}
	// No filtering or replacement of b.Transactions: the real scanner consumes
	// both complete transactions and requests both original provider receipts.
	return s.ingestWindow(t, b, receipts, frozen.Transactions)
}

func (s *infuraSuite) ingestWindow(t *testing.T, b domain.Block, receipts []domain.Receipt, samples []manifestSample) *gorm.DB {
	t.Helper()
	ctx, cancel := s.context(t)
	t.Cleanup(cancel)
	// This DSN is the existing dedicated Compose definition, never a user DB.
	db := testioc.InitDB(t, ctx, config.DatabaseConfig{DSN: "postgres://postgres:postgres@127.0.0.1:15432/chain_indexer_e2e?sslmode=disable"}).Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}).WithContext(ctx)
	testioc.ResetSchema(t, db)
	_, err := testioc.InitMigrationRunner(t, db, migrationDir(t)).Up(ctx)
	infuraOK(t, err, "live_migration")
	chain := repository.NewChainRepository(postgre.NewChainDAO(db))
	x := samples[0]
	reader := infuraFrozenWindow{block: b, receipts: map[string]domain.Receipt{}}
	var rawReceipts []json.RawMessage
	logCount := 0
	for _, receipt := range receipts {
		reader.receipts[receipt.TxHash] = receipt
		rawReceipts = append(rawReceipts, receipt.RawReceipt)
		logCount += len(receipt.Logs)
	}
	infuraOK(t, scanner.New(reader, chain, x.ChainID, b.Header.Number, 0).HandleNewHead(ctx), "frozen_scanner")
	cp, found, err := chain.GetCheckpoint(ctx, x.ChainID)
	infuraOK(t, err, "frozen_checkpoint")
	infuraEqual(t, "frozen_checkpoint_found", found, true)
	infuraEqual(t, "frozen_checkpoint_height", cp.LastProcessedHeight, b.Header.Number)
	infuraEqual(t, "frozen_checkpoint_hash", cp.LastProcessedHash, b.Header.Hash)
	var blocks []postgre.ChainBlock
	infuraOK(t, db.Where("chain_id=? AND block_hash=?", x.ChainID, b.Header.Hash).Find(&blocks).Error, "stored_block_query")
	infuraEqual(t, "stored_block_unique", len(blocks), 1)
	stored := blocks[0]
	infuraEqual(t, "stored_block_transaction_count", stored.TransactionCount, len(b.Transactions))
	infuraEqual(t, "stored_block_parent", stored.ParentHash, b.Header.ParentHash)
	infuraEqual(t, "stored_block_state", stored.Status, "CANONICAL")
	if stored.RawBlock == nil || stored.RawReceipts == nil {
		t.Fatal("stored_block_raw_missing")
	}
	infuraJSON(t, stored.RawBlock.RawMessage(), b.RawBlock)
	wantReceipts, err := json.Marshal(rawReceipts)
	infuraOK(t, err, "receipt_array_encode")
	infuraJSON(t, stored.RawReceipts.RawMessage(), wantReceipts)
	var txCount int64
	infuraOK(t, db.Raw("SELECT COUNT(*) FROM chain_transactions WHERE chain_id=? AND block_hash=?", x.ChainID, b.Header.Hash).Scan(&txCount).Error, "stored_block_tx_count")
	infuraEqual(t, "stored_block_all_transactions", txCount, int64(len(b.Transactions)))
	var rawRows []postgre.ChainRawEvent
	infuraOK(t, db.Where("chain_id=? AND block_hash=?", x.ChainID, b.Header.Hash).Order("log_index").Find(&rawRows).Error, "frozen_raw_query")
	infuraEqual(t, "frozen_raw_count", len(rawRows), logCount)
	var logs []domain.Log
	for _, receipt := range receipts {
		logs = append(logs, receipt.Logs...)
	}
	for i, raw := range rawRows {
		l := logs[i]
		if raw.ID <= 0 || (i > 0 && raw.ID <= rawRows[i-1].ID) {
			t.Fatal("projected_raw_order")
		}
		infuraEqual(t, "stored_log_index", raw.LogIndex, l.Index)
		infuraEqual(t, "stored_log_block", raw.BlockNumber, l.BlockNumber)
		infuraEqual(t, "stored_log_tx_hash", raw.TxHash, l.TxHash)
		infuraEqual(t, "stored_log_tx_index", raw.TxIndex, &l.TxIndex)
		infuraEqual(t, "stored_log_contract", raw.ContractAddress, l.Address)
		infuraEqual(t, "stored_log_data", raw.EventData, &l.Data)
		infuraJSON(t, raw.RawLog.RawMessage(), l.RawLog)
	}
	events := repository.NewEventRepository(postgre.NewEventDAO(db))
	parser, err := transaction.NewParser()
	infuraOK(t, err, "worker_parser")
	worker := transaction.NewWorker(events, parser, 7, time.Second)
	for page := 0; page <= logCount/7; page++ {
		_, err = worker.ProcessBatch(ctx)
		infuraOK(t, err, "worker_page")
	}
	var max, cursor int64
	infuraOK(t, db.Raw("SELECT COALESCE(MAX(id),0) FROM chain_raw_events").Scan(&max).Error, "raw_cursor_query")
	cursor, err = events.GetBizCursor(ctx, "transaction", "evm")
	infuraOK(t, err, "cursor_query")
	infuraEqual(t, "consumed_cursor", cursor, max)
	for i, x := range samples {
		infuraStoredTransaction(t, db, x, b.Transactions[i], receipts[i])
	}
	return db
}

func infuraStoredTransaction(t *testing.T, db *gorm.DB, x manifestSample, tx domain.Transaction, r domain.Receipt) {
	t.Helper()
	var rows []postgre.ChainTransaction
	infuraOK(t, db.Where("chain_id=? AND block_hash=? AND tx_hash=?", x.ChainID, x.BlockHash, x.TxHash).Find(&rows).Error, "transaction_query")
	infuraEqual(t, "single_transaction_fact", len(rows), 1)
	got := rows[0]
	infuraEqual(t, "stored_from", got.FromAddress, x.From)
	infuraEqual(t, "stored_to", got.ToAddress, x.To)
	infuraEqual(t, "stored_value", got.Value, x.Value)
	infuraEqual(t, "stored_contract", got.ContractAddress, x.ContractAddress)
	infuraEqual(t, "stored_nonce", got.Nonce, x.Nonce)
	infuraEqual(t, "stored_input", got.Input, &x.Input)
	infuraEqual(t, "stored_gas_limit", got.GasLimit, &x.GasLimit)
	infuraEqual(t, "stored_gas_used", got.GasUsed, &x.GasUsed)
	infuraEqual(t, "stored_gas_price", got.GasPrice, x.GasPrice)
	infuraEqual(t, "stored_effective_gas_price", got.EffectiveGasPrice, x.EffectiveGasPrice)
	infuraEqual(t, "stored_tx_index", got.TxIndex, int(x.TxIndex))
	infuraEqual(t, "stored_block", got.BlockNumber, x.BlockNumber)
	infuraEqual(t, "stored_time", got.BlockTime, x.BlockTimestamp)
	infuraEqual(t, "stored_transaction_state", got.ChainStatus, "ACTIVE")
	if got.ReorgID != nil || got.RevertedAt != nil {
		t.Fatal("stored_transaction_metadata")
	}
	status := "FAILED"
	if x.ReceiptStatus == 1 {
		status = "SUCCESS"
	}
	infuraEqual(t, "execution_status", got.ExecutionStatus, status)
	if got.RawTransaction == nil || got.RawReceipt == nil {
		t.Fatal("stored_raw_missing")
	}
	infuraJSON(t, got.RawTransaction.RawMessage(), tx.RawTransaction)
	infuraJSON(t, got.RawReceipt.RawMessage(), r.RawReceipt)
}

func infuraJSON(t *testing.T, a, b []byte) {
	t.Helper()
	var av, bv any
	da, db := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	da.UseNumber()
	db.UseNumber()
	infuraOK(t, da.Decode(&av), "stored_json")
	infuraOK(t, db.Decode(&bv), "source_json")
	infuraEqual(t, "json_semantics", av, bv)
}

func infuraTransfers(t *testing.T, db *gorm.DB, x manifestSample) {
	t.Helper()
	var rows []postgre.ChainTransfer
	infuraOK(t, db.Where("chain_id=? AND block_hash=? AND tx_hash=?", x.ChainID, x.BlockHash, x.TxHash).Order("source_type, source_index, batch_index").Find(&rows).Error, "transfer_query")
	wantNative := 0
	if x.ReceiptStatus == 1 && x.Value != "0" {
		wantNative = 1
	}
	infuraEqual(t, "all_transfer_count", len(rows), len(x.Transfers)+wantNative)
	nativeCount := 0
	for _, row := range rows {
		infuraEqual(t, "transfer_chain", row.ChainID, x.ChainID)
		infuraEqual(t, "transfer_block", row.BlockNumber, x.BlockNumber)
		infuraEqual(t, "transfer_hash", row.BlockHash, x.BlockHash)
		infuraEqual(t, "transfer_tx", row.TxHash, x.TxHash)
		infuraEqual(t, "transfer_tx_index", row.TxIndex, func() *int { n := int(x.TxIndex); return &n }())
		infuraEqual(t, "transfer_state", row.ChainStatus, "ACTIVE")
		infuraEqual(t, "transfer_block_time", row.BlockTime, x.BlockTimestamp)
		if row.ReorgID != nil || row.RevertedAt != nil || row.ID <= 0 || row.CreatedAt <= 0 {
			t.Fatal("transfer_metadata")
		}
		if row.SourceType == "TRANSACTION_VALUE" {
			nativeCount++
			infuraEqual(t, "native_source", row.SourceIndex, "tx-value")
			infuraEqual(t, "native_kind", row.TransferType, "NATIVE")
			infuraEqual(t, "native_from", row.FromAddress, x.From)
			infuraEqual(t, "native_amount", row.Amount, &x.Value)
			infuraEqual(t, "native_index", row.BatchIndex, 0)
			to := x.To
			if to == nil {
				to = x.ContractAddress
			}
			if to == nil {
				t.Fatal("native_recipient_missing")
			}
			infuraEqual(t, "native_to", row.ToAddress, *to)
			if row.TokenAddress != nil || row.TokenID != nil {
				t.Fatal("native_token_fields")
			}
			continue
		}
		infuraEqual(t, "event_source_type", row.SourceType, "EVENT_LOG")
		found := false
		for _, w := range x.Transfers {
			if row.SourceIndex == strconv.FormatInt(w.LogIndex, 10) && row.BatchIndex == w.BatchIndex {
				found = true
				infuraEqual(t, "token_kind", row.TransferType, w.Standard)
				infuraEqual(t, "token_contract", row.TokenAddress, &w.Contract)
				infuraEqual(t, "token_from", row.FromAddress, w.From)
				infuraEqual(t, "token_to", row.ToAddress, w.To)
				infuraEqual(t, "token_amount", row.Amount, &w.Amount)
				infuraEqual(t, "token_id", row.TokenID, w.TokenID)
				if row.RawData == nil {
					t.Fatal("token_raw_missing")
				}
				for _, l := range x.Logs {
					if l.LogIndex == w.LogIndex {
						infuraAssertLogJSON(t, row.RawData.RawMessage(), l)
					}
				}
				break
			}
		}
		if !found {
			t.Fatal("unexpected_token_source")
		}
	}
	infuraEqual(t, "native_transfer_count", nativeCount, wantNative)
}

func infuraAssertLogJSON(t *testing.T, raw []byte, l manifestLog) {
	t.Helper()
	var got struct {
		Address string
		Topics  []string
		Data    string
	}
	infuraOK(t, json.Unmarshal(raw, &got), "transfer_raw_log")
	infuraEqual(t, "raw_address", got.Address, l.Contract)
	infuraEqual(t, "raw_topics", got.Topics, l.Topics)
	infuraEqual(t, "raw_data", got.Data, l.Data)
}
