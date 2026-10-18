//go:build e2e

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	evmabi "github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
)

type manifest struct {
	ManifestVersion       string                   `json:"manifest_version"`
	ObservedAt            string                   `json:"observed_at"`
	Preflight             bool                     `json:"preflight_verified"`
	Acceptance            string                   `json:"acceptance_status"`
	Provider              string                   `json:"provider"`
	PreflightMethod       string                   `json:"preflight_method"`
	PreflightRequests     int                      `json:"preflight_request_count"`
	PreflightErrors       []manifestPreflightError `json:"preflight_errors"`
	PreflightObservations []manifestObservation    `json:"preflight_observations"`
	CandidateCorrections  []manifestCorrection     `json:"candidate_corrections"`
	SourceCrossReference  string                   `json:"source_cross_reference"`
	Chains                map[string]manifestChain `json:"chains"`
}
type manifestPreflightError struct {
	Chain      string `json:"chain"`
	TxHash     string `json:"tx_hash"`
	Method     string `json:"method"`
	HTTPCode   *int   `json:"http_code"`
	RPCCode    *int   `json:"rpc_code"`
	ErrorClass string `json:"error_class"`
}
type manifestObservation struct {
	Chain                 string               `json:"chain"`
	Role                  string               `json:"role"`
	BlockNumber           int64                `json:"block_number"`
	ParentNumber          int64                `json:"parent_number"`
	ParentHash            string               `json:"parent_hash"`
	Method                string               `json:"method"`
	HTTPCode              int                  `json:"http_code"`
	RPCCode               int                  `json:"rpc_code"`
	ErrorClass            string               `json:"error_class"`
	Resolution            string               `json:"resolution"`
	ObservedAt            string               `json:"observed_at,omitempty"`
	TxHash                string               `json:"tx_hash,omitempty"`
	BlockRanges           []manifestBlockRange `json:"block_ranges,omitempty"`
	RequestCount          int                  `json:"request_count,omitempty"`
	GetLogsRequests       int                  `json:"get_logs_requests,omitempty"`
	ValidBatchLogsChecked int                  `json:"valid_batch_logs_checked,omitempty"`
	SameLogRepeatedIDs    int                  `json:"same_log_repeated_ids"`
	LocalBoundary         string               `json:"local_boundary,omitempty"`
}
type manifestBlockRange struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}
type manifestCorrection struct {
	Role              string `json:"role"`
	RejectedTxHash    string `json:"rejected_tx_hash,omitempty"`
	PreviousTxHash    string `json:"previous_tx_hash,omitempty"`
	ErrorClass        string `json:"error_class,omitempty"`
	ObservedTxType    *int   `json:"observed_tx_type,omitempty"`
	ReplacementTxHash string `json:"replacement_tx_hash"`
	Reason            string `json:"reason"`
}
type manifestChain struct {
	Name             string                 `json:"chain_name"`
	ChainID          int64                  `json:"chain_id"`
	Explorer         string                 `json:"explorer"`
	HTTPHost         string                 `json:"http_host"`
	WSHost           string                 `json:"ws_host"`
	StartBlock       int64                  `json:"start_block"`
	FixedBlock       manifestBlock          `json:"fixed_block"`
	ConfigStartBlock manifestBlock          `json:"config_start_block"`
	Coverage         []string               `json:"coverage"`
	Samples          []manifestSample       `json:"samples"`
	ScannerBlocks    []manifestScannerBlock `json:"scanner_blocks,omitempty"`
}
type manifestScannerBlock struct {
	Block        manifestBlock    `json:"block"`
	Transactions []manifestSample `json:"transactions"`
}
type manifestBlock struct {
	ObservedAt       string               `json:"observed_at"`
	Preflight        bool                 `json:"preflight_verified"`
	BlockNumber      int64                `json:"block_number"`
	BlockTimestamp   int64                `json:"block_timestamp"`
	BlockHash        string               `json:"block_hash"`
	ParentNumber     int64                `json:"parent_number"`
	ParentHash       string               `json:"parent_hash"`
	SourceURL        string               `json:"source_url"`
	TransactionCount int                  `json:"transaction_count"`
	ReadMethods      *manifestReadMethods `json:"read_methods,omitempty"`
}
type manifestReadMethods struct {
	Block  []string `json:"block"`
	Parent []string `json:"parent"`
}
type manifestSample struct {
	Role                  string             `json:"role"`
	ObservedAt            string             `json:"observed_at"`
	Preflight             bool               `json:"preflight_verified"`
	PreflightReadCount    int                `json:"preflight_read_count"`
	ChainID               int64              `json:"chain_id"`
	TxHash                string             `json:"tx_hash"`
	BlockNumber           int64              `json:"block_number"`
	BlockTimestamp        int64              `json:"block_timestamp"`
	BlockHash             string             `json:"block_hash"`
	ParentHash            string             `json:"parent_hash"`
	ParentNumber          int64              `json:"parent_number"`
	BlockTransactionCount int                `json:"block_transaction_count"`
	TxIndex               int64              `json:"tx_index"`
	TxType                int                `json:"tx_type"`
	ReceiptStatus         int                `json:"receipt_status"`
	SourceURL             string             `json:"source_url"`
	From                  string             `json:"from"`
	To                    *string            `json:"to"`
	Value                 string             `json:"value"`
	Input                 string             `json:"input"`
	Nonce                 int64              `json:"nonce"`
	GasLimit              string             `json:"gas_limit"`
	GasUsed               string             `json:"gas_used"`
	GasPrice              *string            `json:"gas_price"`
	EffectiveGasPrice     *string            `json:"effective_gas_price"`
	ContractAddress       *string            `json:"contract_address"`
	ExpectedLogIndices    []int64            `json:"expected_log_indices"`
	Logs                  []manifestLog      `json:"logs"`
	Transfers             []manifestTransfer `json:"transfers"`
}
type manifestLog struct {
	LogIndex    int64    `json:"log_index"`
	Contract    string   `json:"contract"`
	Topic0      string   `json:"topic0"`
	Topic1      *string  `json:"topic1"`
	Topic2      *string  `json:"topic2"`
	Topic3      *string  `json:"topic3"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber int64    `json:"block_number"`
	BlockHash   string   `json:"block_hash"`
	TxHash      string   `json:"tx_hash"`
	TxIndex     int64    `json:"tx_index"`
	Removed     bool     `json:"removed"`
}

type manifestTransfer struct {
	Standard   string  `json:"standard"`
	Event      string  `json:"event"`
	Contract   string  `json:"contract"`
	Topic0     string  `json:"topic0"`
	LogIndex   int64   `json:"log_index"`
	From       string  `json:"from"`
	To         string  `json:"to"`
	Amount     string  `json:"amount"`
	TokenID    *string `json:"token_id"`
	BatchIndex int     `json:"batch_index"`
}

// SHA-256 of the approved semantic JSON, omitting only observation timestamps.
// Object keys are sorted by encoding/json; array order and decimal/hex strings
// are preserved. This pins every identity, log, transfer, provider and provenance
// field without depending on JSON whitespace or exposing endpoint credentials.
const approvedManifestIdentitySHA256 = "c439f8631ed4fef736bc59257489d0378aaf1c2d3c1315ec1f4ce3e68832d671"

func manifestIdentityDigest(data []byte) ([sha256.Size]byte, error) {
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		return [sha256.Size]byte{}, err
	}
	var omitTimes func(any)
	omitTimes = func(value any) {
		switch item := value.(type) {
		case map[string]any:
			delete(item, "observed_at")
			for _, child := range item {
				omitTimes(child)
			}
		case []any:
			for _, child := range item {
				omitTimes(child)
			}
		}
	}
	omitTimes(document)
	canonical, err := json.Marshal(document)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func TestInfuraManifestContract(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "internal", "test", "integration", "testdata", "infura_samples.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("manifest must contain exactly one JSON document")
	}
	if m.ManifestVersion != "CI-V1-remaining-v29-T041-r1" || !manifestTime(m.ObservedAt) || !m.Preflight || m.Acceptance != "deferred_to_T046" || m.Provider != "infura" || m.PreflightRequests < 12 || m.PreflightMethod == "" || m.PreflightErrors == nil || len(m.PreflightErrors) != 0 || m.SourceCrossReference == "" {
		t.Fatal("invalid manifest metadata")
	}
	digest, err := manifestIdentityDigest(b)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", digest) != approvedManifestIdentitySHA256 {
		t.Fatal("frozen manifest identity digest changed; a versioned T041 revision requires Human approval")
	}
	researchFound := false
	for _, observation := range m.PreflightObservations {
		if observation.Role == "erc1155_duplicate_id_research" {
			if researchFound || observation.Chain != "ethereum" || observation.Method != "eth_getLogs" || !manifestTime(observation.ObservedAt) || observation.RequestCount != 19 || observation.GetLogsRequests != 6 || observation.ValidBatchLogsChecked != 439 || observation.SameLogRepeatedIDs != 0 || !reflect.DeepEqual(observation.BlockRanges, []manifestBlockRange{{17216000, 17218999}, {23224000, 23225999}, {20000000, 20000999}}) || !strings.Contains(observation.LocalBoundary, "TestWorkerPersistenceERC1155") {
				t.Fatal("invalid bounded repeated-ID research boundary")
			}
			researchFound = true
		}
	}
	if !researchFound {
		t.Fatal("missing bounded repeated-ID research")
	}
	want := map[string]int64{"ethereum": 1, "bsc": 56, "polygon": 137, "arbitrum": 42161, "optimism": 10, "base": 8453, "avalanche": 43114}
	providerMatrix := map[string]struct{ httpHost, wsHost, explorer string }{
		"ethereum":  {"mainnet.infura.io", "mainnet.infura.io", "https://etherscan.io"},
		"bsc":       {"bsc-mainnet.infura.io", "bsc-mainnet.infura.io", "https://bscscan.com"},
		"polygon":   {"polygon-mainnet.infura.io", "polygon-mainnet.infura.io", "https://polygonscan.com"},
		"arbitrum":  {"arbitrum-mainnet.infura.io", "arbitrum-mainnet.infura.io", "https://arbiscan.io"},
		"optimism":  {"optimism-mainnet.infura.io", "optimism-mainnet.infura.io", "https://optimistic.etherscan.io"},
		"base":      {"base-mainnet.infura.io", "base-mainnet.infura.io", "https://basescan.org"},
		"avalanche": {"avalanche-mainnet.infura.io", "avalanche-mainnet.infura.io", "https://avascan.info"},
	}
	wantRoles := map[string]map[string]bool{"ethereum": {"native_type2": true, "erc20": true, "erc721": true, "erc1155_single": true, "erc1155_batch_unknown": true, "failed": true, "contract_creation": true, "contract_creation_value": true, "type0": true, "type1": true}, "bsc": {"native_erc20_multilog": true}, "polygon": {"native_erc20": true, "erc721": true}, "arbitrum": {"native": true, "erc20": true}, "optimism": {"native": true, "erc20": true}, "base": {"native": true, "erc20": true, "erc721": true}, "avalanche": {"native": true, "erc20": true}}
	hashRE := regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
	addrRE := regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	topicRE := regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
	dataRE := regexp.MustCompile(`^0x([0-9a-fA-F]{2})*$`)
	if len(m.Chains) != len(want) {
		t.Fatal("unexpected chain count")
	}
	for name, id := range want {
		c, ok := m.Chains[name]
		provider := providerMatrix[name]
		if !ok || c.Name != name || c.ChainID != id || c.StartBlock <= 0 || c.HTTPHost != provider.httpHost || c.WSHost != provider.wsHost || c.Explorer != provider.explorer || len(c.Samples) == 0 {
			t.Fatalf("missing chain contract: %s", name)
		}
		first := c.Samples[0]
		if c.FixedBlock.BlockNumber != first.BlockNumber || c.FixedBlock.BlockHash != first.BlockHash || c.FixedBlock.BlockTimestamp != first.BlockTimestamp || c.FixedBlock.ParentNumber != first.ParentNumber || c.FixedBlock.ParentHash != first.ParentHash {
			t.Fatalf("fixed block differs from first sample: %s", name)
		}
		for _, fixed := range []struct {
			block  manifestBlock
			number int64
		}{{c.FixedBlock, c.Samples[0].BlockNumber}, {c.ConfigStartBlock, c.StartBlock}} {
			f := fixed.block
			if !f.Preflight || !manifestTime(f.ObservedAt) || f.BlockNumber != fixed.number || f.BlockTimestamp <= 0 || f.ParentNumber+1 != f.BlockNumber || !hashRE.MatchString(f.BlockHash) || !hashRE.MatchString(f.ParentHash) || !strings.HasPrefix(f.SourceURL, c.Explorer+"/") || !strings.HasSuffix(f.SourceURL, fmt.Sprintf("/block/%d", f.BlockNumber)) {
				t.Fatalf("invalid fixed/start block: %s", name)
			}
		}
		seen := map[string]bool{}
		features := map[string]bool{}
		allSamples := append([]manifestSample(nil), c.Samples...)
		scannerRoles := map[string]bool{}
		if (name == "ethereum" && len(c.ScannerBlocks) != 1) || (name != "ethereum" && len(c.ScannerBlocks) != 0) {
			t.Fatalf("scanner block coverage: %s", name)
		}
		for _, frozen := range c.ScannerBlocks {
			block := frozen.Block
			if !block.Preflight || !manifestTime(block.ObservedAt) || block.BlockNumber <= 0 || block.BlockTimestamp <= 0 || block.ParentNumber+1 != block.BlockNumber || !hashRE.MatchString(block.BlockHash) || !hashRE.MatchString(block.ParentHash) || block.TransactionCount != len(frozen.Transactions) || block.TransactionCount < 2 || !strings.HasPrefix(block.SourceURL, c.Explorer+"/") || !strings.HasSuffix(block.SourceURL, fmt.Sprintf("/block/%d", block.BlockNumber)) {
				t.Fatalf("invalid complete scanner block: %s", name)
			}
			txHashes := map[string]bool{}
			unknown, standardAfter := false, false
			lastLogIndex, logCount := int64(-1), 0
			for i, tx := range frozen.Transactions {
				if tx.Role != fmt.Sprintf("scanner_transaction_%d", i) || txHashes[tx.TxHash] || tx.BlockNumber != block.BlockNumber || tx.BlockHash != block.BlockHash || tx.BlockTimestamp != block.BlockTimestamp || tx.ParentNumber != block.ParentNumber || tx.ParentHash != block.ParentHash || tx.BlockTransactionCount != block.TransactionCount || tx.TxIndex != int64(i) {
					t.Fatalf("incomplete scanner transaction set: %s", name)
				}
				txHashes[tx.TxHash] = true
				scannerRoles[tx.Role] = true
				for _, log := range tx.Logs {
					if log.LogIndex <= lastLogIndex {
						t.Fatal("scanner block log order")
					}
					lastLogIndex = log.LogIndex
					logCount++
					if len(manifestDecode(t, log)) == 0 {
						unknown = true
					} else if unknown {
						standardAfter = true
					}
				}
			}
			if logCount < 2 || !unknown || !standardAfter {
				t.Fatal("scanner block requires unknown followed by standard event")
			}
			allSamples = append(allSamples, frozen.Transactions...)
		}
		for _, s := range allSamples {
			if (!wantRoles[name][s.Role] && !scannerRoles[s.Role]) || seen[s.Role] {
				t.Fatalf("invalid/duplicate role: %s/%s", name, s.Role)
			}
			seen[s.Role] = true
			source, sourceErr := url.Parse(s.SourceURL)
			if !s.Preflight || s.PreflightReadCount != 2 || !manifestTime(s.ObservedAt) || s.ChainID != id || !hashRE.MatchString(s.TxHash) || !hashRE.MatchString(s.BlockHash) || !hashRE.MatchString(s.ParentHash) || s.BlockNumber <= 0 || s.BlockTimestamp <= 0 || s.ParentNumber+1 != s.BlockNumber || s.TxIndex < 0 || s.BlockTransactionCount <= int(s.TxIndex) || s.TxType < 0 || s.TxType > 2 || (s.ReceiptStatus != 0 && s.ReceiptStatus != 1) || !addrRE.MatchString(s.From) || sourceErr != nil || source.Scheme != "https" || source.User != nil || source.RawQuery != "" || !strings.HasPrefix(s.SourceURL, c.Explorer+"/") || !strings.HasSuffix(source.Path, "/tx/"+s.TxHash) {
				t.Fatalf("invalid sample identity: %s/%s", name, s.Role)
			}
			if s.To != nil && !addrRE.MatchString(*s.To) {
				t.Fatalf("invalid recipient: %s", s.Role)
			}
			if value, ok := new(big.Int).SetString(s.Value, 10); !ok || value.Sign() < 0 || value.BitLen() > 256 {
				t.Fatalf("invalid value: %s", s.Role)
			} else if value.Sign() > 0 {
				features["native"] = true
			}
			features[fmt.Sprintf("type%d", s.TxType)] = true
			if s.BlockTransactionCount > 1 {
				features["multi_transaction"] = true
			}
			if len(s.Logs) > 1 {
				features["multi_log"] = true
			}
			if s.Role == "failed" && s.ReceiptStatus != 0 {
				t.Fatalf("failed sample status: %s", s.TxHash)
			}
			if !scannerRoles[s.Role] && s.Role != "failed" && s.ReceiptStatus != 1 {
				t.Fatalf("unexpected failed receipt: %s", s.Role)
			}
			if (s.Role == "type0" && s.TxType != 0) || (s.Role == "type1" && s.TxType != 1) || (s.Role == "native_type2" && s.TxType != 2) {
				t.Fatalf("typed role mismatch: %s", s.Role)
			}
			creation := s.Role == "contract_creation" || s.Role == "contract_creation_value"
			if creation && (s.To != nil || s.ContractAddress == nil || !addrRE.MatchString(*s.ContractAddress)) {
				t.Fatalf("creation fields: %s", s.TxHash)
			}
			if !creation && s.ContractAddress != nil {
				t.Fatalf("unexpected creation address: %s", s.Role)
			}
			if s.Role == "contract_creation_value" {
				value, _ := new(big.Int).SetString(s.Value, 10)
				if value.Sign() <= 0 {
					t.Fatal("positive creation value missing")
				}
			}
			if s.Role == "contract_creation" && s.Value != "0" {
				t.Fatal("zero-value creation identity changed")
			}
			if strings.HasPrefix(s.Role, "native") || s.Role == "native_type2" {
				if s.Value == "" || s.Value == "0" {
					t.Fatalf("native value: %s", s.TxHash)
				}
			}
			if len(s.ExpectedLogIndices) != len(s.Logs) {
				t.Fatalf("missing log identities: %s", s.Role)
			}
			var transfers []manifestTransfer
			for i, l := range s.Logs {
				if l.LogIndex < 0 || l.LogIndex != s.ExpectedLogIndices[i] || (i > 0 && l.LogIndex <= s.Logs[i-1].LogIndex) || !addrRE.MatchString(l.Contract) || len(l.Topics) > 4 || !dataRE.MatchString(l.Data) || l.BlockNumber != s.BlockNumber || l.BlockHash != s.BlockHash || l.TxHash != s.TxHash || l.TxIndex != s.TxIndex || l.Removed {
					t.Fatalf("invalid log: %s/%s", name, s.Role)
				}
				for j, p := range []*string{&l.Topic0, l.Topic1, l.Topic2, l.Topic3} {
					if j < len(l.Topics) {
						if p == nil || *p != l.Topics[j] || !topicRE.MatchString(*p) {
							t.Fatalf("topic mismatch: %s/%s", name, s.Role)
						}
					} else if p != nil && *p != "" {
						t.Fatalf("extra topic: %s/%s", name, s.Role)
					}
				}
				decoded := manifestDecode(t, l)
				if len(decoded) == 0 {
					features["unknown"] = true
				}
				for _, transfer := range decoded {
					features[transfer.Standard] = true
					features[transfer.Event] = true
				}
				transfers = append(transfers, decoded...)
			}
			if len(transfers) != len(s.Transfers) || (len(transfers) > 0 && !reflect.DeepEqual(transfers, s.Transfers)) {
				t.Fatalf("token fields differ from receipt: %s/%s", name, s.Role)
			}
			roleFeature := map[string]string{"erc20": "ERC20", "native_erc20": "ERC20", "native_erc20_multilog": "ERC20", "erc721": "ERC721", "erc1155_single": "TransferSingle", "erc1155_batch_unknown": "TransferBatch"}[s.Role]
			if roleFeature != "" {
				covered := false
				for _, transfer := range transfers {
					covered = covered || transfer.Standard == roleFeature || transfer.Event == roleFeature
				}
				if !covered {
					t.Fatalf("role has no matching receipt transfer: %s/%s", name, s.Role)
				}
			}
		}
		for role := range wantRoles[name] {
			if !seen[role] {
				t.Fatalf("missing role: %s/%s", name, role)
			}
		}
		for _, feature := range []string{"native", "ERC20"} {
			if !features[feature] {
				t.Fatalf("missing chain coverage: %s/%s", name, feature)
			}
		}
		if name == "ethereum" {
			for _, feature := range []string{"ERC721", "TransferSingle", "TransferBatch", "multi_transaction", "multi_log", "unknown", "type0", "type1", "type2"} {
				if !features[feature] {
					t.Fatalf("missing Ethereum coverage: %s", feature)
				}
			}
		}
		if (name == "polygon" || name == "base") && !features["ERC721"] {
			t.Fatalf("missing ERC721: %s", name)
		}
	}
}

func manifestTime(value string) bool {
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

// Decode only public receipt fields; the manifest never contains raw RPC envelopes.
func manifestDecode(t *testing.T, l manifestLog) []manifestTransfer {
	t.Helper()
	events, err := evmabi.NewTransferEvents()
	if err != nil {
		t.Fatal(err)
	}
	var event evmabi.ParsedEvent
	standard := ""
	switch {
	case l.Topic0 == events.ERC20Transfer.Topic0 && len(l.Topics) == 3:
		event, standard = events.ERC20Transfer, "ERC20"
	case l.Topic0 == events.ERC721Transfer.Topic0 && len(l.Topics) == 4:
		event, standard = events.ERC721Transfer, "ERC721"
	case l.Topic0 == events.ERC1155TransferSingle.Topic0:
		event, standard = events.ERC1155TransferSingle, "ERC1155"
	case l.Topic0 == events.ERC1155TransferBatch.Topic0:
		event, standard = events.ERC1155TransferBatch, "ERC1155"
	default:
		return nil
	}
	d, err := evmabi.NewDecoder().Decode(event, l.Topics, l.Data)
	if err != nil {
		t.Fatalf("invalid token receipt log %d", l.LogIndex)
	}
	from, fromOK := d["from"].(string)
	to, toOK := d["to"].(string)
	if !fromOK || !toOK {
		t.Fatal("missing token addresses")
	}
	base := manifestTransfer{Standard: standard, Event: event.Name, Contract: l.Contract, Topic0: l.Topic0, LogIndex: l.LogIndex, From: strings.ToLower(from), To: strings.ToLower(to)}
	switch event.Name {
	case "TransferBatch":
		ids, idsOK := d["ids"].([]*big.Int)
		values, valuesOK := d["values"].([]*big.Int)
		if !idsOK || !valuesOK || len(ids) == 0 || len(ids) != len(values) {
			t.Fatal("invalid batch arrays")
		}
		out := make([]manifestTransfer, len(ids))
		for i := range ids {
			out[i] = base
			token := ids[i].String()
			out[i].TokenID = &token
			out[i].Amount = values[i].String()
			out[i].BatchIndex = i
		}
		return out
	case "TransferSingle":
		token := d["id"].(*big.Int).String()
		base.TokenID = &token
		base.Amount = d["value"].(*big.Int).String()
	case "Transfer":
		if standard == "ERC721" {
			token := d["tokenId"].(*big.Int).String()
			base.TokenID = &token
			base.Amount = "1"
		} else {
			base.Amount = d["value"].(*big.Int).String()
		}
	}
	return []manifestTransfer{base}
}
