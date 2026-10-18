//go:build e2e

package integration

import (
	"context"
	"errors"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gorilla/websocket"
)

func TestInfuraLE02EndpointMatrix(t *testing.T) {
	s := infuraSuiteFor(t)
	want := map[string]struct {
		id   int64
		host string
	}{"ethereum": {1, "mainnet.infura.io"}, "bsc": {56, "bsc-mainnet.infura.io"}, "polygon": {137, "polygon-mainnet.infura.io"}, "arbitrum": {42161, "arbitrum-mainnet.infura.io"}, "optimism": {10, "optimism-mainnet.infura.io"}, "base": {8453, "base-mainnet.infura.io"}, "avalanche": {43114, "avalanche-mainnet.infura.io"}}
	for _, name := range infuraNames {
		c := s.m.Chains[name]
		w := want[name]
		infuraEqual(t, "official_chain_id", c.ChainID, w.id)
		infuraEqual(t, "official_http_host", c.HTTPHost, w.host)
		infuraEqual(t, "official_ws_host", c.WSHost, w.host)
	}
	if s.maximum > 25000 {
		t.Fatal("request_credit_estimate_exceeds_25000")
	}
}

func TestInfuraLE03HTTPChainID(t *testing.T) { infuraChainIDs(t, false) }
func TestInfuraLE04WSSChainID(t *testing.T)  { infuraChainIDs(t, true) }

func infuraChainIDs(t *testing.T, ws bool) {
	s := infuraSuiteFor(t)
	for _, name := range infuraNames {
		t.Run(name, func(t *testing.T) {
			endpoint := s.c.Chains[name].HTTPURL
			if ws {
				endpoint = s.c.Chains[name].WSURL
			}
			ctx, cancel := s.context(t)
			client, err := rpc.DialContext(ctx, endpoint)
			cancel()
			infuraOK(t, err, "chain_id_connect: "+name)
			defer client.Close()
			ctx, cancel = s.action(t)
			var id hexutil.Uint64
			err = client.CallContext(ctx, &id, "eth_chainId")
			cancel()
			infuraOK(t, err, "chain_id_rpc: "+name)
			infuraEqual(t, "chain_id", int64(id), s.m.Chains[name].ChainID)
		})
	}
}

type infuraConnection struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *infuraConnection) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func TestInfuraLE05NewHeadsLifecycle(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, name := range infuraNames {
		t.Run(name, func(t *testing.T) {
			if s.estimate+1000 > s.maximum {
				t.Fatal("wss_session_budget")
			}
			s.estimate += 1000
			closed := make(chan struct{})
			dialer := websocket.Dialer{HandshakeTimeout: 30 * time.Second, NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				c, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &infuraConnection{Conn: c, closed: closed}, nil
			}}
			ctx, cancel := s.context(t)
			client, err := rpc.DialOptions(ctx, s.c.Chains[name].WSURL, rpc.WithWebsocketDialer(dialer))
			cancel()
			infuraOK(t, err, "new_heads_connect: "+name)
			// Closing the transport first makes failure cleanup independent of any
			// unsubscribe RPC; normal completion explicitly verifies unsubscribe.
			defer client.Close()
			heads := make(chan struct {
				Number           hexutil.Uint64
				Hash, ParentHash string
				Timestamp        hexutil.Uint64
			}, 1)
			ctx, cancel = s.action(t)
			sub, err := client.EthSubscribe(ctx, heads, "newHeads")
			cancel()
			if err != nil {
				t.Fatal(infuraSubscriptionError(err))
			}
			defer func() { client.Close(); sub.Unsubscribe() }()
			deadline := time.Now().Add(90 * time.Second)
			if s.deadline.Before(deadline) {
				deadline = s.deadline
			}
			ctx, cancel = context.WithDeadline(context.Background(), deadline)
			defer cancel()
			select {
			case h := <-heads:
				hash := regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
				if h.Number == 0 || h.Timestamp == 0 || !hash.MatchString(h.Hash) || !hash.MatchString(h.ParentHash) || h.Hash == h.ParentHash {
					t.Fatal("new_heads_invalid_shape")
				}
			case err := <-sub.Err():
				t.Fatal(infuraSubscriptionError(err))
			case <-ctx.Done():
				t.Fatal("new_heads_deadline")
			}
			// Receive exactly one unfrozen future head, then stop the subscription.
			unsubCtx, unsubCancel := s.action(t)
			done := make(chan struct{})
			go func() { sub.Unsubscribe(); close(done) }()
			select {
			case <-done:
			case <-unsubCtx.Done():
				client.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("unsubscribe_goroutine_not_joined")
				}
				t.Error("unsubscribe_deadline")
			}
			unsubCancel()
			cancel()
			select {
			case _, open := <-sub.Err():
				if open {
					t.Fatal("subscription_not_closed")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("subscription_close_deadline")
			}
			client.Close()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("wss_connection_not_closed")
			}
		})
	}
}

func infuraSubscriptionError(err error) string {
	var e rpc.Error
	if errors.As(err, &e) {
		return "new_heads_provider_rpc_code=" + strconv.Itoa(e.ErrorCode())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "new_heads_deadline"
	}
	return "new_heads_provider_transport_error"
}

func TestInfuraLE06FixedBlockHeader(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, name := range infuraNames {
		t.Run(name, func(t *testing.T) {
			c := s.m.Chains[name]
			w := c.FixedBlock
			b := s.block(t, name, w.BlockNumber)
			infuraEqual(t, "complete_transaction_count", len(b.Transactions), w.TransactionCount)
			infuraEqual(t, "fixed_number", b.Header.Number, w.BlockNumber)
			infuraEqual(t, "fixed_hash", b.Header.Hash, w.BlockHash)
			infuraEqual(t, "fixed_parent", b.Header.ParentHash, w.ParentHash)
			infuraEqual(t, "fixed_timestamp", b.Header.Timestamp, w.BlockTimestamp)
			p := s.header(t, name, w.ParentNumber, w.ParentHash)
			infuraEqual(t, "fixed_parent_number", p.Number, w.ParentNumber)
			infuraEqual(t, "fixed_parent_hash", p.Hash, w.ParentHash)
			if p.Timestamp <= 0 || p.Timestamp > b.Header.Timestamp {
				t.Fatal("fixed_parent_time")
			}
		})
	}
}

func TestInfuraLE07FixedReceiptLogIdentity(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, name := range infuraNames {
		t.Run(name, func(t *testing.T) {
			samples := append([]manifestSample(nil), s.m.Chains[name].Samples...)
			for _, block := range s.m.Chains[name].ScannerBlocks {
				samples = append(samples, block.Transactions...)
			}
			for _, x := range samples {
				t.Run(x.Role, func(t *testing.T) { s.sample(t, name, x) })
			}
		})
	}
}

func TestInfuraLE08Native(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, name := range infuraNames {
		t.Run(name, func(t *testing.T) {
			found := false
			for _, x := range s.m.Chains[name].Samples {
				if strings.HasPrefix(x.Role, "native") {
					found = true
					db := s.ingest(t, name, x)
					infuraTransfers(t, db, x)
				}
			}
			if !found {
				t.Fatal("native_manifest_role_missing")
			}
		})
	}
}

func TestInfuraLE09ERC20(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, name := range infuraNames {
		t.Run(name, func(t *testing.T) {
			found := false
			for _, x := range s.m.Chains[name].Samples {
				if x.Role == "erc20" || strings.Contains(x.Role, "native_erc20") {
					found = true
					db := s.ingest(t, name, x)
					infuraTransfers(t, db, x)
				}
			}
			if !found {
				t.Fatal("erc20_manifest_role_missing")
			}
		})
	}
}

func TestInfuraLE10ERC721(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, name := range []string{"ethereum", "polygon", "base"} {
		t.Run(name, func(t *testing.T) {
			found := false
			for _, x := range s.m.Chains[name].Samples {
				if x.Role == "erc721" {
					found = true
					db := s.ingest(t, name, x)
					infuraTransfers(t, db, x)
				}
			}
			if !found {
				t.Fatal("erc721_manifest_role_missing")
			}
		})
	}
}

func TestInfuraLE11ERC1155(t *testing.T) {
	s := infuraSuiteFor(t)
	for _, role := range []string{"erc1155_single", "erc1155_batch_unknown"} {
		t.Run(role, func(t *testing.T) {
			found := false
			for _, x := range s.m.Chains["ethereum"].Samples {
				if x.Role == role {
					found = true
					db := s.ingest(t, "ethereum", x)
					infuraTransfers(t, db, x)
					// Decode the observed raw topics/data through the authoritative ABI
					// catalog as well as checking each persisted batch item above.
					var decoded []manifestTransfer
					for _, l := range x.Logs {
						decoded = append(decoded, manifestDecode(t, l)...)
					}
					infuraEqual(t, "typed_abi_items", decoded, x.Transfers)
					for _, l := range x.Logs {
						items := manifestDecode(t, l)
						if len(items) == 0 || items[0].Event != "TransferBatch" {
							continue
						}
						var rows []struct {
							TokenID, Amount string
							BatchIndex      int
						}
						infuraOK(t, db.Raw("SELECT token_id,amount,batch_index FROM chain_transfers WHERE chain_id=? AND block_hash=? AND tx_hash=? AND source_type='EVENT_LOG' AND source_index=? ORDER BY batch_index", x.ChainID, x.BlockHash, x.TxHash, strconv.FormatInt(l.LogIndex, 10)).Scan(&rows).Error, "live_batch_order_query")
						infuraEqual(t, "live_batch_item_count", len(rows), len(items))
						for i, item := range items {
							infuraEqual(t, "live_batch_order", rows[i].BatchIndex, i)
							infuraEqual(t, "live_batch_token_id", rows[i].TokenID, *item.TokenID)
							infuraEqual(t, "live_batch_value", rows[i].Amount, item.Amount)
						}
					}
					// Frozen live Batch logs establish their observed order/value/index.
					// Same-log duplicate IDs are not invented here: their production
					// persistence authority is the protected T044
					// TestWorkerPersistenceERC1155 (ids [7,7], real binary + PG).
				}
			}
			if !found {
				t.Fatal("erc1155_manifest_role_missing")
			}
		})
	}
}

func TestInfuraLE12MultiTransactionMultiLogUnknown(t *testing.T) {
	s := infuraSuiteFor(t)
	blocks := s.m.Chains["ethereum"].ScannerBlocks
	infuraEqual(t, "complete_scanner_block_manifest", len(blocks), 1)
	frozen := blocks[0]
	infuraEqual(t, "complete_scanner_frozen_tx_count", frozen.Block.TransactionCount, 2)
	db := s.ingestComplete(t, "ethereum", frozen)
	var stored struct{ TransactionCount, ReceiptCount int }
	infuraOK(t, db.Raw("SELECT transaction_count,jsonb_array_length(raw_receipts) AS receipt_count FROM chain_blocks WHERE chain_id=1 AND block_hash=?", frozen.Block.BlockHash).Scan(&stored).Error, "complete_scanner_block_query")
	infuraEqual(t, "complete_scanner_stored_tx_count", stored.TransactionCount, 2)
	infuraEqual(t, "complete_scanner_stored_receipt_count", stored.ReceiptCount, 2)
	var rawCount, transferCount, max, cursor int64
	infuraOK(t, db.Raw("SELECT COUNT(*),COALESCE(MAX(id),0) FROM chain_raw_events WHERE chain_id=1 AND block_hash=?", frozen.Block.BlockHash).Row().Scan(&rawCount, &max), "complete_scanner_raw_query")
	infuraEqual(t, "complete_scanner_raw_count", rawCount, int64(110))
	infuraOK(t, db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE chain_id=1 AND block_hash=?", frozen.Block.BlockHash).Scan(&transferCount).Error, "complete_scanner_transfer_query")
	infuraEqual(t, "complete_scanner_transfer_count", transferCount, int64(50))
	infuraOK(t, db.Raw("SELECT cursor FROM chain_event_biz_cursors WHERE biz='transaction' AND chain_family='evm'").Scan(&cursor).Error, "complete_scanner_cursor_query")
	infuraEqual(t, "complete_scanner_last_raw_cursor", cursor, max)
	unknown, standardAfter := false, false
	for _, x := range frozen.Transactions {
		infuraTransfers(t, db, x)
		for _, l := range x.Logs {
			items := manifestDecode(t, l)
			if len(items) == 0 {
				unknown = true
				var count int64
				infuraOK(t, db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE chain_id=? AND block_hash=? AND tx_hash=? AND source_type='EVENT_LOG' AND source_index=?", x.ChainID, x.BlockHash, x.TxHash, strconv.FormatInt(l.LogIndex, 10)).Scan(&count).Error, "unknown_source_query")
				infuraEqual(t, "unknown_zero_transfer", count, int64(0))
			} else if unknown && items[0].Standard == "ERC721" {
				standardAfter = true
			}
		}
	}
	if !unknown || !standardAfter {
		t.Fatal("complete_scanner_unknown_followed_by_erc721_missing")
	}
}

func TestInfuraLE13FailedCreationEnvelopeTypes(t *testing.T) {
	s := infuraSuiteFor(t)
	seen := map[string]bool{}
	for _, x := range s.m.Chains["ethereum"].Samples {
		if x.Role == "failed" || x.Role == "contract_creation" || x.Role == "contract_creation_value" || x.Role == "type0" || x.Role == "type1" || x.Role == "native_type2" {
			t.Run(x.Role, func(t *testing.T) {
				seen[x.Role] = true
				_, tx, r := s.sample(t, "ethereum", x)
				infuraEqual(t, "rpc_envelope", tx.Type, x.TxType)
				db := s.ingest(t, "ethereum", x)
				infuraTransfers(t, db, x)
				if x.Role == "contract_creation" {
					if tx.To != nil || r.ContractAddress == nil {
						t.Fatal("creation_receipt_contract")
					}
					if x.Value != "0" {
						t.Fatal("frozen_creation_value_drift")
					}
				}
				if x.Role == "contract_creation_value" {
					if tx.To != nil || r.ContractAddress == nil || x.Value == "0" || r.Status != 1 {
						t.Fatal("positive_creation_shape")
					}
					var count int64
					infuraOK(t, db.Raw("SELECT COUNT(*) FROM chain_transfers WHERE chain_id=? AND block_hash=? AND tx_hash=? AND source_type='TRANSACTION_VALUE' AND source_index='tx-value' AND batch_index=0 AND transfer_type='NATIVE' AND from_address=? AND to_address=? AND amount=?", x.ChainID, x.BlockHash, x.TxHash, x.From, *r.ContractAddress, x.Value).Scan(&count).Error, "positive_creation_native_query")
					infuraEqual(t, "positive_creation_receipt_recipient_unique", count, int64(1))
				}
				if x.Role == "failed" {
					infuraEqual(t, "failed_status", r.Status, 0)
				}
				var count int64
				infuraOK(t, db.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='chain_transactions' AND column_name='tx_type'").Scan(&count).Error, "type_absence_query")
				infuraEqual(t, "no_persisted_type_column", count, int64(0))
			})
		}
	}
	infuraEqual(t, "execution_shape_coverage", seen, map[string]bool{"failed": true, "contract_creation": true, "contract_creation_value": true, "type0": true, "type1": true, "native_type2": true})
}
