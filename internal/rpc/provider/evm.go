package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/rpc"
)

type client struct{ rpc *rpc.Client }

// boundaryError exposes only stable operation context, never untrusted causes.
type boundaryError struct {
	operation string
}

func (e boundaryError) Error() string { return "evm provider: " + e.operation + " failed" }
func safeError(op string, err error) error {
	if err == nil {
		return nil
	}
	return boundaryError{op}
}

func connect(ctx context.Context, endpoint string) (*client, func() error, error) {
	c, err := rpc.DialContext(ctx, endpoint)
	if err != nil {
		return nil, nil, safeError("connect", err)
	}
	return &client{c}, func() error { c.Close(); return nil }, nil
}

func NewHTTPReader(ctx context.Context, endpoint string) (Reader, func() error, error) {
	return connect(ctx, endpoint)
}

func NewWSSubscriber(ctx context.Context, endpoint string) (HeadSubscriber, func() error, error) {
	return connect(ctx, endpoint)
}

type wireHeader struct {
	Number     hexutil.Uint64
	Hash       string
	ParentHash string
	Timestamp  hexutil.Uint64
}

func (h wireHeader) domain() domain.BlockHeader {
	return domain.BlockHeader{Number: int64(h.Number), Hash: h.Hash, ParentHash: h.ParentHash, Timestamp: int64(h.Timestamp)}
}

func (c *client) call(ctx context.Context, method string, args ...any) (json.RawMessage, error) {
	var raw json.RawMessage
	if err := c.rpc.CallContext(ctx, &raw, method, args...); err != nil {
		return nil, safeError(method, err)
	}
	if string(raw) == "null" {
		return nil, safeError(method, errors.New("not found"))
	}
	return raw, nil
}

func (c *client) LatestBlockNumber(ctx context.Context) (int64, error) {
	var n hexutil.Uint64
	err := c.rpc.CallContext(ctx, &n, "eth_blockNumber")
	return int64(n), safeError("eth_blockNumber", err)
}

func (c *client) HeaderByNumber(ctx context.Context, n int64) (domain.BlockHeader, error) {
	raw, err := c.call(ctx, "eth_getBlockByNumber", fmt.Sprintf("0x%x", n), false)
	if err != nil {
		return domain.BlockHeader{}, err
	}
	var h wireHeader
	err = json.Unmarshal(raw, &h)
	return h.domain(), safeError("decode header", err)
}

func (c *client) BlockByNumber(ctx context.Context, n int64) (domain.Block, error) {
	raw, err := c.call(ctx, "eth_getBlockByNumber", fmt.Sprintf("0x%x", n), true)
	if err != nil {
		return domain.Block{}, err
	}
	var w struct {
		wireHeader
		TransactionsRoot, ReceiptsRoot, StateRoot string
		GasUsed, GasLimit                         hexutil.Uint64
		BaseFeePerGas                             *hexutil.Big
		Transactions                              []json.RawMessage
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return domain.Block{}, safeError("decode block", err)
	}
	b := domain.Block{Header: w.wireHeader.domain(), TransactionsRoot: w.TransactionsRoot, ReceiptsRoot: w.ReceiptsRoot, StateRoot: w.StateRoot, GasUsed: int64(w.GasUsed), GasLimit: int64(w.GasLimit), BaseFeePerGas: decimal(w.BaseFeePerGas), RawBlock: raw}
	for _, rawTx := range w.Transactions {
		var t struct {
			Hash, From, Input                  string
			To                                 *string
			TransactionIndex, Nonce, Type, Gas hexutil.Uint64
			Value, GasPrice                    *hexutil.Big
		}
		if err := json.Unmarshal(rawTx, &t); err != nil {
			return domain.Block{}, safeError("decode transaction", err)
		}
		b.Transactions = append(b.Transactions, domain.Transaction{Hash: t.Hash, Index: int(t.TransactionIndex), From: t.From, To: t.To, Nonce: int64(t.Nonce), Value: decimal(t.Value), Input: t.Input, Type: int(t.Type), GasLimit: int64(t.Gas), GasPrice: decimal(t.GasPrice), RawTransaction: rawTx})
	}
	return b, nil
}

func decimal(n *hexutil.Big) string {
	if n == nil {
		return ""
	}
	return n.ToInt().String()
}

func (c *client) ReceiptByTransactionHash(ctx context.Context, hash string) (domain.Receipt, error) {
	raw, err := c.call(ctx, "eth_getTransactionReceipt", hash)
	if err != nil {
		return domain.Receipt{}, err
	}
	var w struct {
		TransactionHash   string
		BlockNumber       hexutil.Uint64
		BlockHash         string
		TransactionIndex  hexutil.Uint64
		Status, GasUsed   hexutil.Uint64
		EffectiveGasPrice *hexutil.Big
		ContractAddress   *string
		Logs              []json.RawMessage
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return domain.Receipt{}, safeError("decode receipt", err)
	}
	r := domain.Receipt{TxHash: w.TransactionHash, BlockNumber: int64(w.BlockNumber), BlockHash: w.BlockHash, TxIndex: int(w.TransactionIndex), Status: int(w.Status), GasUsed: int64(w.GasUsed), EffectiveGasPrice: decimal(w.EffectiveGasPrice), ContractAddress: w.ContractAddress, RawReceipt: raw}
	for _, rawLog := range w.Logs {
		var l struct {
			BlockNumber, TransactionIndex, LogIndex   hexutil.Uint64
			BlockHash, TransactionHash, Address, Data string
			Topics                                    []string
		}
		if err := json.Unmarshal(rawLog, &l); err != nil {
			return domain.Receipt{}, safeError("decode log", err)
		}
		r.Logs = append(r.Logs, domain.Log{BlockNumber: int64(l.BlockNumber), BlockHash: l.BlockHash, TxHash: l.TransactionHash, TxIndex: int(l.TransactionIndex), Index: int(l.LogIndex), Address: l.Address, Topics: l.Topics, Data: l.Data, RawLog: rawLog})
	}
	return r, nil
}

func (c *client) SubscribeNewHeads(ctx context.Context, heads chan<- domain.BlockHeader) (Subscription, error) {
	wire := make(chan wireHeader)
	sub, err := c.rpc.EthSubscribe(ctx, wire, "newHeads")
	if err != nil {
		return nil, safeError("subscribe newHeads", err)
	}
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case <-quit:
				return nil
			case <-ctx.Done():
				return nil
			case err := <-sub.Err():
				return safeError("newHeads subscription", err)
			case h := <-wire:
				select {
				case heads <- h.domain():
				case <-quit:
					return nil
				case <-ctx.Done():
					return nil
				case err := <-sub.Err():
					return safeError("newHeads subscription", err)
				}
			}
		}
	}), nil
}
