package repository

import (
	"context"
	"encoding/json"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"github.com/Stone-afk/chain-indexer-service/pkg/xsql"
	"time"
)

type chainRepository struct{ dao postgre.ChainDAO }

func NewChainRepository(dao postgre.ChainDAO) ChainRepository { return &chainRepository{dao: dao} }

func (r *chainRepository) GetCheckpoint(ctx context.Context, chainID int64) (domain.Checkpoint, bool, error) {
	row, found, err := r.dao.GetCheckpoint(ctx, chainID)
	return domain.Checkpoint{ChainID: row.ChainID, LastProcessedHeight: row.LastProcessedHeight, LastProcessedHash: row.LastProcessedHash}, found, err
}

func (r *chainRepository) GetCanonicalBlock(ctx context.Context, chainID, height int64) (domain.StoredBlock, bool, error) {
	e, found, err := r.dao.GetCanonicalBlock(ctx, chainID, height)
	return domain.StoredBlock{ChainID: e.ChainID, BlockNumber: e.BlockNumber, BlockHash: e.BlockHash, ParentHash: e.ParentHash, BlockTime: e.BlockTime, Status: e.Status, Finality: e.Finality, TransactionCount: e.TransactionCount, StateRoot: e.StateRoot, TransactionsRoot: e.TransactionsRoot, ReceiptsRoot: e.ReceiptsRoot, GasLimit: e.GasLimit, GasUsed: e.GasUsed, BaseFeePerGas: e.BaseFeePerGas, RawBlock: domainJSON(e.RawBlock), RawReceipts: domainJSON(e.RawReceipts), ReorgID: e.ReorgID, OrphanedAt: e.OrphanedAt, ChainVersion: e.ChainVersion, CreatedAt: e.CreatedAt}, found, err
}

func domainJSON(raw *xsql.JSONField) json.RawMessage {
	if raw == nil {
		return nil
	}
	return raw.RawMessage()
}

func entityJSON(raw json.RawMessage) *xsql.JSONField {
	if raw == nil {
		return nil
	}
	value := xsql.JSONField(append([]byte(nil), raw...))
	return &value
}

func checkpointEntity(c domain.Checkpoint) postgre.ChainCheckpoint {
	return postgre.ChainCheckpoint{ChainID: c.ChainID, LastProcessedHeight: c.LastProcessedHeight, LastProcessedHash: c.LastProcessedHash}
}

func (r *chainRepository) SaveCanonicalBatch(ctx context.Context, b domain.CanonicalBatch) error {
	e := b.Block
	rows := postgre.CanonicalBatch{Block: postgre.ChainBlock{ChainID: e.ChainID, BlockNumber: e.BlockNumber, BlockHash: e.BlockHash, ParentHash: e.ParentHash, BlockTime: e.BlockTime, Status: e.Status, Finality: e.Finality, TransactionCount: e.TransactionCount, StateRoot: e.StateRoot, TransactionsRoot: e.TransactionsRoot, ReceiptsRoot: e.ReceiptsRoot, GasLimit: e.GasLimit, GasUsed: e.GasUsed, BaseFeePerGas: e.BaseFeePerGas, RawBlock: entityJSON(e.RawBlock), RawReceipts: entityJSON(e.RawReceipts), ReorgID: e.ReorgID, OrphanedAt: e.OrphanedAt, ChainVersion: e.ChainVersion, CreatedAt: e.CreatedAt}, Checkpoint: checkpointEntity(b.Checkpoint)}
	for _, tx := range b.Transactions {
		rows.Transactions = append(rows.Transactions, postgre.ChainTransaction{ChainID: tx.ChainID, BlockNumber: tx.BlockNumber, BlockHash: tx.BlockHash, TxHash: tx.TxHash, TxIndex: tx.TxIndex, FromAddress: tx.FromAddress, ToAddress: tx.ToAddress, Nonce: tx.Nonce, Value: tx.Value, Input: tx.Input, MethodID: tx.MethodID, MethodName: tx.MethodName, TxType: tx.TxType, GasLimit: tx.GasLimit, GasUsed: tx.GasUsed, GasPrice: tx.GasPrice, EffectiveGasPrice: tx.EffectiveGasPrice, TransactionFee: tx.TransactionFee, ExecutionStatus: tx.ExecutionStatus, ContractAddress: tx.ContractAddress, ChainStatus: tx.ChainStatus, ReorgID: tx.ReorgID, RawTransaction: entityJSON(tx.RawTransaction), RawReceipt: entityJSON(tx.RawReceipt), BlockTime: tx.BlockTime, CreatedAt: tx.CreatedAt, RevertedAt: tx.RevertedAt})
	}
	for _, ev := range b.RawEvents {
		rows.RawEvents = append(rows.RawEvents, postgre.ChainRawEvent{ChainID: ev.ChainID, BlockNumber: ev.BlockNumber, BlockHash: ev.BlockHash, TxHash: ev.TxHash, TxIndex: ev.TxIndex, LogIndex: ev.LogIndex, ContractAddress: ev.ContractAddress, Topic0: ev.Topic0, Topic1: ev.Topic1, Topic2: ev.Topic2, Topic3: ev.Topic3, EventData: ev.EventData, RawLog: xsql.JSONField(append([]byte(nil), ev.RawLog...)), ChainStatus: ev.ChainStatus, ReorgID: ev.ReorgID, RevertedAt: ev.RevertedAt})
	}
	for _, tr := range b.NativeTransfers {
		rows.NativeTransfers = append(rows.NativeTransfers, postgre.ChainTransfer{ChainID: tr.ChainID, BlockNumber: tr.BlockNumber, BlockHash: tr.BlockHash, TxHash: tr.TxHash, TxIndex: tr.TxIndex, SourceType: tr.SourceType, SourceIndex: tr.SourceIndex, TransferType: tr.TransferType, TokenAddress: tr.TokenAddress, FromAddress: tr.FromAddress, ToAddress: tr.ToAddress, Amount: tr.Amount, TokenID: tr.TokenID, BatchIndex: tr.BatchIndex, ChainStatus: tr.ChainStatus, ReorgID: tr.ReorgID, RawData: entityJSON(tr.RawData), BlockTime: tr.BlockTime, CreatedAt: tr.CreatedAt, RevertedAt: tr.RevertedAt})
	}
	return r.dao.SaveCanonicalBatch(ctx, rows)
}

func (r *chainRepository) ApplyReorg(ctx context.Context, reorg domain.Reorg, checkpoint domain.Checkpoint) error {
	completedAt := time.Now().Unix()
	row := postgre.ChainReorg{ReorgID: reorg.ReorgID, ChainID: reorg.ChainID, AncestorHeight: reorg.AncestorHeight, AncestorHash: reorg.AncestorHash, OldHeadHeight: reorg.OldHeadHeight, OldHeadHash: reorg.OldHeadHash, NewHeadHeight: reorg.NewHeadHeight, NewHeadHash: reorg.NewHeadHash, ReorgDepth: reorg.ReorgDepth, Status: "COMPLETED", DetectedAt: reorg.DetectedAt, CompletedAt: &completedAt}
	return r.dao.ApplyReorg(ctx, row, checkpointEntity(checkpoint))
}
