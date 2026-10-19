package scanner

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

func mapCanonicalBatch(chainID int64, block domain.Block, receipts []domain.Receipt, createdAt int64) (domain.CanonicalBatch, error) {
	if len(receipts) != len(block.Transactions) {
		return domain.CanonicalBatch{}, fmt.Errorf("receipt count does not match transactions")
	}
	byHash := make(map[string]domain.Receipt, len(receipts))
	for _, receipt := range receipts {
		if _, exists := byHash[receipt.TxHash]; exists {
			return domain.CanonicalBatch{}, fmt.Errorf("duplicate receipt")
		}
		byHash[receipt.TxHash] = receipt
	}
	h := block.Header
	if block.BaseFeePerGas != "" {
		if _, err := domain.ParseUint256Decimal(block.BaseFeePerGas); err != nil {
			return domain.CanonicalBatch{}, fmt.Errorf("invalid block base fee: %w", err)
		}
	}
	batch := domain.CanonicalBatch{
		Block: domain.StoredBlock{
			ChainID: chainID, BlockNumber: h.Number, BlockHash: h.Hash, ParentHash: h.ParentHash,
			BlockTime: h.Timestamp, Status: domain.BlockStatusCanonical.ToString(), Finality: domain.FinalityConfirmed.ToString(),
			TransactionCount: len(block.Transactions), StateRoot: &block.StateRoot,
			TransactionsRoot: &block.TransactionsRoot, ReceiptsRoot: &block.ReceiptsRoot,
			GasLimit:      mapperString(strconv.FormatInt(block.GasLimit, 10)),
			GasUsed:       mapperString(strconv.FormatInt(block.GasUsed, 10)),
			BaseFeePerGas: mapperOptionalString(block.BaseFeePerGas), RawBlock: block.RawBlock, CreatedAt: createdAt,
		},
		Checkpoint: domain.Checkpoint{ChainID: chainID, LastProcessedHeight: h.Number, LastProcessedHash: h.Hash},
	}
	rawReceipts := make([]json.RawMessage, 0, len(receipts))
	for _, tx := range block.Transactions {
		receipt, exists := byHash[tx.Hash]
		if !exists {
			return domain.CanonicalBatch{}, fmt.Errorf("missing transaction receipt")
		}
		delete(byHash, tx.Hash)
		if receipt.BlockNumber != h.Number || receipt.BlockHash != h.Hash || receipt.TxIndex != tx.Index {
			return domain.CanonicalBatch{}, fmt.Errorf("receipt identity does not match transaction and block")
		}
		var txGasPrice, effectiveGasPrice domain.Uint256
		var err error
		if tx.GasPrice != "" {
			txGasPrice, err = domain.ParseUint256Decimal(tx.GasPrice)
			if err != nil {
				return domain.CanonicalBatch{}, fmt.Errorf("invalid transaction gas amount")
			}
		}
		if receipt.EffectiveGasPrice != "" {
			effectiveGasPrice, err = domain.ParseUint256Decimal(receipt.EffectiveGasPrice)
			if err != nil {
				return domain.CanonicalBatch{}, fmt.Errorf("invalid transaction gas amount")
			}
		}
		gasPriceValue := effectiveGasPrice
		if receipt.EffectiveGasPrice == "" {
			gasPriceValue = txGasPrice
		}
		if receipt.EffectiveGasPrice == "" && tx.GasPrice == "" || receipt.GasUsed < 0 {
			return domain.CanonicalBatch{}, fmt.Errorf("invalid transaction gas amount")
		}
		value, err := domain.ParseUint256Decimal(tx.Value)
		if err != nil {
			return domain.CanonicalBatch{}, fmt.Errorf("invalid transaction value")
		}
		gasPriceInt, _ := new(big.Int).SetString(gasPriceValue.String(), 10)
		feeInt := new(big.Int).Mul(gasPriceInt, big.NewInt(receipt.GasUsed))
		fee, err := domain.ParseUint256Decimal(feeInt.String())
		if err != nil {
			return domain.CanonicalBatch{}, fmt.Errorf("invalid transaction fee")
		}
		feeString := fee.String()
		status := domain.ExecutionStatusFailed.ToString()
		if receipt.Status == 1 {
			status = domain.ExecutionStatusSuccess.ToString()
		}
		stored := domain.StoredTransaction{
			ChainID: chainID, BlockNumber: h.Number, BlockHash: h.Hash, TxHash: tx.Hash,
			TxIndex: tx.Index, FromAddress: tx.From, ToAddress: tx.To, Nonce: tx.Nonce,
			Value: tx.Value, Input: &tx.Input,
			GasLimit: mapperString(strconv.FormatInt(tx.GasLimit, 10)),
			GasUsed:  mapperString(strconv.FormatInt(receipt.GasUsed, 10)), GasPrice: mapperOptionalString(tx.GasPrice),
			TransactionFee: &feeString, ExecutionStatus: status, ContractAddress: receipt.ContractAddress,
			ChainStatus: domain.ChainStatusActive.ToString(), RawTransaction: tx.RawTransaction, RawReceipt: receipt.RawReceipt,
			BlockTime: h.Timestamp, CreatedAt: createdAt,
		}
		if receipt.EffectiveGasPrice != "" {
			stored.EffectiveGasPrice = &receipt.EffectiveGasPrice
		}
		if len(tx.Input) >= 10 && tx.Input[:2] == "0x" {
			stored.MethodID = mapperString(tx.Input[:10])
		}
		batch.Transactions = append(batch.Transactions, stored)
		rawReceipts = append(rawReceipts, receipt.RawReceipt)
		for _, log := range receipt.Logs {
			if log.BlockNumber != h.Number || log.BlockHash != h.Hash || log.TxHash != tx.Hash || log.TxIndex != tx.Index {
				return domain.CanonicalBatch{}, fmt.Errorf("log identity does not match transaction and block")
			}
			event := domain.RawEvent{
				ChainID: chainID, BlockNumber: h.Number, BlockHash: h.Hash, TxHash: tx.Hash,
				TxIndex: &tx.Index, LogIndex: log.Index, ContractAddress: log.Address,
				EventData: &log.Data, RawLog: log.RawLog, ChainStatus: domain.ChainStatusActive.ToString(),
			}
			topics := []*(*string){&event.Topic0, &event.Topic1, &event.Topic2, &event.Topic3}
			for i := 0; i < len(log.Topics) && i < len(topics); i++ {
				*topics[i] = mapperString(log.Topics[i])
			}
			batch.RawEvents = append(batch.RawEvents, event)
		}
		if receipt.Status == 1 && value.String() != "0" {
			to := tx.To
			if to == nil {
				to = receipt.ContractAddress
			}
			if to == nil {
				return domain.CanonicalBatch{}, fmt.Errorf("missing native transfer recipient")
			}
			batch.NativeTransfers = append(batch.NativeTransfers, domain.Transfer{
				ChainID: chainID, BlockNumber: h.Number, BlockHash: h.Hash, TxHash: tx.Hash,
				TxIndex: &tx.Index, SourceType: domain.TransferSourceTransactionValue.ToString(), SourceIndex: domain.TransferSourceIndexTransactionValue.ToString(),
				TransferType: domain.TransferTypeNative.ToString(), FromAddress: tx.From, ToAddress: *to, Amount: &tx.Value,
				ChainStatus: domain.ChainStatusActive.ToString(), BlockTime: h.Timestamp, CreatedAt: createdAt,
			})
		}
	}
	var err error
	batch.Block.RawReceipts, err = json.Marshal(rawReceipts)
	if err != nil {
		return domain.CanonicalBatch{}, fmt.Errorf("encode raw receipts: %w", err)
	}
	return batch, nil
}

func mapperString(value string) *string { return &value }

func mapperOptionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
