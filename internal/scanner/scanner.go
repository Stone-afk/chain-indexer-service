package scanner

import (
	"context"
	"fmt"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

func (s *Scanner) HandleNewHead(ctx context.Context) error {
	latest, err := s.reader.LatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("read latest block number: %w", err)
	}
	if latest < s.confirmBlocks {
		return nil
	}
	safe := latest - s.confirmBlocks
	cp, found, err := s.repo.GetCheckpoint(ctx, s.chainID)
	if err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}
	next := s.startBlock
	previousHash := ""
	if found {
		header, err := s.reader.HeaderByNumber(ctx, cp.LastProcessedHeight)
		if err != nil {
			return fmt.Errorf("read checkpoint header: %w", err)
		}
		if header.Number != cp.LastProcessedHeight {
			return fmt.Errorf("checkpoint header height mismatch at height %d", cp.LastProcessedHeight)
		}
		if header.Hash != cp.LastProcessedHash {
			cp, err = s.recoverReorg(ctx, cp, latest)
			if err != nil {
				return err
			}
		}
		if safe <= cp.LastProcessedHeight {
			return nil
		}
		next = cp.LastProcessedHeight + 1
		previousHash = cp.LastProcessedHash
	}
	for height := next; height <= safe; height++ {
		block, err := s.reader.BlockByNumber(ctx, height)
		if err != nil {
			return fmt.Errorf("read block %d: %w", height, err)
		}
		if block.Header.Number != height {
			return fmt.Errorf("block height mismatch at height %d", height)
		}
		if (found || height > next) && block.Header.ParentHash != previousHash {
			return fmt.Errorf("block parent hash mismatch at height %d", height)
		}
		receipts := make([]domain.Receipt, 0, len(block.Transactions))
		for _, tx := range block.Transactions {
			receipt, err := s.reader.ReceiptByTransactionHash(ctx, tx.Hash)
			if err != nil {
				return fmt.Errorf("read receipt at block %d: %w", height, err)
			}
			receipts = append(receipts, receipt)
		}
		batch, err := mapCanonicalBatch(s.chainID, block, receipts, time.Now().Unix())
		if err != nil {
			return fmt.Errorf("map block %d: %w", height, err)
		}
		if err := s.repo.SaveCanonicalBatch(ctx, batch); err != nil {
			return fmt.Errorf("save block %d: %w", height, err)
		}
		previousHash = block.Header.Hash
		if height == safe {
			break
		}
	}
	return nil
}
