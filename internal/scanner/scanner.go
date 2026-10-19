package scanner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

var (
	ErrFinalIdentityMismatch = errors.New("final block identity mismatch")
	ErrStateFenceRejected    = errors.New("stored block state fence rejected")
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
		stored, found, err := s.repo.GetBlockByHash(ctx, s.chainID, block.Header.Hash)
		if err != nil {
			return fmt.Errorf("classify block %d: %w", height, err)
		}
		// This final RPC observation narrows, but cannot eliminate, the window
		// before the database write; it is not a distributed consistency fence.
		finalHeader, err := s.reader.HeaderByNumber(ctx, height)
		if err != nil {
			return fmt.Errorf("read final block header %d: %w", height, err)
		}
		if finalHeader.Number != block.Header.Number || finalHeader.Hash != block.Header.Hash || finalHeader.ParentHash != block.Header.ParentHash {
			return fmt.Errorf("final block header mismatch at height %d: %w", height, ErrFinalIdentityMismatch)
		}
		if !found {
			if err := s.repo.SaveCanonicalBatch(ctx, batch, finalHeader); err != nil {
				return fmt.Errorf("save block %d: %w", height, err)
			}
		} else {
			switch stored.Status {
			case domain.BlockStatusCanonical.ToString():
				if err := s.repo.SaveCanonicalBatch(ctx, batch, finalHeader); err != nil {
					return fmt.Errorf("replay block %d: %w", height, err)
				}
			case domain.BlockStatusOrphaned.ToString():
				if stored.ReorgID == nil || *stored.ReorgID == "" {
					return fmt.Errorf("orphaned block %d missing reorg ID: %w", height, ErrStateFenceRejected)
				}
				if err := s.repo.ReactivateCanonicalBatch(ctx, batch, finalHeader, *stored.ReorgID); err != nil {
					return fmt.Errorf("reactivate block %d: %w", height, err)
				}
			default:
				return fmt.Errorf("unsupported stored block status %q at height %d: %w", stored.Status, height, ErrStateFenceRejected)
			}
		}
		previousHash = block.Header.Hash
		if height == safe {
			break
		}
	}
	return nil
}
