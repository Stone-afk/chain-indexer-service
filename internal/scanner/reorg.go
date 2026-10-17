package scanner

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

func (s *Scanner) recoverReorg(ctx context.Context, old domain.Checkpoint, latest int64) (domain.Checkpoint, error) {
	for height := old.LastProcessedHeight - 1; height >= 0; height-- {
		local, found, err := s.repo.GetCanonicalBlock(ctx, s.chainID, height)
		if err != nil {
			return domain.Checkpoint{}, fmt.Errorf("read canonical block %d: %w", height, err)
		}
		if !found || local.ChainID != s.chainID || local.BlockNumber != height {
			return domain.Checkpoint{}, fmt.Errorf("canonical block missing or invalid at height %d", height)
		}
		remote, err := s.reader.HeaderByNumber(ctx, height)
		if err != nil {
			return domain.Checkpoint{}, fmt.Errorf("read ancestor header %d: %w", height, err)
		}
		if remote.Number != height {
			return domain.Checkpoint{}, fmt.Errorf("ancestor header height mismatch at height %d", height)
		}
		if local.BlockHash != remote.Hash {
			continue
		}
		head, err := s.reader.HeaderByNumber(ctx, latest)
		if err != nil {
			return domain.Checkpoint{}, fmt.Errorf("read reorg latest header: %w", err)
		}
		if head.Number != latest {
			return domain.Checkpoint{}, fmt.Errorf("reorg latest header height mismatch at height %d", latest)
		}
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return domain.Checkpoint{}, fmt.Errorf("generate reorg ID: %w", err)
		}
		id[6] = (id[6] & 0x0f) | 0x40
		id[8] = (id[8] & 0x3f) | 0x80
		reorg := domain.Reorg{
			ReorgID: fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]),
			ChainID: s.chainID, AncestorHeight: height, AncestorHash: remote.Hash,
			OldHeadHeight: old.LastProcessedHeight, OldHeadHash: old.LastProcessedHash,
			NewHeadHeight: latest, NewHeadHash: head.Hash,
			ReorgDepth: int(old.LastProcessedHeight - height), DetectedAt: time.Now().Unix(),
		}
		ancestor := domain.Checkpoint{ChainID: s.chainID, LastProcessedHeight: height, LastProcessedHash: remote.Hash}
		if err := s.repo.ApplyReorg(ctx, reorg, ancestor); err != nil {
			return domain.Checkpoint{}, fmt.Errorf("apply reorg: %w", err)
		}
		return ancestor, nil
	}
	return domain.Checkpoint{}, fmt.Errorf("no common ancestor for checkpoint at height %d", old.LastProcessedHeight)
}
