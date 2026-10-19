package scanner

import (
	"context"
	"errors"
	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"reflect"
	"regexp"
	"testing"
)

func TestParentHashDiscontinuityStops(t *testing.T) {
	r, p, _ := fixture(t)
	p.found = true
	p.cp = domain.Checkpoint{ChainID: 1, LastProcessedHeight: 2, LastProcessedHash: "hash-2"}
	b := r.blocks[3]
	b.Header.ParentHash = "alien"
	r.blocks[3] = b
	err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
	require(t, !(err == nil), "expected discontinuity")
	require(t, !(len(p.batches) != 0 || len(p.reorgs) != 0 || p.cp.LastProcessedHeight != 2), "discontinuity wrote state")
}
func TestShallowReorgDoesNotRollback(t *testing.T) {
	r, p, _ := fixture(t)
	p.found = true
	p.cp = domain.Checkpoint{ChainID: 1, LastProcessedHeight: 3, LastProcessedHash: "hash-3"}
	r.headers[5] = domain.BlockHeader{Number: 5, Hash: "changed-unconfirmed", ParentHash: "changed-4"}
	err := New(r, p, 1, 0, 2).HandleNewHead(context.Background())
	require(t, !(err != nil), err)
	require(t, !(len(p.reorgs) != 0 || len(p.batches) != 0 || p.cp.LastProcessedHash != "hash-3"), "shallow reorg changed state")
}
func deepFork(t *testing.T) (*readerFake, *chainRepoFake, *[]string) {
	r, p, calls := fixture(t)
	p.found = true
	p.cp = domain.Checkpoint{ChainID: 1, LastProcessedHeight: 4, LastProcessedHash: "old-4"}
	for _, n := range []int64{3, 4} {
		v := p.stored[n]
		v.BlockHash = "old"
		if n == 4 {
			v.BlockHash = "old-4"
			v.ParentHash = "old"
		}
		p.stored[n] = v
	}
	return r, p, calls
}
func TestDeepReorgAncestorApplyBeforeRescan(t *testing.T) {
	r, p, calls := deepFork(t)
	err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
	require(t, !(err != nil), err)
	require(t, !(len(p.reorgs) != 1 || len(p.rewinds) != 1), p.reorgs, p.rewinds)
	v := p.reorgs[0]
	if v.ChainID != 1 || v.AncestorHeight != 2 || v.AncestorHash != "hash-2" || v.OldHeadHeight != 4 || v.OldHeadHash != "old-4" || v.NewHeadHeight != 5 || v.NewHeadHash != "hash-5" || v.ReorgDepth != 2 || v.DetectedAt <= 0 {
		t.Fatalf("reorg %+v", v)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(v.ReorgID) {
		t.Fatalf("UUID %q", v.ReorgID)
	}
	require(t, !(p.rewinds[0] != (domain.Checkpoint{ChainID: 1, LastProcessedHeight: 2, LastProcessedHash: "hash-2"}) || !reflect.DeepEqual(heights(p.batches), []int64{3, 4, 5}) || p.cp.LastProcessedHeight != 5), p.rewinds, heights(p.batches), p.cp)
	apply := -1
	for i, c := range *calls {
		if c == "apply" {
			apply = i
		}
		require(t, !(c == "save:3" && (apply < 0 || apply >= i)), "saved before reorg commit")
	}
}
func TestAncestorFailureLeavesStateUntouched(t *testing.T) {
	for _, stage := range []string{"header:3", "stored:3", "missing ancestor", "apply"} {
		t.Run(stage, func(t *testing.T) {
			r, p, calls := deepFork(t)
			if stage == "missing ancestor" {
				p.stored = map[int64]domain.StoredBlock{}
			} else {
				r.fail = stage
				p.fail = stage
			}
			err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
			require(t, !(err == nil), "expected error")
			if stage != "missing ancestor" && !errors.Is(err, errStage) {
				t.Fatalf("lost cause: %v", err)
			}
			require(t, !(len(p.batches) != 0 || len(p.reorgs) != 0 || p.cp.LastProcessedHash != "old-4"), "failed search/apply changed state")
			for _, c := range *calls {
				require(t, !(c == "save:3"), "rescan before successful apply")
			}
		})
	}
}
func TestReorgRescanFailureKeepsLastCommittedHeight(t *testing.T) {
	r, p, calls := deepFork(t)
	r.fail = "receipt:tx-4"
	err := New(r, p, 1, 0, 0).HandleNewHead(context.Background())
	require(t, errors.Is(err, errStage), err)
	require(t, !(len(p.reorgs) != 1 || !reflect.DeepEqual(heights(p.batches), []int64{3}) || p.cp.LastProcessedHeight != 3 || p.cp.LastProcessedHash != "hash-3"), p.reorgs, heights(p.batches), p.cp)
	for _, c := range *calls {
		require(t, !(c == "block:5"), "crossed failed rescan")
	}
}
