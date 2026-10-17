package domain

import "testing"

func TestEventTransferResultPreservesEmptyAndExpandedEvents(t *testing.T) {
	results := []EventTransferResult{{RawEventID: 7}, {RawEventID: 9, Transfers: []Transfer{{ChainID: 137, BatchIndex: 0}, {ChainID: 137, BatchIndex: 1}}}}
	if results[0].RawEventID != 7 || len(results[0].Transfers) != 0 || results[1].RawEventID != 9 || len(results[1].Transfers) != 2 || results[1].Transfers[1].BatchIndex != 1 {
		t.Fatalf("results=%+v", results)
	}
}
