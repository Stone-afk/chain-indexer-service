package transaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
)

func TestRunAlreadyCanceledDoesNotProcessBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repository := &fakeEventRepo{
		events:    []domain.RawEvent{{ID: 1}},
		commitErr: errors.New("commit after cancellation"),
	}
	err := NewWorker(repository, mustParser(t), 100, time.Second).Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if repository.lists != 0 || repository.commits != 0 {
		t.Fatalf("repository calls after cancellation: lists=%d commits=%d", repository.lists, repository.commits)
	}
}
