package transaction

import (
	"context"
	"time"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
)

const (
	// transactionBiz 是交易转账 worker 的业务游标名称。
	transactionBiz string = "transaction"
	// evmChainFamily 是 EVM 链族的业务游标隔离键。
	evmChainFamily string = "evm"
)

type Worker struct {
	repository   repository.EventRepository
	parser       *Parser
	batchSize    int
	idleDuration time.Duration
}

func NewWorker(repository repository.EventRepository, parser *Parser, batchSize int, idleDuration time.Duration) *Worker {
	return &Worker{
		repository:   repository,
		parser:       parser,
		batchSize:    batchSize,
		idleDuration: idleDuration,
	}
}

func (w *Worker) ProcessBatch(ctx context.Context) (int, error) {
	cursor, err := w.repository.GetBizCursor(ctx, transactionBiz, evmChainFamily)
	if err != nil {
		return 0, err
	}

	events, err := w.repository.ListRawEvents(ctx, cursor, w.batchSize)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}

	results := make([]domain.EventTransferResult, len(events))
	for i, event := range events {
		results[i], err = w.parser.Parse(event)
		if err != nil {
			return 0, err
		}
	}

	lastCursor := events[len(events)-1].ID
	if err := w.repository.CommitTransfersAndAdvanceCursor(
		ctx,
		transactionBiz,
		evmChainFamily,
		lastCursor,
		results,
	); err != nil {
		return 0, err
	}
	return len(events), nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		processed, err := w.ProcessBatch(ctx)
		if err != nil {
			return err
		}
		if processed > 0 {
			continue
		}

		timer := time.NewTimer(w.idleDuration)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
