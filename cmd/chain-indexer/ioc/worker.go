package ioc

import (
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/worker/transaction"
	"time"
)

func InitWorker(r repository.EventRepository) (*transaction.Worker, error) {
	p, err := transaction.NewParser()
	if err != nil {
		return nil, err
	}
	return transaction.NewWorker(r, p, 100, time.Second), nil
}
