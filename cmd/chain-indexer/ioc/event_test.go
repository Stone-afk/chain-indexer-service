package ioc

import (
	"context"
	"testing"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
	"gorm.io/gorm"
)

func TestEventRepositoryIoCSignature(t *testing.T) {
	var initDAO func(*gorm.DB) postgre.EventDAO = InitEventDAO
	var initRepository func(postgre.EventDAO) repository.EventRepository = InitEventRepository
	// Construction does not connect to PostgreSQL. Consumers receive only the
	// repository contract, including the atomic result/cursor operation.
	r := initRepository(initDAO(&gorm.DB{}))
	var commit func(context.Context, string, string, int64, []domain.EventTransferResult) error = r.CommitTransfersAndAdvanceCursor
	_ = commit
}
