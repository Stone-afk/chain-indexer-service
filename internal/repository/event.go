package repository

import (
	"context"
	"fmt"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	"github.com/Stone-afk/chain-indexer-service/internal/repository/postgre"
)

type eventRepository struct{ dao postgre.EventDAO }

func NewEventRepository(dao postgre.EventDAO) EventRepository { return &eventRepository{dao: dao} }

func (r *eventRepository) GetBizCursor(ctx context.Context, biz, chainFamily string) (int64, error) {
	if biz == "" || chainFamily == "" {
		return 0, fmt.Errorf("biz and chain family are required")
	}
	return r.dao.GetBizCursor(ctx, biz, chainFamily)
}

func (r *eventRepository) AdvanceBizCursor(ctx context.Context, biz, chainFamily string, cursor int64) error {
	if biz == "" || chainFamily == "" || cursor < 0 {
		return fmt.Errorf("invalid cursor arguments")
	}
	return r.dao.AdvanceBizCursor(ctx, biz, chainFamily, cursor)
}

func (r *eventRepository) ListRawEvents(ctx context.Context, cursor int64, limit int) ([]domain.RawEvent, error) {
	if cursor < 0 || limit <= 0 {
		return nil, fmt.Errorf("invalid page arguments")
	}
	entities, err := r.dao.ListRawEvents(ctx, cursor, limit)
	if err != nil {
		return nil, err
	}
	result := make([]domain.RawEvent, 0, len(entities))
	for _, e := range entities {
		result = append(result, domain.RawEvent{
			ID: e.ID, ChainID: e.ChainID, BlockNumber: e.BlockNumber,
			BlockHash: e.BlockHash, TxHash: e.TxHash, TxIndex: e.TxIndex,
			LogIndex: e.LogIndex, ContractAddress: e.ContractAddress,
			Topic0: e.Topic0, Topic1: e.Topic1, Topic2: e.Topic2, Topic3: e.Topic3,
			EventData: e.EventData, RawLog: e.RawLog.RawMessage(),
			ChainStatus: e.ChainStatus, ReorgID: e.ReorgID, RevertedAt: e.RevertedAt,
		})
	}
	return result, nil
}
