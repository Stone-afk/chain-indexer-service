package transaction

import (
	"math/big"
	"strconv"
	"strings"

	"github.com/Stone-afk/chain-indexer-service/internal/domain"
	evmabi "github.com/Stone-afk/chain-indexer-service/pkg/evm/abi"
)

type Parser struct {
	events  evmabi.TransferEvents
	decoder *evmabi.Decoder
}

func NewParser() (*Parser, error) {
	events, err := evmabi.NewTransferEvents()
	if err != nil {
		return nil, err
	}
	return &Parser{events: events, decoder: evmabi.NewDecoder()}, nil
}

func (p *Parser) Parse(raw domain.RawEvent) (domain.EventTransferResult, error) {
	empty := domain.EventTransferResult{RawEventID: raw.ID}
	topics := eventTopics(raw)
	event, ok := p.eventFor(raw, len(topics))
	if !ok || raw.EventData == nil {
		return empty, nil
	}

	decoded, err := p.decoder.Decode(event, topics, *raw.EventData)
	if err != nil || decoded == nil {
		return empty, nil
	}
	return p.mapDecoded(raw, event, decoded), nil
}

func (p *Parser) eventFor(raw domain.RawEvent, topicCount int) (evmabi.ParsedEvent, bool) {
	if raw.Topic0 == nil {
		return evmabi.ParsedEvent{}, false
	}
	switch {
	case strings.EqualFold(*raw.Topic0, p.events.ERC20Transfer.Topic0):
		if topicCount == 4 {
			return p.events.ERC721Transfer, true
		}
		if topicCount == 3 {
			return p.events.ERC20Transfer, true
		}
		return evmabi.ParsedEvent{}, false
	case strings.EqualFold(*raw.Topic0, p.events.ERC1155TransferSingle.Topic0):
		return p.events.ERC1155TransferSingle, true
	case strings.EqualFold(*raw.Topic0, p.events.ERC1155TransferBatch.Topic0):
		return p.events.ERC1155TransferBatch, true
	default:
		return evmabi.ParsedEvent{}, false
	}
}

func eventTopics(raw domain.RawEvent) []string {
	topics := make([]string, 0, 4)
	for _, topic := range []*string{raw.Topic0, raw.Topic1, raw.Topic2, raw.Topic3} {
		if topic == nil {
			break
		}
		topics = append(topics, *topic)
	}
	return topics
}

func (p *Parser) mapDecoded(raw domain.RawEvent, event evmabi.ParsedEvent, decoded map[string]any) domain.EventTransferResult {
	empty := domain.EventTransferResult{RawEventID: raw.ID}
	from, fromOK := decoded["from"].(string)
	to, toOK := decoded["to"].(string)
	if !fromOK || !toOK {
		return empty
	}

	switch {
	case sameEvent(event, p.events.ERC20Transfer):
		amount, ok := uint256Decimal(decoded["value"])
		if !ok {
			return empty
		}
		return p.singleResult(raw, domain.TransferTypeERC20.ToString(), from, to, amount, nil)
	case sameEvent(event, p.events.ERC721Transfer):
		tokenID, ok := uint256Decimal(decoded["tokenId"])
		if !ok {
			return empty
		}
		amount := "1"
		return p.singleResult(raw, domain.TransferTypeERC721.ToString(), from, to, amount, &tokenID)
	case sameEvent(event, p.events.ERC1155TransferSingle):
		if _, ok := decoded["operator"].(string); !ok {
			return empty
		}
		tokenID, idOK := uint256Decimal(decoded["id"])
		amount, amountOK := uint256Decimal(decoded["value"])
		if !idOK || !amountOK {
			return empty
		}
		return p.singleResult(raw, domain.TransferTypeERC1155.ToString(), from, to, amount, &tokenID)
	case sameEvent(event, p.events.ERC1155TransferBatch):
		if _, ok := decoded["operator"].(string); !ok {
			return empty
		}
		ids, idsOK := uint256Decimals(decoded["ids"])
		amounts, amountsOK := uint256Decimals(decoded["values"])
		if !idsOK || !amountsOK || len(ids) == 0 || len(ids) != len(amounts) {
			return empty
		}
		transfers := make([]domain.Transfer, len(ids))
		for i := range ids {
			transfers[i] = transferFrom(raw, domain.TransferTypeERC1155.ToString(), from, to, amounts[i], &ids[i], i)
		}
		return domain.EventTransferResult{RawEventID: raw.ID, Transfers: transfers}
	default:
		return empty
	}
}

func (p *Parser) singleResult(raw domain.RawEvent, typ, from, to, amount string, tokenID *string) domain.EventTransferResult {
	return domain.EventTransferResult{
		RawEventID: raw.ID,
		Transfers:  []domain.Transfer{transferFrom(raw, typ, from, to, amount, tokenID, 0)},
	}
}

func transferFrom(raw domain.RawEvent, typ, from, to, amount string, tokenID *string, batchIndex int) domain.Transfer {
	tokenAddress := raw.ContractAddress
	return domain.Transfer{
		ChainID:      raw.ChainID,
		BlockNumber:  raw.BlockNumber,
		BlockTime:    raw.BlockTime,
		BlockHash:    raw.BlockHash,
		TxHash:       raw.TxHash,
		TxIndex:      raw.TxIndex,
		SourceType:   domain.TransferSourceEventLog.ToString(),
		SourceIndex:  sourceIndex(raw.LogIndex),
		TransferType: typ,
		TokenAddress: &tokenAddress,
		FromAddress:  from,
		ToAddress:    to,
		Amount:       &amount,
		TokenID:      tokenID,
		BatchIndex:   batchIndex,
		ChainStatus:  raw.ChainStatus,
		ReorgID:      raw.ReorgID,
		RawData:      raw.RawLog,
		RevertedAt:   raw.RevertedAt,
	}
}

func sourceIndex(logIndex int) string { return strconv.Itoa(logIndex) }

func uint256Decimals(value any) ([]string, bool) {
	values, ok := value.([]*big.Int)
	if !ok {
		return nil, false
	}
	result := make([]string, len(values))
	for i, item := range values {
		var valid bool
		result[i], valid = uint256Decimal(item)
		if !valid {
			return nil, false
		}
	}
	return result, true
}

func uint256Decimal(value any) (string, bool) {
	number, ok := value.(*big.Int)
	if !ok || number == nil || number.Sign() < 0 || number.BitLen() > 256 {
		return "", false
	}
	return number.String(), true
}

func sameEvent(left, right evmabi.ParsedEvent) bool {
	if !strings.EqualFold(left.Topic0, right.Topic0) || len(left.IndexedFlags) != len(right.IndexedFlags) {
		return false
	}
	for i := range left.IndexedFlags {
		if left.IndexedFlags[i] != right.IndexedFlags[i] {
			return false
		}
	}
	return true
}
