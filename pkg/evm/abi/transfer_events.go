package abi

import "fmt"

const (
	erc20TransferDeclaration         = "event Transfer(address indexed from, address indexed to, uint256 value)"
	erc721TransferDeclaration        = "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)"
	erc1155TransferSingleDeclaration = "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)"
	erc1155TransferBatchDeclaration  = "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)"
)

// TransferEvents contains the standard token transfer event declarations.
type TransferEvents struct {
	ERC20Transfer         ParsedEvent
	ERC721Transfer        ParsedEvent
	ERC1155TransferSingle ParsedEvent
	ERC1155TransferBatch  ParsedEvent
}

// NewTransferEvents parses the standard token transfer event declarations.
func NewTransferEvents() (TransferEvents, error) {
	var events TransferEvents
	var err error
	events.ERC20Transfer, err = ParseEventDeclaration(erc20TransferDeclaration)
	if err != nil {
		return TransferEvents{}, fmt.Errorf("parse ERC20 Transfer: %w", err)
	}
	events.ERC721Transfer, err = ParseEventDeclaration(erc721TransferDeclaration)
	if err != nil {
		return TransferEvents{}, fmt.Errorf("parse ERC721 Transfer: %w", err)
	}
	events.ERC1155TransferSingle, err = ParseEventDeclaration(erc1155TransferSingleDeclaration)
	if err != nil {
		return TransferEvents{}, fmt.Errorf("parse ERC1155 TransferSingle: %w", err)
	}
	events.ERC1155TransferBatch, err = ParseEventDeclaration(erc1155TransferBatchDeclaration)
	if err != nil {
		return TransferEvents{}, fmt.Errorf("parse ERC1155 TransferBatch: %w", err)
	}
	return events, nil
}
