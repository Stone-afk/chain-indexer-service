package domain

type Checkpoint struct {
	ChainID             int64
	LastProcessedHeight int64
	LastProcessedHash   string
}
