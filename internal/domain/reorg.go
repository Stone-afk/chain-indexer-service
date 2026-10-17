package domain

type Reorg struct {
	ReorgID        string
	ChainID        int64
	AncestorHeight int64
	AncestorHash   string
	OldHeadHeight  int64
	OldHeadHash    string
	NewHeadHeight  int64
	NewHeadHash    string
	ReorgDepth     int
	DetectedAt     int64
}
