package listener

import "context"

// HeadHandler handles a new-head trigger synchronously.
type HeadHandler interface {
	HandleNewHead(context.Context) error
}
