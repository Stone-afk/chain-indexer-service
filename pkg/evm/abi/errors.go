package abi

// DecodeError identifies malformed input for a recognized event.
type DecodeError struct {
	Err error
}

func (e *DecodeError) Error() string { return "decode event: " + e.Err.Error() }
func (e *DecodeError) Unwrap() error { return e.Err }
