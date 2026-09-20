package fpmodule

// Outcome mirrors the original's {status, type} result tag.
type Outcome uint8

const (
	// OKOutcome: fingerprinting succeeded. Equivalent to {ok, result}.
	OKOutcome Outcome = iota
	// ErrUpOutcome: the target responded but fingerprinting did not
	// succeed (e.g. connection refused/reset, unexpected protocol data).
	// Equivalent to {error, up}.
	ErrUpOutcome
	// ErrUnknownOutcome: fingerprinting failed and reachability is
	// unknown (e.g. DNS failure, internal error). Equivalent to
	// {error, unknown}.
	ErrUnknownOutcome
)

func (o Outcome) String() string {
	switch o {
	case OKOutcome:
		return "ok"
	case ErrUpOutcome:
		return "error_up"
	case ErrUnknownOutcome:
		return "error_unknown"
	default:
		return "unknown"
	}
}

// Result is a finished probe's outcome. Value should be one of: string,
// bool, []string, []any, or fmt.Stringer/netip.Addr — the shapes output
// modules know how to format.
type Result struct {
	Outcome Outcome
	Value   any
}
