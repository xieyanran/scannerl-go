package fpmodule

// Kind discriminates the three verbs a Module.Next call can return,
// mirroring the original callback_next_step's {continue,...},
// {restart,...} and {result,...} tuples.
type Kind uint8

const (
	// KindContinue: send Payload on the current connection (opening one
	// first if needed) and wait for NumPackets reads or a timeout, then
	// call Next again.
	KindContinue Kind = iota
	// KindRestart: close the current connection and open a new one,
	// optionally against a new target/port, then call Next again.
	KindRestart
	// KindResult: the probe is finished.
	KindResult
)

// Step is a tagged union of the three verbs above. Never construct one as
// a struct literal; use Continue, Restart, OK, ErrUp or ErrUnknown so the
// Kind tag can't drift out of sync with the fields that back it.
type Step struct {
	Kind Kind

	// KindContinue fields.
	NumPackets int
	Payload    []byte

	// KindRestart fields. Empty NewTarget / zero NewPort mean "keep the
	// current one".
	NewTarget string
	NewPort   int

	// KindResult field.
	Result Result

	// State is carried forward to the next call's Input.State, for
	// KindContinue and KindRestart.
	State any
}

// Continue sends payload and waits for numPackets reads (or Unlimited) or
// a timeout before Next is called again.
func Continue(numPackets int, payload []byte, state any) Step {
	return Step{Kind: KindContinue, NumPackets: numPackets, Payload: payload, State: state}
}

// Restart closes and reopens the connection, optionally against a new
// target/port ("" / 0 to keep the current one).
func Restart(newTarget string, newPort int, state any) Step {
	return Step{Kind: KindRestart, NewTarget: newTarget, NewPort: newPort, State: state}
}

// ResultStep finishes the probe with an arbitrary Result.
func ResultStep(r Result) Step {
	return Step{Kind: KindResult, Result: r}
}

// OK finishes the probe successfully with value v.
func OK(v any) Step {
	return ResultStep(Result{Outcome: OKOutcome, Value: v})
}

// ErrUp finishes the probe as "reachable but fingerprinting failed".
func ErrUp(v any) Step {
	return ResultStep(Result{Outcome: ErrUpOutcome, Value: v})
}

// ErrUnknown finishes the probe as "reachability unknown".
func ErrUnknown(v any) Step {
	return ResultStep(Result{Outcome: ErrUnknownOutcome, Value: v})
}
