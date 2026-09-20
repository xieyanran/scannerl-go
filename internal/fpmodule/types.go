// Package fpmodule defines the contract that every fingerprinting probe
// implements, mirroring the original scannerl's fp_module.erl behavior.
package fpmodule

import "time"

// Transport selects how the engine dials a target before handing control
// to a Module.
type Transport uint8

const (
	TCP Transport = iota
	UDP
	SSL
)

func (t Transport) String() string {
	switch t {
	case TCP:
		return "tcp"
	case UDP:
		return "udp"
	case SSL:
		return "ssl"
	default:
		return "unknown"
	}
}

// Unlimited is the MaxPkt/NumPackets sentinel meaning "keep reading until
// the timeout fires", equivalent to the original's maxpkt=infinity.
const Unlimited = -1

// Config is a module's default dial parameters, returned once by
// DefaultConfig and used unless overridden by CLI flags (-p, -t, -j).
type Config struct {
	Port      int
	Transport Transport
	Timeout   time.Duration
	MaxPkt    int
}

// Module is a fingerprinting probe. A single Module value is constructed
// once and shared across every worker goroutine for the entire run, so
// implementations MUST be stateless: all per-target mutable data flows
// through Input.State / Step.State, never through fields on the receiver.
// Run tests with -race to catch violations.
type Module interface {
	// DefaultConfig returns the module's default port, transport, timeout
	// and max-packet count.
	DefaultConfig() Config

	// Description is a one-line summary shown by -l/--list-modules.
	Description() string

	// Arguments describes the module's own colon-separated arguments
	// (e.g. "-m httpbg:true"), one help string per argument, also shown
	// by -l/--list-modules.
	Arguments() []string

	// Next drives the probe forward. It is called once with a zero-value
	// State (first packet), then again after each Continue/Restart step
	// the engine performs, until it returns a Result step.
	Next(in Input) Step
}
