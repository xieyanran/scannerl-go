// Package modules registers the built-in fpmodule.Module implementations
// by side-effect import (see cmd/scannerl/main.go).
package modules

import (
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

func init() {
	fpmodule.Register("tcpbanner", tcpBanner{})
}

// tcpBanner grabs whatever bytes a service sends unprompted right after
// the TCP handshake (SSH, SMTP, FTP, POP3 and friends all speak first).
// It never sends anything itself.
type tcpBanner struct{}

func (tcpBanner) DefaultConfig() fpmodule.Config {
	return fpmodule.Config{
		Transport: fpmodule.TCP,
		Timeout:   5 * time.Second,
		MaxPkt:    1,
	}
}

func (tcpBanner) Description() string {
	return "grabs the first bytes a TCP service sends unprompted, without sending anything"
}

func (tcpBanner) Arguments() []string { return nil }

// Next is called once with in.State == nil, right after connect. Whatever
// Step it returns, the engine executes, then calls Next again with
// whatever came back before returning a final Result.
//
// Work out, before writing the body:
//
//  1. First call (in.State == nil): there's no data yet, and this module
//     never sends anything. What Step asks the engine to "just listen for
//     one round, then call me back"? Check fpmodule.Continue's signature
//     and what fpmodule.Unlimited means vs. a concrete NumPackets. What do
//     you pass in the returned Step's State — and why must it be
//     non-nil, given that probeOne (internal/engine/probe.go) uses
//     `in.State == nil` as its ONLY way to recognize "this is the very
//     first call for this target"?
//
//  2. Second call: in.PacketRcv and in.Data describe what the ONE Continue
//     round above produced — not a running total. If PacketRcv < 1
//     (nothing arrived before the timeout), is that OK("") or ErrUp? If
//     you got bytes, return fpmodule.OK(string(in.Data)).
//
// internal/engine/probe_test.go's scriptModule + startTCPServer show the
// same two-call shape being driven and asserted against a real TCP
// listener — worth a read before/while writing this.
func (tcpBanner) Next(in fpmodule.Input) fpmodule.Step {
	if in.State == nil {
		// First call: no data yet, just ask the engine to listen for one
		// round and call us back with whatever it got.
		return fpmodule.Continue(1, nil, "listening")
	}

	if in.PacketRcv < 1 {
		// Second call: nothing arrived before the timeout, return an error.
		return fpmodule.ErrUp("no banner received")
	}

	// Second call: we got bytes, return them as OK.
	return fpmodule.OK(string(in.Data))
}
