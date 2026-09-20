// Package output defines the contract every result formatter implements,
// mirroring the original scannerl's out_behavior.erl.
package output

import "github.com/xieyanran/scannerl-go/internal/fpmodule"

// ScanInfo describes the run, made available to Output.Init the way the
// original passed a #scaninfo record.
type ScanInfo struct {
	Version string
	Module  string
	Port    int
}

// Record is one finished probe, flowing from the engine to every
// configured Output. Target/Port are always the ORIGINALLY requested
// target/port, even if the module issued a Restart against a different
// host along the way.
type Record struct {
	Module string
	Target string
	Port   int
	Result fpmodule.Result
}

// Output is a result formatter/sink. Like fpmodule.Module, one instance
// is constructed per run (via a factory registered in the registry) and
// must not be shared/reused across runs after Clean.
type Output interface {
	// Init prepares the output (opening files, printing headers, ...)
	// using the module's own colon-separated arguments.
	Init(info ScanInfo, args []string) error

	// Write handles exactly one result record.
	Write(rec Record) error

	// Clean releases any resources acquired by Init (closing files, ...).
	Clean() error

	// Description is a one-line summary shown by -l/--list-modules.
	Description() string

	// Arguments describes this output's own colon-separated arguments,
	// also shown by -l/--list-modules.
	Arguments() []string
}
