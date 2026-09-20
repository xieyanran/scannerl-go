package engine

import (
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

// defaultWorkers deliberately does NOT copy the original's default of
// 28232 concurrent processes -- cheap for a BEAM process, not for OS file
// descriptors backing a goroutine's in-flight connection. This is a much
// more conservative default; large scans should raise it explicitly with
// -P alongside a raised `ulimit -n`.
const DefaultWorkers = 256

// Config bundles one run's fully-resolved settings: CLI overrides already
// merged onto the module's own DefaultConfig. The engine never
// re-derives these from Module.DefaultConfig() itself.
type Config struct {
	Module     fpmodule.Module
	ModuleName string
	ModArgs    []string

	Transport fpmodule.Transport
	Timeout   time.Duration
	MaxPkt    int
	Retry     int

	Workers   int
	PrivPorts bool
	WWW       bool
	SockOpts  SockOpts
}
