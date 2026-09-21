// Package outputs registers the built-in Output implementations by
// side-effect import (see cmd/scannerl/main.go).
package outputs

import (
	"fmt"
	"io"
	"os"

	"github.com/xieyanran/scannerl-go/internal/output"
)

func init() {
	output.Register("stdout", func() output.Output { return &stdoutOutput{w: os.Stdout} })
}

// stdoutOutput prints one line per Record. It needs no locking even
// though multiple worker goroutines finish probes concurrently: every
// Write call is already serialized through output.Sink's single
// consumer goroutine, so at most one Write is ever in flight.
type stdoutOutput struct {
	w io.Writer
}

func (o *stdoutOutput) Init(info output.ScanInfo, args []string) error { return nil }

func (o *stdoutOutput) Write(rec output.Record) error {
	_, err := fmt.Fprintf(o.w, "%s:%d\t%s\t%s\t%v\n",
		rec.Target, rec.Port, rec.Module, rec.Result.Outcome, rec.Result.Value)
	return err
}

func (o *stdoutOutput) Clean() error { return nil }

func (o *stdoutOutput) Description() string { return "prints one line per result to stdout" }

func (o *stdoutOutput) Arguments() []string { return nil }
