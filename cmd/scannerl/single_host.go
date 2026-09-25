package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/xieyanran/scannerl-go/internal/engine"
	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// runSingleHost drives one M1-style scan: parse srcCfg into targets,
// probe each with mod via the engine, and write every result to the
// named output. This is today's (Milestone 1) behavior, run when -role
// is unset -- the whole run happens on this one process, no Coordinator
// or Worker involved.
func runSingleHost(ctx context.Context, mod fpmodule.Module, modCfg fpmodule.Config, moduleName, outputName string, srcCfg target.SourceConfig, workers, connectRetries int, connectBackoff time.Duration) error {
	out, ok := output.New(outputName)
	if !ok {
		return fmt.Errorf("unknown output %q (available: %v)", outputName, output.Names())
	}

	if err := out.Init(output.ScanInfo{Module: moduleName, Port: modCfg.Port}, nil); err != nil {
		return fmt.Errorf("output init: %w", err)
	}
	sink := output.NewSink([]output.Output{out}, 0)

	targets, errs := target.Stream(ctx, srcCfg)

	var wg sync.WaitGroup
	wg.Go(func() {
		for err := range errs {
			fmt.Fprintln(os.Stderr, "scannerl:", err)
		}
	})

	eng := engine.New(engine.Config{
		Module:         mod,
		ModuleName:     moduleName,
		Transport:      modCfg.Transport,
		Timeout:        modCfg.Timeout,
		MaxPkt:         modCfg.MaxPkt,
		Workers:        workers,
		ConnectRetries: connectRetries,
		ConnectBackoff: connectBackoff,
	}, sink)

	eng.Run(ctx, targets) // blocks until targets is closed or ctx is done
	wg.Wait()             // make sure every errs entry got printed before we close the sink

	if cleanupErrs := sink.Close(); len(cleanupErrs) > 0 {
		return fmt.Errorf("output cleanup: %v", cleanupErrs)
	}
	return nil
}
