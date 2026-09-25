package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/xieyanran/scannerl-go/internal/engine"
	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/worker"
)

// runWorker dials a Coordinator at connectAddr, receives its one-time
// target shard, probes it with mod via the unmodified M1 engine, and
// streams every result back over gRPC instead of writing to a local
// output -- this is almost a line-for-line match of runSingleHost; only
// where targets come from and where results go differ.
func runWorker(ctx context.Context, mod fpmodule.Module, modCfg fpmodule.Config, moduleName, connectAddr, workerID string, workers int) error {
	wc, err := worker.Dial(connectAddr)
	if err != nil {
		return err
	}
	defer wc.Close()

	targets, errs, err := wc.GetTargets(ctx, workerID)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for err := range errs {
			fmt.Fprintln(os.Stderr, "scannerl:", err)
		}
	})

	out := wc.NewOutput(ctx)
	if err := out.Init(output.ScanInfo{Module: moduleName, Port: modCfg.Port}, nil); err != nil {
		return fmt.Errorf("output init: %w", err)
	}
	sink := output.NewSink([]output.Output{out}, 0)

	eng := engine.New(engine.Config{
		Module:     mod,
		ModuleName: moduleName,
		Transport:  modCfg.Transport,
		Timeout:    modCfg.Timeout,
		MaxPkt:     modCfg.MaxPkt,
		Workers:    workers,
	}, sink)

	eng.Run(ctx, targets)
	wg.Wait()

	if cleanupErrs := sink.Close(); len(cleanupErrs) > 0 {
		return fmt.Errorf("output cleanup: %v", cleanupErrs)
	}
	return nil
}
