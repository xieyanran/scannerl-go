package main

import (
	"context"
	"fmt"
	"net"

	"github.com/xieyanran/scannerl-go/internal/coordinator"
	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// runCoordinator serves a Coordinator on listenAddr: it waits for exactly
// numWorkers Workers to connect, shards srcCfg's target list once across
// them, and merges every result streamed back into the named output --
// producing one unified output stream, just like runSingleHost does.
func runCoordinator(ctx context.Context, moduleName string, modCfg fpmodule.Config, outputName string, srcCfg target.SourceConfig, numWorkers int, listenAddr string) error {
	out, ok := output.New(outputName)
	if !ok {
		return fmt.Errorf("unknown output %q (available: %v)", outputName, output.Names())
	}
	if err := out.Init(output.ScanInfo{Module: moduleName, Port: modCfg.Port}, nil); err != nil {
		return fmt.Errorf("output init: %w", err)
	}
	sink := output.NewSink([]output.Output{out}, 0)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("coordinator: listen: %w", err)
	}

	coord := coordinator.New(ctx, coordinator.Config{
		Workers: numWorkers,
		Source:  srcCfg,
		Sink:    sink,
	})

	serveErr := make(chan error, 1)
	go func() { serveErr <- coord.Serve(ln) }()

	select {
	case <-ctx.Done():
		// Stop (not GracefulStop) so a GetTargets call still blocked
		// waiting for the remaining workers doesn't hang shutdown.
		coord.Stop()
		<-serveErr
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("coordinator: serve: %w", err)
		}
	}

	if cleanupErrs := sink.Close(); len(cleanupErrs) > 0 {
		return fmt.Errorf("output cleanup: %v", cleanupErrs)
	}
	return nil
}
