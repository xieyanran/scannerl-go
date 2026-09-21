// Command scannerl is the CLI entrypoint wiring target parsing, the probe
// engine and result output together for a single-host scan (Milestone 1:
// no distributed mode).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/xieyanran/scannerl-go/internal/engine"
	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"

	_ "github.com/xieyanran/scannerl-go/internal/fpmodule/modules" // registers built-in modules
	_ "github.com/xieyanran/scannerl-go/internal/output/outputs"   // registers built-in outputs
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "scannerl:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		moduleName = flag.String("m", "", "fingerprint module name (see -l)")
		outputName = flag.String("o", "stdout", "output name")
		targetsCSV = flag.String("i", "", "comma-separated targets (ip[/cidr][:port])")
		targetFile = flag.String("f", "", "file of targets, one per line")
		port       = flag.Int("p", 0, "port override (0 = module default)")
		workers    = flag.Int("P", 0, "worker pool size (0 = engine default)")
		listFlag   = flag.Bool("l", false, "list registered modules and outputs, then exit")
	)
	flag.Parse()

	if *listFlag {
		fmt.Println("modules:", fpmodule.Names())
		fmt.Println("outputs:", output.Names())
		return nil
	}

	mod, ok := fpmodule.Get(*moduleName)
	if !ok {
		return fmt.Errorf("unknown module %q (available: %v)", *moduleName, fpmodule.Names())
	}
	out, ok := output.New(*outputName)
	if !ok {
		return fmt.Errorf("unknown output %q (available: %v)", *outputName, output.Names())
	}

	modCfg := mod.DefaultConfig()
	if *port != 0 {
		modCfg.Port = *port
	}

	// TODO(you): replace this with a context that's cancelled on
	// SIGINT/SIGTERM (signal.NotifyContext), and `defer stop()` it. Every
	// downstream call below already respects ctx cancellation
	// (target.Stream stops producing, engine.Run's workers stop picking up
	// new targets, liveConn forces in-flight reads/writes to unblock) — so
	// this one change is what makes Ctrl+C actually work end to end.
	ctx := context.TODO()

	if err := out.Init(output.ScanInfo{Module: *moduleName, Port: modCfg.Port}, nil); err != nil {
		return fmt.Errorf("output init: %w", err)
	}
	sink := output.NewSink([]output.Output{out}, 0)

	var srcCfg target.SourceConfig
	if *targetsCSV != "" {
		srcCfg.Targets = strings.Split(*targetsCSV, ",")
	}
	if *targetFile != "" {
		srcCfg.TargetFiles = []string{*targetFile}
	}
	srcCfg.DefaultPort = modCfg.Port

	targets, errs := target.Stream(ctx, srcCfg)
	_ = errs // TODO(you): remove once errs is actually drained below

	// TODO(you): target.Stream's doc comment requires BOTH returned
	// channels to be drained concurrently, or a blocked errs reader can
	// stall targets too. Spawn a goroutine that ranges over errs (e.g.
	// fmt.Fprintln(os.Stderr, ...) each one) until it's closed. Then make
	// sure run() doesn't return until that goroutine has actually
	// finished — think about what synchronizes "goroutine is done" here
	// (there's more than one reasonable primitive), and where that
	// synchronization point belongs relative to eng.Run below and
	// sink.Close() at the end. Closing the sink before every in-flight
	// Send has happened silently drops results.

	eng := engine.New(engine.Config{
		Module:     mod,
		ModuleName: *moduleName,
		Transport:  modCfg.Transport,
		Timeout:    modCfg.Timeout,
		MaxPkt:     modCfg.MaxPkt,
		Workers:    *workers,
	}, sink)

	eng.Run(ctx, targets) // blocks until targets is closed or ctx is done

	if cleanupErrs := sink.Close(); len(cleanupErrs) > 0 {
		return fmt.Errorf("output cleanup: %v", cleanupErrs)
	}
	return nil
}
