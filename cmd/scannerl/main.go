// Command scannerl is the CLI entrypoint wiring target parsing, the probe
// engine and result output together for a single-host scan (Milestone 1:
// no distributed mode).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	var wg sync.WaitGroup
	wg.Go(func() {
		for err := range errs {
			fmt.Fprintln(os.Stderr, "scannerl:", err)
		}
	})

	eng := engine.New(engine.Config{
		Module:     mod,
		ModuleName: *moduleName,
		Transport:  modCfg.Transport,
		Timeout:    modCfg.Timeout,
		MaxPkt:     modCfg.MaxPkt,
		Workers:    *workers,
	}, sink)

	eng.Run(ctx, targets) // blocks until targets is closed or ctx is done
	wg.Wait()             // make sure every errs entry got printed before we close the sink

	if cleanupErrs := sink.Close(); len(cleanupErrs) > 0 {
		return fmt.Errorf("output cleanup: %v", cleanupErrs)
	}
	return nil
}
