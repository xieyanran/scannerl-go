// Command scannerl is the CLI entrypoint wiring target parsing, the probe
// engine and result output together. -role selects between a
// single-host scan (Milestone 1, the default) and the distributed
// Coordinator/Worker roles added in Milestone 2 (see
// internal/coordinator, internal/worker).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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

		role        = flag.String("role", "", `run mode: "" (single-host, default), "coordinator", or "worker"`)
		numWorkers  = flag.Int("workers", 0, "coordinator: number of workers to wait for before pushing shards")
		listenAddr  = flag.String("listen", ":9090", "coordinator: gRPC listen address")
		registerTO  = flag.Duration("register-timeout", 0, "coordinator: abort if fewer than -workers connect within this long (0 = wait forever)")
		connectAddr = flag.String("connect", "", "worker: coordinator address to dial")
		workerID    = flag.String("worker-id", "", "worker: identifier reported to the coordinator (default: hostname)")

		connectRetries = flag.Int("connect-retries", 2, "extra CONNECT attempts after a dial failure, with backoff (0 disables)")
		connectBackoff = flag.Duration("connect-backoff", 250*time.Millisecond, "delay before the first connect retry, doubling each attempt")
		maxDuration    = flag.Duration("max-duration", 0, "overall scan deadline bounding the whole run, not just per-connection timeouts (0 = unlimited)")
	)
	flag.Parse()

	if *listFlag {
		fmt.Println("modules:", fpmodule.Names())
		fmt.Println("outputs:", output.Names())
		return nil
	}

	set := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })

	mod, ok := fpmodule.Get(*moduleName)
	if !ok {
		return fmt.Errorf("unknown module %q (available: %v)", *moduleName, fpmodule.Names())
	}
	modCfg := mod.DefaultConfig()
	if *port != 0 {
		modCfg.Port = *port
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// An optional overall deadline, bounding the whole run end to end --
	// distinct from modCfg.Timeout, which only bounds a single
	// dial/read/write. Applies to every role, alongside the
	// signal-based cancellation above.
	if *maxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *maxDuration)
		defer cancel()
	}

	switch *role {
	case "":
		return runSingleHost(ctx, mod, modCfg, *moduleName, *outputName, buildSourceConfig(*targetsCSV, *targetFile, modCfg.Port), *workers, *connectRetries, *connectBackoff)

	case "coordinator":
		if *numWorkers <= 0 {
			return fmt.Errorf("-role coordinator requires -workers > 0")
		}
		return runCoordinator(ctx, *moduleName, modCfg, *outputName, buildSourceConfig(*targetsCSV, *targetFile, modCfg.Port), *numWorkers, *listenAddr, *registerTO)

	case "worker":
		if *connectAddr == "" {
			return fmt.Errorf("-role worker requires -connect")
		}
		if set["i"] || set["f"] {
			fmt.Fprintln(os.Stderr, "scannerl: -i/-f are ignored in worker role (targets come from the coordinator)")
		}
		if set["o"] {
			fmt.Fprintln(os.Stderr, "scannerl: -o is ignored in worker role (results are streamed to the coordinator)")
		}
		id := *workerID
		if id == "" {
			if h, err := os.Hostname(); err == nil {
				id = h
			}
		}
		return runWorker(ctx, mod, modCfg, *moduleName, *connectAddr, id, *workers, *connectRetries, *connectBackoff)

	default:
		return fmt.Errorf(`unknown -role %q (want "", "coordinator", or "worker")`, *role)
	}
}

// buildSourceConfig builds the target.SourceConfig a single-host run or a
// Coordinator's shard computation resolves targets from.
func buildSourceConfig(targetsCSV, targetFile string, defaultPort int) target.SourceConfig {
	var srcCfg target.SourceConfig
	if targetsCSV != "" {
		srcCfg.Targets = strings.Split(targetsCSV, ",")
	}
	if targetFile != "" {
		srcCfg.TargetFiles = []string{targetFile}
	}
	srcCfg.DefaultPort = defaultPort
	return srcCfg
}
