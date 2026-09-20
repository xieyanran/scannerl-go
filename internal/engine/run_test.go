package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// recordingOutput collects every Record it's given; safe for concurrent
// Write calls since output.Sink actually serializes them through one
// goroutine, but the mutex keeps this test double correct even if that
// changes.
type recordingOutput struct {
	mu     sync.Mutex
	writes []output.Record
}

func (r *recordingOutput) Init(output.ScanInfo, []string) error { return nil }
func (r *recordingOutput) Write(rec output.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, rec)
	return nil
}
func (r *recordingOutput) Clean() error        { return nil }
func (r *recordingOutput) Description() string { return "recording" }
func (r *recordingOutput) Arguments() []string { return nil }

func TestRunNeverExceedsWorkerCount(t *testing.T) {
	addr, _ := startTCPServer(t, holdOpenHandler)
	host, port := hostPort(t, addr)

	const workers = 3
	const numTargets = 9
	const workDuration = 80 * time.Millisecond

	var active, maxActive atomic.Int32
	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		n := active.Add(1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(workDuration)
		active.Add(-1)
		return fpmodule.OK("done")
	}}

	rec := &recordingOutput{}
	sink := output.NewSink([]output.Output{rec}, 0)
	e := New(Config{Module: mod, ModuleName: "test", Timeout: time.Second, Workers: workers}, sink)

	targets := make(chan target.Target, numTargets)
	for i := 0; i < numTargets; i++ {
		targets <- target.Target{Host: host, Port: port}
	}
	close(targets)

	start := time.Now()
	e.Run(context.Background(), targets)
	elapsed := time.Since(start)
	sink.Close()

	if got := maxActive.Load(); got > workers {
		t.Fatalf("observed %d concurrent probes, want <= %d (worker pool cap violated)", got, workers)
	}
	if len(rec.writes) != numTargets {
		t.Fatalf("got %d records, want %d", len(rec.writes), numTargets)
	}
	// Fully serial would take numTargets*workDuration; with a working
	// pool of `workers` goroutines it should take roughly
	// (numTargets/workers)*workDuration. Assert it's well under serial.
	serial := numTargets * workDuration
	if elapsed >= serial {
		t.Fatalf("elapsed %v was not faster than serial %v; workers do not appear to run concurrently", elapsed, serial)
	}
}

func TestRunStopsPromptlyOnContextCancellation(t *testing.T) {
	addr, _ := startTCPServer(t, holdOpenHandler)
	host, port := hostPort(t, addr)

	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		if in.State == nil {
			return fpmodule.Continue(1, []byte("PING"), "waiting")
		}
		return fpmodule.ErrUp("should not get here in this test")
	}}

	rec := &recordingOutput{}
	sink := output.NewSink([]output.Output{rec}, 0)
	// A long per-round timeout, so only cancellation (not a natural
	// timeout) can make this return quickly.
	e := New(Config{Module: mod, ModuleName: "test", Timeout: 30 * time.Second, Workers: 2}, sink)

	targets := make(chan target.Target)
	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan struct{})
	go func() {
		e.Run(ctx, targets)
		close(runDone)
	}()

	targets <- target.Target{Host: host, Port: port}
	time.Sleep(50 * time.Millisecond) // let the worker dial and block on the read
	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after context cancellation")
	}
	sink.Close()
}
