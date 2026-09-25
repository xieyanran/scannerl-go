// Package coordinator implements the Coordinator side of scannerl's
// distributed mode: it waits for a fixed number of Workers to connect,
// pushes each one a static shard of the full target list, and merges
// every Worker's streamed-back results into one unified output.Sink.
package coordinator

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// Config configures a Coordinator.
type Config struct {
	// Workers is the number of GetTargets callers to wait for before
	// computing and pushing shards. Must be > 0.
	Workers int
	// Source describes the full target list to shard, in the same shape
	// cmd/scannerl builds for a single-host run.
	Source target.SourceConfig
	// Sink receives every Result reported back by every Worker, merged
	// into one unified output stream -- the same *output.Sink a
	// single-host run uses, unmodified.
	Sink *output.Sink
	// RegisterTimeout bounds how long GetTargets will wait, from
	// Coordinator startup, for all Workers to connect. If fewer have
	// arrived once it elapses, the Coordinator aborts: every
	// already-connected Worker's GetTargets call fails, and Serve
	// returns an error. Zero disables the timeout (wait forever,
	// matching the original design).
	RegisterTimeout time.Duration
}

// Coordinator implements the Scanner gRPC service's server side: it waits
// for exactly Config.Workers GetTargets callers, then computes a static,
// one-time shard of Config.Source's target list for each and streams it
// down; it also receives every ReportResults stream and feeds every
// Result into Config.Sink.
type Coordinator struct {
	scannerpb.UnimplementedScannerServer

	ctx context.Context
	cfg Config
	srv *grpc.Server

	mu      sync.Mutex
	arrived int
	shards  [][]target.Target
	// ready is closed exactly once, by whichever register() call is the
	// Config.Workers-th -- Go's idiomatic one-shot broadcast. Closing
	// (rather than e.g. sync.Cond) lets a waiting GetTargets call also
	// select on stream.Context().Done(), so it unblocks promptly if its
	// own connection drops while still waiting on the others.
	ready chan struct{}
	// aborted is closed exactly once if Config.RegisterTimeout elapses
	// before every Worker arrived -- a second, distinct one-shot signal
	// from ready, so a blocked GetTargets call can tell "the rendezvous
	// completed" from "the Coordinator gave up waiting" and return a
	// clear error for the latter instead of just hanging forever.
	aborted  chan struct{}
	abortErr error

	pendingMu sync.Mutex
	// pending tracks every target pushed to a Worker that hasn't yet had
	// a Result reported back for it, keyed by "host:port". Initialized
	// (to the full target list) when shards are computed, and shrunk as
	// ReportResults receives matching Results. See ReportUnresolved.
	pending map[string]target.Target
}

// New builds a Coordinator. ctx is used to drain cfg.Source (via
// target.Stream) when computing shards, and to feed cfg.Sink.
func New(ctx context.Context, cfg Config) *Coordinator {
	c := &Coordinator{
		ctx:     ctx,
		cfg:     cfg,
		ready:   make(chan struct{}),
		aborted: make(chan struct{}),
		pending: make(map[string]target.Target),
	}
	c.srv = grpc.NewServer()
	scannerpb.RegisterScannerServer(c.srv, c)
	return c
}

// Serve blocks, accepting connections on ln until Stop is called, or
// until Config.RegisterTimeout elapses with fewer than Config.Workers
// connected -- in which case Serve itself stops the server and returns
// the error explaining why.
func (c *Coordinator) Serve(ln net.Listener) error {
	if c.cfg.RegisterTimeout > 0 {
		timer := time.AfterFunc(c.cfg.RegisterTimeout, c.onRegisterTimeout)
		defer timer.Stop()
	}

	err := c.srv.Serve(ln)

	c.mu.Lock()
	abortErr := c.abortErr
	c.mu.Unlock()
	if abortErr != nil {
		return abortErr
	}
	return err
}

// onRegisterTimeout fires once, Config.RegisterTimeout after Serve
// started, if fewer than Config.Workers had connected by then. It
// unblocks every already-connected GetTargets call with a clear error
// (via aborted) and stops the server so Serve returns that same error to
// the caller, instead of listening forever for a scan that will now
// never start.
func (c *Coordinator) onRegisterTimeout() {
	c.mu.Lock()
	if c.arrived >= c.cfg.Workers {
		c.mu.Unlock()
		return // the rendezvous already completed; nothing to abort
	}
	c.abortErr = fmt.Errorf("coordinator: only %d/%d workers connected within %s, aborting",
		c.arrived, c.cfg.Workers, c.cfg.RegisterTimeout)
	close(c.aborted)
	c.mu.Unlock()

	c.srv.Stop()
}

// Stop immediately terminates the server and any in-flight RPCs,
// including any GetTargets call still blocked waiting for the remaining
// workers to connect -- unlike GracefulStop, which would hang forever in
// that case.
func (c *Coordinator) Stop() {
	c.srv.Stop()
}

// GetTargets implements the Scanner service: it blocks until exactly
// Config.Workers callers (including this one) have registered, then
// streams this call's shard down and closes.
//
// Known limitation, matching the "static push, no rebalancing" design:
// if a worker disconnects while fewer than Config.Workers have arrived,
// the rendezvous never completes on its own -- Config.RegisterTimeout is
// the only way to bound how long the others wait for it.
func (c *Coordinator) GetTargets(req *scannerpb.WorkerHello, stream scannerpb.Scanner_GetTargetsServer) error {
	idx, err := c.register()
	if err != nil {
		return err
	}

	select {
	case <-c.ready:
	case <-c.aborted:
		return status.Errorf(codes.DeadlineExceeded,
			"coordinator: rendezvous timed out waiting for all %d workers", c.cfg.Workers)
	case <-stream.Context().Done():
		return stream.Context().Err()
	}

	for _, t := range c.shards[idx] {
		if err := stream.Send(scannerpb.ToProtoTarget(t)); err != nil {
			return err
		}
	}
	return nil
}

// register assigns idx the next arrival slot. Once the Config.Workers-th
// caller registers, it computes every shard synchronously (while still
// holding mu) and closes ready. No separate "compute once" guard (e.g.
// sync.Once) is needed on top of the mutex: by construction, only the one
// call that flips arrived to Config.Workers can ever reach that branch,
// since every registration after it is rejected below.
func (c *Coordinator) register() (idx int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.arrived >= c.cfg.Workers {
		return 0, status.Errorf(codes.ResourceExhausted,
			"coordinator: already have %d workers, rejecting extra connection", c.cfg.Workers)
	}
	idx = c.arrived
	c.arrived++
	if c.arrived == c.cfg.Workers {
		c.shards = c.computeShards()
		close(c.ready)
	}
	return idx, nil
}

func (c *Coordinator) computeShards() [][]target.Target {
	ts := drainTargets(c.ctx, c.cfg.Source)
	rand.Shuffle(len(ts), func(i, j int) { ts[i], ts[j] = ts[j], ts[i] })

	c.pendingMu.Lock()
	for _, t := range ts {
		c.pending[pendingKey(t.Host, t.Port)] = t
	}
	c.pendingMu.Unlock()

	return splitEven(ts, c.cfg.Workers)
}

func pendingKey(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// drainTargets fully resolves cfg into a slice, draining both of
// target.Stream's channels concurrently per its documented contract.
// Source errors (a bad target file, an unparseable entry, ...) are
// logged to stderr and otherwise ignored, matching cmd/scannerl's
// single-host path, which proceeds with whatever targets did parse
// rather than aborting the whole run over a partial source error.
func drainTargets(ctx context.Context, cfg target.SourceConfig) []target.Target {
	out, errs := target.Stream(ctx, cfg)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errs {
			fmt.Fprintln(os.Stderr, "coordinator:", err)
		}
	}()

	var ts []target.Target
	for t := range out {
		ts = append(ts, t)
	}
	wg.Wait()
	return ts
}

// ReportResults implements the Scanner service: it reads every Result the
// worker sends until the worker closes its stream, feeding each into
// Config.Sink -- the same *output.Sink a single-host run uses, unmodified.
// Multiple workers' ReportResults calls run concurrently (grpc-go
// dispatches each RPC on its own goroutine); Sink.Send is already safe
// for concurrent callers (it's just a channel send), so no extra locking
// is needed here.
func (c *Coordinator) ReportResults(stream scannerpb.Scanner_ReportResultsServer) error {
	var n int64
	for {
		pr, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&scannerpb.ReportAck{Received: n})
		}
		if err != nil {
			// The worker's connection dropped mid-scan (crash, network
			// partition, EC2 termination, ...). There is no rebalancing
			// (matching #9's static, one-time push design): whatever
			// this worker's shard still hadn't reported stays in
			// c.pending and is surfaced later by ReportUnresolved,
			// rather than silently disappearing. This log line is just
			// the immediate, at-the-time signal; the count it reports is
			// coordinator-wide (some of it may belong to other workers
			// still legitimately in progress), since a dropped
			// ReportResults stream has no way to identify which shard it
			// was sending on.
			c.pendingMu.Lock()
			remaining := len(c.pending)
			c.pendingMu.Unlock()
			fmt.Fprintf(os.Stderr, "coordinator: a worker's result stream ended unexpectedly (%v); %d target(s) coordinator-wide have not yet reported a result\n", err, remaining)
			return err
		}
		rec, err := scannerpb.FromProtoResult(pr)
		if err != nil {
			// A single malformed Result shouldn't abort this worker's
			// whole stream; log it and keep reading.
			fmt.Fprintln(os.Stderr, "coordinator:", err)
			continue
		}
		c.pendingMu.Lock()
		delete(c.pending, pendingKey(rec.Target, rec.Port))
		c.pendingMu.Unlock()
		c.cfg.Sink.Send(c.ctx, rec)
		n++
	}
}

// ReportUnresolved sends a synthetic ErrUnknown Record into Config.Sink
// for every target that was pushed to a Worker but never got a real
// Result back -- e.g. because that Worker crashed or lost its connection
// mid-scan (see #17), or the Coordinator was shut down before every
// Worker finished. This makes that data loss visible in the actual final
// output, rather than those targets silently disappearing. Call this once
// Serve has returned (i.e. the Coordinator is no longer accepting new
// results), before closing Config.Sink.
func (c *Coordinator) ReportUnresolved() {
	c.pendingMu.Lock()
	pending := make([]target.Target, 0, len(c.pending))
	for _, t := range c.pending {
		pending = append(pending, t)
	}
	c.pendingMu.Unlock()

	for _, t := range pending {
		c.cfg.Sink.Send(c.ctx, output.Record{
			Target: t.Host,
			Port:   t.Port,
			Result: fpmodule.Result{
				Outcome: fpmodule.ErrUnknownOutcome,
				Value:   "worker disconnected or the coordinator shut down before a result was reported for this target",
			},
		})
	}
}
