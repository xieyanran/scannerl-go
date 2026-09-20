// Package engine drives targets through a fpmodule.Module using a fixed
// worker-goroutine pool, replacing the original's Erlang broker/FSM pair.
package engine

import (
	"context"
	"sync"

	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// Engine runs one scan: cfg.Module against every Target from Run's input
// channel, sending each finished Record to sink.
type Engine struct {
	cfg  Config
	sink *output.Sink
}

// New builds an Engine. cfg.Workers <= 0 falls back to DefaultWorkers.
func New(cfg Config, sink *output.Sink) *Engine {
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	return &Engine{cfg: cfg, sink: sink}
}

// Run starts cfg.Workers goroutines pulling from targets, probing each
// one and sending its Record to the sink, until targets is closed or ctx
// is done. It blocks until every worker has stopped.
//
// A fixed pool (rather than one goroutine per target gated by a
// semaphore) is the direct analogue of the original broker's add_childs,
// which hard-caps concurrently-active children and requeues excess
// targets rather than gating goroutine birth -- for target counts
// reaching into the millions (a /8 CIDR range), this keeps peak
// goroutine/fd count deterministic.
func (e *Engine) Run(ctx context.Context, targets <-chan target.Target) {
	var wg sync.WaitGroup
	wg.Add(e.cfg.Workers)
	for i := 0; i < e.cfg.Workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case t, ok := <-targets:
					if !ok {
						return
					}
					e.sink.Send(ctx, e.probeOne(ctx, t))
				}
			}
		}()
	}
	wg.Wait()
}
