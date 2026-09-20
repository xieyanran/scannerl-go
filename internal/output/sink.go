package output

import "context"

// defaultQueueCapacity backs "-Q 0" (unset). The original defaults -Q to
// infinity; a literal unbounded queue would let a slow Output grow memory
// without bound for a large scan, so this picks a large fixed capacity
// instead — big enough that no realistic scan outpaces its Outputs before
// draining it, without that failure mode. -Q N picks an explicit bound.
const defaultQueueCapacity = 100_000

// Sink fans finished Records out to every configured Output. All Outputs
// are driven from one internal goroutine, so a run with multiple -o
// targets sees records in the same order on each, and (matching the
// original's utils:output_send list iteration) a slow Output head-of-
// line-blocks the others rather than racing them.
type Sink struct {
	outs []Output
	ch   chan Record
	done chan struct{}
}

// NewSink starts a Sink writing to outs. queueMax > 0 bounds the number
// of unprocessed records buffered before Send blocks, backpressuring
// callers; queueMax <= 0 uses defaultQueueCapacity.
func NewSink(outs []Output, queueMax int) *Sink {
	capacity := queueMax
	if capacity <= 0 {
		capacity = defaultQueueCapacity
	}
	s := &Sink{
		outs: outs,
		ch:   make(chan Record, capacity),
		done: make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *Sink) run() {
	defer close(s.done)
	for rec := range s.ch {
		for _, o := range s.outs {
			// An Output's own error handling/logging is its
			// responsibility; the sink can't do more than best-effort
			// delivery to the remaining outputs.
			_ = o.Write(rec)
		}
	}
}

// Send enqueues rec, blocking if the queue is full until ctx is done.
func (s *Sink) Send(ctx context.Context, rec Record) {
	select {
	case s.ch <- rec:
	case <-ctx.Done():
	}
}

// Close stops accepting records, waits for the queue to drain, and calls
// Clean on every configured Output, collecting any errors.
func (s *Sink) Close() []error {
	close(s.ch)
	<-s.done
	var errs []error
	for _, o := range s.outs {
		if err := o.Clean(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
