package output

import (
	"context"
	"testing"
	"time"
)

type recordingOutput struct {
	writes  []Record
	cleaned bool
}

func (r *recordingOutput) Init(ScanInfo, []string) error { return nil }
func (r *recordingOutput) Write(rec Record) error        { r.writes = append(r.writes, rec); return nil }
func (r *recordingOutput) Clean() error                  { r.cleaned = true; return nil }
func (r *recordingOutput) Description() string           { return "recording" }
func (r *recordingOutput) Arguments() []string           { return nil }

func TestSinkFanOutAndClose(t *testing.T) {
	a := &recordingOutput{}
	b := &recordingOutput{}
	s := NewSink([]Output{a, b}, 10)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s.Send(ctx, Record{Module: "m", Target: "t", Port: i})
	}
	if errs := s.Close(); len(errs) != 0 {
		t.Fatalf("Close() returned errors: %v", errs)
	}

	for _, out := range []*recordingOutput{a, b} {
		if len(out.writes) != 5 {
			t.Fatalf("got %d writes, want 5", len(out.writes))
		}
		for i, rec := range out.writes {
			if rec.Port != i {
				t.Fatalf("writes out of order: writes[%d].Port = %d, want %d", i, rec.Port, i)
			}
		}
		if !out.cleaned {
			t.Fatalf("Close() did not call Clean on output")
		}
	}
}

// blockingOutput blocks its first Write until release is closed, so tests
// can force the sink's queue to fill up and observe backpressure.
type blockingOutput struct {
	release chan struct{}
	writes  []Record
	first   bool
}

func (b *blockingOutput) Init(ScanInfo, []string) error { return nil }
func (b *blockingOutput) Write(rec Record) error {
	if !b.first {
		b.first = true
		<-b.release
	}
	b.writes = append(b.writes, rec)
	return nil
}
func (b *blockingOutput) Clean() error        { return nil }
func (b *blockingOutput) Description() string { return "blocking" }
func (b *blockingOutput) Arguments() []string { return nil }

func TestSinkSendRespectsContextWhenQueueFull(t *testing.T) {
	out := &blockingOutput{release: make(chan struct{})}
	s := NewSink([]Output{out}, 1)

	// First send is picked up immediately by the sink goroutine and
	// blocks inside Write; second send fills the capacity-1 buffer.
	bg := context.Background()
	s.Send(bg, Record{Port: 1})
	s.Send(bg, Record{Port: 2})

	// The queue is now full and the sink goroutine is stuck in Write, so
	// a third Send must not be able to complete until we release it.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.Send(ctx, Record{Port: 3})
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("Send returned after %v, expected it to block until context deadline", elapsed)
	}

	close(out.release)
	if errs := s.Close(); len(errs) != 0 {
		t.Fatalf("Close() returned errors: %v", errs)
	}
	if len(out.writes) != 2 {
		t.Fatalf("got %d writes, want 2 (the third Send should have been dropped by context cancellation)", len(out.writes))
	}
}
