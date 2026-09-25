package coordinator

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// recordingOutput is a minimal output.Output test double, duplicated
// locally per this repo's existing per-package test convention (see
// internal/output/sink_test.go) -- with a mutex, since here it's fed
// concurrently by more than one ReportResults stream.
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

func (r *recordingOutput) snapshot() []output.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]output.Record, len(r.writes))
	copy(out, r.writes)
	return out
}

// startCoordinator serves cfg on a real 127.0.0.1:0 listener and returns
// its address. The server is stopped automatically at test cleanup.
func startCoordinator(t *testing.T, cfg Config) (addr string, coord *Coordinator) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	coord = New(context.Background(), cfg)
	go coord.Serve(ln)
	t.Cleanup(coord.Stop)
	return ln.Addr().String(), coord
}

func dial(t *testing.T, addr string) scannerpb.ScannerClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return scannerpb.NewScannerClient(conn)
}

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for goroutines to finish")
	}
}

func TestGetTargetsBlocksUntilAllWorkersArrive(t *testing.T) {
	cfg := Config{
		Workers: 3,
		Source: target.SourceConfig{Targets: []string{
			"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80", "10.0.0.4:80", "10.0.0.5:80", "10.0.0.6:80",
		}},
	}
	addr, _ := startCoordinator(t, cfg)

	done := make(chan struct{}, 3)
	callAndDrain := func() {
		stream, err := dial(t, addr).GetTargets(context.Background(), &scannerpb.WorkerHello{WorkerId: "w"})
		if err != nil {
			t.Errorf("GetTargets: %v", err)
			return
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("Recv: %v", err)
				return
			}
		}
		done <- struct{}{}
	}

	go callAndDrain()
	go callAndDrain()

	select {
	case <-done:
		t.Fatal("a GetTargets call returned before all 3 workers connected")
	case <-time.After(200 * time.Millisecond):
	}

	go callAndDrain()

	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for GetTargets call %d to finish after the 3rd worker connected", i)
		}
	}
}

func TestShardsPartitionFullTargetListExactly(t *testing.T) {
	entries := []string{
		"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80", "10.0.0.4:80",
		"10.0.0.5:80", "10.0.0.6:80", "10.0.0.7:80",
	}
	cfg := Config{Workers: 3, Source: target.SourceConfig{Targets: entries}}
	addr, _ := startCoordinator(t, cfg)

	var mu sync.Mutex
	var got []string
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := dial(t, addr).GetTargets(context.Background(), &scannerpb.WorkerHello{WorkerId: "w"})
			if err != nil {
				t.Errorf("GetTargets: %v", err)
				return
			}
			for {
				pt, err := stream.Recv()
				if err == io.EOF {
					return
				}
				if err != nil {
					t.Errorf("Recv: %v", err)
					return
				}
				mu.Lock()
				got = append(got, fmt.Sprintf("%s:%d", pt.GetHost(), pt.GetPort()))
				mu.Unlock()
			}
		}()
	}
	waitOrTimeout(t, &wg, 2*time.Second)

	sort.Strings(got)
	want := append([]string(nil), entries...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("union of shards = %v, want %v (no duplicates/drops)", got, want)
	}
}

func TestExtraWorkerRejected(t *testing.T) {
	cfg := Config{Workers: 1, Source: target.SourceConfig{Targets: []string{"10.0.0.1:80"}}}
	addr, _ := startCoordinator(t, cfg)

	// The 1st connection completes the rendezvous immediately (Workers=1).
	stream, err := dial(t, addr).GetTargets(context.Background(), &scannerpb.WorkerHello{WorkerId: "first"})
	if err != nil {
		t.Fatalf("GetTargets: %v", err)
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
	}

	// A 2nd connection past Config.Workers should be rejected outright.
	stream2, err := dial(t, addr).GetTargets(context.Background(), &scannerpb.WorkerHello{WorkerId: "second"})
	if err != nil {
		t.Fatalf("GetTargets: %v", err)
	}
	_, recvErr := stream2.Recv()
	st, ok := status.FromError(recvErr)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("2nd worker's error = %v, want codes.ResourceExhausted", recvErr)
	}
}

func TestReportResultsFeedsSharedSink(t *testing.T) {
	rec := &recordingOutput{}
	sink := output.NewSink([]output.Output{rec}, 0)
	cfg := Config{Workers: 1, Source: target.SourceConfig{Targets: []string{"10.0.0.1:80"}}, Sink: sink}
	addr, _ := startCoordinator(t, cfg)

	send := func(n int, module string) {
		stream, err := dial(t, addr).ReportResults(context.Background())
		if err != nil {
			t.Errorf("ReportResults: %v", err)
			return
		}
		for i := 0; i < n; i++ {
			pr := &scannerpb.Result{
				Module: module, Target: fmt.Sprintf("10.0.0.%d", i), Port: 80,
				Outcome: scannerpb.Outcome_OUTCOME_OK, ValueJson: `"ok"`,
			}
			if err := stream.Send(pr); err != nil {
				t.Errorf("Send: %v", err)
				return
			}
		}
		ack, err := stream.CloseAndRecv()
		if err != nil {
			t.Errorf("CloseAndRecv: %v", err)
			return
		}
		if ack.GetReceived() != int64(n) {
			t.Errorf("ack.Received = %d, want %d", ack.GetReceived(), n)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); send(5, "workerA") }()
	go func() { defer wg.Done(); send(7, "workerB") }()
	waitOrTimeout(t, &wg, 2*time.Second)

	if errs := sink.Close(); len(errs) != 0 {
		t.Fatalf("sink.Close: %v", errs)
	}

	writes := rec.snapshot()
	if len(writes) != 12 {
		t.Fatalf("got %d writes, want 12", len(writes))
	}
	var countA, countB int
	for _, w := range writes {
		switch w.Module {
		case "workerA":
			countA++
		case "workerB":
			countB++
		}
	}
	if countA != 5 || countB != 7 {
		t.Errorf("countA=%d countB=%d, want 5 and 7 (writes from concurrent streams both landed)", countA, countB)
	}
}
