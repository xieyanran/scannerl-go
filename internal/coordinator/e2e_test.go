package coordinator

import (
	"context"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xieyanran/scannerl-go/internal/engine"
	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	_ "github.com/xieyanran/scannerl-go/internal/fpmodule/modules" // registers "tcpbanner"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
	"github.com/xieyanran/scannerl-go/internal/worker"
)

// startBannerServer runs a real TCP server that writes reply to every
// accepted connection then closes it, matching what the "tcpbanner"
// module expects (a service that speaks first).
func startBannerServer(t *testing.T, reply string) (addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				conn.Write([]byte(reply))
			}()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// TestDistributedScanEndToEnd wires one real Coordinator and two real
// worker.Clients around the unmodified M1 engine/sink against real TCP
// servers, exercising the full round trip issues #8-#11 designed:
// shard push -> per-worker engine.Run -> streamed results -> merged
// output.Sink. This is the test that validates those pieces actually
// compose, not just each in isolation.
func TestDistributedScanEndToEnd(t *testing.T) {
	addr1 := startBannerServer(t, "banner-one")
	addr2 := startBannerServer(t, "banner-two")

	mod, ok := fpmodule.Get("tcpbanner")
	if !ok {
		t.Fatal(`fpmodule.Get("tcpbanner") not found; is internal/fpmodule/modules imported?`)
	}
	modCfg := mod.DefaultConfig()

	rec := &recordingOutput{}
	sink := output.NewSink([]output.Output{rec}, 0)
	coordCfg := Config{
		Workers: 2,
		Source: target.SourceConfig{
			Targets:     []string{addr1, addr2},
			DefaultPort: modCfg.Port,
		},
		Sink: sink,
	}
	coordAddr, _ := startCoordinator(t, coordCfg)

	runOneWorker := func(t *testing.T, workerID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		wc, err := worker.Dial(coordAddr)
		if err != nil {
			t.Errorf("worker.Dial: %v", err)
			return
		}
		defer wc.Close()

		targets, errs, err := wc.GetTargets(ctx, workerID)
		if err != nil {
			t.Errorf("GetTargets: %v", err)
			return
		}
		var errWg sync.WaitGroup
		errWg.Add(1)
		go func() {
			defer errWg.Done()
			for err := range errs {
				t.Errorf("worker %s: %v", workerID, err)
			}
		}()

		out := wc.NewOutput(ctx)
		if err := out.Init(output.ScanInfo{}, nil); err != nil {
			t.Errorf("Init: %v", err)
			return
		}
		workerSink := output.NewSink([]output.Output{out}, 0)

		eng := engine.New(engine.Config{
			Module:     mod,
			ModuleName: "tcpbanner",
			Transport:  modCfg.Transport,
			Timeout:    modCfg.Timeout,
			MaxPkt:     modCfg.MaxPkt,
			Workers:    4,
		}, workerSink)
		eng.Run(ctx, targets)
		errWg.Wait()

		if errs := workerSink.Close(); len(errs) != 0 {
			t.Errorf("worker sink close: %v", errs)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runOneWorker(t, "w1") }()
	go func() { defer wg.Done(); runOneWorker(t, "w2") }()
	waitOrTimeout(t, &wg, 5*time.Second)

	if errs := sink.Close(); len(errs) != 0 {
		t.Fatalf("coordinator sink close: %v", errs)
	}

	writes := rec.snapshot()
	if len(writes) != 2 {
		t.Fatalf("coordinator received %d records, want 2 (writes: %+v)", len(writes), writes)
	}
	var banners []string
	for _, w := range writes {
		if w.Result.Outcome != fpmodule.OKOutcome {
			t.Errorf("record for %s: outcome = %v, want OKOutcome (value=%v)", w.Target, w.Result.Outcome, w.Result.Value)
			continue
		}
		s, ok := w.Result.Value.(string)
		if !ok {
			t.Errorf("record for %s: value = %#v, want a string", w.Target, w.Result.Value)
			continue
		}
		banners = append(banners, s)
	}

	sort.Strings(banners)
	want := []string{"banner-one", "banner-two"}
	sort.Strings(want)
	if !reflect.DeepEqual(banners, want) {
		t.Errorf("banners received = %v, want %v", banners, want)
	}
}
