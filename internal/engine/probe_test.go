package engine

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// scriptModule is a stateless fpmodule.Module test double: its behavior
// is entirely a function of Input (mainly Input.State), never receiver
// fields, so it's safe to share across concurrent probes exactly like a
// real Module must be.
type scriptModule struct {
	fn func(in fpmodule.Input) fpmodule.Step
}

func (m scriptModule) DefaultConfig() fpmodule.Config {
	return fpmodule.Config{Transport: fpmodule.TCP}
}
func (m scriptModule) Description() string                  { return "script" }
func (m scriptModule) Arguments() []string                  { return nil }
func (m scriptModule) Next(in fpmodule.Input) fpmodule.Step { return m.fn(in) }

// startTCPServer runs handle for every accepted connection until closeFn
// is called, which also waits for every handler to return. handle
// receives a done channel it should select on to exit promptly if the
// test wants to hold a connection open rather than closing it itself.
func startTCPServer(t *testing.T, handle func(conn net.Conn, done <-chan struct{})) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				handle(conn, done)
			}()
		}
	}()
	closeFn = func() {
		close(done)
		ln.Close()
		wg.Wait()
	}
	t.Cleanup(closeFn)
	return ln.Addr().String(), closeFn
}

// replyOnceHandler reads whatever the client sends first, writes reply,
// then holds the connection open until done fires.
func replyOnceHandler(reply []byte) func(net.Conn, <-chan struct{}) {
	return func(conn net.Conn, done <-chan struct{}) {
		buf := make([]byte, 4096)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Read(buf); err != nil {
			return
		}
		conn.Write(reply)
		<-done
	}
}

// countingNeverReplyHandler counts every non-empty read (each resend
// shows up as a separate read on the same connection) and never replies.
func countingNeverReplyHandler(count *atomic.Int32) func(net.Conn, <-chan struct{}) {
	return func(conn net.Conn, done <-chan struct{}) {
		buf := make([]byte, 4096)
		for {
			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, err := conn.Read(buf)
			if n > 0 {
				count.Add(1)
			}
			if err != nil {
				select {
				case <-done:
					return
				default:
				}
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
		}
	}
}

// proactiveReplyHandler writes reply as soon as the connection is
// accepted, without waiting to read anything from the client first --
// for provoking a module's Continue(0, ...) "expect nothing back" case.
func proactiveReplyHandler(reply []byte) func(net.Conn, <-chan struct{}) {
	return func(conn net.Conn, done <-chan struct{}) {
		conn.Write(reply)
		<-done
	}
}

// holdOpenHandler accepts and does nothing until done fires.
func holdOpenHandler(conn net.Conn, done <-chan struct{}) {
	<-done
}

func newTestEngine(cfg Config) *Engine {
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second
	}
	if cfg.Transport == 0 {
		cfg.Transport = fpmodule.TCP
	}
	return New(cfg, nil)
}

func hostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parsing port %q: %v", portStr, err)
	}
	return host, port
}

func TestProbeOneBasicRequestResponse(t *testing.T) {
	addr, _ := startTCPServer(t, replyOnceHandler([]byte("PONG")))
	host, port := hostPort(t, addr)

	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		if in.State == nil {
			return fpmodule.Continue(1, []byte("PING"), "waited")
		}
		if in.PacketRcv < 1 {
			return fpmodule.ErrUp("timeout")
		}
		return fpmodule.OK(string(in.Data))
	}}

	e := newTestEngine(Config{Module: mod, ModuleName: "test"})
	rec := e.probeOne(context.Background(), target.Target{Host: host, Port: port})

	if rec.Result.Outcome != fpmodule.OKOutcome || rec.Result.Value != "PONG" {
		t.Fatalf("got %+v, want OK(\"PONG\")", rec)
	}
	if rec.Target != host || rec.Port != port {
		t.Fatalf("record target/port = %s:%d, want %s:%d", rec.Target, rec.Port, host, port)
	}
}

func TestProbeOneRetriesOnEmptyRoundThenGivesUp(t *testing.T) {
	var count atomic.Int32
	addr, _ := startTCPServer(t, countingNeverReplyHandler(&count))
	host, port := hostPort(t, addr)

	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		if in.State == nil {
			return fpmodule.Continue(1, []byte("PING"), "waited")
		}
		if in.PacketRcv < 1 {
			return fpmodule.ErrUp("timeout")
		}
		return fpmodule.OK("unexpected data")
	}}

	e := newTestEngine(Config{Module: mod, ModuleName: "test", Timeout: 50 * time.Millisecond, Retry: 2})
	start := time.Now()
	rec := e.probeOne(context.Background(), target.Target{Host: host, Port: port})
	elapsed := time.Since(start)

	if rec.Result.Outcome != fpmodule.ErrUpOutcome || rec.Result.Value != "timeout" {
		t.Fatalf("got %+v, want ErrUp(\"timeout\")", rec)
	}
	if got := count.Load(); got != 3 { // 1 initial send + 2 retries
		t.Fatalf("server received %d payloads, want 3 (1 initial + retry=2)", got)
	}
	if elapsed < 2*50*time.Millisecond {
		t.Fatalf("elapsed %v is too short for 2 retries at 50ms timeout each", elapsed)
	}
}

func TestProbeOneRestartPreservesOriginalTargetInRecord(t *testing.T) {
	addr1, _ := startTCPServer(t, replyOnceHandler([]byte("MOVED")))
	addr2, _ := startTCPServer(t, replyOnceHandler([]byte("OK")))
	host1, port1 := hostPort(t, addr1)
	host2, port2 := hostPort(t, addr2)

	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		switch s := in.State; {
		case s == nil:
			return fpmodule.Continue(1, []byte("HELLO"), "first")
		case s == "first":
			if string(in.Data) != "MOVED" {
				return fpmodule.ErrUp("unexpected first reply")
			}
			return fpmodule.Restart(host2, port2, "second")
		case s == "second":
			return fpmodule.Continue(1, []byte("HELLO2"), "waited2")
		default: // "waited2"
			return fpmodule.OK(string(in.Data))
		}
	}}

	e := newTestEngine(Config{Module: mod, ModuleName: "test"})
	// The ORIGINAL target points at server 1; the module redirects to
	// server 2 mid-probe via Restart.
	rec := e.probeOne(context.Background(), target.Target{Host: host1, Port: port1})

	if rec.Result.Outcome != fpmodule.OKOutcome || rec.Result.Value != "OK" {
		t.Fatalf("got %+v, want OK(\"OK\") (from server 2)", rec)
	}
	if rec.Target != host1 || rec.Port != port1 {
		t.Fatalf("record target/port = %s:%d, want the ORIGINAL %s:%d (server 1), not server 2 (%s:%d)",
			rec.Target, rec.Port, host1, port1, host2, port2)
	}
}

func TestProbeOneTooManyPacketsIsHardStop(t *testing.T) {
	addr, _ := startTCPServer(t, proactiveReplyHandler([]byte("unexpected")))
	host, port := hostPort(t, addr)

	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		if in.State == nil {
			return fpmodule.Continue(0, nil, "waitnone") // expect NO packets back
		}
		t.Fatalf("Next called a second time; the engine should hard-stop on unexpected data instead")
		return fpmodule.ErrUnknown("unreachable")
	}}

	e := newTestEngine(Config{Module: mod, ModuleName: "test", Timeout: 500 * time.Millisecond})
	rec := e.probeOne(context.Background(), target.Target{Host: host, Port: port})

	if rec.Result.Outcome != fpmodule.ErrUpOutcome {
		t.Fatalf("got %+v, want ErrUp outcome for toomanypacketreceived", rec)
	}
}

func TestProbeOneConnectionRefused(t *testing.T) {
	// Bind and immediately close to get a port nothing is listening on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	host, port := hostPort(t, addr)

	mod := scriptModule{fn: func(in fpmodule.Input) fpmodule.Step {
		t.Fatal("Next should not be called when the dial itself fails")
		return fpmodule.ErrUnknown("unreachable")
	}}

	e := newTestEngine(Config{Module: mod, ModuleName: "test", Timeout: time.Second})
	rec := e.probeOne(context.Background(), target.Target{Host: host, Port: port})

	if rec.Result.Outcome != fpmodule.ErrUpOutcome {
		t.Fatalf("got %+v, want ErrUp (connection refused means the target is up)", rec)
	}
}
