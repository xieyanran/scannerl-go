package engine

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// closedPort reserves a port and immediately closes it, so a dial
// against it fails with ECONNREFUSED -- reusing the same technique as
// TestProbeOneConnectionRefused in probe_test.go.
func closedPort(t *testing.T) (host string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return hostPort(t, addr)
}

func TestDialWithRetrySucceedsAfterTransientFailure(t *testing.T) {
	host, port := closedPort(t)
	addr := net.JoinHostPort(host, fmt.Sprint(port))

	// The port is refused right now; start listening on that same
	// address again after a short delay, simulating a target that's
	// briefly unavailable and then recovers -- exactly the case
	// ConnectRetries exists for.
	accepted := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		ln2, err := net.Listen("tcp", addr)
		if err != nil {
			return // the address was reused by something else; the test will time out and fail below
		}
		defer ln2.Close()
		conn, err := ln2.Accept()
		if err != nil {
			return
		}
		conn.Close()
		close(accepted)
	}()

	e := newTestEngine(Config{
		ModuleName:     "test",
		ConnectRetries: 3,
		ConnectBackoff: 100 * time.Millisecond,
	})

	conn, _, err := e.dialWithRetry(context.Background(), host, port)
	if err != nil {
		t.Fatalf("dialWithRetry: %v, want success once the listener came back up", err)
	}
	conn.Close()

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted a connection")
	}
}

func TestDialWithRetryGivesUpAfterExhaustingRetries(t *testing.T) {
	host, port := closedPort(t)

	e := newTestEngine(Config{
		ModuleName:     "test",
		ConnectRetries: 2,
		ConnectBackoff: 50 * time.Millisecond,
	})

	start := time.Now()
	_, _, err := e.dialWithRetry(context.Background(), host, port)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("dialWithRetry succeeded against a permanently closed port, want an error")
	}
	// 1 initial attempt + 2 retries, backoff doubling from 50ms: waits of
	// ~50ms then ~100ms between attempts.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("elapsed %v is too short for 2 backoff waits (50ms + 100ms); were both retries attempted?", elapsed)
	}
}

func TestDialWithRetryStopsPromptlyOnContextCancellation(t *testing.T) {
	host, port := closedPort(t)

	e := newTestEngine(Config{
		ModuleName:     "test",
		ConnectRetries: 10,
		ConnectBackoff: 5 * time.Second, // long enough that only cancellation can return this quickly
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, _, err := e.dialWithRetry(ctx, host, port)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("dialWithRetry succeeded, want an error from context cancellation")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("dialWithRetry took %v to return after cancellation, want it to stop promptly", elapsed)
	}
}
