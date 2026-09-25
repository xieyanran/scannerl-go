package modules

import (
	"strings"
	"testing"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

func TestHTTPBannerFirstCallSendsGetRequest(t *testing.T) {
	step := httpBanner{}.Next(fpmodule.Input{State: nil, Target: "example.com"})

	if step.Kind != fpmodule.KindContinue {
		t.Fatalf("expected KindContinue, got %v", step.Kind)
	}
	if step.NumPackets != fpmodule.Unlimited {
		t.Fatalf("expected NumPackets = Unlimited, got %d", step.NumPackets)
	}
	if step.State == nil {
		t.Fatalf("expected non-nil State after first invocation, got nil")
	}

	payload := string(step.Payload)
	if !strings.HasPrefix(payload, "GET / HTTP/1.1\r\n") {
		t.Fatalf("payload = %q, want it to start with the GET request line", payload)
	}
	if !strings.Contains(payload, "Host: example.com\r\n") {
		t.Fatalf("payload = %q, want a Host header for the target", payload)
	}
	if !strings.HasSuffix(payload, "\r\n\r\n") {
		t.Fatalf("payload = %q, want it to end with the blank line terminating headers", payload)
	}
}

func TestHTTPBannerReturnsHeadersOnly(t *testing.T) {
	resp := "HTTP/1.1 200 OK\r\nServer: nginx\r\nContent-Length: 5\r\n\r\n<html>"
	step := httpBanner{}.Next(fpmodule.Input{
		PacketRcv: 1,
		Data:      []byte(resp),
		State:     "requested",
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}
	if step.Result.Outcome != fpmodule.OKOutcome {
		t.Fatalf("expected OKOutcome, got %v", step.Result.Outcome)
	}

	want := "HTTP/1.1 200 OK\r\nServer: nginx\r\nContent-Length: 5"
	if step.Result.Value != want {
		t.Fatalf("got %q, want headers only %q", step.Result.Value, want)
	}
}

func TestHTTPBannerFallsBackToWholeDataWithoutBlankLine(t *testing.T) {
	step := httpBanner{}.Next(fpmodule.Input{
		PacketRcv: 1,
		Data:      []byte("HTTP/1.1 200 OK\r\nServer: nginx"),
		State:     "requested",
	})

	if step.Kind != fpmodule.KindResult || step.Result.Outcome != fpmodule.OKOutcome {
		t.Fatalf("got %+v, want OK", step)
	}
	if step.Result.Value != "HTTP/1.1 200 OK\r\nServer: nginx" {
		t.Fatalf("got %v, want the whole partial response", step.Result.Value)
	}
}

func TestHTTPBannerNoResponseReturnsErrUp(t *testing.T) {
	step := httpBanner{}.Next(fpmodule.Input{
		PacketRcv: 0,
		Data:      nil,
		State:     "requested",
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}
	if step.Result.Outcome != fpmodule.ErrUpOutcome {
		t.Fatalf("expected ErrUpOutcome, got %v", step.Result.Outcome)
	}
}
