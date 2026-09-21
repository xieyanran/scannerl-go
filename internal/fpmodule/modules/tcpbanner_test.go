package modules

import (
	"testing"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

func TestTCPBannerFirstCallAsksEngineToListen(t *testing.T) {
	step := tcpBanner{}.Next(fpmodule.Input{State: nil})

	if step.Kind != fpmodule.KindContinue {
		t.Fatalf("expected KindContinue, got %v", step.Kind)
	}
	if step.State == nil {
		t.Fatalf("expected non-nil State after first invocation, got nil")
	}
}

func TestTCPBannerReturnsBannerAsOK(t *testing.T) {
	step := tcpBanner{}.Next(fpmodule.Input{
		PacketRcv: 1,
		Data:      []byte("SSH-2.0-OpenSSH_8.9p1 Debian-3"),
		State:     "listening",
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}

	if step.Result.Outcome != fpmodule.OKOutcome {
		t.Fatalf("expected OKOutcome, got %v", step.Result.Outcome)
	}

	if step.Result.Value != "SSH-2.0-OpenSSH_8.9p1 Debian-3" {
		t.Fatalf("expected banner value, got %v", step.Result.Value)
	}
}

func TestTCPBannerNoResponseReturnsErrUp(t *testing.T) {
	step := tcpBanner{}.Next(fpmodule.Input{
		PacketRcv: 0,
		Data:      nil,
		State:     "listening",
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}

	if step.Result.Outcome != fpmodule.ErrUpOutcome {
		t.Fatalf("expected ErrUpOutcome, got %v", step.Result.Outcome)
	}
}
