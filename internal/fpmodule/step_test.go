package fpmodule

import "testing"

func TestStepConstructors(t *testing.T) {
	c := Continue(3, []byte("hi"), "phase1")
	if c.Kind != KindContinue || c.NumPackets != 3 || string(c.Payload) != "hi" || c.State != "phase1" {
		t.Fatalf("Continue produced unexpected step: %+v", c)
	}

	r := Restart("host2", 8443, "phase2")
	if r.Kind != KindRestart || r.NewTarget != "host2" || r.NewPort != 8443 || r.State != "phase2" {
		t.Fatalf("Restart produced unexpected step: %+v", r)
	}

	ok := OK("value")
	if ok.Kind != KindResult || ok.Result.Outcome != OKOutcome || ok.Result.Value != "value" {
		t.Fatalf("OK produced unexpected step: %+v", ok)
	}

	up := ErrUp("timeout")
	if up.Kind != KindResult || up.Result.Outcome != ErrUpOutcome || up.Result.Value != "timeout" {
		t.Fatalf("ErrUp produced unexpected step: %+v", up)
	}

	unk := ErrUnknown("dns_timeout")
	if unk.Kind != KindResult || unk.Result.Outcome != ErrUnknownOutcome || unk.Result.Value != "dns_timeout" {
		t.Fatalf("ErrUnknown produced unexpected step: %+v", unk)
	}
}
