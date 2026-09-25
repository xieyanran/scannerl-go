package worker

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
)

func TestGRPCOutputForwardsWritesToServer(t *testing.T) {
	fake := &fakeScannerServer{}
	addr := startFakeServer(t, fake)
	client := dialFake(t, addr)

	out := NewOutput(context.Background(), client)
	if err := out.Init(output.ScanInfo{}, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}

	records := []output.Record{
		{Module: "tcpbanner", Target: "10.0.0.1", Port: 80,
			Result: fpmodule.Result{Outcome: fpmodule.OKOutcome, Value: "banner"}},
		{Module: "https_certif", Target: "example.com", Port: 443,
			Result: fpmodule.Result{Outcome: fpmodule.OKOutcome, Value: []string{"a", "b"}}},
		{Module: "mod", Target: "1.2.3.4", Port: 22,
			Result: fpmodule.Result{Outcome: fpmodule.ErrUnknownOutcome, Value: nil}},
	}
	for _, rec := range records {
		if err := out.Write(rec); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := out.Clean(); err != nil {
		t.Fatalf("Clean: %v", err)
	}

	got := fake.snapshot()
	if len(got) != len(records) {
		t.Fatalf("server received %d results, want %d", len(got), len(records))
	}
	for i, pr := range got {
		rec, err := scannerpb.FromProtoResult(pr)
		if err != nil {
			t.Fatalf("FromProtoResult: %v", err)
		}
		want := records[i]
		want.Result.Value = jsonRoundTrip(t, want.Result.Value)
		if !reflect.DeepEqual(rec, want) {
			t.Errorf("record %d = %+v, want %+v", i, rec, want)
		}
	}
}

// jsonRoundTrip mirrors what the wire actually does to Result.Value (see
// internal/rpc/scannerpb's ToProtoResult/FromProtoResult), so a test's
// "want" reflects what the far side actually receives rather than the
// original Go static type.
func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	if v == nil {
		return nil
	}
	b, err := json.Marshal(output.NormalizeValue(v))
	if err != nil {
		t.Fatalf("jsonRoundTrip: marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("jsonRoundTrip: unmarshal: %v", err)
	}
	return out
}
