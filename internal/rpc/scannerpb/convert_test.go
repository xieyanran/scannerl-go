package scannerpb

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

func TestTargetRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   target.Target
	}{
		{"plain", target.Target{Host: "10.0.0.1", Port: 443}},
		{"domain", target.Target{Host: "example.com", Port: 80, IsDomain: true}},
		{"withArg", target.Target{Host: "1.2.3.4", Port: 22, Arg: []string{"extra", "more"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromProtoTarget(ToProtoTarget(tt.in))
			if !reflect.DeepEqual(got, tt.in) {
				t.Errorf("round trip = %+v, want %+v", got, tt.in)
			}
		})
	}
}

// TestResultRoundTrip checks that Record survives a wire round trip
// semantically. Value is JSON-encoded on the wire, which does not
// preserve Go's static type ([]string becomes []interface{} of strings
// on the far side) -- so "want" is normalized through the same JSON round
// trip before comparison, matching what a real receiver actually sees.
func TestResultRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   output.Record
	}{
		{"stringValue", output.Record{
			Module: "tcpbanner", Target: "10.0.0.1", Port: 80,
			Result: fpmodule.Result{Outcome: fpmodule.OKOutcome, Value: "banner text"},
		}},
		{"stringSliceValue", output.Record{
			Module: "https_certif", Target: "example.com", Port: 443,
			Result: fpmodule.Result{Outcome: fpmodule.OKOutcome, Value: []string{"subject=CN=x", "issuer=CN=y"}},
		}},
		{"boolValue", output.Record{
			Module: "mod", Target: "1.2.3.4", Port: 22,
			Result: fpmodule.Result{Outcome: fpmodule.ErrUpOutcome, Value: true},
		}},
		{"nilValue", output.Record{
			Module: "mod", Target: "1.2.3.4", Port: 22,
			Result: fpmodule.Result{Outcome: fpmodule.ErrUnknownOutcome, Value: nil},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr, err := ToProtoResult(tt.in)
			if err != nil {
				t.Fatalf("ToProtoResult: %v", err)
			}
			got, err := FromProtoResult(pr)
			if err != nil {
				t.Fatalf("FromProtoResult: %v", err)
			}

			want := tt.in
			want.Result.Value = jsonNormalize(t, tt.in.Result.Value)

			if !reflect.DeepEqual(got, want) {
				t.Errorf("round trip = %+v, want %+v", got, want)
			}
		})
	}
}

// jsonNormalize round-trips v through JSON the same way the wire does, so
// a test's "want" reflects what FromProtoResult actually produces.
func jsonNormalize(t *testing.T, v any) any {
	t.Helper()
	if v == nil {
		return nil
	}
	b, err := json.Marshal(output.NormalizeValue(v))
	if err != nil {
		t.Fatalf("jsonNormalize: marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("jsonNormalize: unmarshal: %v", err)
	}
	return out
}

func TestOutcomeRoundTrip(t *testing.T) {
	for _, o := range []fpmodule.Outcome{fpmodule.OKOutcome, fpmodule.ErrUpOutcome, fpmodule.ErrUnknownOutcome} {
		rec := output.Record{Result: fpmodule.Result{Outcome: o}}
		pr, err := ToProtoResult(rec)
		if err != nil {
			t.Fatalf("ToProtoResult(%v): %v", o, err)
		}
		got, err := FromProtoResult(pr)
		if err != nil {
			t.Fatalf("FromProtoResult: %v", err)
		}
		if got.Result.Outcome != o {
			t.Errorf("outcome round trip = %v, want %v", got.Result.Outcome, o)
		}
	}
}

func TestFromProtoResultRejectsUnspecifiedOutcome(t *testing.T) {
	_, err := FromProtoResult(&Result{Outcome: Outcome_OUTCOME_UNSPECIFIED})
	if err == nil {
		t.Fatal("expected an error for OUTCOME_UNSPECIFIED, got nil")
	}
}
