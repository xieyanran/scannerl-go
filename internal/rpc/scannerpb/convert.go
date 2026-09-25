package scannerpb

import (
	"encoding/json"
	"fmt"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// ToProtoTarget converts a target.Target into its wire form.
func ToProtoTarget(t target.Target) *Target {
	return &Target{
		Host:     t.Host,
		Port:     int32(t.Port),
		IsDomain: t.IsDomain,
		Arg:      t.Arg,
	}
}

// FromProtoTarget converts a wire Target back into a target.Target.
func FromProtoTarget(pt *Target) target.Target {
	return target.Target{
		Host:     pt.GetHost(),
		Port:     int(pt.GetPort()),
		IsDomain: pt.GetIsDomain(),
		Arg:      pt.GetArg(),
	}
}

var outcomeToProto = map[fpmodule.Outcome]Outcome{
	fpmodule.OKOutcome:         Outcome_OUTCOME_OK,
	fpmodule.ErrUpOutcome:      Outcome_OUTCOME_ERROR_UP,
	fpmodule.ErrUnknownOutcome: Outcome_OUTCOME_ERROR_UNKNOWN,
}

var outcomeFromProto = map[Outcome]fpmodule.Outcome{
	Outcome_OUTCOME_OK:            fpmodule.OKOutcome,
	Outcome_OUTCOME_ERROR_UP:      fpmodule.ErrUpOutcome,
	Outcome_OUTCOME_ERROR_UNKNOWN: fpmodule.ErrUnknownOutcome,
}

// ToProtoResult converts an output.Record into its wire form. Result.Value
// (documented as one of: string, bool, []string, []any, or
// fmt.Stringer/netip.Addr) is normalized via output.NormalizeValue, then
// JSON-encoded into value_json.
func ToProtoResult(rec output.Record) (*Result, error) {
	outcome, ok := outcomeToProto[rec.Result.Outcome]
	if !ok {
		return nil, fmt.Errorf("scannerpb: unknown outcome %v", rec.Result.Outcome)
	}
	var valueJSON string
	if rec.Result.Value != nil {
		b, err := json.Marshal(output.NormalizeValue(rec.Result.Value))
		if err != nil {
			return nil, fmt.Errorf("scannerpb: encoding result value: %w", err)
		}
		valueJSON = string(b)
	}
	return &Result{
		Module:    rec.Module,
		Target:    rec.Target,
		Port:      int32(rec.Port),
		Outcome:   outcome,
		ValueJson: valueJSON,
	}, nil
}

// FromProtoResult converts a wire Result back into an output.Record.
//
// Round-tripping Value through JSON does not preserve its original Go
// static type: a []string becomes []interface{} of strings on this side,
// since encoding/json has no way to know the origin shape once it's
// decoding into `any`. This is invisible to every current consumer
// (stdoutOutput's %v formatting and jsonOutput's re-marshal both render
// []string{"a","b"} and []interface{}{"a","b"} identically) but would
// matter to a hypothetical caller doing a type switch on Value.
func FromProtoResult(pr *Result) (output.Record, error) {
	outcome, ok := outcomeFromProto[pr.GetOutcome()]
	if !ok {
		return output.Record{}, fmt.Errorf("scannerpb: unknown or unspecified outcome %v", pr.GetOutcome())
	}
	var value any
	if pr.GetValueJson() != "" {
		if err := json.Unmarshal([]byte(pr.GetValueJson()), &value); err != nil {
			return output.Record{}, fmt.Errorf("scannerpb: decoding result value: %w", err)
		}
	}
	return output.Record{
		Module: pr.GetModule(),
		Target: pr.GetTarget(),
		Port:   int(pr.GetPort()),
		Result: fpmodule.Result{
			Outcome: outcome,
			Value:   value,
		},
	}, nil
}
