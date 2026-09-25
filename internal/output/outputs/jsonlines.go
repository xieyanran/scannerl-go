package outputs

import (
	"encoding"
	"encoding/json"
	"fmt"
	"os"

	"github.com/xieyanran/scannerl-go/internal/output"
)

func init() {
	output.Register("json", func() output.Output {
		return &jsonOutput{enc: json.NewEncoder(os.Stdout)}
	})
}

// jsonOutput prints one JSON object per Record (JSON-lines, e.g. for
// piping into jq). Like stdoutOutput, it needs no locking: every Write
// call is already serialized through output.Sink's single consumer
// goroutine, so at most one Write is ever in flight.
type jsonOutput struct {
	enc *json.Encoder
}

// jsonRecord is Record reshaped for marshaling: Result's Outcome/Value
// are flattened into the top-level object instead of nesting.
type jsonRecord struct {
	Target  string `json:"target"`
	Port    int    `json:"port"`
	Module  string `json:"module"`
	Outcome string `json:"outcome"`
	Value   any    `json:"value"`
}

func (o *jsonOutput) Init(info output.ScanInfo, args []string) error { return nil }

func (o *jsonOutput) Write(rec output.Record) error {
	return o.enc.Encode(jsonRecord{
		Target:  rec.Target,
		Port:    rec.Port,
		Module:  rec.Module,
		Outcome: rec.Result.Outcome.String(),
		Value:   jsonValue(rec.Result.Value),
	})
}

func (o *jsonOutput) Clean() error { return nil }

func (o *jsonOutput) Description() string {
	return "prints one JSON object per result to stdout (JSON-lines)"
}

func (o *jsonOutput) Arguments() []string { return nil }

// jsonValue adapts a fpmodule.Result.Value (documented as one of: string,
// bool, []string, []any, or fmt.Stringer/netip.Addr) into something
// encoding/json renders sensibly. The first group already marshals as
// expected; a bare fmt.Stringer (netip.Addr included, via TextMarshaler)
// would otherwise marshal as its struct fields rather than its text form.
func jsonValue(v any) any {
	if tm, ok := v.(encoding.TextMarshaler); ok {
		if b, err := tm.MarshalText(); err == nil {
			return string(b)
		}
	}
	if s, ok := v.(fmt.Stringer); ok {
		return s.String()
	}
	return v
}
