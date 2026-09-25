package outputs

import (
	"bytes"
	"encoding/json"
	"net/netip"
	"testing"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
)

// plainStringer has no MarshalText, only String, exercising
// output.NormalizeValue's fmt.Stringer fallback (as opposed to the
// encoding.TextMarshaler path netip.Addr takes).
type plainStringer struct{}

func (plainStringer) String() string { return "plain" }

func TestJSONOutputWrite(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "hello", `"hello"`},
		{"bool", true, `true`},
		{"stringSlice", []string{"a", "b"}, `["a","b"]`},
		{"stringer", plainStringer{}, `"plain"`},
		{"textMarshaler", netip.MustParseAddr("192.0.2.1"), `"192.0.2.1"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			o := &jsonOutput{enc: json.NewEncoder(&buf)}

			if err := o.Write(output.Record{
				Module: "mod",
				Target: "example.com",
				Port:   443,
				Result: fpmodule.Result{Outcome: fpmodule.OKOutcome, Value: tt.value},
			}); err != nil {
				t.Fatalf("Write: %v", err)
			}

			var got map[string]json.RawMessage
			if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
				t.Fatalf("output is not valid JSON: %v (%s)", err, buf.String())
			}

			for field, want := range map[string]string{
				"target":  `"example.com"`,
				"port":    `443`,
				"module":  `"mod"`,
				"outcome": `"ok"`,
				"value":   tt.want,
			} {
				if string(got[field]) != want {
					t.Errorf("field %q = %s, want %s", field, got[field], want)
				}
			}
		})
	}
}

func TestJSONOutputWriteOneLinePerRecord(t *testing.T) {
	var buf bytes.Buffer
	o := &jsonOutput{enc: json.NewEncoder(&buf)}

	for i := 0; i < 3; i++ {
		if err := o.Write(output.Record{
			Target: "10.0.0.1",
			Port:   80,
			Result: fpmodule.Result{Outcome: fpmodule.ErrUpOutcome, Value: "x"},
		}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3 (output: %s)", len(lines), buf.String())
	}
	for _, line := range lines {
		if !json.Valid(line) {
			t.Errorf("line is not valid JSON: %s", line)
		}
	}
}

func TestJSONOutputRegistered(t *testing.T) {
	o, ok := output.New("json")
	if !ok {
		t.Fatal(`output.New("json") not found; is it registered?`)
	}
	if o.Description() == "" {
		t.Error("Description() is empty")
	}
}
