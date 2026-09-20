package target

import (
	"net/netip"
	"reflect"
	"testing"
)

func TestParseIPEntry(t *testing.T) {
	tests := []struct {
		entry   string
		defPort int
		want    ParsedIP
		wantErr bool
	}{
		{
			entry: "10.0.0.1", defPort: 80,
			want: ParsedIP{Prefix: netip.MustParsePrefix("10.0.0.1/32"), Port: 80},
		},
		{
			entry: "10.0.0.1:8080", defPort: 80,
			want: ParsedIP{Prefix: netip.MustParsePrefix("10.0.0.1/32"), Port: 8080},
		},
		{
			entry: "10.0.0.0/24", defPort: 80,
			want: ParsedIP{Prefix: netip.MustParsePrefix("10.0.0.0/24"), Port: 80},
		},
		{
			// host bits get masked off, matching the original's mask_ip.
			entry: "10.0.0.5/24", defPort: 80,
			want: ParsedIP{Prefix: netip.MustParsePrefix("10.0.0.0/24"), Port: 80},
		},
		{
			entry: "10.0.0.0:8080/24", defPort: 80,
			want: ParsedIP{Prefix: netip.MustParsePrefix("10.0.0.0/24"), Port: 8080},
		},
		{
			entry: "10.0.0.1+extra1+extra2", defPort: 80,
			want: ParsedIP{Prefix: netip.MustParsePrefix("10.0.0.1/32"), Port: 80, Arg: []string{"extra1", "extra2"}},
		},
		{entry: "not-an-ip", defPort: 80, wantErr: true},
		{entry: "10.0.0.1/33", defPort: 80, wantErr: true},
		{entry: "10.0.0.1:notaport", defPort: 80, wantErr: true},
		{entry: "::1", defPort: 80, wantErr: true}, // IPv6 unsupported, matching the original
	}

	for _, tt := range tests {
		t.Run(tt.entry, func(t *testing.T) {
			got, err := ParseIPEntry(tt.entry, tt.defPort)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseIPEntry(%q) = %+v, want error", tt.entry, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseIPEntry(%q) unexpected error: %v", tt.entry, err)
			}
			if got.Prefix != tt.want.Prefix || got.Port != tt.want.Port || !reflect.DeepEqual(got.Arg, tt.want.Arg) {
				t.Fatalf("ParseIPEntry(%q) = %+v, want %+v", tt.entry, got, tt.want)
			}
		})
	}
}

func TestParseDomainEntry(t *testing.T) {
	tests := []struct {
		entry   string
		defPort int
		want    Target
		wantErr bool
	}{
		{
			entry: "example.com", defPort: 80,
			want: Target{Host: "example.com", Port: 80, IsDomain: true},
		},
		{
			entry: "example.com:8443", defPort: 80,
			want: Target{Host: "example.com", Port: 8443, IsDomain: true},
		},
		{
			entry: "example.com+extra", defPort: 80,
			want: Target{Host: "example.com", Port: 80, IsDomain: true, Arg: []string{"extra"}},
		},
		{entry: "", defPort: 80, wantErr: true},
		{entry: "example.com:notaport", defPort: 80, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.entry, func(t *testing.T) {
			got, err := ParseDomainEntry(tt.entry, tt.defPort)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseDomainEntry(%q) = %+v, want error", tt.entry, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDomainEntry(%q) unexpected error: %v", tt.entry, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseDomainEntry(%q) = %+v, want %+v", tt.entry, got, tt.want)
			}
		})
	}
}
