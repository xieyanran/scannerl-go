package modules

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

// fakeTLSConn implements fpmodule.Conn using a canned TLSConnectionState;
// embedding a nil net.Conn is safe since httpsCertif.Next never calls the
// embedded methods.
type fakeTLSConn struct {
	net.Conn
	state tls.ConnectionState
	ok    bool
}

func (f fakeTLSConn) TLSConnectionState() (tls.ConnectionState, bool) { return f.state, f.ok }
func (f fakeTLSConn) UpgradeTLS(cfg *tls.Config) (tls.ConnectionState, error) {
	return tls.ConnectionState{}, nil
}

func TestHTTPSCertifReturnsCertFields(t *testing.T) {
	notBefore := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cert := &x509.Certificate{
		Subject:   pkix.Name{CommonName: "example.com"},
		Issuer:    pkix.Name{CommonName: "Test CA"},
		NotBefore: notBefore,
		NotAfter:  notAfter,
		DNSNames:  []string{"example.com", "www.example.com"},
	}

	step := httpsCertif{}.Next(fpmodule.Input{
		Conn: fakeTLSConn{
			ok:    true,
			state: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}},
		},
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}
	if step.Result.Outcome != fpmodule.OKOutcome {
		t.Fatalf("expected OKOutcome, got %v", step.Result.Outcome)
	}

	fields, ok := step.Result.Value.([]string)
	if !ok {
		t.Fatalf("expected []string value, got %T", step.Result.Value)
	}
	joined := strings.Join(fields, "\n")
	for _, want := range []string{
		"subject=CN=example.com",
		"issuer=CN=Test CA",
		"not_before=2024-01-01T00:00:00Z",
		"not_after=2025-01-01T00:00:00Z",
		"dns_names=example.com,www.example.com",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("fields %v missing %q", fields, want)
		}
	}
}

func TestHTTPSCertifNoCertificatePresented(t *testing.T) {
	step := httpsCertif{}.Next(fpmodule.Input{
		Conn: fakeTLSConn{ok: true, state: tls.ConnectionState{}},
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}
	if step.Result.Outcome != fpmodule.ErrUpOutcome {
		t.Fatalf("expected ErrUpOutcome, got %v", step.Result.Outcome)
	}
}

func TestHTTPSCertifNotTLSConnection(t *testing.T) {
	step := httpsCertif{}.Next(fpmodule.Input{
		Conn: fakeTLSConn{ok: false},
	})

	if step.Kind != fpmodule.KindResult {
		t.Fatalf("expected KindResult, got %v", step.Kind)
	}
	if step.Result.Outcome != fpmodule.ErrUnknownOutcome {
		t.Fatalf("expected ErrUnknownOutcome, got %v", step.Result.Outcome)
	}
}
