package modules

import (
	"strings"
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

func init() {
	fpmodule.Register("https_certif", httpsCertif{})
}

// httpsCertif is structurally different from tcpBanner and httpBanner: for
// fpmodule.SSL transport, internal/engine/dial.go already performs the TLS
// handshake before Next is ever called, so there's nothing to Continue
// for -- the peer certificate is already sitting on in.Conn on the very
// first (and only) call.
type httpsCertif struct{}

func (httpsCertif) DefaultConfig() fpmodule.Config {
	return fpmodule.Config{
		Port:      443,
		Transport: fpmodule.SSL,
		Timeout:   5 * time.Second,
	}
}

func (httpsCertif) Description() string {
	return "performs a TLS handshake (certificate validation disabled) and returns the peer certificate's subject, issuer, validity window and DNS SANs"
}

func (httpsCertif) Arguments() []string { return nil }

// Next is a one-call state machine, not two: dial.go's handshake already
// ran before this is invoked, so in.Conn.TLSConnectionState() has whatever
// the server presented and a Result can be returned immediately -- no
// Continue round, and in.State is never used (it's always nil, since this
// never returns anything but a Result).
//
// dial.go dials with InsecureSkipVerify: true (this tool fingerprints
// untrusted targets on purpose), so a successful handshake here says
// nothing about the certificate's trustworthiness -- only that one was
// presented, and what it says about itself.
func (httpsCertif) Next(in fpmodule.Input) fpmodule.Step {
	st, ok := in.Conn.TLSConnectionState()
	if !ok {
		return fpmodule.ErrUnknown("connection is not TLS")
	}
	if len(st.PeerCertificates) == 0 {
		return fpmodule.ErrUp("no certificate presented")
	}

	cert := st.PeerCertificates[0]
	fields := []string{
		"subject=" + cert.Subject.String(),
		"issuer=" + cert.Issuer.String(),
		"not_before=" + cert.NotBefore.UTC().Format(time.RFC3339),
		"not_after=" + cert.NotAfter.UTC().Format(time.RFC3339),
	}
	if len(cert.DNSNames) > 0 {
		fields = append(fields, "dns_names="+strings.Join(cert.DNSNames, ","))
	}
	return fpmodule.OK(fields)
}
