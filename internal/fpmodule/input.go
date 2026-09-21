package fpmodule

import (
	"crypto/tls"
	"net"
	"net/netip"
)

// Conn is the live connection made available to modules that need
// transport-level access beyond a byte buffer: TLS certificate grabbing
// (https_certif) and mid-connection STARTTLS upgrade (smtp/imap/pop3
// _certif). Simple request/response modules never touch it.
type Conn interface {
	net.Conn

	// TLSConnectionState reports the current TLS state, ok=false if the
	// live connection is not (yet) TLS.
	TLSConnectionState() (tls.ConnectionState, bool)

	// UpgradeTLS wraps the current connection in a TLS client using the
	// same underlying socket (STARTTLS-style) and performs the handshake.
	// On success, all subsequent engine-mediated reads/writes on this
	// Conn transparently speak TLS.
	UpgradeTLS(cfg *tls.Config) (tls.ConnectionState, error)
}

// Input is the state handed to Module.Next on every call.
type Input struct {
	// Target/IP/Port are the CURRENT effective target: Target is the
	// original host string unless a Restart step changed it, IP is the
	// resolved address actually dialed, Port is the current port.
	Target string
	IP     netip.Addr
	Port   int

	// Args are the module's own colon-separated CLI arguments, e.g.
	// ["true"] for "-m httpbg:true".
	Args []string

	// MaxPkt is the run's configured max-packet-per-round cap (Config.MaxPkt,
	// itself defaulted from Module.DefaultConfig unless overridden by -j),
	// handed to the module so it can cap the NumPackets it requests on a
	// Continue step.
	MaxPkt int

	// PacketRcv/Data describe ONLY the most recent Continue round's
	// response, not bytes accumulated across the whole probe. PacketRcv
	// < 1 after a wait means the round timed out.
	PacketRcv int
	Data      []byte

	// State is opaque module data carried forward from the previous
	// Step.State. It is nil on the first call for a target (equivalent
	// to the original's moddata == undefined).
	State any

	// Conn is the live connection, valid for the duration of this call.
	Conn Conn
}
