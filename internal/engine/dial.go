package engine

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"strings"
	"syscall"
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

// eaccessMax matches the original's eaccess_max: how many times to retry
// a privileged-source-port dial after EACCES before giving up.
const eaccessMax = 2

// lookup resolves host to an IPv4 address: a literal IP parses directly
// (no DNS involved, so -w never applies to -f/-F targets); a domain is
// resolved via DNS, retried against "www."+host on failure when
// tryWWW is set, matching the original's -w/--www and utils_fp:lookupwww.
func lookup(ctx context.Context, host string, tryWWW bool) (netip.Addr, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		if !addr.Is4() {
			return netip.Addr{}, fmt.Errorf("engine: only IPv4 targets are supported, got %q", host)
		}
		return addr, nil
	}

	addr, err := resolveOnce(ctx, host)
	if err == nil {
		return addr, nil
	}
	if tryWWW && !strings.HasPrefix(host, "www.") {
		if addr, wwwErr := resolveOnce(ctx, "www."+host); wwwErr == nil {
			return addr, nil
		}
	}
	return netip.Addr{}, err
}

func resolveOnce(ctx context.Context, host string) (netip.Addr, error) {
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err != nil {
		return netip.Addr{}, err
	}
	if len(ips) == 0 {
		return netip.Addr{}, fmt.Errorf("engine: no A record for %s", host)
	}
	addr, ok := netip.AddrFromSlice(ips[0].To4())
	if !ok {
		return netip.Addr{}, fmt.Errorf("engine: could not parse resolved address for %s", host)
	}
	return addr, nil
}

// dialWithRetry wraps dial with a bounded retry-with-backoff around the
// CONNECT step, separate from Config.Retry (which only governs resending
// within an already-established connection, never the dial itself). A
// transient dial failure (momentary network blip, target briefly
// overloaded) gets Config.ConnectRetries extra attempts, with the delay
// between attempts starting at Config.ConnectBackoff and doubling each
// time. Every dial error is retried uniformly (no attempt is made to
// distinguish "transient" from "permanent" failures like ECONNREFUSED --
// that classification happens afterward, in classifyConnectErr, once
// retries are exhausted): a wrong guess about which errors are worth
// retrying would either waste time on truly permanent failures or, worse,
// give up early on a target that would have come up.
func (e *Engine) dialWithRetry(ctx context.Context, host string, port int) (*liveConn, netip.Addr, error) {
	conn, ip, err := e.dial(ctx, host, port)
	backoff := e.cfg.ConnectBackoff
	for attempt := 0; err != nil && attempt < e.cfg.ConnectRetries; attempt++ {
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ip, ctx.Err()
		}
		backoff *= 2
		conn, ip, err = e.dial(ctx, host, port)
	}
	return conn, ip, err
}

// dial resolves and connects to host:port per cfg, returning a live
// connection ready for the probe loop. For cfg.Transport == SSL, the TLS
// handshake is performed here (certificate verification is deliberately
// disabled throughout -- this tool fingerprints arbitrary untrusted
// targets, it does not validate a known server).
func (e *Engine) dial(ctx context.Context, host string, port int) (*liveConn, netip.Addr, error) {
	ip, err := lookup(ctx, host, e.cfg.WWW)
	if err != nil {
		return nil, netip.Addr{}, err
	}

	network := "tcp4"
	if e.cfg.Transport == fpmodule.UDP {
		network = "udp4"
	}
	addr := net.JoinHostPort(ip.String(), fmt.Sprint(port))

	conn, err := e.dialWithPrivPorts(ctx, network, addr)
	if err != nil {
		return nil, ip, err
	}
	applyConnOpts(conn, e.cfg.SockOpts)

	if e.cfg.Transport != fpmodule.SSL {
		return newLiveConn(ctx, conn, e.cfg.Timeout), ip, nil
	}

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, ip, err
	}
	return newTLSLiveConn(ctx, tlsConn, e.cfg.Timeout), ip, nil
}

// dialWithPrivPorts dials network/addr, retrying with a freshly-chosen
// privileged source port on EACCES when cfg.PrivPorts is set, up to
// eaccessMax additional attempts, matching the original's
// connecting/3 eaccess_retry handling.
func (e *Engine) dialWithPrivPorts(ctx context.Context, network, addr string) (net.Conn, error) {
	attempts := 1
	if e.cfg.PrivPorts {
		attempts += eaccessMax
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		dialer := &net.Dialer{Timeout: e.cfg.Timeout}
		if opt := e.cfg.SockOpts.KeepAlive; opt != nil {
			dialer.KeepAlive = *opt
		}
		localAddr, err := localAddrFor(network, e.cfg.PrivPorts, e.cfg.SockOpts.BindAddress)
		if err != nil {
			return nil, err
		}
		dialer.LocalAddr = localAddr

		conn, err := dialer.DialContext(ctx, network, addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if !e.cfg.PrivPorts || !errors.Is(err, syscall.EACCES) {
			return nil, err
		}
	}
	return nil, lastErr
}

// localAddrFor builds the dialer's local address, if privileged-port
// binding and/or an explicit bind address were requested. It returns nil
// (let the OS choose) when neither is set.
func localAddrFor(network string, privPorts bool, bindAddress string) (net.Addr, error) {
	if !privPorts && bindAddress == "" {
		return nil, nil
	}
	var ip net.IP
	if bindAddress != "" {
		ip = net.ParseIP(bindAddress)
		if ip == nil {
			return nil, fmt.Errorf("engine: invalid bind address %q", bindAddress)
		}
	}
	port := 0
	if privPorts {
		p, err := randPrivilegedPort()
		if err != nil {
			return nil, err
		}
		port = p
	}
	if strings.HasPrefix(network, "udp") {
		return &net.UDPAddr{IP: ip, Port: port}, nil
	}
	return &net.TCPAddr{IP: ip, Port: port}, nil
}

// randPrivilegedPort returns a cryptographically-random port in [1,1024],
// matching the original's rand:uniform(1024) range for -X/--priv-ports.
func randPrivilegedPort() (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1024))
	if err != nil {
		return 0, err
	}
	return int(n.Int64()) + 1, nil
}

func applyConnOpts(conn net.Conn, opts SockOpts) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if opts.NoDelay != nil {
		_ = tcpConn.SetNoDelay(*opts.NoDelay)
	}
	if opts.LingerSecs != nil {
		_ = tcpConn.SetLinger(*opts.LingerSecs)
	}
}
