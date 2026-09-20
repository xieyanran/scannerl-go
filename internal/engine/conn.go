package engine

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"
)

// liveConn wraps a net.Conn to implement fpmodule.Conn: it tracks TLS
// state and supports swapping the wrapped connection in place for a
// STARTTLS-style mid-connection upgrade. One liveConn is created per
// dialed connection and watches ctx for the lifetime of that connection,
// forcing a deadline to unblock any in-flight Read/Write promptly on
// cancellation.
type liveConn struct {
	net.Conn
	tlsState *tls.ConnectionState
	timeout  time.Duration

	closeOnce sync.Once
	closed    chan struct{}
}

func newLiveConn(ctx context.Context, c net.Conn, timeout time.Duration) *liveConn {
	lc := &liveConn{Conn: c, timeout: timeout, closed: make(chan struct{})}
	go lc.watchCancel(ctx)
	return lc
}

func newTLSLiveConn(ctx context.Context, c *tls.Conn, timeout time.Duration) *liveConn {
	lc := newLiveConn(ctx, c, timeout)
	st := c.ConnectionState()
	lc.tlsState = &st
	return lc
}

func (c *liveConn) watchCancel(ctx context.Context) {
	select {
	case <-ctx.Done():
		_ = c.Conn.SetDeadline(time.Now())
	case <-c.closed:
	}
}

// Close stops the cancellation watcher before closing the underlying
// connection, so probeOne can always call Close without leaking the
// watcher goroutine regardless of why the probe ended.
func (c *liveConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *liveConn) TLSConnectionState() (tls.ConnectionState, bool) {
	if c.tlsState == nil {
		return tls.ConnectionState{}, false
	}
	return *c.tlsState, true
}

// UpgradeTLS wraps the current connection in a TLS client over the same
// underlying socket and performs the handshake, bounded by the module's
// configured timeout (Module.Next has no context of its own to bound
// this with). On success it swaps the wrapped connection in place, so
// subsequent engine-mediated reads/writes on this Conn transparently
// speak TLS.
func (c *liveConn) UpgradeTLS(cfg *tls.Config) (tls.ConnectionState, error) {
	if c.timeout > 0 {
		_ = c.Conn.SetDeadline(time.Now().Add(c.timeout))
		defer c.Conn.SetDeadline(time.Time{})
	}
	tc := tls.Client(c.Conn, cfg)
	if err := tc.Handshake(); err != nil {
		return tls.ConnectionState{}, err
	}
	c.Conn = tc
	st := tc.ConnectionState()
	c.tlsState = &st
	return st, nil
}

// readOne performs a single Read bounded by timeout, returning any bytes
// read even if an error also occurred (matching io.Reader's contract
// that both can be non-zero/non-nil at once).
func (c *liveConn) readOne(timeout time.Duration) ([]byte, error) {
	if err := c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	buf := make([]byte, 65536) // matches the original's {recbuf, 65536}
	n, err := c.Read(buf)
	if n > 0 {
		return buf[:n], nil
	}
	return nil, err
}

// writeAll writes payload in full, bounded by timeout. An empty payload
// is still written (a harmless no-op), matching the original which never
// special-cases it either.
func (c *liveConn) writeAll(payload []byte, timeout time.Duration) error {
	if err := c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err := c.Write(payload)
	return err
}
