package modules

import (
	"bytes"
	"fmt"
	"time"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
)

func init() {
	fpmodule.Register("http", httpBanner{})
}

// httpBanner is a deliberately different shape from tcpBanner: instead of
// passively listening, it sends a GET request on the first Continue and
// parses whatever comes back.
type httpBanner struct{}

func (httpBanner) DefaultConfig() fpmodule.Config {
	return fpmodule.Config{
		Port:      80,
		Transport: fpmodule.TCP,
		Timeout:   5 * time.Second,
		// A response's headers and body can arrive split across several
		// TCP segments and its total size isn't known up front the way
		// tcpbanner's single unprompted banner is, so keep reading every
		// round that produces more instead of stopping after one.
		MaxPkt: fpmodule.Unlimited,
	}
}

func (httpBanner) Description() string {
	return "sends a GET / request and returns the HTTP status line and headers"
}

func (httpBanner) Arguments() []string { return nil }

// Next sends a minimal request on the first call, then parses whatever came
// back on the second.
//
//  1. First call (in.State == nil): build a hand-rolled "GET / HTTP/1.1"
//     request against in.Target and Continue with it, asking the engine
//     (via MaxPkt: Unlimited above) to keep reading rounds until one
//     produces nothing more. "Connection: close" tells the server to close
//     the socket once it's done writing the response, so that Unlimited
//     read loop (internal/engine/probe.go's sendRecv) stops on EOF right
//     after the response ends instead of blocking for the full timeout.
//
//  2. Second call: in.PacketRcv/in.Data describe everything collected
//     across every round the Continue above triggered. If PacketRcv < 1,
//     nothing came back before the timeout: ErrUp. Otherwise, a body can be
//     arbitrarily large and possibly chunked or compressed, so rather than
//     parse or return the whole response, OK's value is just the status
//     line and headers -- everything up to the first blank line -- which
//     is enough to fingerprint the server without this module needing a
//     full HTTP parser.
func (httpBanner) Next(in fpmodule.Input) fpmodule.Step {
	if in.State == nil {
		req := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", in.Target)
		return fpmodule.Continue(fpmodule.Unlimited, []byte(req), "requested")
	}

	if in.PacketRcv < 1 {
		return fpmodule.ErrUp("no response received")
	}

	if idx := bytes.Index(in.Data, []byte("\r\n\r\n")); idx != -1 {
		return fpmodule.OK(string(in.Data[:idx]))
	}
	return fpmodule.OK(string(in.Data))
}
