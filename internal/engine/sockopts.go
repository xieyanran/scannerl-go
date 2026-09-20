package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SockOpts is the reduced-scope reinterpretation of -K/--socket: Go's
// net.Dialer doesn't expose an arbitrary key-value option bag the way
// Erlang's inet:setopts does, so only a small, useful allowlist is
// supported instead of an arbitrary passthrough.
type SockOpts struct {
	NoDelay     *bool
	KeepAlive   *time.Duration
	BindAddress string
	LingerSecs  *int
}

// ParseSockOpts parses the comma-separated key[:value] list from -K.
// Recognized keys: nodelay, keepalive:<seconds>, bind:<address>,
// linger:<seconds>. Unknown keys are rejected rather than silently
// ignored, so a typo doesn't silently do nothing.
func ParseSockOpts(raw string) (SockOpts, error) {
	var opts SockOpts
	if raw == "" {
		return opts, nil
	}
	for _, item := range strings.Split(raw, ",") {
		key, value, _ := strings.Cut(item, ":")
		switch key {
		case "nodelay":
			v := true
			opts.NoDelay = &v
		case "keepalive":
			secs, err := strconv.Atoi(value)
			if err != nil {
				return SockOpts{}, fmt.Errorf("engine: invalid keepalive value %q", value)
			}
			d := time.Duration(secs) * time.Second
			opts.KeepAlive = &d
		case "bind":
			if value == "" {
				return SockOpts{}, fmt.Errorf("engine: bind requires an address")
			}
			opts.BindAddress = value
		case "linger":
			secs, err := strconv.Atoi(value)
			if err != nil {
				return SockOpts{}, fmt.Errorf("engine: invalid linger value %q", value)
			}
			opts.LingerSecs = &secs
		default:
			return SockOpts{}, fmt.Errorf("engine: unknown socket option %q (want one of nodelay, keepalive, bind, linger)", key)
		}
	}
	return opts, nil
}
