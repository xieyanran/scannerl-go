package target

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// ParsedIP is one parsed -f/-F entry: a single address (Prefix.Bits()==32)
// or a CIDR range to expand, already masked to its network address.
type ParsedIP struct {
	Prefix netip.Prefix
	Port   int
	Arg    []string
}

// ParseIPEntry parses one -f/-F entry, IPv4 only (matching the original,
// which is dotted-quad based throughout). Supported forms, matching the
// original's tgt:parse_ip:
//
//	ip                 e.g. 10.0.0.1
//	ip:port             e.g. 10.0.0.1:8080          (single host)
//	ip/prefix           e.g. 10.0.0.0/24             (range, default port)
//	ip:port/prefix       e.g. 10.0.0.0:8080/24        (range, explicit port)
//
// "ip/prefix:port" (prefix before port) is intentionally not supported —
// the original's own parser silently mis-parses that order (falls back to
// a /32 without applying the port), which isn't behavior worth preserving;
// this port rejects it outright instead.
func ParseIPEntry(entry string, defPort int) (ParsedIP, error) {
	head, arg := splitArg(entry)

	bits := 32
	portStr := ""
	switch {
	case strings.Contains(head, "/"):
		var rangePart string
		head, rangePart, _ = strings.Cut(head, "/")
		if strings.Contains(head, ":") {
			head, portStr, _ = strings.Cut(head, ":")
		}
		b, err := strconv.Atoi(rangePart)
		if err != nil || b < 0 || b > 32 {
			return ParsedIP{}, fmt.Errorf("target: invalid prefix %q in %q", rangePart, entry)
		}
		bits = b
	case strings.Contains(head, ":"):
		head, portStr, _ = strings.Cut(head, ":")
	}

	addr, err := netip.ParseAddr(head)
	if err != nil || !addr.Is4() {
		return ParsedIP{}, fmt.Errorf("target: invalid IPv4 address %q in %q", head, entry)
	}

	port, err := parsePort(portStr, defPort, entry)
	if err != nil {
		return ParsedIP{}, err
	}

	return ParsedIP{Prefix: netip.PrefixFrom(addr, bits).Masked(), Port: port, Arg: arg}, nil
}

// ParseDomainEntry parses one -d/-D entry in the form domain[:port][+arg],
// matching the original's tgt:parse_domain. The host is never IP-parsed,
// even if it looks like one — the -d/-D vs -f/-F choice determines whether
// DNS resolution happens at dial time.
func ParseDomainEntry(entry string, defPort int) (Target, error) {
	head, arg := splitArg(entry)

	host, portStr := head, ""
	if strings.Contains(head, ":") {
		host, portStr, _ = strings.Cut(head, ":")
	}
	if host == "" {
		return Target{}, fmt.Errorf("target: empty domain in %q", entry)
	}

	port, err := parsePort(portStr, defPort, entry)
	if err != nil {
		return Target{}, err
	}

	return Target{Host: host, Port: port, IsDomain: true, Arg: arg}, nil
}

func parsePort(portStr string, defPort int, entry string) (int, error) {
	if portStr == "" {
		return defPort, nil
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p < 0 || p > 65535 {
		return 0, fmt.Errorf("target: invalid port %q in %q", portStr, entry)
	}
	return p, nil
}

// splitArg peels off any "+"-separated argument suffixes, e.g.
// "1.2.3.4+extra1+extra2" -> ("1.2.3.4", ["extra1", "extra2"]). arg is nil
// (not an empty slice) when there is no "+" suffix.
func splitArg(entry string) (head string, arg []string) {
	parts := strings.Split(entry, "+")
	if len(parts) == 1 {
		return parts[0], nil
	}
	return parts[0], parts[1:]
}
