// Package target resolves scannerl's -f/-F/-d/-D flags into a stream of
// individual targets to probe, expanding CIDR ranges and deduplicating
// along the way without ever materializing huge ranges as a slice.
package target

// Target is a single host to probe.
type Target struct {
	// Host is the original target text: a literal IP, or a domain name.
	// It is never a CIDR — those are expanded before reaching a Target.
	Host string
	// Port is the port to probe, already resolved from the -p default or
	// a per-target override.
	Port int
	// IsDomain is true if Host came from -d/-D (resolved via DNS at dial
	// time, eligible for the -w www-retry) rather than -f/-F (a literal
	// IP or a single address out of an expanded CIDR).
	IsDomain bool
	// Arg carries any "+"-separated suffixes (e.g. "1.2.3.4+extra"),
	// preserved for module use but unused by any of the currently
	// ported modules.
	Arg []string
}
