package target

import (
	"context"
	"net/netip"
)

// ExpandCIDR streams every address in prefix, in ascending order, without
// ever materializing the range as a slice — required since a /8 holds
// 16M+ addresses. Matches the original's tgt:get_tgts, which enumerates
// every address in the range including the network and broadcast
// addresses: these are fingerprinting targets, not a routing table, so
// there's nothing to special-case out.
//
// The channel is closed once the range is exhausted or ctx is done.
func ExpandCIDR(ctx context.Context, prefix netip.Prefix) <-chan netip.Addr {
	out := make(chan netip.Addr)
	go func() {
		defer close(out)
		if !prefix.IsValid() {
			return
		}
		for addr := prefix.Masked().Addr(); prefix.Contains(addr); {
			select {
			case out <- addr:
			case <-ctx.Done():
				return
			}
			next := addr.Next()
			if !next.IsValid() {
				return
			}
			addr = next
		}
	}()
	return out
}
