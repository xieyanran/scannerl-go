package target

import (
	"context"
	"net/netip"
	"testing"
	"time"
)

func TestExpandCIDRSingleHost(t *testing.T) {
	ctx := context.Background()
	got := collect(ExpandCIDR(ctx, netip.MustParsePrefix("10.0.0.5/32")))
	want := []netip.Addr{netip.MustParseAddr("10.0.0.5")}
	if !addrsEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestExpandCIDRSmallRange(t *testing.T) {
	ctx := context.Background()
	got := collect(ExpandCIDR(ctx, netip.MustParsePrefix("10.0.0.0/30")))
	want := []netip.Addr{
		netip.MustParseAddr("10.0.0.0"),
		netip.MustParseAddr("10.0.0.1"),
		netip.MustParseAddr("10.0.0.2"),
		netip.MustParseAddr("10.0.0.3"),
	}
	if !addrsEqual(got, want) {
		t.Fatalf("got %v, want %v (network and broadcast addresses must be included, matching the original)", got, want)
	}
}

func TestExpandCIDRMasksHostBits(t *testing.T) {
	ctx := context.Background()
	got := collect(ExpandCIDR(ctx, netip.MustParsePrefix("10.0.0.5/30").Masked()))
	want := []netip.Addr{
		netip.MustParseAddr("10.0.0.4"),
		netip.MustParseAddr("10.0.0.5"),
		netip.MustParseAddr("10.0.0.6"),
		netip.MustParseAddr("10.0.0.7"),
	}
	if !addrsEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestExpandCIDRStreamsWithoutMaterializing proves a huge prefix (/8, 16M+
// addresses) starts yielding immediately and can be stopped early via
// context cancellation, rather than being built up front as a slice.
func TestExpandCIDRStreamsWithoutMaterializing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := ExpandCIDR(ctx, netip.MustParsePrefix("10.0.0.0/8"))

	select {
	case addr := <-ch:
		if addr != netip.MustParseAddr("10.0.0.0") {
			t.Fatalf("first address = %v, want 10.0.0.0", addr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the first address of a /8; expansion is not streaming")
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			// A handful of in-flight addresses may still land before the
			// goroutine observes cancellation; drain until closed.
			for range ch {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close promptly after context cancellation")
	}
}

func collect(ch <-chan netip.Addr) []netip.Addr {
	var out []netip.Addr
	for addr := range ch {
		out = append(out, addr)
	}
	return out
}

func addrsEqual(a, b []netip.Addr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
