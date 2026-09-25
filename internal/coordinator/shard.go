package coordinator

import "github.com/xieyanran/scannerl-go/internal/target"

// splitEven divides ts (assumed already shuffled by the caller) into n
// contiguous, roughly-equal shards -- no two shards differ in length by
// more than one element. The first len(ts)%n shards get the extra
// element. n may exceed len(ts), in which case some shards are empty
// (nil, not an error) -- that worker's GetTargets stream simply closes
// immediately.
func splitEven(ts []target.Target, n int) [][]target.Target {
	shards := make([][]target.Target, n)
	if n == 0 {
		return shards
	}
	base, excess := len(ts)/n, len(ts)%n
	start := 0
	for i := 0; i < n; i++ {
		size := base
		if i < excess {
			size++
		}
		shards[i] = ts[start : start+size]
		start += size
	}
	return shards
}
