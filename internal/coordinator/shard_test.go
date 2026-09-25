package coordinator

import (
	"testing"

	"github.com/xieyanran/scannerl-go/internal/target"
)

func mkTargets(n int) []target.Target {
	ts := make([]target.Target, n)
	for i := range ts {
		ts[i] = target.Target{Host: string(rune('a' + i)), Port: i}
	}
	return ts
}

func TestSplitEven(t *testing.T) {
	tests := []struct {
		name      string
		total     int
		n         int
		wantSizes []int
	}{
		{"evenlyDivisible", 9, 3, []int{3, 3, 3}},
		{"remainderGoesToFirstShards", 10, 3, []int{4, 3, 3}},
		{"singleShard", 5, 1, []int{5}},
		{"moreShardsThanTargets", 2, 5, []int{1, 1, 0, 0, 0}},
		{"empty", 0, 3, []int{0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := mkTargets(tt.total)
			shards := splitEven(ts, tt.n)

			if len(shards) != tt.n {
				t.Fatalf("got %d shards, want %d", len(shards), tt.n)
			}

			gotSizes := make([]int, tt.n)
			var total int
			seen := make(map[string]bool)
			for i, s := range shards {
				gotSizes[i] = len(s)
				total += len(s)
				for _, tg := range s {
					key := tg.Host
					if seen[key] {
						t.Errorf("target %q appears in more than one shard", key)
					}
					seen[key] = true
				}
			}
			if total != tt.total {
				t.Errorf("shards contain %d targets total, want %d", total, tt.total)
			}
			for i, want := range tt.wantSizes {
				if gotSizes[i] != want {
					t.Errorf("shard %d size = %d, want %d (all sizes: %v)", i, gotSizes[i], want, gotSizes)
				}
			}
		})
	}
}
