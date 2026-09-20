package target

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestStreamMergesAndDedupsAllSources(t *testing.T) {
	dir := t.TempDir()
	targetFile := writeFile(t, dir, "targets.txt", "10.0.0.2\n10.0.0.1\n") // 10.0.0.1 dup with -f below
	domainFile := writeFile(t, dir, "domains.txt", "b.example.com\na.example.com\n")

	cfg := SourceConfig{
		Targets:     []string{"10.0.0.1", "10.0.0.1"}, // dup within -f itself
		TargetFiles: []string{targetFile},
		Domains:     []string{"c.example.com"},
		DomainFiles: []string{domainFile},
		DefaultPort: 80,
	}

	targets, errs := streamAll(t, cfg)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.Host)
	}
	sort.Strings(hosts)
	want := []string{"10.0.0.1", "10.0.0.2", "a.example.com", "b.example.com", "c.example.com"}
	if !stringsEqual(hosts, want) {
		t.Fatalf("got hosts %v, want %v", hosts, want)
	}
}

func TestStreamExpandsCIDRAndReportsErrorsWithoutStopping(t *testing.T) {
	cfg := SourceConfig{
		Targets:     []string{"not-an-ip", "10.0.0.0/30"},
		Domains:     []string{"", "ok.example.com"},
		DefaultPort: 443,
	}

	targets, errs := streamAll(t, cfg)

	if len(errs) != 2 {
		t.Fatalf("got %d errors, want 2 (one bad IP entry, one empty domain): %v", len(errs), errs)
	}

	var hosts []string
	for _, tg := range targets {
		hosts = append(hosts, tg.Host)
	}
	sort.Strings(hosts)
	want := []string{"10.0.0.0", "10.0.0.1", "10.0.0.2", "10.0.0.3", "ok.example.com"}
	if !stringsEqual(hosts, want) {
		t.Fatalf("got hosts %v, want %v", hosts, want)
	}
}

// streamAll drains both of Stream's channels concurrently, which callers
// must do since a stalled errs reader can block the out channel too.
func streamAll(t *testing.T, cfg SourceConfig) ([]Target, []error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, errCh := Stream(ctx, cfg)
	var targets []Target
	var errs []error
	outDone, errDone := false, false
	for !outDone || !errDone {
		select {
		case tg, ok := <-out:
			if !ok {
				outDone = true
				continue
			}
			targets = append(targets, tg)
		case err, ok := <-errCh:
			if !ok {
				errDone = true
				continue
			}
			errs = append(errs, err)
		case <-ctx.Done():
			t.Fatal("timed out draining Stream")
		}
	}
	return targets, errs
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

func stringsEqual(a, b []string) bool {
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
