package target

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

// SourceConfig configures Stream. Targets/Domains hold already-comma-split
// entries (splitting "-f a,b,c" is the CLI layer's job); TargetFiles/
// DomainFiles hold paths read one entry per line.
type SourceConfig struct {
	Targets     []string
	TargetFiles []string
	Domains     []string
	DomainFiles []string
	DefaultPort int
}

// Stream resolves cfg into a deduplicated stream of Targets, expanding
// CIDR ranges lazily as they're consumed, and reports any parse/read
// errors encountered along the way on the second channel. Both channels
// are closed once every source has been fully read (or ctx is done).
// Callers MUST drain both channels concurrently (e.g. from a select loop
// or a dedicated goroutine per channel): a blocked errs reader can stall
// the out channel too, since both are produced by the same goroutine.
//
// DNS resolution is deliberately NOT done here for -d/-D entries: it
// happens lazily per dial attempt in the engine, matching the original
// (utils_fp:lookup is called from the connecting state, not the broker),
// which keeps this producer non-blocking and parallelizes lookups across
// the worker pool instead of serializing them up front.
func Stream(ctx context.Context, cfg SourceConfig) (<-chan Target, <-chan error) {
	out := make(chan Target)
	errs := make(chan error)

	go func() {
		defer close(out)
		defer close(errs)

		seen := make(map[string]struct{})
		emit := func(t Target) bool {
			key := fmt.Sprintf("%s:%d", t.Host, t.Port)
			if _, dup := seen[key]; dup {
				return true
			}
			seen[key] = struct{}{}
			select {
			case out <- t:
				return true
			case <-ctx.Done():
				return false
			}
		}
		fail := func(err error) bool {
			select {
			case errs <- err:
				return true
			case <-ctx.Done():
				return false
			}
		}
		emitIPEntry := func(entry string) bool {
			parsed, err := ParseIPEntry(entry, cfg.DefaultPort)
			if err != nil {
				return fail(err)
			}
			for addr := range ExpandCIDR(ctx, parsed.Prefix) {
				if !emit(Target{Host: addr.String(), Port: parsed.Port, Arg: parsed.Arg}) {
					return false
				}
			}
			return true
		}
		emitDomainEntry := func(entry string) bool {
			t, err := ParseDomainEntry(entry, cfg.DefaultPort)
			if err != nil {
				return fail(err)
			}
			return emit(t)
		}

		for _, entry := range cfg.Targets {
			if !emitIPEntry(entry) {
				return
			}
		}
		for _, path := range cfg.TargetFiles {
			lines, err := readLines(path)
			if err != nil {
				if !fail(err) {
					return
				}
				continue
			}
			for _, entry := range lines {
				if !emitIPEntry(entry) {
					return
				}
			}
		}
		for _, entry := range cfg.Domains {
			if !emitDomainEntry(entry) {
				return
			}
		}
		for _, path := range cfg.DomainFiles {
			lines, err := readLines(path)
			if err != nil {
				if !fail(err) {
					return
				}
				continue
			}
			for _, entry := range lines {
				if !emitDomainEntry(entry) {
					return
				}
			}
		}
	}()

	return out, errs
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("target: reading %s: %w", path, err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("target: reading %s: %w", path, err)
	}
	return lines, nil
}
