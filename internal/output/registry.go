package output

import (
	"fmt"
	"sort"
	"sync"
)

var (
	mu       sync.RWMutex
	registry = map[string]func() Output{}
)

// Register makes an Output factory available under name, for use by
// outmodules' init() functions. A factory (not a shared instance) is
// registered because, unlike fpmodule.Module, an Output holds per-run
// state acquired in Init (e.g. an open file) and a fresh instance is
// needed each time one is configured with -o.
func Register(name string, factory func() Output) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("output: module %q already registered", name))
	}
	registry[name] = factory
}

// New creates a fresh Output instance registered under name.
func New(name string) (Output, bool) {
	mu.RLock()
	factory, ok := registry[name]
	mu.RUnlock()
	if !ok {
		return nil, false
	}
	return factory(), true
}

// Names returns every registered output name, sorted.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
