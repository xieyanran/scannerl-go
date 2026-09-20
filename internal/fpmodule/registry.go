package fpmodule

import (
	"fmt"
	"sort"
	"sync"
)

var (
	mu       sync.RWMutex
	registry = map[string]Module{}
)

// Register makes a Module available under name, for use by fpmodules'
// init() functions. It panics on duplicate registration, since that can
// only happen from a programming error at package init time.
func Register(name string, m Module) {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("fpmodule: module %q already registered", name))
	}
	registry[name] = m
}

// Get looks up a registered module by name.
func Get(name string) (Module, bool) {
	mu.RLock()
	defer mu.RUnlock()
	m, ok := registry[name]
	return m, ok
}

// Names returns every registered module name, sorted.
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
