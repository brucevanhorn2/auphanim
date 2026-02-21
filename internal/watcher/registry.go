package watcher

import (
	"encoding/json"
	"fmt"
	"sort"
)

var registry = map[string]WatcherFactory{}

// Register associates a factory function with a watcher type name.
// Called from init() in each watcher package.
func Register(typeName string, factory WatcherFactory) {
	registry[typeName] = factory
}

// Create instantiates a Watcher by looking up the registered factory for
// typeName, then calling it with the watcher's name and raw config JSON.
func Create(typeName, name string, rawConfig json.RawMessage) (Watcher, error) {
	factory, ok := registry[typeName]
	if !ok {
		return nil, fmt.Errorf("unknown watcher type %q (registered types: %v)", typeName, registeredTypes())
	}
	return factory(name, rawConfig)
}

func registeredTypes() []string {
	types := make([]string, 0, len(registry))
	for t := range registry {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}
