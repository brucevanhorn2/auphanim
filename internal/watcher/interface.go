package watcher

import (
	"context"
	"encoding/json"

	"auphanim/internal/events"
)

// Status represents the connection state of a watcher.
type Status int

const (
	StatusConnecting Status = iota
	StatusConnected
	StatusError
	StatusStopped
)

func (s Status) String() string {
	switch s {
	case StatusConnecting:
		return "Connecting"
	case StatusConnected:
		return "Connected"
	case StatusError:
		return "Error"
	case StatusStopped:
		return "Stopped"
	default:
		return "Unknown"
	}
}

// Watcher is the core extensibility interface. Every watcher type (postgres,
// kafka, filesystem, future redis/http/sysmetrics/…) implements these four
// methods. Adding a new watcher type requires zero changes to the core
// architecture — just implement the interface and register via init().
type Watcher interface {
	// Name returns the user-configured display name (from config "name" field).
	Name() string

	// Type returns the watcher type string (e.g. "postgres", "kafka").
	Type() string

	// Start launches the background goroutine(s) that watch for events.
	// ctx cancellation triggers graceful shutdown.
	Start(ctx context.Context) error

	// Stop signals the watcher to shut down.
	Stop()

	// Events returns the read-only channel of WatchEvents.
	// The channel is closed when the watcher fully stops.
	Events() <-chan events.WatchEvent

	// Status returns the current connection status.
	Status() Status
}

// WatcherFactory creates a Watcher from the raw config JSON for one entry.
type WatcherFactory func(name string, rawConfig json.RawMessage) (Watcher, error)
