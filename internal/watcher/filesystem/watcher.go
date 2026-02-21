package filesystem

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

func init() {
	watcher.Register("filesystem", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		var cfg Config
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parsing filesystem config: %w", err)
		}
		if cfg.Path == "" {
			return nil, fmt.Errorf("filesystem watcher %q: \"path\" is required", name)
		}
		if cfg.MaxEvents <= 0 {
			cfg.MaxEvents = 100
		}
		return &FilesystemWatcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 64),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

// Config holds the filesystem-specific watcher configuration.
type Config struct {
	Path      string   `json:"path"`
	Recursive bool     `json:"recursive"`
	Patterns  []string `json:"patterns"` // glob patterns, e.g. ["*.pdf", "*.jpg"]
	MaxEvents int      `json:"max_events"`
}

// FilesystemWatcher watches a directory for file-system events via fsnotify.
type FilesystemWatcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
}

func (w *FilesystemWatcher) Name() string  { return w.name }
func (w *FilesystemWatcher) Type() string  { return "filesystem" }
func (w *FilesystemWatcher) Events() <-chan events.WatchEvent { return w.eventsCh }

func (w *FilesystemWatcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *FilesystemWatcher) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	fw, err := fsnotify.NewWatcher()
	if err != nil {
		cancel()
		return fmt.Errorf("creating fsnotify watcher: %w", err)
	}

	if err := fw.Add(w.cfg.Path); err != nil {
		fw.Close()
		cancel()
		return fmt.Errorf("watching path %q: %w", w.cfg.Path, err)
	}

	// fsnotify does not recurse on Linux; walk and add each subdirectory.
	if w.cfg.Recursive {
		if err := filepath.WalkDir(w.cfg.Path, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil // skip unreadable entries
			}
			if d.IsDir() && path != w.cfg.Path {
				_ = fw.Add(path) // best-effort; ignore errors on individual dirs
			}
			return nil
		}); err != nil {
			fw.Close()
			cancel()
			return fmt.Errorf("walking directory tree: %w", err)
		}
	}

	w.mu.Lock()
	w.status = watcher.StatusConnected
	w.mu.Unlock()

	go w.run(ctx, fw)
	return nil
}

func (w *FilesystemWatcher) run(ctx context.Context, fw *fsnotify.Watcher) {
	defer fw.Close()
	defer close(w.eventsCh)
	defer func() {
		w.mu.Lock()
		w.status = watcher.StatusStopped
		w.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case fsEvent, ok := <-fw.Events:
			if !ok {
				return
			}
			// If recursive and a new directory was created, watch it too.
			if w.cfg.Recursive && fsEvent.Op.Has(fsnotify.Create) {
				if info, err := os.Stat(fsEvent.Name); err == nil && info.IsDir() {
					_ = fw.Add(fsEvent.Name)
				}
			}
			// Apply optional pattern filter (matched against the base filename).
			if len(w.cfg.Patterns) > 0 && !w.matchesPattern(fsEvent.Name) {
				continue
			}
			event := w.toWatchEvent(fsEvent)
			if event == nil {
				continue
			}
			select {
			case w.eventsCh <- *event:
			default: // channel full — drop; TUI will catch up
			}

		case err, ok := <-fw.Errors:
			if !ok {
				return
			}
			w.mu.Lock()
			w.status = watcher.StatusError
			w.mu.Unlock()
			select {
			case w.eventsCh <- events.WatchEvent{
				Timestamp:   time.Now(),
				WatcherName: w.name,
				WatcherType: "filesystem",
				Type:        events.EventError,
				Summary:     fmt.Sprintf("watch error: %v", err),
				Detail:      map[string]any{"error": err.Error()},
			}:
			default:
			}
		}
	}
}

func (w *FilesystemWatcher) matchesPattern(path string) bool {
	base := filepath.Base(path)
	for _, pattern := range w.cfg.Patterns {
		matched, err := filepath.Match(pattern, base)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func (w *FilesystemWatcher) toWatchEvent(fsEvent fsnotify.Event) *events.WatchEvent {
	var eventType events.EventType

	switch {
	case fsEvent.Op.Has(fsnotify.Create):
		eventType = events.EventCreated
	case fsEvent.Op.Has(fsnotify.Write):
		eventType = events.EventModified
	case fsEvent.Op.Has(fsnotify.Remove), fsEvent.Op.Has(fsnotify.Rename):
		eventType = events.EventRemoved
	default:
		return nil // ignore Chmod and other ops
	}

	return &events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: w.name,
		WatcherType: "filesystem",
		Type:        eventType,
		Summary:     fsEvent.Name,
		Detail: map[string]any{
			"path":      fsEvent.Name,
			"operation": fsEvent.Op.String(),
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}
}

func (w *FilesystemWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
