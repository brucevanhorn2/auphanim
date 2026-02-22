// Package logfile watches one or more log files and emits one WatchEvent per
// new line, similar to "tail -f". It uses fsnotify to detect writes without
// polling, and handles log rotation by comparing the inode of the open file
// descriptor against the inode of the file at the configured path.
package logfile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// Config holds logfile-specific watcher configuration.
type Config struct {
	// Paths is the list of files to tail. At least one is required.
	Paths []string `json:"paths"`
	// Patterns, when non-empty, keeps only lines that contain at least one
	// of the given substrings (case-insensitive).
	Patterns []string `json:"patterns"`
	// ErrorPatterns marks a line as EventError instead of EventMessage when
	// the line contains any of these substrings (case-insensitive). Defaults
	// to common error keywords when omitted.
	ErrorPatterns []string `json:"error_patterns"`
	MaxEvents     int      `json:"max_events"`
}

// fileState tracks the open file handle and last-read byte offset for one file.
type fileState struct {
	path string  // original configured path
	abs  string  // absolute path (used as map key)
	f    *os.File
	pos  int64
}

// LogfileWatcher tails one or more log files.
type LogfileWatcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
}

func init() {
	watcher.Register("logfile", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		cfg := Config{
			ErrorPatterns: []string{"ERROR", "FATAL", "PANIC", "EXCEPTION", "panic:"},
		}
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parsing logfile config: %w", err)
		}
		if len(cfg.Paths) == 0 {
			return nil, fmt.Errorf("logfile watcher %q: at least one path is required", name)
		}
		if cfg.MaxEvents <= 0 {
			cfg.MaxEvents = 100
		}
		return &LogfileWatcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 256),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

func (w *LogfileWatcher) Name() string                     { return w.name }
func (w *LogfileWatcher) Type() string                     { return "logfile" }
func (w *LogfileWatcher) Events() <-chan events.WatchEvent { return w.eventsCh }

func (w *LogfileWatcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *LogfileWatcher) setStatus(s watcher.Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
}

func (w *LogfileWatcher) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)

	fw, err := fsnotify.NewWatcher()
	if err != nil {
		w.cancel()
		return fmt.Errorf("creating fsnotify watcher: %w", err)
	}

	// Open each file at EOF so we only see new content.
	states := make(map[string]*fileState, len(w.cfg.Paths))
	watchedDirs := make(map[string]struct{})

	for _, path := range w.cfg.Paths {
		abs, err := filepath.Abs(path)
		if err != nil {
			fw.Close()
			w.cancel()
			return fmt.Errorf("resolving path %q: %w", path, err)
		}

		f, err := os.Open(abs)
		if err != nil {
			fw.Close()
			w.cancel()
			return fmt.Errorf("opening log file %q: %w", path, err)
		}

		pos, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			f.Close()
			fw.Close()
			w.cancel()
			return fmt.Errorf("seeking log file %q: %w", path, err)
		}

		states[abs] = &fileState{path: path, abs: abs, f: f, pos: pos}

		// Watch the parent directory so we catch rotation (new file created at
		// same path) in addition to plain writes.
		dir := filepath.Dir(abs)
		if _, seen := watchedDirs[dir]; !seen {
			if err := fw.Add(dir); err != nil {
				// Fall back to watching the file directly.
				_ = fw.Add(abs)
			}
			watchedDirs[dir] = struct{}{}
		}
	}

	w.setStatus(watcher.StatusConnected)
	go w.run(ctx, fw, states)
	return nil
}

func (w *LogfileWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *LogfileWatcher) run(ctx context.Context, fw *fsnotify.Watcher, states map[string]*fileState) {
	defer fw.Close()
	defer close(w.eventsCh)
	defer func() {
		for _, st := range states {
			st.f.Close()
		}
		w.setStatus(watcher.StatusStopped)
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case fsEvent, ok := <-fw.Events:
			if !ok {
				return
			}
			if !fsEvent.Op.Has(fsnotify.Write) && !fsEvent.Op.Has(fsnotify.Create) {
				continue
			}
			absEvent, _ := filepath.Abs(fsEvent.Name)
			st, ok := states[absEvent]
			if !ok {
				continue
			}
			w.readNewLines(st)

		case err, ok := <-fw.Errors:
			if !ok {
				return
			}
			w.setStatus(watcher.StatusError)
			select {
			case w.eventsCh <- events.WatchEvent{
				Timestamp:   time.Now(),
				WatcherName: w.name,
				WatcherType: "logfile",
				Type:        events.EventError,
				Summary:     fmt.Sprintf("watch error: %v", err),
				Detail:      map[string]any{"error": err.Error()},
			}:
			default:
			}
		}
	}
}

// readNewLines reads any new content appended to the file since the last call,
// splits it into lines, and emits one WatchEvent per complete line.
func (w *LogfileWatcher) readNewLines(st *fileState) {
	// Detect rotation: if the file at the path now has a different inode than
	// our open fd, re-open the new file from the beginning.
	if pathInfo, err := os.Stat(st.path); err == nil {
		if fdInfo, err := st.f.Stat(); err == nil && !os.SameFile(pathInfo, fdInfo) {
			st.f.Close()
			newF, err := os.Open(st.path)
			if err != nil {
				return
			}
			st.f = newF
			st.pos = 0
		}
	}

	// Detect truncation.
	if info, err := st.f.Stat(); err == nil && info.Size() < st.pos {
		st.pos = 0
	}

	if _, err := st.f.Seek(st.pos, io.SeekStart); err != nil {
		return
	}

	data, err := io.ReadAll(st.f)
	if err != nil || len(data) == 0 {
		return
	}

	// Only process complete lines (ending with \n).
	lastNL := bytes.LastIndexByte(data, '\n')
	if lastNL == -1 {
		// Partial line — wait for the terminating newline.
		return
	}

	st.pos += int64(lastNL + 1)

	base := filepath.Base(st.path)
	for _, line := range strings.Split(string(data[:lastNL]), "\n") {
		if line == "" {
			continue
		}
		if !w.matchesPattern(line) {
			continue
		}
		evtType := w.classifyLine(line)
		select {
		case w.eventsCh <- events.WatchEvent{
			Timestamp:   time.Now(),
			WatcherName: w.name,
			WatcherType: "logfile",
			Type:        evtType,
			Summary:     base + ": " + line,
			Detail: map[string]any{
				"file": st.path,
				"line": line,
			},
		}:
		default:
		}
	}
}

// matchesPattern returns true if the line should be included.
// An empty Patterns list means all lines pass.
func (w *LogfileWatcher) matchesPattern(line string) bool {
	if len(w.cfg.Patterns) == 0 {
		return true
	}
	lower := strings.ToLower(line)
	for _, p := range w.cfg.Patterns {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// classifyLine returns EventError if the line matches an error pattern.
func (w *LogfileWatcher) classifyLine(line string) events.EventType {
	upper := strings.ToUpper(line)
	for _, p := range w.cfg.ErrorPatterns {
		if strings.Contains(upper, strings.ToUpper(p)) {
			return events.EventError
		}
	}
	return events.EventMessage
}
