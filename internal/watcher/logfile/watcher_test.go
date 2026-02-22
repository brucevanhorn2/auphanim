package logfile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"auphanim/internal/events"
)

// drainN collects up to n events from ch, timing out after d.
func drainN(ch <-chan events.WatchEvent, n int, d time.Duration) []events.WatchEvent {
	var got []events.WatchEvent
	deadline := time.After(d)
	for len(got) < n {
		select {
		case e, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, e)
		case <-deadline:
			return got
		}
	}
	return got
}

func makeWatcher(t *testing.T, paths, patterns, errorPatterns []string) *LogfileWatcher {
	t.Helper()
	cfg := Config{
		Paths:         paths,
		Patterns:      patterns,
		ErrorPatterns: errorPatterns,
		MaxEvents:     50,
	}
	if cfg.ErrorPatterns == nil {
		cfg.ErrorPatterns = []string{"ERROR", "FATAL", "PANIC"}
	}
	return &LogfileWatcher{
		name:     "test",
		cfg:      cfg,
		eventsCh: make(chan events.WatchEvent, 64),
		status:   0,
	}
}

// createLogFile creates an empty file in dir and returns its path.
func createLogFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// appendLines appends lines to the file, each terminated with \n.
func appendLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, l := range lines {
		fmt.Fprintln(f, l)
	}
}

func TestLogfileWatcher_SilentStart(t *testing.T) {
	dir := t.TempDir()
	path := createLogFile(t, dir, "app.log")
	appendLines(t, path, "existing line 1", "existing line 2")

	w := makeWatcher(t, []string{path}, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	// Give the watcher a moment to settle; no events should arrive for
	// content that was present before Start.
	time.Sleep(200 * time.Millisecond)

	select {
	case e := <-w.Events():
		t.Errorf("expected no events for pre-existing content, got: %v", e.Summary)
	default:
		// correct: nothing emitted
	}
}

func TestLogfileWatcher_EmitsNewLines(t *testing.T) {
	dir := t.TempDir()
	path := createLogFile(t, dir, "app.log")

	w := makeWatcher(t, []string{path}, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	appendLines(t, path, "INFO: request received", "INFO: response sent")

	got := drainN(w.Events(), 2, 3*time.Second)
	if len(got) < 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].Type != events.EventMessage {
		t.Errorf("expected EventMessage, got %s", got[0].Type)
	}
	if got[0].WatcherName != "test" {
		t.Errorf("expected watcher name 'test', got %q", got[0].WatcherName)
	}
}

func TestLogfileWatcher_ErrorClassification(t *testing.T) {
	dir := t.TempDir()
	path := createLogFile(t, dir, "app.log")

	w := makeWatcher(t, []string{path}, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	appendLines(t, path, "INFO: all good", "ERROR: database connection failed")

	got := drainN(w.Events(), 2, 3*time.Second)
	if len(got) < 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].Type != events.EventMessage {
		t.Errorf("line 1: expected EventMessage, got %s", got[0].Type)
	}
	if got[1].Type != events.EventError {
		t.Errorf("line 2: expected EventError, got %s", got[1].Type)
	}
}

func TestLogfileWatcher_PatternFilter(t *testing.T) {
	dir := t.TempDir()
	path := createLogFile(t, dir, "app.log")

	// Only emit lines containing "WARN" or "ERROR".
	w := makeWatcher(t, []string{path}, []string{"WARN", "ERROR"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	appendLines(t, path,
		"DEBUG: verbose detail",  // should be filtered out
		"INFO: normal operation", // should be filtered out
		"WARN: disk 80% full",    // should pass
		"ERROR: write failed",    // should pass
	)

	got := drainN(w.Events(), 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("expected 2 filtered events, got %d", len(got))
	}
	for _, e := range got {
		summary := e.Summary
		if !containsAny(summary, "WARN", "ERROR") {
			t.Errorf("filtered event slipped through: %s", summary)
		}
	}
}

func TestLogfileWatcher_SummaryContainsFilename(t *testing.T) {
	dir := t.TempDir()
	path := createLogFile(t, dir, "myapp.log")

	w := makeWatcher(t, []string{path}, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	appendLines(t, path, "hello world")

	got := drainN(w.Events(), 1, 3*time.Second)
	if len(got) < 1 {
		t.Fatal("expected 1 event")
	}
	if got[0].Summary != "myapp.log: hello world" {
		t.Errorf("unexpected summary: %q", got[0].Summary)
	}
}

func containsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
