package filesystem

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"auphanim/internal/events"
)

func TestFilesystemWatcher_Create(t *testing.T) {
	dir := t.TempDir()

	raw, _ := json.Marshal(map[string]any{
		"name":       "test",
		"type":       "filesystem",
		"path":       dir,
		"recursive":  false,
		"max_events": 10,
	})

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}

	w := &FilesystemWatcher{
		name:     "test",
		cfg:      cfg,
		eventsCh: make(chan events.WatchEvent, 16),
		status:   0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	// Create a file and expect an EventCreated notification.
	newFile := filepath.Join(dir, "hello.txt")
	if err := os.WriteFile(newFile, []byte("hi"), 0644); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-w.Events():
		if ev.Type != events.EventCreated && ev.Type != events.EventModified {
			t.Errorf("expected EventCreated or EventModified, got %s", ev.Type)
		}
		if ev.WatcherName != "test" {
			t.Errorf("expected watcher name %q, got %q", "test", ev.WatcherName)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for EventCreated")
	}
}

func TestFilesystemWatcher_PatternFilter(t *testing.T) {
	dir := t.TempDir()

	w := &FilesystemWatcher{
		name: "test",
		cfg: Config{
			Path:      dir,
			Patterns:  []string{"*.pdf"},
			MaxEvents: 10,
		},
		eventsCh: make(chan events.WatchEvent, 16),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	// Create a non-matching file first.
	os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("x"), 0644)

	// Small delay to let the non-matching event be processed.
	time.Sleep(100 * time.Millisecond)

	// Create a matching file.
	os.WriteFile(filepath.Join(dir, "doc.pdf"), []byte("x"), 0644)

	select {
	case ev := <-w.Events():
		matched, _ := filepath.Match("*.pdf", filepath.Base(ev.Detail["path"].(string)))
		if !matched {
			t.Errorf("expected a .pdf event, got path %v", ev.Detail["path"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for filtered event")
	}
}
