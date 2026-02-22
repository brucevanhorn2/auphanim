package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// newWatcher constructs a RedisWatcher pointed at a miniredis instance.
// miniredis is a pure-Go in-process Redis — no real Redis needed.
func newWatcher(mr *miniredis.Miniredis, cfg Config) *RedisWatcher {
	if cfg.Pattern == "" {
		cfg.Pattern = "*"
	}
	if cfg.PollIntervalS <= 0 {
		cfg.PollIntervalS = 1
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = 50
	}
	cfg.Addr = mr.Addr()
	return &RedisWatcher{
		name:     "test",
		cfg:      cfg,
		eventsCh: make(chan events.WatchEvent, 64),
		status:   watcher.StatusConnecting,
	}
}

// collect drains the events channel for up to timeout, then returns everything
// received. It stops early if the channel is closed.
func collect(ch <-chan events.WatchEvent, timeout time.Duration) []events.WatchEvent {
	deadline := time.After(timeout)
	var out []events.WatchEvent
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestNoEventsOnStart verifies the initial scan is silent: keys that already
// exist when auphanim starts must NOT be reported as EventCreated.
func TestNoEventsOnStart(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Set("pre-existing", "value")

	w := newWatcher(mr, Config{})
	ctx, cancel := context.WithCancel(context.Background())

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for one full poll cycle to complete.
	time.Sleep(1500 * time.Millisecond)
	cancel()

	got := collect(w.Events(), 500*time.Millisecond)
	if len(got) != 0 {
		t.Errorf("expected 0 events from initial snapshot, got %d: %+v", len(got), got)
	}
}

// TestNewKey_EmitsCreated verifies that a key SET after startup produces
// exactly one EventCreated with the correct key name.
func TestNewKey_EmitsCreated(t *testing.T) {
	mr := miniredis.RunT(t)

	w := newWatcher(mr, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let the initial snapshot complete.
	time.Sleep(1200 * time.Millisecond)

	mr.Set("hello", "world")

	// Collect events over the next poll cycle.
	got := collect(w.Events(), 1500*time.Millisecond)

	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	if got[0].Type != events.EventCreated {
		t.Errorf("expected EventCreated, got %s", got[0].Type)
	}
	if got[0].Detail["key"] != "hello" {
		t.Errorf("expected key %q, got %v", "hello", got[0].Detail["key"])
	}
}

// TestDeletedKey_EmitsRemoved verifies that deleting a key produces EventRemoved.
func TestDeletedKey_EmitsRemoved(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Set("to-delete", "bye")

	w := newWatcher(mr, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for initial snapshot to capture the key.
	time.Sleep(1200 * time.Millisecond)

	mr.Del("to-delete")

	got := collect(w.Events(), 1500*time.Millisecond)

	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(got), got)
	}
	if got[0].Type != events.EventRemoved {
		t.Errorf("expected EventRemoved, got %s", got[0].Type)
	}
	if got[0].Detail["key"] != "to-delete" {
		t.Errorf("expected key %q, got %v", "to-delete", got[0].Detail["key"])
	}
}

// TestShowValues_String verifies the value is fetched and stored in Detail
// when show_values is true.
func TestShowValues_String(t *testing.T) {
	mr := miniredis.RunT(t)

	w := newWatcher(mr, Config{ShowValues: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(1200 * time.Millisecond)
	mr.Set("greeting", "hello world")

	got := collect(w.Events(), 1500*time.Millisecond)

	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if got[0].Detail["value"] != "hello world" {
		t.Errorf("expected value %q, got %v", "hello world", got[0].Detail["value"])
	}
}

// TestShowValues_Truncation verifies long string values are truncated at 200 chars.
func TestShowValues_Truncation(t *testing.T) {
	mr := miniredis.RunT(t)

	longVal := string(make([]byte, 300)) // 300 zero bytes
	for i := range longVal {
		longVal = longVal[:i] + "x" + longVal[i+1:]
	}

	w := newWatcher(mr, Config{ShowValues: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(1200 * time.Millisecond)
	mr.Set("big", longVal)

	got := collect(w.Events(), 1500*time.Millisecond)

	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	val, ok := got[0].Detail["value"].(string)
	if !ok {
		t.Fatalf("value is not a string: %T", got[0].Detail["value"])
	}
	// Should be 200 chars of content + the ellipsis character (3 bytes in UTF-8)
	if len([]rune(val)) > 201 {
		t.Errorf("value not truncated: length %d", len(val))
	}
}

// TestShowValues_Hash verifies that hash keys include an item_count.
func TestShowValues_Hash(t *testing.T) {
	mr := miniredis.RunT(t)

	w := newWatcher(mr, Config{ShowValues: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(1200 * time.Millisecond)
	mr.HSet("myhash", "field1", "a", "field2", "b", "field3", "c")

	got := collect(w.Events(), 1500*time.Millisecond)

	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if got[0].Detail["type"] != "hash" {
		t.Errorf("expected type hash, got %v", got[0].Detail["type"])
	}
	if got[0].Detail["item_count"] != int64(3) {
		t.Errorf("expected item_count 3, got %v", got[0].Detail["item_count"])
	}
}

// TestPatternFilter verifies the SCAN pattern limits which keys are tracked.
func TestPatternFilter(t *testing.T) {
	mr := miniredis.RunT(t)

	w := newWatcher(mr, Config{Pattern: "session:*"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(1200 * time.Millisecond)

	mr.Set("other:key", "ignored")
	mr.Set("session:abc", "user1")

	got := collect(w.Events(), 1500*time.Millisecond)

	if len(got) != 1 {
		t.Fatalf("expected 1 event (only session:*), got %d: %+v", len(got), got)
	}
	if got[0].Detail["key"] != "session:abc" {
		t.Errorf("expected session:abc, got %v", got[0].Detail["key"])
	}
}

// TestConnectFailure verifies that a bad address emits EventError and sets
// the status to StatusError.
func TestConnectFailure(t *testing.T) {
	w := &RedisWatcher{
		name:     "test",
		cfg:      Config{Addr: "localhost:1", Pattern: "*", PollIntervalS: 1, MaxEvents: 50},
		eventsCh: make(chan events.WatchEvent, 64),
		status:   watcher.StatusConnecting,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got := collect(w.Events(), 3*time.Second)

	if len(got) == 0 {
		t.Fatal("expected at least one EventError, got none")
	}
	if got[0].Type != events.EventError {
		t.Errorf("expected EventError, got %s", got[0].Type)
	}
	if w.Status() != watcher.StatusError && w.Status() != watcher.StatusStopped {
		t.Errorf("expected StatusError or StatusStopped, got %s", w.Status())
	}
}
