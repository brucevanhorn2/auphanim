package store

import (
	"context"
	"testing"
	"time"

	"auphanim/internal/events"
)

func openMemory(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func evt(panel, etype, summary string, ago time.Duration) events.WatchEvent {
	return events.WatchEvent{
		Timestamp:   time.Now().Add(-ago),
		WatcherName: panel,
		WatcherType: "test",
		Type:        events.EventType(etype),
		Summary:     summary,
		Detail:      map[string]any{"k": "v"},
	}
}

func TestInsertAndQuery(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	if err := s.Insert(evt("App Logs", "MESSAGE", "hello world", 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(evt("App Logs", "ERROR", "db timeout", 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(evt("Main DB", "INSERT", "users +1", 0)); err != nil {
		t.Fatal(err)
	}

	rows, err := s.Query(ctx, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

func TestQueryFilterByPanel(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	_ = s.Insert(evt("App Logs", "MESSAGE", "log line", 0))
	_ = s.Insert(evt("Main DB", "INSERT", "row inserted", 0))

	rows, err := s.Query(ctx, QueryOptions{Panel: "App Logs"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Panel != "App Logs" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestQueryFilterByType(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	_ = s.Insert(evt("App Logs", "MESSAGE", "info", 0))
	_ = s.Insert(evt("App Logs", "ERROR", "boom", 0))
	_ = s.Insert(evt("App Logs", "ERROR", "boom again", 0))

	rows, err := s.Query(ctx, QueryOptions{EventType: "ERROR"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 errors, got %d", len(rows))
	}
}

func TestQueryFilterBySince(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	_ = s.Insert(evt("App Logs", "MESSAGE", "old", 10*time.Minute))
	_ = s.Insert(evt("App Logs", "MESSAGE", "recent", 30*time.Second))

	rows, err := s.Query(ctx, QueryOptions{Since: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Summary != "recent" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestQueryNewestFirst(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	_ = s.Insert(evt("App Logs", "MESSAGE", "first", 5*time.Second))
	_ = s.Insert(evt("App Logs", "MESSAGE", "second", 0))

	rows, err := s.Query(ctx, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Summary != "second" {
		t.Fatalf("expected newest first, got %q", rows[0].Summary)
	}
}

func TestSummarize(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	_ = s.Insert(evt("App Logs", "MESSAGE", "line 1", 0))
	_ = s.Insert(evt("App Logs", "MESSAGE", "line 2", 0))
	_ = s.Insert(evt("App Logs", "ERROR", "db timeout", 0))
	_ = s.Insert(evt("Main DB", "INSERT", "row", 0))

	sums, err := s.Summarize(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	byPanel := map[string]PanelSummary{}
	for _, ps := range sums {
		byPanel[ps.Panel] = ps
	}

	if byPanel["App Logs"].TotalCount != 3 {
		t.Errorf("App Logs total: want 3, got %d", byPanel["App Logs"].TotalCount)
	}
	if byPanel["App Logs"].ErrorCount != 1 {
		t.Errorf("App Logs errors: want 1, got %d", byPanel["App Logs"].ErrorCount)
	}
	if byPanel["App Logs"].LastError != "db timeout" {
		t.Errorf("App Logs last error: want %q, got %q", "db timeout", byPanel["App Logs"].LastError)
	}
	if byPanel["Main DB"].TotalCount != 1 {
		t.Errorf("Main DB total: want 1, got %d", byPanel["Main DB"].TotalCount)
	}
}

func TestPrune(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	_ = s.Insert(evt("App Logs", "MESSAGE", "old", 2*time.Hour))
	_ = s.Insert(evt("App Logs", "MESSAGE", "recent", 0))

	deleted, err := s.Prune(1 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("want 1 deleted, got %d", deleted)
	}

	rows, err := s.Query(ctx, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Summary != "recent" {
		t.Fatalf("unexpected rows after prune: %+v", rows)
	}
}

func TestDetailRoundtrip(t *testing.T) {
	s := openMemory(t)
	ctx := context.Background()

	e := events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: "Main DB",
		WatcherType: "postgres",
		Type:        events.EventInsert,
		Summary:     "users +1",
		Detail:      map[string]any{"table": "users", "count": float64(1)},
	}
	_ = s.Insert(e)

	rows, err := s.Query(ctx, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Detail["table"] != "users" {
		t.Errorf("detail roundtrip: want table=users, got %v", rows[0].Detail)
	}
}
