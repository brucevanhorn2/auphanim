package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"auphanim/internal/events"
	"auphanim/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func seed(t *testing.T, st *store.Store, panel, etype, summary string, ago time.Duration) {
	t.Helper()
	e := events.WatchEvent{
		Timestamp:   time.Now().Add(-ago),
		WatcherName: panel,
		WatcherType: "test",
		Type:        events.EventType(etype),
		Summary:     summary,
		Detail:      map[string]any{},
	}
	if err := st.Insert(e); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	st := openStore(t)
	srv := NewServer(st, 0) // port 0 — we use httptest, so port doesn't matter
	return srv, st
}

func get(t *testing.T, srv *Server, path string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(w, req)
	return w.Result()
}

func decodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := get(t, srv, "/api/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	body := decodeJSON(t, resp)
	if body["status"] != "ok" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestEventsEmpty(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := get(t, srv, "/api/events")
	body := decodeJSON(t, resp)
	evts := body["events"].([]any)
	if len(evts) != 0 {
		t.Fatalf("expected empty, got %d events", len(evts))
	}
}

func TestEventsReturnsAll(t *testing.T) {
	srv, st := newTestServer(t)
	seed(t, st, "App Logs", "MESSAGE", "hello", 0)
	seed(t, st, "Main DB", "INSERT", "row added", 0)

	resp := get(t, srv, "/api/events")
	body := decodeJSON(t, resp)
	evts := body["events"].([]any)
	if len(evts) != 2 {
		t.Fatalf("want 2, got %d", len(evts))
	}
	if body["count"].(float64) != 2 {
		t.Fatalf("count mismatch: %v", body["count"])
	}
}

func TestEventsFilterByType(t *testing.T) {
	srv, st := newTestServer(t)
	seed(t, st, "App Logs", "MESSAGE", "info line", 0)
	seed(t, st, "App Logs", "ERROR", "db timeout", 0)

	resp := get(t, srv, "/api/events?type=ERROR")
	body := decodeJSON(t, resp)
	evts := body["events"].([]any)
	if len(evts) != 1 {
		t.Fatalf("want 1 error, got %d", len(evts))
	}
}

func TestEventsFilterByPanel(t *testing.T) {
	srv, st := newTestServer(t)
	seed(t, st, "App Logs", "MESSAGE", "log line", 0)
	seed(t, st, "Main DB", "INSERT", "row", 0)

	resp := get(t, srv, "/api/events?panel=Main+DB")
	body := decodeJSON(t, resp)
	evts := body["events"].([]any)
	if len(evts) != 1 {
		t.Fatalf("want 1 event, got %d", len(evts))
	}
	row := evts[0].(map[string]any)
	if row["panel"] != "Main DB" {
		t.Fatalf("wrong panel: %v", row["panel"])
	}
}

func TestEventsFilterBySince(t *testing.T) {
	srv, st := newTestServer(t)
	seed(t, st, "App Logs", "MESSAGE", "old", 10*time.Minute)
	seed(t, st, "App Logs", "MESSAGE", "recent", 30*time.Second)

	resp := get(t, srv, "/api/events?since=2m")
	body := decodeJSON(t, resp)
	evts := body["events"].([]any)
	if len(evts) != 1 {
		t.Fatalf("want 1 recent event, got %d", len(evts))
	}
}

func TestSummary(t *testing.T) {
	srv, st := newTestServer(t)
	seed(t, st, "App Logs", "MESSAGE", "line", 0)
	seed(t, st, "App Logs", "ERROR", "boom", 0)
	seed(t, st, "Main DB", "INSERT", "row", 0)

	resp := get(t, srv, "/api/summary")
	body := decodeJSON(t, resp)
	panels := body["panels"].([]any)
	if len(panels) != 2 {
		t.Fatalf("want 2 panels, got %d", len(panels))
	}
	byPanel := map[string]map[string]any{}
	for _, p := range panels {
		pm := p.(map[string]any)
		byPanel[pm["panel"].(string)] = pm
	}
	if byPanel["App Logs"]["errors"].(float64) != 1 {
		t.Fatalf("App Logs errors: want 1, got %v", byPanel["App Logs"]["errors"])
	}
	if byPanel["App Logs"]["last_error"] != "boom" {
		t.Fatalf("App Logs last_error: want boom, got %v", byPanel["App Logs"]["last_error"])
	}
	if byPanel["Main DB"]["errors"].(float64) != 0 {
		t.Fatalf("Main DB errors: want 0, got %v", byPanel["Main DB"]["errors"])
	}
}

func TestInvalidSince(t *testing.T) {
	srv, _ := newTestServer(t)
	resp := get(t, srv, "/api/events?since=notaduration")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/events", nil)
	w := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Result().StatusCode)
	}
}
