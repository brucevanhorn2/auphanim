// Package api exposes auphanim's event store over a local HTTP API so that
// coding agents (Claude Code, Copilot, etc.) can query what happened during a
// test session without parsing raw files.
//
// Endpoints
//
//	GET /api/health
//	GET /api/events?panel=X&type=ERROR&since=5m&limit=50
//	GET /api/summary?since=1h
//
// All responses are JSON. The server binds to 127.0.0.1 only.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"auphanim/internal/store"
)

// Server is a lightweight HTTP server that fronts the event store.
type Server struct {
	st   *store.Store
	srv  *http.Server
	port int
}

// NewServer wires up the mux and returns a Server ready to Start.
func NewServer(st *store.Store, port int) *Server {
	s := &Server{st: st, port: port}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/summary", s.handleSummary)

	s.srv = &http.Server{
		Addr:         fmt.Sprintf("127.0.0.1:%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s
}

// Start begins serving in a background goroutine.
// Returns an error if the port cannot be bound (e.g. already in use).
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	go s.srv.Serve(ln) //nolint:errcheck
	return nil
}

// Stop performs a graceful shutdown.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}

// Addr returns the address the server is (or will be) listening on.
func (s *Server) Addr() string { return s.srv.Addr }

// ── Handlers ──────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	since, err := parseSince(q.Get("since"))
	if err != nil {
		http.Error(w, fmt.Sprintf("bad since: %v", err), http.StatusBadRequest)
		return
	}

	opts := store.QueryOptions{
		Panel:     q.Get("panel"),
		EventType: q.Get("type"),
		Since:     since,
		Limit:     parseLimit(q.Get("limit"), 100),
	}

	rows, err := s.st.Query(r.Context(), opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type eventRow struct {
		ID        int64          `json:"id"`
		Timestamp string         `json:"ts"`
		Panel     string         `json:"panel"`
		Type      string         `json:"watcher_type"`
		EventType string         `json:"type"`
		Summary   string         `json:"summary"`
		Detail    map[string]any `json:"detail,omitempty"`
	}
	out := make([]eventRow, len(rows))
	for i, r := range rows {
		out[i] = eventRow{
			ID:        r.ID,
			Timestamp: r.Timestamp.UTC().Format(time.RFC3339),
			Panel:     r.Panel,
			Type:      r.WatcherType,
			EventType: r.EventType,
			Summary:   r.Summary,
			Detail:    r.Detail,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events": out,
		"count":  len(out),
		"query": map[string]any{
			"panel": opts.Panel,
			"type":  opts.EventType,
			"since": q.Get("since"),
			"limit": opts.Limit,
		},
	})
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	since, err := parseSince(q.Get("since"))
	if err != nil {
		http.Error(w, fmt.Sprintf("bad since: %v", err), http.StatusBadRequest)
		return
	}

	sums, err := s.st.Summarize(r.Context(), since)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type summaryRow struct {
		Panel       string `json:"panel"`
		Total       int    `json:"total"`
		Errors      int    `json:"errors"`
		LastError   string `json:"last_error,omitempty"`
		LastErrorAt string `json:"last_error_at,omitempty"`
	}
	rows := make([]summaryRow, len(sums))
	for i, ps := range sums {
		sr := summaryRow{
			Panel:  ps.Panel,
			Total:  ps.TotalCount,
			Errors: ps.ErrorCount,
		}
		if ps.ErrorCount > 0 {
			sr.LastError = ps.LastError
			if !ps.LastErrorAt.IsZero() {
				sr.LastErrorAt = ps.LastErrorAt.UTC().Format(time.RFC3339)
			}
		}
		rows[i] = sr
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"panels": rows,
		"since":  q.Get("since"),
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseSince(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func parseLimit(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
