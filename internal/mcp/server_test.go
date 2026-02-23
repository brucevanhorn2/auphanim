package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"auphanim/internal/events"
	"auphanim/internal/store"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func seed(t *testing.T, st *store.Store, panel, etype, summary string) {
	t.Helper()
	if err := st.Insert(events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: panel,
		WatcherType: "test",
		Type:        events.EventType(etype),
		Summary:     summary,
		Detail:      map[string]any{},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

// rpc sends a single JSON-RPC request to the server and returns the decoded response.
func rpc(t *testing.T, srv *Server, method string, params any) map[string]any {
	t.Helper()

	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
	}
	if params != nil {
		req["params"] = params
	}
	reqBytes, _ := json.Marshal(req)

	var out bytes.Buffer
	srv.out = &out

	// Feed one line to the server.
	in := bytes.NewReader(append(reqBytes, '\n'))
	srv.in = in

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run returns after EOF (one line read).
	if err := srv.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var resp map[string]any
	if err := json.NewDecoder(&out).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v\nraw: %s", err, out.String())
	}
	return resp
}

func newSrv(st *store.Store) *Server {
	return New(st, "test", strings.NewReader(""), &bytes.Buffer{})
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestInitialize(t *testing.T) {
	srv := newSrv(openStore(t))
	resp := rpc(t, srv, "initialize", map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "1"},
	})
	if resp["error"] != nil {
		t.Fatalf("got error: %v", resp["error"])
	}
	result := resp["result"].(map[string]any)
	if result["protocolVersion"] != protocolVersion {
		t.Fatalf("want %s, got %v", protocolVersion, result["protocolVersion"])
	}
	info := result["serverInfo"].(map[string]any)
	if info["name"] != "auphanim" {
		t.Fatalf("unexpected serverInfo: %v", info)
	}
}

func TestToolsList(t *testing.T) {
	srv := newSrv(openStore(t))
	resp := rpc(t, srv, "tools/list", nil)
	result := resp["result"].(map[string]any)
	toolList := result["tools"].([]any)
	names := map[string]bool{}
	for _, raw := range toolList {
		tool := raw.(map[string]any)
		names[tool["name"].(string)] = true
	}
	for _, want := range []string{"get_summary", "get_events", "get_errors"} {
		if !names[want] {
			t.Errorf("tool %q missing from tools/list", want)
		}
	}
}

func TestGetSummaryEmpty(t *testing.T) {
	srv := newSrv(openStore(t))
	resp := rpc(t, srv, "tools/call", map[string]any{
		"name":      "get_summary",
		"arguments": map[string]any{},
	})
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, "No events") {
		t.Fatalf("unexpected text: %q", text)
	}
}

func TestGetSummaryWithData(t *testing.T) {
	st := openStore(t)
	seed(t, st, "App Logs", "MESSAGE", "started")
	seed(t, st, "App Logs", "ERROR", "boom")
	seed(t, st, "Main DB", "INSERT", "row added")

	srv := newSrv(st)
	resp := rpc(t, srv, "tools/call", map[string]any{
		"name":      "get_summary",
		"arguments": map[string]any{},
	})
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)

	if !strings.Contains(text, "App Logs") {
		t.Errorf("missing App Logs in summary:\n%s", text)
	}
	if !strings.Contains(text, "Main DB") {
		t.Errorf("missing Main DB in summary:\n%s", text)
	}
}

func TestGetEvents(t *testing.T) {
	st := openStore(t)
	seed(t, st, "App Logs", "MESSAGE", "hello world")
	seed(t, st, "Main DB", "INSERT", "row")

	srv := newSrv(st)
	resp := rpc(t, srv, "tools/call", map[string]any{
		"name":      "get_events",
		"arguments": map[string]any{},
	})
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)

	if !strings.Contains(text, "hello world") {
		t.Errorf("missing event summary in get_events:\n%s", text)
	}
}

func TestGetErrors(t *testing.T) {
	st := openStore(t)
	seed(t, st, "App Logs", "MESSAGE", "normal line")
	seed(t, st, "App Logs", "ERROR", "db timeout")

	srv := newSrv(st)
	resp := rpc(t, srv, "tools/call", map[string]any{
		"name":      "get_errors",
		"arguments": map[string]any{},
	})
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)

	if !strings.Contains(text, "db timeout") {
		t.Errorf("expected error event in output:\n%s", text)
	}
	if strings.Contains(text, "normal line") {
		t.Errorf("non-error event should not appear in get_errors:\n%s", text)
	}
}

func TestGetEventsPanelFilter(t *testing.T) {
	st := openStore(t)
	seed(t, st, "App Logs", "MESSAGE", "log line")
	seed(t, st, "Main DB", "INSERT", "db row")

	srv := newSrv(st)
	resp := rpc(t, srv, "tools/call", map[string]any{
		"name":      "get_events",
		"arguments": map[string]any{"panel": "Main DB"},
	})
	result := resp["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)

	if strings.Contains(text, "log line") {
		t.Errorf("App Logs event should be filtered out:\n%s", text)
	}
	if !strings.Contains(text, "db row") {
		t.Errorf("Main DB event missing:\n%s", text)
	}
}

func TestUnknownMethod(t *testing.T) {
	srv := newSrv(openStore(t))
	resp := rpc(t, srv, "nonexistent/method", nil)
	if resp["error"] == nil {
		t.Fatal("expected error for unknown method, got none")
	}
	rpcErr := resp["error"].(map[string]any)
	if rpcErr["code"].(float64) != codeMethodNotFound {
		t.Fatalf("expected method-not-found error, got: %v", rpcErr)
	}
}

func TestPing(t *testing.T) {
	srv := newSrv(openStore(t))
	resp := rpc(t, srv, "ping", nil)
	if resp["error"] != nil {
		t.Fatalf("ping returned error: %v", resp["error"])
	}
}
