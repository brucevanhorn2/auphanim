// Package mcp implements a minimal Model Context Protocol (MCP) server over
// stdio for auphanim.
//
// Transport: newline-delimited JSON-RPC 2.0 (read from stdin, write to stdout).
// Protocol version: "2024-11-05"
//
// Exposed tools:
//
//   - get_summary   — per-panel event/error counts
//   - get_events    — list events with optional filters
//   - get_errors    — shorthand for get_events filtered to ERROR type
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"auphanim/internal/store"
)

const protocolVersion = "2024-11-05"

// ── JSON-RPC 2.0 types ────────────────────────────────────────────────────────

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"` // int, string, or nil (notification)
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Standard JSON-RPC error codes.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// ── MCP protocol types ────────────────────────────────────────────────────────

type serverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type capabilities struct {
	Tools map[string]any `json:"tools"`
}

type initResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    capabilities `json:"capabilities"`
	ServerInfo      serverInfo   `json:"serverInfo"`
}

type toolDef struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	InputSchema schema  `json:"inputSchema"`
}

type schema struct {
	Type       string              `json:"type"`
	Properties map[string]schemaProp `json:"properties,omitempty"`
	Required   []string            `json:"required,omitempty"`
}

type schemaProp struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolResult struct {
	Content []textContent `json:"content"`
}

// ── Server ────────────────────────────────────────────────────────────────────

// Server is a minimal MCP stdio server that exposes the event store.
type Server struct {
	st      *store.Store
	version string
	in      io.Reader
	out     io.Writer
}

// New returns a new Server. version is the auphanim application version string.
func New(st *store.Store, version string, in io.Reader, out io.Writer) *Server {
	return &Server{st: st, version: version, in: in, out: out}
}

// Run reads JSON-RPC requests from s.in and writes responses to s.out until
// the reader is exhausted or ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	scanner := bufio.NewScanner(s.in)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024) // 4 MB max line
	enc := json.NewEncoder(s.out)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("stdin read: %w", err)
			}
			return nil // EOF — client disconnected
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			_ = enc.Encode(response{
				JSONRPC: "2.0",
				Error:   &rpcError{Code: codeParseError, Message: "parse error: " + err.Error()},
			})
			continue
		}

		// Notifications have no id — dispatch but don't respond.
		if req.ID == nil {
			s.handleNotification(req.Method)
			continue
		}

		result, rErr := s.dispatch(ctx, req.Method, req.Params)
		resp := response{JSONRPC: "2.0", ID: req.ID}
		if rErr != nil {
			resp.Error = rErr
		} else {
			resp.Result = result
		}
		if err := enc.Encode(resp); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
	}
}

func (s *Server) handleNotification(method string) {
	// "notifications/initialized" — nothing to do.
	_ = method
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *rpcError) {
	switch method {
	case "initialize":
		return s.handleInitialize(params)
	case "tools/list":
		return s.handleToolsList()
	case "tools/call":
		return s.handleToolsCall(ctx, params)
	case "ping":
		return map[string]any{}, nil
	default:
		return nil, &rpcError{Code: codeMethodNotFound, Message: "method not found: " + method}
	}
}

// ── initialize ────────────────────────────────────────────────────────────────

func (s *Server) handleInitialize(_ json.RawMessage) (any, *rpcError) {
	return initResult{
		ProtocolVersion: protocolVersion,
		Capabilities:    capabilities{Tools: map[string]any{}},
		ServerInfo:      serverInfo{Name: "auphanim", Version: s.version},
	}, nil
}

// ── tools/list ────────────────────────────────────────────────────────────────

var tools = []toolDef{
	{
		Name:        "get_summary",
		Description: "Return per-panel event and error counts from the auphanim event store. Use this to quickly understand which panels are active and how many errors have occurred.",
		InputSchema: schema{
			Type: "object",
			Properties: map[string]schemaProp{
				"since": {
					Type:        "string",
					Description: "Only include events newer than this duration (e.g. '5m', '1h', '30s'). Omit for all time.",
				},
			},
		},
	},
	{
		Name:        "get_events",
		Description: "List recent events from the auphanim event store. Returns formatted event rows with timestamp, type, panel, and summary. Use after running tests to see what changed.",
		InputSchema: schema{
			Type: "object",
			Properties: map[string]schemaProp{
				"panel": {
					Type:        "string",
					Description: "Filter to a specific panel name (e.g. 'Main DB', 'App Logs'). Omit for all panels.",
				},
				"type": {
					Type:        "string",
					Description: "Filter by event type: INSERT, UPDATE, DELETE, MESSAGE, CREATED, MODIFIED, REMOVED, ERROR, METRIC. Omit for all types.",
				},
				"since": {
					Type:        "string",
					Description: "Only include events newer than this duration (e.g. '5m', '1h'). Omit for all time.",
				},
				"limit": {
					Type:        "string",
					Description: "Maximum number of events to return (default 50). Provide as a string, e.g. '20'.",
				},
			},
		},
	},
	{
		Name:        "get_errors",
		Description: "List recent ERROR events from the auphanim event store. Shorthand for get_events with type=ERROR. Use when you want to see what went wrong during a test run.",
		InputSchema: schema{
			Type: "object",
			Properties: map[string]schemaProp{
				"panel": {
					Type:        "string",
					Description: "Filter to a specific panel name. Omit for all panels.",
				},
				"since": {
					Type:        "string",
					Description: "Only include errors newer than this duration (e.g. '5m', '1h'). Omit for all time.",
				},
				"limit": {
					Type:        "string",
					Description: "Maximum number of errors to return (default 50).",
				},
			},
		},
	},
}

func (s *Server) handleToolsList() (any, *rpcError) {
	return map[string]any{"tools": tools}, nil
}

// ── tools/call ────────────────────────────────────────────────────────────────

type callParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func (s *Server) handleToolsCall(ctx context.Context, raw json.RawMessage) (any, *rpcError) {
	var p callParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &rpcError{Code: codeInvalidParams, Message: err.Error()}
	}

	var text string
	var err error

	switch p.Name {
	case "get_summary":
		text, err = s.callGetSummary(ctx, p.Arguments)
	case "get_events":
		text, err = s.callGetEvents(ctx, p.Arguments)
	case "get_errors":
		text, err = s.callGetErrors(ctx, p.Arguments)
	default:
		return nil, &rpcError{Code: codeInvalidParams, Message: "unknown tool: " + p.Name}
	}

	if err != nil {
		return nil, &rpcError{Code: -1, Message: err.Error()}
	}

	return toolResult{Content: []textContent{{Type: "text", Text: text}}}, nil
}

// ── Tool implementations ──────────────────────────────────────────────────────

func (s *Server) callGetSummary(ctx context.Context, args map[string]any) (string, error) {
	since, err := parseSince(args)
	if err != nil {
		return "", err
	}

	sums, err := s.st.Summarize(ctx, since)
	if err != nil {
		return "", err
	}

	if len(sums) == 0 {
		hint := ""
		if since > 0 {
			hint = " in the last " + args["since"].(string)
		}
		return "No events recorded" + hint + ".", nil
	}

	var sb strings.Builder
	sb.WriteString("PANEL                 EVENTS  ERRORS  LAST ERROR\n")
	sb.WriteString(strings.Repeat("─", 72) + "\n")
	for _, ps := range sums {
		lastErr := "—"
		if ps.ErrorCount > 0 && ps.LastError != "" {
			lastErr = ps.LastError
			if !ps.LastErrorAt.IsZero() {
				lastErr += fmt.Sprintf(" (%s ago)", formatAge(time.Since(ps.LastErrorAt)))
			}
			if len(lastErr) > 35 {
				lastErr = lastErr[:32] + "…"
			}
		}
		sb.WriteString(fmt.Sprintf("%-22s%-8d%-8d%s\n",
			truncate(ps.Panel, 22), ps.TotalCount, ps.ErrorCount, lastErr))
	}
	return sb.String(), nil
}

func (s *Server) callGetEvents(ctx context.Context, args map[string]any) (string, error) {
	opts, err := buildQueryOpts(args)
	if err != nil {
		return "", err
	}

	rows, err := s.st.Query(ctx, opts)
	if err != nil {
		return "", err
	}

	return formatEvents(rows, sinceStr(args)), nil
}

func (s *Server) callGetErrors(ctx context.Context, args map[string]any) (string, error) {
	opts, err := buildQueryOpts(args)
	if err != nil {
		return "", err
	}
	opts.EventType = "ERROR"

	rows, err := s.st.Query(ctx, opts)
	if err != nil {
		return "", err
	}

	return formatEvents(rows, sinceStr(args)), nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func formatEvents(rows []store.Row, since string) string {
	if len(rows) == 0 {
		hint := ""
		if since != "" {
			hint = " in the last " + since
		}
		return "No events found" + hint + "."
	}

	var sb strings.Builder
	sb.WriteString("TIME      TYPE      PANEL                 SUMMARY\n")
	sb.WriteString(strings.Repeat("─", 72) + "\n")
	for _, r := range rows {
		sb.WriteString(fmt.Sprintf("%-10s%-10s%-22s%s\n",
			r.Timestamp.Local().Format("15:04:05"),
			truncate(r.EventType, 10),
			truncate(r.Panel, 22),
			r.Summary,
		))
	}
	return sb.String()
}

func buildQueryOpts(args map[string]any) (store.QueryOptions, error) {
	opts := store.QueryOptions{Limit: 50}

	if v, ok := args["panel"].(string); ok && v != "" {
		opts.Panel = v
	}
	if v, ok := args["type"].(string); ok && v != "" {
		opts.EventType = v
	}
	since, err := parseSince(args)
	if err != nil {
		return opts, err
	}
	opts.Since = since

	if v, ok := args["limit"].(string); ok && v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			opts.Limit = n
		}
	}

	return opts, nil
}

func parseSince(args map[string]any) (time.Duration, error) {
	v, _ := args["since"].(string)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid since %q: %w", v, err)
	}
	return d, nil
}

func sinceStr(args map[string]any) string {
	v, _ := args["since"].(string)
	return v
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func formatAge(d time.Duration) string {
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}
