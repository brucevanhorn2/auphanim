# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build -o auphanim .

# Run all unit tests (no external services required)
go test ./internal/config/... ./internal/store/... ./internal/api/... ./internal/mcp/... ./internal/watcher/filesystem/... ./internal/watcher/logfile/... ./internal/watcher/redis/...

# Run a single test
go test ./internal/mcp/... -run TestGetSummaryWithData -v

# Run all tests in a package
go test ./internal/store/... -v

# Vet
go vet ./...

# Run with demo config
./auphanim --config demo/auphanim.json

# Smoke-test the MCP server
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | ./auphanim mcp --db :memory:

# Query the store (works while auphanim is running or not)
./auphanim query summary --since 1h
./auphanim query errors --since 5m --json | jq .
```

Integration tests (postgres, kafka, redis) require running services and are not part of the standard test run.

## Architecture

### The event pipeline

Every event flows through one path: **watcher goroutine → channel → tapWatcher → SQLite store + TUI panel**.

```
Watcher.Start() → goroutine → events.WatchEvent channel
                                        ↓
                              tapWatcher.pump()  (main.go)
                                 ↙          ↘
                         store.Insert()    TUI channel
                                               ↓
                                       AppModel.Update()
                                               ↓
                                       PanelModel.AddEvent()
```

`tapWatcher` (in `main.go`) is the middleware that wraps every watcher. It intercepts the event channel, writes to SQLite (best-effort, errors dropped), then forwards to its own channel which the TUI reads. The TUI never touches the store directly.

### Adding a new watcher type

This is the most common extension task. The full recipe:

1. Create `internal/watcher/<type>/watcher.go` implementing `watcher.Watcher` (Name, Type, Start, Stop, Events, Status)
2. Register via `init()`: `watcher.Register("<type>", factory)`
3. Add blank import in `main.go`: `_ "auphanim/internal/watcher/<type>"`
4. No other files need to change — the TUI, store, API, and MCP server all work automatically

The `sysmetrics` watcher is the best reference implementation; `logfile` is the simplest. All watchers must close their `eventsCh` when they stop (signals TUI that the watcher is done).

### TUI architecture (Bubble Tea / Elm)

`internal/ui/app.go` — root `AppModel`. Owns the list of watchers, the `map[string]*PanelModel`, viewport state, column layout, filter text, and flash messages.

`internal/ui/panel.go` — `PanelModel` per watcher. Holds a ring buffer of `WatchEvent`s (capped at `maxEvents`). The sysmetrics panel has a parallel `[]MetricSample` slice for sparkline history. All rendering happens in `View(width, height, focused, status, filter)`.

**Bubble Tea command loop**: `waitForEvent(w)` is a blocking `tea.Cmd` that reads one event from `w.Events()`. After each event is handled in `Update()`, the command is re-issued for that watcher. One goroutine per watcher channel, managed by Bubble Tea.

**Column layout**: `autoColumns(width)` returns 1/2/3 columns based on terminal width (<120 / ≥120 / ≥240). Override with `+`/`-` keys. Panels are sliced into column groups and joined with `lipgloss.JoinHorizontal`.

**Viewport**: when panels outnumber rows, `viewportStart` tracks which panel index is at the top. `ensureViewportValid()` shifts the viewport to keep the focused panel visible.

### Store (`internal/store/store.go`)

Pure-Go SQLite via `modernc.org/sqlite` (no CGo, single binary). Schema: `events(id, ts, panel, wtype, etype, summary, detail)`. WAL journal mode for concurrent reads alongside writes. The store is the source of truth for the HTTP API, query subcommand, and MCP server — the TUI ring buffer is ephemeral.

Key methods: `Insert`, `Query(QueryOptions)`, `Summarize(since)`, `Prune(age)`.

### Query interfaces

All three read from the same SQLite file and can run concurrently:

| Interface | How | When |
|---|---|---|
| `auphanim query` | Opens DB directly | Any time (live or not) |
| HTTP API `localhost:7391` | `internal/api/server.go` | While TUI is running |
| `auphanim mcp` | stdio JSON-RPC 2.0 | MCP client (Claude Code, Copilot) |

### MCP server (`internal/mcp/server.go`)

Zero external dependencies. Raw JSON-RPC 2.0 over newline-delimited stdin/stdout. Protocol version `"2024-11-05"`. Three tools: `get_summary`, `get_events`, `get_errors`. The `dispatch()` method routes by method name; `handleToolsCall()` routes by tool name.

### Version and release

Version is stamped at two places: `var version = "0.3.0"` in `main.go` (used at runtime) and the git tag `v0.3.0` (used by goreleaser to inject via `-X main.version={{.Version}}`). Bump both together. Pushing a `v*` tag triggers the GitHub Actions `Release` workflow which runs goreleaser, publishes binaries, and updates the Homebrew tap (`brucevanhorn2/homebrew-tap`) using `TAP_GITHUB_TOKEN` (stored as a repo secret).

### `--db` and `--retain-days` are PersistentFlags

They are inherited by all subcommands (`query`, `mcp`). Other root flags (`--config`, `--api-port`, `--watch`, `--init`) are local to the root command only.
