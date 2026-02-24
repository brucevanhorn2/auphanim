# Auphanim

A TUI developer tool for watching system-wide changes while testing full-stack software. Fire a test request, then immediately see which database rows changed, which Kafka messages were produced, and which files appeared — all in one terminal window.

Named after the [Ophanim](https://en.wikipedia.org/wiki/Ophanim), the many-eyed angels of Ezekiel's vision.

![Auphanim screenshot](assets/screenshot.png)

---

## Contents

- [Why](#why)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Demo](#demo)
- [Configuration](#configuration)
  - [Environment variable interpolation](#environment-variable-interpolation)
  - [PostgreSQL watcher](#postgresql-watcher)
  - [Kafka watcher](#kafka-watcher)
  - [Filesystem watcher](#filesystem-watcher)
  - [Redis watcher](#redis-watcher)
  - [System metrics watcher](#system-metrics-watcher)
  - [Log file watcher](#log-file-watcher)
- [Key bindings](#key-bindings)
- [Querying events](#querying-events)
  - [HTTP API](#http-api)
  - [query subcommand](#query-subcommand)
  - [MCP server (AI agents)](#mcp-server-ai-agents)
- [CLI reference](#cli-reference)
- [Contributing](#contributing)
  - [Adding a new watcher type](#adding-a-new-watcher-type)
  - [Development setup](#development-setup)

---

## Why

When testing a feature end-to-end you constantly context-switch between terminal tabs: one for `psql`, one for a Kafka consumer, one for `tail -f` on a log directory. Auphanim collapses all of that into a single, always-on terminal panel that you leave running next to your test runner.

**Typical workflow:**

1. Start auphanim pointing at your dev stack.
2. Run your test (curl, Postman, pytest, etc.).
3. Watch every side-effect surface instantly — rows, messages, files — without switching windows.

---

## Installation

### From source

**Prerequisites:** Go 1.22 or later.

```bash
# Clone and build
git clone https://github.com/brucevanhorn2/auphanim
cd auphanim
go build -o auphanim .

# Optional: install to $GOPATH/bin
go install .
```

### Pre-built binaries

Download the latest release for your platform from the [Releases page](https://github.com/brucevanhorn2/auphanim/releases), extract the archive, and place the `auphanim` binary somewhere on your `$PATH`.

The result is a single static binary with no runtime dependencies.

---

## Quick start

**1. Generate an example config:**

```bash
./auphanim --init
# Writes auphanim.json.example to the current directory
cp auphanim.json.example auphanim.json
```

**2. Edit `auphanim.json`** to point at your services (see [Configuration](#configuration)).

**3. Set any environment variables** referenced in the config:

```bash
export DB_USER=dev DB_PASSWORD=dev
export KAFKA_BROKERS=localhost:9092
export UPLOAD_DIR=/tmp/uploads
```

**4. Run:**

```bash
./auphanim
# or specify a config file explicitly:
./auphanim --config /path/to/auphanim.json
```

Auphanim searches for a config file in this order when `--config` is not given:
1. `./auphanim.json`
2. `~/.config/auphanim/config.json`

---

## Demo

The `demo/` directory contains a self-contained Docker Compose environment and a traffic generator so you can try auphanim without an existing stack.

**Start the services:**

```bash
cd demo
docker compose up
```

This starts PostgreSQL 16 and Kafka 3.7 (KRaft, no Zookeeper) on their standard ports.

**Run the traffic generator** (in a second terminal):

```bash
go run ./demo/gen
```

The generator creates `users` and `orders` tables, then continuously inserts, updates, and deletes rows, publishes events to Kafka, and writes receipt JSON files to `/tmp/auphanim-demo/`. It waits automatically for the database to be ready.

**Run auphanim** (in a third terminal):

```bash
./auphanim --config demo/auphanim.json
```

You should immediately see all three panels filling with events.

**Tear down:**

```bash
docker compose down -v   # -v removes the data volumes
```

---

## Configuration

The config file is a JSON object with a single `"watchers"` array. Each entry has a `"name"`, a `"type"`, and type-specific fields.

```json
{
  "watchers": [
    {
      "name": "My display name",
      "type": "postgres | kafka | filesystem",
      ...type-specific fields...
    }
  ]
}
```

`"name"` is the label shown in the panel header. It must be unique across all watchers.

---

### Environment variable interpolation

Any string value in the config may contain `${VAR_NAME}` tokens. Auphanim replaces them with the corresponding environment variable at startup and **fails immediately** if any referenced variable is unset. Variable values are JSON-string-escaped automatically, so values containing quotes or backslashes are safe.

```json
{
  "dsn": "postgresql://${DB_USER}:${DB_PASSWORD}@localhost:5432/mydb"
}
```

```bash
export DB_USER=alice
export DB_PASSWORD=hunter2
```

---

### PostgreSQL watcher

**Type string:** `"postgres"`

Watches one or more tables in a PostgreSQL database. Two modes are available.

```json
{
  "name": "Main DB",
  "type": "postgres",
  "dsn": "postgresql://${DB_USER}:${DB_PASSWORD}@localhost:5432/mydb",
  "tables": ["users", "orders"],
  "mode": "lightweight",
  "poll_interval_s": 3,
  "max_events": 100
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `dsn` | string | **required** | PostgreSQL connection string. Supports all pgx/libpq formats. |
| `tables` | array of strings | **required** | Tables to watch. Names must be alphanumeric + underscore. |
| `mode` | string | `"lightweight"` | `"lightweight"` or `"full_detail"` — see below. |
| `poll_interval_s` | integer | `3` | Polling interval in seconds (lightweight mode only). |
| `max_events` | integer | `100` | Maximum events to keep in the panel ring buffer. |

#### Mode: `lightweight` (default, non-invasive)

Polls `pg_stat_user_tables` every `poll_interval_s` seconds and reports the delta in `n_tup_ins`, `n_tup_upd`, and `n_tup_del` for each watched table. **Requires no schema changes.** Shows row-count summaries, not the actual row data.

Use this mode when you cannot or do not want to modify the database schema.

> **⚠️ About `n_tup_ins`, `n_tup_upd`, `n_tup_del` counters:**
> 
> These PostgreSQL counters in `pg_stat_user_tables` measure **tuple operations at the storage engine level**, not logical row inserts. This means:
> - A single INSERT statement may increment the counter by more than 1 if the table has triggers, check constraints, or other operations that cause row rewrites
> - Table scans, HOT (Heap Only Tuple) updates, and prepared statement re-planning can sometimes increment these counters due to how PostgreSQL's query planner works
> - If you see counts that seem higher than expected, the application is likely performing more database operations than you think (e.g., validation queries, constraint checks, or multiple statements per logical operation)
> 
> **If counts seem wrong:** Switch to `mode: "full_detail"` to see the actual row data and verify what operations are happening. This will help you understand whether auphanim is correctly reporting your application's true database behavior.

#### Mode: `full_detail`

Installs an `AFTER INSERT OR UPDATE OR DELETE` trigger on each watched table. The trigger calls `pg_notify('auphanim_changes', <json_payload>)` with the full row data. Auphanim listens on that channel via a dedicated connection and surfaces the actual `NEW`/`OLD` row values in the detail overlay.

**Setup is automatic and idempotent:** auphanim creates (or replaces) the trigger function and triggers on startup, and drops them on clean shutdown. A hard kill (`kill -9`) leaves the triggers in place — they fire notifications to a channel nobody is listening on, which is harmless. Re-running auphanim replaces them cleanly.

```json
{
  "name": "Main DB",
  "type": "postgres",
  "dsn": "postgresql://${DB_USER}:${DB_PASSWORD}@localhost:5432/mydb",
  "tables": ["users", "orders"],
  "mode": "full_detail"
}
```

> **Note:** `full_detail` mode requires `CREATE FUNCTION` and `CREATE TRIGGER` privileges on the watched tables.

---

### Kafka watcher

**Type string:** `"kafka"`

Consumes one or more Kafka topics and displays each message as it arrives.

```json
{
  "name": "Events",
  "type": "kafka",
  "brokers": ["${KAFKA_BROKERS}"],
  "topics": ["order.created", "order.updated"],
  "group_id": "auphanim-dev",
  "offset": "latest",
  "max_events": 100
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `brokers` | array of strings | **required** | Kafka broker addresses, e.g. `["localhost:9092"]`. Supports multiple for HA. |
| `topics` | array of strings | **required** | Topics to consume. A separate goroutine is started per topic. |
| `group_id` | string | `""` | Consumer group ID. Recommended — enables offset tracking so you pick up where you left off after restart. |
| `offset` | string | `"latest"` | Starting offset when no committed offset exists. `"latest"` (default) or `"earliest"`. |
| `max_events` | integer | `100` | Maximum events to keep in the panel ring buffer. |

**`"offset": "latest"`** is the right default for the typical auphanim workflow: start auphanim, run your test, observe. Use `"earliest"` if you want to replay existing messages.

Message values that are valid JSON are pretty-printed in the detail overlay. Non-JSON values are shown as raw strings.

---

### Filesystem watcher

**Type string:** `"filesystem"`

Watches a directory for file-system events using [fsnotify](https://github.com/fsnotify/fsnotify).

```json
{
  "name": "Uploads",
  "type": "filesystem",
  "path": "${UPLOAD_DIR}",
  "recursive": true,
  "patterns": ["*.pdf", "*.jpg", "*.png"],
  "max_events": 100
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | **required** | Directory to watch. Must exist when auphanim starts. |
| `recursive` | boolean | `false` | Watch all subdirectories. New subdirectories created after startup are watched automatically. |
| `patterns` | array of strings | `[]` (all files) | Glob patterns matched against the **filename only** (not the full path). E.g. `["*.pdf", "report_*.csv"]`. If empty, all files are reported. |
| `max_events` | integer | `100` | Maximum events to keep in the panel ring buffer. |

Events reported: `CREATED`, `MODIFIED`, `REMOVED`.

> **Linux note:** fsnotify does not recurse natively on Linux via inotify. When `recursive: true` is set, auphanim walks the directory tree at startup and adds each subdirectory individually.

---

### Redis watcher

**Type string:** `"redis"`

Polls Redis using [`SCAN`](https://redis.io/docs/latest/commands/scan/) — the non-blocking, cursor-based alternative to `KEYS` — and emits events for keys that appear or disappear between polls. The initial scan is **silent**: keys that already exist when auphanim starts are not reported, so a pre-populated instance won't flood the panel.

```json
{
  "name": "Cache",
  "type": "redis",
  "addr": "${REDIS_ADDR}",
  "password": "",
  "db": 0,
  "pattern": "*",
  "poll_interval_s": 3,
  "show_values": true,
  "max_events": 100
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `addr` | string | **required** | Redis server address, e.g. `"localhost:6379"`. |
| `password` | string | `""` | Optional AUTH password. |
| `db` | integer | `0` | Redis database number. |
| `pattern` | string | `"*"` | SCAN glob pattern. Use a prefix like `"session:*"` to watch only a subset of keys. |
| `poll_interval_s` | integer | `3` | How often to run a full SCAN cycle, in seconds. |
| `show_values` | boolean | `false` | When `true`, fetches the value for each new key and includes it in the detail overlay. String values are truncated at 200 characters. Hash, list, set, and sorted set keys show an item count instead. |
| `max_events` | integer | `100` | Maximum events to keep in the panel ring buffer. |

Events reported: `CREATED` (new key detected), `REMOVED` (key deleted or expired).

> **Why SCAN instead of keyspace notifications?** Keyspace notifications require enabling `notify-keyspace-events` in the Redis config, which is often not possible in managed environments. SCAN works against any Redis instance with zero configuration changes.

> **TTL expiry:** When a key expires between two polls, SCAN simply won't return it — auphanim correctly emits `REMOVED` with no special handling needed.

---

### System metrics watcher

**Type string:** `"sysmetrics"`

Polls CPU usage, memory consumption, and network I/O rates from the local machine and displays them as bar graphs with sparkline history — similar to bpytop or btop.

```json
{
  "name": "System",
  "type": "sysmetrics",
  "poll_interval_s": 2,
  "max_events": 100
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_s` | integer | `2` | How often to sample metrics, in seconds. |
| `max_events` | integer | `100` | Maximum samples to keep in the ring buffer (also caps sparkline history at 60). |

The panel renders four rows, each with a proportional bar graph (green → yellow → red as load increases) and a sparkline of recent history:

```
 CPU   ████████████░░░░░░░░░░░░░░░░  34.0%  ▁▂▃▄▃▅▄▆▇█
 MEM   ███████░░░░░░░░░░░░░░░░░░░░░  38.5%  6.2/16.0GB
 ↓ NET ░░░░░░░░░░░░░░░░░░░░░░░░░░░░  1.2 KB/s
 ↑ NET ░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0.4 KB/s
```

- **CPU** — aggregate CPU utilization across all cores (`/proc/stat` on Linux).
- **MEM** — used vs. total physical RAM in GB.
- **↓ NET / ↑ NET** — receive/transmit bytes per second aggregated across all network interfaces. The bar scales relative to the peak observed since startup.

Pressing `Enter` on a focused sysmetrics panel opens the standard detail overlay showing the full JSON payload (cpu_pct, mem_pct, mem_used_gb, mem_total_gb, net_recv_bps, net_send_bps).

No external services or configuration beyond `poll_interval_s` are required. The watcher works on Linux and macOS.

---

### Log file watcher

**Type string:** `"logfile"`

Tails one or more log files and emits one event per new line — like `tail -f` but integrated into the auphanim panel. Uses [fsnotify](https://github.com/fsnotify/fsnotify) to detect writes without polling. Handles log rotation by comparing inodes, and detects truncation (e.g. `logrotate` with `copytruncate`).

**Silent start:** Lines present in the file before auphanim starts are not emitted. Only new lines written after startup appear.

```json
{
  "name": "App Logs",
  "type": "logfile",
  "paths": ["${LOG_PATH}", "/var/log/nginx/error.log"],
  "patterns": ["WARN", "ERROR"],
  "error_patterns": ["ERROR", "FATAL", "PANIC", "EXCEPTION"],
  "max_events": 200
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `paths` | array of strings | **required** | One or more log files to tail. Each file is watched independently. |
| `patterns` | array of strings | `[]` (all lines) | If non-empty, only lines containing at least one of these substrings (case-insensitive) are emitted. |
| `error_patterns` | array of strings | `["ERROR","FATAL","PANIC","EXCEPTION","panic:"]` | Lines matching any of these substrings are classified as `EventError` (shown in red) rather than `EventMessage`. Should only contain **actual error keywords**, not log levels (see note below). |
| `max_events` | integer | `100` | Maximum events to keep in the panel ring buffer. |

Events reported: `MESSAGE` (normal line), `ERROR` (line matches an error pattern).

Summary format: `filename.log: <line content>` — the base filename is prepended so you can tell which file a line came from when watching multiple files.

> **⚠️ Common mistake with `error_patterns`:** Don't include generic log level keywords like `"INFO"`, `"DEBUG"`, `"WARN"` (or lowercase variants). These appear in every log line and will cause the entire panel to show as red errors.
>
> ❌ **Bad:** `"error_patterns": ["ERROR", "WARN", "INFO", "debug"]` — every line gets classified as ERROR
>
> ✅ **Good:** `"error_patterns": ["ERROR", "FATAL", "PANIC"]` — only actual errors shown in red
>
> If you want warnings to stand out, add `"WARN"` to `error_patterns`: `"error_patterns": ["ERROR", "FATAL", "PANIC", "WARN"]`. Use `"patterns"` (not `error_patterns`) to **filter** which lines to display at all.

---

## Key bindings

| Key | Action |
|---|---|
| `Tab` / `Shift+Tab` | Move focus to the next / previous panel |
| `j` / `↓` | Scroll focused panel toward newer events |
| `k` / `↑` | Scroll focused panel toward older events |
| `g` | Jump to latest (stop scrolling, follow new events) |
| `/` | Enter filter mode — type to filter events in all panels |
| `Esc` | Clear filter (in normal mode); exit filter input (in filter mode) |
| `+` | Add a column (1 → 2 → 3) |
| `-` | Remove a column (3 → 2 → 1) |
| `=` | Reset columns to auto-detect from terminal width |
| `Enter` | Open detail overlay for the last event in the focused panel |
| `Esc` / `q` / `Enter` | Close the detail overlay |
| `s` / `S` | Save all current events to `auphanim_export_<timestamp>.json` |
| `c` / `C` | Clear all event buffers |
| `q` / `Ctrl+C` | Quit |

**Activity indicators:** The header shows a numbered circle (①②③…) for each configured panel. Circles are dim gray when idle. When a panel receives an event, its circle lights up in the event's colour (green for creates, yellow for updates, red for errors, cyan for messages) for 3 seconds. The focused panel's circle is always purple.

**Viewport:** When more panels are configured than fit vertically, only a subset is shown. Tab scrolls the viewport to keep the focused panel visible. The header shows `[1–4 of 8]` when some panels are off-screen.

**Column layout:** Auphanim auto-detects the number of columns from the terminal width (1 col < 120 chars, 2 cols ≥ 120, 3 cols ≥ 240). Use `+`/`-` to override. `=` returns to auto. The footer shows the current column count or `=` when auto-detect is active.

**Filter mode:** Press `/` to start filtering. The footer shows `Filter: <text>▎`. Events across all panels are filtered to lines whose summary contains the text (case-insensitive). Press `Enter` to confirm or `Esc` to clear and exit. The panel title shows `[matching/total]` when a filter is active.

**Scroll indicator:** When a panel is scrolled up (not following latest), its title shows `↑N` where N is how many lines back from the most recent event.

The detail overlay shows the full structured data for the most recent event, with syntax-highlighted JSON.

---

## Querying events

Every event auphanim captures is persisted to a SQLite database (`auphanim.db` in the working directory by default). The database survives restarts — pick up where you left off after a weekend. Use `--db :memory:` for a session-only store that is discarded on exit.

This makes it easy to ask a coding agent (Claude Code, Copilot, etc.) to check what happened during a test run.

---

### HTTP API

While auphanim is running it serves a local REST API on `http://127.0.0.1:7391` (port configurable with `--api-port`, set to `0` to disable). All responses are JSON. The server binds to localhost only.

| Endpoint | Description |
|---|---|
| `GET /api/health` | Returns `{"status":"ok"}` |
| `GET /api/events` | List events, newest first |
| `GET /api/summary` | Per-panel event and error counts |

**`/api/events` query parameters:**

| Parameter | Example | Description |
|---|---|---|
| `panel` | `App+Logs` | Filter by panel name |
| `type` | `ERROR` | Filter by event type |
| `since` | `5m`, `1h`, `2h30m` | Only events newer than this duration |
| `limit` | `20` | Max rows (default 100) |

**Examples:**

```bash
# Quick overview while auphanim is running:
curl -s localhost:7391/api/summary | jq .

# Errors in the last 5 minutes:
curl -s "localhost:7391/api/events?type=ERROR&since=5m" | jq '.events[].summary'

# What did the DB panel see in the last hour?
curl -s "localhost:7391/api/events?panel=Main+DB&since=1h&limit=20" | jq .
```

---

### query subcommand

`auphanim query` opens the SQLite database directly — **it works whether auphanim is currently running or not**. SQLite WAL mode allows reads and writes to happen concurrently, so you can query a live database without blocking event ingestion.

```
auphanim query summary [--since DURATION] [--json]
auphanim query events  [--panel NAME] [--type TYPE] [--since DURATION] [--limit N] [--json]
auphanim query errors  [--panel NAME] [--since DURATION] [--limit N] [--json]
```

**`query summary`** — the fastest way to see if anything went wrong:

```
$ auphanim query summary --since 1h
PANEL        EVENTS  ERRORS  LAST ERROR
App Logs        247       3  db timeout after 30s (3m25s ago)
Main DB          42       0  —
Kafka Events     18       0  —
```

**`query errors`** — list error events, newest first:

```
$ auphanim query errors --since 10m
TIME      TYPE    PANEL     SUMMARY
14:31:08  ERROR   App Logs  app.log: ERROR connection refused
14:31:01  ERROR   App Logs  app.log: ERROR db timeout after 30s
```

**`query events`** — full event list with optional filters:

```bash
auphanim query events --panel "Main DB" --since 30m
auphanim query events --type INSERT --limit 10
```

**`--json` flag** — machine-readable output for piping to `jq` or passing to an agent:

```bash
auphanim query summary --json | jq '.panels[] | select(.errors > 0)'
auphanim query errors --since 5m --json | jq '.'
```

**Typical agent workflow:**

> You: "Did any errors occur during that last test?"
>
> Claude Code runs: `auphanim query errors --since 5m`
>
> Claude Code sees: two ERROR events from App Logs, reports them to you.

---

### MCP server (AI agents)

`auphanim mcp` starts a [Model Context Protocol](https://modelcontextprotocol.io) server on **stdio**. This lets an AI coding agent — Claude Code, GitHub Copilot, Cursor, or any MCP-aware tool — query the event store interactively during a development session, without you having to copy-paste output or leave the chat.

**Available tools:**

| Tool | Description |
|---|---|
| `get_summary` | Per-panel event and error counts. Optional `since` argument. |
| `get_events` | List events with optional `panel`, `type`, `since`, `limit` filters. |
| `get_errors` | Shorthand for `get_events` filtered to `ERROR` type. |

#### Configure Claude Code

Add to your project's `.claude/settings.json` (or `~/.claude.json` for a global entry):

```json
{
  "mcpServers": {
    "auphanim": {
      "type": "stdio",
      "command": "auphanim",
      "args": ["mcp", "--db", "/path/to/auphanim.db"]
    }
  }
}
```

Replace `/path/to/auphanim.db` with the actual path, or omit `--db` entirely to use `auphanim.db` in the current directory.

#### Usage during a session

Start auphanim in one terminal:

```bash
./auphanim --config auphanim.json
```

Then in Claude Code (or another MCP client), just ask natural-language questions:

> "Did any errors occur in the last 5 minutes?"

> "What database changes happened while I was running the test suite?"

> "Show me all events from the App Logs panel since my last deploy."

The agent calls `get_summary` or `get_events` and returns formatted results directly in the conversation.

#### Manual test (verify the MCP server works)

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}' \
  | auphanim mcp --db auphanim.db
```

You should receive a JSON response with `protocolVersion` and `serverInfo`.

---

## CLI reference

```
Usage:
  auphanim [flags]
  auphanim query (summary|events|errors) [flags]
  auphanim mcp [flags]

Global flags (available to all subcommands):
      --db string         SQLite database file (default "auphanim.db"; use ":memory:" for
                          in-session-only storage that is discarded on exit)
      --retain-days int   Delete events older than this many days on startup and hourly
                          (default 7; 0 = keep forever)

Root command flags:
  -c, --config string     Config file path
                          (default search: ./auphanim.json, ~/.config/auphanim/config.json)
      --api-port int      Port for the local HTTP query API (default 7391; 0 = disabled)
      --init              Write auphanim.json.example to the current directory and exit
  -w, --watch             Reload config automatically when the config file changes on disk
  -v, --version           Print version and exit
  -h, --help              Help

Query subcommand flags:
      --since duration    Only include events newer than this (e.g. 5m, 1h, 2h30m)
      --panel string      Filter by panel name
      --type string       Filter by event type (events subcommand only)
      --limit int         Maximum rows to return (default 50)
      --json              Output raw JSON instead of the formatted table
```

### `--init`

Writes an example config to `./auphanim.json.example` demonstrating all watcher types. Copy it to `auphanim.json` and fill in your values.

```bash
./auphanim --init
cp auphanim.json.example auphanim.json
$EDITOR auphanim.json
```

### `--watch`

Polls the config file for changes once per second. When a change is detected, all watchers are stopped gracefully and the new config is loaded and applied without restarting the TUI. Useful when iterating on which watchers or tables to include.

```bash
./auphanim --watch
# or with an explicit config file:
./auphanim -c /etc/auphanim.json --watch
```

If the reloaded config contains a syntax error or references an unknown watcher type, the error is shown in the footer and the existing watchers keep running.

---

## Contributing

### Development setup

```bash
git clone https://github.com/brucevanhorn2/auphanim
cd auphanim
go mod download

# Run unit tests (no external services required)
go test ./internal/config/... ./internal/store/... ./internal/api/... ./internal/mcp/... ./internal/watcher/filesystem/... ./internal/watcher/logfile/...

# Build
go build -o auphanim .

# Run the demo stack for integration testing
cd demo && docker compose up -d
go run ./demo/gen        # traffic generator
./auphanim -c demo/auphanim.json
```

### Adding a new watcher type

The entire extensibility story lives in `internal/watcher/interface.go`. Adding a new source — Redis pub/sub, HTTP endpoint polling, system metrics, gRPC streams — requires **zero changes to the core architecture**.

**Step 1 — Create your package:**

```
internal/watcher/mytype/watcher.go
```

**Step 2 — Implement the interface:**

```go
package mytype

import (
    "context"
    "sync"
    "auphanim/internal/events"
    "auphanim/internal/watcher"
)

type Config struct {
    // your config fields, tagged with json:"..."
}

type MyWatcher struct {
    name     string
    cfg      Config
    eventsCh chan events.WatchEvent
    status   watcher.Status
    mu       sync.RWMutex
    cancel   context.CancelFunc
}

func (w *MyWatcher) Name() string                        { return w.name }
func (w *MyWatcher) Type() string                        { return "mytype" }
func (w *MyWatcher) Events() <-chan events.WatchEvent    { return w.eventsCh }
func (w *MyWatcher) Status() watcher.Status              { /* read w.status under mu */ }

func (w *MyWatcher) Start(ctx context.Context) error {
    ctx, w.cancel = context.WithCancel(ctx)
    go w.run(ctx)
    return nil
}

func (w *MyWatcher) Stop() {
    if w.cancel != nil { w.cancel() }
}

func (w *MyWatcher) run(ctx context.Context) {
    defer close(w.eventsCh)
    for {
        // block until next event or ctx.Done()
        select {
        case <-ctx.Done():
            return
        default:
            // ... fetch event ...
            w.eventsCh <- events.WatchEvent{
                Timestamp:   time.Now(),
                WatcherName: w.name,
                WatcherType: "mytype",
                Type:        events.EventMessage,
                Summary:     "short one-liner",
                Detail:      map[string]any{"key": "value"},
            }
        }
    }
}
```

**Step 3 — Register via `init()`:**

```go
func init() {
    watcher.Register("mytype", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
        var cfg Config
        if err := json.Unmarshal(raw, &cfg); err != nil {
            return nil, err
        }
        return &MyWatcher{
            name:     name,
            cfg:      cfg,
            eventsCh: make(chan events.WatchEvent, 64),
            status:   watcher.StatusConnecting,
        }, nil
    })
}
```

**Step 4 — Blank-import in `main.go`:**

```go
import (
    _ "auphanim/internal/watcher/mytype"
)
```

**Step 5 — Add a config entry:**

```json
{
  "name": "My Source",
  "type": "mytype",
  "your_field": "value",
  "max_events": 100
}
```

That's it. The panel appears automatically. No changes to the UI, registry, config loader, or any other file.

### Event types

Use the appropriate `events.EventType` constant so the TUI applies the right colour:

| Constant | Colour | When to use |
|---|---|---|
| `events.EventInsert` | Green | Row / record created |
| `events.EventUpdate` | Yellow | Row / record modified |
| `events.EventDelete` | Red | Row / record removed |
| `events.EventCreated` | Green | File or resource created |
| `events.EventModified` | Yellow | File or resource changed |
| `events.EventRemoved` | Red | File or resource deleted |
| `events.EventMessage` | Cyan | Message / notification received |
| `events.EventError` | Bright red | Connection or processing error |
| `events.EventMetric` | — | Reserved for future CPU/RAM/network watchers |

### Testing

- Unit tests for `config` and `filesystem` packages require no external services and should be kept fast.
- Integration tests against real services belong in `internal/watcher/<type>/watcher_integration_test.go` and should be skipped unless a build tag or environment variable is set (e.g. `go test -tags integration ./...`).
- The `demo/` Docker Compose stack is the reference environment for manual end-to-end verification.

### Pull requests

- Keep each PR focused on a single watcher type or feature.
- Add at least one unit test for new config validation logic.
- Run `go vet ./...` and `go test ./...` before opening a PR.
- The `auphanim.json.example` in the repo root is auto-generated by `--init`; if you change the example content, update the `exampleConfig` constant in `main.go`.
