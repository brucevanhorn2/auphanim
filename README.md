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
  - [System metrics watcher](#system-metrics-watcher)
- [Key bindings](#key-bindings)
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

### Homebrew (macOS and Linux)

```bash
brew tap brucevanhorn2/tap
brew install auphanim
```

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

## Key bindings

| Key | Action |
|---|---|
| `Tab` / `j` / `↓` | Move focus to the next panel |
| `Shift+Tab` / `k` / `↑` | Move focus to the previous panel |
| `Enter` | Open detail overlay for the last event in the focused panel |
| `Esc` / `q` / `Enter` | Close the detail overlay |
| `c` / `C` | Clear all event buffers |
| `q` / `Ctrl+C` | Quit |

The detail overlay shows the full structured data for the most recent event, with syntax-highlighted JSON.

---

## CLI reference

```
Usage:
  auphanim [flags]

Flags:
  -c, --config string   Config file path
                        (default search: ./auphanim.json, ~/.config/auphanim/config.json)
      --init            Write auphanim.json.example to the current directory and exit
  -v, --version         Print version and exit
  -h, --help            Help
```

### `--init`

Writes a commented example config to `./auphanim.json.example` demonstrating all three watcher types. Copy it to `auphanim.json` and fill in your values.

```bash
./auphanim --init
cp auphanim.json.example auphanim.json
$EDITOR auphanim.json
```

---

## Contributing

### Development setup

```bash
git clone https://github.com/brucevanhorn2/auphanim
cd auphanim
go mod download

# Run unit tests (no external services required)
go test ./internal/config/... ./internal/watcher/filesystem/...

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
