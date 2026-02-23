package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"

	"auphanim/internal/api"
	"auphanim/internal/config"
	"auphanim/internal/events"
	"auphanim/internal/store"
	"auphanim/internal/ui"
	"auphanim/internal/watcher"

	// Blank imports trigger each watcher package's init(), which registers
	// the factory with the global registry.
	_ "auphanim/internal/watcher/filesystem"
	_ "auphanim/internal/watcher/kafka"
	_ "auphanim/internal/watcher/logfile"
	_ "auphanim/internal/watcher/postgres"
	_ "auphanim/internal/watcher/redis"
	_ "auphanim/internal/watcher/sysmetrics"
)

var version = "0.2.0"

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var (
	cfgFile  string
	dbFile   string
	apiPort  int
	initFlag  bool
	watchFlag bool
)

var rootCmd = &cobra.Command{
	Use:   "auphanim",
	Short: "TUI developer tool for watching system-wide changes while testing",
	Long: `Auphanim (named after the Ophanim, many-eyed angels of Ezekiel's vision)
watches databases, message queues, and filesystems simultaneously, showing
all changes in a single terminal window as you fire test requests.`,
	Version: version,
	RunE:    run,
}

func init() {
	rootCmd.Flags().StringVarP(&cfgFile, "config", "c", "",
		"config file (default: ./auphanim.json or ~/.config/auphanim/config.json)")
	rootCmd.Flags().BoolVar(&initFlag, "init", false,
		"write auphanim.json.example to the current directory and exit")
	rootCmd.Flags().BoolVarP(&watchFlag, "watch", "w", false,
		"reload config automatically when the config file changes on disk")
	rootCmd.PersistentFlags().StringVar(&dbFile, "db", "auphanim.db",
		`SQLite database file for event persistence (use ":memory:" for in-session only)`)
	rootCmd.Flags().IntVar(&apiPort, "api-port", 7391,
		"port for the local HTTP query API (0 = disabled)")
}

func run(cmd *cobra.Command, args []string) error {
	if initFlag {
		return writeExample()
	}

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if len(cfg.Watchers) == 0 {
		return fmt.Errorf("no watchers configured in %s", cfgFile)
	}

	// Open the event store.
	st, err := store.Open(dbFile)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Build and start watchers, wrapping each in a tapWatcher that persists
	// every event to the store before forwarding it to the TUI.
	watchers := make([]watcher.Watcher, 0, len(cfg.Watchers))
	for _, wc := range cfg.Watchers {
		w, err := watcher.Create(wc.Type, wc.Name, wc.Config)
		if err != nil {
			return fmt.Errorf("watcher %q: %w", wc.Name, err)
		}
		watchers = append(watchers, newTapWatcher(w, st))
	}

	for _, w := range watchers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("starting watcher %q: %w", w.Name(), err)
		}
	}
	defer func() {
		for _, w := range watchers {
			w.Stop()
		}
	}()

	// Start the HTTP query API if a port is configured.
	if apiPort > 0 {
		srv := api.NewServer(st, apiPort)
		if err := srv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: API server on port %d unavailable: %v\n", apiPort, err)
		} else {
			defer srv.Stop()
		}
	}

	// Resolve cfgFile so the watch goroutine has an absolute path to stat.
	resolvedCfgFile := cfgFile
	if resolvedCfgFile == "" {
		resolvedCfgFile = config.FindConfigFile()
	}

	// Launch the TUI.
	model := ui.NewAppModel(ctx, resolvedCfgFile, watchers)
	p := tea.NewProgram(model, tea.WithAltScreen())

	if watchFlag && resolvedCfgFile != "" {
		go watchConfigFile(ctx, p, resolvedCfgFile)
	}

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI: %w", err)
	}

	return nil
}

// ── tapWatcher ────────────────────────────────────────────────────────────────

// tapWatcher wraps a watcher.Watcher and writes every event to the store
// before forwarding it on its own channel. It satisfies watcher.Watcher so
// the TUI requires no changes.
type tapWatcher struct {
	inner watcher.Watcher
	st    *store.Store
	out   chan events.WatchEvent
}

func newTapWatcher(w watcher.Watcher, st *store.Store) *tapWatcher {
	return &tapWatcher{
		inner: w,
		st:    st,
		out:   make(chan events.WatchEvent, 64),
	}
}

func (t *tapWatcher) Name() string           { return t.inner.Name() }
func (t *tapWatcher) Type() string           { return t.inner.Type() }
func (t *tapWatcher) Status() watcher.Status { return t.inner.Status() }
func (t *tapWatcher) Events() <-chan events.WatchEvent { return t.out }

func (t *tapWatcher) Start(ctx context.Context) error {
	if err := t.inner.Start(ctx); err != nil {
		return err
	}
	go t.pump()
	return nil
}

func (t *tapWatcher) Stop() {
	t.inner.Stop()
}

// pump reads from the inner watcher's channel, persists each event, then
// forwards it. It closes t.out when the inner channel closes.
func (t *tapWatcher) pump() {
	defer close(t.out)
	for e := range t.inner.Events() {
		// Best-effort store write; never block the event pipeline on a DB error.
		_ = t.st.Insert(e)
		t.out <- e
	}
}

// ── watchConfigFile ───────────────────────────────────────────────────────────

func watchConfigFile(ctx context.Context, p *tea.Program, path string) {
	var lastMtime time.Time
	if info, err := os.Stat(path); err == nil {
		lastMtime = info.ModTime()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			if info.ModTime().After(lastMtime) {
				lastMtime = info.ModTime()
				p.Send(ui.ConfigChangedMsg{})
			}
		}
	}
}

// ── Example config ────────────────────────────────────────────────────────────

const exampleConfig = `{
  "watchers": [
    {
      "name": "System",
      "type": "sysmetrics",
      "poll_interval_s": 2,
      "max_events": 100
    },
    {
      "name": "Main DB",
      "type": "postgres",
      "dsn": "postgresql://${DB_USER}:${DB_PASSWORD}@localhost:5432/mydb",
      "tables": ["users", "orders"],
      "poll_interval_s": 3,
      "mode": "lightweight",
      "max_events": 100
    },
    {
      "name": "Events",
      "type": "kafka",
      "brokers": ["${KAFKA_BROKERS}"],
      "topics": ["order.created"],
      "group_id": "auphanim-dev",
      "offset": "latest",
      "max_events": 100
    },
    {
      "name": "Uploads",
      "type": "filesystem",
      "path": "${UPLOAD_DIR}",
      "recursive": true,
      "patterns": ["*.pdf", "*.jpg"],
      "max_events": 100
    },
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
    },
    {
      "name": "App Logs",
      "type": "logfile",
      "paths": ["${LOG_PATH}"],
      "error_patterns": ["ERROR", "FATAL", "PANIC", "EXCEPTION"],
      "max_events": 200
    }
  ]
}
`

func writeExample() error {
	const filename = "auphanim.json.example"
	if err := os.WriteFile(filename, []byte(exampleConfig), 0644); err != nil {
		return fmt.Errorf("writing example config: %w", err)
	}
	fmt.Printf("Wrote %s — copy it to auphanim.json and fill in your settings.\n", filename)
	return nil
}
