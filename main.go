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

	"auphanim/internal/config"
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
	cfgFile   string
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Build watchers from config.
	watchers := make([]watcher.Watcher, 0, len(cfg.Watchers))
	for _, wc := range cfg.Watchers {
		w, err := watcher.Create(wc.Type, wc.Name, wc.Config)
		if err != nil {
			return fmt.Errorf("watcher %q: %w", wc.Name, err)
		}
		watchers = append(watchers, w)
	}

	// Start all watchers before launching the TUI.
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

	// Resolve cfgFile so the watch goroutine has an absolute path to stat.
	resolvedCfgFile := cfgFile
	if resolvedCfgFile == "" {
		resolvedCfgFile = config.FindConfigFile()
	}

	// Launch the TUI.
	model := ui.NewAppModel(ctx, resolvedCfgFile, watchers)
	p := tea.NewProgram(model, tea.WithAltScreen())

	// If --watch is set, poll the config file for changes and notify the TUI.
	if watchFlag && resolvedCfgFile != "" {
		go watchConfigFile(ctx, p, resolvedCfgFile)
	}

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI: %w", err)
	}

	return nil
}

// watchConfigFile polls path every second and sends ConfigChangedMsg to p
// whenever the file's modification time changes.
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
