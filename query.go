package main

// query subcommand: open the SQLite event store directly and print results.
// Because it talks to the file (not a running process), it works whether
// auphanim is currently running or not. SQLite WAL mode allows concurrent
// reads alongside auphanim's writes.
//
// Usage:
//
//	auphanim query summary [--since 1h] [--json]
//	auphanim query events  [--panel X] [--type Y] [--since 5m] [--limit 50] [--json]
//	auphanim query errors  [--panel X] [--since 5m] [--limit 50] [--json]

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"auphanim/internal/store"
)

// ── Flags ─────────────────────────────────────────────────────────────────────

var (
	qSince string
	qJSON  bool
	qPanel string
	qType  string
	qLimit int
)

// ── Command tree ──────────────────────────────────────────────────────────────

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query the event store",
	Long: `Query events persisted in the SQLite event store.

Works whether auphanim is currently running or not — it reads the store
file directly (SQLite WAL mode allows concurrent reads alongside writes).

The --db flag (on the root command) selects which file to read.
Default: ./auphanim.db`,
}

var querySummaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Per-panel event and error counts",
	Example: `  auphanim query summary
  auphanim query summary --since 1h
  auphanim query summary --since 30m --json | jq '.panels[] | select(.errors > 0)'`,
	RunE: runQuerySummary,
}

var queryEventsCmd = &cobra.Command{
	Use:   "events",
	Short: "List events from the store",
	Example: `  auphanim query events
  auphanim query events --panel "App Logs" --since 5m
  auphanim query events --type INSERT --since 1h --limit 20`,
	RunE: runQueryEvents,
}

var queryErrorsCmd = &cobra.Command{
	Use:   "errors",
	Short: "List error events (shorthand for query events --type ERROR)",
	Example: `  auphanim query errors
  auphanim query errors --since 10m
  auphanim query errors --panel "App Logs" --json`,
	RunE: runQueryErrors,
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.AddCommand(querySummaryCmd, queryEventsCmd, queryErrorsCmd)

	// --since and --json are common to all three subcommands.
	for _, cmd := range []*cobra.Command{querySummaryCmd, queryEventsCmd, queryErrorsCmd} {
		cmd.Flags().StringVar(&qSince, "since", "",
			"only include events newer than this duration (e.g. 5m, 1h, 2h30m)")
		cmd.Flags().BoolVar(&qJSON, "json", false,
			"output raw JSON instead of the formatted table")
	}

	// events-specific flags.
	queryEventsCmd.Flags().StringVar(&qPanel, "panel", "", "filter by panel name")
	queryEventsCmd.Flags().StringVar(&qType, "type", "", "filter by event type (ERROR, INSERT, MESSAGE, …)")
	queryEventsCmd.Flags().IntVar(&qLimit, "limit", 50, "maximum number of events to return")

	// errors-specific flags.
	queryErrorsCmd.Flags().StringVar(&qPanel, "panel", "", "filter by panel name")
	queryErrorsCmd.Flags().IntVar(&qLimit, "limit", 50, "maximum number of errors to return")
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func runQuerySummary(cmd *cobra.Command, args []string) error {
	since, err := parseDuration(qSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}

	st, err := store.Open(dbFile)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	sums, err := st.Summarize(context.Background(), since)
	if err != nil {
		return err
	}

	if qJSON {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"panels": sums,
			"since":  qSince,
		})
	}

	if len(sums) == 0 {
		fmt.Println("No events in store" + sinceHint(qSince) + ".")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PANEL\tEVENTS\tERRORS\tLAST ERROR")
	for _, ps := range sums {
		lastErr := "—"
		if ps.ErrorCount > 0 {
			ago := ""
			if !ps.LastErrorAt.IsZero() {
				ago = fmt.Sprintf(" (%s ago)", formatAge(time.Since(ps.LastErrorAt)))
			}
			lastErr = ps.LastError + ago
			if len(lastErr) > 60 {
				lastErr = lastErr[:57] + "…"
			}
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\n", ps.Panel, ps.TotalCount, ps.ErrorCount, lastErr)
	}
	return w.Flush()
}

func runQueryEvents(cmd *cobra.Command, args []string) error {
	since, err := parseDuration(qSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}

	st, err := store.Open(dbFile)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	rows, err := st.Query(context.Background(), store.QueryOptions{
		Panel:     qPanel,
		EventType: qType,
		Since:     since,
		Limit:     qLimit,
	})
	if err != nil {
		return err
	}

	return printEvents(rows, qSince)
}

func runQueryErrors(cmd *cobra.Command, args []string) error {
	since, err := parseDuration(qSince)
	if err != nil {
		return fmt.Errorf("--since: %w", err)
	}

	st, err := store.Open(dbFile)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	rows, err := st.Query(context.Background(), store.QueryOptions{
		Panel:     qPanel,
		EventType: "ERROR",
		Since:     since,
		Limit:     qLimit,
	})
	if err != nil {
		return err
	}

	return printEvents(rows, qSince)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func printEvents(rows []store.Row, since string) error {
	if qJSON {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"events": rows,
			"count":  len(rows),
		})
	}

	if len(rows) == 0 {
		fmt.Println("No events found" + sinceHint(since) + ".")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIME\tTYPE\tPANEL\tSUMMARY")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%-8s\t%s\t%s\n",
			r.Timestamp.Local().Format("15:04:05"),
			r.EventType,
			r.Panel,
			r.Summary,
		)
	}
	return w.Flush()
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func sinceHint(since string) string {
	if since == "" {
		return ""
	}
	return " in the last " + since
}

// formatAge returns a human-readable duration like "3m", "2h5m", "45s".
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
