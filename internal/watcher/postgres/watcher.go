package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

func init() {
	watcher.Register("postgres", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		var cfg Config
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parsing postgres config: %w", err)
		}
		if cfg.DSN == "" {
			return nil, fmt.Errorf("postgres watcher %q: \"dsn\" is required", name)
		}
		if cfg.PollIntervalS <= 0 {
			cfg.PollIntervalS = 3
		}
		if cfg.MaxEvents <= 0 {
			cfg.MaxEvents = 100
		}
		if cfg.Mode == "" {
			cfg.Mode = "lightweight"
		}
		return &PostgresWatcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 64),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

// Config holds the postgres-specific watcher configuration.
type Config struct {
	DSN           string   `json:"dsn"`
	Tables        []string `json:"tables"`
	PollIntervalS int      `json:"poll_interval_s"`
	Mode          string   `json:"mode"`       // "lightweight" (default) or "full_detail"
	MaxEvents     int      `json:"max_events"`
}

// PostgresWatcher watches PostgreSQL table activity.
// In "lightweight" mode it polls pg_stat_user_tables for row-count deltas.
// In "full_detail" mode it installs AFTER triggers that call pg_notify.
type PostgresWatcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
}

func (w *PostgresWatcher) Name() string                        { return w.name }
func (w *PostgresWatcher) Type() string                        { return "postgres" }
func (w *PostgresWatcher) Events() <-chan events.WatchEvent    { return w.eventsCh }

func (w *PostgresWatcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *PostgresWatcher) Start(ctx context.Context) error {
	for _, table := range w.cfg.Tables {
		if err := validateIdentifier(table); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	if w.cfg.Mode == "full_detail" {
		go w.runFullDetail(ctx)
	} else {
		go w.runLightweight(ctx)
	}
	return nil
}

func (w *PostgresWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// --------------------------------------------------------------------------
// Lightweight mode: poll pg_stat_user_tables every N seconds
// --------------------------------------------------------------------------

type tableStats struct {
	ins, upd, del int64
}

func (w *PostgresWatcher) runLightweight(ctx context.Context) {
	defer close(w.eventsCh)
	defer w.setStatus(watcher.StatusStopped)

	conn, err := pgx.Connect(ctx, w.cfg.DSN)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("connect: %v", err))
		return
	}
	defer conn.Close(context.Background())

	w.setStatus(watcher.StatusConnected)

	prev, err := w.queryStats(ctx, conn)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("initial stats query: %v", err))
		return
	}

	ticker := time.NewTicker(time.Duration(w.cfg.PollIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curr, err := w.queryStats(ctx, conn)
			if err != nil {
				w.setStatus(watcher.StatusError)
				w.sendError(fmt.Sprintf("stats poll: %v", err))
				return
			}
			for _, table := range w.cfg.Tables {
				p := prev[table]
				c := curr[table]
				if d := c.ins - p.ins; d > 0 {
					w.emit(events.EventInsert,
						fmt.Sprintf("%-24s +%d inserted", table, d),
						map[string]any{"table": table, "inserted": d})
				}
				if d := c.upd - p.upd; d > 0 {
					w.emit(events.EventUpdate,
						fmt.Sprintf("%-24s +%d updated", table, d),
						map[string]any{"table": table, "updated": d})
				}
				if d := c.del - p.del; d > 0 {
					w.emit(events.EventDelete,
						fmt.Sprintf("%-24s +%d deleted", table, d),
						map[string]any{"table": table, "deleted": d})
				}
			}
			prev = curr
		}
	}
}

func (w *PostgresWatcher) queryStats(ctx context.Context, conn *pgx.Conn) (map[string]tableStats, error) {
	rows, err := conn.Query(ctx, `
		SELECT relname, n_tup_ins, n_tup_upd, n_tup_del
		FROM pg_stat_user_tables
		WHERE relname = ANY($1)`, w.cfg.Tables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]tableStats, len(w.cfg.Tables))
	for rows.Next() {
		var name string
		var s tableStats
		if err := rows.Scan(&name, &s.ins, &s.upd, &s.del); err != nil {
			return nil, err
		}
		result[name] = s
	}
	return result, rows.Err()
}

// --------------------------------------------------------------------------
// Full-detail mode: LISTEN/NOTIFY via pg_notify triggers
// --------------------------------------------------------------------------

const notifyChannel = "auphanim_changes"

const createTriggerFnSQL = `
CREATE OR REPLACE FUNCTION auphanim_notify_change() RETURNS trigger AS $$
DECLARE
    payload TEXT;
BEGIN
    payload := json_build_object(
        'table', TG_TABLE_NAME,
        'op',    TG_OP,
        'new',   CASE WHEN TG_OP <> 'DELETE' THEN row_to_json(NEW) ELSE NULL END,
        'old',   CASE WHEN TG_OP <> 'INSERT' THEN row_to_json(OLD) ELSE NULL END
    )::text;
    IF length(payload) > 7900 THEN
        payload := left(payload, 7900);
    END IF;
    PERFORM pg_notify('auphanim_changes', payload);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;`

func (w *PostgresWatcher) runFullDetail(ctx context.Context) {
	defer close(w.eventsCh)
	defer w.setStatus(watcher.StatusStopped)

	// A background (non-cancellable) context for setup/teardown so we can
	// clean up even after the user's ctx is cancelled.
	bg := context.Background()

	setupConn, err := pgx.Connect(bg, w.cfg.DSN)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("connect for setup: %v", err))
		return
	}

	if _, err := setupConn.Exec(bg, createTriggerFnSQL); err != nil {
		setupConn.Close(bg)
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("create trigger function: %v", err))
		return
	}

	for _, table := range w.cfg.Tables {
		sqls := []string{
			fmt.Sprintf("DROP TRIGGER IF EXISTS auphanim_%s_trigger ON %s", table, table),
			fmt.Sprintf("CREATE TRIGGER auphanim_%s_trigger AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION auphanim_notify_change()", table, table),
		}
		for _, sql := range sqls {
			if _, err := setupConn.Exec(bg, sql); err != nil {
				setupConn.Close(bg)
				w.setStatus(watcher.StatusError)
				w.sendError(fmt.Sprintf("setup trigger on %q: %v", table, err))
				return
			}
		}
	}
	setupConn.Close(bg)

	// Best-effort trigger cleanup on shutdown.
	defer func() {
		cleanConn, err := pgx.Connect(bg, w.cfg.DSN)
		if err != nil {
			return
		}
		defer cleanConn.Close(bg)
		for _, table := range w.cfg.Tables {
			cleanConn.Exec(bg, fmt.Sprintf("DROP TRIGGER IF EXISTS auphanim_%s_trigger ON %s", table, table))
		}
	}()

	listenConn, err := pgx.Connect(ctx, w.cfg.DSN)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("connect for LISTEN: %v", err))
		return
	}
	defer listenConn.Close(bg)

	if _, err := listenConn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("LISTEN: %v", err))
		return
	}

	w.setStatus(watcher.StatusConnected)

	for {
		notification, err := listenConn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // normal shutdown
			}
			w.setStatus(watcher.StatusError)
			w.sendError(fmt.Sprintf("wait for notification: %v", err))
			return
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
			continue // truncated or invalid payload; skip
		}

		table, _ := payload["table"].(string)
		op, _ := payload["op"].(string)

		var eventType events.EventType
		switch op {
		case "INSERT":
			eventType = events.EventInsert
		case "UPDATE":
			eventType = events.EventUpdate
		case "DELETE":
			eventType = events.EventDelete
		default:
			continue
		}

		summary := buildSummary(table, op, payload)
		w.emit(eventType, summary, payload)
	}
}

// buildSummary creates a short human-readable summary from the notification payload.
func buildSummary(table, op string, payload map[string]any) string {
	row, _ := payload["new"].(map[string]any)
	if row == nil {
		row, _ = payload["old"].(map[string]any)
	}

	var parts []string
	count := 0
	for k, v := range row {
		if count >= 3 {
			break
		}
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		count++
	}

	if len(parts) > 0 {
		return fmt.Sprintf("%-20s %s  %s", table, op, strings.Join(parts, " "))
	}
	return fmt.Sprintf("%-20s %s", table, op)
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

func (w *PostgresWatcher) emit(eventType events.EventType, summary string, detail map[string]any) {
	select {
	case w.eventsCh <- events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: w.name,
		WatcherType: "postgres",
		Type:        eventType,
		Summary:     summary,
		Detail:      detail,
	}:
	default:
	}
}

func (w *PostgresWatcher) sendError(msg string) {
	select {
	case w.eventsCh <- events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: w.name,
		WatcherType: "postgres",
		Type:        events.EventError,
		Summary:     msg,
		Detail:      map[string]any{"error": msg},
	}:
	default:
	}
}

func (w *PostgresWatcher) setStatus(s watcher.Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
}

// validateIdentifier ensures a table name is safe to interpolate into SQL.
func validateIdentifier(name string) error {
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_') {
			return fmt.Errorf("invalid table name %q: only letters, digits, and underscores are allowed", name)
		}
	}
	return nil
}
