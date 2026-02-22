// Package store persists watcher events to a SQLite database.
//
// The database path is configurable; pass ":memory:" for an in-memory store
// that vanishes when auphanim exits, or a file path like "auphanim.db" for a
// durable store that survives restarts (pick up where you left off on Monday).
//
// Schema
//
//	events(id, ts, panel, wtype, etype, summary, detail)
//
// All queries return events ordered newest-first unless otherwise noted.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"auphanim/internal/events"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

const schema = `
CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY,
    ts      DATETIME NOT NULL,
    panel   TEXT     NOT NULL,
    wtype   TEXT     NOT NULL,
    etype   TEXT     NOT NULL,
    summary TEXT     NOT NULL,
    detail  TEXT     NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_ts    ON events(ts);
CREATE INDEX IF NOT EXISTS idx_etype ON events(etype);
CREATE INDEX IF NOT EXISTS idx_panel ON events(panel);

-- Trim rows older than a configurable retention period.
-- Called explicitly; SQLite has no background jobs.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`

// Store wraps a SQLite database and exposes event persistence and querying.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and applies the schema.
// Use ":memory:" for an ephemeral in-session store.
func Open(path string) (*Store, error) {
	// WAL mode gives better concurrent read performance and crash safety.
	dsn := path
	if path != ":memory:" {
		dsn = path + "?_journal_mode=WAL&_busy_timeout=5000"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db %q: %w", path, err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// Insert persists a single event. It is safe to call from multiple goroutines.
func (s *Store) Insert(e events.WatchEvent) error {
	detail, err := json.Marshal(e.Detail)
	if err != nil {
		detail = []byte("{}")
	}
	_, err = s.db.Exec(
		`INSERT INTO events(ts, panel, wtype, etype, summary, detail)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.Timestamp.UTC().Format(time.RFC3339Nano),
		e.WatcherName,
		e.WatcherType,
		string(e.Type),
		e.Summary,
		string(detail),
	)
	return err
}

// QueryOptions filters results returned by Query.
// Zero values mean "no filter" for that field.
type QueryOptions struct {
	Panel     string        // exact panel name; empty = all panels
	EventType string        // exact event type ("ERROR", "INSERT", …); empty = all
	Since     time.Duration // only events newer than time.Now()-Since; 0 = all time
	Limit     int           // max rows; 0 = 100
}

// Row is a single event row returned from the database.
type Row struct {
	ID        int64
	Timestamp time.Time
	Panel     string
	WatcherType string
	EventType string
	Summary   string
	Detail    map[string]any
}

// Query returns events matching opts, newest first.
func (s *Store) Query(ctx context.Context, opts QueryOptions) ([]Row, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	args := []any{}
	where := "1=1"

	if opts.Panel != "" {
		where += " AND panel = ?"
		args = append(args, opts.Panel)
	}
	if opts.EventType != "" {
		where += " AND etype = ?"
		args = append(args, opts.EventType)
	}
	if opts.Since > 0 {
		cutoff := time.Now().UTC().Add(-opts.Since).Format(time.RFC3339Nano)
		where += " AND ts >= ?"
		args = append(args, cutoff)
	}

	args = append(args, limit)
	q := fmt.Sprintf(
		`SELECT id, ts, panel, wtype, etype, summary, detail
		 FROM events WHERE %s ORDER BY ts DESC LIMIT ?`,
		where,
	)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		var ts, detailStr string
		if err := rows.Scan(&r.ID, &ts, &r.Panel, &r.WatcherType, &r.EventType, &r.Summary, &detailStr); err != nil {
			return nil, err
		}
		r.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
		_ = json.Unmarshal([]byte(detailStr), &r.Detail)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Summary returns per-panel event counts and the most recent error summary.
type PanelSummary struct {
	Panel      string
	TotalCount int
	ErrorCount int
	LastError  string
	LastErrorAt time.Time
}

// Summarize returns one PanelSummary per panel seen in the database.
func (s *Store) Summarize(ctx context.Context, since time.Duration) ([]PanelSummary, error) {
	cutoff := ""
	if since > 0 {
		cutoff = time.Now().UTC().Add(-since).Format(time.RFC3339Nano)
	}

	whereClause := ""
	args := []any{}
	if cutoff != "" {
		whereClause = "WHERE ts >= ?"
		args = append(args, cutoff)
	}

	q := fmt.Sprintf(`
		SELECT panel,
		       COUNT(*)                                        AS total,
		       SUM(CASE WHEN etype = 'ERROR' THEN 1 ELSE 0 END) AS errors
		FROM events %s
		GROUP BY panel
		ORDER BY panel`, whereClause)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summaries := map[string]*PanelSummary{}
	var order []string
	for rows.Next() {
		var ps PanelSummary
		if err := rows.Scan(&ps.Panel, &ps.TotalCount, &ps.ErrorCount); err != nil {
			return nil, err
		}
		summaries[ps.Panel] = &ps
		order = append(order, ps.Panel)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Fetch last error per panel in a second query.
	for _, name := range order {
		ps := summaries[name]
		if ps.ErrorCount == 0 {
			continue
		}
		errArgs := []any{name}
		errWhere := "panel = ? AND etype = 'ERROR'"
		if cutoff != "" {
			errWhere += " AND ts >= ?"
			errArgs = append(errArgs, cutoff)
		}
		row := s.db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT ts, summary FROM events WHERE %s ORDER BY ts DESC LIMIT 1`, errWhere),
			errArgs...)
		var ts string
		_ = row.Scan(&ts, &ps.LastError)
		ps.LastErrorAt, _ = time.Parse(time.RFC3339Nano, ts)
	}

	out := make([]PanelSummary, len(order))
	for i, name := range order {
		out[i] = *summaries[name]
	}
	return out, nil
}

// Prune deletes events older than age. Safe to call periodically.
func (s *Store) Prune(age time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-age).Format(time.RFC3339Nano)
	res, err := s.db.Exec(`DELETE FROM events WHERE ts < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
