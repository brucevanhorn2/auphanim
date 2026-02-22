package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	rdb "github.com/redis/go-redis/v9"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

func init() {
	watcher.Register("redis", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		var cfg Config
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parsing redis config: %w", err)
		}
		if cfg.Addr == "" {
			return nil, fmt.Errorf("redis watcher %q: \"addr\" is required", name)
		}
		if cfg.Pattern == "" {
			cfg.Pattern = "*"
		}
		if cfg.PollIntervalS <= 0 {
			cfg.PollIntervalS = 3
		}
		if cfg.MaxEvents <= 0 {
			cfg.MaxEvents = 100
		}
		return &RedisWatcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 64),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

// Config holds the redis-specific watcher configuration.
type Config struct {
	Addr          string `json:"addr"`            // e.g. "localhost:6379"
	Password      string `json:"password"`        // optional
	DB            int    `json:"db"`              // default 0
	Pattern       string `json:"pattern"`         // SCAN pattern, default "*"
	PollIntervalS int    `json:"poll_interval_s"` // default 3
	ShowValues    bool   `json:"show_values"`     // fetch value for new string keys
	MaxEvents     int    `json:"max_events"`      // default 100
}

// RedisWatcher polls Redis using SCAN and emits events for keys that appear
// or disappear between polls. The initial scan is silent — only changes after
// startup are reported, so a pre-populated keyspace doesn't flood the panel.
type RedisWatcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
}

func (w *RedisWatcher) Name() string                        { return w.name }
func (w *RedisWatcher) Type() string                        { return "redis" }
func (w *RedisWatcher) Events() <-chan events.WatchEvent    { return w.eventsCh }

func (w *RedisWatcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *RedisWatcher) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	go w.run(ctx)
	return nil
}

func (w *RedisWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// ── Core goroutine ────────────────────────────────────────────────────────────

func (w *RedisWatcher) run(ctx context.Context) {
	defer close(w.eventsCh)
	defer w.setStatus(watcher.StatusStopped)

	client := rdb.NewClient(&rdb.Options{
		Addr:     w.cfg.Addr,
		Password: w.cfg.Password,
		DB:       w.cfg.DB,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("connect: %v", err))
		return
	}
	w.setStatus(watcher.StatusConnected)

	// Silent initial snapshot — populates prev without emitting any events.
	// This prevents flooding the panel when auphanim starts against a
	// Redis instance that already has keys in it.
	prev, err := w.scan(ctx, client)
	if err != nil {
		w.setStatus(watcher.StatusError)
		w.sendError(fmt.Sprintf("initial scan: %v", err))
		return
	}

	ticker := time.NewTicker(time.Duration(w.cfg.PollIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curr, err := w.scan(ctx, client)
			if err != nil {
				if ctx.Err() != nil {
					return // normal shutdown
				}
				w.setStatus(watcher.StatusError)
				w.sendError(fmt.Sprintf("scan: %v", err))
				return
			}
			w.diff(ctx, client, prev, curr)
			prev = curr
		}
	}
}

// scan performs a full cursor-based SCAN returning all matching keys.
// SCAN is non-blocking: each call returns a small batch and a cursor; we
// loop until the cursor wraps back to 0 (one complete iteration).
func (w *RedisWatcher) scan(ctx context.Context, client *rdb.Client) (map[string]bool, error) {
	result := make(map[string]bool)
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, w.cfg.Pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			result[k] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
		// Respect context cancellation mid-scan on large keyspaces.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return result, nil
}

// diff compares the previous and current key sets and emits events for keys
// that were created or removed between the two polls.
func (w *RedisWatcher) diff(ctx context.Context, client *rdb.Client, prev, curr map[string]bool) {
	// Keys in curr but not prev → EventCreated
	for key := range curr {
		if prev[key] {
			continue
		}
		detail := w.buildDetail(ctx, client, key)
		keyType, _ := detail["type"].(string)
		ttlStr, _ := detail["ttl"].(string)

		summary := fmt.Sprintf("%-40s  type=%-6s", key, keyType)
		if ttlStr != "" && ttlStr != "none" {
			summary += "  ttl=" + ttlStr
		}
		w.emit(events.EventCreated, summary, detail)
	}

	// Keys in prev but not curr → EventRemoved (expired or explicitly deleted)
	for key := range prev {
		if curr[key] {
			continue
		}
		w.emit(events.EventRemoved, key, map[string]any{"key": key})
	}
}

// buildDetail fetches the key's type, TTL, and (if show_values is set)
// its value or item count, returning a map suitable for the detail overlay.
// All Redis calls here are best-effort: if the key has expired between the
// SCAN and these calls, the missing fields are simply omitted.
func (w *RedisWatcher) buildDetail(ctx context.Context, client *rdb.Client, key string) map[string]any {
	detail := map[string]any{"key": key}

	keyType, err := client.Type(ctx, key).Result()
	if err != nil {
		keyType = "unknown"
	}
	detail["type"] = keyType

	ttl, err := client.TTL(ctx, key).Result()
	if err == nil {
		if ttl < 0 {
			detail["ttl"] = "none"
		} else {
			detail["ttl"] = ttl.Round(time.Second).String()
		}
	}

	if !w.cfg.ShowValues {
		return detail
	}

	switch keyType {
	case "string":
		val, err := client.Get(ctx, key).Result()
		if err == nil {
			if len(val) > 200 {
				val = val[:200] + "…"
			}
			detail["value"] = val
		}
	case "hash":
		if n, err := client.HLen(ctx, key).Result(); err == nil {
			detail["item_count"] = n
		}
	case "list":
		if n, err := client.LLen(ctx, key).Result(); err == nil {
			detail["item_count"] = n
		}
	case "set":
		if n, err := client.SCard(ctx, key).Result(); err == nil {
			detail["item_count"] = n
		}
	case "zset":
		if n, err := client.ZCard(ctx, key).Result(); err == nil {
			detail["item_count"] = n
		}
	}

	return detail
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (w *RedisWatcher) emit(eventType events.EventType, summary string, detail map[string]any) {
	select {
	case w.eventsCh <- events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: w.name,
		WatcherType: "redis",
		Type:        eventType,
		Summary:     summary,
		Detail:      detail,
	}:
	default:
	}
}

func (w *RedisWatcher) sendError(msg string) {
	select {
	case w.eventsCh <- events.WatchEvent{
		Timestamp:   time.Now(),
		WatcherName: w.name,
		WatcherType: "redis",
		Type:        events.EventError,
		Summary:     msg,
		Detail:      map[string]any{"error": msg},
	}:
	default:
	}
}

func (w *RedisWatcher) setStatus(s watcher.Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
}
