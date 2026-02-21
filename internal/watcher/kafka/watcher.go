package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

func init() {
	watcher.Register("kafka", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		var cfg Config
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("parsing kafka config: %w", err)
		}
		if len(cfg.Brokers) == 0 {
			return nil, fmt.Errorf("kafka watcher %q: \"brokers\" is required", name)
		}
		if len(cfg.Topics) == 0 {
			return nil, fmt.Errorf("kafka watcher %q: \"topics\" is required", name)
		}
		if cfg.MaxEvents <= 0 {
			cfg.MaxEvents = 100
		}
		if cfg.Offset == "" {
			cfg.Offset = "latest"
		}
		return &KafkaWatcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 64),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

// Config holds the kafka-specific watcher configuration.
type Config struct {
	Brokers   []string `json:"brokers"`
	Topics    []string `json:"topics"`
	GroupID   string   `json:"group_id"`
	Offset    string   `json:"offset"`     // "latest" (default) or "earliest"
	MaxEvents int      `json:"max_events"`
}

// KafkaWatcher consumes one or more Kafka topics and emits WatchEvents.
type KafkaWatcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
	readers  []*kafkago.Reader
}

func (w *KafkaWatcher) Name() string                        { return w.name }
func (w *KafkaWatcher) Type() string                        { return "kafka" }
func (w *KafkaWatcher) Events() <-chan events.WatchEvent    { return w.eventsCh }

func (w *KafkaWatcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *KafkaWatcher) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	startOffset := kafkago.LastOffset
	if w.cfg.Offset == "earliest" {
		startOffset = kafkago.FirstOffset
	}

	for _, topic := range w.cfg.Topics {
		r := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     w.cfg.Brokers,
			Topic:       topic,
			GroupID:     w.cfg.GroupID,
			StartOffset: startOffset,
		})
		w.readers = append(w.readers, r)
	}

	w.mu.Lock()
	w.status = watcher.StatusConnected
	w.mu.Unlock()

	var wg sync.WaitGroup
	for _, r := range w.readers {
		wg.Add(1)
		go func(r *kafkago.Reader) {
			defer wg.Done()
			w.readLoop(ctx, r)
		}(r)
	}

	// Close the events channel once all readers finish.
	go func() {
		wg.Wait()
		close(w.eventsCh)
		w.mu.Lock()
		w.status = watcher.StatusStopped
		w.mu.Unlock()
	}()

	return nil
}

func (w *KafkaWatcher) readLoop(ctx context.Context, r *kafkago.Reader) {
	defer r.Close()

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // normal shutdown via context cancellation
			}
			w.mu.Lock()
			w.status = watcher.StatusError
			w.mu.Unlock()
			select {
			case w.eventsCh <- events.WatchEvent{
				Timestamp:   time.Now(),
				WatcherName: w.name,
				WatcherType: "kafka",
				Type:        events.EventError,
				Summary:     fmt.Sprintf("read error on %s: %v", r.Config().Topic, err),
				Detail:      map[string]any{"topic": r.Config().Topic, "error": err.Error()},
			}:
			default:
			}
			return
		}

		// Try to parse value as JSON; fall back to raw string.
		var valueJSON any
		if err := json.Unmarshal(msg.Value, &valueJSON); err != nil {
			valueJSON = string(msg.Value)
		}

		preview := string(msg.Value)
		if len(preview) > 60 {
			preview = preview[:60] + "…"
		}

		summary := fmt.Sprintf("%-20s p=%-3d off=%-6d  %s",
			msg.Topic, msg.Partition, msg.Offset, preview)

		detail := map[string]any{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"key":       string(msg.Key),
			"value":     valueJSON,
			"timestamp": msg.Time.Format(time.RFC3339),
		}
		if len(msg.Headers) > 0 {
			detail["headers"] = headersToMap(msg.Headers)
		}

		select {
		case w.eventsCh <- events.WatchEvent{
			Timestamp:   time.Now(),
			WatcherName: w.name,
			WatcherType: "kafka",
			Type:        events.EventMessage,
			Summary:     summary,
			Detail:      detail,
		}:
		default:
		}
	}
}

func headersToMap(headers []kafkago.Header) map[string]string {
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

func (w *KafkaWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}
