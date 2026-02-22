// Package sysmetrics watches CPU, memory, and network I/O using gopsutil.
// It emits an EventMetric every poll_interval_s seconds with the current
// system resource usage, suitable for the TUI metrics panel.
package sysmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	pscpu "github.com/shirou/gopsutil/v3/cpu"
	psmem "github.com/shirou/gopsutil/v3/mem"
	psnet "github.com/shirou/gopsutil/v3/net"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// Config holds the sysmetrics-specific configuration fields.
type Config struct {
	PollIntervalS int `json:"poll_interval_s"`
	MaxEvents     int `json:"max_events"`
}

// SysMetricsWatcher polls system metrics and emits EventMetric events.
type SysMetricsWatcher struct {
	name     string
	cfg      Config
	eventsCh chan events.WatchEvent
	status   watcher.Status
	mu       sync.RWMutex
	cancel   context.CancelFunc
}

func init() {
	watcher.Register("sysmetrics", func(name string, raw json.RawMessage) (watcher.Watcher, error) {
		cfg := Config{
			PollIntervalS: 2,
			MaxEvents:     100,
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &cfg); err != nil {
				return nil, err
			}
		}
		if cfg.PollIntervalS <= 0 {
			cfg.PollIntervalS = 2
		}
		return &SysMetricsWatcher{
			name:     name,
			cfg:      cfg,
			eventsCh: make(chan events.WatchEvent, 64),
			status:   watcher.StatusConnecting,
		}, nil
	})
}

func (w *SysMetricsWatcher) Name() string                       { return w.name }
func (w *SysMetricsWatcher) Type() string                       { return "sysmetrics" }
func (w *SysMetricsWatcher) Events() <-chan events.WatchEvent   { return w.eventsCh }

func (w *SysMetricsWatcher) Status() watcher.Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *SysMetricsWatcher) setStatus(s watcher.Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
}

func (w *SysMetricsWatcher) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	go w.run(ctx)
	return nil
}

func (w *SysMetricsWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *SysMetricsWatcher) run(ctx context.Context) {
	defer close(w.eventsCh)

	// Prime the CPU counter — first call always returns 0 because there is no
	// previous measurement to diff against. After priming, the next call
	// returns the CPU usage over the elapsed time since this prime call.
	_, _ = pscpu.Percent(0, false)

	// Capture initial network counters so the first real reading has a valid
	// baseline. Use the zero time to indicate "not yet initialized."
	var (
		prevSend uint64
		prevRecv uint64
		prevTime time.Time
	)
	if counters, err := psnet.IOCounters(false); err == nil && len(counters) > 0 {
		prevSend = counters[0].BytesSent
		prevRecv = counters[0].BytesRecv
		prevTime = time.Now()
	}

	w.setStatus(watcher.StatusConnected)

	ticker := time.NewTicker(time.Duration(w.cfg.PollIntervalS) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.setStatus(watcher.StatusStopped)
			return

		case t := <-ticker.C:
			evt, err := w.collect(t, &prevSend, &prevRecv, &prevTime)
			if err != nil {
				w.setStatus(watcher.StatusError)
				select {
				case w.eventsCh <- events.WatchEvent{
					Timestamp:   time.Now(),
					WatcherName: w.name,
					WatcherType: "sysmetrics",
					Type:        events.EventError,
					Summary:     err.Error(),
					Detail:      map[string]any{"error": err.Error()},
				}:
				default:
				}
				continue
			}
			w.setStatus(watcher.StatusConnected)
			select {
			case w.eventsCh <- evt:
			default:
			}
		}
	}
}

// collect gathers one sample of CPU, memory, and network metrics.
func (w *SysMetricsWatcher) collect(
	t time.Time,
	prevSend, prevRecv *uint64,
	prevTime *time.Time,
) (events.WatchEvent, error) {
	// ── CPU ──────────────────────────────────────────────────────────────────
	cpuPcts, err := pscpu.Percent(0, false)
	if err != nil {
		return events.WatchEvent{}, fmt.Errorf("cpu: %w", err)
	}
	cpuPct := 0.0
	if len(cpuPcts) > 0 {
		cpuPct = cpuPcts[0]
	}

	// ── Memory ───────────────────────────────────────────────────────────────
	vmStat, err := psmem.VirtualMemory()
	if err != nil {
		return events.WatchEvent{}, fmt.Errorf("mem: %w", err)
	}
	const gb = 1024 * 1024 * 1024
	memUsedGB := float64(vmStat.Used) / gb
	memTotalGB := float64(vmStat.Total) / gb
	memPct := vmStat.UsedPercent

	// ── Network ──────────────────────────────────────────────────────────────
	var sendBps, recvBps float64
	if counters, err := psnet.IOCounters(false); err == nil && len(counters) > 0 {
		if !prevTime.IsZero() {
			elapsed := t.Sub(*prevTime).Seconds()
			if elapsed > 0 {
				sendBps = float64(counters[0].BytesSent-*prevSend) / elapsed
				recvBps = float64(counters[0].BytesRecv-*prevRecv) / elapsed
			}
		}
		*prevSend = counters[0].BytesSent
		*prevRecv = counters[0].BytesRecv
		*prevTime = t
	}

	summary := fmt.Sprintf(
		"CPU %.1f%%  MEM %.1f%% (%.1f/%.1f GB)  ↓ %s  ↑ %s",
		cpuPct, memPct, memUsedGB, memTotalGB,
		fmtBytesPerSec(recvBps), fmtBytesPerSec(sendBps),
	)

	return events.WatchEvent{
		Timestamp:   t,
		WatcherName: w.name,
		WatcherType: "sysmetrics",
		Type:        events.EventMetric,
		Summary:     summary,
		Detail: map[string]any{
			"cpu_pct":      cpuPct,
			"mem_pct":      memPct,
			"mem_used_gb":  memUsedGB,
			"mem_total_gb": memTotalGB,
			"net_recv_bps": recvBps,
			"net_send_bps": sendBps,
		},
	}, nil
}

// fmtBytesPerSec formats a bytes-per-second rate as a human-readable string.
func fmtBytesPerSec(bps float64) string {
	switch {
	case bps >= 1024*1024*1024:
		return fmt.Sprintf("%.2f GB/s", bps/1024/1024/1024)
	case bps >= 1024*1024:
		return fmt.Sprintf("%.2f MB/s", bps/1024/1024)
	case bps >= 1024:
		return fmt.Sprintf("%.1f KB/s", bps/1024)
	default:
		return fmt.Sprintf("%.0f B/s", bps)
	}
}
