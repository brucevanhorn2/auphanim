package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// MetricSample holds one snapshot of system metrics, extracted from an
// EventMetric event so the panel can render sparklines without re-parsing.
type MetricSample struct {
	CPUPct     float64
	MemPct     float64
	MemUsedGB  float64
	MemTotalGB float64
	NetRecvBps float64
	NetSendBps float64
}

// PanelModel is the sub-model for a single watcher panel.
// It holds a ring-buffer of the most recent events.
type PanelModel struct {
	watcherName    string
	watcherType    string
	maxEvents      int
	evts           []events.WatchEvent // ring buffer
	lastEvt        events.WatchEvent   // copy of most recent event
	hasLast        bool
	metrics        []MetricSample // history for sparklines (sysmetrics type only)
	scrollOffset   int            // lines scrolled up from the bottom; 0 = follow latest
	lastActivityAt time.Time      // when the most recent event arrived
}

func newPanelModel(w watcher.Watcher, maxEvents int) *PanelModel {
	if maxEvents <= 0 {
		maxEvents = 100
	}
	return &PanelModel{
		watcherName: w.Name(),
		watcherType: w.Type(),
		maxEvents:   maxEvents,
		evts:        make([]events.WatchEvent, 0, maxEvents),
	}
}

// AddEvent appends an event to the ring buffer, evicting the oldest if full.
func (p *PanelModel) AddEvent(e events.WatchEvent) {
	if len(p.evts) >= p.maxEvents {
		p.evts = p.evts[1:]
	}
	p.evts = append(p.evts, e)
	p.lastEvt = e
	p.hasLast = true
	p.lastActivityAt = time.Now()

	// For sysmetrics panels, also maintain the metrics history for sparklines.
	if e.Type == events.EventMetric {
		const maxHistory = 60
		s := MetricSample{
			CPUPct:     floatFromDetail(e.Detail, "cpu_pct"),
			MemPct:     floatFromDetail(e.Detail, "mem_pct"),
			MemUsedGB:  floatFromDetail(e.Detail, "mem_used_gb"),
			MemTotalGB: floatFromDetail(e.Detail, "mem_total_gb"),
			NetRecvBps: floatFromDetail(e.Detail, "net_recv_bps"),
			NetSendBps: floatFromDetail(e.Detail, "net_send_bps"),
		}
		if len(p.metrics) >= maxHistory {
			p.metrics = p.metrics[1:]
		}
		p.metrics = append(p.metrics, s)
	}
}

// LastEvent returns a pointer to the most recent event, or nil if none.
func (p *PanelModel) LastEvent() *events.WatchEvent {
	if !p.hasLast {
		return nil
	}
	return &p.lastEvt
}

// Clear empties the event buffer and resets scroll position.
func (p *PanelModel) Clear() {
	p.evts = p.evts[:0]
	p.metrics = p.metrics[:0]
	p.hasLast = false
	p.scrollOffset = 0
}

// filteredEvents returns p.evts filtered by the given substring (case-insensitive).
// An empty filter returns all events.
func (p *PanelModel) filteredEvents(filter string) []events.WatchEvent {
	if filter == "" {
		return p.evts
	}
	lower := strings.ToLower(filter)
	var out []events.WatchEvent
	for _, e := range p.evts {
		if strings.Contains(strings.ToLower(e.Summary), lower) {
			out = append(out, e)
		}
	}
	return out
}

// ScrollUp scrolls the panel toward older events (increase offset).
func (p *PanelModel) ScrollUp(filter string) {
	filtered := p.filteredEvents(filter)
	max := len(filtered) - 1
	if max < 0 {
		return
	}
	if p.scrollOffset < max {
		p.scrollOffset++
	}
}

// ScrollDown scrolls toward newer events (decrease offset); reaches 0 = follow.
func (p *PanelModel) ScrollDown() {
	if p.scrollOffset > 0 {
		p.scrollOffset--
	}
}

// ScrollReset returns to follow-latest mode.
func (p *PanelModel) ScrollReset() {
	p.scrollOffset = 0
}

// View renders the panel into a string of exactly width×height characters.
// focused controls whether the border is highlighted.
// status is the current watcher.Status (queried from the live watcher).
// filter is the global filter substring (empty = show all).
func (p *PanelModel) View(width, height int, focused bool, status watcher.Status, filter string) string {
	// Inner dimensions (subtract border: 1 char each side)
	contentW := width - 2
	contentH := height - 2
	if contentW < 1 {
		contentW = 1
	}
	if contentH < 1 {
		contentH = 1
	}

	var content string
	if p.watcherType == "sysmetrics" {
		content = p.metricsView(contentW, contentH, status)
	} else {
		content = p.eventListView(contentW, contentH, status, filter)
	}

	borderStyle := styleBorderNormal
	if focused {
		borderStyle = styleBorderFocused
	}
	return borderStyle.Width(contentW).Render(content)
}

// eventListView renders the standard event-log view used by all non-metrics panels.
func (p *PanelModel) eventListView(contentW, contentH int, status watcher.Status, filter string) string {
	// --- Title line ---
	dot := statusDot(status)
	typeLabel := watcherTypeLabel(p.watcherType)

	// Build title decorations for scroll / filter state.
	extra := ""
	filtered := p.filteredEvents(filter)
	if filter != "" {
		extra += fmt.Sprintf(" [%d/%d]", len(filtered), len(p.evts))
	}
	if p.scrollOffset > 0 {
		extra += fmt.Sprintf(" ↑%d", p.scrollOffset)
	}

	title := stylePanelTitle.Render(fmt.Sprintf(" %s  %s: %s%s ", dot, typeLabel, p.watcherName, extra))
	if lipgloss.Width(title) > contentW {
		title = title[:contentW]
	}

	// --- Event lines ---
	eventRows := contentH - 1 // one line reserved for title
	if eventRows < 0 {
		eventRows = 0
	}

	// Apply scroll offset: end is the exclusive upper bound into filtered.
	end := len(filtered)
	if p.scrollOffset > 0 {
		end = len(filtered) - p.scrollOffset
		if end < 0 {
			end = 0
		}
	}
	start := end - eventRows
	if start < 0 {
		start = 0
	}
	evtsToShow := filtered[start:end]

	lines := make([]string, 0, contentH)
	lines = append(lines, title)

	for _, e := range evtsToShow {
		lines = append(lines, renderEventLine(e, contentW))
	}
	// Pad to fill remaining space.
	for len(lines) < contentH {
		lines = append(lines, "")
	}

	return strings.Join(lines, "\n")
}

// metricsView renders a bpytop-style metrics panel with bar graphs and sparklines.
func (p *PanelModel) metricsView(contentW, contentH int, status watcher.Status) string {
	// --- Title line ---
	dot := statusDot(status)
	title := stylePanelTitle.Render(fmt.Sprintf(" %s  Sys: %s ", dot, p.watcherName))
	if lipgloss.Width(title) > contentW {
		title = title[:contentW]
	}

	lines := []string{title}

	if len(p.metrics) == 0 {
		lines = append(lines, lipgloss.NewStyle().Foreground(colorGray).Render("  Collecting metrics…"))
		for len(lines) < contentH {
			lines = append(lines, "")
		}
		return strings.Join(lines, "\n")
	}

	latest := p.metrics[len(p.metrics)-1]

	// How many sparkline chars to show (up to 20, clipped to history length).
	const maxSparkW = 20
	sparkHistory := p.metrics
	if len(sparkHistory) > maxSparkW {
		sparkHistory = sparkHistory[len(sparkHistory)-maxSparkW:]
	}

	// Build per-metric value slices for sparklines.
	cpuVals := make([]float64, len(sparkHistory))
	memVals := make([]float64, len(sparkHistory))
	recvVals := make([]float64, len(sparkHistory))
	sendVals := make([]float64, len(sparkHistory))
	maxRecv, maxSend := 1.0, 1.0
	for i, s := range sparkHistory {
		cpuVals[i] = s.CPUPct
		memVals[i] = s.MemPct
		recvVals[i] = s.NetRecvBps
		sendVals[i] = s.NetSendBps
		if s.NetRecvBps > maxRecv {
			maxRecv = s.NetRecvBps
		}
		if s.NetSendBps > maxSend {
			maxSend = s.NetSendBps
		}
	}

	// Dynamically size the bar.
	// Layout per row: [label 5][space 1][bar barW][space 2][value 14][space 2][spark sparkW]
	// Fixed overhead: 5 + 1 + 2 + 14 + 2 = 24. sparkW = maxSparkW.
	const (
		labelW  = 5
		valueW  = 14
		padding = 5 // spaces between sections
	)
	sparkW := maxSparkW
	barW := contentW - labelW - padding - valueW - sparkW
	if barW < 8 {
		// Shrink sparkline to reclaim space.
		sparkW = 0
		barW = contentW - labelW - padding - valueW
	}
	if barW < 4 {
		barW = 4
	}

	cpuLine := renderMetricRow(
		"CPU",
		latest.CPUPct, 100, barW,
		fmt.Sprintf("%5.1f%%", latest.CPUPct),
		valueW,
		sparkline(cpuVals, 100),
		sparkW,
		contentW,
	)
	memLine := renderMetricRow(
		"MEM",
		latest.MemPct, 100, barW,
		fmt.Sprintf("%.1f/%.1fGB", latest.MemUsedGB, latest.MemTotalGB),
		valueW,
		sparkline(memVals, 100),
		sparkW,
		contentW,
	)
	recvLine := renderMetricRow(
		"↓ NET",
		latest.NetRecvBps, maxRecv, barW,
		fmtBps(latest.NetRecvBps),
		valueW,
		sparkline(recvVals, maxRecv),
		sparkW,
		contentW,
	)
	sendLine := renderMetricRow(
		"↑ NET",
		latest.NetSendBps, maxSend, barW,
		fmtBps(latest.NetSendBps),
		valueW,
		sparkline(sendVals, maxSend),
		sparkW,
		contentW,
	)

	lines = append(lines, cpuLine, memLine, recvLine, sendLine)

	for len(lines) < contentH {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

// renderMetricRow produces one metrics bar row.
//
//	label [bar] value sparkline
func renderMetricRow(
	label string,
	val, maxVal float64,
	barW int,
	valueStr string,
	valueW int,
	spark string,
	sparkW int,
	contentW int,
) string {
	labelS := lipgloss.NewStyle().
		Foreground(colorCyan).
		Width(5).
		Render(label)

	barStr := barGraph(val, maxVal, barW)
	barColor := metricBarColor(val, maxVal)
	barS := lipgloss.NewStyle().Foreground(barColor).Render(barStr)

	valS := lipgloss.NewStyle().
		Foreground(colorWhite).
		Width(valueW).
		Render(valueStr)

	var row string
	if sparkW > 0 {
		sparkS := lipgloss.NewStyle().Foreground(colorGray).Render(spark)
		row = fmt.Sprintf(" %s %s  %s  %s", labelS, barS, valS, sparkS)
	} else {
		row = fmt.Sprintf(" %s %s  %s", labelS, barS, valS)
	}

	// Truncate to content width to be safe.
	if lipgloss.Width(row) > contentW {
		// Trim from the right (strip trailing sparkline chars).
		runes := []rune(row)
		for lipgloss.Width(string(runes)) > contentW && len(runes) > 0 {
			runes = runes[:len(runes)-1]
		}
		row = string(runes)
	}
	return row
}

// barGraph renders a [████░░░░] bar of width chars representing val/maxVal.
func barGraph(val, maxVal float64, width int) string {
	if maxVal <= 0 {
		return strings.Repeat("░", width)
	}
	pct := val / maxVal
	if pct > 1 {
		pct = 1
	}
	if pct < 0 {
		pct = 0
	}
	filled := int(pct * float64(width))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

// sparkline converts a slice of values into a string of Unicode block chars.
// Values are normalized against maxVal.
const sparkChars = "▁▂▃▄▅▆▇█"

func sparkline(values []float64, maxVal float64) string {
	chars := []rune(sparkChars)
	n := len(chars)
	var sb strings.Builder
	for _, v := range values {
		idx := 0
		if maxVal > 0 {
			idx = int(v/maxVal*float64(n-1) + 0.5)
		}
		if idx >= n {
			idx = n - 1
		}
		if idx < 0 {
			idx = 0
		}
		sb.WriteRune(chars[idx])
	}
	return sb.String()
}

// metricBarColor returns green/yellow/red based on how high val is relative to max.
func metricBarColor(val, maxVal float64) lipgloss.Color {
	if maxVal <= 0 {
		return colorBlue
	}
	pct := val / maxVal * 100
	switch {
	case pct >= 90:
		return colorRed
	case pct >= 70:
		return colorYellow
	default:
		return colorGreen
	}
}

// fmtBps formats bytes-per-second for the metrics panel value column.
func fmtBps(bps float64) string {
	switch {
	case bps >= 1024*1024*1024:
		return fmt.Sprintf("%.2fGB/s", bps/1024/1024/1024)
	case bps >= 1024*1024:
		return fmt.Sprintf("%.2fMB/s", bps/1024/1024)
	case bps >= 1024:
		return fmt.Sprintf("%.1fKB/s", bps/1024)
	default:
		return fmt.Sprintf("%.0fB/s", bps)
	}
}

// renderEventLine formats a single event for display in the panel.
func renderEventLine(e events.WatchEvent, maxWidth int) string {
	ts := styleTimestamp.Render(e.Timestamp.Format("15:04:05"))
	typeStr := eventTypeStyle(e.Type).Width(8).Render(string(e.Type))
	summary := e.Summary

	// Build the line and truncate if it overflows (lipgloss.Width accounts for ANSI).
	line := ts + "  " + typeStr + "  " + summary
	if lipgloss.Width(line) > maxWidth {
		overhead := lipgloss.Width(ts + "  " + typeStr + "  ")
		remaining := maxWidth - overhead
		if remaining > 0 && len(summary) > remaining {
			summary = summary[:remaining]
		}
		line = ts + "  " + typeStr + "  " + summary
	}
	return line
}

// watcherTypeLabel returns a short display label for the watcher type.
func watcherTypeLabel(t string) string {
	switch t {
	case "postgres":
		return "DB"
	case "kafka":
		return "Kafka"
	case "filesystem":
		return "FS"
	case "logfile":
		return "Log"
	case "redis":
		return "Redis"
	case "sysmetrics":
		return "Sys"
	default:
		return t
	}
}

// floatFromDetail safely extracts a float64 from a WatchEvent Detail map.
func floatFromDetail(d map[string]any, key string) float64 {
	if v, ok := d[key]; ok {
		if f, ok := v.(float64); ok {
			return f
		}
	}
	return 0
}
