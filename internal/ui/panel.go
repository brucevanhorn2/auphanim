package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// PanelModel is the sub-model for a single watcher panel.
// It holds a ring-buffer of the most recent events.
type PanelModel struct {
	watcherName string
	watcherType string
	maxEvents   int
	evts        []events.WatchEvent // ring buffer
	lastEvt     events.WatchEvent   // copy of most recent event
	hasLast     bool
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
}

// LastEvent returns a pointer to the most recent event, or nil if none.
func (p *PanelModel) LastEvent() *events.WatchEvent {
	if !p.hasLast {
		return nil
	}
	return &p.lastEvt
}

// Clear empties the event buffer.
func (p *PanelModel) Clear() {
	p.evts = p.evts[:0]
	p.hasLast = false
}

// View renders the panel into a string of exactly width×height characters.
// focused controls whether the border is highlighted.
// status is the current watcher.Status (queried from the live watcher).
func (p *PanelModel) View(width, height int, focused bool, status watcher.Status) string {
	// Inner dimensions (subtract border: 1 char each side)
	contentW := width - 2
	contentH := height - 2
	if contentW < 1 {
		contentW = 1
	}
	if contentH < 1 {
		contentH = 1
	}

	// --- Title line ---
	dot := statusDot(status)
	typeLabel := watcherTypeLabel(p.watcherType)
	title := stylePanelTitle.Render(fmt.Sprintf(" %s  %s: %s ", dot, typeLabel, p.watcherName))
	if lipgloss.Width(title) > contentW {
		title = title[:contentW]
	}

	// --- Event lines ---
	eventRows := contentH - 1 // one line reserved for title
	if eventRows < 0 {
		eventRows = 0
	}

	// Show the most recent events (newest at bottom).
	evtsToShow := p.evts
	if len(evtsToShow) > eventRows {
		evtsToShow = evtsToShow[len(evtsToShow)-eventRows:]
	}

	lines := make([]string, 0, contentH)
	lines = append(lines, title)

	for _, e := range evtsToShow {
		lines = append(lines, renderEventLine(e, contentW))
	}
	// Pad to fill remaining space.
	for len(lines) < contentH {
		lines = append(lines, "")
	}

	content := strings.Join(lines, "\n")

	borderStyle := styleBorderNormal
	if focused {
		borderStyle = styleBorderFocused
	}
	return borderStyle.Width(contentW).Render(content)
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
	default:
		return t
	}
}
