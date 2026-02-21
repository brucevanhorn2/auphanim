package ui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// ── Messages ─────────────────────────────────────────────────────────────────

type watchEventMsg struct {
	event events.WatchEvent
}

type watcherStoppedMsg struct {
	name string
}

// ── AppModel ─────────────────────────────────────────────────────────────────

// AppModel is the root Bubble Tea model. It owns all watcher panels and routes
// incoming events to the correct panel sub-model.
type AppModel struct {
	watchers []watcher.Watcher
	panels   map[string]*PanelModel // keyed by watcher.Name()
	order    []string               // panel display order (preserves config order)
	width    int
	height   int
	focused  int          // index into order of the focused panel
	detail   *DetailModel // non-nil when the detail overlay is open
}

// NewAppModel constructs an AppModel from a slice of already-started Watchers.
func NewAppModel(watchers []watcher.Watcher) AppModel {
	panels := make(map[string]*PanelModel, len(watchers))
	order := make([]string, 0, len(watchers))
	for _, w := range watchers {
		panels[w.Name()] = newPanelModel(w, 100)
		order = append(order, w.Name())
	}
	return AppModel{
		watchers: watchers,
		panels:   panels,
		order:    order,
	}
}

// ── Bubble Tea interface ──────────────────────────────────────────────────────

func (m AppModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, len(m.watchers))
	for i, w := range m.watchers {
		cmds[i] = waitForEvent(w)
	}
	return tea.Batch(cmds...)
}

func (m AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		if m.detail != nil {
			m.detail = newDetailModel(&m.detail.event, m.width, m.height)
		}

	case watchEventMsg:
		if panel, ok := m.panels[msg.event.WatcherName]; ok {
			panel.AddEvent(msg.event)
		}
		// Re-subscribe to keep receiving events from this watcher.
		for _, w := range m.watchers {
			if w.Name() == msg.event.WatcherName {
				return m, waitForEvent(w)
			}
		}

	case watcherStoppedMsg:
		// No re-subscription needed; the channel is closed.

	case tea.KeyMsg:
		// While the detail overlay is open, only ESC/q dismiss it.
		if m.detail != nil {
			switch msg.String() {
			case "esc", "q", "enter":
				m.detail = nil
			}
			return m, nil
		}

		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit

		case "c", "C":
			for _, p := range m.panels {
				p.Clear()
			}

		case "tab", "down", "j":
			if len(m.order) > 0 {
				m.focused = (m.focused + 1) % len(m.order)
			}

		case "shift+tab", "up", "k":
			if len(m.order) > 0 {
				m.focused = (m.focused - 1 + len(m.order)) % len(m.order)
			}

		case "enter":
			if len(m.order) > 0 {
				fp := m.panels[m.order[m.focused]]
				if last := fp.LastEvent(); last != nil {
					m.detail = newDetailModel(last, m.width, m.height)
				}
			}

		case "esc":
			m.detail = nil
		}
	}

	return m, nil
}

func (m AppModel) View() string {
	if m.width == 0 {
		return "Starting…"
	}

	if m.detail != nil {
		return m.detail.View()
	}

	header := m.headerView()
	footer := footerView(m.width)

	headerH := lipgloss.Height(header)
	footerH := lipgloss.Height(footer)
	available := m.height - headerH - footerH

	if len(m.order) == 0 || available < 1 {
		return lipgloss.JoinVertical(lipgloss.Left, header, footer)
	}

	// Distribute height evenly; last panel gets any remainder.
	panelH := available / len(m.order)
	if panelH < 3 {
		panelH = 3
	}

	panelViews := make([]string, len(m.order))
	for i, name := range m.order {
		w := m.watcherByName(name)
		var st watcher.Status
		if w != nil {
			st = w.Status()
		}
		h := panelH
		if i == len(m.order)-1 {
			// Give the last panel any leftover height.
			used := panelH * (len(m.order) - 1)
			h = available - used
			if h < panelH {
				h = panelH
			}
		}
		panelViews[i] = m.panels[name].View(m.width, h, i == m.focused, st)
	}

	body := strings.Join(panelViews, "")
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// waitForEvent blocks on the watcher's event channel and returns the next event
// as a Bubble Tea message. Bubble Tea runs this in a goroutine it manages.
func waitForEvent(w watcher.Watcher) tea.Cmd {
	return func() tea.Msg {
		event, ok := <-w.Events()
		if !ok {
			return watcherStoppedMsg{name: w.Name()}
		}
		return watchEventMsg{event: event}
	}
}

func (m AppModel) watcherByName(name string) watcher.Watcher {
	for _, w := range m.watchers {
		if w.Name() == name {
			return w
		}
	}
	return nil
}

func (m AppModel) headerView() string {
	n := len(m.watchers)
	noun := "source"
	if n != 1 {
		noun = "sources"
	}
	title := fmt.Sprintf("  AUPHANIM  —  Watching %d %s", n, noun)
	return styleHeader.Width(m.width).Render(title)
}

func footerView(width int) string {
	help := "  TAB/j/k: focus   ENTER: detail   C: clear   Q: quit   ESC: close"
	return styleFooter.Width(width).Render(help)
}
