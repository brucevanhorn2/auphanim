package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"auphanim/internal/config"
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

// ConfigChangedMsg is sent (from outside the model) when the config file
// changes on disk. It is exported so main.go can pass it to tea.Program.Send.
type ConfigChangedMsg struct{}

type configReloadedMsg struct {
	watchers []watcher.Watcher
}

type configReloadErrorMsg struct {
	err error
}

type exportDoneMsg struct {
	filename string
	err      error
}

type clearFlashMsg struct{}

// ── AppModel ─────────────────────────────────────────────────────────────────

// AppModel is the root Bubble Tea model. It owns all watcher panels and routes
// incoming events to the correct panel sub-model.
type AppModel struct {
	ctx    context.Context
	cfgFile string

	watchers     []watcher.Watcher
	panels       map[string]*PanelModel // keyed by watcher.Name()
	order        []string               // panel display order
	panelWeights map[string]int         // relative height weight per panel (min 1)
	width        int
	height       int
	focused      int          // index into order of the focused panel
	detail       *DetailModel // non-nil when the detail overlay is open

	filterMode bool
	filterText string

	flashMsg string
}

// NewAppModel constructs an AppModel from a slice of already-started Watchers.
func NewAppModel(ctx context.Context, cfgFile string, watchers []watcher.Watcher) AppModel {
	panels := make(map[string]*PanelModel, len(watchers))
	order := make([]string, 0, len(watchers))
	weights := make(map[string]int, len(watchers))
	for _, w := range watchers {
		panels[w.Name()] = newPanelModel(w, 100)
		order = append(order, w.Name())
		weights[w.Name()] = 1
	}
	return AppModel{
		ctx:          ctx,
		cfgFile:      cfgFile,
		watchers:     watchers,
		panels:       panels,
		order:        order,
		panelWeights: weights,
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
		for _, w := range m.watchers {
			if w.Name() == msg.event.WatcherName {
				return m, waitForEvent(w)
			}
		}

	case watcherStoppedMsg:
		// Channel closed — no re-subscription needed.

	case ConfigChangedMsg:
		// Launch async reload; keeps UI responsive while stopping/restarting watchers.
		return m, reloadConfigCmd(m.cfgFile, m.ctx, m.watchers)

	case configReloadedMsg:
		// Stop channels that may still be pumping (already stopped in the cmd,
		// but ensure they are before we rebuild).
		panels := make(map[string]*PanelModel, len(msg.watchers))
		order := make([]string, 0, len(msg.watchers))
		weights := make(map[string]int, len(msg.watchers))
		for _, w := range msg.watchers {
			name := w.Name()
			// Preserve existing panel history where the watcher name is the same.
			if old, ok := m.panels[name]; ok {
				panels[name] = old
			} else {
				panels[name] = newPanelModel(w, 100)
			}
			order = append(order, name)
			if existing, ok := m.panelWeights[name]; ok {
				weights[name] = existing
			} else {
				weights[name] = 1
			}
		}
		m.watchers = msg.watchers
		m.panels = panels
		m.order = order
		m.panelWeights = weights
		if m.focused >= len(m.order) {
			m.focused = len(m.order) - 1
		}
		m.flashMsg = "Config reloaded"
		cmds := make([]tea.Cmd, len(msg.watchers)+1)
		for i, w := range msg.watchers {
			cmds[i] = waitForEvent(w)
		}
		cmds[len(msg.watchers)] = flashClearCmd(3 * time.Second)
		return m, tea.Batch(cmds...)

	case configReloadErrorMsg:
		m.flashMsg = fmt.Sprintf("Reload error: %v", msg.err)
		return m, flashClearCmd(5 * time.Second)

	case exportDoneMsg:
		if msg.err != nil {
			m.flashMsg = fmt.Sprintf("Export failed: %v", msg.err)
		} else {
			m.flashMsg = fmt.Sprintf("Saved → %s", msg.filename)
		}
		return m, flashClearCmd(5 * time.Second)

	case clearFlashMsg:
		m.flashMsg = ""

	case tea.KeyMsg:
		// ── Filter mode ──────────────────────────────────────────────────────
		if m.filterMode {
			switch msg.Type {
			case tea.KeyEsc:
				m.filterMode = false
				m.filterText = ""
			case tea.KeyEnter:
				m.filterMode = false
			case tea.KeyBackspace, tea.KeyCtrlH:
				if len(m.filterText) > 0 {
					runes := []rune(m.filterText)
					m.filterText = string(runes[:len(runes)-1])
				}
			case tea.KeyRunes:
				m.filterText += string(msg.Runes)
			}
			return m, nil
		}

		// ── Detail overlay ────────────────────────────────────────────────────
		if m.detail != nil {
			switch msg.String() {
			case "esc", "q", "enter":
				m.detail = nil
			}
			return m, nil
		}

		// ── Normal mode ───────────────────────────────────────────────────────
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit

		case "c", "C":
			for _, p := range m.panels {
				p.Clear()
			}

		case "tab":
			if len(m.order) > 0 {
				m.focused = (m.focused + 1) % len(m.order)
			}

		case "shift+tab":
			if len(m.order) > 0 {
				m.focused = (m.focused - 1 + len(m.order)) % len(m.order)
			}

		case "k", "up":
			if len(m.order) > 0 {
				m.panels[m.order[m.focused]].ScrollUp(m.filterText)
			}

		case "j", "down":
			if len(m.order) > 0 {
				m.panels[m.order[m.focused]].ScrollDown()
			}

		case "g":
			if len(m.order) > 0 {
				m.panels[m.order[m.focused]].ScrollReset()
			}

		case "/":
			m.filterMode = true
			m.filterText = ""

		case "esc":
			if m.filterText != "" {
				m.filterText = ""
			}

		case "enter":
			if len(m.order) > 0 {
				fp := m.panels[m.order[m.focused]]
				if last := fp.LastEvent(); last != nil {
					m.detail = newDetailModel(last, m.width, m.height)
				}
			}

		case "+":
			if len(m.order) > 0 {
				name := m.order[m.focused]
				if m.panelWeights[name] < 10 {
					m.panelWeights[name]++
				}
			}

		case "-":
			if len(m.order) > 0 {
				name := m.order[m.focused]
				if m.panelWeights[name] > 1 {
					m.panelWeights[name]--
				}
			}

		case "=":
			for name := range m.panelWeights {
				m.panelWeights[name] = 1
			}

		case "s", "S":
			return m, exportCmd(m.panels, m.order)
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
	footer := m.footerView()

	headerH := lipgloss.Height(header)
	footerH := lipgloss.Height(footer)
	available := m.height - headerH - footerH

	if len(m.order) == 0 || available < 1 {
		return lipgloss.JoinVertical(lipgloss.Left, header, footer)
	}

	// Compute heights from weights.
	totalWeight := 0
	for _, name := range m.order {
		totalWeight += m.panelWeights[name]
	}
	if totalWeight == 0 {
		totalWeight = len(m.order)
	}

	panelViews := make([]string, len(m.order))
	usedHeight := 0
	for i, name := range m.order {
		w := m.watcherByName(name)
		var st watcher.Status
		if w != nil {
			st = w.Status()
		}

		var h int
		if i == len(m.order)-1 {
			h = available - usedHeight
		} else {
			h = (m.panelWeights[name] * available) / totalWeight
		}
		if h < 3 {
			h = 3
		}
		usedHeight += h

		panelViews[i] = m.panels[name].View(m.width, h, i == m.focused, st, m.filterText)
	}

	body := strings.Join(panelViews, "")
	return lipgloss.JoinVertical(lipgloss.Left, header, body, footer)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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

func (m AppModel) footerView() string {
	if m.filterMode {
		bar := fmt.Sprintf("  Filter: %s▎", m.filterText)
		return styleFooter.Width(m.width).Render(bar)
	}
	if m.flashMsg != "" {
		return styleFooter.Width(m.width).Render("  " + m.flashMsg)
	}
	help := "  TAB: panel   j/k: scroll   g: latest   /: filter   s: save   +/-: resize   ENTER: detail   C: clear   Q: quit"
	return styleFooter.Width(m.width).Render(help)
}

// reloadConfigCmd stops old watchers, reloads the config, and starts fresh ones.
func reloadConfigCmd(cfgFile string, ctx context.Context, oldWatchers []watcher.Watcher) tea.Cmd {
	return func() tea.Msg {
		for _, w := range oldWatchers {
			w.Stop()
		}
		// Brief pause to let goroutines drain before we start fresh watchers.
		time.Sleep(150 * time.Millisecond)

		cfg, err := config.Load(cfgFile)
		if err != nil {
			return configReloadErrorMsg{err: err}
		}

		newWatchers := make([]watcher.Watcher, 0, len(cfg.Watchers))
		for _, wc := range cfg.Watchers {
			w, err := watcher.Create(wc.Type, wc.Name, wc.Config)
			if err != nil {
				return configReloadErrorMsg{err: fmt.Errorf("watcher %q: %w", wc.Name, err)}
			}
			if err := w.Start(ctx); err != nil {
				return configReloadErrorMsg{err: fmt.Errorf("starting watcher %q: %w", w.Name(), err)}
			}
			newWatchers = append(newWatchers, w)
		}
		return configReloadedMsg{watchers: newWatchers}
	}
}

// exportCmd collects all panel events and writes them to a JSON file.
func exportCmd(panels map[string]*PanelModel, order []string) tea.Cmd {
	// Snapshot events synchronously before launching goroutine.
	type panelSnap struct {
		name  string
		wtype string
		evts  []events.WatchEvent
	}
	snaps := make([]panelSnap, len(order))
	for i, name := range order {
		p := panels[name]
		evtsCopy := make([]events.WatchEvent, len(p.evts))
		copy(evtsCopy, p.evts)
		snaps[i] = panelSnap{name: name, wtype: p.watcherType, evts: evtsCopy}
	}

	return func() tea.Msg {
		type exportPanel struct {
			WatcherName string              `json:"watcher_name"`
			WatcherType string              `json:"watcher_type"`
			Events      []events.WatchEvent `json:"events"`
		}
		type exportData struct {
			ExportedAt time.Time      `json:"exported_at"`
			Panels     []exportPanel  `json:"panels"`
		}

		data := exportData{ExportedAt: time.Now()}
		for _, s := range snaps {
			data.Panels = append(data.Panels, exportPanel{
				WatcherName: s.name,
				WatcherType: s.wtype,
				Events:      s.evts,
			})
		}

		b, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return exportDoneMsg{err: err}
		}

		filename := fmt.Sprintf("auphanim_export_%s.json", time.Now().Format("20060102_150405"))
		if err := os.WriteFile(filename, b, 0644); err != nil {
			return exportDoneMsg{err: err}
		}
		return exportDoneMsg{filename: filename}
	}
}

// flashClearCmd returns a command that sends clearFlashMsg after d.
func flashClearCmd(d time.Duration) tea.Cmd {
	return func() tea.Msg {
		time.Sleep(d)
		return clearFlashMsg{}
	}
}
