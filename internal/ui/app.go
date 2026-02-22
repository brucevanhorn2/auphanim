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

// tickMsg is sent every second to keep activity indicators current.
type tickMsg struct{}

// ── AppModel ─────────────────────────────────────────────────────────────────

// AppModel is the root Bubble Tea model. It owns all watcher panels and routes
// incoming events to the correct panel sub-model.
type AppModel struct {
	ctx     context.Context
	cfgFile string

	watchers []watcher.Watcher
	panels   map[string]*PanelModel // keyed by watcher.Name()
	order    []string               // panel display order

	width  int
	height int

	// Layout
	columns        int // actual columns used (derived from width or override)
	columnsOverride int // 0 = auto; 1/2/3 = explicit
	viewportStart  int // index of the first visible panel row

	focused int          // index into order of the focused panel
	detail  *DetailModel // non-nil when the detail overlay is open

	filterMode bool
	filterText string

	flashMsg string
}

// NewAppModel constructs an AppModel from a slice of already-started Watchers.
func NewAppModel(ctx context.Context, cfgFile string, watchers []watcher.Watcher) AppModel {
	panels := make(map[string]*PanelModel, len(watchers))
	order := make([]string, 0, len(watchers))
	for _, w := range watchers {
		panels[w.Name()] = newPanelModel(w, 100)
		order = append(order, w.Name())
	}
	return AppModel{
		ctx:     ctx,
		cfgFile: cfgFile,
		watchers: watchers,
		panels:  panels,
		order:   order,
		columns: 1, // updated on first WindowSizeMsg
	}
}

// ── Layout helpers ────────────────────────────────────────────────────────────

// autoColumns returns the number of columns that best fit the given terminal width.
func autoColumns(width int) int {
	switch {
	case width >= 240:
		return 3
	case width >= 120:
		return 2
	default:
		return 1
	}
}

// layout computes how many panel rows are visible, the height per row, and the
// available body height, given the current model state.
// "rows" here means rows of panels (each row holds `cols` panels side by side).
func (m AppModel) layout() (cols, numVisibleRows, rowH, available int) {
	// Header and footer each occupy 1 rendered line.
	available = m.height - 2
	if available < 3 {
		available = 3
	}

	cols = m.columns
	if cols < 1 {
		cols = 1
	}

	n := len(m.order)
	if n == 0 {
		numVisibleRows = 0
		rowH = available
		return
	}

	totalRows := (n + cols - 1) / cols

	// Minimum row height is 3 (border + title line + at least one event line).
	numVisibleRows = available / 3
	if numVisibleRows < 1 {
		numVisibleRows = 1
	}
	if numVisibleRows > totalRows {
		numVisibleRows = totalRows
	}

	rowH = available / numVisibleRows
	if rowH < 3 {
		rowH = 3
	}
	return
}

// ensureViewportValid adjusts viewportStart so the focused panel is always visible.
func (m AppModel) ensureViewportValid() AppModel {
	n := len(m.order)
	if n == 0 {
		return m
	}
	cols, numVisible, _, _ := m.layout()
	totalRows := (n + cols - 1) / cols

	focusedRow := m.focused / cols

	// Clamp viewportStart to valid range.
	maxStart := totalRows - numVisible
	if maxStart < 0 {
		maxStart = 0
	}
	if m.viewportStart > maxStart {
		m.viewportStart = maxStart
	}
	if m.viewportStart < 0 {
		m.viewportStart = 0
	}

	// Scroll viewport to include the focused row.
	if focusedRow < m.viewportStart {
		m.viewportStart = focusedRow
	}
	if focusedRow >= m.viewportStart+numVisible {
		m.viewportStart = focusedRow - numVisible + 1
	}
	return m
}

// ── Bubble Tea interface ──────────────────────────────────────────────────────

func (m AppModel) Init() tea.Cmd {
	cmds := make([]tea.Cmd, len(m.watchers)+1)
	for i, w := range m.watchers {
		cmds[i] = waitForEvent(w)
	}
	cmds[len(m.watchers)] = tickCmd()
	return tea.Batch(cmds...)
}

func (m AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tickMsg:
		// Re-issue tick every second to refresh activity indicators.
		return m, tickCmd()

	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		if m.columnsOverride == 0 {
			m.columns = autoColumns(m.width)
		}
		m = m.ensureViewportValid()
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
		return m, reloadConfigCmd(m.cfgFile, m.ctx, m.watchers)

	case configReloadedMsg:
		panels := make(map[string]*PanelModel, len(msg.watchers))
		order := make([]string, 0, len(msg.watchers))
		for _, w := range msg.watchers {
			name := w.Name()
			if old, ok := m.panels[name]; ok {
				panels[name] = old
			} else {
				panels[name] = newPanelModel(w, 100)
			}
			order = append(order, name)
		}
		m.watchers = msg.watchers
		m.panels = panels
		m.order = order
		if m.focused >= len(m.order) {
			m.focused = len(m.order) - 1
		}
		m = m.ensureViewportValid()
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
				m = m.ensureViewportValid()
			}

		case "shift+tab":
			if len(m.order) > 0 {
				m.focused = (m.focused - 1 + len(m.order)) % len(m.order)
				m = m.ensureViewportValid()
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
			// Increase columns (up to 3).
			next := m.columns + 1
			if next > 3 {
				next = 3
			}
			m.columnsOverride = next
			m.columns = next
			m = m.ensureViewportValid()

		case "-":
			// Decrease columns (down to 1).
			next := m.columns - 1
			if next < 1 {
				next = 1
			}
			m.columnsOverride = next
			m.columns = next
			m = m.ensureViewportValid()

		case "=":
			// Reset to auto-detect.
			m.columnsOverride = 0
			m.columns = autoColumns(m.width)
			m = m.ensureViewportValid()

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
	if available < 1 {
		available = 1
	}

	if len(m.order) == 0 {
		return lipgloss.JoinVertical(lipgloss.Left, header, footer)
	}

	cols, numVisible, rowH, _ := m.layout()
	n := len(m.order)

	rowViews := make([]string, numVisible)
	for r := 0; r < numVisible; r++ {
		rowIdx := m.viewportStart + r

		// Last visible row absorbs any leftover height from integer division.
		h := rowH
		if r == numVisible-1 {
			h = available - rowH*(numVisible-1)
			if h < 3 {
				h = 3
			}
		}

		// Build column views for this row.
		colViews := make([]string, cols)
		for c := 0; c < cols; c++ {
			panelIdx := rowIdx*cols + c

			// Distribute width: last column absorbs rounding remainder.
			colW := m.width / cols
			if c == cols-1 {
				colW = m.width - (m.width/cols)*(cols-1)
			}

			if panelIdx >= n {
				// Empty slot — fill with blank space so JoinHorizontal aligns.
				colViews[c] = lipgloss.NewStyle().
					Width(colW).Height(h).Render("")
				continue
			}

			name := m.order[panelIdx]
			w := m.watcherByName(name)
			var st watcher.Status
			if w != nil {
				st = w.Status()
			}
			colViews[c] = m.panels[name].View(colW, h, panelIdx == m.focused, st, m.filterText)
		}

		if cols == 1 {
			rowViews[r] = colViews[0]
		} else {
			rowViews[r] = lipgloss.JoinHorizontal(lipgloss.Top, colViews...)
		}
	}

	body := strings.Join(rowViews, "")
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

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg{}
	})
}

func (m AppModel) watcherByName(name string) watcher.Watcher {
	for _, w := range m.watchers {
		if w.Name() == name {
			return w
		}
	}
	return nil
}

// circledNumber returns the Unicode circled digit for n (1–20), or "[N]" beyond that.
func circledNumber(n int) string {
	circled := []rune{
		'①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩',
		'⑪', '⑫', '⑬', '⑭', '⑮', '⑯', '⑰', '⑱', '⑲', '⑳',
	}
	if n >= 1 && n <= len(circled) {
		return string(circled[n-1])
	}
	return fmt.Sprintf("[%d]", n)
}

func (m AppModel) headerView() string {
	// Build activity indicators — one circled number per panel.
	const activityWindow = 3 * time.Second
	var sb strings.Builder
	for i, name := range m.order {
		p := m.panels[name]
		num := circledNumber(i + 1)

		var style lipgloss.Style
		switch {
		case i == m.focused:
			// Focused panel: always highlighted in the accent colour.
			style = lipgloss.NewStyle().Foreground(colorPurple).Bold(true)

		case !p.lastActivityAt.IsZero() && time.Since(p.lastActivityAt) < activityWindow:
			// Recently active: colour by the type of the last event.
			if p.hasLast {
				style = eventTypeStyle(p.lastEvt.Type).Bold(true)
			} else {
				style = lipgloss.NewStyle().Foreground(colorCyan).Bold(true)
			}

		default:
			style = lipgloss.NewStyle().Foreground(colorGray)
		}
		sb.WriteString(style.Render(num))
	}
	indicators := sb.String()

	n := len(m.watchers)
	noun := "source"
	if n != 1 {
		noun = "sources"
	}

	// Viewport position hint when not all panels fit on screen.
	cols, numVisible, _, _ := m.layout()
	totalRows := (n + cols - 1) / cols
	posHint := ""
	if numVisible < totalRows {
		firstPanel := m.viewportStart*cols + 1
		lastPanel := (m.viewportStart+numVisible)*cols
		if lastPanel > n {
			lastPanel = n
		}
		posHint = fmt.Sprintf("  [%d–%d of %d]", firstPanel, lastPanel, n)
	}

	title := fmt.Sprintf("  AUPHANIM  %s  —  %d %s%s", indicators, n, noun, posHint)
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
	colHint := "="
	if m.columnsOverride > 0 {
		colHint = fmt.Sprintf("%dcol", m.columns)
	}
	help := fmt.Sprintf(
		"  TAB: panel  j/k: scroll  g: latest  /: filter  s: save  +/-: columns(%s)  ENTER: detail  C: clear  Q: quit",
		colHint,
	)
	return styleFooter.Width(m.width).Render(help)
}

// reloadConfigCmd stops old watchers, reloads the config, and starts fresh ones.
func reloadConfigCmd(cfgFile string, ctx context.Context, oldWatchers []watcher.Watcher) tea.Cmd {
	return func() tea.Msg {
		for _, w := range oldWatchers {
			w.Stop()
		}
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

// exportCmd collects all panel events and writes them to a timestamped JSON file.
func exportCmd(panels map[string]*PanelModel, order []string) tea.Cmd {
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
			ExportedAt time.Time     `json:"exported_at"`
			Panels     []exportPanel `json:"panels"`
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

// flashClearCmd returns a command that clears the flash message after d.
func flashClearCmd(d time.Duration) tea.Cmd {
	return func() tea.Msg {
		time.Sleep(d)
		return clearFlashMsg{}
	}
}
