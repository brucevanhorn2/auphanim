package ui

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"auphanim/internal/events"
)

// DetailModel renders a full-screen overlay showing the structured data for
// the most recent event in the focused panel.
type DetailModel struct {
	event  events.WatchEvent
	width  int
	height int
}

func newDetailModel(e *events.WatchEvent, width, height int) *DetailModel {
	return &DetailModel{
		event:  *e,
		width:  width,
		height: height,
	}
}

// View renders the detail overlay. It replaces the entire screen.
func (d *DetailModel) View() string {
	innerW := d.width - 6  // 2 border + 2 padding each side
	innerH := d.height - 4 // 2 border + 1 padding each side + 1 slack

	var sb strings.Builder

	// Heading
	sb.WriteString(styleDetailTitle.Render("EVENT DETAIL") + "  ")
	sb.WriteString(lipgloss.NewStyle().Foreground(colorGray).Render("(ESC to close)"))
	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(colorGray).Render(strings.Repeat("─", innerW)))
	sb.WriteString("\n")

	// Metadata rows
	meta := []struct{ label, value string }{
		{"Watcher", d.event.WatcherName},
		{"Type", d.event.WatcherType},
		{"Event", string(d.event.Type)},
		{"Time", d.event.Timestamp.Format("2006-01-02 15:04:05.000")},
		{"Summary", d.event.Summary},
	}
	labelStyle := lipgloss.NewStyle().Foreground(colorBlue).Width(10)
	for _, m := range meta {
		line := labelStyle.Render(m.label+":") + " " + m.value
		if lipgloss.Width(line) > innerW {
			line = line[:innerW]
		}
		sb.WriteString(line + "\n")
	}

	sb.WriteString("\n")
	sb.WriteString(lipgloss.NewStyle().Foreground(colorGray).Render(strings.Repeat("─", innerW)))
	sb.WriteString("\n")

	// JSON detail block
	if d.event.Detail != nil {
		data, err := json.MarshalIndent(d.event.Detail, "", "  ")
		if err == nil {
			jsonLines := strings.Split(string(data), "\n")
			// How many lines do we have left?
			usedLines := 2 + len(meta) + 3 // heading, separator, meta, blank, separator, blank
			remaining := innerH - usedLines
			if remaining < 1 {
				remaining = 1
			}
			if len(jsonLines) > remaining {
				jsonLines = jsonLines[:remaining]
				jsonLines = append(jsonLines, lipgloss.NewStyle().Foreground(colorGray).Render("  … (truncated)"))
			}
			for _, line := range jsonLines {
				colorized := colorizeJSONLine(line, innerW)
				sb.WriteString(colorized + "\n")
			}
		}
	}

	body := sb.String()
	// Strip trailing newline to let the border style control padding.
	body = strings.TrimRight(body, "\n")

	return styleDetailBorder.
		Width(d.width - 6).
		Render(body)
}

// colorizeJSONLine applies syntax highlighting to a single JSON line.
func colorizeJSONLine(line string, maxWidth int) string {
	if len(line) > maxWidth*2 { // rough guard before ANSI processing
		line = line[:maxWidth*2]
	}

	trimmed := strings.TrimLeft(line, " \t")
	indent := line[:len(line)-len(trimmed)]

	// Structural characters with no key
	if trimmed == "{" || trimmed == "}" || trimmed == "[" || trimmed == "]" ||
		trimmed == "{}," || trimmed == "}," || trimmed == "]," {
		return indent + lipgloss.NewStyle().Foreground(colorGray).Render(trimmed)
	}

	// Key: value pair
	if strings.HasPrefix(trimmed, `"`) {
		colonIdx := strings.Index(trimmed, `": `)
		if colonIdx > 0 {
			key := trimmed[:colonIdx+1]
			rest := trimmed[colonIdx+3:]

			coloredKey := styleJSONKey.Render(fmt.Sprintf(`%s`, key)) + ":"
			coloredVal := colorizeJSONValue(rest)
			result := indent + coloredKey + " " + coloredVal
			if lipgloss.Width(result) > maxWidth {
				result = result[:maxWidth]
			}
			return result
		}
	}

	// Fallback: render as-is
	return indent + lipgloss.NewStyle().Foreground(colorWhite).Render(trimmed)
}

// colorizeJSONValue colorizes the value portion of a JSON key:value line.
func colorizeJSONValue(val string) string {
	// Strip trailing comma for display; we'll re-add it after colorizing.
	trailing := ""
	if strings.HasSuffix(val, ",") {
		trailing = ","
		val = val[:len(val)-1]
	}

	var colored string
	switch {
	case val == "null":
		colored = styleJSONNull.Render(val)
	case val == "true" || val == "false":
		colored = styleJSONBool.Render(val)
	case strings.HasPrefix(val, `"`):
		colored = styleJSONString.Render(val)
	case len(val) > 0 && (val[0] == '-' || (val[0] >= '0' && val[0] <= '9')):
		colored = styleJSONNumber.Render(val)
	case val == "{" || val == "[" || val == "{}" || val == "[]":
		colored = lipgloss.NewStyle().Foreground(colorGray).Render(val)
	default:
		colored = lipgloss.NewStyle().Foreground(colorWhite).Render(val)
	}

	return colored + lipgloss.NewStyle().Foreground(colorGray).Render(trailing)
}
