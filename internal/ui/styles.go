package ui

import (
	"github.com/charmbracelet/lipgloss"

	"auphanim/internal/events"
	"auphanim/internal/watcher"
)

// Palette
var (
	colorGreen  = lipgloss.Color("#00FF87")
	colorYellow = lipgloss.Color("#FFD700")
	colorRed    = lipgloss.Color("#FF5555")
	colorCyan   = lipgloss.Color("#55FFFF")
	colorPurple = lipgloss.Color("#7C3AED")
	colorBlue   = lipgloss.Color("#87AFFF")
	colorGray   = lipgloss.Color("#666666")
	colorWhite  = lipgloss.Color("#DDDDDD")
)

// Base styles
var (
	styleBorderNormal = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#444444"))

	styleBorderFocused = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorPurple)

	styleHeader = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorBlue).
			Background(lipgloss.Color("#1A1A2E")).
			Padding(0, 2)

	styleFooter = lipgloss.NewStyle().
			Foreground(colorGray).
			Padding(0, 2)

	stylePanelTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorBlue)

	styleEventSelected = lipgloss.NewStyle().
				Foreground(colorWhite).
				Background(colorPurple).
				Bold(true)

	styleTimestamp = lipgloss.NewStyle().
			Foreground(colorGray)

	styleDetailBorder = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorPurple).
				Padding(1, 2)

	styleDetailTitle = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorPurple)

	styleJSONKey    = lipgloss.NewStyle().Foreground(colorBlue)
	styleJSONString = lipgloss.NewStyle().Foreground(colorGreen)
	styleJSONNumber = lipgloss.NewStyle().Foreground(colorCyan)
	styleJSONBool   = lipgloss.NewStyle().Foreground(colorYellow)
	styleJSONNull   = lipgloss.NewStyle().Foreground(colorGray)
)

// eventTypeStyle returns the lipgloss style for a given EventType.
func eventTypeStyle(t events.EventType) lipgloss.Style {
	switch t {
	case events.EventInsert, events.EventCreated:
		return lipgloss.NewStyle().Foreground(colorGreen).Bold(true)
	case events.EventUpdate, events.EventModified:
		return lipgloss.NewStyle().Foreground(colorYellow).Bold(true)
	case events.EventDelete, events.EventRemoved:
		return lipgloss.NewStyle().Foreground(colorRed).Bold(true)
	case events.EventMessage:
		return lipgloss.NewStyle().Foreground(colorCyan).Bold(true)
	case events.EventError:
		return lipgloss.NewStyle().Foreground(colorRed).Bold(true)
	default:
		return lipgloss.NewStyle().Foreground(colorWhite)
	}
}

// statusDot returns a colored ● character for the watcher status.
func statusDot(s watcher.Status) string {
	switch s {
	case watcher.StatusConnected:
		return lipgloss.NewStyle().Foreground(colorGreen).Render("●")
	case watcher.StatusConnecting:
		return lipgloss.NewStyle().Foreground(colorYellow).Render("●")
	case watcher.StatusError:
		return lipgloss.NewStyle().Foreground(colorRed).Render("●")
	default:
		return lipgloss.NewStyle().Foreground(colorGray).Render("●")
	}
}
