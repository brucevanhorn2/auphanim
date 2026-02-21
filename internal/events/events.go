package events

import "time"

// EventType classifies what happened.
type EventType string

const (
	EventInsert   EventType = "INSERT"
	EventUpdate   EventType = "UPDATE"
	EventDelete   EventType = "DELETE"
	EventMessage  EventType = "MESSAGE"
	EventCreated  EventType = "CREATED"
	EventModified EventType = "MODIFIED"
	EventRemoved  EventType = "REMOVED"
	EventError    EventType = "ERROR"
	EventMetric   EventType = "METRIC" // reserved for future CPU/RAM watchers
)

// WatchEvent is the universal event type passed through all watcher channels.
type WatchEvent struct {
	Timestamp   time.Time
	WatcherName string
	WatcherType string
	Type        EventType
	Summary     string         // pre-formatted single line shown in the event log
	Detail      map[string]any // full structured data shown in the detail overlay
}
