package world

import (
	"fmt"
	"sync"
	"time"
)

// Entry is one API call (or a phase marker) in the log.
type Entry struct {
	Seq    int     `json:"seq"`
	At     float64 `json:"at"`               // seconds since the world was created
	Actor  string  `json:"actor"`            // "controller-1", "user", "kubelet", "gc", "faultlab"
	Verb   string  `json:"verb"`             // create, update, patch, delete, event, mark
	Kind   string  `json:"kind,omitempty"`   // resource, with "/status" for the status subresource
	Object string  `json:"object,omitempty"` // namespace/name
	Result string  `json:"result"`           // created, updated, no change, deleting, deleted, or the error
	Fault  string  `json:"fault,omitempty"`  // set when faultlab injected this result
}

// Changed reports whether the call changed something stored.
func (e Entry) Changed() bool {
	switch e.Result {
	case "created", "updated", "deleting", "deleted":
		return true
	}
	return false
}

// IsWrite reports whether the entry is a write request (successful or not).
func (e Entry) IsWrite() bool {
	switch e.Verb {
	case "create", "update", "patch", "delete":
		return true
	}
	return false
}

func (e Entry) String() string {
	if e.Verb == "mark" {
		return fmt.Sprintf("--- %s ---", e.Result)
	}
	s := fmt.Sprintf("%-13s %-7s %-26s %-40s %s", e.Actor, e.Verb, e.Kind, e.Object, e.Result)
	if e.Fault != "" {
		s += "  [" + e.Fault + "]"
	}
	return s
}

// Log is the record of every call made in a world.
type Log struct {
	mu      sync.Mutex
	entries []Entry
}

func (l *Log) add(e Entry, start time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.Seq = len(l.entries)
	e.At = time.Since(start).Seconds()
	l.entries = append(l.entries, e)
}

// Len returns the number of entries so far; use it as a bookmark.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Since returns the entries from bookmark i on.
func (l *Log) Since(i int) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i > len(l.entries) {
		i = len(l.entries)
	}
	return append([]Entry(nil), l.entries[i:]...)
}

// Writes returns the write requests by actor in es.
func Writes(es []Entry, actor string) []Entry {
	var out []Entry
	for _, e := range es {
		if e.Actor == actor && e.IsWrite() {
			out = append(out, e)
		}
	}
	return out
}
