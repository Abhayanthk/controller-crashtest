// Package lab runs a controller in a pretend cluster and drives it through
// a scenario, with or without a fault.
//
//   - target.go  what a controller under test must provide (Target)
//   - probe.go   how faultlab sees inside a client-go controller's work queue
//   - run.go     one run: set up, change, break, restart, settle, snapshot
package lab

import (
	"context"
	"time"

	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// Target is one controller under test. To add a controller, write a package
// under targets/ that returns a Target, and list it in cmd/faultlab.
type Target interface {
	// Name is the short name used on the command line, e.g. "modelserving".
	Name() string
	// NewWorld returns an empty pretend cluster that serves every kind the
	// controller reads or writes.
	NewWorld() *world.World
	// Start starts a new controller process (empty memory) connected
	// through conn, running until ctx ends.
	Start(ctx context.Context, conn *world.Conn) (Controller, error)
	// Scenarios lists the user stories to run.
	Scenarios() []Scenario
}

// Controller is a running controller, seen from outside.
type Controller interface {
	// Busy reports whether work is queued or running, or a requeue is due
	// within horizon, and what it is waiting for.
	Busy(horizon time.Duration) (bool, string)
	// Resync queues every object the controller has seen once more.
	Resync()
	// Stats says how much the controller did.
	Stats() Stats
}

// Stats counts a controller process's reconciles.
type Stats struct {
	Name       string `json:"name"`
	Reconciles int    `json:"reconciles"`
	Errors     int    `json:"errors"`
	LastError  string `json:"lastError,omitempty"`
}

// Scenario is one user story: a starting world and one change to it.
type Scenario struct {
	Name  string
	About string
	// Setup builds the world before the change. The controller then
	// settles it, without faults.
	Setup func(w *world.World) error
	// Change is the action under test. Faults are armed just before it.
	Change func(w *world.World) error
	// Then is an optional second change, e.g. a rollback. The interrupt
	// check makes it while the controller is still working on Change.
	Then func(w *world.World) error
}

// Gauge is a number a target measures on the world after every change while
// the controller works, such as how many ServingGroups are ready. Fault runs
// must not do worse than the run without faults.
type Gauge struct {
	Name string
	// Measure reads the world. It must not write.
	Measure func(w *world.World) int
	// LowerIsWorse: the lowest value matters (availability). Otherwise the
	// highest value matters (resources held).
	LowerIsWorse bool
}

// Measured is implemented by targets that have gauges.
type Measured interface {
	Gauges() []Gauge
}

// Renamer is implemented by targets whose objects carry names that two
// correct runs may choose differently, e.g. which ServingGroup ordinal
// survives a scale-down that arrives mid-rollout. The interrupt check
// compares Rename(state) instead of the raw state.
type Renamer interface {
	Rename(st world.State) world.State
}
