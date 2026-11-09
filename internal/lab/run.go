package lab

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// Plan says what to break in a run.
type Plan struct {
	// Fault is armed just before the scenario's change:
	// "" (none), "crash:K", "error:K" or "conflict". See ParseFault.
	Fault string `json:"fault,omitempty"`
	// Resync: once settled, queue every object again (one more reconcile
	// each) and record the writes it causes.
	Resync bool `json:"resync,omitempty"`
	// Restart: once settled, replace the controller process with a new one
	// and record the writes the new one makes.
	Restart bool `json:"restart,omitempty"`
	// Then: make the scenario's second change once the first has settled
	// (unless an interrupt fault already made it mid-way), and settle again.
	Then bool `json:"then,omitempty"`
}

// Options tune how long the runner waits.
type Options struct {
	// Quiet: the controller counts as idle after this long with no queued
	// work, no reconcile running and no writes.
	Quiet time.Duration
	// Horizon: requeues due within this long are waited for; later ones
	// are treated as background timers.
	Horizon time.Duration
	// Timeout: give up when a settle takes longer than this.
	Timeout time.Duration
}

// DefaultOptions suit kthena's controllers, which requeue after 1 s.
var DefaultOptions = Options{Quiet: 150 * time.Millisecond, Horizon: 3 * time.Second, Timeout: 60 * time.Second}

// Outcome is what happened in one run.
type Outcome struct {
	Target   string `json:"target"`
	Scenario string `json:"scenario"`
	Plan     Plan   `json:"plan"`

	Settled bool   `json:"settled"`
	Problem string `json:"problem,omitempty"` // why the run did not settle or could not run
	// Hit is the write the fault failed first; nil when the fault never fired.
	Hit *world.Call `json:"hit,omitempty"`
	// Writes counts the controller's writes after the change (up to a crash).
	Writes int `json:"writes"`
	// State is the end state (before any resync or restart).
	State world.State `json:"state,omitempty"`

	ResyncWrites  []world.Entry `json:"resyncWrites,omitempty"`
	RestartWrites []world.Entry `json:"restartWrites,omitempty"`
	RestartState  world.State   `json:"restartState,omitempty"`

	// Gauges: the lowest and highest value of each gauge from the change
	// to the end state.
	Gauges map[string]Range `json:"gauges,omitempty"`

	Controllers []Stats       `json:"controllers,omitempty"`
	Log         []world.Entry `json:"log,omitempty"`
	Seconds     float64       `json:"seconds"`
}

// Range is the lowest and highest value a gauge took.
type Range struct {
	Min          int  `json:"min"`
	Max          int  `json:"max"`
	LowerIsWorse bool `json:"lowerIsWorse"`
}

// gauges records each gauge's range while switched on.
type gauges struct {
	w    *world.World
	list []Gauge

	mu     sync.Mutex
	on     bool
	ranges map[string]Range
}

func newGauges(t Target, w *world.World) *gauges {
	g := &gauges{w: w, ranges: map[string]Range{}}
	if m, ok := t.(Measured); ok {
		g.list = m.Gauges()
	}
	w.OnChange(g.measure)
	return g
}

func (g *gauges) start() {
	g.mu.Lock()
	g.on = true
	g.mu.Unlock()
	g.measure()
}

func (g *gauges) stop() map[string]Range {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.on = false
	if len(g.ranges) == 0 {
		return nil
	}
	return g.ranges
}

func (g *gauges) measure() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.on {
		return
	}
	for _, gauge := range g.list {
		v := gauge.Measure(g.w)
		r, seen := g.ranges[gauge.Name]
		if !seen {
			r = Range{Min: v, Max: v, LowerIsWorse: gauge.LowerIsWorse}
		}
		r.Min, r.Max = min(r.Min, v), max(r.Max, v)
		g.ranges[gauge.Name] = r
	}
}

// ParseFault turns a plan's fault string into a world.Fault:
// crash:K, error:K, conflict, or interrupt:K (makes the scenario's Then
// just before write K).
func ParseFault(spec string, sc Scenario) (world.Fault, error) {
	name, arg, _ := strings.Cut(spec, ":")
	if name == "" {
		return nil, nil
	}
	if name == "conflict" {
		return world.ConflictOnce(), nil
	}
	k, err := strconv.Atoi(arg)
	if err != nil || k < 0 {
		return nil, fmt.Errorf("fault %q: want %s:K with K >= 0", spec, name)
	}
	switch name {
	case "crash":
		return world.CrashAt(k), nil
	case "error":
		return world.FailOnce(k), nil
	case "interrupt":
		if sc.Then == nil {
			return nil, fmt.Errorf("fault %q: scenario %s has no second change (Then)", spec, sc.Name)
		}
		return world.InterruptAt(k, sc.Then), nil
	}
	return nil, fmt.Errorf("unknown fault %q (want crash:K, error:K, interrupt:K or conflict)", spec)
}

// Run runs one scenario on target under plan, in a new world.
func Run(t Target, s Scenario, p Plan, o Options) (out Outcome) {
	began := time.Now()
	out = Outcome{Target: t.Name(), Scenario: s.Name, Plan: p}
	fault, err := ParseFault(p.Fault, s)
	if err != nil {
		out.Problem = err.Error()
		return out
	}
	interrupt, _ := fault.(*world.Interrupt)
	w := t.NewWorld()
	g := newGauges(t, w)
	r := &runner{t: t, w: w, o: o}
	defer func() {
		r.stopAll()
		out.Log = w.Log().Since(0)
		out.Controllers = r.stats()
		out.Seconds = time.Since(began).Seconds()
	}()

	w.Mark("setup")
	if err := s.Setup(w); err != nil {
		out.Problem = "setup: " + err.Error()
		return out
	}
	c, conn, err := r.start()
	if err != nil {
		out.Problem = err.Error()
		return out
	}
	if err := r.settle(c, conn); err != nil {
		out.Problem = "the starting world did not settle: " + err.Error()
		return out
	}

	w.Mark("change")
	conn.Arm(fault)
	g.start()
	if err := s.Change(w); err != nil {
		out.Problem = "change: " + err.Error()
		return out
	}
	err = r.settle(c, conn)
	out.Writes, out.Hit = conn.Writes(), conn.Hit()
	if conn.Dead() {
		w.Mark("crash: " + fault.Name() + ", starting a new controller process")
		r.stopLast()
		if c, conn, err = r.start(); err == nil {
			err = r.settle(c, conn)
		}
	}
	if err == nil && s.Then != nil && (p.Then || interrupt != nil) {
		fired, thenErr := false, error(nil)
		if interrupt != nil {
			fired, thenErr = interrupt.Fired()
		}
		if !fired {
			w.Mark("second change")
			thenErr = s.Then(w)
			if thenErr == nil {
				err = r.settle(c, conn)
			}
		}
		if thenErr != nil {
			out.Problem = "change: second change: " + thenErr.Error()
			return out
		}
	}
	out.Gauges = g.stop()
	out.State = w.Snapshot()
	if err != nil {
		out.Problem = err.Error()
		return out
	}
	out.Settled = true

	if p.Resync {
		w.Mark("resync: one more reconcile of everything")
		mark := w.Log().Len()
		c.Resync()
		if err := r.settle(c, conn); err != nil {
			out.Problem = "after resync: " + err.Error()
		}
		out.ResyncWrites = world.Writes(w.Log().Since(mark), conn.Name())
	}
	if p.Restart {
		w.Mark("restart: a new controller process")
		r.stopLast()
		mark := w.Log().Len()
		if c, conn, err = r.start(); err == nil {
			err = r.settle(c, conn)
		}
		if err != nil {
			out.Problem = "after restart: " + err.Error()
		}
		out.RestartWrites = world.Writes(w.Log().Since(mark), conn.Name())
		out.RestartState = w.Snapshot()
	}
	return out
}

type process struct {
	c      Controller
	conn   *world.Conn
	cancel context.CancelFunc
}

type runner struct {
	t     Target
	w     *world.World
	o     Options
	procs []process
}

func (r *runner) start() (Controller, *world.Conn, error) {
	conn := r.w.Connect()
	ctx, cancel := context.WithCancel(context.Background())
	c, err := r.t.Start(ctx, conn)
	if err != nil {
		cancel()
		conn.Close()
		return nil, nil, fmt.Errorf("start %s: %w", conn.Name(), err)
	}
	r.procs = append(r.procs, process{c: c, conn: conn, cancel: cancel})
	return c, conn, nil
}

// stopLast stops the newest process and waits (up to 5 s) until none of
// its reconciles is still running, so nothing of it overlaps the next one.
func (r *runner) stopLast() {
	n := len(r.procs)
	if n == 0 {
		return
	}
	p := r.procs[n-1]
	p.conn.Close()
	p.cancel()
	if d, ok := p.c.(interface{ Drained() bool }); ok {
		for deadline := time.Now().Add(5 * time.Second); !d.Drained() && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (r *runner) stopAll() {
	for _, p := range r.procs {
		p.conn.Close()
		p.cancel()
	}
}

func (r *runner) stats() []Stats {
	var out []Stats
	for _, p := range r.procs {
		s := p.c.Stats()
		s.Name = p.conn.Name()
		out = append(out, s)
	}
	return out
}

// settle waits until the controller is idle and the kubelet has nothing left
// to do. It returns early, without error, when the connection dies (a crash).
func (r *runner) settle(c Controller, conn *world.Conn) error {
	deadline := time.Now().Add(r.o.Timeout)
	for round := 0; round < 500; round++ {
		// The kubelet acts as soon as the controller pauses, like a real one
		// that doesn't wait for the controller's timers...
		if err := r.waitIdle(c, conn, 0, deadline); err != nil || conn.Dead() {
			return err
		}
		if r.w.KubeletTick() > 0 {
			continue
		}
		// ...but the run only ends once requeues due soon have run too.
		if err := r.waitIdle(c, conn, r.o.Horizon, deadline); err != nil || conn.Dead() {
			return err
		}
		if r.w.KubeletTick() == 0 {
			return nil
		}
	}
	return fmt.Errorf("pods never stopped changing (500 kubelet rounds)")
}

func (r *runner) waitIdle(c Controller, conn *world.Conn, horizon time.Duration, deadline time.Time) error {
	var idleSince time.Time
	for !conn.Dead() {
		busy, why := c.Busy(horizon)
		if !busy && r.w.QuietFor(r.o.Quiet) {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) >= r.o.Quiet {
				return nil
			}
		} else {
			idleSince = time.Time{}
		}
		if time.Now().After(deadline) {
			return r.notSettled(c, why)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// notSettled explains a timeout: what the controller kept doing.
func (r *runner) notSettled(c Controller, why string) error {
	log := r.w.Log().Since(0)
	recent := map[string]int{}
	if n := len(log); n > 0 {
		last := log[n-1].At
		for _, e := range log {
			if e.IsWrite() && e.At > last-10 {
				recent[e.Actor+" "+e.Verb+" "+e.Kind+" "+e.Object+" -> "+e.Result]++
			}
		}
	}
	keys := make([]string, 0, len(recent))
	for k := range recent {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if recent[keys[i]] != recent[keys[j]] {
			return recent[keys[i]] > recent[keys[j]]
		}
		return keys[i] < keys[j]
	})
	var top []string
	for i := 0; i < len(keys) && i < 3; i++ {
		top = append(top, fmt.Sprintf("%dx %s", recent[keys[i]], keys[i]))
	}
	st := c.Stats()
	if why == "" {
		why = "queue empty, but writes keep coming"
	}
	msg := fmt.Sprintf("did not settle within %s (controller: %s; %d reconciles, %d failed", r.o.Timeout, why, st.Reconciles, st.Errors)
	if st.LastError != "" {
		msg += "; last error: " + st.LastError
	}
	msg += ")"
	if len(top) > 0 {
		msg += "; writes in the last 10s: " + strings.Join(top, "; ")
	}
	return fmt.Errorf("%s", msg)
}
