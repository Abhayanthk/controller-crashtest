// Package checks turns runs into verdicts. Every check compares a run against
// a rule that needs no hand-written expected values:
//
//	clean      no faults: the controller settles, and two runs end the same
//	resync     once settled, one more reconcile of everything writes nothing
//	restart    once settled, a new controller process writes nothing
//	crash      crash before write k (every k), restart: same end state as clean
//	api-error  write k fails once with a server error (every k): same end state
//	conflict   first update of each object gets 409 Conflict: same end state
//	interrupt  a second change arrives mid-way (every k): same end state
package checks

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/abhayanthk/controller-crashtest/internal/lab"
	"github.com/abhayanthk/controller-crashtest/internal/world"
)

// Check is one kind of test.
type Check struct {
	Name  string `json:"name"`
	About string `json:"about"`
}

// All lists the checks in the order they are run and reported.
var All = []Check{
	{"clean", "no faults: the controller settles, and two runs end in the same state"},
	{"resync", "once settled, one more reconcile of everything writes nothing"},
	{"restart", "once settled, a new controller process writes nothing and changes nothing"},
	{"crash", "the controller crashes before write k (every k); a new process must reach the same end state"},
	{"api-error", "write k fails once with a server error (every k); the controller must reach the same end state"},
	{"conflict", "the first update of each object gets 409 Conflict after another writer; same end state"},
	{"interrupt", "a second change (rollback, scale, delete) arrives before write k of the first (every k); same end state as making it after the first finished"},
}

// Status is a verdict.
type Status string

const (
	Pass  Status = "pass"
	Warn  Status = "warn"  // works, but wastes something (e.g. writes that change nothing)
	Fail  Status = "fail"  // a rule is broken
	Flaky Status = "flaky" // broken in some runs of the same case only
	Skip  Status = "skip"  // the case did not apply (e.g. write k never happened)
	Error Status = "error" // crashtest or the scenario itself failed
)

// Result is the verdict on one case.
type Result struct {
	Scenario string        `json:"scenario"`
	Check    string        `json:"check"`
	Case     string        `json:"case,omitempty"`
	Status   Status        `json:"status"`
	Summary  string        `json:"summary"`
	Details  []string      `json:"details,omitempty"`
	Hit      *world.Call   `json:"hit,omitempty"`
	Runs     int           `json:"runs"`
	Failed   int           `json:"failed,omitempty"`
	Log      []world.Entry `json:"log,omitempty"` // kept for results that are not pass or skip
	Seconds  float64       `json:"seconds"`
}

// Runner runs one scenario under a plan. The CLI runs each in a child
// process, so a panic in the controller becomes a finding instead of
// stopping crashtest; tests run them in-process.
type Runner func(scenario string, plan lab.Plan) lab.Outcome

// Suite runs checks over scenarios.
type Suite struct {
	Run Runner
	// Rename, when set, is applied to both states before the interrupt
	// check compares them (see lab.Renamer).
	Rename   func(world.State) world.State
	Checks   []string // names to run; empty = all
	Parallel int
	Retries  int          // extra runs of a failing case, to tell real from flaky
	Progress func(string) // called with a line per finished scenario/check
}

func (s *Suite) wants(check string) bool {
	if len(s.Checks) == 0 {
		return true
	}
	for _, c := range s.Checks {
		if c == check {
			return true
		}
	}
	return false
}

type job struct {
	scenario string
	check    string
	k        int
	plan     lab.Plan
	ref      *lab.Outcome
	rename   func(world.State) world.State
}

// RunAll runs the chosen checks on every scenario and returns the results,
// sorted by scenario, check and case.
func (s *Suite) RunAll(scenarios []lab.Scenario) []Result {
	order := map[string]int{}
	for i, sc := range scenarios {
		order[sc.Name] = i
	}
	// Stage 1: a reference run per scenario (no faults, then resync and
	// restart), plus a second clean run to see if the end state repeats.
	// For scenarios with a second change, also a run that makes it after
	// the first settled: the reference for the interrupt check.
	refs := make([]lab.Outcome, len(scenarios))
	twins := make([]lab.Outcome, len(scenarios))
	thens := make([]lab.Outcome, len(scenarios))
	s.pool(len(scenarios)*3, func(i int) {
		sc := scenarios[i/3]
		switch {
		case i%3 == 0:
			refs[i/3] = s.Run(sc.Name, lab.Plan{Resync: true, Restart: true})
		case i%3 == 1 && s.wants("clean"):
			twins[i/3] = s.Run(sc.Name, lab.Plan{})
		case i%3 == 2 && s.wants("interrupt") && sc.Then != nil:
			thens[i/3] = s.Run(sc.Name, lab.Plan{Then: true})
		}
	})

	var results []Result
	var jobs []job
	for i, sc := range scenarios {
		ref := &refs[i]
		rs := referenceResults(s, ref, &twins[i])
		results = append(results, rs...)
		for _, r := range rs {
			s.progress(r.Scenario, r.Check, []Result{r})
		}
		if !ref.Settled {
			continue
		}
		for k := 0; k < ref.Writes; k++ {
			if s.wants("crash") {
				jobs = append(jobs, job{sc.Name, "crash", k, lab.Plan{Fault: fmt.Sprintf("crash:%d", k)}, ref, nil})
			}
			if s.wants("api-error") {
				jobs = append(jobs, job{sc.Name, "api-error", k, lab.Plan{Fault: fmt.Sprintf("error:%d", k)}, ref, nil})
			}
		}
		if s.wants("conflict") {
			jobs = append(jobs, job{sc.Name, "conflict", 0, lab.Plan{Fault: "conflict"}, ref, nil})
		}
		if then := &thens[i]; s.wants("interrupt") && sc.Then != nil {
			if !then.Settled {
				r := Result{Scenario: sc.Name, Check: "interrupt", Status: Fail, Runs: 1, Seconds: then.Seconds,
					Summary: "never settled after the second change, without faults: " + then.Problem, Log: then.Log}
				if runError(then) {
					r.Status, r.Summary = Error, then.Problem
				}
				results = append(results, r)
				s.progress(sc.Name, "interrupt", []Result{r})
				continue
			}
			for k := 0; k < then.Writes; k++ {
				jobs = append(jobs, job{sc.Name, "interrupt", k, lab.Plan{Fault: fmt.Sprintf("interrupt:%d", k)}, then, s.Rename})
			}
		}
	}

	// Stage 2: every fault case, in parallel.
	caseResults := make([]Result, len(jobs))
	var mu sync.Mutex
	left := map[string]int{}
	done := map[string][]Result{}
	for _, j := range jobs {
		left[j.scenario+"/"+j.check]++
	}
	s.pool(len(jobs), func(i int) {
		r := s.runCase(jobs[i])
		caseResults[i] = r
		mu.Lock()
		key := r.Scenario + "/" + r.Check
		left[key]--
		done[key] = append(done[key], r)
		finished := left[key] == 0
		mu.Unlock()
		if finished {
			s.progress(r.Scenario, r.Check, done[key])
		}
	})
	results = append(results, caseResults...)

	checkOrder := map[string]int{}
	for i, c := range All {
		checkOrder[c.Name] = i
	}
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if order[a.Scenario] != order[b.Scenario] {
			return order[a.Scenario] < order[b.Scenario]
		}
		if checkOrder[a.Check] != checkOrder[b.Check] {
			return checkOrder[a.Check] < checkOrder[b.Check]
		}
		return caseIndex(a) < caseIndex(b)
	})
	return results
}

func caseIndex(r Result) int {
	if r.Hit != nil {
		return r.Hit.N
	}
	var k int
	if _, err := fmt.Sscanf(r.Case, "write %d", &k); err == nil {
		return k
	}
	return 0
}

func (s *Suite) progress(scenario, check string, rs []Result) {
	if s.Progress == nil {
		return
	}
	counts := map[Status]int{}
	for _, r := range rs {
		counts[r.Status]++
	}
	var parts []string
	for _, st := range []Status{Pass, Warn, Fail, Flaky, Skip, Error} {
		if counts[st] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[st], st))
		}
	}
	s.Progress(fmt.Sprintf("%-26s %-10s %s", scenario, check, strings.Join(parts, ", ")))
}

func (s *Suite) pool(n int, f func(i int)) {
	p := s.Parallel
	if p < 1 {
		p = 1
	}
	next := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < p; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				f(i)
			}
		}()
	}
	for i := 0; i < n; i++ {
		next <- i
	}
	close(next)
	wg.Wait()
}

// runCase runs a fault case, and runs it again (up to Retries times) if it
// fails, to tell a real failure from a timing-dependent one.
func (s *Suite) runCase(j job) Result {
	out := s.Run(j.scenario, j.plan)
	r := judgeFault(j, &out)
	r.Runs = 1
	if r.Status != Fail {
		return r
	}
	r.Failed = 1
	for i := 0; i < s.Retries; i++ {
		again := s.Run(j.scenario, j.plan)
		r.Runs++
		r.Seconds += again.Seconds
		if judgeFault(j, &again).Status == Fail {
			r.Failed++
		}
	}
	if r.Failed < r.Runs {
		r.Status = Flaky
		r.Summary += fmt.Sprintf(" (in %d of %d runs)", r.Failed, r.Runs)
	} else if r.Runs > 1 {
		r.Summary += fmt.Sprintf(" (in all %d runs)", r.Runs)
	}
	return r
}

// judgeFault compares a fault run with the reference run.
func judgeFault(j job, out *lab.Outcome) Result {
	r := Result{Scenario: j.scenario, Check: j.check, Hit: out.Hit, Seconds: out.Seconds}
	switch j.check {
	case "crash":
		r.Case = fmt.Sprintf("crash before write %d", j.k)
	case "api-error":
		r.Case = fmt.Sprintf("API error on write %d", j.k)
	case "interrupt":
		r.Case = fmt.Sprintf("second change before write %d", j.k)
	}
	if out.Hit != nil {
		r.Case += ": " + out.Hit.Verb + " " + out.Hit.Kind + " " + out.Hit.Object
	}
	switch {
	case runError(out):
		r.Status, r.Summary = Error, out.Problem
	case strings.HasPrefix(out.Problem, panicked):
		r.Status, r.Summary, r.Details = Fail, crashSummary(out.Problem), strings.Split(out.Problem, "\n")
	case out.Hit == nil && j.check != "conflict":
		r.Status, r.Summary = Skip, fmt.Sprintf("write %d never happened in this run", j.k)
		if j.check == "interrupt" {
			r.Summary += "; the second change was made after the first settled"
		}
	case out.Hit == nil:
		r.Status, r.Summary = Skip, "the controller made no update or patch to conflict with"
	case !out.Settled:
		r.Status, r.Summary = Fail, "never settled: "+out.Problem
	default:
		want, got := j.ref.State, out.State
		if j.rename != nil {
			want, got = j.rename(want), j.rename(got)
		}
		diffs := world.Diff(want, got)
		var worse []string
		// Gauges compare runs of the same change with and without a fault.
		// The interrupt check's reference makes the two changes one after
		// the other, passing through states (e.g. 3 replicas mid-rollout)
		// an interleaved run never has, so its gauges are not comparable.
		if j.check != "interrupt" {
			worse = worseGauges(j.ref.Gauges, out.Gauges)
		}
		switch {
		case len(diffs) > 0:
			r.Status, r.Summary = Fail, fmt.Sprintf("end state differs from the run without faults (%d differences)", len(diffs))
			r.Details = append(worse, diffLines(diffs)...)
		case len(worse) > 0:
			r.Status, r.Summary = Fail, "same end state, but worse along the way: "+strings.Join(worse, "; ")
		default:
			r.Status, r.Summary = Pass, "same end state as the run without faults"
			return r
		}
	}
	if r.Status != Pass && r.Status != Skip {
		r.Log = out.Log
	}
	return r
}

// panicked starts the Problem of a run whose controller process died
// (see cmd/crashtest); the rest of the Problem is the panic and its stack.
const panicked = ProcessCrashed

// ProcessCrashed starts the Problem of a run whose child process died.
const ProcessCrashed = "the controller process crashed"

// worseGauges lists the gauges a fault run did worse on than the reference.
func worseGauges(ref, got map[string]lab.Range) []string {
	var out []string
	for _, name := range sortedKeys(ref) {
		want, ok := got[name]
		if !ok {
			continue
		}
		have := ref[name]
		if have.LowerIsWorse && want.Min < have.Min {
			out = append(out, fmt.Sprintf("%s fell to %d (%d without faults)", name, want.Min, have.Min))
		}
		if !have.LowerIsWorse && want.Max > have.Max {
			out = append(out, fmt.Sprintf("%s rose to %d (%d without faults)", name, want.Max, have.Max))
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// crashSummary names what killed a controller process.
func crashSummary(problem string) string {
	if strings.Contains(problem, "panic: cache ") && strings.Contains(problem, " modified") {
		return "the controller changed an object inside its informer cache (KUBE_CACHE_MUTATION_DETECTOR)"
	}
	if strings.Contains(problem, "fatal error: concurrent map") {
		return "the controller process died: concurrent map access"
	}
	return "the controller process panicked"
}

// runError reports problems that are crashtest's or the scenario's, not the controller's.
func runError(out *lab.Outcome) bool {
	for _, p := range []string{"setup:", "change:", "start controller", "fault ", "unknown fault", "crashtest:"} {
		if strings.HasPrefix(out.Problem, p) {
			return true
		}
	}
	return false
}

func diffLines(diffs []world.Difference) []string {
	var out []string
	for i, d := range diffs {
		if i == 40 {
			out = append(out, fmt.Sprintf("... and %d more", len(diffs)-i))
			break
		}
		out = append(out, d.String())
	}
	return out
}

// referenceResults judges clean, resync and restart from the reference run.
func referenceResults(s *Suite, ref, twin *lab.Outcome) []Result {
	var out []Result
	base := Result{Scenario: ref.Scenario, Runs: 1, Seconds: ref.Seconds}
	if runError(ref) {
		r := base
		r.Check, r.Status, r.Summary, r.Log = "clean", Error, ref.Problem, ref.Log
		return []Result{r}
	}
	if s.wants("clean") {
		r := base
		r.Check = "clean"
		switch {
		case strings.HasPrefix(ref.Problem, panicked):
			r.Status, r.Summary, r.Details = Fail, crashSummary(ref.Problem), strings.Split(ref.Problem, "\n")
		case !ref.Settled:
			r.Status, r.Summary, r.Log = Fail, "never settled: "+ref.Problem, ref.Log
		case twin.Settled && len(world.Diff(ref.State, twin.State)) > 0:
			d := world.Diff(ref.State, twin.State)
			r.Status, r.Summary, r.Details, r.Log = Fail, "two runs without faults ended in different states", diffLines(d), twin.Log
		default:
			r.Status = Pass
			r.Summary = fmt.Sprintf("settled; %d writes after the change", ref.Writes)
			if n, last := failedReconciles(ref); n > 0 {
				r.Status, r.Log = Warn, ref.Log
				r.Summary = fmt.Sprintf("settled, but %d reconciles failed without any fault; last: %s", n, last)
			}
		}
		out = append(out, r)
	}
	if !ref.Settled {
		return out
	}
	if s.wants("resync") {
		r := base
		r.Check = "resync"
		r.Status, r.Summary, r.Details = judgeWrites(ref.ResyncWrites, "one more reconcile")
		if r.Status != Pass {
			r.Log = ref.Log
		}
		out = append(out, r)
	}
	if s.wants("restart") {
		r := base
		r.Check = "restart"
		r.Status, r.Summary, r.Details = judgeWrites(ref.RestartWrites, "a new controller process")
		if d := world.Diff(ref.State, ref.RestartState); len(d) > 0 {
			r.Status = Fail
			r.Summary = "a restart changed the end state: " + r.Summary
			r.Details = append(r.Details, diffLines(d)...)
		}
		if r.Status != Pass {
			r.Log = ref.Log
		}
		out = append(out, r)
	}
	return out
}

func failedReconciles(o *lab.Outcome) (int, string) {
	n, last := 0, ""
	for _, c := range o.Controllers {
		n += c.Errors
		if c.LastError != "" {
			last = c.LastError
		}
	}
	return n, last
}

// judgeWrites: writes that change something on a settled world break the
// rule (fail); writes that change nothing only waste API calls (warn).
func judgeWrites(es []world.Entry, who string) (Status, string, []string) {
	if len(es) == 0 {
		return Pass, who + " wrote nothing", nil
	}
	changed := 0
	var lines []string
	for _, e := range es {
		if e.Changed() {
			changed++
		}
		lines = append(lines, e.String())
	}
	if changed > 0 {
		return Fail, fmt.Sprintf("%s made %d writes, %d of them changed something", who, len(es), changed), lines
	}
	return Warn, fmt.Sprintf("%s made %d writes that changed nothing (wasted API calls)", who, len(es)), lines
}
