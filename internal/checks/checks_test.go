package checks

import (
	"flag"
	"io"
	"os"
	"testing"
	"time"

	"k8s.io/klog/v2"

	"github.com/abhayanthk/controller-crashtest/internal/lab"
	"github.com/abhayanthk/controller-crashtest/internal/toy"
)

func TestMain(m *testing.M) {
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	_ = fs.Set("logtostderr", "false")
	_ = fs.Set("alsologtostderr", "false")
	_ = fs.Set("stderrthreshold", "FATAL")
	klog.SetOutput(io.Discard)
	os.Exit(m.Run())
}

var fast = lab.Options{Quiet: 60 * time.Millisecond, Horizon: time.Second, Timeout: 15 * time.Second}

func runToy(t *testing.T, bugs toy.Bugs) map[string][]Result {
	t.Helper()
	target := toy.Target(bugs)
	scenarios := target.Scenarios()
	suite := &Suite{Parallel: 8, Run: func(name string, plan lab.Plan) lab.Outcome {
		for _, s := range scenarios {
			if s.Name == name {
				return lab.Run(target, s, plan, fast)
			}
		}
		t.Fatalf("no scenario %q", name)
		return lab.Outcome{}
	}}
	byCheck := map[string][]Result{}
	for _, r := range suite.RunAll(scenarios) {
		byCheck[r.Check] = append(byCheck[r.Check], r)
	}
	return byCheck
}

func worst(rs []Result) Status {
	rank := map[Status]int{Pass: 0, Skip: 0, Warn: 1, Flaky: 2, Fail: 3, Error: 4}
	w := Pass
	for _, r := range rs {
		if rank[r.Status] > rank[w] {
			w = r.Status
		}
	}
	return w
}

func expect(t *testing.T, got map[string][]Result, want map[string]Status) {
	t.Helper()
	for _, c := range All {
		w, ok := want[c.Name]
		if !ok {
			w = Pass
		}
		if g := worst(got[c.Name]); g != w {
			t.Errorf("check %s: got %s, want %s", c.Name, g, w)
			for _, r := range got[c.Name] {
				if r.Status != Pass && r.Status != Skip {
					t.Logf("  %s %s %s: %s %v", r.Scenario, r.Case, r.Status, r.Summary, r.Details)
				}
			}
		}
	}
}

// The bug-free controller must pass every check: no false alarms.
func TestCorrectControllerPasses(t *testing.T) {
	expect(t, runToy(t, toy.Bugs{}), nil)
}

// A name kept only in memory leaks a second copy when the process restarts.
func TestFindsStateLostOnRestart(t *testing.T) {
	expect(t, runToy(t, toy.Bugs{ForgetOnRestart: true}), map[string]Status{"restart": Fail, "crash": Fail})
}

// Writing an unchanged status on every reconcile wastes API calls.
func TestFindsWastedWrites(t *testing.T) {
	expect(t, runToy(t, toy.Bugs{RewriteStatus: true}), map[string]Status{"resync": Warn, "restart": Warn})
}

// A controller that drops a failed write never retries it.
func TestFindsSwallowedErrors(t *testing.T) {
	expect(t, runToy(t, toy.Bugs{SwallowErrors: true}), map[string]Status{"api-error": Fail, "conflict": Fail})
}
