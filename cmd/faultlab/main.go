// Command faultlab breaks kthena's controllers on purpose and checks that
// they still end up right.
//
//	faultlab list                         targets, scenarios and checks
//	faultlab run [flags]                  run the checks, print a summary, save a report
//	faultlab trace -scenario S [-fault F] run one case and print every API call
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/abhayanthk/kthena-faultlab/internal/checks"
	"github.com/abhayanthk/kthena-faultlab/internal/lab"
	"github.com/abhayanthk/kthena-faultlab/internal/report"
	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

const usage = `faultlab breaks kthena's controllers on purpose and checks that they recover.

Usage:
  faultlab list                          show targets, scenarios and checks
  faultlab run   [flags]                 run checks; see "faultlab run -h"
  faultlab trace -scenario S [-fault F]  run one case and print every API call
                                         F is crash:K, error:K or conflict
`

func main() {
	silenceKlog()
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	args := os.Args[2:]
	var err error
	switch os.Args[1] {
	case "list":
		list()
	case "run":
		err = run(args)
	case "trace":
		err = trace(args)
	case "case": // one run, in a child process started by "run"
		err = runCase(args)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "faultlab:", err)
		os.Exit(1)
	}
}

// silenceKlog stops the controllers' logs from flooding the terminal.
func silenceKlog() {
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	_ = fs.Set("logtostderr", "false")
	_ = fs.Set("alsologtostderr", "false")
	_ = fs.Set("stderrthreshold", "FATAL")
	klog.SetOutput(io.Discard)
}

// showKlog sends the controllers' logs to stdout, between faultlab's lines.
func showKlog(v int) {
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	_ = fs.Set("logtostderr", "false")
	_ = fs.Set("v", fmt.Sprint(v))
	klog.SetOutput(os.Stdout)
}

func list() {
	for _, t := range targets {
		fmt.Printf("target %s\n\n  scenarios:\n", t.Name())
		for _, s := range t.Scenarios() {
			fmt.Printf("    %-26s %s\n", s.Name, s.About)
		}
		fmt.Println()
	}
	fmt.Println("checks:")
	for _, c := range checks.All {
		fmt.Printf("    %-26s %s\n", c.Name, c.About)
	}
}

func findTarget(name string) (lab.Target, error) {
	for _, t := range targets {
		if t.Name() == name {
			return t, nil
		}
	}
	return nil, fmt.Errorf("no target %q (see faultlab list)", name)
}

func findScenario(t lab.Target, name string) (lab.Scenario, error) {
	for _, s := range t.Scenarios() {
		if s.Name == name {
			return s, nil
		}
	}
	return lab.Scenario{}, fmt.Errorf("target %s has no scenario %q (see faultlab list)", t.Name(), name)
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	targetName := fs.String("target", "modelserving", "controller to test")
	scenarioList := fs.String("scenario", "", "comma-separated scenarios (default all)")
	checkList := fs.String("check", "", "comma-separated checks (default all)")
	parallel := fs.Int("parallel", 2*runtime.NumCPU(), "runs at the same time")
	retries := fs.Int("retries", 2, "extra runs of a failing case, to tell real from flaky")
	outDir := fs.String("out", "results", "folder for reports")
	inProcess := fs.Bool("inprocess", false, "run every case in this process (a controller panic then stops faultlab)")
	timeout := fs.Duration("timeout", lab.DefaultOptions.Timeout, "give up when a controller does not settle within this")
	_ = fs.Parse(args)

	t, err := findTarget(*targetName)
	if err != nil {
		return err
	}
	scenarios := t.Scenarios()
	if names := split(*scenarioList); len(names) > 0 {
		scenarios = nil
		for _, n := range names {
			s, err := findScenario(t, n)
			if err != nil {
				return err
			}
			scenarios = append(scenarios, s)
		}
	}
	var chosen []checks.Check
	names := split(*checkList)
	for _, c := range checks.All {
		for _, n := range names {
			if n == c.Name {
				chosen = append(chosen, c)
			}
		}
	}
	if len(names) == 0 {
		chosen = checks.All
	} else if len(chosen) != len(names) {
		return fmt.Errorf("unknown check in %q (see faultlab list)", *checkList)
	}

	opts := lab.DefaultOptions
	opts.Timeout = *timeout
	runner := childRunner(t.Name(), opts)
	if *inProcess {
		runner = func(scenario string, plan lab.Plan) lab.Outcome {
			s, _ := findScenario(t, scenario)
			return lab.Run(t, s, plan, opts)
		}
	}
	rep := report.New(t.Name(), scenarios, chosen)
	fmt.Printf("faultlab: %d scenarios, checks %s, %d at a time\n\n", len(scenarios), checkNames(chosen), *parallel)
	suite := &checks.Suite{Run: runner, Checks: names, Parallel: *parallel, Retries: *retries,
		Progress: func(line string) { fmt.Println("  " + line) }}
	if r, ok := t.(lab.Renamer); ok {
		suite.Rename = r.Rename
	}
	rep.Results = suite.RunAll(scenarios)
	rep.Seconds = time.Since(rep.Started).Seconds()

	fmt.Println()
	rep.Print(os.Stdout, isTerminal())
	folder, err := rep.Save(*outDir)
	if err != nil {
		return err
	}
	fmt.Printf("\nReport: %s/report.json (summary.txt next to it)\n", folder)
	for _, r := range rep.Results {
		if r.Status == checks.Fail || r.Status == checks.Error {
			os.Exit(1)
		}
	}
	return nil
}

func checkNames(cs []checks.Check) string {
	var n []string
	for _, c := range cs {
		n = append(n, c.Name)
	}
	return strings.Join(n, ", ")
}

func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// childRunner runs each case in a new faultlab process, so a controller that
// panics or hangs becomes a finding instead of taking faultlab down.
func childRunner(target string, opts lab.Options) checks.Runner {
	self, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return func(scenario string, plan lab.Plan) lab.Outcome {
		planJSON, _ := json.Marshal(plan)
		limit := 5 * opts.Timeout
		ctx, cancel := context.WithTimeout(context.Background(), limit)
		defer cancel()
		cmd := exec.CommandContext(ctx, self, "case", "-target", target, "-scenario", scenario,
			"-plan", string(planJSON), "-timeout", opts.Timeout.String())
		// The mutation detector makes the process panic when the controller
		// changes an object it got from an informer's cache.
		cmd.Env = append(os.Environ(), "KUBE_CACHE_MUTATION_DETECTOR=true")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		runErr := cmd.Run()
		var out lab.Outcome
		if runErr == nil && json.Unmarshal(stdout.Bytes(), &out) == nil {
			return out
		}
		out = lab.Outcome{Target: target, Scenario: scenario, Plan: plan}
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			out.Problem = fmt.Sprintf("the run did not finish within %s; the controller may be stuck", limit)
		case runErr == nil:
			out.Problem = "faultlab: could not read the run's result: " + strings.TrimSpace(clip(stdout.String(), 500))
		default:
			out.Problem = checks.ProcessCrashed + " (" + runErr.Error() + ")\n" + crashText(stderr.String())
			if altered := strings.Index(stdout.String(), "CACHE "); altered >= 0 {
				out.Problem += "\n" + clip(stdout.String()[altered:], 3000)
			}
		}
		return out
	}
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// crashText keeps the panic or fatal error and the top of its stack.
func crashText(stderr string) string {
	lines := strings.Split(stderr, "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "panic:") || strings.HasPrefix(l, "fatal error:") {
			end := i + 30
			if end > len(lines) {
				end = len(lines)
			}
			return strings.Join(lines[i:end], "\n")
		}
	}
	if len(lines) > 30 {
		lines = lines[len(lines)-30:]
	}
	return strings.Join(lines, "\n")
}

func runCase(args []string) error {
	fs := flag.NewFlagSet("case", flag.ExitOnError)
	targetName := fs.String("target", "", "")
	scenarioName := fs.String("scenario", "", "")
	planJSON := fs.String("plan", "{}", "")
	timeout := fs.Duration("timeout", lab.DefaultOptions.Timeout, "")
	_ = fs.Parse(args)
	t, err := findTarget(*targetName)
	if err != nil {
		return err
	}
	s, err := findScenario(t, *scenarioName)
	if err != nil {
		return err
	}
	var plan lab.Plan
	if err := json.Unmarshal([]byte(*planJSON), &plan); err != nil {
		return err
	}
	opts := lab.DefaultOptions
	opts.Timeout = *timeout
	return json.NewEncoder(os.Stdout).Encode(lab.Run(t, s, plan, opts))
}

func trace(args []string) error {
	fs := flag.NewFlagSet("trace", flag.ExitOnError)
	targetName := fs.String("target", "modelserving", "controller to test")
	scenarioName := fs.String("scenario", "", "scenario to run (required)")
	fault := fs.String("fault", "", "crash:K, error:K or conflict (default none)")
	timeout := fs.Duration("timeout", lab.DefaultOptions.Timeout, "give up when the controller does not settle within this")
	verbosity := fs.Int("klog", 0, "show the controller's own logs at this klog verbosity (e.g. 4)")
	_ = fs.Parse(args)
	t, err := findTarget(*targetName)
	if err != nil {
		return err
	}
	s, err := findScenario(t, *scenarioName)
	if err != nil {
		return err
	}
	if *verbosity > 0 {
		showKlog(*verbosity)
	}
	opts := lab.DefaultOptions
	opts.Timeout = *timeout
	out := lab.Run(t, s, lab.Plan{Fault: *fault, Resync: true, Restart: true}, opts)
	for _, e := range out.Log {
		fmt.Println(e.String())
	}
	fmt.Printf("\nsettled: %v   writes after the change: %d   seconds: %.1f\n", out.Settled, out.Writes, out.Seconds)
	if out.Problem != "" {
		fmt.Println("problem:", out.Problem)
	}
	if out.Hit != nil {
		fmt.Println("fault hit:", out.Hit)
	}
	for _, c := range out.Controllers {
		fmt.Printf("%s: %d reconciles, %d failed %s\n", c.Name, c.Reconciles, c.Errors, c.LastError)
	}
	if *fault != "" {
		// An interrupted run is compared with one that made the second
		// change after the first settled.
		ref := lab.Run(t, s, lab.Plan{Then: strings.HasPrefix(*fault, "interrupt")}, opts)
		diffs := world.Diff(ref.State, out.State)
		fmt.Printf("\ndifferences from a run without faults: %d\n", len(diffs))
		for _, d := range diffs {
			fmt.Println("  " + d.String())
		}
	}
	return nil
}
