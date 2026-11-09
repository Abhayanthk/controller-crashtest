// Package report writes results for people (a table in the terminal) and
// for tools (report.json, which the web UI will read).
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/abhayanthk/controller-crashtest/internal/checks"
	"github.com/abhayanthk/controller-crashtest/internal/lab"
)

// Report is everything one `crashtest run` found.
type Report struct {
	Tool      string          `json:"tool"`
	Target    string          `json:"target"`
	Kthena    string          `json:"kthena"` // kthena module version (or local path) the run used
	Started   time.Time       `json:"started"`
	Seconds   float64         `json:"seconds"`
	Scenarios []Scenario      `json:"scenarios"`
	Checks    []checks.Check  `json:"checks"`
	Results   []checks.Result `json:"results"`
}

// Scenario describes one scenario that ran.
type Scenario struct {
	Name  string `json:"name"`
	About string `json:"about"`
}

// New starts a report.
func New(target string, scenarios []lab.Scenario, cs []checks.Check) *Report {
	r := &Report{Tool: "controller-crashtest", Target: target, Kthena: KthenaVersion(), Started: time.Now(), Checks: cs}
	for _, s := range scenarios {
		r.Scenarios = append(r.Scenarios, Scenario{s.Name, s.About})
	}
	return r
}

// KthenaVersion says which kthena this binary was built against.
func KthenaVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, d := range info.Deps {
		if d.Path == "github.com/volcano-sh/kthena" {
			if d.Replace != nil {
				return d.Replace.Path + " (local)"
			}
			if d.Version == "(devel)" {
				return "local checkout (go.work)"
			}
			return d.Version
		}
	}
	return "unknown"
}

// Save writes report.json and summary.txt into a new folder under dir and
// returns the folder.
func (r *Report) Save(dir string) (string, error) {
	folder := filepath.Join(dir, r.Started.Format("2006-01-02_15-04-05"))
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(folder, "report.json"), data, 0o644); err != nil {
		return "", err
	}
	f, err := os.Create(filepath.Join(folder, "summary.txt"))
	if err != nil {
		return "", err
	}
	defer f.Close()
	r.Print(f, false)
	return folder, nil
}

type palette struct{ green, yellow, red, gray, bold, reset string }

func colors(on bool) palette {
	if !on {
		return palette{}
	}
	return palette{"\033[32m", "\033[33m", "\033[31m", "\033[90m", "\033[1m", "\033[0m"}
}

// Print writes the summary table and the findings.
func (r *Report) Print(w io.Writer, color bool) {
	c := colors(color)
	runs := 0
	for _, res := range r.Results {
		runs += res.Runs
	}
	fmt.Fprintf(w, "%scontroller-crashtest%s · target %s · kthena %s\n", c.bold, c.reset, r.Target, r.Kthena)
	fmt.Fprintf(w, "%d scenarios · %d checks · %d runs · %s\n\n", len(r.Scenarios), len(r.Checks), runs, time.Duration(r.Seconds*float64(time.Second)).Round(time.Second))

	byCell := map[string][]checks.Result{}
	for _, res := range r.Results {
		byCell[res.Scenario+"/"+res.Check] = append(byCell[res.Scenario+"/"+res.Check], res)
	}
	fmt.Fprintf(w, "%-26s", "scenario")
	for _, ch := range r.Checks {
		fmt.Fprintf(w, " %-12s", ch.Name)
	}
	fmt.Fprintln(w)
	for _, s := range r.Scenarios {
		fmt.Fprintf(w, "%-26s", s.Name)
		for _, ch := range r.Checks {
			text, col := cell(byCell[s.Name+"/"+ch.Name], c)
			fmt.Fprintf(w, " %s%-12s%s", col, text, c.reset)
		}
		fmt.Fprintln(w)
	}

	groups := findings(r.Results)
	if len(groups) == 0 {
		fmt.Fprintf(w, "\n%sNo findings.%s\n", c.green, c.reset)
		return
	}
	fmt.Fprintf(w, "\n%sFindings (%d)%s\n", c.bold, len(groups), c.reset)
	for _, g := range groups {
		col := c.red
		if g.status == checks.Warn || g.status == checks.Flaky {
			col = c.yellow
		}
		fmt.Fprintf(w, "\n%s%s%s  %s: %s\n", col, strings.ToUpper(string(g.status)), c.reset, g.check, g.summary)
		for i, where := range g.where {
			if i == 8 {
				fmt.Fprintf(w, "   %s... and %d more%s\n", c.gray, len(g.where)-i, c.reset)
				break
			}
			fmt.Fprintf(w, "   %sin %s%s\n", c.gray, where, c.reset)
		}
		for i, d := range g.details {
			if i == 12 {
				fmt.Fprintf(w, "   ... (%d more lines in report.json)\n", len(g.details)-i)
				break
			}
			fmt.Fprintf(w, "   %s\n", d)
		}
	}
}

func cell(rs []checks.Result, c palette) (string, string) {
	if len(rs) == 0 {
		return "", ""
	}
	n := map[checks.Status]int{}
	for _, r := range rs {
		n[r.Status]++
	}
	total := len(rs) - n[checks.Skip]
	count := func(k int) string {
		if len(rs) == 1 {
			return ""
		}
		return fmt.Sprintf(" %d/%d", k, total)
	}
	switch {
	case n[checks.Error] > 0:
		return "error", c.red
	case n[checks.Fail] > 0:
		return "FAIL" + count(n[checks.Fail]), c.red
	case n[checks.Flaky] > 0:
		return "flaky" + count(n[checks.Flaky]), c.yellow
	case n[checks.Warn] > 0:
		return "warn" + count(n[checks.Warn]), c.yellow
	case total == 0:
		return "-", c.gray
	}
	if len(rs) == 1 {
		return "ok", c.green
	}
	return fmt.Sprintf("ok %d", total), c.green
}

type group struct {
	status  checks.Status
	check   string
	summary string
	details []string
	where   []string // "scenario" or "scenario: case"
}

// findings groups results that failed the same way (same check, summary and
// details), so one cause seen in 12 crash points or 10 scenarios reads as
// one finding.
func findings(rs []checks.Result) []group {
	var out []group
	index := map[string]int{}
	for _, r := range rs {
		if r.Status == checks.Pass || r.Status == checks.Skip {
			continue
		}
		key := strings.Join(append([]string{string(r.Status), r.Check, r.Summary}, r.Details...), "\n")
		i, ok := index[key]
		if !ok {
			i = len(out)
			index[key] = i
			out = append(out, group{status: r.Status, check: r.Check, summary: r.Summary, details: r.Details})
		}
		where := r.Scenario
		if r.Case != "" {
			where += ": " + r.Case
		}
		out[i].where = append(out[i].where, where)
	}
	rank := map[checks.Status]int{checks.Error: 0, checks.Fail: 1, checks.Flaky: 2, checks.Warn: 3}
	sort.SliceStable(out, func(i, j int) bool { return rank[out[i].status] < rank[out[j].status] })
	return out
}
