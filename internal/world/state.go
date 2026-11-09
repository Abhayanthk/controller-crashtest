package world

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

// State is what a run built: every object of the kinds marked Compare,
// cleaned of the values that differ between two correct runs anyway
// (uids, resourceVersions, timestamps, pod IPs, faultlab's own annotations,
// and conditions whose status is False: whether a False condition is still
// listed depends on the object's history, not on its current state).
// Keys look like "Pod default/demo-0-prefill-0-0".
type State map[string]map[string]any

// Snapshot returns the current State.
func (w *World) Snapshot() State {
	st := State{}
	for _, k := range w.kinds {
		if !k.Compare {
			continue
		}
		for _, obj := range w.store.list(k.GVR, "", nil) {
			m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				continue
			}
			clean(m)
			a, _ := meta.Accessor(obj)
			st[k.GVK.Kind+" "+objectPath(a.GetNamespace(), a.GetName())] = m
		}
	}
	return st
}

var (
	uidPattern  = regexp.MustCompile(`00000000-0000-0000-0000-\d{12}`)
	timePattern = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
)

func clean(m map[string]any) {
	if md, ok := m["metadata"].(map[string]any); ok {
		for _, f := range []string{"resourceVersion", "uid", "creationTimestamp", "managedFields", "selfLink", "deletionGracePeriodSeconds"} {
			delete(md, f)
		}
		if ann, ok := md["annotations"].(map[string]any); ok {
			for a := range ann {
				if strings.HasPrefix(a, "faultlab.dev/") {
					delete(ann, a)
				}
			}
			if len(ann) == 0 {
				delete(md, "annotations")
			}
		}
	}
	if st, ok := m["status"].(map[string]any); ok {
		// Pod and host IPs come from the network, not the controller.
		for _, f := range []string{"podIP", "podIPs", "hostIP", "hostIPs"} {
			delete(st, f)
		}
		if conds, ok := st["conditions"].([]any); ok {
			kept := []any{}
			for _, c := range conds {
				if cm, ok := c.(map[string]any); !ok || cm["status"] != "False" {
					kept = append(kept, c)
				}
			}
			if len(kept) == 0 {
				delete(st, "conditions")
			} else {
				st["conditions"] = kept
			}
		}
	}
	scrub(m)
}

// scrub drops uid fields and replaces uids and timestamps inside strings,
// including JSON kept in annotations.
func scrub(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if k == "uid" {
				delete(t, k)
				continue
			}
			if s, ok := x.(string); ok {
				t[k] = scrubString(s)
				continue
			}
			scrub(x)
		}
	case []any:
		for i, x := range t {
			if s, ok := x.(string); ok {
				t[i] = scrubString(s)
				continue
			}
			scrub(x)
		}
	}
}

func scrubString(s string) string {
	s = uidPattern.ReplaceAllString(s, "<uid>")
	return timePattern.ReplaceAllString(s, "<time>")
}

// Difference is one way two states differ. Want is the reference run (no
// faults), Got is the run being checked.
type Difference struct {
	Object string `json:"object"`
	Path   string `json:"path,omitempty"`
	Want   string `json:"want"`
	Got    string `json:"got"`
}

func (d Difference) String() string {
	if d.Path == "" {
		return fmt.Sprintf("%s: %s, expected %s", d.Object, d.Got, d.Want)
	}
	return fmt.Sprintf("%s  %s: %s, expected %s", d.Object, d.Path, d.Got, d.Want)
}

// Diff lists how got differs from want, sorted by object and path.
func Diff(want, got State) []Difference {
	var out []Difference
	for _, key := range unionKeys(want, got) {
		w, inWant := want[key]
		g, inGot := got[key]
		switch {
		case !inGot:
			out = append(out, Difference{Object: key, Want: "present", Got: "missing"})
		case !inWant:
			out = append(out, Difference{Object: key, Want: "absent", Got: "extra object"})
		default:
			diffValue(key, "", w, g, &out)
		}
	}
	return out
}

func diffValue(obj, path string, a, b any, out *[]Difference) {
	am, aIsMap := a.(map[string]any)
	bm, bIsMap := b.(map[string]any)
	if aIsMap && bIsMap {
		for _, k := range unionKeys(am, bm) {
			diffValue(obj, join(path, k), am[k], bm[k], out)
		}
		return
	}
	as, aIsList := a.([]any)
	bs, bIsList := b.([]any)
	if aIsList && bIsList {
		for i := 0; i < len(as) || i < len(bs); i++ {
			var x, y any
			if i < len(as) {
				x = as[i]
			}
			if i < len(bs) {
				y = bs[i]
			}
			diffValue(obj, fmt.Sprintf("%s[%d]", path, i), x, y, out)
		}
		return
	}
	if !reflect.DeepEqual(a, b) {
		*out = append(*out, Difference{Object: obj, Path: path, Want: show(a), Got: show(b)})
	}
}

func join(path, k string) string {
	if path == "" {
		return k
	}
	return path + "." + k
}

func show(v any) string {
	if v == nil {
		return "<none>"
	}
	if s, ok := v.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	if len(b) > 300 {
		return string(b[:300]) + "..."
	}
	return string(b)
}

func unionKeys[V any](a, b map[string]V) []string {
	seen := map[string]bool{}
	var keys []string
	for _, m := range []map[string]V{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}
