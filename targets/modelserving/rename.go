package modelserving

import (
	"sort"
	"strconv"
	"strings"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"

	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// Rename names ServingGroups by their order instead of their ordinal:
// the lowest surviving group becomes "group-a", the next "group-b", and so
// on. When a scale-down arrives in the middle of a rollout, kthena deletes
// outdated groups first, so it may keep demo-1 where a later scale-down
// keeps demo-0. Both are correct; this makes them compare equal.
func (target) Rename(st world.State) world.State {
	groups := map[string]bool{}
	for _, obj := range st {
		if md, ok := obj["metadata"].(map[string]any); ok {
			if labels, ok := md["labels"].(map[string]any); ok {
				if g, ok := labels[workloadv1alpha1.GroupNameLabelKey].(string); ok {
					groups[g] = true
				}
			}
		}
	}
	names := make([]string, 0, len(groups))
	for g := range groups {
		names = append(names, g)
	}
	sort.Slice(names, func(i, j int) bool { return ordinal(names[i]) < ordinal(names[j]) })
	pairs := []string{}
	for i, g := range names {
		alias := "group-" + string(rune('a'+i))
		pairs = append(pairs, g+"-", alias+"-", `"`+g+`"`, `"`+alias+`"`)
	}
	r := strings.NewReplacer(pairs...)
	out := world.State{}
	for key, obj := range st {
		out[renameKey(r, key)] = renameValue(r, groups, obj).(map[string]any)
	}
	return out
}

// renameKey renames the group in a key like "PodGroup default/demo-1",
// where the name ends the key instead of being followed by "-".
func renameKey(r *strings.Replacer, key string) string {
	k := r.Replace(key + "-")
	return k[:len(k)-1]
}

func ordinal(group string) int {
	n, _ := strconv.Atoi(group[strings.LastIndex(group, "-")+1:])
	return n
}

func renameValue(r *strings.Replacer, groups map[string]bool, v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, x := range t {
			m[k] = renameValue(r, groups, x)
		}
		return m
	case []any:
		l := make([]any, len(t))
		for i, x := range t {
			l[i] = renameValue(r, groups, x)
		}
		return l
	case string:
		if groups[t] {
			return strings.Trim(r.Replace(`"`+t+`"`), `"`)
		}
		return r.Replace(t)
	}
	return v
}
