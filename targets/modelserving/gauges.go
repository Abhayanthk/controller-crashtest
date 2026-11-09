package modelserving

import (
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	corev1 "k8s.io/api/core/v1"

	"github.com/abhayanthk/kthena-faultlab/internal/lab"
	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// Gauges are measured after every change while the controller works. A
// fault must not make them worse than a run without faults.
func (target) Gauges() []lab.Gauge {
	return []lab.Gauge{
		{Name: "serving ServingGroups", Measure: servingGroups, LowerIsWorse: true},
		{Name: "pods", Measure: func(w *world.World) int { return len(w.List(&podKind)) }},
	}
}

// servingGroups counts ServingGroups that can take traffic: every role in
// the spec has at least one instance whose entry pod and workers are all
// Ready and none of them is terminating.
func servingGroups(w *world.World) int {
	obj, err := w.Get(ref())
	if err != nil {
		return 0
	}
	ms := obj.(*workloadv1alpha1.ModelServing)

	type instance struct{ group, roleID string }
	ok := map[instance]bool{}
	hasEntry := map[instance]bool{}
	roleOf := map[instance]string{}
	for _, o := range w.List(&podKind) {
		pod := o.(*corev1.Pod)
		in := instance{pod.Labels[workloadv1alpha1.GroupNameLabelKey], pod.Labels[workloadv1alpha1.RoleIDKey]}
		if _, seen := ok[in]; !seen {
			ok[in] = true
		}
		if pod.DeletionTimestamp != nil || !isReady(pod) {
			ok[in] = false
		}
		if pod.Labels[workloadv1alpha1.EntryLabelKey] == "true" {
			hasEntry[in] = true
		}
		roleOf[in] = pod.Labels[workloadv1alpha1.RoleLabelKey]
	}
	rolesReady := map[string]map[string]bool{}
	for in, good := range ok {
		if good && hasEntry[in] && in.group != "" {
			if rolesReady[in.group] == nil {
				rolesReady[in.group] = map[string]bool{}
			}
			rolesReady[in.group][roleOf[in]] = true
		}
	}
	n := 0
	for _, ready := range rolesReady {
		all := true
		for _, r := range ms.Spec.Template.Roles {
			all = all && ready[r.Name]
		}
		if all {
			n++
		}
	}
	return n
}

func isReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
