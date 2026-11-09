package modelserving

import (
	"errors"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/abhayanthk/kthena-faultlab/internal/lab"
	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// Scenarios are the user stories faultlab drives the controller through.
// Each starts from a settled world (Setup) and makes one change (Change);
// the checks break things while the controller handles that change.
func (target) Scenarios() []lab.Scenario {
	return []lab.Scenario{
		{
			Name:   "create",
			About:  "create a ModelServing with 2 ServingGroups (prefill with a worker, decode)",
			Setup:  create(volcano()),
			Change: create(demo(2)),
		},
		{
			Name:   "scale-up",
			About:  "replicas 1 -> 3",
			Setup:  create(volcano(), demo(1)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](3) }),
		},
		{
			Name:   "scale-down",
			About:  "replicas 3 -> 1",
			Setup:  create(volcano(), demo(3)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](1) }),
		},
		{
			Name:   "role-scale-up",
			About:  "decode role replicas 1 -> 2 in each ServingGroup",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { roleNamed(ms, "decode").Replicas = ptr.To[int32](2) }),
		},
		{
			Name:  "role-scale-down",
			About: "decode role replicas 2 -> 1 in each ServingGroup",
			Setup: create(volcano(), with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				roleNamed(ms, "decode").Replicas = ptr.To[int32](2)
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { roleNamed(ms, "decode").Replicas = ptr.To[int32](1) }),
		},
		{
			Name:   "rolling-update",
			About:  "new image, default strategy (ServingGroupRollingUpdate, Recreate)",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:  "rolling-update-surge",
			About: "new image with maxSurge 1, maxUnavailable 0",
			Setup: create(volcano(), with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy = rollingUpdate(intOrStr(0), intOrStr(1), nil)
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:  "rolling-update-partition",
			About: "new image with partition 1: only ServingGroups 1 and up move",
			Setup: create(volcano(), with(demo(3), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy = rollingUpdate(nil, nil, intOrStr(1))
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:  "role-rolling-update",
			About: "new image, RoleRollingUpdate (one ServingGroup, as kthena recommends)",
			Setup: create(volcano(), with(demo(1), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.RoleRollingUpdate}
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:  "inplace-update",
			About: "new image, updateMethod InPlace (containers restart on the new image)",
			Setup: create(volcano(), with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate, UpdateMethod: workloadv1alpha1.InPlaceUpdateMethod}
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:   "recover-role",
			About:  "a decode container crashes; recoveryPolicy RoleRecreate rebuilds that role",
			Setup:  create(volcano(), demo(2)),
			Change: restartContainer("demo-0-decode-0-0"),
		},
		{
			Name:  "recover-group",
			About: "a decode container crashes; recoveryPolicy ServingGroupRecreate rebuilds the ServingGroup",
			Setup: create(volcano(), with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RecoveryPolicy = workloadv1alpha1.ServingGroupRecreate
			})),
			Change: restartContainer("demo-0-decode-0-0"),
		},
		{
			Name:   "pod-deleted",
			About:  "someone deletes a prefill worker pod (e.g. a node drain)",
			Setup:  create(volcano(), demo(2)),
			Change: deletePod("demo-1-prefill-0-1"),
		},
		{
			Name:   "delete",
			About:  "delete the ModelServing",
			Setup:  create(volcano(), demo(2)),
			Change: func(w *world.World) error { return w.Delete(ref()) },
		},
		{
			Name:   "create-without-volcano",
			About:  "create when Volcano is not installed (no PodGroups)",
			Setup:  create(),
			Change: create(demo(2)),
		},
		{
			Name:   "scale-to-zero",
			About:  "replicas 2 -> 0, then back to 2",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](0) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](2) }),
		},
		{
			Name:   "rollback",
			About:  "new image, then back to the old image while the rollout runs",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV1) }),
		},
		{
			Name:   "update-twice",
			About:  "new image, then a newer image while the rollout runs",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV3) }),
		},
		{
			Name:   "scale-down-mid-update",
			About:  "new image with 3 replicas, then replicas 1 while the rollout runs",
			Setup:  create(volcano(), demo(3)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](1) }),
		},
		{
			Name:   "scale-up-mid-update",
			About:  "new image with 1 replica, then replicas 3 while the rollout runs",
			Setup:  create(volcano(), demo(1)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](3) }),
		},
		{
			Name:   "update-mid-scale-up",
			About:  "replicas 1 -> 3, then a new image while the new groups start",
			Setup:  create(volcano(), demo(1)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](3) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:   "scale-down-mid-scale-up",
			About:  "replicas 1 -> 3, then 3 -> 1 while the new groups start",
			Setup:  create(volcano(), demo(1)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](3) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](1) }),
		},
		{
			Name:   "delete-mid-create",
			About:  "create, then delete the ModelServing while its pods are being created",
			Setup:  create(volcano()),
			Change: create(demo(2)),
			Then:   func(w *world.World) error { return w.Delete(ref()) },
		},
		{
			Name:   "delete-mid-update",
			About:  "new image, then delete the ModelServing while the rollout runs",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then:   func(w *world.World) error { return w.Delete(ref()) },
		},
		{
			Name:   "recover-mid-update",
			About:  "new image, then a decode container crashes while the rollout runs",
			Setup:  create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			// Mid-rollout the pod may be gone or not started yet; then no
			// container can crash, and the second change is a no-op.
			Then: func(w *world.World) error {
				if err := w.RestartContainer(namespace, "demo-1-decode-0-0"); !errors.Is(err, world.ErrNotRunning) {
					return err
				}
				return nil
			},
		},
		{
			Name:  "inplace-rollback",
			About: "updateMethod InPlace: new image, then back to the old one while pods restart",
			Setup: create(volcano(), with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{Type: workloadv1alpha1.ServingGroupRollingUpdate, UpdateMethod: workloadv1alpha1.InPlaceUpdateMethod}
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then:   edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV1) }),
		},
		{
			Name:  "partition-release",
			About: "a rollout held at partition 1 is released (partition 1 -> 0)",
			Setup: create(volcano(), with(demo(3), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy = rollingUpdate(nil, nil, intOrStr(1))
			})),
			// Setup settles first; the image change then rolls out to groups 1-2.
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
			Then: edit(func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition = intOrStr(0)
			}),
		},
		{
			Name:  "coordinated-roles",
			About: "RoleRollingUpdate with roleCoordination (maxSkew 50%), 2 instances per role",
			Setup: create(volcano(), with(demo(1), func(ms *workloadv1alpha1.ModelServing) {
				roleNamed(ms, "prefill").Replicas = ptr.To[int32](2)
				roleNamed(ms, "decode").Replicas = ptr.To[int32](2)
				skew := intstr.FromString("50%")
				ms.Spec.RolloutStrategy = &workloadv1alpha1.RolloutStrategy{
					Type:             workloadv1alpha1.RoleRollingUpdate,
					RoleCoordination: &workloadv1alpha1.RoleCoordination{MaxSkew: &skew},
				}
			})),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) { setImage(ms, imageV2) }),
		},
		{
			Name:  "gang-policy",
			About: "create with gangPolicy.minRoleReplicas (PodGroup minMember per role)",
			Setup: create(volcano()),
			Change: create(with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				roleNamed(ms, "decode").Replicas = ptr.To[int32](2)
				ms.Spec.Template.GangPolicy = &workloadv1alpha1.GangPolicy{MinRoleReplicas: map[string]int32{"prefill": 1, "decode": 1}}
			})),
		},
		{
			Name:  "headless-service-plugin",
			About: "create with the built-in headless-service plugin (a Service per role instance)",
			Setup: create(volcano()),
			Change: create(with(demo(2), func(ms *workloadv1alpha1.ModelServing) {
				ms.Spec.Plugins = []workloadv1alpha1.PluginSpec{{Name: "headless-service", Type: workloadv1alpha1.PluginTypeBuiltIn}}
			})),
		},
		{
			Name:  "worker-scale",
			About: "prefill workers per instance 1 -> 2",
			Setup: create(volcano(), demo(2)),
			Change: edit(func(ms *workloadv1alpha1.ModelServing) {
				r := roleNamed(ms, "prefill")
				r.WorkerReplicas = 2
			}),
		},
	}
}

func create(objs ...runtime.Object) func(*world.World) error {
	return func(w *world.World) error { return w.Create(objs...) }
}

func with(ms *workloadv1alpha1.ModelServing, f func(*workloadv1alpha1.ModelServing)) *workloadv1alpha1.ModelServing {
	f(ms)
	return ms
}

func edit(f func(*workloadv1alpha1.ModelServing)) func(*world.World) error {
	return func(w *world.World) error { return world.Change(w, ref(), f) }
}

func restartContainer(pod string) func(*world.World) error {
	return func(w *world.World) error { return w.RestartContainer(namespace, pod) }
}

func deletePod(pod string) func(*world.World) error {
	return func(w *world.World) error {
		return w.Delete(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: namespace}})
	}
}
