# Findings in kthena

Run against kthena `main` at
[`22a74a5`](https://github.com/volcano-sh/kthena/commit/22a74a588080) (2026-11-09),
the version pinned in `go.mod`. Line numbers below refer to that commit.

None of these has been filed upstream yet. Each one still needs proof on a
real cluster (kind) before it is reported; faultlab's pretend cluster is
close to a real one, not the same.

**Severity**: *high* = serving capacity is lost and does not come back by
itself; *medium* = a status that other components read stays wrong and does
not heal; *low* = wasted work, noise or a cosmetic leak.

| # | Finding | Severity | Found by |
| --- | --- | --- | --- |
| F1 | A restart during a coordinated Role rollout leaves the Role instances it was replacing missing | high | crash |
| F2 | A restart during scale-to-zero leaves the ModelServing status stuck (old replica counts, `Progressing=True`, `Available=False`) | medium | crash |
| F3 | The status is written on every reconcile, even when nothing changed | low | resync, restart |
| F4 | `revisionHistoryLimit` is never enforced; old ControllerRevisions are deleted all-or-nothing, depending on timing | low | crash, clean runs |
| F5 | After a restart mid Role rollout, replacements are created while the old pods still terminate, failing with `AlreadyExists` until they are gone | low | crash |
| F6 | One failed API call cancels crash recovery: the Role (or ServingGroup) is never recreated | medium | api-error |

---

## F1. A restart during a coordinated Role rollout leaves Role instances missing (high)

**What happens.** A ModelServing uses `RoleRollingUpdate` with
`roleCoordination`. During an image update the controller deletes an
outdated Role instance (here `prefill-0` and `decode-0`), and the controller
process restarts while those pods are still terminating. The new process
lets the old pods finish terminating, then goes idle. **The two Role
instances are never recreated.** The ModelServing reports
`UpdateInProgress=True` and stays at reduced capacity. kthena's informers
have no periodic resync, so nothing brings the instances back until some
unrelated event happens to queue this ModelServing.

**Reproduce.**

```bash
./bin/faultlab trace -scenario coordinated-roles -fault crash:13
```

```text
controller-1  delete  pods   default/demo-0-prefill-0-0   deleting
controller-1  delete  pods   default/demo-0-prefill-0-1   deleting
controller-1  delete  pods   default/demo-0-decode-0-0    deleting
controller-1  update  modelservings/status ...            [crash before write 13]
controller-2  update  modelservings/status default/demo   updated
kubelet       delete  pods   default/demo-0-decode-0-0    deleted
kubelet       delete  pods   default/demo-0-prefill-0-0   deleted
kubelet       delete  pods   default/demo-0-prefill-0-1   deleted
(controller-2 does nothing more: the run settles with three pods missing)
--- resync ---
controller-2  create  pods   default/demo-0-prefill-0-0   created   <- only after faultlab forces a reconcile
```

Fails in all 3 runs of crash points 5 and 13.

**Why** (with `-klog 4`, the controller's own log shows each step):

1. On start, `updatePod` ignores pods that are terminating
   (`model_serving_controller.go:355-359`), so the new process never puts
   `prefill-0` and `decode-0` in its in-memory store.
2. When those pods are finally deleted, `deletePod` calls `handleDeletedPod`,
   which, for `RoleRecreate`, calls `DeleteRole` (`:2276`).
3. `DeleteRole` first marks the Role as Deleting in the store
   (`:1606-1611`). The Role isn't in the store, so this fails with
   `role decode-0 not found in roleName decode of group demo-0` and returns.
   The deferred re-queue that `DeleteRole` sets up for errors is registered
   only after this point (`:1616-1620`), so it never runs.
4. `deletePod` only logs the error (`:472`). Nothing queues the
   ModelServing again.

**Ways to fix** (for discussion, not decided): queue the ModelServing whenever
one of its pods is deleted, whatever `handleDeletedPod` returns; or register
the re-queue in `DeleteRole` before the first early return; or have the
restarted process record Role instances whose pods are all terminating as
Deleting.

**Related, not the same:** [volcano-sh/kthena#1957](https://github.com/volcano-sh/kthena/issues/1957)
(open) is the opposite failure in the same handler: a late delete event
recovers a Role again and deletes its healthy replacement.

## F2. A restart during scale-to-zero leaves the status stuck (medium)

**What happens.** A ModelServing with 2 ServingGroups is scaled to 0. The
controller deletes both groups, writes a status (still counting the
terminating groups: `replicas: 2`, `Progressing=True`) and restarts before
the pods are gone. The new process never writes the status again: it keeps
saying `replicas: 2`, `availableReplicas: 2`, `Progressing=True`,
`Available=False` and an old `observedGeneration`, through more reconciles
and another restart, for as long as the ModelServing stays at 0.

**Reproduce.**

```bash
./bin/faultlab trace -scenario scale-to-zero -fault crash:9
```

Fails in all 3 runs of 8 of the 17 crash points (every crash after the
groups were deleted).

**Why.** With no ServingGroups in memory, `updateModelServingStatusInRound`
takes a separate branch (`model_serving_controller.go:2574`). That branch
resets the replica counters and `observedGeneration` only when
`roleCoordination` is set (`:2596`), and its conditions don't move from
`Progressing` to `Available`. Without a restart the same controller writes
the right status earlier, while the groups are still in its memory as
Deleting.

**Related, not the same:** [volcano-sh/kthena#1761](https://github.com/volcano-sh/kthena/issues/1761)
(closed) made this branch reachable, so a ModelServing created at 0 gets a
status at all. The gap here is inside the branch: what an earlier process
wrote is never corrected.

**Who reads it.** ModelBooster treats a ModelServing as active only if
`Available=True` (`pkg/model-booster-controller/controller/model_booster_controller.go:246`),
so its ModelBooster stops reporting Active. The scale subresource reports
`.status.replicas` (2) to `kubectl` and autoscalers.

## F3. The status is written on every reconcile (low)

Once a rollout is over, `updateModelServingStatusInRound` sets
`shouldUpdate = true` whenever all replica counts equal `spec.replicas`, even
if `CurrentReplicas` already equals `UpdatedReplicas`
(`model_serving_controller.go:2772-2776`). Every reconcile of a settled
ModelServing then sends a status update that changes nothing. The API server
skips storing it, but the request still costs an API call and client QPS
budget. Seen in 28 of 31 scenarios (resync and restart checks). This was the
first finding of the spike that led to faultlab.

```bash
./bin/faultlab trace -scenario create
```

## F4. `revisionHistoryLimit` is not enforced (low)

The API says `revisionHistoryLimit` is "the maximum number of non-live
revisions to retain" (default 10). The ModelServing controller never reads
it (it is only defaulted, in `plugins/legacy_child.go:117`). Old
ControllerRevisions are removed by `CleanupOldControllerRevisions`, which
deletes *every* unreferenced revision, and only in the one reconcile where
the status revision changes while nothing is progressing
(`model_serving_controller.go:2819`). If that reconcile is missed, the old
revisions stay until a later rollout happens to hit it.

- A plain two-group rolling update without any fault keeps the old revision.
- `rolling-update-surge` deletes it, except after a crash near the end
  (crash points 22-27, some runs).

So history is either wiped or never trimmed, depending on timing, and
`revisionHistoryLimit` has no effect.

## F5. Replacements are created while old pods still terminate (low)

After a restart in the middle of a `RoleRollingUpdate`, the new process
tries to create the replacement pods while the old ones (same names) are
still terminating. Each attempt fails (`AlreadyExists`, then
`existing entry pod ... does not match expected identity`), and the
reconcile is retried with backoff until the old pods are gone (7 failed
reconciles in the trace below; with a real 30 s grace period, more). The
identity check keeps it safe; the cost is error noise and backoff.

```bash
./bin/faultlab trace -scenario role-rolling-update -fault crash:4
```

## F6. One failed API call cancels crash recovery (medium)

**What happens.** A container in a ModelServing pod crashes and restarts.
With `recoveryPolicy: RoleRecreate` (the default) kthena must recreate every
pod of that Role; the API docs say this is "to ensure all pods/containers in
the group will be started in the same time". kthena tries to delete the pod
once. If that one delete fails (a timeout, 429 throttling, an API server
restart), it never tries again: as soon as the restarted container is Ready,
the pod is treated as healthy, and the Role is never recreated.
`ServingGroupRecreate` loses its recovery the same way.

**Reproduce.**

```bash
./bin/faultlab trace -scenario recover-role -fault error:0
```

```text
kubelet       update  pods  default/demo-0-decode-0-0   updated   <- container crashed, pod not Ready
controller-1  delete  pods  default/demo-0-decode-0-0   error: Internal error occurred  [API error on write 0]
controller-1  update  modelservings/status ...          updated
kubelet       update  pods  default/demo-0-decode-0-0   updated   <- restarted container Ready again
(nothing more: the pod keeps restartCount 1; the Role was not recreated)
```

In-process 3 of 3 runs; in the sweep 2 of 3 (`recover-role`) and 1 of 3
(`recover-group`), because it depends on whether another not-Ready update
arrives before the pod is Ready again.

**Why.** `handleErrorPod` (`model_serving_controller.go:2147`) starts a
goroutine, `handlePodAfterGraceTime`, that makes the one delete. On error it
logs `cannot delete pod ... when it error` and returns: no retry, no
re-queue, and the reconcile loop doesn't know a recovery is pending. Once the
pod is Ready again, `updatePod` sends it to `handleReadyPod`, because Ready
is checked before unexpected restarts (`:386`). With
`restartGracePeriodSeconds > 0` the same goroutine sleeps first, so a
controller restart during that sleep loses the recovery too.

**Impact.** For a one-pod Role, little: the container restarted anyway. For
a Role with workers (multi-node inference), the entry and its workers no
longer restart together, which is what `RoleRecreate` exists for.

---

## Design question (not counted as a bug)

**D1. Scale-down and image change together delete every group at once.**
When the image and `replicas` (3 → 1) change together, kthena deletes the
two extra groups and, in the same round, the group it keeps (to update it),
so no group can serve until the new one is Ready. With `replicas: 1` the
default `maxUnavailable` of 1 (`utils/utils.go:598`) allows this, but the
two extra old groups could have kept serving until then. Shown by
`./bin/faultlab trace -scenario scale-down-mid-update -fault interrupt:0`
(gauge "serving ServingGroups: lowest 0").

## False alarms faultlab caught in itself

Each was found while checking a finding, fixed in faultlab, and is the
reason for a rule in [how-it-works.md](how-it-works.md):

| Looked like | Was | Fix |
| --- | --- | --- |
| Recovery never runs after a container crash | the pretend kubelet reported the crashed container as restarted *and Ready* in one step; kthena checks Ready first (`model_serving_controller.go:386`) | the kubelet now reports the pod not Ready after a crash, as a real one does |
| `fatal error: concurrent map read and map write` | the crashed process was still finishing a reconcile while the next one was being built; kthena's constructor writes to client-go's global scheme | wait for the old process to stop before starting the next |
| `UpdateInProgress=False` missing after a restart | whether a False condition is still listed depends on history | False conditions are not compared |
| Scale-down mid-rollout keeps `demo-1` instead of `demo-0` | kthena deletes outdated groups first, by design | the interrupt check compares groups by order, not ordinal |
| Availability lower when a second change arrives mid-way | the reference passes through a 3-replica rollout the interrupted run never has | gauges are compared only for the same change |
| Two clean runs of the toy controller differ | the planted bug used a random name | the planted bug names copies after the process |
