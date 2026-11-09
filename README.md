# controller-crashtest

Crash-tests Kubernetes controllers: breaks them on purpose (crashes, failed
API calls, write conflicts, changes in the middle of a rollout) at every
write they make, and checks that they still end up right.

It runs a controller's **real code**, started the same way its manager
starts it, against an in-memory Kubernetes API server that follows the rules
a real one does. No cluster needed; a run takes about two seconds.

The first target is [kthena](https://github.com/volcano-sh/kthena)'s
ModelServing controller (kthena is a subproject of CNCF's Volcano). Any controller built on client-go
with a work queue can be added (see
[docs/adding-a-controller.md](docs/adding-a-controller.md)).

## Why

A controller is a loop: read what the user asked for, read what exists, write
until they match. In production it must survive things unit tests rarely try:

- a **crash between any two writes** (OOM kill, node drain, upgrade);
- an **API call that fails** once;
- a **409 Conflict** because someone else changed the object;
- the user **changing their mind mid-way** (a rollback during a rollout).

crashtest tries all of them, at **every write** the controller makes, and
compares the end state with a run that had no faults. That comparison is the
test: no hand-written expected values.

## Quick start

Needs Go 1.26 (any recent Go downloads it on its own).

```bash
make run
```

```bash
make run ARGS="-scenario rolling-update -check crash"
```

```bash
./bin/crashtest trace -scenario role-rolling-update -fault crash:4
```

`make run` (about 7 minutes: 31 scenarios, about 1,700 runs) prints a table
(scenarios x checks) and the findings, and saves `results/<time>/report.json`.
`trace` prints every API call of one case.

## What it checks

| Check | What breaks | The rule |
| --- | --- | --- |
| clean | nothing | the controller settles, and two runs end the same |
| resync | nothing | once settled, one more reconcile writes nothing |
| restart | nothing | once settled, a new process writes nothing |
| crash | the process dies before write *k*, every *k* | a new process reaches the same end state |
| api-error | write *k* fails once (HTTP 500), every *k* | same end state |
| conflict | first update of each object gets 409 | same end state |
| interrupt | a second change (rollback, scale, delete) before write *k* | same end state as making it later |

Every run is a separate process with client-go's cache mutation detector on,
so a panic, or a write to the informer cache, becomes a finding too.

[docs/how-it-works.md](docs/how-it-works.md) explains the pretend cluster,
how crashtest knows the controller is done, and how it avoids false alarms.

## Findings in kthena

On kthena `main` (2026-11-09). Not filed yet: each still needs proof on a
real cluster. Details and reproduce commands in
[docs/findings.md](docs/findings.md).

| # | Finding | Severity |
| --- | --- | --- |
| F1 | A restart during a coordinated Role rollout leaves the Role instances it was replacing missing; at the first step the rollout deadlocks | high |
| F2 | A restart during scale-to-zero leaves the status stuck (`replicas: 2`, `Available=False`) | medium |
| F6 | One failed API call cancels crash recovery (`RoleRecreate` never happens) | medium |
| F3 | The status is written on every reconcile | low |
| F4 | `revisionHistoryLimit` is never enforced | low |
| F5 | After a restart mid rollout, replacements are created while old pods still terminate | low |

## Layout

```text
cmd/crashtest/          the command: list, run, trace
internal/world/        the pretend cluster: API server rules, faults, kubelet, call log, snapshots
internal/lab/          one run of one scenario under one fault; sees inside the controller's queue
internal/checks/       the checks: turn runs into pass, warn or fail
internal/report/       the terminal summary and report.json
internal/toy/          a tiny controller with planted bugs: the tests for crashtest itself
targets/modelserving/  kthena's ModelServing controller: how to start it, and its scenarios
docs/                  how it works, adding a controller, findings
```

To test another controller, see
[docs/adding-a-controller.md](docs/adding-a-controller.md).

## Testing your own kthena changes

```bash
make kthena-local KTHENA=../kthena
```

builds against a local kthena checkout (through `go.work`); `make kthena-pinned`
goes back to the version in `go.mod`.

## Status

- **Phase 1 (now):** the framework and kthena's ModelServing controller.
- **Phase 2:** a local web UI over `report.json`.
- **Later:** kthena's autoscaler and ModelBooster controllers.

## Related work

[Sieve](https://github.com/sieve-project/sieve) (OSDI '22) and
[Acto](https://github.com/xlab-uiuc/acto) (SOSP '23) test operators on a real
cluster; [Kamera](https://github.com/tgoodwin/kamera) simulates
controller-runtime controllers in-process. crashtest is in-process like Kamera
and borrows Sieve's idea of comparing against a run without faults, for
client-go controllers like kthena's.
