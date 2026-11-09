# kthena-faultlab

Breaks [kthena](https://github.com/volcano-sh/kthena)'s Kubernetes controllers
on purpose (crashes, failed API calls, write conflicts, changes in the middle
of a rollout) and checks that they still end up right.

It runs kthena's **real controller code**, started the same way
`kthena-controller-manager` starts it, against an in-memory Kubernetes API
server that follows the rules a real one does. No cluster needed; a run takes
about two seconds.

## Why

A controller is a loop: read what the user asked for, read what exists, write
until they match. In production it must survive things unit tests rarely try:

- a **crash between any two writes** (OOM kill, node drain, upgrade);
- an **API call that fails** once;
- a **409 Conflict** because someone else changed the object;
- the user **changing their mind mid-way** (a rollback during a rollout).

faultlab tries all of them, at **every write** the controller makes, and
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
./bin/faultlab trace -scenario role-rolling-update -fault crash:4
```

`make run` prints a table (scenarios x checks) and the findings, and saves
`results/<time>/report.json`. `trace` prints every API call of one case.

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
how faultlab knows the controller is done, and how it avoids false alarms.

## Findings in kthena

See [docs/findings.md](docs/findings.md).

## Layout

```text
cmd/faultlab/          the command: list, run, trace
internal/world/        the pretend cluster: API server rules, faults, kubelet, call log, snapshots
internal/lab/          one run of one scenario under one fault; sees inside the controller's queue
internal/checks/       the checks: turn runs into pass, warn or fail
internal/report/       the terminal summary and report.json
internal/toy/          a tiny controller with planted bugs: the tests for faultlab itself
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
controller-runtime controllers in-process. faultlab is in-process like Kamera
and borrows Sieve's idea of comparing against a run without faults, for
client-go controllers like kthena's.
