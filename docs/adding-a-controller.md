# Adding a controller

A controller under test is a **target**: a Go package under `targets/` that
implements four methods (`internal/lab/target.go`):

```go
type Target interface {
	Name() string                                                    // "modelserving"
	NewWorld() *world.World                                          // the kinds it uses
	Start(ctx context.Context, conn *world.Conn) (Controller, error) // one new process
	Scenarios() []Scenario                                           // what users do
}
```

The smallest complete example is `internal/toy/toy.go` (about 200 lines,
including the controller itself). The real one is `targets/modelserving/`.

## Steps

**1. Make the folder.** `targets/<name>/target.go`, `package <name>`.

**2. List the kinds** the controller reads or writes, and build the world:

```go
var podKind = world.Kind{
	GVR: corev1.SchemeGroupVersion.WithResource("pods"),
	GVK: corev1.SchemeGroupVersion.WithKind("Pod"),
	Status:  true, // has a status subresource
	Compare: true, // part of the end state checks compare
}

func (target) NewWorld() *world.World { return world.New(scheme, podKind, ...) }
```

- `Compare` only the kinds the controller **owns** (it creates or updates them).
- `Custom: true` for kinds served from a CRD.
- `Default` and `Admit` let the world apply CRD defaults and call the project's
  validating webhook (see `targets/modelserving/apiserver.go`).
- A kind the controller watches but that isn't listed makes the run fail with
  "is not served by this world".

**3. Start the controller** the way its `main` does, on fresh fake clientsets
attached to the connection:

```go
func (target) Start(ctx context.Context, conn *world.Conn) (lab.Controller, error) {
	kube := kubefake.NewClientset()
	conn.Attach(kube) // every call now goes to the world
	c, err := mycontroller.New(kube)
	if err != nil {
		return nil, err
	}
	probe, err := lab.WatchQueue(conn.Name(), c, "workqueue", "syncHandler")
	if err != nil {
		return nil, err
	}
	go c.Run(ctx, workers)
	return probe, nil
}
```

`WatchQueue` needs the names of the controller struct's work-queue field and
sync-function field. Look them up in the controller's source.

**4. Write scenarios**: what a user does, as a starting world plus one change.

```go
{
	Name:   "scale-down",
	About:  "replicas 3 -> 1",
	Setup:  create(volcano(), demo(3)),
	Change: edit(func(ms *v1alpha1.ModelServing) { ms.Spec.Replicas = ptr.To[int32](1) }),
	Then:   nil, // optional second change, used by the interrupt check
}
```

Scenario helpers act like a person with kubectl: `w.Create`, `world.Change`
(edit the latest copy), `w.Delete`, `w.RestartContainer`.

**5. Register it** in `cmd/faultlab/targets.go`:

```go
var targets = []lab.Target{
	modelserving.New(),
	mycontroller.New(),
}
```

**6. Run it.**

```bash
make run ARGS="-target mycontroller -check clean"
```

Start with `-check clean` until every scenario settles, then run everything.

## When something fails

```bash
./bin/faultlab trace -target mycontroller -scenario scale-down -fault crash:3
```

prints every API call of that one case, then how its end state differs from a
run without faults.
