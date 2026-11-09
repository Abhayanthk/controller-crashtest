// Package world is the pretend Kubernetes cluster that controllers run in.
//
//   - store.go   keeps the objects and feeds watches (the "etcd").
//   - api.go     applies the API server's rules to every write.
//   - conn.go    connects one controller process to the world.
//   - faults.go  breaks a connection on purpose (crash, error, conflict).
//   - kubelet.go runs the pods.
//   - state.go   takes the snapshot two runs are compared on.
//   - log.go     records every call, for counting writes and for reports.
package world

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Op is an admission operation.
type Op string

const (
	Create Op = "CREATE"
	Update Op = "UPDATE"
)

// Kind is one resource type the pretend API server serves.
type Kind struct {
	GVR schema.GroupVersionResource
	GVK schema.GroupVersionKind

	// ClusterScoped objects have no namespace (CRDs, nodes).
	ClusterScoped bool
	// Status means the type has a status subresource: a normal update keeps
	// the stored status, and an update of /status only changes status.
	Status bool
	// Custom means the type is served from a CRD: updates must carry a
	// resourceVersion and strategic merge patches are rejected.
	Custom bool
	// Compare puts objects of this kind in the state that two runs are
	// compared on. Mark the kinds the controller owns.
	Compare bool

	// Default applies server-side defaults, e.g. a CRD's default markers.
	Default func(obj runtime.Object) error
	// Admit runs validating admission (webhooks). Return an error to deny.
	Admit func(op Op, obj, old runtime.Object) error
}

// World is one pretend cluster. Build a fresh one for every run.
type World struct {
	scheme *runtime.Scheme
	kinds  []*Kind
	byGVR  map[schema.GroupVersionResource]*Kind
	byGVK  map[schema.GroupVersionKind]*Kind
	store  *store
	start  time.Time

	mu    sync.Mutex // one write at a time, like etcd
	rv    int64
	uid   int64
	conns int

	lastWrite atomic.Int64 // unix nanos of the last write by anyone
	log       Log

	hooksMu sync.Mutex
	hooks   []func()
}

// OnChange registers f to run after every change that is stored, by anyone.
// f runs while the world is locked for writing: it may read, never write.
func (w *World) OnChange(f func()) {
	w.hooksMu.Lock()
	defer w.hooksMu.Unlock()
	w.hooks = append(w.hooks, f)
}

// New builds an empty world that serves the given kinds.
func New(scheme *runtime.Scheme, kinds ...Kind) *World {
	w := &World{
		scheme: scheme,
		byGVR:  map[schema.GroupVersionResource]*Kind{},
		byGVK:  map[schema.GroupVersionKind]*Kind{},
		store:  newStore(),
		start:  time.Now(),
	}
	for i := range kinds {
		k := kinds[i]
		w.kinds = append(w.kinds, &k)
		w.byGVR[k.GVR] = &k
		w.byGVK[k.GVK] = &k
	}
	w.lastWrite.Store(time.Now().UnixNano())
	return w
}

// Scheme returns the scheme every object in the world is registered in.
func (w *World) Scheme() *runtime.Scheme { return w.scheme }

// Log returns the record of every call made so far.
func (w *World) Log() *Log { return &w.log }

// QuietFor reports whether nobody has written anything for d.
func (w *World) QuietFor(d time.Duration) bool {
	return time.Since(time.Unix(0, w.lastWrite.Load())) >= d
}

// Mark adds a phase marker ("setup", "change", "crash", ...) to the log.
func (w *World) Mark(phase string) {
	w.log.add(Entry{Actor: "faultlab", Verb: "mark", Result: phase}, w.start)
}

func (w *World) kindOf(obj runtime.Object) (*Kind, error) {
	gvks, _, err := w.scheme.ObjectKinds(obj)
	if err != nil {
		return nil, err
	}
	for _, gvk := range gvks {
		if k := w.byGVK[gvk]; k != nil {
			return k, nil
		}
	}
	return nil, fmt.Errorf("faultlab: %T is not served by this world (add its Kind to the target)", obj)
}

// KindFor returns the kind served for a resource, or nil.
func (w *World) KindFor(gvr schema.GroupVersionResource) *Kind { return w.byGVR[gvr] }

func (w *World) newObject(k *Kind) (runtime.Object, error) { return w.scheme.New(k.GVK) }

func (w *World) nextRV() string {
	w.rv++
	return fmt.Sprintf("%d", w.rv)
}

func (w *World) nextUID() string {
	w.uid++
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", w.uid)
}
