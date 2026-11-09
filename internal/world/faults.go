package world

import (
	"errors"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
)

// Call is a controller's write request, as a fault sees it.
type Call struct {
	N      int    `json:"n"`      // 0-based index among the writes since the fault was armed
	Verb   string `json:"verb"`   // create, update, patch, delete
	Kind   string `json:"kind"`   // resource, with "/status" for the status subresource
	Object string `json:"object"` // namespace/name

	kind *Kind
}

func (c Call) String() string {
	return fmt.Sprintf("write %d: %s %s %s", c.N, c.Verb, c.Kind, c.Object)
}

// A Fault acts just before a controller write reaches the API server.
// fired says the fault acted on this write (the run records it as the
// fault's hit); a non-nil error makes the write fail.
type Fault interface {
	Name() string
	Before(c *Conn, call Call) (fired bool, err error)
}

// CrashAt kills the controller process just before write k (0-based, counted
// from when the fault was armed). That write and every later call fail.
func CrashAt(k int) Fault { return crashAt(k) }

type crashAt int

func (f crashAt) Name() string { return fmt.Sprintf("crash before write %d", int(f)) }

func (f crashAt) Before(c *Conn, call Call) (bool, error) {
	if call.N < int(f) {
		return false, nil
	}
	c.kill()
	return true, errDead
}

// FailOnce fails write k once with a server error (500). The controller keeps
// running and must retry on its own.
func FailOnce(k int) Fault { return failOnce(k) }

type failOnce int

func (f failOnce) Name() string { return fmt.Sprintf("API error on write %d", int(f)) }

func (f failOnce) Before(_ *Conn, call Call) (bool, error) {
	if call.N != int(f) {
		return false, nil
	}
	return true, apierrors.NewInternalError(errors.New("faultlab: injected server error"))
}

// ConflictOnce answers the first update or patch of every object with
// 409 Conflict, after another writer changed that object, as happens in a
// real cluster when two writers race.
func ConflictOnce() Fault { return &conflictOnce{seen: map[string]bool{}} }

type conflictOnce struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (f *conflictOnce) Name() string { return "409 Conflict on the first update of each object" }

func (f *conflictOnce) Before(c *Conn, call Call) (bool, error) {
	if call.Verb != "update" && call.Verb != "patch" {
		return false, nil
	}
	f.mu.Lock()
	first := !f.seen[call.Object+" "+call.kind.GVR.Resource]
	f.seen[call.Object+" "+call.kind.GVR.Resource] = true
	f.mu.Unlock()
	if !first {
		return false, nil
	}
	ns, name := splitPath(call.Object)
	c.w.touch(call.kind, ns, name)
	return true, apierrors.NewConflict(call.kind.GVR.GroupResource(), name,
		errors.New("the object has been modified; please apply your changes to the latest version and try again"))
}

// InterruptAt makes a second user change (then) just before write k of the
// controller's work on the first one, and lets the write through.
func InterruptAt(k int, then func(*World) error) *Interrupt {
	return &Interrupt{k: k, then: then}
}

// Interrupt is the fault InterruptAt returns.
type Interrupt struct {
	k    int
	then func(*World) error

	mu    sync.Mutex
	fired bool
	err   error
}

func (f *Interrupt) Name() string { return fmt.Sprintf("second change before write %d", f.k) }

func (f *Interrupt) Before(c *Conn, call Call) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if call.N != f.k || f.fired {
		return false, nil
	}
	f.fired = true
	c.w.Mark("second change, before " + call.String())
	f.err = f.then(c.w)
	return true, nil
}

// Fired reports whether the second change was made, and its error.
func (f *Interrupt) Fired() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fired, f.err
}

// TouchedAnnotation is what the pretend "other writer" changes. State
// snapshots ignore every annotation under faultlab.dev/.
const TouchedAnnotation = "faultlab.dev/touched"

// touch plays another writer: it changes an annotation, so the object's
// resourceVersion moves on and watchers see an update.
func (w *World) touch(k *Kind, ns, name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	obj, ok := w.store.get(k.GVR, ns, name)
	if !ok {
		return
	}
	m, _ := meta.Accessor(obj)
	ann := m.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[TouchedAnnotation] = fmt.Sprint(w.rv + 1)
	m.SetAnnotations(ann)
	w.system("other-writer", "update", k, obj)
}

func splitPath(p string) (ns, name string) {
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			return p[:i], p[i+1:]
		}
	}
	return "", p
}
