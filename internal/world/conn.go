package world

import (
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	kt "k8s.io/client-go/testing"
)

// Conn is one controller process's connection to the API server. Each start
// of a controller gets its own Conn, so when a fault crashes one process, its
// replacement still works, and a leftover goroutine of the dead process can't
// write anything.
type Conn struct {
	w    *World
	name string

	mu      sync.Mutex
	fault   Fault
	writes  int   // writes since the fault was armed
	dead    bool  // crashed or stopped
	hit     *Call // the first write the fault failed
	watches []*watcher
}

// Fake is the part of a generated fake clientset that Attach needs.
// Every clientset made by client-gen has it (it embeds testing.Fake).
type Fake interface {
	PrependReactor(verb, resource string, reaction kt.ReactionFunc)
	PrependWatchReactor(resource string, reaction kt.WatchReactionFunc)
}

var errDead = apierrors.NewServiceUnavailable("crashtest: this controller process is gone")

// Connect opens a connection for a new controller process.
func (w *World) Connect() *Conn {
	w.mu.Lock()
	w.conns++
	n := w.conns
	w.mu.Unlock()
	return &Conn{w: w, name: fmt.Sprintf("controller-%d", n)}
}

// Name is how the log calls this process: controller-1, controller-2, ...
func (c *Conn) Name() string { return c.name }

// Attach sends every call of a fake clientset to the world through c.
// Give each Conn its own fresh fake clientsets.
func (c *Conn) Attach(f Fake) {
	f.PrependReactor("*", "*", c.react)
	f.PrependWatchReactor("*", c.watch)
}

// Arm sets the fault for every write from now on and restarts the write count.
func (c *Conn) Arm(f Fault) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fault, c.writes = f, 0
}

// Writes returns how many writes the process sent since Arm.
func (c *Conn) Writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

// Dead reports whether the process has crashed or been stopped.
func (c *Conn) Dead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

// Hit returns the first write the fault failed, or nil.
func (c *Conn) Hit() *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hit
}

// Close stops the process's connection: every call fails and watches end.
func (c *Conn) Close() { c.kill() }

func (c *Conn) kill() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dead = true
	for _, w := range c.watches {
		w.Stop()
	}
}

func (c *Conn) react(a kt.Action) (bool, runtime.Object, error) {
	if c.Dead() {
		return true, nil, errDead
	}
	gvr := a.GetResource()
	if gvr.Resource == "events" {
		return c.event(a)
	}
	k := c.w.byGVR[gvr]
	if k == nil {
		return true, nil, fmt.Errorf("crashtest: %s is not served by this world; add it to the target's kinds", gvr)
	}
	ns := a.GetNamespace()
	switch a := a.(type) {
	case kt.GetActionImpl:
		obj, err := c.w.get(k, ns, a.Name)
		return true, obj, err
	case kt.ListActionImpl:
		obj, err := c.w.list(k, ns, a.ListRestrictions.Labels)
		return true, obj, err
	case kt.CreateActionImpl:
		if a.Subresource != "" {
			break
		}
		m, err := meta.Accessor(a.Object)
		if err != nil {
			return true, nil, err
		}
		if err := c.before(Call{Verb: "create", Kind: k.GVR.Resource, Object: objectPath(nsOr(m.GetNamespace(), ns), m.GetName()), kind: k}); err != nil {
			return true, nil, err
		}
		obj, err := c.w.create(c.name, k, ns, a.Object)
		return true, obj, err
	case kt.UpdateActionImpl:
		m, err := meta.Accessor(a.Object)
		if err != nil {
			return true, nil, err
		}
		call := Call{Verb: "update", Kind: resourceName(k, a.Subresource), Object: objectPath(nsOr(m.GetNamespace(), ns), m.GetName()), kind: k}
		if err := c.before(call); err != nil {
			return true, nil, err
		}
		obj, err := c.w.update(c.name, k, ns, a.Object, a.Subresource)
		return true, obj, err
	case kt.PatchActionImpl:
		call := Call{Verb: "patch", Kind: resourceName(k, a.Subresource), Object: objectPath(ns, a.Name), kind: k}
		if err := c.before(call); err != nil {
			return true, nil, err
		}
		obj, err := c.w.patch(c.name, k, ns, a.Name, a.PatchType, a.Patch, a.Subresource)
		return true, obj, err
	case kt.DeleteActionImpl:
		if err := c.before(Call{Verb: "delete", Kind: k.GVR.Resource, Object: objectPath(ns, a.Name), kind: k}); err != nil {
			return true, nil, err
		}
		return true, nil, c.w.delete(c.name, k, ns, a.Name, a.DeleteOptions)
	}
	return true, nil, fmt.Errorf("crashtest: %s %s is not supported", a.GetVerb(), resourceName(k, a.GetSubresource()))
}

func nsOr(ns, fallback string) string {
	if ns != "" {
		return ns
	}
	return fallback
}

// before counts a write and lets the fault decide whether it gets through.
func (c *Conn) before(call Call) error {
	c.mu.Lock()
	call.N = c.writes
	c.writes++
	f := c.fault
	c.mu.Unlock()
	if f == nil {
		return nil
	}
	fired, err := f.Before(c, call)
	if fired {
		c.mu.Lock()
		if c.hit == nil {
			hit := call
			c.hit = &hit
		}
		c.mu.Unlock()
	}
	if err != nil {
		c.w.record(Entry{Actor: c.name, Verb: call.Verb, Kind: call.Kind, Object: call.Object, Fault: f.Name()}, err)
	}
	return err
}

func (c *Conn) watch(a kt.Action) (bool, watch.Interface, error) {
	if c.Dead() {
		return true, nil, errDead
	}
	k := c.w.byGVR[a.GetResource()]
	if k == nil {
		return true, nil, fmt.Errorf("crashtest: %s is not served by this world; add it to the target's kinds", a.GetResource())
	}
	var sel labels.Selector
	var bookmark runtime.Object
	if wa, ok := a.(kt.WatchActionImpl); ok {
		sel = wa.WatchRestrictions.Labels
		if si := wa.ListOptions.SendInitialEvents; si != nil && *si {
			obj, err := c.w.newObject(k)
			if err != nil {
				return true, nil, err
			}
			m, _ := meta.Accessor(obj)
			c.w.mu.Lock()
			m.SetResourceVersion(fmt.Sprint(c.w.rv))
			c.w.mu.Unlock()
			m.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
			bookmark = obj
		}
	}
	w := c.w.store.watch(k.GVR, a.GetNamespace(), sel, bookmark)
	c.mu.Lock()
	c.watches = append(c.watches, w)
	c.mu.Unlock()
	return true, w, nil
}

// event records Kubernetes Events in the log instead of storing them.
func (c *Conn) event(a kt.Action) (bool, runtime.Object, error) {
	switch a := a.(type) {
	case kt.CreateActionImpl:
		if ev, ok := a.Object.(*corev1.Event); ok {
			c.w.record(Entry{Actor: c.name, Verb: "event", Kind: ev.InvolvedObject.Kind,
				Object: objectPath(ev.InvolvedObject.Namespace, ev.InvolvedObject.Name), Result: ev.Reason + ": " + ev.Message}, nil)
		}
		return true, a.Object, nil
	case kt.UpdateActionImpl:
		return true, a.Object, nil
	case kt.PatchActionImpl:
		return true, &corev1.Event{}, nil
	}
	return true, nil, apierrors.NewNotFound(a.GetResource().GroupResource(), "")
}
