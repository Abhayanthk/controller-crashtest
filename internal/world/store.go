package world

import (
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// store keeps every object, like etcd behind a real API server.
// It knows nothing about API rules; api.go applies those before calling it.
// Everything that goes in or comes out is a deep copy.
type store struct {
	mu       sync.RWMutex
	objects  map[schema.GroupVersionResource]map[types.NamespacedName]runtime.Object
	watchers map[schema.GroupVersionResource][]*watcher
}

func newStore() *store {
	return &store{
		objects:  map[schema.GroupVersionResource]map[types.NamespacedName]runtime.Object{},
		watchers: map[schema.GroupVersionResource][]*watcher{},
	}
}

func key(obj runtime.Object) types.NamespacedName {
	m, _ := meta.Accessor(obj)
	return types.NamespacedName{Namespace: m.GetNamespace(), Name: m.GetName()}
}

func (s *store) get(gvr schema.GroupVersionResource, ns, name string) (runtime.Object, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[gvr][types.NamespacedName{Namespace: ns, Name: name}]
	if !ok {
		return nil, false
	}
	return obj.DeepCopyObject(), true
}

// list returns the objects in namespace ns ("" = all), sorted by namespace/name.
func (s *store) list(gvr schema.GroupVersionResource, ns string, sel labels.Selector) []runtime.Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []runtime.Object
	for k, obj := range s.objects[gvr] {
		if ns != "" && k.Namespace != ns {
			continue
		}
		if sel != nil && !sel.Empty() && !matches(sel, obj) {
			continue
		}
		out = append(out, obj.DeepCopyObject())
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := key(out[i]), key(out[j])
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return out
}

// put stores obj and tells the watchers (ADDED or MODIFIED).
func (s *store) put(gvr schema.GroupVersionResource, obj runtime.Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects[gvr] == nil {
		s.objects[gvr] = map[types.NamespacedName]runtime.Object{}
	}
	k := key(obj)
	_, existed := s.objects[gvr][k]
	s.objects[gvr][k] = obj.DeepCopyObject()
	ev := watch.Added
	if existed {
		ev = watch.Modified
	}
	s.notify(gvr, ev, obj)
}

// remove deletes the object and tells the watchers (DELETED).
func (s *store) remove(gvr schema.GroupVersionResource, ns, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := types.NamespacedName{Namespace: ns, Name: name}
	obj, ok := s.objects[gvr][k]
	if !ok {
		return
	}
	delete(s.objects[gvr], k)
	s.notify(gvr, watch.Deleted, obj)
}

func (s *store) notify(gvr schema.GroupVersionResource, t watch.EventType, obj runtime.Object) {
	live := s.watchers[gvr][:0]
	for _, w := range s.watchers[gvr] {
		if w.isStopped() {
			continue
		}
		live = append(live, w)
		if w.ns != "" && w.ns != key(obj).Namespace {
			continue
		}
		if w.sel != nil && !w.sel.Empty() && !matches(w.sel, obj) {
			continue
		}
		w.send(watch.Event{Type: t, Object: obj.DeepCopyObject()})
	}
	s.watchers[gvr] = live
}

// watch starts a watch. With bookmark set (watch-list, client-go's default
// since 1.35), it first sends every current object as ADDED and then the
// bookmark, atomically with registering the watcher, like a real API server.
func (s *store) watch(gvr schema.GroupVersionResource, ns string, sel labels.Selector, bookmark runtime.Object) *watcher {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := newWatcher(ns, sel)
	if bookmark != nil {
		var keys []types.NamespacedName
		for k := range s.objects[gvr] {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		for _, k := range keys {
			obj := s.objects[gvr][k]
			if (ns == "" || k.Namespace == ns) && (sel == nil || sel.Empty() || matches(sel, obj)) {
				w.send(watch.Event{Type: watch.Added, Object: obj.DeepCopyObject()})
			}
		}
		w.send(watch.Event{Type: watch.Bookmark, Object: bookmark})
	}
	s.watchers[gvr] = append(s.watchers[gvr], w)
	return w
}

func matches(sel labels.Selector, obj runtime.Object) bool {
	m, err := meta.Accessor(obj)
	return err == nil && sel.Matches(labels.Set(m.GetLabels()))
}

// watcher is a watch.Interface with an unbounded buffer. client-go's fake
// watcher panics once 100 events are waiting; a busy world can get there.
type watcher struct {
	ns   string
	sel  labels.Selector
	ch   chan watch.Event
	wake chan struct{}
	done chan struct{}

	mu      sync.Mutex
	queue   []watch.Event
	stopped bool
}

func newWatcher(ns string, sel labels.Selector) *watcher {
	w := &watcher{ns: ns, sel: sel, ch: make(chan watch.Event), wake: make(chan struct{}, 1), done: make(chan struct{})}
	go w.pump()
	return w
}

func (w *watcher) send(ev watch.Event) {
	w.mu.Lock()
	if !w.stopped {
		w.queue = append(w.queue, ev)
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *watcher) pump() {
	defer close(w.ch)
	for {
		w.mu.Lock()
		if len(w.queue) == 0 {
			w.mu.Unlock()
			select {
			case <-w.wake:
				continue
			case <-w.done:
				return
			}
		}
		ev := w.queue[0]
		w.queue = w.queue[1:]
		w.mu.Unlock()
		select {
		case w.ch <- ev:
		case <-w.done:
			return
		}
	}
}

func (w *watcher) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

func (w *watcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.stopped {
		w.stopped = true
		close(w.done)
	}
}

func (w *watcher) ResultChan() <-chan watch.Event { return w.ch }
