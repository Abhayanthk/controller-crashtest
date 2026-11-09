package world

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// The functions in this file act as a person with kubectl would: through
// the API rules (defaults, admission, conflicts), never through faults.
// Scenarios use them to set up the world and to make the change under test.

// Create creates each object.
func (w *World) Create(objs ...runtime.Object) error {
	for _, obj := range objs {
		k, err := w.kindOf(obj)
		if err != nil {
			return err
		}
		m, _ := meta.Accessor(obj)
		if _, err := w.create("user", k, m.GetNamespace(), obj); err != nil {
			return err
		}
	}
	return nil
}

// Get returns the stored copy of the object with obj's kind, namespace and name.
func (w *World) Get(obj runtime.Object) (runtime.Object, error) {
	k, err := w.kindOf(obj)
	if err != nil {
		return nil, err
	}
	m, _ := meta.Accessor(obj)
	return w.get(k, m.GetNamespace(), m.GetName())
}

// Update stores a changed object; it must carry the current resourceVersion.
func (w *World) Update(obj runtime.Object) (runtime.Object, error) {
	k, err := w.kindOf(obj)
	if err != nil {
		return nil, err
	}
	m, _ := meta.Accessor(obj)
	return w.update("user", k, m.GetNamespace(), obj, "")
}

// Delete deletes the object (pods terminate gracefully, finalizers are kept).
func (w *World) Delete(obj runtime.Object) error {
	k, err := w.kindOf(obj)
	if err != nil {
		return err
	}
	m, _ := meta.Accessor(obj)
	return w.delete("user", k, m.GetNamespace(), m.GetName(), metav1.DeleteOptions{})
}

// List returns every stored object of a kind, sorted by namespace and name.
func (w *World) List(k *Kind) []runtime.Object { return w.store.list(k.GVR, "", nil) }

// Change reads the latest copy of obj, applies edit and stores it, like
// `kubectl edit`. T is the object's pointer type, e.g. *corev1.Pod.
func Change[T runtime.Object](w *World, obj T, edit func(T)) error {
	cur, err := w.Get(obj)
	if err != nil {
		return err
	}
	t := cur.(T)
	edit(t)
	_, err = w.Update(t)
	return err
}
