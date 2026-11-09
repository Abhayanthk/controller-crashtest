package world

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// This file is the pretend API server: the rules a real kube-apiserver
// applies to every request, which client-go's fake clientset skips.
//
//   resourceVersion   bumped on every change; an update with an old one gets 409 Conflict
//   uid               set on create; delete preconditions check it
//   generation        1 on create, +1 when anything but metadata/status changes
//   status            create can't set it; update keeps it; update of /status changes only it
//   no-op writes      an update that changes nothing is not stored and sends no watch event
//   finalizers        delete only sets deletionTimestamp until the last finalizer is gone
//   pods              deleting a running pod waits for the kubelet (graceful termination)
//   garbage collector deleting an owner deletes what it owns (ownerReferences)
//   CRD defaults      Kind.Default runs on create and update
//   admission         Kind.Admit runs on create and update (validating webhooks)

var errSubresource = errors.New("subresource not served")

func isPod(k *Kind) bool { return k.GVR.Group == "" && k.GVR.Resource == "pods" }

func resourceName(k *Kind, sub string) string {
	if sub == "" {
		return k.GVR.Resource
	}
	return k.GVR.Resource + "/" + sub
}

func objectPath(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

func (w *World) record(e Entry, err error) {
	if err != nil {
		msg := err.Error()
		if len(msg) > 240 {
			msg = msg[:240] + "..."
		}
		e.Result = "error: " + msg
	}
	if e.IsWrite() {
		w.lastWrite.Store(time.Now().UnixNano())
	}
	w.log.add(e, w.start)
	if e.Changed() {
		w.hooksMu.Lock()
		hooks := w.hooks
		w.hooksMu.Unlock()
		for _, h := range hooks {
			h()
		}
	}
}

func (w *World) get(k *Kind, ns, name string) (runtime.Object, error) {
	if obj, ok := w.store.get(k.GVR, ns, name); ok {
		return obj, nil
	}
	return nil, apierrors.NewNotFound(k.GVR.GroupResource(), name)
}

func (w *World) list(k *Kind, ns string, sel labels.Selector) (runtime.Object, error) {
	list, err := w.scheme.New(k.GVK.GroupVersion().WithKind(k.GVK.Kind + "List"))
	if err != nil {
		return nil, err
	}
	if err := meta.SetList(list, w.store.list(k.GVR, ns, sel)); err != nil {
		return nil, err
	}
	if lm, err := meta.ListAccessor(list); err == nil {
		w.mu.Lock()
		lm.SetResourceVersion(fmt.Sprint(w.rv))
		w.mu.Unlock()
	}
	return list, nil
}

func (w *World) create(actor string, k *Kind, ns string, in runtime.Object) (runtime.Object, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	obj := in.DeepCopyObject()
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	if k.ClusterScoped {
		m.SetNamespace("")
	} else if m.GetNamespace() == "" {
		m.SetNamespace(ns)
	}
	if m.GetName() == "" && m.GetGenerateName() != "" {
		m.SetName(fmt.Sprintf("%s%05d", m.GetGenerateName(), w.uid+1))
	}
	e := Entry{Actor: actor, Verb: "create", Kind: k.GVR.Resource, Object: objectPath(m.GetNamespace(), m.GetName())}
	if m.GetName() == "" {
		err := apierrors.NewInvalid(k.GVK.GroupKind(), "", field.ErrorList{field.Required(field.NewPath("metadata", "name"), "name or generateName is required")})
		w.record(e, err)
		return nil, err
	}
	if _, exists := w.store.get(k.GVR, m.GetNamespace(), m.GetName()); exists {
		err := apierrors.NewAlreadyExists(k.GVR.GroupResource(), m.GetName())
		w.record(e, err)
		return nil, err
	}
	if k.Default != nil {
		if err := k.Default(obj); err != nil {
			w.record(e, err)
			return nil, err
		}
	}
	if k.Status { // create cannot set status
		if obj, err = w.withStatus(k, obj, nil); err != nil {
			return nil, err
		}
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	}
	if k.Admit != nil {
		if err := k.Admit(Create, obj, nil); err != nil {
			err = apierrors.NewForbidden(k.GVR.GroupResource(), e.Object, err)
			w.record(e, err)
			return nil, err
		}
	}
	m, _ = meta.Accessor(obj)
	m.SetUID(types.UID(w.nextUID()))
	m.SetResourceVersion(w.nextRV())
	m.SetCreationTimestamp(metav1.Now())
	m.SetGeneration(1)
	m.SetDeletionTimestamp(nil)
	m.SetDeletionGracePeriodSeconds(nil)
	w.store.put(k.GVR, obj)
	e.Result = "created"
	w.record(e, nil)
	if refs := m.GetOwnerReferences(); len(refs) > 0 && !w.anyOwnerExists(refs) {
		// The garbage collector deletes objects whose owners are all gone.
		_ = w.deleteLocked("gc", k, m.GetNamespace(), m.GetName(), metav1.DeleteOptions{})
	}
	return obj.DeepCopyObject(), nil
}

func (w *World) anyOwnerExists(refs []metav1.OwnerReference) bool {
	for _, k := range w.kinds {
		for _, obj := range w.store.list(k.GVR, "", nil) {
			m, _ := meta.Accessor(obj)
			for _, r := range refs {
				if r.UID == m.GetUID() {
					return true
				}
			}
		}
	}
	return false
}

func (w *World) update(actor string, k *Kind, ns string, in runtime.Object, sub string) (runtime.Object, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	obj := in.DeepCopyObject()
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	if !k.ClusterScoped && m.GetNamespace() == "" {
		m.SetNamespace(ns)
	}
	e := Entry{Actor: actor, Verb: "update", Kind: resourceName(k, sub), Object: objectPath(m.GetNamespace(), m.GetName())}
	old, err := w.get(k, m.GetNamespace(), m.GetName())
	if err != nil {
		w.record(e, err)
		return nil, err
	}
	if rv := m.GetResourceVersion(); rv == "" && k.Custom {
		err := apierrors.NewInvalid(k.GVK.GroupKind(), m.GetName(), field.ErrorList{field.Invalid(field.NewPath("metadata", "resourceVersion"), rv, "must be specified for an update")})
		w.record(e, err)
		return nil, err
	}
	out, err := w.apply(e, k, obj, old, sub)
	return out, err
}

func (w *World) patch(actor string, k *Kind, ns, name string, pt types.PatchType, data []byte, sub string) (runtime.Object, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := Entry{Actor: actor, Verb: "patch", Kind: resourceName(k, sub), Object: objectPath(ns, name)}
	old, err := w.get(k, ns, name)
	if err != nil {
		w.record(e, err)
		return nil, err
	}
	oldJSON, err := json.Marshal(old)
	if err != nil {
		return nil, err
	}
	var newJSON []byte
	switch pt {
	case types.JSONPatchType:
		var p jsonpatch.Patch
		if p, err = jsonpatch.DecodePatch(data); err == nil {
			newJSON, err = p.Apply(oldJSON)
		}
	case types.MergePatchType:
		newJSON, err = jsonpatch.MergePatch(oldJSON, data)
	case types.StrategicMergePatchType:
		if k.Custom {
			err = apierrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, "patch", k.GVR.GroupResource(), name,
				"strategic merge patch is not supported for custom resources", 0, false)
			break
		}
		var schemaObj runtime.Object
		if schemaObj, err = w.newObject(k); err == nil {
			newJSON, err = strategicpatch.StrategicMergePatch(oldJSON, data, schemaObj)
		}
	default:
		err = apierrors.NewBadRequest(fmt.Sprintf("faultlab: patch type %q is not supported", pt))
	}
	if err != nil {
		if !apierrors.IsUnsupportedMediaType(err) && apierrors.ReasonForError(err) == metav1.StatusReasonUnknown {
			err = apierrors.NewBadRequest(err.Error())
		}
		w.record(e, err)
		return nil, err
	}
	obj, err := w.newObject(k)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(newJSON, obj); err != nil {
		err = apierrors.NewBadRequest(err.Error())
		w.record(e, err)
		return nil, err
	}
	return w.apply(e, k, obj, old, sub)
}

// apply runs the rules shared by update and patch, then stores the result.
// The caller holds w.mu.
func (w *World) apply(e Entry, k *Kind, obj, old runtime.Object, sub string) (runtime.Object, error) {
	if sub != "" && (sub != "status" || !k.Status) {
		err := apierrors.NewNotFound(k.GVR.GroupResource(), e.Object+"/"+sub)
		w.record(e, fmt.Errorf("%w: %v", errSubresource, err))
		return nil, err
	}
	m, _ := meta.Accessor(obj)
	om, _ := meta.Accessor(old)
	if rv := m.GetResourceVersion(); rv != "" && rv != om.GetResourceVersion() {
		err := apierrors.NewConflict(k.GVR.GroupResource(), m.GetName(),
			errors.New("the object has been modified; please apply your changes to the latest version and try again"))
		w.record(e, err)
		return nil, err
	}
	var err error
	switch {
	case sub == "status": // only status changes
		obj, err = w.withStatus(k, old, obj)
	case k.Status: // status stays as stored
		if k.Default != nil {
			if err = k.Default(obj); err != nil {
				w.record(e, err)
				return nil, err
			}
		}
		obj, err = w.withStatus(k, obj, old)
	case k.Default != nil:
		err = k.Default(obj)
	}
	if err != nil {
		w.record(e, err)
		return nil, err
	}
	m, _ = meta.Accessor(obj)
	m.SetUID(om.GetUID())
	m.SetCreationTimestamp(om.GetCreationTimestamp())
	m.SetDeletionTimestamp(om.GetDeletionTimestamp())
	m.SetDeletionGracePeriodSeconds(om.GetDeletionGracePeriodSeconds())
	m.SetResourceVersion(om.GetResourceVersion())
	m.SetGeneration(om.GetGeneration())
	if sub == "" {
		if k.Admit != nil {
			if err := k.Admit(Update, obj, old); err != nil {
				err = apierrors.NewForbidden(k.GVR.GroupResource(), e.Object, err)
				w.record(e, err)
				return nil, err
			}
		}
		if changed, err := specChanged(obj, old); err == nil && changed {
			m.SetGeneration(om.GetGeneration() + 1)
		}
	}
	if equality.Semantic.DeepEqual(obj, old) {
		e.Result = "no change"
		w.record(e, nil)
		return old, nil
	}
	m.SetResourceVersion(w.nextRV())
	if m.GetDeletionTimestamp() != nil && len(m.GetFinalizers()) == 0 && !isPod(k) {
		w.store.remove(k.GVR, m.GetNamespace(), m.GetName())
		e.Result = "deleted"
		w.record(e, nil)
		w.collectGarbage(m.GetUID())
		return obj, nil
	}
	w.store.put(k.GVR, obj)
	e.Result = "updated"
	w.record(e, nil)
	return obj.DeepCopyObject(), nil
}

func (w *World) delete(actor string, k *Kind, ns, name string, opts metav1.DeleteOptions) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deleteLocked(actor, k, ns, name, opts)
}

func (w *World) deleteLocked(actor string, k *Kind, ns, name string, opts metav1.DeleteOptions) error {
	e := Entry{Actor: actor, Verb: "delete", Kind: k.GVR.Resource, Object: objectPath(ns, name)}
	old, err := w.get(k, ns, name)
	if err != nil {
		w.record(e, err)
		return err
	}
	m, _ := meta.Accessor(old)
	if p := opts.Preconditions; p != nil {
		var msg string
		if p.UID != nil && *p.UID != m.GetUID() {
			msg = fmt.Sprintf("Precondition failed: UID in precondition: %v, UID in object meta: %v", *p.UID, m.GetUID())
		}
		if p.ResourceVersion != nil && *p.ResourceVersion != m.GetResourceVersion() {
			msg = fmt.Sprintf("Precondition failed: ResourceVersion in precondition: %v, ResourceVersion in object meta: %v", *p.ResourceVersion, m.GetResourceVersion())
		}
		if msg != "" {
			err := apierrors.NewConflict(k.GVR.GroupResource(), name, errors.New(msg))
			w.record(e, err)
			return err
		}
	}
	if m.GetDeletionTimestamp() != nil {
		e.Result = "no change"
		w.record(e, nil)
		return nil
	}
	grace := int64(0)
	if pod, ok := old.(*corev1.Pod); ok {
		grace = podGracePeriod(pod, opts)
	}
	if len(m.GetFinalizers()) > 0 || grace > 0 {
		ts := metav1.NewTime(time.Now().Add(time.Duration(grace) * time.Second))
		m.SetDeletionTimestamp(&ts)
		m.SetDeletionGracePeriodSeconds(&grace)
		m.SetResourceVersion(w.nextRV())
		w.store.put(k.GVR, old)
		e.Result = "deleting"
		w.record(e, nil)
		return nil
	}
	w.store.remove(k.GVR, ns, name)
	e.Result = "deleted"
	w.record(e, nil)
	w.collectGarbage(m.GetUID())
	return nil
}

// podGracePeriod mirrors the API server: pods that never reached a node, and
// pods that already finished, go at once; running pods wait for the kubelet.
func podGracePeriod(pod *corev1.Pod, opts metav1.DeleteOptions) int64 {
	if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return 0
	}
	if opts.GracePeriodSeconds != nil {
		return *opts.GracePeriodSeconds
	}
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		return *pod.Spec.TerminationGracePeriodSeconds
	}
	return corev1.DefaultTerminationGracePeriodSeconds
}

// collectGarbage plays the garbage collector: objects owned only by the
// removed object are deleted too (background propagation).
func (w *World) collectGarbage(owner types.UID) {
	for _, k := range w.kinds {
		for _, obj := range w.store.list(k.GVR, "", nil) {
			m, _ := meta.Accessor(obj)
			refs := m.GetOwnerReferences()
			if len(refs) == 0 || m.GetDeletionTimestamp() != nil {
				continue
			}
			ownedOnlyByRemoved := true
			for _, r := range refs {
				if r.UID != owner {
					ownedOnlyByRemoved = false
				}
			}
			if ownedOnlyByRemoved {
				_ = w.deleteLocked("gc", k, m.GetNamespace(), m.GetName(), metav1.DeleteOptions{})
			}
		}
	}
}

// system writes obj as a system component would (kubelet, scheduler): no
// admission, no subresource rules, no faults. The caller holds w.mu.
func (w *World) system(actor, verb string, k *Kind, obj runtime.Object) {
	m, _ := meta.Accessor(obj)
	m.SetResourceVersion(w.nextRV())
	w.store.put(k.GVR, obj)
	w.record(Entry{Actor: actor, Verb: verb, Kind: k.GVR.Resource, Object: objectPath(m.GetNamespace(), m.GetName()), Result: "updated"}, nil)
}

// withStatus returns obj with its status taken from src (nil src = no status).
func (w *World) withStatus(k *Kind, obj, src runtime.Object) (runtime.Object, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	delete(m, "status")
	if src != nil {
		sm, err := runtime.DefaultUnstructuredConverter.ToUnstructured(src)
		if err != nil {
			return nil, err
		}
		if s, ok := sm["status"]; ok {
			m["status"] = s
		}
	}
	out, err := w.newObject(k)
	if err != nil {
		return nil, err
	}
	return out, runtime.DefaultUnstructuredConverter.FromUnstructured(m, out)
}

// specChanged reports whether anything other than metadata and status differs.
func specChanged(a, b runtime.Object) (bool, error) {
	am, err := runtime.DefaultUnstructuredConverter.ToUnstructured(a)
	if err != nil {
		return false, err
	}
	bm, err := runtime.DefaultUnstructuredConverter.ToUnstructured(b)
	if err != nil {
		return false, err
	}
	for _, m := range []map[string]any{am, bm} {
		delete(m, "metadata")
		delete(m, "status")
		delete(m, "apiVersion")
		delete(m, "kind")
	}
	return !equality.Semantic.DeepEqual(am, bm), nil
}
