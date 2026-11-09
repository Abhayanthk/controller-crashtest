// Package toy is a tiny controller with bugs you can switch on. crashtest's
// own tests run the checks against it: each planted bug must be caught, and
// the bug-free version must pass. It is also the smallest example of a
// target (see docs/adding-a-controller.md).
//
// What it does: for every ConfigMap labelled app=toy, keep a Secret named
// <name>-copy holding the same data, and record the Secret's name in the
// ConfigMap's "toy/copied-to" annotation.
package toy

import (
	"context"
	"reflect"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/abhayanthk/controller-crashtest/internal/lab"
	"github.com/abhayanthk/controller-crashtest/internal/world"
)

// Bugs are the mistakes the controller can be told to make.
type Bugs struct {
	// ForgetOnRestart names the copy after the process that made it and
	// keeps the name only in memory, so a restarted process makes a second
	// copy.
	ForgetOnRestart bool
	// RewriteStatus writes the annotation on every reconcile, changed or not.
	RewriteStatus bool
	// SwallowErrors drops a failed write instead of retrying it.
	SwallowErrors bool
}

// Target returns a toy target with the given bugs.
func Target(bugs Bugs) lab.Target { return target{bugs} }

type target struct{ bugs Bugs }

func (target) Name() string { return "toy" }

var (
	configMaps = world.Kind{GVR: corev1.SchemeGroupVersion.WithResource("configmaps"), GVK: corev1.SchemeGroupVersion.WithKind("ConfigMap"), Compare: true}
	secrets    = world.Kind{GVR: corev1.SchemeGroupVersion.WithResource("secrets"), GVK: corev1.SchemeGroupVersion.WithKind("Secret"), Compare: true}
)

func (target) NewWorld() *world.World { return world.New(scheme.Scheme, configMaps, secrets) }

func source(data string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "default", Labels: map[string]string{"app": "toy"}},
		Data:       map[string]string{"value": data},
	}
}

func (target) Scenarios() []lab.Scenario {
	return []lab.Scenario{
		{
			Name:   "create",
			About:  "create a ConfigMap",
			Setup:  func(*world.World) error { return nil },
			Change: func(w *world.World) error { return w.Create(source("1")) },
		},
		{
			Name:  "update",
			About: "change the ConfigMap's data",
			Setup: func(w *world.World) error { return w.Create(source("1")) },
			Change: func(w *world.World) error {
				return world.Change(w, source(""), func(cm *corev1.ConfigMap) { cm.Data["value"] = "2" })
			},
		},
	}
}

func (t target) Start(ctx context.Context, conn *world.Conn) (lab.Controller, error) {
	kube := kubefake.NewClientset()
	conn.Attach(kube)
	c := newController(kube, t.bugs, conn.Name())
	probe, err := lab.WatchQueue(conn.Name(), c, "workqueue", "syncHandler")
	if err != nil {
		return nil, err
	}
	go c.run(ctx, 2)
	return probe, nil
}

type controller struct {
	kube        kubernetes.Interface
	factory     informers.SharedInformerFactory
	configMaps  corelisters.ConfigMapLister
	secrets     corelisters.SecretLister
	workqueue   workqueue.TypedRateLimitingInterface[any]
	syncHandler func(ctx context.Context, key string) error
	bugs        Bugs
	process     string

	mu    sync.Mutex
	names map[string]string // ForgetOnRestart's memory
}

func newController(kube kubernetes.Interface, bugs Bugs, process string) *controller {
	f := informers.NewSharedInformerFactory(kube, 0)
	c := &controller{
		kube: kube, factory: f, bugs: bugs, process: process, names: map[string]string{},
		configMaps: f.Core().V1().ConfigMaps().Lister(),
		secrets:    f.Core().V1().Secrets().Lister(),
		workqueue:  workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[any]()),
	}
	c.syncHandler = c.sync
	enqueue := func(obj any) {
		if key, err := cache.MetaNamespaceKeyFunc(obj); err == nil {
			c.workqueue.Add(key)
		}
	}
	_, _ = f.Core().V1().ConfigMaps().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: enqueue, UpdateFunc: func(_, o any) { enqueue(o) }, DeleteFunc: enqueue,
	})
	_, _ = f.Core().V1().Secrets().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj any) {
			if s, ok := obj.(*corev1.Secret); ok && len(s.OwnerReferences) > 0 {
				c.workqueue.Add(s.Namespace + "/" + s.OwnerReferences[0].Name)
			}
		},
	})
	return c
}

func (c *controller) run(ctx context.Context, workers int) {
	defer c.workqueue.ShutDown()
	c.factory.Start(ctx.Done())
	c.factory.WaitForCacheSync(ctx.Done())
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, func(ctx context.Context) {
			for c.next(ctx) {
			}
		}, time.Second)
	}
	<-ctx.Done()
}

func (c *controller) next(ctx context.Context) bool {
	key, quit := c.workqueue.Get()
	if quit {
		return false
	}
	defer c.workqueue.Done(key)
	if err := c.syncHandler(ctx, key.(string)); err != nil {
		c.workqueue.AddRateLimited(key)
		return true
	}
	c.workqueue.Forget(key)
	return true
}

func (c *controller) sync(ctx context.Context, key string) error {
	ns, name, _ := cache.SplitMetaNamespaceKey(key)
	cm, err := c.configMaps.ConfigMaps(ns).Get(name)
	if apierrors.IsNotFound(err) || err == nil && cm.Labels["app"] != "toy" {
		return nil
	}
	if err != nil {
		return err
	}
	copyName := name + "-copy"
	if c.bugs.ForgetOnRestart {
		c.mu.Lock()
		if c.names[key] == "" {
			c.names[key] = name + "-copy-" + c.process
		}
		copyName = c.names[key]
		c.mu.Unlock()
	}
	want := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: copyName, Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cm, corev1.SchemeGroupVersion.WithKind("ConfigMap"))}},
		StringData: nil,
		Data:       map[string][]byte{},
	}
	for k, v := range cm.Data {
		want.Data[k] = []byte(v)
	}
	switch got, err := c.secrets.Secrets(ns).Get(copyName); {
	case apierrors.IsNotFound(err):
		_, err = c.kube.CoreV1().Secrets(ns).Create(ctx, want, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return c.failed(err)
		}
	case err != nil:
		return err
	case !reflect.DeepEqual(got.Data, want.Data):
		got = got.DeepCopy()
		got.Data = want.Data
		if _, err := c.kube.CoreV1().Secrets(ns).Update(ctx, got, metav1.UpdateOptions{}); err != nil {
			return c.failed(err)
		}
	}
	if cm.Annotations["toy/copied-to"] == copyName && !c.bugs.RewriteStatus {
		return nil
	}
	cm = cm.DeepCopy()
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations["toy/copied-to"] = copyName
	if _, err := c.kube.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return c.failed(err)
	}
	return nil
}

func (c *controller) failed(err error) error {
	if c.bugs.SwallowErrors {
		return nil
	}
	return err
}

var _ runtime.Object = (*corev1.Secret)(nil)
