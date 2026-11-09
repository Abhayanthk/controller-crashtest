// Package modelserving is the faultlab target for kthena's ModelServing
// controller (github.com/volcano-sh/kthena/pkg/model-serving-controller).
//
// The controller is started exactly as kthena-controller-manager starts it
// (NewModelServingController, then Run with 5 workers), on fake clientsets
// wired to the pretend cluster.
package modelserving

import (
	"context"

	"github.com/volcano-sh/kthena/pkg/model-serving-controller/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanofake "volcano.sh/apis/pkg/client/clientset/versioned/fake"

	kthenafake "github.com/volcano-sh/kthena/client-go/clientset/versioned/fake"
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"

	"github.com/abhayanthk/kthena-faultlab/internal/lab"
	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// workers matches kthena-controller-manager's --workers default.
const workers = 5

type target struct{}

// New returns the ModelServing target.
func New() lab.Target { return target{} }

func (target) Name() string { return "modelserving" }

var scheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(kubescheme.AddToScheme(s))
	utilruntime.Must(workloadv1alpha1.AddToScheme(s))
	utilruntime.Must(schedulingv1beta1.AddToScheme(s))
	utilruntime.Must(apiextensionsv1.AddToScheme(s))
	return s
}()

// Kinds the controller reads or writes. Compare marks what it owns.
var (
	podKind = world.Kind{GVR: corev1.SchemeGroupVersion.WithResource("pods"), GVK: corev1.SchemeGroupVersion.WithKind("Pod"),
		Status: true, Compare: true}
	serviceKind = world.Kind{GVR: corev1.SchemeGroupVersion.WithResource("services"), GVK: corev1.SchemeGroupVersion.WithKind("Service"),
		Status: true, Compare: true}
	revisionKind = world.Kind{GVR: appsv1.SchemeGroupVersion.WithResource("controllerrevisions"), GVK: appsv1.SchemeGroupVersion.WithKind("ControllerRevision"),
		Compare: true}
	modelServingKind = world.Kind{GVR: workloadv1alpha1.SchemeGroupVersion.WithResource("modelservings"), GVK: workloadv1alpha1.SchemeGroupVersion.WithKind("ModelServing"),
		Status: true, Custom: true, Compare: true, Default: applyCRDDefaults, Admit: admit}
	podGroupKind = world.Kind{GVR: schedulingv1beta1.SchemeGroupVersion.WithResource("podgroups"), GVK: schedulingv1beta1.SchemeGroupVersion.WithKind("PodGroup"),
		Status: true, Custom: true, Compare: true}
	crdKind = world.Kind{GVR: apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"), GVK: apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition"),
		ClusterScoped: true}
)

func (target) NewWorld() *world.World {
	return world.New(scheme, podKind, serviceKind, revisionKind, modelServingKind, podGroupKind, crdKind)
}

func (target) Start(ctx context.Context, conn *world.Conn) (lab.Controller, error) {
	kube := kubefake.NewClientset()
	kthena := kthenafake.NewSimpleClientset()
	volcano := volcanofake.NewSimpleClientset()
	apiext := apiextfake.NewSimpleClientset()
	for _, f := range []world.Fake{kube, kthena, volcano, apiext} {
		conn.Attach(f)
	}
	c, err := controller.NewModelServingController(kube, kthena, volcano, apiext)
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
