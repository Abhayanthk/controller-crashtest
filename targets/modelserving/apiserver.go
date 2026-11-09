package modelserving

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/webhook"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/abhayanthk/kthena-faultlab/internal/world"
)

// What the real API server does for ModelServing that a fake doesn't:
// apply the CRD's default markers, and call kthena's validating webhook.

// modelserving-crd.yaml is copied from the kthena version in go.mod
// (charts/kthena/charts/workload/crds/). `make crd` refreshes it.
//
//go:embed modelserving-crd.yaml
var crdYAML []byte

var crdSchema = func() *structuralschema.Structural {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(crdYAML, crd); err != nil {
		panic(err)
	}
	for _, v := range crd.Spec.Versions {
		if v.Name != "v1alpha1" {
			continue
		}
		props := &apiextensions.JSONSchemaProps{}
		if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v.Schema.OpenAPIV3Schema, props, nil); err != nil {
			panic(err)
		}
		s, err := structuralschema.NewStructural(props)
		if err != nil {
			panic(err)
		}
		return s
	}
	panic("modelserving-crd.yaml has no v1alpha1 version")
}()

// applyCRDDefaults fills in the CRD's default values, as the API server
// does when it decodes a ModelServing.
func applyCRDDefaults(obj runtime.Object) error {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	structuraldefaulting.Default(u, crdSchema)
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u, obj)
}

var validator = webhook.NewModelServingValidator()

// admit sends the object to kthena's own validating webhook handler.
func admit(op world.Op, obj, old runtime.Object) error {
	gv := workloadv1alpha1.SchemeGroupVersion
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	req := &admissionv1.AdmissionRequest{
		UID:       "faultlab",
		Kind:      metav1.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: "ModelServing"},
		Resource:  metav1.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: "modelservings"},
		Name:      m.GetName(),
		Namespace: m.GetNamespace(),
		Operation: admissionv1.Operation(op),
		Object:    runtime.RawExtension{Raw: raw},
	}
	if old != nil {
		if req.OldObject.Raw, err = json.Marshal(old); err != nil {
			return err
		}
	}
	body, err := json.Marshal(admissionv1.AdmissionReview{TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}, Request: req})
	if err != nil {
		return err
	}
	r := httptest.NewRequest(http.MethodPost, "/validate-workload-ai-v1alpha1-modelserving", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	validator.Handle(rec, r)
	if rec.Code != http.StatusOK {
		return fmt.Errorf("kthena webhook answered HTTP %d: %s", rec.Code, rec.Body.String())
	}
	var resp admissionv1.AdmissionReview
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return err
	}
	if resp.Response == nil || !resp.Response.Allowed {
		msg := "denied"
		if resp.Response != nil && resp.Response.Result != nil {
			msg = resp.Response.Result.Message
		}
		return fmt.Errorf("admission webhook denied the request: %s", msg)
	}
	return nil
}
