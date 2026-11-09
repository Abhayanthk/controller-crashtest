package modelserving

import (
	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

const (
	namespace = "default"
	name      = "demo"
	imageV1   = "vllm/vllm-openai:v0.10.0"
	imageV2   = "vllm/vllm-openai:v0.10.1"
	imageV3   = "vllm/vllm-openai:v0.10.2"
)

// demo is the ModelServing most scenarios start from: a disaggregated
// deployment with two roles per ServingGroup. Each prefill instance is an
// entry pod plus one worker pod; each decode instance is a single pod.
func demo(replicas int32) *workloadv1alpha1.ModelServing {
	return &workloadv1alpha1.ModelServing{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: workloadv1alpha1.ModelServingSpec{
			Replicas: ptr.To(replicas),
			Template: workloadv1alpha1.ServingGroup{Roles: []workloadv1alpha1.Role{
				role("prefill", 1, 1),
				role("decode", 1, 0),
			}},
		},
	}
}

func role(name string, replicas, workers int32) workloadv1alpha1.Role {
	r := workloadv1alpha1.Role{Name: name, Replicas: ptr.To(replicas), WorkerReplicas: workers, EntryTemplate: podTemplate(imageV1)}
	if workers > 0 {
		t := podTemplate(imageV1)
		r.WorkerTemplate = &t
	}
	return r
}

func podTemplate(image string) workloadv1alpha1.PodTemplateSpec {
	return workloadv1alpha1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "engine", Image: image}},
	}}
}

// ref names the demo ModelServing, for world.Change.
func ref() *workloadv1alpha1.ModelServing {
	return &workloadv1alpha1.ModelServing{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

// setImage changes the image of every pod template.
func setImage(ms *workloadv1alpha1.ModelServing, image string) {
	for i := range ms.Spec.Template.Roles {
		r := &ms.Spec.Template.Roles[i]
		for j := range r.EntryTemplate.Spec.Containers {
			r.EntryTemplate.Spec.Containers[j].Image = image
		}
		if r.WorkerTemplate != nil {
			for j := range r.WorkerTemplate.Spec.Containers {
				r.WorkerTemplate.Spec.Containers[j].Image = image
			}
		}
	}
}

func roleNamed(ms *workloadv1alpha1.ModelServing, name string) *workloadv1alpha1.Role {
	for i := range ms.Spec.Template.Roles {
		if ms.Spec.Template.Roles[i].Name == name {
			return &ms.Spec.Template.Roles[i]
		}
	}
	panic("no role " + name)
}

func rollingUpdate(maxUnavailable, maxSurge, partition *intstr.IntOrString) *workloadv1alpha1.RolloutStrategy {
	return &workloadv1alpha1.RolloutStrategy{
		Type: workloadv1alpha1.ServingGroupRollingUpdate,
		RollingUpdateConfiguration: &workloadv1alpha1.RollingUpdateConfiguration{
			MaxUnavailable: maxUnavailable, MaxSurge: maxSurge, Partition: partition,
		},
	}
}

func intOrStr(i int) *intstr.IntOrString { v := intstr.FromInt32(int32(i)); return &v }

// volcano is Volcano's PodGroup CRD. When it exists, kthena gang-schedules
// every ServingGroup with a PodGroup. The schema only carries what kthena
// looks at: spec.subGroupPolicy, which recent Volcano versions have.
func volcano() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "podgroups.scheduling.volcano.sh"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "scheduling.volcano.sh",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "podgroups", Kind: "PodGroup"},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1beta1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"spec": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"subGroupPolicy": {Type: "array"},
						}},
					},
				}},
			}},
		},
	}
}
