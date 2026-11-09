package world

import (
	"crypto/sha256"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var podsGVR = corev1.SchemeGroupVersion.WithResource("pods")

// KubeletTick plays the scheduler and every node's kubelet for one round,
// in name order:
//   - a pod being deleted finishes terminating and is removed;
//   - a pod not running yet is placed on a node, started and reported Ready;
//   - a container whose image changed in the spec is restarted on the new image.
//
// It returns how many pods it changed. A real kubelet is much slower than a
// controller, so the runner only calls this when the controller is idle.
func (w *World) KubeletTick() int {
	k := w.byGVR[podsGVR]
	if k == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	changed := 0
	for _, obj := range w.store.list(k.GVR, "", nil) {
		pod := obj.(*corev1.Pod)
		switch {
		case pod.DeletionTimestamp != nil:
			w.store.remove(k.GVR, pod.Namespace, pod.Name)
			w.record(Entry{Actor: "kubelet", Verb: "delete", Kind: "pods", Object: objectPath(pod.Namespace, pod.Name), Result: "deleted"}, nil)
			w.collectGarbage(pod.UID)
		case len(pod.Spec.SchedulingGates) > 0 || upToDate(pod):
			continue
		default:
			runPod(pod)
			w.system("kubelet", "update", k, pod)
		}
		changed++
	}
	return changed
}

// ErrNotRunning means the pod is gone or has no running container, so a
// container in it can't crash.
var ErrNotRunning = errors.New("pod is not running")

// RestartContainer plays a container crash: the kubelet restarts the first
// container of the pod, so its restartCount goes up by one, and the pod is
// not Ready until the next kubelet round (a restarted model server has to
// load its model again before its readiness probe passes).
func (w *World) RestartContainer(ns, name string) error {
	k := w.byGVR[podsGVR]
	w.mu.Lock()
	defer w.mu.Unlock()
	obj, ok := w.store.get(podsGVR, ns, name)
	if !ok {
		return fmt.Errorf("%w: %v", ErrNotRunning, apierrors.NewNotFound(podsGVR.GroupResource(), name))
	}
	pod := obj.(*corev1.Pod)
	if pod.DeletionTimestamp != nil || len(pod.Status.ContainerStatuses) == 0 {
		return fmt.Errorf("%w: %s/%s", ErrNotRunning, ns, name)
	}
	now := metav1.Now()
	s := &pod.Status.ContainerStatuses[0]
	s.RestartCount++
	s.ContainerID = containerID(pod, s.Name, s.RestartCount)
	s.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", FinishedAt: now}}
	s.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}
	s.Ready = false
	for i := range pod.Status.Conditions {
		if c := &pod.Status.Conditions[i]; c.Type == corev1.PodReady || c.Type == corev1.ContainersReady {
			c.Status, c.LastTransitionTime = corev1.ConditionFalse, now
		}
	}
	w.system("kubelet", "update", k, pod)
	return nil
}

func upToDate(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || !conditionTrue(pod, corev1.PodReady) ||
		len(pod.Status.ContainerStatuses) != len(pod.Spec.Containers) {
		return false
	}
	for _, c := range pod.Spec.Containers {
		s := containerStatus(pod.Status.ContainerStatuses, c.Name)
		if s == nil || s.Image != c.Image || !s.Ready {
			return false
		}
	}
	return true
}

func runPod(pod *corev1.Pod) {
	now := metav1.Now()
	if pod.Spec.NodeName == "" {
		pod.Spec.NodeName = "node-1"
	}
	st := &pod.Status
	st.Phase = corev1.PodRunning
	st.HostIP, st.HostIPs = "10.0.0.1", []corev1.HostIP{{IP: "10.0.0.1"}}
	ip := podIP(pod.Namespace + "/" + pod.Name)
	st.PodIP, st.PodIPs = ip, []corev1.PodIP{{IP: ip}}
	if st.StartTime == nil {
		st.StartTime = &now
	}
	started := true
	var statuses []corev1.ContainerStatus
	for _, c := range pod.Spec.Containers {
		var restarts int32
		var last corev1.ContainerState
		if old := containerStatus(st.ContainerStatuses, c.Name); old != nil {
			restarts, last = old.RestartCount, old.LastTerminationState
			if old.Image != c.Image { // a new image in place restarts the container
				restarts++
				last = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", FinishedAt: now}}
			}
		}
		statuses = append(statuses, corev1.ContainerStatus{
			Name: c.Name, Image: c.Image, ImageID: imageID(c.Image),
			ContainerID: containerID(pod, c.Name, restarts), Ready: true, Started: &started, RestartCount: restarts,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
			LastTerminationState: last,
		})
	}
	st.ContainerStatuses = statuses
	st.InitContainerStatuses = nil
	for _, c := range pod.Spec.InitContainers {
		st.InitContainerStatuses = append(st.InitContainerStatuses, corev1.ContainerStatus{
			Name: c.Name, Image: c.Image, ImageID: imageID(c.Image), ContainerID: containerID(pod, c.Name, 0),
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", FinishedAt: now}},
		})
	}
	for _, t := range []corev1.PodConditionType{corev1.PodScheduled, corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady} {
		setCondition(pod, t, now)
	}
}

func setCondition(pod *corev1.Pod, t corev1.PodConditionType, now metav1.Time) {
	for i := range pod.Status.Conditions {
		if c := &pod.Status.Conditions[i]; c.Type == t {
			if c.Status != corev1.ConditionTrue {
				c.Status, c.LastTransitionTime = corev1.ConditionTrue, now
			}
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: t, Status: corev1.ConditionTrue, LastTransitionTime: now})
}

func conditionTrue(pod *corev1.Pod, t corev1.PodConditionType) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == t {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func containerStatus(ss []corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	for i := range ss {
		if ss[i].Name == name {
			return &ss[i]
		}
	}
	return nil
}

func containerID(pod *corev1.Pod, container string, restarts int32) string {
	return fmt.Sprintf("containerd://%s-%s-%d", pod.UID, container, restarts)
}

func imageID(image string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(image))) }

func podIP(key string) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("10.%d.%d.%d", h[0], h[1], h[2]|1)
}
