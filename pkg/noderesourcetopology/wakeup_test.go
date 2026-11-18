/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package noderesourcetopology

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-logr/logr/testr"
	topologyv1alpha2 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/go-logr/logr"
)

const wakeupProfile = "sched"

func TestWakeupActivatesPodThatFits(t *testing.T) {
	nrt := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "4", "8Gi"))
	pod := guaranteedPod(wakeupProfile, "ns", "fits", 2)
	w, rejected, activated := newTestWaker(pod)
	w.withNRTs(nrt)
	rejected.keys = []string{"ns/fits"}

	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{nrt})

	if len(activated) != 1 || activated["ns/fits"] != pod {
		t.Fatalf("activated = %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 {
		t.Fatalf("fitting pod was dropped from the rejected set: %v", rejected.keys)
	}
}

func TestWakeupLeavesPodThatDoesNotFit(t *testing.T) {
	nrt := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "2", "8Gi"))
	pod := guaranteedPod(wakeupProfile, "ns", "too-big", 4)
	w, rejected, activated := newTestWaker(pod)
	w.withNRTs(nrt)
	rejected.keys = []string{"ns/too-big"}

	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{nrt})

	if len(activated) != 0 {
		t.Fatalf("activated = %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 || rejected.keys[0] != "ns/too-big" {
		t.Fatalf("rejected keys = %v", rejected.keys)
	}
}

func TestWakeupContainerScopeSubtractsBeforeTheNextContainer(t *testing.T) {
	// One NUMA node has 4 CPUs. Each container asks for 3.
	// Checked alone, either container fits. After the first subtraction, the second does not.
	nrt := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "4", "8Gi"))
	pod := guaranteedPod(wakeupProfile, "ns", "split", 3, 3)
	w, rejected, activated := newTestWaker(pod)
	w.withNRTs(nrt)
	rejected.keys = []string{"ns/split"}

	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{nrt})

	if len(activated) != 0 {
		t.Fatalf("container subtraction still activated the pod: %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 {
		t.Fatalf("rejected keys = %v", rejected.keys)
	}
}

func TestWakeupScopeChangesTheVerdict(t *testing.T) {
	// Two NUMA nodes, 2 CPUs each. Two containers of 2 CPUs.
	// Container scope places one container on each node. Pod scope sees a request of 4 and rejects both nodes.
	zones := []topologyv1alpha2.Zone{wakeupZone("0", "2", "4Gi"), wakeupZone("1", "2", "4Gi")}
	pod := guaranteedPod(wakeupProfile, "ns", "spread", 2, 2)

	containerNRT := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodeContainerLevel, zones...)
	w, _, activated := newTestWaker(pod)
	w.withNRTs(containerNRT)
	w.rejected.(*fakeRejectedPods).keys = []string{"ns/spread"}
	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{containerNRT})
	if len(activated) != 1 {
		t.Fatalf("container scope activated = %v", activatedNames(activated))
	}

	podNRT := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodePodLevel, zones...)
	w, rejected, activated := newTestWaker(pod)
	w.withNRTs(podNRT)
	rejected.keys = []string{"ns/spread"}
	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{podNRT})
	if len(activated) != 0 {
		t.Fatalf("pod scope activated = %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 {
		t.Fatalf("pod-scope reject dropped the key: %v", rejected.keys)
	}
}

func TestWakeupActivatesWhenALaterNodeFits(t *testing.T) {
	tight := wakeupNRT("node-tight", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "1", "8Gi"))
	roomy := wakeupNRT("node-roomy", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "4", "8Gi"))
	pod := guaranteedPod(wakeupProfile, "ns", "fits-second", 2)
	w, _, activated := newTestWaker(pod)
	w.withNRTs(tight, roomy)
	w.rejected.(*fakeRejectedPods).keys = []string{"ns/fits-second"}

	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{tight, roomy})

	if len(activated) != 1 {
		t.Fatalf("activated = %v", activatedNames(activated))
	}
}

func TestWakeupDropsForeignBoundAndMissingPods(t *testing.T) {
	nrt := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "4", "8Gi"))
	foreign := guaranteedPod("other-scheduler", "ns", "foreign", 1)
	bound := guaranteedPod(wakeupProfile, "ns", "bound", 1)
	bound.Spec.NodeName = "node-a"
	same := guaranteedPod(wakeupProfile, "ns", "same", 1)

	w, rejected, activated := newTestWaker(foreign, bound, same)
	w.withNRTs(nrt)
	rejected.keys = []string{"ns/foreign", "ns/bound", "ns/missing", "ns/same"}

	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{nrt})

	if len(activated) != 1 || activated["ns/same"] == nil {
		t.Fatalf("activated = %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 || rejected.keys[0] != "ns/same" {
		t.Fatalf("rejected keys = %v, want [ns/same]", rejected.keys)
	}
}

func TestWakeupKeepsKeyOnGetError(t *testing.T) {
	nrt := wakeupNRT("node-a", topologyv1alpha2.SingleNUMANodeContainerLevel, wakeupZone("0", "4", "8Gi"))
	w, rejected, activated := newTestWaker()
	w.pods = mapPodGetter{err: errors.New("informer not synced")}
	rejected.keys = []string{"ns/pending"}

	w.wake(testr.New(t), []*topologyv1alpha2.NodeResourceTopology{nrt})

	if len(activated) != 0 {
		t.Fatalf("activated = %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 {
		t.Fatalf("key dropped after a transient get error: %v", rejected.keys)
	}
}

func TestWakeupEmptyFlushDoesNotActivate(t *testing.T) {
	pod := guaranteedPod(wakeupProfile, "ns", "fits", 1)
	w, rejected, activated := newTestWaker(pod)
	rejected.keys = []string{"ns/fits"}

	w.wake(testr.New(t), nil)

	if len(activated) != 0 {
		t.Fatalf("activated = %v", activatedNames(activated))
	}
	if len(rejected.keys) != 1 {
		t.Fatalf("empty flush changed the rejected set: %v", rejected.keys)
	}
}

type fakeRejectedPods struct {
	keys []string
}

func (f *fakeRejectedPods) RejectedPodKeys() []string {
	out := make([]string, len(f.keys))
	copy(out, f.keys)
	return out
}

func (f *fakeRejectedPods) ForgetRejectedPodKey(key string) {
	kept := make([]string, 0, len(f.keys))
	for _, existing := range f.keys {
		if existing != key {
			kept = append(kept, existing)
		}
	}
	f.keys = kept
}

type mapPodGetter struct {
	pods map[string]*corev1.Pod
	err  error
}

func (g mapPodGetter) Get(namespace, name string) (*corev1.Pod, error) {
	if g.err != nil {
		return nil, g.err
	}
	pod, ok := g.pods[namespace+"/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
	}
	return pod, nil
}

type mapNodeGetter struct {
	nodes map[string]*corev1.Node
}

func (g mapNodeGetter) Get(name string) (*corev1.Node, error) {
	node, ok := g.nodes[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, name)
	}
	return node, nil
}

func newTestWaker(pods ...*corev1.Pod) (*rejectedPodWaker, *fakeRejectedPods, map[string]*corev1.Pod) {
	byKey := make(map[string]*corev1.Pod, len(pods))
	for _, pod := range pods {
		byKey[pod.Namespace+"/"+pod.Name] = pod
	}
	rejected := &fakeRejectedPods{}
	activated := map[string]*corev1.Pod{}
	w := &rejectedPodWaker{
		schedProfileName: wakeupProfile,
		pods:             mapPodGetter{pods: byKey},
		nodes:            mapNodeGetter{nodes: map[string]*corev1.Node{}},
		rejected:         rejected,
		activate: func(_ logr.Logger, got map[string]*corev1.Pod) {
			for key, pod := range got {
				activated[key] = pod
			}
		},
	}
	return w, rejected, activated
}

func (w *rejectedPodWaker) withNRTs(nrts ...*topologyv1alpha2.NodeResourceTopology) {
	nodes := make(map[string]*corev1.Node, len(nrts))
	for _, nrt := range nrts {
		nodes[nrt.Name] = makeNodeFromNRT(nrt)
	}
	w.nodes = mapNodeGetter{nodes: nodes}
}

func guaranteedPod(schedulerName, namespace, name string, cpus ...int64) *corev1.Pod {
	containers := make([]corev1.Container, len(cpus))
	for i, cpu := range cpus {
		qty := corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewQuantity(cpu, resource.DecimalSI),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}
		containers[i] = corev1.Container{
			Name: fmt.Sprintf("c%d", i),
			Resources: corev1.ResourceRequirements{
				Requests: qty,
				Limits:   qty,
			},
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PodSpec{
			SchedulerName: schedulerName,
			Containers:    containers,
		},
	}
}

func wakeupNRT(nodeName string, policy topologyv1alpha2.TopologyManagerPolicy, zones ...topologyv1alpha2.Zone) *topologyv1alpha2.NodeResourceTopology {
	return &topologyv1alpha2.NodeResourceTopology{
		ObjectMeta:       metav1.ObjectMeta{Name: nodeName},
		TopologyPolicies: []string{string(policy)},
		Zones:            zones,
	}
}

func wakeupZone(id, cpu, memory string) topologyv1alpha2.Zone {
	return topologyv1alpha2.Zone{
		Name: "node-" + id,
		Type: "Node",
		Resources: topologyv1alpha2.ResourceInfoList{
			makeTopologyResInfoWithAllocatable(string(corev1.ResourceCPU), cpu, cpu),
			makeTopologyResInfoWithAllocatable(string(corev1.ResourceMemory), memory, memory),
		},
	}
}

func activatedNames(activated map[string]*corev1.Pod) []string {
	names := make([]string, 0, len(activated))
	for key := range activated {
		names = append(names, key)
	}
	return names
}
