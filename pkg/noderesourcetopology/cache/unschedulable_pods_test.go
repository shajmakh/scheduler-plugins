/*
Copyright The Kubernetes Authors.

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

package cache

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	tu "sigs.k8s.io/scheduler-plugins/test/util"
)

const testProfileName = "topo-aware"

func TestSelectPodsToActivate(t *testing.T) {
	nrtResources := sets.New[corev1.ResourceName](corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceName(nicResourceName))

	exclusiveCPU := resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "1Gi")
	burstableCPU := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
	device := resourceList(corev1.ResourceName(nicResourceName), "1")

	burstable := unschedulablePod("burstable", testProfileName, corev1.PodReasonUnschedulable, "", burstableCPU)
	burstable.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")}

	testCases := []struct {
		name     string
		profile  string
		pods     []*corev1.Pod
		expected []string
	}{
		{
			name:    "unschedulable exclusive pod of this profile",
			profile: testProfileName,
			pods: []*corev1.Pod{
				unschedulablePod("wake", testProfileName, corev1.PodReasonUnschedulable, "", exclusiveCPU),
			},
			expected: []string{"ns/wake"},
		},
		{
			name:    "skip other profile, non-exclusive, scheduler error, bound, and not-yet-attempted",
			profile: testProfileName,
			pods: []*corev1.Pod{
				unschedulablePod("other-profile", "default", corev1.PodReasonUnschedulable, "", exclusiveCPU),
				burstable,
				unschedulablePod("sched-error", testProfileName, corev1.PodReasonSchedulerError, "", exclusiveCPU),
				unschedulablePod("already-bound", testProfileName, corev1.PodReasonUnschedulable, "node1", exclusiveCPU),
				unschedulablePod("device", testProfileName, corev1.PodReasonUnschedulable, "", device),
				{
					ObjectMeta: metav1.ObjectMeta{Name: "no-condition", Namespace: "ns"},
					Spec: corev1.PodSpec{
						SchedulerName: testProfileName,
						Containers: []corev1.Container{{
							Resources: corev1.ResourceRequirements{Requests: exclusiveCPU, Limits: exclusiveCPU},
						}},
					},
				},
			},
			expected: []string{"ns/device"},
		},
		{
			name:     "empty profile selects nothing",
			profile:  "",
			pods:     []*corev1.Pod{unschedulablePod("wake", testProfileName, corev1.PodReasonUnschedulable, "", exclusiveCPU)},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := podKeys(selectPodsToActivate(tc.pods, tc.profile, nrtResources))
			if diff := cmp.Diff(tc.expected, got); diff != "" {
				t.Fatalf("pods to activate mismatch (-want +got): %s", diff)
			}
		})
	}
}

func TestFlushBlockingNodeActivatesUnschedulableExclusivePods(t *testing.T) {
	fakeClient, err := tu.NewFakeClient()
	if err != nil {
		t.Fatal(err)
	}

	exclusiveCPU := resourceList(corev1.ResourceCPU, "2", corev1.ResourceMemory, "1Gi")
	burstableCPU := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
	device := resourceList(corev1.ResourceName(nicResourceName), "1")

	burstablePod := unschedulablePod("burstable", testProfileName, corev1.PodReasonUnschedulable, "", burstableCPU)
	burstablePod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")}

	lister := &fakePodLister{}
	lister.AddPod(unschedulablePod("wake-cpu", testProfileName, corev1.PodReasonUnschedulable, "", exclusiveCPU))
	lister.AddPod(unschedulablePod("wake-device", testProfileName, corev1.PodReasonUnschedulable, "", device))
	lister.AddPod(unschedulablePod("other-profile", "default", corev1.PodReasonUnschedulable, "", exclusiveCPU))
	lister.AddPod(burstablePod)

	nrtCache := mustOverReserve(t, fakeClient, lister)
	nrt := makeTestNRT("node1")
	nrtCache.TestOnlyUpdateNRT(nrt)

	var activated [][]string
	nrtCache.SetUnschedulablePodActivator(testProfileName, func(_ logr.Logger, pods []*corev1.Pod) {
		activated = append(activated, podKeys(pods))
	})

	lh := testr.New(t)
	if gen := nrtCache.FlushNodes(lh, nrtUpdate{nrt: nrt.DeepCopy()}); gen == 0 {
		t.Fatal("expected generation to increase")
	}
	if len(activated) != 0 {
		t.Fatalf("flush of a fresh node woke pods: %v", activated)
	}

	// Pessimistic overallocation keeps the node Fresh. Only a !Fresh flush wakes pods.
	nrtCache.NodeMaybeOverReserved("node1", &corev1.Pod{})
	if gen := nrtCache.FlushNodes(lh, nrtUpdate{nrt: nrt.DeepCopy()}); gen == 0 {
		t.Fatal("expected generation to increase")
	}
	if _, info := nrtCache.GetCachedNRTCopy(context.Background(), "node1", &corev1.Pod{}); !info.Fresh {
		t.Fatal("over-reserved node should stay fresh")
	}
	if len(activated) != 0 {
		t.Fatalf("flush of an over-reserved fresh node woke pods: %v", activated)
	}

	nrtCache.NodeHasForeignPods("node1", &corev1.Pod{})
	if _, info := nrtCache.GetCachedNRTCopy(context.Background(), "node1", &corev1.Pod{}); info.Fresh {
		t.Fatal("expected node to be stale after foreign pods")
	}

	genBeforeEmpty := nrtCache.generation
	if gen := nrtCache.FlushNodes(lh); gen != genBeforeEmpty {
		t.Fatalf("empty flush changed generation: got %d expected %d", gen, genBeforeEmpty)
	}
	if len(activated) != 0 {
		t.Fatalf("empty flush woke pods while the node was still stale: %v", activated)
	}
	if _, info := nrtCache.GetCachedNRTCopy(context.Background(), "node1", &corev1.Pod{}); info.Fresh {
		t.Fatal("empty flush cleared the stale node")
	}

	if gen := nrtCache.FlushNodes(lh, nrtUpdate{nrt: nrt.DeepCopy()}); gen == genBeforeEmpty {
		t.Fatal("expected generation to increase after flushing the stale node")
	}
	if _, info := nrtCache.GetCachedNRTCopy(context.Background(), "node1", &corev1.Pod{}); !info.Fresh {
		t.Fatal("expected node to be fresh after flush")
	}
	if len(activated) != 1 {
		t.Fatalf("expected one activation, got %d: %v", len(activated), activated)
	}
	expected := []string{"ns/wake-cpu", "ns/wake-device"}
	if diff := cmp.Diff(expected, activated[0]); diff != "" {
		t.Fatalf("activated pods mismatch (-want +got): %s", diff)
	}
}

func TestFlushBlockingNodeSkipsActivationWhenPodListFails(t *testing.T) {
	fakeClient, err := tu.NewFakeClient()
	if err != nil {
		t.Fatal(err)
	}

	lister := &fakePodLister{err: errors.New("lister failed")}
	nrtCache := mustOverReserve(t, fakeClient, lister)
	nrt := makeTestNRT("node1")
	nrtCache.TestOnlyUpdateNRT(nrt)
	nrtCache.NodeHasForeignPods("node1", &corev1.Pod{})

	called := false
	nrtCache.SetUnschedulablePodActivator(testProfileName, func(_ logr.Logger, pods []*corev1.Pod) {
		called = true
	})

	nrtCache.FlushNodes(testr.New(t), nrtUpdate{nrt: nrt.DeepCopy()})
	if called {
		t.Fatal("activator ran after the pod list failed")
	}
}

func unschedulablePod(name, scheduler, reason, nodeName string, requests corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
		},
		Spec: corev1.PodSpec{
			SchedulerName: scheduler,
			NodeName:      nodeName,
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: requests,
					Limits:   requests,
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: reason,
			}},
		},
	}
}

func resourceList(kv ...any) corev1.ResourceList {
	out := corev1.ResourceList{}
	for i := 0; i < len(kv); i += 2 {
		out[kv[i].(corev1.ResourceName)] = resource.MustParse(kv[i+1].(string))
	}
	return out
}

func podKeys(pods []*corev1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	keys := make([]string, 0, len(pods))
	for _, pod := range pods {
		keys = append(keys, pod.Namespace+"/"+pod.Name)
	}
	sort.Strings(keys)
	return keys
}
