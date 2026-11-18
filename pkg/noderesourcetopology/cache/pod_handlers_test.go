/*
Copyright 2022 The Kubernetes Authors.

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
	"testing"

	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	k8scache "k8s.io/client-go/tools/cache"
)

func TestIsForeignPod(t *testing.T) {
	tests := []struct {
		name          string
		profileNames  []string
		pod           *corev1.Pod
		nrtResources  sets.Set[corev1.ResourceName]
		exclusiveOnly bool
		expected      bool
	}{
		{
			name: "empty",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
			},
		},
		{
			name:         "no-node-no-profile",
			profileNames: []string{"secondary-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
			},
		},
		{
			name:         "node-no-profile-no-devs",
			profileNames: []string{"secondary-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName: "random-node",
					Containers: []corev1.Container{
						{
							Name: "cnt",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:         "node-no-profile-no-devs",
			profileNames: []string{"secondary-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName: "random-node",
					InitContainers: []corev1.Container{
						{
							Name: "cnt",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:         "node-no-profile-devs-only",
			profileNames: []string{"secondary-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName: "random-node",
					Containers: []corev1.Container{
						{
							Name: "cnt",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceName("veryfast.io/fpga"): resource.MustParse("1"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceName("veryfast.io/fpga"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:         "node-no-profile-devs-only",
			profileNames: []string{"secondary-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName: "random-node",
					InitContainers: []corev1.Container{
						{
							Name: "cnt",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceName("veryfast.io/fpga"): resource.MustParse("1"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceName("veryfast.io/fpga"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:         "node-profile",
			profileNames: []string{"secondary-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName:      "random-node",
					SchedulerName: "secondary-scheduler",
				},
			},
		},
		{
			name:         "node-multi-profile",
			profileNames: []string{"secondary-scheduler-A", "secondary-scheduler-B", "fancy-scheduler"},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName:      "random-node",
					SchedulerName: "secondary-scheduler-B",
				},
			},
		},
		{
			name:          "exclusive-only-dev-in-nrt",
			profileNames:  []string{"secondary-scheduler"},
			exclusiveOnly: true,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName: "random-node",
					Containers: []corev1.Container{
						{
							Name: "cnt",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceName("veryfast.io/fpga"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			nrtResources: sets.New(corev1.ResourceName("veryfast.io/fpga")),
			expected:     true,
		},
		{
			name:          "exclusive-only-dev-not-in-nrt",
			profileNames:  []string{"secondary-scheduler"},
			exclusiveOnly: true,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					NodeName: "random-node",
					Containers: []corev1.Container{
						{
							Name: "cnt",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceName("veryfast.io/fpga"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, profileName := range tt.profileNames {
				RegisterSchedulerProfileName(testr.New(t), profileName)
			}
			defer CleanRegisteredSchedulerProfileNames()
			defer TrackAllForeignPods()

			if tt.exclusiveOnly {
				TrackOnlyForeignPodsWithExclusiveResources()
			}

			got := IsForeignPod(tt.pod, tt.nrtResources)
			if got != tt.expected {
				t.Errorf("%s: pod %q foreign status got %v expected %v", tt.name, tt.pod.Name, got, tt.expected)
			}
		})
	}
}

func TestHandlePodDeleteForgetsRejectedPod(t *testing.T) {
	const profile = "sched"
	pending := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pending"}, Spec: corev1.PodSpec{SchedulerName: profile}}
	bound := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "bound"}, Spec: corev1.PodSpec{SchedulerName: profile, NodeName: "node-1"}}

	t.Run("pending pod", func(t *testing.T) {
		ov := &OverReserve{lh: testr.New(t)}
		ov.EnableRequeueUnschedulablePods()
		ov.NoteRejectedPod(pending)
		handlePodDelete(testr.New(t), profile, ov, pending, PodCacheHandlersOpts{ForgetRejectedPods: true})
		if ov.HasRejectedPod(pending) {
			t.Fatal("deleted pending pod is still recorded")
		}
	})

	t.Run("bound pod", func(t *testing.T) {
		ov := &OverReserve{lh: testr.New(t)}
		ov.EnableRequeueUnschedulablePods()
		ov.NoteRejectedPod(bound)
		handlePodDelete(testr.New(t), profile, ov, bound, PodCacheHandlersOpts{ForgetRejectedPods: true})
		if ov.HasRejectedPod(bound) {
			t.Fatal("deleted bound pod is still recorded")
		}
	})

	t.Run("forget disabled leaves the key", func(t *testing.T) {
		ov := &OverReserve{lh: testr.New(t)}
		ov.EnableRequeueUnschedulablePods()
		ov.NoteRejectedPod(pending)
		handlePodDelete(testr.New(t), profile, ov, pending, PodCacheHandlersOpts{})
		if !ov.HasRejectedPod(pending) {
			t.Fatal("pod was forgotten while ForgetRejectedPods is off")
		}
	})

	t.Run("foreign delete leaves the key", func(t *testing.T) {
		ov := &OverReserve{lh: testr.New(t)}
		ov.EnableRequeueUnschedulablePods()
		ov.NoteRejectedPod(pending)
		foreign := pending.DeepCopy()
		foreign.Spec.SchedulerName = "other-scheduler"
		handlePodDelete(testr.New(t), profile, ov, foreign, PodCacheHandlersOpts{ForgetRejectedPods: true})
		if !ov.HasRejectedPod(pending) {
			t.Fatal("foreign delete removed the recorded pod")
		}
	})
}

func TestPodFromDeleteEvent(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pending"}}
	got, ok := podFromDeleteEvent(pod)
	if !ok || got != pod {
		t.Fatalf("direct pod: ok=%v pod=%v", ok, got)
	}
	got, ok = podFromDeleteEvent(k8scache.DeletedFinalStateUnknown{Obj: pod})
	if !ok || got != pod {
		t.Fatalf("tombstone: ok=%v pod=%v", ok, got)
	}
	if _, ok := podFromDeleteEvent("nope"); ok {
		t.Fatal("non-pod object was accepted")
	}
}
