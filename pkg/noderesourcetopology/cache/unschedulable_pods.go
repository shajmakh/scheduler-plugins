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
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"

	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/logging"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/resourcerequests"
)

// PodActivator moves pods back onto the scheduler active queue.
// The scheduler ignores pods that are not currently in the unschedulable or backoff queues.
type PodActivator func(lh logr.Logger, pods []*corev1.Pod)

// SetUnschedulablePodActivator registers the profile and the callback used after a !Fresh node is flushed.
// profileName must be the scheduler name of this profile (pod.spec.schedulerName).
func (ov *OverReserve) SetUnschedulablePodActivator(profileName string, act PodActivator) {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	ov.profileName = profileName
	ov.podActivator = act
}

// activateUnschedulableExclusivePods lists pods and wakes those that are:
//   - unschedulable after a failed scheduling attempt (PodScheduled=False, reason=Unschedulable)
//   - owned by this scheduler profile
//   - requesting exclusive resources
//
// The pod lister is the scheduler's view of the cluster. Pods already in the active queue,
// or unschedulable for a reason other than a rejected scheduling attempt, are left alone.
func (ov *OverReserve) activateUnschedulableExclusivePods(lh logr.Logger) {
	ov.lock.Lock()
	act := ov.podActivator
	profileName := ov.profileName
	ov.lock.Unlock()
	if act == nil || profileName == "" {
		return
	}

	pods, err := ov.podLister.List(labels.Everything())
	if err != nil {
		lh.Error(err, "cannot list pods to activate after cache flush")
		return
	}

	matched := selectPodsToActivate(pods, profileName, ov.nrtResNames.Union())
	if len(matched) == 0 {
		lh.V(4).Info("no unschedulable exclusive pods to activate after cache flush")
		return
	}

	lh.V(2).Info("activating unschedulable exclusive pods after cache flush", "count", len(matched))
	for _, pod := range matched {
		lh.V(4).Info("activating unschedulable pod after cache flush", logging.KeyPod, klog.KObj(pod), logging.KeyPodUID, logging.PodUID(pod))
	}
	act(lh, matched)
}

func selectPodsToActivate(pods []*corev1.Pod, profileName string, nrtResources sets.Set[corev1.ResourceName]) []*corev1.Pod {
	if profileName == "" {
		return nil
	}
	matched := make([]*corev1.Pod, 0)
	for _, pod := range pods {
		if pod == nil || pod.Spec.SchedulerName != profileName {
			continue
		}
		if !isUnschedulablePod(pod) {
			continue
		}
		if !resourcerequests.AreExclusiveForPod(pod, nrtResources) {
			continue
		}
		matched = append(matched, pod)
	}
	return matched
}

// isUnschedulablePod reports whether the scheduler has rejected this pod and left it unassigned.
// SchedulerError retries are not included: those are not parked for the unschedulable timeout.
func isUnschedulablePod(pod *corev1.Pod) bool {
	if pod.Spec.NodeName != "" {
		return false
	}
	_, cond := podutil.GetPodCondition(&pod.Status, corev1.PodScheduled)
	if cond == nil {
		return false
	}
	return cond.Status == corev1.ConditionFalse && cond.Reason == corev1.PodReasonUnschedulable
}
