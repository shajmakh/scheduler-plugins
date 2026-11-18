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

package noderesourcetopology

import (
	"context"
	"testing"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	apiconfig "sigs.k8s.io/scheduler-plugins/apis/config"
)

func TestFilterRecordsRejectedPodOnce(t *testing.T) {
	const nodeName = "node-preempt"
	nrt := makePreemptionNRT(nodeName)
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(makeNodeFromNRT(nrt))
	pod := makeGuaranteedPod("default", "pending", containerName, 4, "1Gi")

	cache := &fakeFilterCache{nrt: nrt}
	tm := TopologyMatch{nrtCache: cache, preemptionMode: apiconfig.PreemptionDisabled}

	first := tm.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo)
	second := tm.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo)
	if first == nil || !first.IsRejected() || second == nil || !second.IsRejected() {
		t.Fatalf("expected both attempts rejected, got %v and %v", first, second)
	}
	if len(cache.rejectedPods) != 1 || cache.rejectedPods[0] != "default/pending" {
		t.Fatalf("rejected pods = %v, want [default/pending]", cache.rejectedPods)
	}
}

func TestFilterSkipsRecordOnSuccessAndBestEffort(t *testing.T) {
	const nodeName = "node-preempt"
	nrt := makePreemptionNRT(nodeName)
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(makeNodeFromNRT(nrt))

	t.Run("missing nrt", func(t *testing.T) {
		cache := &fakeFilterCache{}
		tm := TopologyMatch{nrtCache: cache, preemptionMode: apiconfig.PreemptionDisabled}
		pod := makeGuaranteedPod("default", "pending", containerName, 1, "1Gi")
		if status := tm.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo); status != nil {
			t.Fatalf("expected success, got %v", status)
		}
		if len(cache.rejectedPods) != 0 {
			t.Fatalf("rejected pods = %v", cache.rejectedPods)
		}
	})

	t.Run("best effort", func(t *testing.T) {
		cache := &fakeFilterCache{nrt: nrt}
		tm := TopologyMatch{nrtCache: cache, preemptionMode: apiconfig.PreemptionDisabled}
		pod := makeBestEffortPod("default", "be", containerName)
		if status := tm.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo); status != nil {
			t.Fatalf("expected success, got %v", status)
		}
		if len(cache.rejectedPods) != 0 {
			t.Fatalf("rejected pods = %v", cache.rejectedPods)
		}
	})
}

func TestFilterRecordsStaleTerminatedAndEvictionFailure(t *testing.T) {
	const nodeName = "node-preempt"
	nrt := makePreemptionNRT(nodeName)
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(makeNodeFromNRT(nrt))
	pod := makeGuaranteedPod("default", "pending", containerName, 4, "1Gi")

	t.Run("stale node", func(t *testing.T) {
		cache := &fakeFilterCache{stale: true}
		tm := TopologyMatch{nrtCache: cache, preemptionMode: apiconfig.PreemptionDisabled}
		status := tm.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo)
		if status == nil || status.Code() != fwk.UnschedulableAndUnresolvable {
			t.Fatalf("expected unschedulable and unresolvable, got %v", status)
		}
		if len(cache.rejectedPods) != 1 {
			t.Fatalf("rejected pods = %v", cache.rejectedPods)
		}
	})

	t.Run("terminated pods", func(t *testing.T) {
		cache := &fakeFilterCache{nrt: nrt, withTerminatedPods: true}
		tm := TopologyMatch{nrtCache: cache, preemptionMode: apiconfig.PreemptionDisabled}
		status := tm.Filter(context.Background(), framework.NewCycleState(), pod, nodeInfo)
		if status == nil || status.Code() != fwk.UnschedulableAndUnresolvable {
			t.Fatalf("expected unschedulable and unresolvable, got %v", status)
		}
		if len(cache.rejectedPods) != 1 {
			t.Fatalf("rejected pods = %v", cache.rejectedPods)
		}
	})

	t.Run("eviction simulation failure", func(t *testing.T) {
		victim := makeGuaranteedPod("default", "victim", containerName, 1, "1Gi")
		cache := &fakeFilterCache{nrt: nrt, numaPlacement: makeEncodedInfoForPod(victim, 0)}
		tm := TopologyMatch{nrtCache: cache, preemptionMode: apiconfig.PreemptionEnabled}
		// Placement info exists, but the victim is not the one encoded for that pod, so the simulation fails.
		other := makeGuaranteedPod("default", "other-victim", "other", 1, "1Gi")
		status := tm.Filter(context.Background(), cycleStateWithVictims(t, other), pod, nodeInfo)
		if status == nil || status.Code() != fwk.UnschedulableAndUnresolvable {
			t.Fatalf("expected unschedulable and unresolvable, got %v", status)
		}
		if len(cache.rejectedPods) != 1 || cache.rejectedPods[0] != "default/pending" {
			t.Fatalf("rejected pods = %v", cache.rejectedPods)
		}
	})
}

func TestReserveForgetsRejectedPod(t *testing.T) {
	cache := &fakeFilterCache{}
	tm := TopologyMatch{nrtCache: cache}
	pod := makeGuaranteedPod("default", "pending", containerName, 1, "1Gi")
	cache.NoteRejectedPod(pod)

	if status := tm.Reserve(context.Background(), framework.NewCycleState(), pod, "node-preempt"); !status.IsSuccess() {
		t.Fatalf("Reserve status: %v", status)
	}
	if len(cache.rejectedPods) != 0 {
		t.Fatalf("rejected pods after Reserve = %v", cache.rejectedPods)
	}
}
