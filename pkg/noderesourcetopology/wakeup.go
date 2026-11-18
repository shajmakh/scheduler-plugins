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
	"github.com/go-logr/logr"
	topologyv1alpha2 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	corelisters "k8s.io/client-go/listers/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	nrtcache "sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/cache"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/logging"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/nodeconfig"
)

// rejectedPodKeys is the rejected-pod set the flush wakeup reads.
// ForgetRejectedPodKey drops a key whose live object can no longer be woken.
type rejectedPodKeys interface {
	RejectedPodKeys() []string
	ForgetRejectedPodKey(key string)
}

// podByName loads a live pod. Pending pods are included; the filtered node lister is not.
type podByName interface {
	Get(namespace, name string) (*corev1.Pod, error)
}

type nodeByName interface {
	Get(name string) (*corev1.Node, error)
}

type corePodLister struct {
	lister corelisters.PodLister
}

func (p corePodLister) Get(namespace, name string) (*corev1.Pod, error) {
	return p.lister.Pods(namespace).Get(name)
}

type coreNodeLister struct {
	lister corelisters.NodeLister
}

func (n coreNodeLister) Get(name string) (*corev1.Node, error) {
	return n.lister.Get(name)
}

// rejectedPodWaker moves same-profile rejected pods that fit a flushed node back to the active queue.
type rejectedPodWaker struct {
	schedProfileName string
	pods             podByName
	nodes            nodeByName
	rejected         rejectedPodKeys
	activate         func(logr.Logger, map[string]*corev1.Pod)
}

func newRejectedPodWaker(handle fwk.Handle, profileName string, rejected rejectedPodKeys) *rejectedPodWaker {
	factory := handle.SharedInformerFactory()
	return &rejectedPodWaker{
		schedProfileName: profileName,
		pods:             corePodLister{lister: factory.Core().V1().Pods().Lister()},
		nodes:            coreNodeLister{lister: factory.Core().V1().Nodes().Lister()},
		rejected:         rejected,
		activate:         handle.Activate,
	}
}

// wake runs after a successful flush when requeueUnschedulablePods is OnlyExclusiveResources.
// nrts are the NodeResourceTopology objects that flush installed. The cache lock is not held.
func (w *rejectedPodWaker) wake(lh logr.Logger, nrts []*topologyv1alpha2.NodeResourceTopology) {
	if len(nrts) == 0 || w == nil || w.rejected == nil {
		return
	}
	keys := w.rejected.RejectedPodKeys()
	if len(keys) == 0 {
		return
	}

	nodes := w.prepareFlushedNodes(lh, nrts)
	var toActivate map[string]*corev1.Pod
	for _, key := range keys {
		pod, keep := w.podToConsider(lh, key)
		if pod == nil {
			if !keep {
				w.rejected.ForgetRejectedPodKey(key)
			}
			continue
		}
		if !podFitsAnyFlushedNode(lh, pod, nodes) {
			continue
		}
		if toActivate == nil {
			toActivate = make(map[string]*corev1.Pod, 1)
		}
		toActivate[key] = pod
	}
	if len(toActivate) == 0 {
		return
	}
	lh.V(3).Info("activating rejected pods that fit a flushed node", "count", len(toActivate))
	w.activate(lh, toActivate)
}

// podToConsider returns the live pod to test, or (nil, keep).
// keep is true when a transient Get error should leave the key for the next flush.
func (w *rejectedPodWaker) podToConsider(lh logr.Logger, key string) (*corev1.Pod, bool) {
	namespace, name := nrtcache.NamespacedPodFromKey(key)
	if namespace == "" || name == "" {
		return nil, false
	}
	pod, err := w.pods.Get(namespace, name)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		lh.V(6).Info("cannot get rejected pod", "pod", key, "error", err)
		return nil, true
	}
	if pod.Spec.SchedulerName != w.schedProfileName {
		lh.V(6).Info("rejected pod not for this scheduler profile", "pod", key, "schedulerName", pod.Spec.SchedulerName)
		return nil, false
	}
	if pod.Spec.NodeName != "" {
		lh.V(6).Info("rejected pod already bound, skipping requeue", "pod", key, "nodeName", pod.Spec.NodeName)
		return nil, false
	}
	return pod, true
}

type flushedNode struct {
	name       string
	conf       nodeconfig.TopologyManager
	numaNodes  NUMANodeList
	nodeInfo   fwk.NodeInfo
	handler    filterFn
	perPodNUMA bool
}

func (w *rejectedPodWaker) prepareFlushedNodes(lh logr.Logger, nrts []*topologyv1alpha2.NodeResourceTopology) []flushedNode {
	nodes := make([]flushedNode, 0, len(nrts))
	for _, nrt := range nrts {
		if nrt == nil {
			continue
		}
		node, err := w.nodes.Get(nrt.Name)
		if err != nil {
			lh.Error(err, "cannot get node for flushed NRT", logging.KeyNode, nrt.Name)
			continue
		}
		info := framework.NewNodeInfo()
		info.SetNode(node)
		conf := nodeconfig.TopologyManagerFromNodeResourceTopology(lh, nrt)
		handler, scope := filterHandlerFromTopologyManager(conf)
		nodes = append(nodes, flushedNode{
			name:       nrt.Name,
			conf:       conf,
			numaNodes:  createNUMANodeList(lh, nrt.Zones),
			nodeInfo:   info,
			handler:    handler,
			perPodNUMA: scope == "container",
		})
	}
	return nodes
}

// podFitsAnyFlushedNode reports whether the single-NUMA handler accepts pod on any flushed node.
// A node whose topology manager policy is not single-numa-node accepts the pod: Filter would too.
// Container scope copies the NUMA list per pod because the handler subtracts from it.
func podFitsAnyFlushedNode(lh logr.Logger, pod *corev1.Pod, nodes []flushedNode) bool {
	for _, node := range nodes {
		if node.fits(lh, pod) {
			return true
		}
	}
	return false
}

func (n flushedNode) fits(lh logr.Logger, pod *corev1.Pod) bool {
	if n.handler == nil {
		return true
	}
	numaNodes := n.numaNodes
	if n.perPodNUMA {
		numaNodes = n.numaNodes.DeepCopy()
	}
	status := n.handler(lh, pod, &filterInfo{
		nodeName:        n.name,
		node:            n.nodeInfo,
		topologyManager: n.conf,
		numaNodes:       numaNodes,
		qos:             v1qos.GetPodQOS(pod),
	})
	return status == nil
}
