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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	topologyv1alpha2 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2"
	"github.com/k8stopologyawareschedwg/numaplacement"
	"github.com/k8stopologyawareschedwg/podfingerprint"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiconfig "sigs.k8s.io/scheduler-plugins/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/logging"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/podprovider"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/resourcerequests"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/stringify"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

const (
	// defaultMaxNRTUpdates is the watcher update channel size initial heuristic, no hard data.
	// we used a bounded channel to prevent excessive event accumulation. The watcher code
	// is now in charge to detect and handle overflow using retries.
	defaultMaxNRTUpdates = 128
)

type OverReserve struct {
	lh               logr.Logger
	client           ctrlclient.Reader
	lock             sync.Mutex
	generation       uint64
	nrts             *nrtStore
	nrtResNames      *nrtResourcesStore
	assumedResources map[string]*resourceStore // nodeName -> resourceStore
	// nodesMaybeOverreserved counts how many times a node is filtered out. This is used as trigger condition to try
	// to resync nodes. See The documentation of Resync() below for more details.
	nodesMaybeOverreserved counter
	nodesWithForeignPods   counter
	nodesWithAttrUpdate    counter
	podLister              podprovider.Lister
	resyncMethod           apiconfig.CacheResyncMethod
	resyncScope            apiconfig.CacheResyncScope
	preemptionMode         apiconfig.PreemptionMode
	nrtUpdateCh            chan NRTEvent
	watchCancel            context.CancelFunc
	nrtWatcher             *Watcher
	closeOnce              sync.Once
}

func NewOverReserve(
	ctx context.Context,
	lh logr.Logger,
	cfg *apiconfig.NodeResourceTopologyCache,
	client ctrlclient.WithWatch,
	podLister podprovider.Lister,
	preemptionMode apiconfig.PreemptionMode,
) (*OverReserve, error) {
	if client == nil || podLister == nil {
		return nil, fmt.Errorf("received nil references")
	}

	nrtObjs := &topologyv1alpha2.NodeResourceTopologyList{}
	if err := client.List(ctx, nrtObjs); err != nil {
		return nil, err
	}

	resyncMethod := getCacheResyncMethod(lh, cfg)
	resyncScope := getCacheResyncScope(lh, cfg)

	lh.V(2).Info("initializing", "noderesourcetopologies", len(nrtObjs.Items), "method", resyncMethod, "scope", resyncScope)
	watcherCtx, watchCancel := context.WithCancel(ctx)
	obj := &OverReserve{
		lh:                     lh,
		client:                 client,
		nrts:                   newNrtStore(lh, nrtObjs.Items),
		nrtResNames:            newNrtResourcesStore(nrtObjs.Items),
		assumedResources:       make(map[string]*resourceStore),
		nodesMaybeOverreserved: newCounter(),
		nodesWithForeignPods:   newCounter(),
		nodesWithAttrUpdate:    newCounter(),
		nrtUpdateCh:            make(chan NRTEvent, defaultMaxNRTUpdates),
		podLister:              podLister,
		resyncMethod:           resyncMethod,
		preemptionMode:         preemptionMode,
		watchCancel:            watchCancel,
	}

	if resyncScope == apiconfig.CacheResyncScopeAll {
		obj.nrtWatcher = NewWatcher(obj.lh, obj.nrtUpdateCh, nrtObjs.Items)
		go obj.nrtWatcher.NodeResourceTopologies(watcherCtx, client)
	} else {
		obj.nrtWatcher = &Watcher{}
	}

	return obj, nil
}

func (ov *OverReserve) GetCachedNRTCopy(ctx context.Context, nodeName string, pod *corev1.Pod) (*topologyv1alpha2.NodeResourceTopology, CachedNRTInfo) {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	info := CachedNRTInfo{Generation: ov.generation}
	if ov.nodesWithForeignPods.IsSet(nodeName) {
		return nil, info
	}

	info.Fresh = true
	nrt := ov.nrts.GetNRTCopyByNodeName(nodeName)
	if nrt == nil {
		return nil, info
	}
	nodeAssumedResources, ok := ov.assumedResources[nodeName]
	if !ok {
		return nrt, info
	}

	logID := klog.KObj(pod)
	lh := ov.lh.WithValues(logging.KeyPod, logID, logging.KeyPodUID, logging.PodUID(pod), logging.KeyNode, nodeName, logging.KeyGeneration, ov.generation)

	lh.V(6).Info("NRT", "fromcache", stringify.NodeResourceTopologyResources(nrt))
	nodeAssumedResources.UpdateNRT(nrt, logging.KeyPod, logID)

	lh.V(5).Info("NRT", "withassumed", stringify.NodeResourceTopologyResources(nrt))
	return nrt, info
}

func (ov *OverReserve) GetCachedNUMAPlacementInfo(nodeName string) *numaplacement.EncodedInfo {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	return ov.nrts.GetNUMAPlacementInfoByNodeName(nodeName)
}

func (ov *OverReserve) NodeMaybeOverReserved(nodeName string, pod *corev1.Pod) {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	val := ov.nodesMaybeOverreserved.Incr(nodeName)
	ov.lh.V(4).Info("mark discarded", logging.KeyNode, nodeName, "count", val)
}

func (ov *OverReserve) NodeHasForeignPods(nodeName string, pod *corev1.Pod) {
	lh := ov.lh.WithValues(logging.KeyPod, klog.KObj(pod), logging.KeyPodUID, logging.PodUID(pod), logging.KeyNode, nodeName)
	ov.lock.Lock()
	defer ov.lock.Unlock()
	if !ov.nrts.Contains(nodeName) {
		lh.V(5).Info("ignoring foreign pods", "nrtinfo", "missing")
		return
	}
	val := ov.nodesWithForeignPods.Incr(nodeName)
	lh.V(2).Info("marked with foreign pods", logging.KeyNode, nodeName, "count", val)
}

func (ov *OverReserve) ReserveNodeResources(nodeName string, pod *corev1.Pod) {
	lh := ov.lh.WithValues(logging.KeyPod, klog.KObj(pod), logging.KeyPodUID, logging.PodUID(pod), logging.KeyNode, nodeName)
	ov.lock.Lock()
	defer ov.lock.Unlock()
	if !ov.nrts.Contains(nodeName) {
		lh.V(5).Info("ignoring reserve", "nrtinfo", "missing")
		return
	}
	nodeAssumedResources, ok := ov.assumedResources[nodeName]
	if !ok {
		nodeAssumedResources = newResourceStore(ov.lh)
		ov.assumedResources[nodeName] = nodeAssumedResources
	}

	nodeAssumedResources.AddPod(pod)
	lh.V(2).Info("post reserve", logging.KeyNode, nodeName, "assumedResources", nodeAssumedResources.String())
}

func (ov *OverReserve) UnreserveNodeResources(nodeName string, pod *corev1.Pod) {
	lh := ov.lh.WithValues(logging.KeyPod, klog.KObj(pod), logging.KeyPodUID, logging.PodUID(pod), logging.KeyNode, nodeName)
	ov.lock.Lock()
	defer ov.lock.Unlock()
	nodeAssumedResources, ok := ov.assumedResources[nodeName]
	if !ok {
		// this should not happen, so we're vocal about it
		// we don't return error because not much to do to recover anyway
		lh.V(2).Info("no resources tracked", logging.KeyNode, nodeName)
		return
	}

	nodeAssumedResources.DeletePod(pod)
	lh.V(2).Info("post unreserve", logging.KeyNode, nodeName, "assumedResources", nodeAssumedResources.String())
}

func (ov *OverReserve) PostBind(nodeName string, pod *corev1.Pod) {}

// Close is safe to call multiple times and concurrently: sync.Once
// guarantees the shutdown sequence runs exactly once and it is then idempotent
func (ov *OverReserve) Close() {
	ov.closeOnce.Do(ov.closeCache)
}

// closeCache is not just close to avoid clashes with builtins
func (ov *OverReserve) closeCache() {
	ov.watchCancel()
	ov.nrtWatcher.Wait()
}

type DesyncedNodes struct {
	Generation        uint64
	MaybeOverReserved []string
	ConfigChanged     []string
}

func (rn DesyncedNodes) String() string {
	return fmt.Sprintf("desyncedNodes{MaybeOverReserved: %v, ConfigChanged: %v}", rn.MaybeOverReserved, rn.ConfigChanged)
}

func (rn DesyncedNodes) Len() int {
	return len(rn.MaybeOverReserved) + len(rn.ConfigChanged)
}

func (rn DesyncedNodes) DirtyCount() int {
	return len(rn.MaybeOverReserved)
}

// GetDesyncNodes returns info with all the node names which have been discarded previously,
// so which are supposed to be `dirty` in the cache.
// A node can be discarded for two reasons:
//  1. it legitimately cannot fit containers because it has not enough free resources
//  2. it was pessimistically overallocated, so the node is a candidate for resync
//
// or
//  3. it received a metadata update while at steady state (resource allocation didn't change),
//     typically at idle time (unloaded node)
//
// This function enables the caller to know the slice of nodes should be considered for resync,
// avoiding the need to rescan the full node list.
func (ov *OverReserve) GetDesyncedNodes(lh logr.Logger) DesyncedNodes {
	ov.lock.Lock()
	defer ov.lock.Unlock()

	// make sure to log the generation to be able to crosscorrelate with later logs
	lh = lh.WithValues(logging.KeyGeneration, ov.generation)

	// this is intentionally aggressive. We don't yet make any attempt to find out if the
	// node was discarded because pessimistically overrserved (which should indeed trigger
	// a resync) or if it was discarded because the actual resources on the node really were
	// exhausted. We do like this because this is the safest approach. We will optimize
	// the node selection logic later on to make the resync procedure less aggressive but
	// still correct.
	nodes := ov.nodesWithForeignPods.Clone()
	foreignCount := nodes.Len()

	overreservedCount := ov.nodesMaybeOverreserved.Len()
	for _, node := range ov.nodesMaybeOverreserved.Keys() {
		nodes.Incr(node)
	}

	// always use local copies
	configChangeNodes := ov.nodesWithAttrUpdate.Clone()
	configChangeCount := configChangeNodes.Len()

	if nodes.Len() > 0 {
		lh.V(4).Info("found dirty nodes", "foreign", foreignCount, "discarded", overreservedCount, "configChange", configChangeCount, "total", nodes.Len())
	}
	return DesyncedNodes{
		Generation:        ov.generation,
		MaybeOverReserved: nodes.Keys(),
		ConfigChanged:     configChangeNodes.Keys(),
	}
}

// Resync implements the cache resync loop step. This function checks if the latest available NRT information received matches the
// state of a dirty node, for all the dirty nodes. If this is the case, the cache of a node can be Flush()ed.
// The trigger for attempting to resync a node is not just that we overallocated it. If a node was overallocated but still has capacity,
// we keep using it. But we cannot predict when the capacity is too low, because that would mean predicting the future workload requests.
// The best heuristic found so far is count how many times the node was skipped *AND* crosscheck with its overallocation state.
// If *both* a node has pessimistic overallocation accounted to it *and* was discarded "too many" (how much is too much is a runtime parameter
// which needs to be set and tuned) times, then it becomes a candidate for resync. Just using one of these two factors would lead to
// too aggressive resync attempts, so to more, likely unnecessary, computation work on the scheduler side.
func (ov *OverReserve) Resync() {
	// we are not working with a specific pod, so we need a unique key to track this flow
	lh_ := ov.lh.WithName(logging.FlowCacheSync)
	lh_.V(4).Info(logging.FlowBegin)
	defer lh_.V(4).Info(logging.FlowEnd)

	ov.drainNRTEvents(lh_)

	nodes := ov.GetDesyncedNodes(lh_)
	// we start without because chicken/egg problem. This is the earliest we can use the generation value.
	lh_ = lh_.WithValues(logging.KeyGeneration, nodes.Generation)

	// avoid as much as we can unnecessary work and logs.
	if nodes.Len() == 0 {
		lh_.V(5).Info("no dirty nodes detected")
		return
	}

	nrtUpdates := ov.MakeNRTUpdates(context.Background(), lh_, nodes)

	ov.FlushNodes(lh_, nrtUpdates...)
}

func (ov *OverReserve) MakeNRTUpdates(ctx context.Context, lh_ logr.Logger, nodes DesyncedNodes) []nrtUpdate {
	overReserved := sets.New(nodes.MaybeOverReserved...)
	configChanged := sets.New(nodes.ConfigChanged...)
	allNodes := overReserved.Clone().Insert(nodes.ConfigChanged...).UnsortedList()

	var nrtUpdates []nrtUpdate
	for _, nodeName := range allNodes {
		lh := lh_.WithValues(logging.KeyNode, nodeName)

		nd, err := ov.collectNodeData(ctx, lh_, nodeName)
		if err != nil {
			lh.V(2).Info("failed to collect node data", "error", err)
			continue
		}

		if overReserved.Has(nodeName) {
			if err := ov.isNRTFresher(lh, nd); err != nil {
				lh.V(2).Info("failed gate", "reason", err.Error())
			} else {
				lh.V(4).Info("overriding cached info", "reason", "resynced")
				nrtUpdates = append(nrtUpdates, ov.nrtUpdateFromNodeData(nd))
			}
		}
		if configChanged.Has(nodeName) {
			lh.V(4).Info("overriding cached info", "reason", "configChanged")
			nrtUpdates = append(nrtUpdates, ov.nrtUpdateFromNodeData(nd))
		}
	}

	return nrtUpdates
}

type nodeData struct {
	NRT  *topologyv1alpha2.NodeResourceTopology
	Pods []podData
}

func (ov *OverReserve) collectNodeData(ctx context.Context, lh logr.Logger, nodeName string) (nodeData, error) {
	nrtCandidate := &topologyv1alpha2.NodeResourceTopology{}
	if err := ov.client.Get(ctx, types.NamespacedName{Name: nodeName}, nrtCandidate); err != nil {
		return nodeData{}, err
	}

	pods, err := ov.podLister.ListByNode(lh, nodeName)
	if err != nil {
		return nodeData{}, err
	}

	nrtResources := ov.nrtResNames.Get(nodeName)
	podDataList := make([]podData, 0, len(pods))
	for _, pod := range pods {
		if ov.preemptionMode == apiconfig.PreemptionEnabled {
			podDataList = append(podDataList, categorizePodForPreemption(pod, nrtResources))
		} else {
			podDataList = append(podDataList, categorizePod(pod, nrtResources))
		}
	}

	return nodeData{
		NRT:  nrtCandidate,
		Pods: podDataList,
	}, nil
}

func (ov *OverReserve) isNRTFresher(lh logr.Logger, nd nodeData) error {
	pfpExpected, onlyExclRes := podFingerprintForNodeTopology(nd.NRT, ov.resyncMethod)
	if pfpExpected == "" {
		return errors.New("missing NodeTopology podset fingerprint data")
	}

	lh.V(4).Info("trying to sync NodeTopology", "fingerprint", pfpExpected, "onlyExclusiveResources", onlyExclRes)

	err := checkPodFingerprintForNode(lh, nd.Pods, nd.NRT.Name, pfpExpected, onlyExclRes)
	if errors.Is(err, podfingerprint.ErrSignatureMismatch) {
		// can happen, not critical
		return errors.New("NodeTopology podset fingerprint mismatch")
	}
	if err != nil {
		// should never happen, let's be vocal
		return fmt.Errorf("checking NodeTopology podset fingerprint: %w", err)
	}

	return nil
}

func (ov *OverReserve) nrtUpdateFromNodeData(nd nodeData) nrtUpdate {
	nrtUpdate := nrtUpdate{
		nrt: nd.NRT,
	}
	if ov.preemptionMode == apiconfig.PreemptionEnabled {
		nrtUpdate.pods = nd.Pods
	}
	return nrtUpdate
}

// FlushNodes drops all the cached information about a given node, resetting its state clean.
func (ov *OverReserve) FlushNodes(lh logr.Logger, nrtUpdates ...nrtUpdate) uint64 {
	ov.lock.Lock()
	defer ov.lock.Unlock()

	if len(nrtUpdates) == 0 {
		return ov.generation
	}

	for _, nrtUpdate := range nrtUpdates {
		lh.V(2).Info("flushing", logging.KeyNode, nrtUpdate.nrt.Name)
		ov.nrts.Update(nrtUpdate)
		ov.nrtResNames.Update(nrtUpdate.nrt)
		delete(ov.assumedResources, nrtUpdate.nrt.Name)
		ov.nodesMaybeOverreserved.Delete(nrtUpdate.nrt.Name)
		ov.nodesWithForeignPods.Delete(nrtUpdate.nrt.Name)
		ov.nodesWithAttrUpdate.Delete(nrtUpdate.nrt.Name)
	}

	// increase only if we mutated the internal state
	ov.generation += 1
	lh.V(2).Info("generation", "new", ov.generation)
	return ov.generation

}

// to be used only in tests
func (ov *OverReserve) TestOnlyUpdateNRT(nrt *topologyv1alpha2.NodeResourceTopology) {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	ov.nrts.Update(nrtUpdate{
		nrt: nrt,
	})
}

// TestOnlyWatcherStatus reports the lifecycle status of the underlying NRT watcher.
// to be used only in tests.
func (ov *OverReserve) TestOnlyWatcherStatus() WatcherStatus {
	return ov.nrtWatcher.TestOnlyWatcherStatus()
}

func categorizePod(pod *corev1.Pod, nrtResources sets.Set[corev1.ResourceName]) podData {
	pd := podData{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}
	if resourcerequests.AreExclusiveForPod(pod, nrtResources) {
		pd.ExclusiveResources = ExclusiveResourceAlloc
	} else {
		pd.ExclusiveResources = ExclusiveResourceNone
	}
	return pd
}

func categorizePodForPreemption(pod *corev1.Pod, nrtResources sets.Set[corev1.ResourceName]) podData {
	qos := v1qos.GetPodQOS(pod)
	ret := podData{
		Namespace: pod.Namespace,
		Name:      pod.Name,
	}

	for _, ctr := range pod.Spec.InitContainers {
		// filter out init containers with restart policy other than Always because these are *supposed* to
		// run fast and finish, hence not consuming exclusive resources in a steady state while the pod is Running.
		if !util.IsSidecarInitContainer(&ctr) {
			continue
		}
		if !resourcerequests.IsExclusiveForContainer(qos, ctr, nrtResources) {
			continue
		}
		ret.PinnedContainers = append(ret.PinnedContainers, ctr.Name)
	}

	for _, ctr := range pod.Spec.Containers {
		if !resourcerequests.IsExclusiveForContainer(qos, ctr, nrtResources) {
			continue
		}
		ret.PinnedContainers = append(ret.PinnedContainers, ctr.Name)
	}
	return ret
}

func getCacheResyncMethod(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache) apiconfig.CacheResyncMethod {
	var resyncMethod apiconfig.CacheResyncMethod
	if cfg != nil && cfg.ResyncMethod != nil {
		resyncMethod = *cfg.ResyncMethod
	} else { // explicitly set to nil?
		resyncMethod = apiconfig.CacheResyncAutodetect
		lh.Info("cache resync method missing", "fallback", resyncMethod)
	}
	return resyncMethod
}

func getCacheResyncScope(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache) apiconfig.CacheResyncScope {
	var resyncScope apiconfig.CacheResyncScope
	if cfg != nil && cfg.ResyncScope != nil {
		resyncScope = *cfg.ResyncScope
	} else { // explicitly set to nil?
		resyncScope = apiconfig.CacheResyncScopeAll
		lh.Info("cache resync scope missing", "fallback", resyncScope)
	}
	return resyncScope
}

// drainNRTEvents processes nodes received from the watcher goroutine via
// the nrtUpdateCh channel.
func (ov *OverReserve) drainNRTEvents(lh logr.Logger) {
	ov.lock.Lock()
	defer ov.lock.Unlock()
	queued := 0
	for {
		select {
		case nrtEv := <-ov.nrtUpdateCh:
			queued += ov.processNRTEvent(nrtEv, lh)
		default:
			if queued > 0 {
				lh.V(4).Info("drained NRT events", "queued", queued)
			}
			return
		}
	}
}

func (ov *OverReserve) processNRTEvent(nrtEv NRTEvent, lh logr.Logger) int {
	switch nrtEv.Reason {
	case WatchReasonAttrChanged:
		ov.nodesWithAttrUpdate.Incr(nrtEv.NodeName)
		return 1
	}
	lh.V(2).Info("unsupported NRT event", "reason", nrtEv.Reason.String(), logging.KeyNode, nrtEv.NodeName)
	return 0
}

