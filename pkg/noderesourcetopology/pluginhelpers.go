/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	k8scache "k8s.io/client-go/tools/cache"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/go-logr/logr"
	topologyv1alpha2 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2"
	"github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2/helper"
	"github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2/helper/numanode"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiconfig "sigs.k8s.io/scheduler-plugins/apis/config"
	nrtcache "sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/cache"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/logging"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/podprovider"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/stringify"
)

const (
	maxNUMAId = 64
)

func initNodeTopologyInformer(ctx context.Context, lh logr.Logger,
	tcfg *apiconfig.NodeResourceTopologyMatchArgs, handle fwk.Handle) (nrtcache.Interface, error) {
	client, err := ctrlclient.NewWithWatch(handle.KubeConfig(), ctrlclient.Options{Scheme: scheme})
	if err != nil {
		lh.Error(err, "cannot create client for NodeTopologyResource", "kubeConfig", handle.KubeConfig())
		return nil, err
	}

	if tcfg.DiscardReservedNodes {
		return nrtcache.NewDiscardReserved(lh.WithName(logging.SubsystemNRTCache), client), nil
	}

	if tcfg.CacheResyncPeriodSeconds <= 0 {
		return nrtcache.NewPassthrough(lh.WithName(logging.SubsystemNRTCache), client), nil
	}

	podSharedInformer, podLister, err := podprovider.NewFromHandle(lh, handle, tcfg.Cache)
	if err != nil {
		return nil, err
	}

	ovCache, err := nrtcache.NewOverReserve(ctx, lh.WithName(logging.SubsystemNRTCache), tcfg.Cache, client, podLister, *tcfg.PreemptionMode)
	if err != nil {
		return nil, err
	}

	fwk, ok := handle.(framework.Framework)
	if !ok || fwk.ProfileName() == "" {
		lh.Info("cannot determine the scheduler profile name")
		return nil, fmt.Errorf("cannot determine the scheduler profile name")
	}
	profileName := fwk.ProfileName()

	initNodeTopologyPodDetection(lh, tcfg.Cache, profileName, podSharedInformer, ovCache, *tcfg.PreemptionMode)
	var postResyncHandler *rejectedPodWaker
	if ovCache.RequeuesRejectedPods() {
		postResyncHandler = newRejectedPodWaker(handle, profileName, ovCache)
	}

	resyncPeriod := time.Duration(tcfg.CacheResyncPeriodSeconds) * time.Second

	go wait.Forever(func() {
		flushed := ovCache.Resync()
		if postResyncHandler == nil || len(flushed) == 0 {
			return
		}
		postResyncHandler.wake(lh, flushed)
	}, resyncPeriod)

	lh.V(3).Info("enable NodeTopology cache (needs the Reserve plugin)", "resyncPeriod", resyncPeriod)

	return ovCache, nil
}

func initNodeTopologyPodDetection(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache, profileName string, podSharedInformer k8scache.SharedInformer, nrtCache *nrtcache.OverReserve, preemptionMode apiconfig.PreemptionMode) {
	if podSharedInformer == nil {
		lh.Info("pod informer missing")
		return
	}

	lh = lh.WithValues("profileName", profileName)
	opts := &nrtcache.PodCacheHandlersOpts{}

	setupForeignPodsDetection(lh, cfg, profileName, opts)
	setupTerminatedOwnPodsDetection(lh, cfg, preemptionMode, opts)
	setupRejectedPodsDetection(lh, cfg, profileName, nrtCache, opts)

	nrtcache.SetupPodCacheHandlers(lh.WithName(logging.SubsystemNRTCache), profileName, podSharedInformer, nrtCache, *opts)
}

func setupForeignPodsDetection(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache, profileName string, opts *nrtcache.PodCacheHandlersOpts) {
	foreignPodsDetect := getForeignPodsDetectMode(lh, cfg)
	foreignEnabled := foreignPodsDetect != apiconfig.ForeignPodsDetectNone
	if !foreignEnabled {
		lh.Info("foreign pods detection disabled by configuration")
		return
	}

	lh.Info("setting up foreign pods detection", "mode", foreignPodsDetect)
	if foreignPodsDetect == apiconfig.ForeignPodsDetectOnlyExclusiveResources {
		nrtcache.TrackOnlyForeignPodsWithExclusiveResources()
	} else {
		nrtcache.TrackAllForeignPods()
	}
	nrtcache.RegisterSchedulerProfileName(lh.WithName(logging.SubsystemForeignPods), profileName)
	opts.ForeignPodsDetect = foreignEnabled
}

func setupTerminatedOwnPodsDetection(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache, preemptionMode apiconfig.PreemptionMode, opts *nrtcache.PodCacheHandlersOpts) {
	terminatedOwnPodsDetect := getTerminatedOwnPodsDetectMode(lh, cfg, preemptionMode)
	terminatedEnabled := terminatedOwnPodsDetect != apiconfig.TerminatedOwnPodsDetectNone
	if !terminatedEnabled {
		lh.Info("terminated own-pods detection disabled by configuration")
		return
	}

	lh.Info("setting up terminated own-pods detection", "mode", terminatedOwnPodsDetect)
	opts.TerminatedOwnPodsDetect = terminatedEnabled
}

func setupRejectedPodsDetection(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache, profileName string, nrtCache *nrtcache.OverReserve, opts *nrtcache.PodCacheHandlersOpts) {
	requeueEnabled := requeueUnschedulablePodsEnabled(cfg)
	if !requeueEnabled {
		lh.Info("rejected pod requeue disabled by configuration")
		return
	}

	lh.Info("enabling rejected pod requeue", "name", profileName)
	nrtCache.EnableRequeueUnschedulablePods()
	opts.ForgetRejectedPods = requeueEnabled
}

// requeueUnschedulablePodsEnabled reports whether cache.requeueUnschedulablePods is enabled.
// Only OnlyExclusiveResources turns the feature on.
func requeueUnschedulablePodsEnabled(cfg *apiconfig.NodeResourceTopologyCache) bool {
	return cfg != nil &&
		cfg.RequeueUnschedulablePods != nil &&
		*cfg.RequeueUnschedulablePods == apiconfig.RequeueUnschedulablePodsOnlyExclusiveResources
}

func createNUMANodeList(lh logr.Logger, zones topologyv1alpha2.ZoneList) NUMANodeList {
	numaIDToZoneIDx := make([]int, maxNUMAId)
	nodes := NUMANodeList{}
	// filter non Node zones and create idToIdx lookup array
	for i, zone := range zones {
		if zone.Type != helper.ZoneTypeNUMANode {
			continue
		}

		numaID, err := numanode.NameToID(zone.Name)
		if err != nil || numaID > maxNUMAId {
			lh.Error(err, "error getting the numaID", "zone", zone.Name, "numaID", numaID)
			continue
		}

		numaIDToZoneIDx[numaID] = i

		resources := extractResources(zone)
		numaItems := []interface{}{"numaCell", numaID}
		lh.V(6).Info("extracted NUMA resources", stringify.ResourceListToLoggableWithValues(numaItems, resources)...)
		nodes = append(nodes, NUMANode{NUMAID: numaID, Resources: resources})
	}

	// iterate over nodes and fill them with Costs
	for i, node := range nodes {
		nodes[i] = *node.WithCosts(extractCosts(zones[numaIDToZoneIDx[node.NUMAID]].Costs))
	}

	return nodes
}

func extractCosts(costs topologyv1alpha2.CostList) map[int]int {
	nodeCosts := make(map[int]int)

	// return early if CostList is missing
	if len(costs) == 0 {
		return nodeCosts
	}

	for _, cost := range costs {
		numaID, err := numanode.NameToID(cost.Name)
		if err != nil || numaID > maxNUMAId {
			continue
		}
		nodeCosts[numaID] = int(cost.Value)
	}

	return nodeCosts
}

func extractResources(zone topologyv1alpha2.Zone) corev1.ResourceList {
	res := make(corev1.ResourceList)
	for _, resInfo := range zone.Resources {
		res[corev1.ResourceName(resInfo.Name)] = resInfo.Available.DeepCopy()
	}
	return res
}

func onlyNonNUMAResources(numaNodes NUMANodeList, resources corev1.ResourceList) bool {
	for resourceName := range resources {
		for _, node := range numaNodes {
			if _, ok := node.Resources[resourceName]; ok {
				return false
			}
		}
	}

	return true
}

func getForeignPodsDetectMode(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache) apiconfig.ForeignPodsDetectMode {
	var foreignPodsDetect apiconfig.ForeignPodsDetectMode
	if cfg != nil && cfg.ForeignPodsDetect != nil {
		foreignPodsDetect = *cfg.ForeignPodsDetect
	} else { // explicitly set to nil?
		foreignPodsDetect = apiconfig.ForeignPodsDetectAll
		lh.Info("foreign pods detection value missing", "fallback", foreignPodsDetect)
	}
	return foreignPodsDetect
}

func getTerminatedOwnPodsDetectMode(lh logr.Logger, cfg *apiconfig.NodeResourceTopologyCache, preemptionMode apiconfig.PreemptionMode) apiconfig.TerminatedOwnPodsDetectMode {
	var terminatedOwnPodsDetect apiconfig.TerminatedOwnPodsDetectMode
	if preemptionMode == apiconfig.PreemptionEnabled {
		//Preemption requires terminated-own-pods detection to avoid false preemption
		terminatedOwnPodsDetect = apiconfig.TerminatedOwnPodsDetectOnlyExclusiveResources
		lh.Info("forcing terminated own-pods detection because preemption is enabled", "mode", terminatedOwnPodsDetect)
		return terminatedOwnPodsDetect
	}

	if cfg != nil && cfg.TerminatedOwnPodsDetect != nil {
		terminatedOwnPodsDetect = *cfg.TerminatedOwnPodsDetect
	} else {
		terminatedOwnPodsDetect = apiconfig.TerminatedOwnPodsDetectNone
		lh.Info("terminated own-pods detection value missing", "fallback", terminatedOwnPodsDetect)
	}
	return terminatedOwnPodsDetect
}

func getPreemptionMode(lh logr.Logger, tcfg *apiconfig.NodeResourceTopologyMatchArgs) apiconfig.PreemptionMode {
	if tcfg.PreemptionMode != nil {
		return *tcfg.PreemptionMode
	}
	preemptionMode := apiconfig.PreemptionDisabled
	lh.Info("preemption mode missing", "fallback", preemptionMode)
	return preemptionMode
}
