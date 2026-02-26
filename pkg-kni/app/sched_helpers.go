/*
 * Copyright 2026 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app

import (
	"net/http"
	goruntime "runtime"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/prometheus/slis"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/component-base/zpages/flagz"
	"k8s.io/component-base/zpages/statusz"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/metrics/resources"
	"sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/scheme"
)

/*
This is a helper file to wrap TLS configuration internally without vendor updates.
It verbatim copies the code from
https://github.com/kubernetes/kubernetes/blob/master/cmd/kube-scheduler/app/server.go
*/

const (
	kubeScheduler = "kube-scheduler"
)

// buildHandlerChain wraps the given handler with the standard filters.
func buildHandlerChain(handler http.Handler, authn authenticator.Request, authz authorizer.Authorizer) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}
	failedHandler := genericapifilters.Unauthorized(scheme.Codecs)

	handler = genericapifilters.WithAuthorization(handler, authz, scheme.Codecs)
	handler = genericapifilters.WithAuthentication(handler, authn, failedHandler, nil, nil)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

func installMetricHandler(pathRecorderMux *mux.PathRecorderMux, informers informers.SharedInformerFactory, isLeader func() bool) {
	configz.InstallHandler(pathRecorderMux)
	pathRecorderMux.Handle("/metrics", legacyregistry.HandlerWithReset())

	resourceMetricsHandler := resources.Handler(informers.Core().V1().Pods().Lister())
	pathRecorderMux.HandleFunc("/metrics/resources", func(w http.ResponseWriter, req *http.Request) {
		if !isLeader() {
			return
		}
		resourceMetricsHandler.ServeHTTP(w, req)
	})
}

// newEndpointsHandler creates an API health server from the config, and will also
// embed the metrics handler and z-pages handler.
// TODO: healthz check is deprecated, please use livez and readyz instead. Will be removed in the future.
func newEndpointsHandler(config *kubeschedulerconfig.KubeSchedulerConfiguration, informers informers.SharedInformerFactory, isLeader func() bool, healthzChecks, readyzChecks []healthz.HealthChecker, flagReader flagz.Reader) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux(kubeScheduler)
	healthz.InstallHandler(pathRecorderMux, healthzChecks...)
	healthz.InstallLivezHandler(pathRecorderMux)
	healthz.InstallReadyzHandler(pathRecorderMux, readyzChecks...)
	installMetricHandler(pathRecorderMux, informers, isLeader)
	slis.SLIMetricsWithReset{}.Install(pathRecorderMux)

	if config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routes.DebugFlags{}.Install(pathRecorderMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		if flagReader != nil {
			flagz.Install(pathRecorderMux, kubeScheduler, flagReader)
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		statusz.Install(pathRecorderMux, kubeScheduler, statusz.NewRegistry(compatibility.DefaultBuildEffectiveVersion(), statusz.WithListedPaths(pathRecorderMux.ListedPaths())))
	}

	return pathRecorderMux
}
