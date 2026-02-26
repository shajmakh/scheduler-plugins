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
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/blang/semver/v4"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/server/healthz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/leaderelection"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/configz"
	utilversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
	schedulerserverconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
)

// run executes the scheduler based on the given configuration. It only returns on error or when context is done.
// it is a copy of the Run function from https://github.com/kubernetes/kubernetes/blob/master/cmd/kube-scheduler/app/server.go
// with only the serveFn function replaced to allow control over TLS configuration.
func run(ctx context.Context, cc *schedulerserverconfig.CompletedConfig, sched *scheduler.Scheduler, serveFn serveFunc) error {
	logger := klog.FromContext(ctx)

	// To help debugging, immediately log version
	logger.Info("Starting Kubernetes Scheduler", "version", utilversion.Get())

	logger.Info("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	// Configz registration.
	if cz, err := configz.New("componentconfig"); err != nil {
		return fmt.Errorf("unable to register configz: %s", err)
	} else {
		cz.Set(cc.ComponentConfig)
	}

	// Start events processing pipeline.
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())
	defer cc.EventBroadcaster.Shutdown()

	// Setup healthz checks.
	var checks, readyzChecks []healthz.HealthChecker
	if cc.ComponentConfig.LeaderElection.LeaderElect {
		checks = append(checks, cc.LeaderElection.WatchDog)
		readyzChecks = append(readyzChecks, cc.LeaderElection.WatchDog)
	}
	readyzChecks = append(readyzChecks, healthz.NewShutdownHealthz(ctx.Done()))

	waitingForLeader := make(chan struct{})
	isLeader := func() bool {
		select {
		case _, ok := <-waitingForLeader:
			// if channel is closed, we are leading
			return !ok
		default:
			// channel is open, we are waiting for a leader
			return false
		}
	}

	handlerSyncReadyCh := make(chan struct{})
	handlerSyncCheck := healthz.NamedCheck("sched-handler-sync", func(_ *http.Request) error {
		select {
		case <-handlerSyncReadyCh:
			return nil
		default:
		}
		return fmt.Errorf("handlers are not fully synchronized")
	})
	readyzChecks = append(readyzChecks, handlerSyncCheck)

	if cc.LeaderElection != nil && utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CoordinatedLeaderElection) {
		binaryVersion, err := semver.ParseTolerant(cc.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent).BinaryVersion().String())
		if err != nil {
			return err
		}
		emulationVersion, err := semver.ParseTolerant(cc.ComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent).EmulationVersion().String())
		if err != nil {
			return err
		}

		// Start lease candidate controller for coordinated leader election
		leaseCandidate, waitForSync, err := leaderelection.NewCandidate(
			cc.Client,
			metav1.NamespaceSystem,
			cc.LeaderElection.Lock.Identity(),
			kubeScheduler,
			binaryVersion.FinalizeVersion(),
			emulationVersion.FinalizeVersion(),
			coordinationv1.OldestEmulationVersion,
		)
		if err != nil {
			return err
		}
		readyzChecks = append(readyzChecks, healthz.NewInformerSyncHealthz(waitForSync))
		go leaseCandidate.Run(ctx)
	}

	// Start up the server for endpoints.
	gracefulShutdownSecureServer := func() {}
	if cc.SecureServing != nil {
		handler := buildHandlerChain(newEndpointsHandler(&cc.ComponentConfig, cc.InformerFactory, isLeader, checks, readyzChecks, cc.Flagz), cc.Authentication.Authenticator, cc.Authorization.Authorizer)
		internalStopCh := make(chan struct{})
		shutdownTimeout := 5 * time.Second
		stoppedCh, listenerStoppedCh, err := serveFn(cc.SecureServing, handler, shutdownTimeout, internalStopCh)
		if err != nil {
			// fail early for secure handlers, removing the old error loop from above
			close(internalStopCh)
			return fmt.Errorf("failed to start secure server: %v", err)
		}
		gracefulShutdownSecureServer = func() {
			close(internalStopCh)
			<-listenerStoppedCh
			logger.Info("[graceful-termination] secure server has stopped listening")
			<-stoppedCh
			logger.Info("[graceful-termination] secure server is exiting")
		}
	}

	startInformersAndWaitForSync := func(ctx context.Context) {
		// Start all informers.
		cc.InformerFactory.Start(ctx.Done())
		// DynInformerFactory can be nil in tests.
		if cc.DynInformerFactory != nil {
			cc.DynInformerFactory.Start(ctx.Done())
		}

		// Wait for all caches to sync before scheduling.
		cc.InformerFactory.WaitForCacheSync(ctx.Done())
		// DynInformerFactory can be nil in tests.
		if cc.DynInformerFactory != nil {
			cc.DynInformerFactory.WaitForCacheSync(ctx.Done())
		}

		// Wait for all handlers to sync (all items in the initial list delivered) before scheduling.
		if err := sched.WaitForHandlersSync(ctx); err != nil {
			logger.Error(err, "handlers are not fully synchronized")
		}

		close(handlerSyncReadyCh)
		logger.V(3).Info("Handlers synced")
	}
	if !cc.ComponentConfig.DelayCacheUntilActive || cc.LeaderElection == nil {
		startInformersAndWaitForSync(ctx)
	}
	// If leader election is enabled, runCommand via LeaderElector until done and exit.
	if cc.LeaderElection != nil {
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.CoordinatedLeaderElection) {
			cc.LeaderElection.Coordinated = true
		}
		cc.LeaderElection.Callbacks = leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				close(waitingForLeader)
				if cc.ComponentConfig.DelayCacheUntilActive {
					logger.Info("Starting informers and waiting for sync...")
					startInformersAndWaitForSync(ctx)
					logger.Info("Sync completed")
				}
				sched.Run(ctx)
			},
			OnStoppedLeading: func() {
				gracefulShutdownSecureServer()
				select {
				case <-ctx.Done():
					// We were asked to terminate. Exit 0.
					logger.Info("Requested to terminate, exiting")
					os.Exit(0)
				default:
					// We lost the lock.
					logger.Error(nil, "Leaderelection lost")
					klog.FlushAndExit(klog.ExitFlushTimeout, 1)
				}
			},
		}
		leaderElector, err := leaderelection.NewLeaderElector(*cc.LeaderElection)
		if err != nil {
			return fmt.Errorf("couldn't create leader elector: %v", err)
		}

		leaderElector.Run(ctx)

		return fmt.Errorf("lost lease")
	}

	// Leader election is disabled, so runCommand inline until done.
	close(waitingForLeader)
	sched.Run(ctx)
	gracefulShutdownSecureServer()
	return fmt.Errorf("finished without leader elect")
}
