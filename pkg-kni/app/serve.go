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
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	openshifttls "github.com/openshift/controller-runtime-common/pkg/tls"
)

/*
- `**serveFunc` type** -- the callback signature (5 lines)
- `**customServe()`** -- the `serveFunc` implementation that constructs an `http.Server` with a custom
`*tls.Config` and calls the exported `server.RunServer()` (~30 lines, replaces `SecureServingInfo.Serve()`)
- `**buildCustomTLSConfig()`** -- your custom TLS config construction (placeholder until requirements are defined)
*/

type serveFunc func(
	serving *apiserver.SecureServingInfo,
	handler http.Handler,
	shutdownTimeout time.Duration,
	stopCh <-chan struct{},
) (<-chan struct{}, <-chan struct{}, error)

func customServe(s *apiserver.SecureServingInfo, handler http.Handler, shutdownTimeout time.Duration, stopCh <-chan struct{}) (<-chan struct{}, <-chan struct{}, error) {
	if s.Listener == nil {
		return nil, nil, fmt.Errorf("listener must not be nil")
	}

	var tlsConfig *tls.Config
	// TODO: replace the TLS config setup here with the custom TLS config
	// Create a cancellable context so the TLS controller can trigger a shutdown
	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	// Ensure the context is cancelled when the program exits.
	defer cancel()
	restConfig := ctrl.GetConfigOrDie()

	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		klog.Errorf("unable to create Kubernetes client: %v", err)
		return nil, nil, fmt.Errorf("unable to create Kubernetes client: %v", err)
	}

	tlsConfig = buildCustomTLSConfig(ctx, k8sClient)
	if tlsConfig == nil {
		return nil, nil, fmt.Errorf("failed to build custom TLS config")
	}

	secureServer := &http.Server{
		Addr:           s.Listener.Addr().String(),
		Handler:        handler,
		MaxHeaderBytes: 1 << 20,
		TLSConfig:      tlsConfig,

		IdleTimeout:       90 * time.Second, // matches http.DefaultTransport keep-alive timeout
		ReadHeaderTimeout: 32 * time.Second, // just shy of requestTimeoutUpperBound
	}

	if !s.DisableHTTP2 {
		// At least 99% of serialized resources in surveyed clusters were smaller than 256kb.
		// This should be big enough to accommodate most API POST requests in a single frame,
		// and small enough to allow a per connection buffer of this size multiplied by `MaxConcurrentStreams`.
		const resourceBody99Percentile = 256 * 1024

		http2Options := &http2.Server{
			IdleTimeout: 90 * time.Second, // matches http.DefaultTransport keep-alive timeout
			// shrink the per-stream buffer and max framesize from the 1MB default while still accommodating most API POST requests in a single frame
			MaxUploadBufferPerStream: resourceBody99Percentile,
			MaxReadFrameSize:         resourceBody99Percentile,
		}

		// use the overridden concurrent streams setting or make the default of 250 explicit so we can size MaxUploadBufferPerConnection appropriately
		if s.HTTP2MaxStreamsPerConnection > 0 {
			http2Options.MaxConcurrentStreams = uint32(s.HTTP2MaxStreamsPerConnection)
		} else {
			// match http2.initialMaxConcurrentStreams used by clients
			// this makes it so that a malicious client can only open 400 streams before we forcibly close the connection
			// https://github.com/golang/net/commit/b225e7ca6dde1ef5a5ae5ce922861bda011cfabd
			http2Options.MaxConcurrentStreams = 100
		}

		// increase the connection buffer size from the 1MB default to handle the specified number of concurrent streams
		http2Options.MaxUploadBufferPerConnection = http2Options.MaxUploadBufferPerStream * int32(http2Options.MaxConcurrentStreams)
		// apply settings to the server
		if err := http2.ConfigureServer(secureServer, http2Options); err != nil {
			return nil, nil, fmt.Errorf("error configuring http2: %v", err)
		}
	}

	// use tlsHandshakeErrorWriter to handle messages of tls handshake error
	tlsErrorWriter := &tlsHandshakeErrorWriter{os.Stderr}
	tlsErrorLogger := log.New(tlsErrorWriter, "", 0)
	secureServer.ErrorLog = tlsErrorLogger

	klog.Infof("Serving securely on %s", secureServer.Addr)
	return apiserver.RunServer(secureServer, s.Listener, shutdownTimeout, stopCh)
}

func buildCustomTLSConfig(ctx context.Context, k8sClient client.Client) *tls.Config {
	//TODO: custom TLS config construction
	// fetch from the OCP cluster config
	//openshifttls.FetchAPIServerTLSProfile(context.Background(), "default")

	// Fetch the TLS profile from the APIServer resource.
	tlsProfileSpec, err := openshifttls.FetchAPIServerTLSProfile(ctx, k8sClient)
	if err != nil {
		klog.Errorf("unable to get TLS profile from API server: %v", err)
		return nil
	}

	// Create the TLS configuration function for the server endpoints.
	tlsConfigFunc, unsupportedCiphers := openshifttls.NewTLSConfigFromProfile(tlsProfileSpec)
	if len(unsupportedCiphers) > 0 {
		klog.Infof("Some ciphers from TLS profile are not supported: %v", unsupportedCiphers)
	}
	//tlsOpts := []func(*tls.Config){tlsConfigFunc}
	// TODO: return the TLS config
	return nil
}

// tlsHandshakeErrorWriter writes TLS handshake errors to klog with
// trace level - V(5), to avoid flooding of tls handshake errors.
type tlsHandshakeErrorWriter struct {
	out io.Writer
}

const tlsHandshakeErrorPrefix = "http: TLS handshake error"

func (w *tlsHandshakeErrorWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), tlsHandshakeErrorPrefix) {
		klog.V(5).Info(string(p))
		metrics.TLSHandshakeErrors.Inc()
		return len(p), nil
	}

	// for non tls handshake error, log it as usual
	return w.out.Write(p)
}
