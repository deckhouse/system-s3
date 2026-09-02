/*
Copyright 2026 Flant JSC

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

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-common-lib/logger/kwhlogger"
	"github.com/deckhouse/system-s3/images/webhooks/handlers"
)

// webhookReadHeaderTimeout bounds how long a client may take to send its request
// headers. Generous for the only client there is (the apiserver, on the same
// network), short enough that idle half-open connections cannot accumulate.
const webhookReadHeaderTimeout = 10 * time.Second

type config struct {
	certFile string
	keyFile  string
}

func httpHandlerHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = fmt.Fprint(w, "Ok.")
}

func initFlags() (config, error) {
	cfg := config{}

	fl := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fl.StringVar(&cfg.certFile, "tls-cert-file", "", "TLS certificate file")
	fl.StringVar(&cfg.keyFile, "tls-key-file", "", "TLS key file")

	err := fl.Parse(os.Args[1:])
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}

const (
	port = ":8443"

	SystemBucketValidatorID = "SystemBucketValidator"
)

func main() {
	// LOG_LEVEL arrives as a number 0..4 from lib-helm, which is what the
	// controller container in this same pod already reads. The webhook used to map
	// it from the ERROR..TRACE spelling instead, which the charts never emit, so
	// its level was always the default no matter what the module setting said.
	log, err := logger.NewLoggerFromEnv(logger.InfoLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create the logger: %s\n", err)
		os.Exit(1)
	}
	kwhLog := kwhlogger.New(log)

	cfg, err := initFlags()
	if err != nil {
		fmt.Printf("unable to parse config: err: %s", err.Error())
		os.Exit(1)
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to build in-cluster config: %s", err)
		os.Exit(1)
	}
	dynClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to build dynamic client: %s", err)
		os.Exit(1)
	}
	validator := handlers.NewValidator(dynClient)

	systemBucketWebhookHandler, err := handlers.GetValidatingWebhookHandler(
		validator.SystemBucketValidate,
		SystemBucketValidatorID,
		&unstructured.Unstructured{},
		kwhLog,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating systemBucketWebhookHandler: %s", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/systembucket-validate", systemBucketWebhookHandler)
	mux.HandleFunc("/healthz", httpHandlerHealthz)

	// An explicit server, not http.ListenAndServeTLS, for two reasons.
	//
	// ReadHeaderTimeout: this endpoint gates object creation with
	// failurePolicy: Fail, so connections that open and never finish sending
	// headers are not just wasted sockets — enough of them and no SystemBucket can
	// be created at all. Only the apiserver reaches this port, which is why the
	// surface is narrow rather than absent.
	//
	// MinVersion: without it the floor is whatever the Go runtime's default is,
	// which is a property of the toolchain rather than a decision of this module.
	srv := &http.Server{
		Addr:              port,
		Handler:           mux,
		ReadHeaderTimeout: webhookReadHeaderTimeout,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}

	log.Info("listening", "address", port)
	err = srv.ListenAndServeTLS(cfg.certFile, cfg.keyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error serving webhook: %s", err)
		os.Exit(1)
	}
}
