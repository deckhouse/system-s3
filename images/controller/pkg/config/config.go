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

package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-common-lib/monitoring"
)

const (
	ControllerNamespaceEnv     = "CONTROLLER_NAMESPACE"
	HealthProbeBindAddressEnv  = "HEALTH_PROBE_BIND_ADDRESS"
	MaxConcurrentReconcilesEnv = "MAX_CONCURRENT_RECONCILES"
	RequeueIntervalEnv         = "REQUEUE_INTERVAL_SECONDS"
	ReconcileTimeoutEnv        = "RECONCILE_TIMEOUT_SECONDS"
	ReadyResyncIntervalEnv     = "READY_RESYNC_INTERVAL_SECONDS"
	GarageImageEnv             = "GARAGE_IMAGE"
	SingleReplicaEnv           = "SINGLE_REPLICA"
	SizePerNodeEnv             = "SIZE_PER_NODE"
	StorageClassEnv            = "STORAGE_CLASS"
	ClusterDomainEnv           = "CLUSTER_DOMAIN"

	DefaultControllerNamespace     = "d8-system-s3"
	DefaultControllerName          = "system-s3-controller"
	DefaultSizePerNode             = "10Gi"
	DefaultStorageClass            = "system-s3-local"
	DefaultHealthProbeBindAddress  = ":8081"
	DefaultRequeueIntervalSeconds  = 30
	DefaultMaxConcurrentReconciles = 1
	DefaultClusterDomain           = "cluster.local"
	// DefaultReadyResyncIntervalSeconds is how often an already-Ready
	// SystemBucket is re-driven. It is the cadence at which a bucket or an access
	// key deleted behind the controller's back (directly in Garage) is noticed and
	// restored, and it bounds how long a missed watch event can leave a consumer
	// with credentials that no longer work (300s = 5m).
	DefaultReadyResyncIntervalSeconds = 300
	// DefaultReconcileTimeoutSeconds bounds a single reconcile. Reconcilers talk
	// to backends over HTTP, exec into data-plane pods and wait on the API
	// server; a reconcile context carries no deadline of its own, so one call
	// that never returns takes its worker with it — and with
	// MaxConcurrentReconciles at 1 that is the whole controller, silently, until
	// the pod is restarted. A deadline turns that into an error, a log line
	// naming the object, and a retry.
	DefaultReconcileTimeoutSeconds = 300
)

type Options struct {
	Loglevel               logger.Verbosity
	HealthProbeBindAddress string
	// MetricsBindAddress is where the manager serves /metrics, injected via
	// METRICS_BIND_ADDRESS from Helm.
	MetricsBindAddress      string
	ControllerNamespace     string
	MaxConcurrentReconciles int
	RequeueInterval         time.Duration
	// ReconcileTimeout bounds a single Reconcile call, so a backend that stops
	// answering cannot wedge a controller worker indefinitely.
	ReconcileTimeout time.Duration
	// ReadyResyncInterval is the requeue cadence for already-Ready SystemBuckets,
	// so drift in the backend (a bucket or key removed outside the controller)
	// self-heals within minutes instead of waiting for the ~10h informer resync.
	ReadyResyncInterval time.Duration
	// GarageImage is the module registry reference for the Garage server
	// image, injected via the GARAGE_IMAGE env var from Helm.
	GarageImage string
	// SingleReplica runs the system storage as one Garage replica with
	// replication factor 1 instead of the default three, injected via
	// SINGLE_REPLICA from the module settings.
	SingleReplica bool
	// SizePerNode is the capacity of the node-sticky local volume provisioned
	// per replica, injected via SIZE_PER_NODE from the module settings.
	SizePerNode string
	// StorageClass is the managed StorageClass backing those local volumes,
	// injected via STORAGE_CLASS.
	StorageClass string
	// ClusterDomain is the Kubernetes cluster DNS domain (e.g. cluster.local),
	// injected via CLUSTER_DOMAIN from global.discovery.clusterDomain. Used to
	// build in-cluster Service FQDNs.
	ClusterDomain string
}

// defaultMetricsBindAddress keeps the endpoint on loopback when the chart says
// nothing: the kube-rbac-proxy of the same pod is what publishes it. The fleet-wide
// fallback is the wildcard ":8080", which would both answer unauthenticated on the
// pod IP and collide with the proxy, since the two carry the same port number.
const defaultMetricsBindAddress = "127.0.0.1:8080"

func NewConfig() *Options {
	var opts Options

	opts.Loglevel = logger.VerbosityFromEnv(logger.InfoLevel)

	opts.MetricsBindAddress = monitoring.BindAddressFromEnvOr(defaultMetricsBindAddress)

	if v := os.Getenv(HealthProbeBindAddressEnv); v != "" {
		opts.HealthProbeBindAddress = v
	} else {
		opts.HealthProbeBindAddress = DefaultHealthProbeBindAddress
	}

	opts.ControllerNamespace = os.Getenv(ControllerNamespaceEnv)
	if opts.ControllerNamespace == "" {
		if ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			opts.ControllerNamespace = string(ns)
		} else {
			log.Printf("Failed to read namespace from filesystem: %v; falling back to %q", err, DefaultControllerNamespace)
			opts.ControllerNamespace = DefaultControllerNamespace
		}
	}

	opts.MaxConcurrentReconciles = DefaultMaxConcurrentReconciles
	if v := os.Getenv(MaxConcurrentReconcilesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.MaxConcurrentReconciles = n
		}
	}

	opts.RequeueInterval = time.Duration(DefaultRequeueIntervalSeconds) * time.Second
	if v := os.Getenv(RequeueIntervalEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.RequeueInterval = time.Duration(n) * time.Second
		}
	}

	opts.ReconcileTimeout = time.Duration(DefaultReconcileTimeoutSeconds) * time.Second
	if v := os.Getenv(ReconcileTimeoutEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.ReconcileTimeout = time.Duration(n) * time.Second
		}
	}

	opts.ReadyResyncInterval = time.Duration(DefaultReadyResyncIntervalSeconds) * time.Second
	if v := os.Getenv(ReadyResyncIntervalEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.ReadyResyncInterval = time.Duration(n) * time.Second
		}
	}

	opts.GarageImage = os.Getenv(GarageImageEnv)

	opts.SingleReplica = os.Getenv(SingleReplicaEnv) == "true"

	opts.SizePerNode = os.Getenv(SizePerNodeEnv)
	if opts.SizePerNode == "" {
		opts.SizePerNode = DefaultSizePerNode
	}

	opts.StorageClass = os.Getenv(StorageClassEnv)
	if opts.StorageClass == "" {
		opts.StorageClass = DefaultStorageClass
	}

	opts.ClusterDomain = os.Getenv(ClusterDomainEnv)
	if opts.ClusterDomain == "" {
		opts.ClusterDomain = DefaultClusterDomain
	}

	return &opts
}
