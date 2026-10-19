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
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"

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

	// ControllerImageEnv carries the controller's own image. The migration Job
	// runs that same image with a different argument, and a pod cannot find out
	// the reference it was pulled from, so the chart names it here.
	ControllerImageEnv = "CONTROLLER_IMAGE"

	// HighAvailabilityEnv carries the platform's high-availability mode, already
	// resolved by the chart (module setting -> global.highAvailability -> whether
	// the control plane is highly available). It decides the replica count and the
	// replication factor the data plane is built with.
	HighAvailabilityEnv = "HIGH_AVAILABILITY"

	// HighAvailabilityExplicitEnv says whether the value above was written by
	// somebody or derived from the shape of the control plane. The chart knows
	// which; the controller cannot tell from a boolean, and the difference
	// decides whether losing masters is allowed to shrink the store.
	HighAvailabilityExplicitEnv = "HIGH_AVAILABILITY_EXPLICIT"

	// The credentials endpoint (see internal/sts): where it listens and which
	// certificate it serves. The certificate is the module's serving certificate
	// from the webhooks-https-certs Secret (named after the admission webhook that
	// used to serve it), mounted into this container by the chart. It is renewed
	// in place, so the endpoint re-reads the files rather than loading them once.
	STSBindAddressEnv = "STS_BIND_ADDRESS"
	STSCertFileEnv    = "STS_TLS_CERT_FILE"
	STSKeyFileEnv     = "STS_TLS_KEY_FILE"

	// STSEndpointEnv is the address consumers reach the endpoint at, and
	// STSCertSecretEnv the Secret its CA is read from so it can be published to
	// them. Both are rendered by the chart: the Service name and the certificate
	// are the chart's to decide, and having the controller compose them from parts
	// is how the two drift apart.
	STSEndpointEnv   = "STS_ENDPOINT"
	STSCertSecretEnv = "STS_CERT_SECRET"

	// The data plane's TLS. Garage serves none of its own; a sidecar in every
	// Garage pod terminates it with the certificate in GarageTLSSecretEnv, which
	// the chart renders from the module's CA. The controller mounts the same
	// Secret into the Garage pods and the migration Job, reads its ca.crt from
	// GarageTLSCAFileEnv to verify Garage itself, and publishes the CA to
	// consumers next to the endpoint.
	GarageTLSSecretEnv = "GARAGE_TLS_SECRET"
	GarageTLSCAFileEnv = "GARAGE_TLS_CA_FILE"

	StorageClassEnv  = "STORAGE_CLASS"
	ClusterDomainEnv = "CLUSTER_DOMAIN"

	// Where the data plane may run, as the chart works it out with the platform's
	// "system" strategy: a JSON node selector and a JSON list of tolerations.
	DataPlaneNodeSelectorEnv = "DATA_PLANE_NODE_SELECTOR"
	DataPlaneTolerationsEnv  = "DATA_PLANE_TOLERATIONS"

	// VPAEnabledEnv says whether the cluster has the VerticalPodAutoscaler API,
	// which the chart reads off the list of enabled modules.
	VPAEnabledEnv = "VPA_ENABLED"

	DefaultControllerNamespace     = "d8-system-s3"
	DefaultControllerName          = "system-s3-controller"
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

	// DefaultSTSBindAddress is the port the credentials endpoint listens on. Not
	// 8443, because that was the port of the admission webhook container the Pod
	// used to run, and two listeners on one port in one network namespace is one
	// listener and one crash loop. Consumers reach the endpoint through its
	// Service, so the number is internal to the Pod and there is no reason to
	// move it now.
	DefaultSTSBindAddress = ":8444"
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
	// ControllerImage is this controller's own image, which the object copier of
	// a migration runs (see ControllerImageEnv). Empty means no migration can
	// copy anything, which the phase says out loud rather than starting a Job
	// with no image.
	ControllerImage string
	// HighAvailability runs the system storage as three Garage replicas with
	// replication factor 3; without it, as a single replica with factor 1.
	// Injected via HIGH_AVAILABILITY (see HighAvailabilityEnv).
	//
	// The default is true, which is the safe direction for a value that failed to
	// arrive: three replicas on a cluster that wanted one costs footprint, one
	// replica on a cluster that wanted three costs the redundancy it asked for.
	HighAvailability bool
	// HighAvailabilityExplicit reports that HighAvailability came from a setting
	// somebody wrote (the module's own or global's) rather than from the number
	// of masters the cluster happens to have.
	//
	// The default is false, and it is the safe direction for a value that failed
	// to arrive: a store is then never shrunk without somebody asking.
	HighAvailabilityExplicit bool
	// StorageClass is the class the data plane's volumes are ordered from,
	// injected via STORAGE_CLASS (the chart resolves it: the module setting, then
	// global.modules.storageClass, then the cluster default).
	//
	// It has no default here, and must not have one. Empty means the cluster
	// offered no class at all, and the only honest response to that is to bring up
	// no data plane and say so — a default would name a class that may not exist
	// and turn a clear refusal into claims stuck Pending.
	StorageClass string
	// STSBindAddress, STSCertFile and STSKeyFile configure the credentials
	// endpoint. With no certificate the endpoint does not start at all: it hands
	// out secret keys, and doing that over plaintext because a file was missing is
	// not a degraded mode worth having.
	STSBindAddress string
	STSCertFile    string
	STSKeyFile     string

	// STSEndpoint is published to consumers in their credentials Secret, and
	// STSCertSecret is where the CA that signs it is read from.
	STSEndpoint   string
	STSCertSecret string

	// GarageTLSSecret is the Secret holding the data plane's serving certificate
	// (tls.crt, tls.key, ca.crt), and GarageTLSCAFile the controller's own mount
	// of its ca.crt. Without them the data plane is not brought up: every client
	// of it verifies against this CA, and there is no plaintext mode to fall back
	// to.
	GarageTLSSecret string
	GarageTLSCAFile string

	// ClusterDomain is the Kubernetes cluster DNS domain (e.g. cluster.local),
	// injected via CLUSTER_DOMAIN from global.discovery.clusterDomain. Used to
	// build in-cluster Service FQDNs.
	ClusterDomain string

	// DataPlaneNodeSelector and DataPlaneTolerations are where the data plane
	// may run. Empty is any node without a taint, which is also what the chart
	// renders for a cluster with neither the module's node role nor the system
	// one.
	DataPlaneNodeSelector map[string]string
	DataPlaneTolerations  []corev1.Toleration

	// VPAEnabled gives every data plane a VerticalPodAutoscaler. The default is
	// false: an object of a kind the cluster does not serve is an error on every
	// pass, while the static requests are what the data plane ran with before.
	VPAEnabled bool
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

	opts.MaxConcurrentReconciles = positiveIntFromEnv(MaxConcurrentReconcilesEnv, DefaultMaxConcurrentReconciles)
	opts.RequeueInterval = secondsFromEnv(RequeueIntervalEnv, DefaultRequeueIntervalSeconds)
	opts.ReconcileTimeout = secondsFromEnv(ReconcileTimeoutEnv, DefaultReconcileTimeoutSeconds)
	opts.ReadyResyncInterval = secondsFromEnv(ReadyResyncIntervalEnv, DefaultReadyResyncIntervalSeconds)

	opts.GarageImage = os.Getenv(GarageImageEnv)
	opts.ControllerImage = os.Getenv(ControllerImageEnv)

	opts.HighAvailability = boolFromEnv(HighAvailabilityEnv, true)
	opts.HighAvailabilityExplicit = boolFromEnv(HighAvailabilityExplicitEnv, false)

	opts.STSBindAddress = os.Getenv(STSBindAddressEnv)
	if opts.STSBindAddress == "" {
		opts.STSBindAddress = DefaultSTSBindAddress
	}
	opts.STSCertFile = os.Getenv(STSCertFileEnv)
	opts.STSKeyFile = os.Getenv(STSKeyFileEnv)
	opts.STSEndpoint = os.Getenv(STSEndpointEnv)
	opts.STSCertSecret = os.Getenv(STSCertSecretEnv)
	opts.GarageTLSSecret = os.Getenv(GarageTLSSecretEnv)
	opts.GarageTLSCAFile = os.Getenv(GarageTLSCAFileEnv)

	opts.StorageClass = os.Getenv(StorageClassEnv)

	opts.ClusterDomain = os.Getenv(ClusterDomainEnv)
	if opts.ClusterDomain == "" {
		opts.ClusterDomain = DefaultClusterDomain
	}

	jsonFromEnv(DataPlaneNodeSelectorEnv, &opts.DataPlaneNodeSelector)
	jsonFromEnv(DataPlaneTolerationsEnv, &opts.DataPlaneTolerations)

	opts.VPAEnabled = boolFromEnv(VPAEnabledEnv, false)

	return &opts
}

// jsonFromEnv decodes a JSON value, leaving out untouched when the variable is
// empty.
//
// Unlike the helpers below it has no default to fall back to, and it stops the
// process instead. The values it reads say where the data plane runs, and every
// fallback there is a placement nobody chose: an empty selector puts replicas on
// whatever node is free, and their volumes, on a class pinned to nodes, stay
// there. A controller that refuses to start says what is wrong in the one place
// an operator is already looking.
func jsonFromEnv(name string, out any) {
	v := os.Getenv(name)
	if v == "" {
		return
	}
	if err := json.Unmarshal([]byte(v), out); err != nil {
		log.Fatalf("%s=%q is not valid JSON: %v", name, v, err)
	}
}

// The three helpers below share one rule: a value that cannot be used is
// reported, not swallowed.
//
// The chart renders all of these, and it renders them correctly, so none of this
// fires in a normal installation. It fires when somebody edits the environment of
// a running pod to change the module's behaviour — which is exactly the moment a
// typo must not look like it worked. HIGH_AVAILABILITY is the sharpest case: it
// decides the shape of the data plane at first init and is pinned from then on,
// so a silent "it did not take" is a store built in the wrong mode.

// positiveIntFromEnv reads a positive integer, falling back to def and saying so
// when the value is unusable.
func positiveIntFromEnv(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("%s=%q is not a positive integer; using %d", name, v, def)
		return def
	}
	return n
}

// secondsFromEnv reads a positive number of seconds as a duration.
func secondsFromEnv(name string, defSeconds int) time.Duration {
	return time.Duration(positiveIntFromEnv(name, defSeconds)) * time.Second
}

// boolFromEnv reads a boolean through strconv.ParseBool, so "True", "1" and "yes"
// mean what whoever typed them meant, and anything else is reported rather than
// read as false.
func boolFromEnv(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Printf("%s=%q is not a boolean; using %t", name, v, def)
		return def
	}
	return b
}
