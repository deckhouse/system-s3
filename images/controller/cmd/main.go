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
	"context"
	"errors"
	"fmt"
	"os"
	goruntime "runtime"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-common-lib/monitoring"
	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/garage"
	"github.com/deckhouse/system-s3/images/controller/internal/controller"
	"github.com/deckhouse/system-s3/images/controller/internal/migrator"
	"github.com/deckhouse/system-s3/images/controller/internal/sts"
	"github.com/deckhouse/system-s3/images/controller/internal/tlsproxy"
	"github.com/deckhouse/system-s3/images/controller/pkg/config"
	"github.com/deckhouse/system-s3/images/controller/pkg/kubutils"
)

var (
	buildDate = ""
	version   = ""
	commit    = ""
)

var resourcesSchemeFuncs = []func(*apiruntime.Scheme) error{
	v1alpha1.AddToScheme,
	clientgoscheme.AddToScheme,
	v1.AddToScheme,
}

func main() {
	// SetupSignalHandler, not Background: a cancellable context is what lets
	// SIGTERM stop the manager at all, and it is half of what makes the leader
	// lease change hands quickly (LeaderElectionReleaseOnCancel below is the
	// other half). Without both, the next pod waits out LeaseDuration before it
	// can start, and for that whole window nothing is converging the data plane —
	// which for this module is the only one there is.
	ctx := ctrl.SetupSignalHandler()
	cfgParams := config.NewConfig()

	baseLog, err := logger.NewLogger(cfgParams.Loglevel)
	if err != nil {
		fmt.Printf("unable to create NewLogger, err: %v\n", err)
		os.Exit(1)
	}
	log := baseLog.Named("main")

	// The same binary, started by the migration Job to copy objects between two
	// Garage clusters. It is here rather than in an image of its own because it
	// needs exactly what the controller already carries — the S3 client — and an
	// image built from a second copy of this repository would be a second thing to
	// keep in step. It talks to no Kubernetes API and returns instead of starting
	// a manager.
	if len(os.Args) > 1 && os.Args[1] == migrator.Command {
		runCopier(ctx, baseLog.Named("copier"))
		return
	}

	// And the same binary again as the data plane's TLS terminator, a sidecar in
	// every Garage pod. Garage serves no TLS of its own; see internal/tlsproxy for
	// why the terminator is this code and not a proxy from somewhere else.
	if len(os.Args) > 1 && os.Args[1] == tlsproxy.Command {
		runTerminator(ctx, baseLog.Named("tls-terminator"))
		return
	}

	log.Info("starting system-s3-controller",
		"version", version, "commit", commit, "buildDate", buildDate,
		"goVersion", goruntime.Version(), "os", goruntime.GOOS, "arch", goruntime.GOARCH)
	log.Info("configuration",
		"controllerNamespace", cfgParams.ControllerNamespace,
		"maxConcurrentReconciles", cfgParams.MaxConcurrentReconciles,
		// A Duration renders as a nanosecond integer in JSON; the string is what an
		// operator reading the start-up line wants.
		"requeueInterval", cfgParams.RequeueInterval.String(),
		"highAvailability", cfgParams.HighAvailability,
		"storageClass", cfgParams.StorageClass)

	// The store description is built once at startup: it is what the module
	// settings say the data plane should be, and it replaces the custom resource
	// the System profile used to carry (see internal/backend). How big it is is
	// not in it — that is the sum of the SystemBucket sizes, which changes while
	// the controller runs and is recomputed on every convergence.
	store := &backend.Store{
		Name:                     backend.StoreName,
		HighAvailability:         cfgParams.HighAvailability,
		HighAvailabilityExplicit: cfgParams.HighAvailabilityExplicit,
		StorageClass:             cfgParams.StorageClass,
	}

	kConfig, err := kubutils.KubernetesDefaultConfigCreate()
	if err != nil {
		log.Error("unable to create the kubernetes config", logger.Err(err))
		os.Exit(1)
	}

	scheme := apiruntime.NewScheme()
	for _, f := range resourcesSchemeFuncs {
		if err := f(scheme); err != nil {
			log.Error("unable to register the scheme", logger.Err(err))
			os.Exit(1)
		}
	}

	// Configured, not yet serving: the listener is opened by the manager below, and a
	// port already in use is reported from mgr.Start.
	log.Info("metrics endpoint configured", "address", cfgParams.MetricsBindAddress+monitoring.MetricsPath)

	managerOpts := manager.Options{
		Scheme:                  scheme,
		HealthProbeBindAddress:  cfgParams.HealthProbeBindAddress,
		Metrics:                 metricsserver.Options{BindAddress: cfgParams.MetricsBindAddress},
		LeaderElection:          true,
		LeaderElectionNamespace: cfgParams.ControllerNamespace,
		LeaderElectionID:        config.DefaultControllerName,

		// Step down on the way out instead of letting the lease expire.
		//
		// The Deployment runs two replicas on an HA control plane, so on every
		// rollout, eviction or node drain the leader is asked to stop while a
		// standby is already up and polling the lease. client-go only writes the
		// "no holder" record on shutdown when ReleaseOnCancel is set; with the
		// default (false) the standby cannot take over until the lease expires —
		// LeaseDuration, 15s by default — and until then nothing reconciles: no
		// SystemBucket is served and the data plane's convergence loop is stopped.
		//
		// The setting is only safe if the process really ends once the manager
		// stops, because a released lease means another replica may already be
		// acting: main() returns straight after mgr.Start, and nothing runs after
		// it.
		LeaderElectionReleaseOnCancel: true,

		// Stop the runnables in ten seconds, not thirty.
		//
		// The release above only happens after every runnable has stopped (or this
		// timeout expires): controller-runtime cancels leader election in a defer
		// that runs after runnables.StopAndWait. The default is 30s, exactly the
		// Pod's default terminationGracePeriodSeconds, so a slow drain — the store
		// loop sitting in a Garage HTTP call, say — would race SIGKILL and the lease
		// would be left held after all. Ten seconds leaves the release a wide
		// margin, and cutting a reconcile short costs nothing here: every pass is
		// idempotent and the next leader starts it over.
		GracefulShutdownTimeout: ptr.To(10 * time.Second),
		Logger:                  baseLog.GetLogger(),
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				// Only the Secrets this module writes itself.
				//
				// The default cache is cluster-wide and this controller watches
				// Secrets — it owns the credentials Secret it puts in a consumer's
				// namespace. Unscoped, that means an informer over every Secret in
				// the cluster: on a DKP with a few thousand of them, most being Helm
				// 3 release state, that is tens to hundreds of megabytes of heap and
				// a full list of all of them on every restart, for a controller whose
				// job is a handful of platform buckets.
				//
				// Everything this module creates carries the label, so everything it
				// reads through the cached client is visible. Reads of Secrets it does
				// not own go through the uncached APIReader, which this does not
				// affect.
				&v1.Secret{}: {
					Label: labels.SelectorFromSet(labels.Set{
						v1alpha1.ManagedByLabel: v1alpha1.ManagedByValue,
					}),
				},

				// ConfigMaps for the same reason, and with more force: the module
				// writes one next to every consumer's SystemBucket (the connection
				// parameters), and a cluster has thousands of ConfigMaps that are
				// none of its business. Everything this module creates carries the
				// label.
				&v1.ConfigMap{}: {
					Label: labels.SelectorFromSet(labels.Set{
						v1alpha1.ManagedByLabel: v1alpha1.ManagedByValue,
					}),
				},

				// Nodes are deliberately NOT cached, and neither are
				// PersistentVolumes.
				//
				// The nodes the data plane may run on are read on one path: the
				// placement reconcile, every 30 seconds and only for a store in the
				// highly available mode. One filtered list there costs less than a
				// watch kept warm for it (see candidateHostnames).
				//
				// PersistentVolumes for the same kind of reason: the module creates
				// none, and the one thing it reads them for — whether a bound volume is
				// tied to a node — is a handful of gets per pass through the APIReader.
				// An informer would be a watch over every PersistentVolume in a cluster
				// that may run sds-local-volume and sds-replicated-volume, which is
				// thousands of objects for three reads.
			},
		},
	}

	mgr, err := manager.New(kConfig, managerOpts)
	if err != nil {
		log.Error("unable to create the manager", logger.Err(err))
		os.Exit(1)
	}
	log.Info("kubernetes manager created")

	driver := garage.New(mgr.GetClient(), mgr.GetAPIReader(), mgr.GetConfig(), baseLog,
		cfgParams.ControllerNamespace, cfgParams.GarageImage, cfgParams.ControllerImage, cfgParams.ClusterDomain).
		WithTLS(cfgParams.GarageTLSSecret, cfgParams.GarageTLSCAFile).
		WithPlacement(garage.Placement{
			NodeSelector: cfgParams.DataPlaneNodeSelector,
			Tolerations:  cfgParams.DataPlaneTolerations,
		}).
		WithVPA(cfgParams.VPAEnabled)

	// The data plane converges from the background loop as well as from every
	// SystemBucket reconcile, so a cluster with no consumer yet still gets its
	// storage built. Leader election keeps the loop to one replica.
	storeManager := controller.NewStoreManager(driver, store, baseLog, cfgParams.RequeueInterval, mgr.GetClient(), mgr.GetAPIReader())
	if err := mgr.Add(storeManager); err != nil {
		log.Error("unable to register the store manager", logger.Err(err))
		os.Exit(1)
	}

	// The credentials endpoint: temporary keys for callers that present a
	// ServiceAccount token and pass a SubjectAccessReview. It is registered only
	// when the chart gave it a certificate — it hands out secret keys, and serving
	// that over plaintext because a mount was missing is not a fallback worth
	// having. The module logs the refusal rather than failing to start: the
	// reconcilers are what the cluster's buckets depend on.
	if cfgParams.STSCertFile != "" && cfgParams.STSKeyFile != "" {
		if err := mgr.Add(&sts.Runnable{
			Addr:     cfgParams.STSBindAddress,
			CertFile: cfgParams.STSCertFile,
			KeyFile:  cfgParams.STSKeyFile,
			Log:      baseLog.Named("sts"),
			Server: &sts.Server{
				Reader:     mgr.GetAPIReader(),
				Issuer:     driver,
				Authorizer: &sts.KubeAuthorizer{Client: mgr.GetClient()},
				Store:      store,
				Log:        baseLog.Named("sts"),
			},
		}); err != nil {
			log.Error("unable to register the credentials endpoint", logger.Err(err))
			os.Exit(1)
		}
	} else {
		log.Warn("the credentials endpoint is not served: no TLS certificate was configured",
			"certFile", cfgParams.STSCertFile, "keyFile", cfgParams.STSKeyFile)
	}

	if err := controller.AddSystemBucketReconcilerToManager(mgr, cfgParams, baseLog, storeManager, driver); err != nil {
		log.Error("unable to register the SystemBucket reconciler", logger.Err(err))
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error("unable to add the healthz check", logger.Err(err))
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error("unable to add the readyz check", logger.Err(err))
		os.Exit(1)
	}

	log.Info("starting the manager")
	if err := mgr.Start(ctx); err != nil {
		log.Error("the manager exited with an error", logger.Err(err))
		os.Exit(1)
	}
}

// runCopier runs one pass of the object copy and exits: a Job that finishes is
// the signal the migration phases wait for, and an exit code is the only part of
// it they read. Everything else goes to the log, where an operator can watch an
// hours-long copy make progress.
func runTerminator(ctx context.Context, log logger.Logger) {
	routes, err := tlsproxy.ParseRoutes(os.Getenv(tlsproxy.EnvRoutes))
	if err != nil {
		log.Error("the TLS terminator cannot start", logger.Err(err))
		os.Exit(1)
	}
	p := &tlsproxy.Proxy{
		Routes:        routes,
		CertFile:      os.Getenv(tlsproxy.EnvCertFile),
		KeyFile:       os.Getenv(tlsproxy.EnvKeyFile),
		Log:           log,
		Metrics:       tlsproxy.NewMetrics(),
		MetricsListen: os.Getenv(tlsproxy.EnvMetricsListen),
		MetricsToken:  os.Getenv(tlsproxy.EnvMetricsToken),
	}
	if err := p.Run(ctx); err != nil {
		log.Error("the TLS terminator stopped", logger.Err(err))
		os.Exit(1)
	}
}

func runCopier(ctx context.Context, log logger.Logger) {
	cfg, err := migrator.ConfigFromEnv()
	if err != nil {
		log.Error("the copier cannot start", logger.Err(err))
		os.Exit(1)
	}
	log.Info("copying objects between two Garage clusters",
		"sourceEndpoint", cfg.Source.Endpoint, "targetEndpoint", cfg.Target.Endpoint,
		"buckets", strings.Join(cfg.Buckets, ","), "workers", cfg.Workers)

	if err := migrator.Run(ctx, cfg, log); err != nil {
		log.Error("the copy did not finish", logger.Err(err))
		if errors.Is(err, migrator.ErrQuotaExceeded) {
			os.Exit(migrator.QuotaExitCode)
		}
		os.Exit(1)
	}
	log.Info("every bucket is copied")
}
