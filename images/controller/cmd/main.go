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
	"fmt"
	"log/slog"
	"os"
	goruntime "runtime"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
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
	// SetupSignalHandler, not Background: without a cancellable context SIGTERM
	// does not make the manager release the leader lease. The next pod then waits
	// out LeaseDuration before it can start, and for that whole window nothing is
	// converging the data plane — which for this module is the only one there is.
	ctx := ctrl.SetupSignalHandler()
	cfgParams := config.NewConfig()

	baseLog, err := logger.NewLogger(cfgParams.Loglevel)
	if err != nil {
		fmt.Printf("unable to create NewLogger, err: %v\n", err)
		os.Exit(1)
	}
	log := baseLog.Named("main")

	log.Info("starting system-s3-controller",
		"version", version, "commit", commit, "buildDate", buildDate,
		"goVersion", goruntime.Version(), "os", goruntime.GOOS, "arch", goruntime.GOARCH)
	log.Info("configuration",
		"controllerNamespace", cfgParams.ControllerNamespace,
		"maxConcurrentReconciles", cfgParams.MaxConcurrentReconciles,
		// A Duration renders as a nanosecond integer in JSON; the string is what an
		// operator reading the start-up line wants.
		"requeueInterval", cfgParams.RequeueInterval.String(),
		"singleReplica", cfgParams.SingleReplica,
		"sizePerNode", cfgParams.SizePerNode,
		"storageClass", cfgParams.StorageClass)

	// The store description is built once at startup: it is what the module
	// settings say the data plane should be, and it replaces the custom resource
	// the System profile used to carry (see internal/backend).
	sizePerNode, err := resource.ParseQuantity(cfgParams.SizePerNode)
	if err != nil {
		log.Error("SIZE_PER_NODE is not a Kubernetes quantity", logger.Err(err), slog.String("sizePerNode", cfgParams.SizePerNode))
		os.Exit(1)
	}
	store := &backend.Store{
		Name:          backend.StoreName,
		SingleReplica: cfgParams.SingleReplica,
		SizePerNode:   sizePerNode,
		StorageClass:  cfgParams.StorageClass,
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
		cfgParams.ControllerNamespace, cfgParams.GarageImage, cfgParams.ClusterDomain)

	// The data plane converges from the background loop as well as from every
	// SystemBucket reconcile, so a cluster with no consumer yet still gets its
	// storage built. Leader election keeps the loop to one replica.
	storeManager := controller.NewStoreManager(driver, store, baseLog, cfgParams.RequeueInterval)
	if err := mgr.Add(storeManager); err != nil {
		log.Error("unable to register the store manager", logger.Err(err))
		os.Exit(1)
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
