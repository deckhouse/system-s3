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

// Package monitoring holds the module's own metrics.
//
// Everything else the controller exports comes from controller-runtime
// (reconcile counts and durations, workqueue depth, client-go latency), which
// describes the controller. These two describe the thing the controller runs, and
// the data plane has no custom resource to publish it on: its state is visible
// only as a condition copied onto every SystemBucket, so a cluster with no
// SystemBucket yet has nowhere to show that its system storage never came up.
package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const namespace = "system_s3"

var (
	// storeReady is the readiness of the single system data plane, 1 or 0. Same
	// value as the StorageReady condition, which only exists where a SystemBucket
	// exists.
	storeReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_ready",
		Help:      "Whether the system object storage data plane is up and serving S3 (1) or not (0).",
	}, []string{"store"})

	// storeCapacityBytes is the capacity the backend reports for its assigned
	// layout — the sum of the per-replica capacities Garage actually accepted, not
	// the sizePerNode setting multiplied out. The difference is the point: a
	// replica missing from the layout shows up here as capacity that never
	// arrived.
	storeCapacityBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_capacity_bytes",
		Help:      "Total capacity assigned in the data plane's layout, in bytes.",
	}, []string{"store"})
)

func init() {
	metrics.Registry.MustRegister(storeReady)
	metrics.Registry.MustRegister(storeCapacityBytes)
}

// StoreReady records the readiness of the named store.
func StoreReady(store string, ready bool) {
	v := 0.0
	if ready {
		v = 1
	}
	storeReady.WithLabelValues(store).Set(v)
}

// StoreCapacityBytes records the store's assigned capacity. Capacity is optional
// — a store whose layout is not assigned yet has none to report — and an absent
// value publishes 0 rather than the previous reading, because a stale capacity is
// worse than none: it would keep an alert quiet about a store that has stopped
// answering.
func StoreCapacityBytes(store string, bytes *int64) {
	if bytes == nil {
		storeCapacityBytes.WithLabelValues(store).Set(0)
		return
	}
	storeCapacityBytes.WithLabelValues(store).Set(float64(*bytes))
}
