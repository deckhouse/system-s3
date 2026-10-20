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

package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// awaitingDriver is a store with no data plane: no SystemBucket has asked for
// one. It answers every pass the same way and counts the questions the loop
// would have to ask a Garage that does not exist.
type awaitingDriver struct {
	countingDriver
	passes atomic.Int32
	keys   atomic.Int32
	stop   context.CancelFunc
}

func (d *awaitingDriver) EnsureStore(context.Context, *backend.Store) (backend.StoreState, error) {
	if d.passes.Add(1) >= 3 {
		d.stop()
	}
	return backend.StoreState{AwaitingFirstBucket: true, Message: "no SystemBucket asks for the system object storage yet"}, nil
}

func (d *awaitingDriver) TemporaryKeys(context.Context, *backend.Store) ([]backend.TemporaryKey, error) {
	d.keys.Add(1)
	return nil, nil
}

// storeReadySeries counts the store_ready series of the store as Prometheus
// would scrape them.
func storeReadySeries(t *testing.T, store string) int {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range families {
		if f.GetName() != "system_s3_store_ready" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "store" && l.GetValue() == store {
					n++
				}
			}
		}
	}
	return n
}

// TestAStoreWaitingForItsFirstBucketIsNotReportedDown: no store_ready series —
// a 0 there pages (D8SystemS3StoreNotReady) — and no sweeps or key listings,
// which are calls to a Garage admin API that does not exist yet.
func TestAStoreWaitingForItsFirstBucketIsNotReportedDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drv := &awaitingDriver{stop: cancel}
	m := &StoreManager{
		driver:   drv,
		store:    &backend.Store{Name: "first-bucket-test"},
		log:      testLogger(t),
		interval: time.Millisecond,
	}
	// A series left from a data plane that was there before (the namespace was
	// emptied by hand): it has to go, not stay at its last value.
	m.publish(backend.StoreState{Ready: false})
	if got := storeReadySeries(t, m.store.Name); got != 1 {
		t.Fatalf("store_ready series before: %d", got)
	}

	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := storeReadySeries(t, m.store.Name); got != 0 {
		t.Errorf("store_ready has %d series for a store with no data plane", got)
	}
	if got := drv.swept.Load(); got != 0 {
		t.Errorf("expired keys were swept %d times with no data plane", got)
	}
	if got := drv.keys.Load(); got != 0 {
		t.Errorf("temporary keys were listed %d times with no data plane", got)
	}
	if !m.State().AwaitingFirstBucket {
		t.Error("the manager does not hold the waiting state")
	}
}
