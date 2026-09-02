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
	"sync"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/garage"
)

// StoreManager owns the module's single system data plane.
//
// The data plane has no custom resource, so there is no object to reconcile it
// from and no status to publish it on: the desired state comes from the module
// settings (a backend.Store built once at startup) and the observed state is
// cached here for the SystemBucket reconciler to gate on as StorageReady.
//
// It converges the data plane from two directions. Every SystemBucket reconcile
// calls Ensure first, so a consumer arriving before the store is up drives it;
// and Start ticks in the background, so the store is built and kept converged
// even when the cluster holds no SystemBucket at all — which is the normal state
// of a fresh installation, and the one where waiting for a consumer to show up
// would mean the first consumer waits for a whole Garage bootstrap.
//
// Ensure is serialized: the ticker and the reconciler both call it, and the
// driver's reconcile is not safe to run twice concurrently (it pins the
// replication factor, recycles PVCs and rewrites the Garage layout).
type StoreManager struct {
	driver   *garage.Driver
	store    *backend.Store
	log      logger.Logger
	interval time.Duration

	mu    sync.Mutex
	state backend.StoreState

	// lastNotReadyMsg and lastNotReadyAt throttle the "still not ready" log.
	//
	// The store has no custom resource of its own, so on a cluster with no
	// SystemBucket yet — the normal state of a fresh installation, and the reason
	// the background ticker exists — the log is the only place its message can
	// appear. Logging only readiness transitions meant a store that never came up
	// said so at most once and then went quiet every thirty seconds forever.
	//
	// Repeating it every pass would be its own problem, so it repeats when the
	// message changes, and otherwise at most once per notReadyLogInterval.
	lastNotReadyMsg string
	lastNotReadyAt  time.Time

	// now is time.Now, replaced in tests.
	now func() time.Time
}

// notReadyLogInterval is how often a store that is still not ready repeats
// itself when nothing about the reason has changed.
const notReadyLogInterval = 5 * time.Minute

// clock is time.Now unless a test replaced it.
func (m *StoreManager) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

// shouldRepeatNotReady decides whether a store that is still not ready says so
// again: always when the reason changed, otherwise no more often than
// notReadyLogInterval. A changed message is new information and a repeated one is
// a reminder, and the two deserve different rates.
func (m *StoreManager) shouldRepeatNotReady(msg string) bool {
	if msg != m.lastNotReadyMsg {
		return true
	}
	return m.clock().Sub(m.lastNotReadyAt) >= notReadyLogInterval
}

// NewStoreManager builds the manager for the given store description.
func NewStoreManager(driver *garage.Driver, store *backend.Store, log logger.Logger, interval time.Duration) *StoreManager {
	return &StoreManager{
		driver:   driver,
		store:    store,
		log:      log.Named("store-manager"),
		interval: interval,
		state:    backend.StoreState{Message: "the system object storage has not been reconciled yet"},
	}
}

// Store is the desired-state description the driver calls take.
func (m *StoreManager) Store() *backend.Store { return m.store }

// Ensure reconciles the data plane and returns its observed state.
func (m *StoreManager) Ensure(ctx context.Context) (backend.StoreState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.driver.EnsureStore(ctx, m.store)
	if err != nil {
		// Keep the last good state: the caller reports the error, and a failed
		// probe is not evidence that the store went away.
		return m.state, err
	}
	switch {
	case state.Ready != m.state.Ready:
		m.log.Info("the system object storage changed readiness", "ready", state.Ready, "message", state.Message)
		m.lastNotReadyMsg, m.lastNotReadyAt = "", time.Time{}
	case !state.Ready:
		// Not a transition, and not normal either. Without this the only channel
		// carrying this message is the StorageReady condition on a SystemBucket,
		// and on an installation that has none there is nothing to carry it: no
		// object, no event, no metric, and no repeated log line. The first consumer
		// to create a SystemBucket a week later would be the first to see why.
		if m.shouldRepeatNotReady(state.Message) {
			m.log.Warn("the system object storage is still not ready", "message", state.Message)
			m.lastNotReadyMsg, m.lastNotReadyAt = state.Message, m.clock()
		}
	}
	m.state = state
	return state, nil
}

// State returns the last observed state without touching the backend.
func (m *StoreManager) State() backend.StoreState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Start runs the background convergence loop. It satisfies manager.Runnable, so
// the controller-runtime manager starts it after the caches sync and stops it on
// shutdown; leader election keeps it to one replica.
func (m *StoreManager) Start(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		if _, err := m.Ensure(ctx); err != nil {
			// Log and keep ticking: the data plane is reconciled again on the next
			// tick, and returning an error here would take the whole manager down
			// over a transient backend failure.
			m.log.Error("unable to reconcile the system object storage", logger.Err(err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
