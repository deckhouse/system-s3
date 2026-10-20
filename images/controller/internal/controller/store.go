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
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/garage"
	"github.com/deckhouse/system-s3/images/controller/internal/monitoring"
)

// storeDriver is the one driver call the manager makes. Narrowed to an interface
// so the collapsing in Ensure can be tested without a Kubernetes API and a Garage
// admin endpoint behind it; *garage.Driver is the only implementation.
type storeDriver interface {
	EnsureStore(ctx context.Context, store *backend.Store) (backend.StoreState, error)

	// SweepExpiredTemporaryAccess removes the key records the credentials
	// endpoint left behind. It is here rather than on a timer of its own because
	// this loop is already the module's "every so often, on one replica" place,
	// and a second one would be a second thing to reason about.
	SweepExpiredTemporaryAccess(ctx context.Context, store *backend.Store) (int, error)

	// SweepIncompleteUploads removes the parts of multipart uploads that were
	// started and never finished. Garage keeps them for good otherwise, and they
	// occupy the volume without appearing in any listing or counting against any
	// quota.
	SweepIncompleteUploads(ctx context.Context, store *backend.Store) (int, error)

	// TemporaryKeys lists the unexpired temporary keys, for the metric that is
	// the only place outside Garage they can be seen.
	TemporaryKeys(ctx context.Context, store *backend.Store) ([]backend.TemporaryKey, error)

	// DataPlanes lists the Garage clusters of the store with their pods, for the
	// metrics the console draws the data plane from.
	DataPlanes(ctx context.Context, store *backend.Store) ([]backend.DataPlane, error)
}

// StoreManager owns the module's single system data plane.
//
// Desired state comes from module settings (a backend.Store built at startup).
// Observed conditions are published on Module/system-s3, including when no
// SystemBucket exists. Bucket reconciles use the cached readiness as a dependency.
//
// It converges the data plane from two directions. Every SystemBucket reconcile
// calls Ensure first, so the first consumer builds the data plane without
// waiting for the background tick; and Start ticks in the background, so the
// store is kept converged when no SystemBucket happens to be reconciling — and
// kept at all once the last one is deleted. Nothing is built before the first
// SystemBucket exists (see garage/first_bucket.go); the tick then only says so
// on Module/system-s3.
//
// Ensure is serialized: the ticker and the reconciler both call it, and the
// driver's reconcile is not safe to run twice concurrently (it pins the
// replication factor, recycles PVCs and rewrites the Garage layout).
type StoreManager struct {
	driver       storeDriver
	store        *backend.Store
	log          logger.Logger
	interval     time.Duration
	moduleStatus *moduleStatusPublisher

	mu    sync.Mutex
	state backend.StoreState

	// flight collapses concurrent Ensure calls instead of queueing them.
	//
	// Serialising convergence is right — EnsureStore pins the replication factor,
	// recycles PVCs and rewrites the layout, and two of those at once is not a
	// thing to allow. Queueing it was the problem. A reconcile waits on a plain
	// mutex, which does not see the context deadline, so a reconcile with a
	// 300-second timeout queued behind a convergence that takes ten minutes waits
	// the full ten: the timeout only fires once the lock is acquired, which is
	// precisely the case withReconcileTimeout claims to protect against.
	//
	// And N reconciles produced N full convergences in sequence, each listing PVs,
	// Nodes and Pods and calling the admin API, when all N wanted the same answer.
	// Raising MAX_CONCURRENT_RECONCILES — an operation that looks harmless and
	// usually speeds a controller up — made the work quadratic instead.
	//
	// singleflight gives the same guarantee with the opposite shape: the first
	// caller converges, everyone who arrives while it runs gets that result.
	flight singleflight.Group

	// lastNotReadyMsg and lastNotReadyAt throttle the "still not ready" log.
	//
	// The store has no custom resource of its own, so once the last SystemBucket
	// is deleted the log is the only place besides Module/system-s3 its message
	// can appear. Logging only readiness transitions meant a store that never came
	// up said so at most once and then went quiet every thirty seconds forever.
	//
	// Repeating it every pass would be its own problem, so it repeats when the
	// message changes, and otherwise at most once per notReadyLogInterval.
	lastNotReadyMsg string
	lastNotReadyAt  time.Time

	// lastSweep is when the expired temporary keys were last cleaned up. Touched
	// only from Start's goroutine.
	lastSweep time.Time

	// lastUploadSweep is when the unfinished uploads were last cleaned up, and
	// uploadSweeping is set while that runs. lastUploadSweep is touched only from
	// Start's goroutine; the sweep itself runs in one of its own.
	lastUploadSweep time.Time
	uploadSweeping  atomic.Bool

	// lastKeys is when the list of temporary keys was last published. Touched
	// only from Start's goroutine.
	lastKeys time.Time

	// certReader, certNamespace and certSecrets are where the certificates
	// whose expiry is published are read from, and lastCerts when that was last
	// done (see publishCertificates). lastCerts is touched only from Start's
	// goroutine.
	certReader    client.Reader
	certNamespace string
	certSecrets   []string
	lastCerts     time.Time

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
func NewStoreManager(driver *garage.Driver, store *backend.Store, log logger.Logger, interval time.Duration, c client.Client, reader client.Reader) *StoreManager {
	return &StoreManager{
		driver:       driver,
		store:        store,
		log:          log.Named("store-manager"),
		interval:     interval,
		moduleStatus: &moduleStatusPublisher{client: c, reader: reader},
		state:        backend.StoreState{Message: "the system object storage has not been reconciled yet"},
	}
}

// Store is the desired-state description the driver calls take.
func (m *StoreManager) Store() *backend.Store { return m.store }

// Ensure reconciles the data plane and returns its observed state.
func (m *StoreManager) Ensure(ctx context.Context) (backend.StoreState, error) {
	// One convergence at a time, and everyone waiting shares its result rather
	// than queueing for their own. See the flight field.
	res, err, _ := m.flight.Do("ensure", func() (interface{}, error) {
		return m.ensureOnce(ctx)
	})
	state, _ := res.(backend.StoreState)
	return state, err
}

// ensureOnce is one convergence. Only ever entered by a single caller at a time,
// through flight.
func (m *StoreManager) ensureOnce(ctx context.Context) (backend.StoreState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.driver.EnsureStore(ctx, m.store)
	// Before the error check: a pass that failed half way still leaves pods that
	// are ready or not, and which ones is what an operator looking into the
	// failure wants to see.
	m.publishDataPlanes(ctx)
	// Exported before the Module is patched and whether or not that works: the
	// patch is a write to the API server that can fail on its own, and an alert
	// on the conditions should not go quiet because the status could not be
	// written.
	publishConditions(state)
	if m.moduleStatus != nil {
		if publishErr := m.moduleStatus.Publish(ctx, state); publishErr != nil {
			// A reporting failure must not turn functioning S3 into a dependency
			// failure. The background loop retries publication on the next pass.
			m.log.Error("unable to publish Module/system-s3 conditions", logger.Err(publishErr))
		}
	}
	if err != nil {
		// Keep the last good state: the caller reports the error, and a failed
		// probe is not evidence that the store went away.
		return m.state, err
	}
	switch {
	case state.Ready != m.state.Ready:
		m.log.Info("the system object storage changed readiness", "ready", state.Ready, "message", state.Message)
		m.lastNotReadyMsg, m.lastNotReadyAt = "", time.Time{}
	case !state.Ready && !state.AwaitingFirstBucket:
		// Keep a throttled reminder alongside Module conditions and metrics. Not
		// for a store nobody has asked for yet: that is how most clusters stay, and
		// a warning every five minutes for it would be noise on all of them.
		if m.shouldRepeatNotReady(state.Message) {
			m.log.Warn("the system object storage is still not ready", "message", state.Message)
			m.lastNotReadyMsg, m.lastNotReadyAt = state.Message, m.clock()
		}
	}
	m.state = state
	m.publish(state)
	return state, nil
}

// publish mirrors the observed state onto the module's metrics.
//
// These gauges complement Module conditions and exist independently of buckets.
//
// The capacity is also the only consumer of what the driver computes from the
// assigned layout; before this it was calculated on every pass and dropped.
func (m *StoreManager) publish(state backend.StoreState) {
	monitoring.Migration(m.store.Name, state.MigrationPhase, state.MigrationStartedAt)
	// A store with no data plane has no readiness and no capacity to report, and
	// a 0 in store_ready would page about it (D8SystemS3StoreNotReady). The series
	// appear with the data plane.
	if state.AwaitingFirstBucket {
		monitoring.ForgetStore(m.store.Name)
		return
	}
	monitoring.StoreReady(m.store.Name, state.Ready)

	var capacity *int64
	if state.TotalCapacity != nil {
		v := state.TotalCapacity.Value()
		capacity = &v
	}
	monitoring.StoreCapacityBytes(m.store.Name, capacity)
}

// publishConditions mirrors the conditions written on Module/system-s3 onto the
// condition metric.
func publishConditions(state backend.StoreState) {
	conds := moduleConditions(state)
	series := make([]monitoring.Condition, 0, len(conds))
	for _, c := range conds {
		series = append(series, monitoring.Condition{Type: c.Type, Status: string(c.Status), Reason: c.Reason})
	}
	monitoring.ModuleConditions(series)
}

// publishDataPlanes refreshes replica_ready, store_info and plane_info. A failed
// read keeps the previous series rather than clearing them, for the same reason
// as publishTemporaryKeys: no replicas is a claim, not an absence of one.
//
// It runs on every pass rather than on a slower clock of its own. The pass is
// what the background loop does every REQUEUE_INTERVAL_SECONDS (30 by default),
// which bounds how stale a pod's readiness can be, and nothing here watches pods
// to do better: a readiness flip is not something the controller acts on.
func (m *StoreManager) publishDataPlanes(ctx context.Context) {
	planes, err := m.driver.DataPlanes(ctx, m.store)
	if err != nil {
		m.log.Error("unable to read the data planes for the metrics", logger.Err(err))
		return
	}
	series := make([]monitoring.DataPlane, 0, len(planes))
	for _, p := range planes {
		pods := make([]monitoring.DataPlanePod, 0, len(p.Pods))
		for _, pod := range p.Pods {
			pods = append(pods, monitoring.DataPlanePod{Name: pod.Name, Node: pod.Node, Ready: pod.Ready})
		}
		series = append(series, monitoring.DataPlane{
			Incarnation: p.Incarnation, Serving: p.Serving, StorageClass: p.StorageClass,
			HighAvailability: p.HighAvailability, Replicas: p.Replicas, Pods: pods,
		})
	}
	monitoring.DataPlanes(series)
}

// sweepInterval is how often the expired temporary keys are cleaned up. They are
// already powerless — Garage stopped honouring them on its own clock — so this is
// housekeeping, and doing it on every 30-second tick would be a ListKeys per tick
// for nothing.
const sweepInterval = 10 * time.Minute

// sweep removes the expired temporary keys, no more often than sweepInterval.
func (m *StoreManager) sweep(ctx context.Context) {
	now := m.clock()
	if !m.lastSweep.IsZero() && now.Sub(m.lastSweep) < sweepInterval {
		return
	}
	m.lastSweep = now

	swept, err := m.driver.SweepExpiredTemporaryAccess(ctx, m.store)
	if err != nil {
		m.log.Error("unable to sweep expired temporary access keys", logger.Err(err))
	} else if swept > 0 {
		m.log.Info("removed expired temporary access keys", "count", swept)
	}

	// Not gated on the first sweep succeeding: the two clean up different things,
	// and an admin API that failed to list keys is no reason to leave a day's worth
	// of abandoned upload parts on the volume.
	m.sweepUploads(ctx, now)
}

// uploadSweepInterval is how often the unfinished uploads are cleaned up.
//
// Only uploads older than a day are removed, so a sweep every few hours loses
// nothing. And a sweep is not cheap: Garage looks for unfinished uploads by
// walking the whole bucket in one read of its metadata, and in a bucket with
// millions of deleted objects that read lasts minutes. Run every ten minutes, it
// outlived the admin client's timeout and was started again while Garage still
// ran the previous one — and a long read keeps LMDB from reusing the pages that
// writes free, so the metadata file grew until it filled the volume.
const uploadSweepInterval = 6 * time.Hour

// sweepUploads starts a sweep of the unfinished uploads, no more often than
// uploadSweepInterval and never while the previous one runs. It does not wait:
// the sweep can take as long as Garage needs, and the loop that converges the
// store is not to wait with it.
func (m *StoreManager) sweepUploads(ctx context.Context, now time.Time) {
	if !m.lastUploadSweep.IsZero() && now.Sub(m.lastUploadSweep) < uploadSweepInterval {
		return
	}
	if !m.uploadSweeping.CompareAndSwap(false, true) {
		return
	}
	m.lastUploadSweep = now
	go func() {
		defer m.uploadSweeping.Store(false)
		uploads, err := m.driver.SweepIncompleteUploads(ctx, m.store)
		if err != nil {
			m.log.Error("unable to sweep unfinished uploads", logger.Err(err))
			return
		}
		if uploads > 0 {
			m.log.Info("removed unfinished multipart uploads", "count", uploads)
		}
	}()
}

// keysInterval is how often the list of unexpired temporary keys is published.
// A key lives 15 to 30 minutes, so a list that lags by more than a minute is a
// list of the wrong keys.
const keysInterval = time.Minute

// publishTemporaryKeys refreshes temporary_key_expiration_seconds, no more often
// than keysInterval. A failed read keeps the previous list rather than clearing
// it: an empty list would claim nobody holds access.
func (m *StoreManager) publishTemporaryKeys(ctx context.Context) {
	now := m.clock()
	if !m.lastKeys.IsZero() && now.Sub(m.lastKeys) < keysInterval {
		return
	}
	m.lastKeys = now

	keys, err := m.driver.TemporaryKeys(ctx, m.store)
	if err != nil {
		m.log.Error("unable to list temporary access keys", logger.Err(err))
		return
	}
	series := make([]monitoring.TemporaryKey, 0, len(keys))
	for _, k := range keys {
		series = append(series, monitoring.TemporaryKey{
			Bucket: k.Bucket, AccessKeyID: k.AccessKeyID, Subject: k.Subject, Write: k.Write, ExpiresAt: k.ExpiresAt,
		})
	}
	monitoring.TemporaryKeys(series)
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
		state, err := m.Ensure(ctx)
		if err != nil {
			// Log and keep ticking: the data plane is reconciled again on the next
			// tick, and returning an error here would take the whole manager down
			// over a transient backend failure.
			m.log.Error("unable to reconcile the system object storage", logger.Err(err))
		}
		// The sweeps and the key list are questions to Garage's admin API, and
		// before the first SystemBucket there is no Garage to ask: every tick
		// would log a failed call to a Service that does not exist.
		if !state.AwaitingFirstBucket {
			m.sweep(ctx)
			m.publishTemporaryKeys(ctx)
		}
		m.publishCertificates(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
