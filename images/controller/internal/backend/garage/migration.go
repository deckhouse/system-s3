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

package garage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// migrationPhase is where a move between two Garage clusters has got to.
//
// The phases are the states of the machine in design/rework-storageclass-ha-jwt.ru.md
// (§7). Each is idempotent: a controller that restarts mid-phase re-enters it and
// carries on, which is the only way an operation measured in hours survives a
// process whose reconcile budget is measured in minutes.
type migrationPhase string

const (
	// phaseConverged: one data plane, and it is the one the settings ask for.
	// This is the state of an installation that has never migrated.
	phaseConverged migrationPhase = "Converged"
	// phaseProvisioning: the incoming plane is being built and brought up
	// healthy, with its buckets created and its quotas set. Nothing is copied yet
	// and nothing is at stake: the serving plane is untouched.
	phaseProvisioning migrationPhase = "Provisioning"
	// phaseCopying: the bulk copy is running. Consumers keep reading and writing
	// the serving plane, so the copy is behind by whatever was written while it
	// ran; that is what the delta pass in Cutover is for.
	phaseCopying migrationPhase = "Copying"
	// phaseVerifying: the two clusters are compared bucket by bucket. A mismatch
	// goes back to Copying rather than forward.
	phaseVerifying migrationPhase = "Verifying"
	// phaseCutover: the only window in which writes are refused. Write access is
	// revoked on the serving plane, the delta and deletions are synchronized, and
	// the endpoint is switched. Consumers must obtain new temporary credentials.
	phaseCutover migrationPhase = "Cutover"
	// phaseDraining: the incoming plane serves; the old one is kept read-only
	// until the new one is confirmed to hold the data, and not a moment longer.
	phaseDraining migrationPhase = "Draining"
	// phaseDecommission: the plane that no longer serves is being torn down. It
	// is also where an aborted migration ends up — "tear down the plane that is
	// not serving" is the same work whether the migration finished or was called
	// off.
	phaseDecommission migrationPhase = "Decommission"

	// The in-place change of the replica count (see resize.go). There is one
	// data plane throughout: the same cluster is stopped, its layout is purged
	// and it comes back with the other replication factor.
	//
	// phaseResizeFreeze (shrinking only): writes are refused and the cluster is
	// waited on until every node holds everything, so that the replica that
	// stays holds everything.
	phaseResizeFreeze migrationPhase = "ResizeFreeze"
	// phaseResizeStop: the data plane is scaled to zero.
	phaseResizeStop migrationPhase = "ResizeStop"
	// phaseResizePurge: cluster_layout is removed from the volume of the replica
	// that carries the data over, and garage.toml gets the new factor.
	phaseResizePurge migrationPhase = "ResizePurge"
	// phaseResizeSync: the cluster runs with the new factor and a new layout;
	// the replicas are filled up (growing) or the extra volumes are released
	// (shrinking), and writes are given back.
	phaseResizeSync migrationPhase = "ResizeSync"
)

// writesFrozen reports whether writes are refused right now: in the cutover of
// a move, and in an in-place shrink from the freeze until writes are given back.
func (s migrationState) writesFrozen() bool {
	return s.phase == phaseCutover || (s.phase.resizing() && !s.frozenAt.IsZero())
}

// resizing reports whether the phase is one of the in-place change.
func (p migrationPhase) resizing() bool {
	switch p {
	case phaseResizeFreeze, phaseResizeStop, phaseResizePurge, phaseResizeSync:
		return true
	}
	return false
}

// There is deliberately no failed phase.
//
// Nothing a migration runs into is terminal: a class that binds nothing, a
// cluster with no room, a copy that cannot get through are all conditions that
// clear on their own or when an operator acts, and the phase that hit one
// re-enters it on the next pass and says how long it has been waiting. A failed
// state would be a second way to spell "still in Provisioning" that an operator
// would then have to get out of by hand.

// migrationStateKey names the keys of the ConfigMap the phase is kept in.
const (
	migrationKeyServing    = "serving"
	migrationKeyOther      = "other-plane"
	migrationKeyPhase      = "phase"
	migrationKeyOtherHA    = "other-high-availability"
	migrationKeyOtherClass = "other-storage-class"
	migrationKeyStartedAt  = "started-at"
	migrationKeyFrozenAt   = "frozen-at"
	migrationKeyCutoverAt  = "cutover-at"
	// Of the in-place change: the keys whose write access the freeze took away,
	// to be given it back on the same cluster, and when the block repair was
	// launched.
	migrationKeyFrozenKeys       = "frozen-keys"
	migrationKeyRepairAt         = "repair-launched-at"
	migrationKeyWriteFence       = "write-fence"
	migrationKeyFencePods        = "write-fence-pods"
	migrationKeyEndpointRecorded = "endpoint-recorded"
)

const (
	writeFenceStopping   = "Stopping"
	writeFenceRolling    = "Rolling"
	writeFenceRestarting = "Restarting"
	writeFenceReady      = "Ready"
)

// migrationStateName is the ConfigMap the phase lives in. It belongs to the
// store, not to either plane (see stableName): it is the one object that
// outlives both.
func migrationStateName(store *backend.Store) string {
	return stableName(store) + "-migration"
}

// migrationState is which plane serves, which one is being built, and how far
// the move between them has got.
//
// The target shape (ha, storageClass) is recorded rather than re-read from the
// settings on every pass. A migration takes hours, the settings can be edited
// while it runs, and a plane that changed shape underneath a copy would have to
// be thrown away and rebuilt — so the shape is fixed when the migration starts,
// and an edit made mid-flight is picked up by the NEXT migration, once this one
// has converged.
type migrationState struct {
	// serving is the plane behind the endpoint Service: the one consumers reach.
	serving int32
	// other is the plane that is not serving, or 0 when there is only one. Which
	// one that is changes at the cutover and nowhere else: before it, the plane
	// being filled; after it, the plane being retired. The two swap places in a
	// single write, which is also the write that moves the endpoint — so there is
	// no moment where the state says one thing and the Service another.
	other int32
	phase migrationPhase
	// ha and storageClass are the shape the incoming plane is built for.
	ha           bool
	storageClass string
	startedAt    time.Time
	// frozenAt is when writes were refused, i.e. when the cutover began. It is
	// what bounds the freeze: a cutover that cannot finish gives the writes back
	// rather than holding them until somebody notices.
	frozenAt time.Time
	// cutoverAt is when the endpoint moved.
	cutoverAt time.Time
	// frozenKeys are the bucket keys whose write access an in-place change took
	// away. A cutover never gives write access back — the endpoint moves to a
	// cluster where the keys do not exist — but an in-place change stays on the
	// same cluster, and has to give it back to exactly the keys that had it.
	frozenKeys []frozenKey
	// repairAt is when the in-place growth launched the block repair.
	repairAt   time.Time
	writeFence string
	// fencePods are the UIDs of the source pods that ran when writes were
	// frozen, for the rolling fence of a highly available source.
	fencePods []string
	// recorded controls whether the endpoint selector names a plane. A ConfigMap
	// created only for write tickets preserves a legacy wide selector; an actual
	// migration state write starts naming the plane (see endpointSelector).
	recorded bool
}

// migrating reports whether the store has two data planes: a move is in flight,
// or one has just finished and the old cluster is still there.
func (s migrationState) migrating() bool {
	return s.phase != phaseConverged && s.other != 0
}

// loadMigrationState reads the phase, or reports the converged state of an
// installation that has never migrated.
//
// When the ConfigMap is absent the serving plane is read from the endpoint
// Service's own selector instead of assumed to be the first. The Service is the
// definition of "what serves" rather than a record of it — whatever it selects is
// what consumers reach — so a lost ConfigMap cannot point the module at a plane
// that is not the one answering.
func (d *Driver) loadMigrationState(ctx context.Context, store *backend.Store) (migrationState, error) {
	cm := &corev1.ConfigMap{}
	err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: migrationStateName(store)}, cm)
	switch {
	case apierrors.IsNotFound(err):
		serving, named, err := d.servingFromEndpoint(ctx, store)
		if err != nil {
			return migrationState{}, err
		}
		// A Service whose selector already names a plane says this store has
		// migrated before, whatever happened to the ConfigMap. Reading that as "it
		// never did" would widen the selector again, and a namespace that still
		// holds a second cluster would have both of them behind one endpoint.
		return migrationState{serving: serving, phase: phaseConverged, recorded: named}, nil
	case err != nil:
		return migrationState{}, err
	}
	state := parseMigrationState(cm.Data)
	state.recorded = cm.Data[migrationKeyEndpointRecorded] != "false"
	return state, nil
}

// servingFromEndpoint reads which plane the endpoint Service selects. A Service
// that does not exist yet, or one whose selector predates the incarnation label,
// is the first plane — that is the installation this label was added to.
func (d *Driver) servingFromEndpoint(ctx context.Context, store *backend.Store) (serving int32, named bool, err error) {
	svc := &corev1.Service{}
	err = d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: s3SvcName(store)}, svc)
	switch {
	case apierrors.IsNotFound(err):
		return firstIncarnation, false, nil
	case err != nil:
		return 0, false, err
	}
	value, named := svc.Spec.Selector[incarnationLabel]
	return parseIncarnation(value), named, nil
}

// parseIncarnation reads an incarnation written as a label or a ConfigMap value,
// with anything absent or unreadable meaning the first one.
func parseIncarnation(value string) int32 {
	n, err := strconv.Atoi(value)
	if err != nil || n < int(firstIncarnation) {
		return firstIncarnation
	}
	return int32(n)
}

// parseMigrationState reads the state out of the ConfigMap data. Anything
// missing or unreadable reads as the converged state of the first plane: a
// half-written ConfigMap must not start a migration by accident, and the phases
// re-derive whether a move is needed from the settings on the next pass anyway.
func parseMigrationState(data map[string]string) migrationState {
	state := migrationState{
		serving:      parseIncarnation(data[migrationKeyServing]),
		phase:        migrationPhase(data[migrationKeyPhase]),
		ha:           data[migrationKeyOtherHA] == "true",
		storageClass: data[migrationKeyOtherClass],
		startedAt:    parseTime(data[migrationKeyStartedAt]),
		frozenAt:     parseTime(data[migrationKeyFrozenAt]),
		cutoverAt:    parseTime(data[migrationKeyCutoverAt]),
		repairAt:     parseTime(data[migrationKeyRepairAt]),
		frozenKeys:   parseFrozenKeys(data[migrationKeyFrozenKeys]),
		writeFence:   data[migrationKeyWriteFence],
		fencePods:    parseList(data[migrationKeyFencePods]),
	}
	// Any real plane, the first one included. It is the first one exactly half the
	// time: before a cutover the other plane is the one being built, after it the
	// one being retired, and a store on its first migration retires plane 1.
	//
	// Reading 1 as "no other plane" is what a first version of this did, and it
	// cost the whole drain: the state written by the cutover read back as
	// converged, the phase never reached the decommission, and the cluster that
	// was supposed to be removed after the switch was kept forever.
	if n, err := strconv.Atoi(data[migrationKeyOther]); err == nil && n >= int(firstIncarnation) {
		state.other = int32(n)
	}
	if !knownPhase(state.phase) {
		state.phase = phaseConverged
	}
	if state.other == state.serving {
		// "The other plane is this plane" is not a state, it is a corruption. Acting
		// on it would have a phase provision, copy into, or decommission the cluster
		// that is serving.
		state.other = 0
	}
	if state.phase.resizing() {
		// The in-place change has one plane and no other: it is the one kind of
		// phase that is not a move between two.
		state.other = 0
		return state
	}
	if state.phase != phaseConverged && state.other == 0 {
		// A phase with no plane to move to is not a phase. Reading it as converged
		// leaves the serving plane serving and lets the next pass start the
		// migration properly, rather than driving a machine over a plane that was
		// never named.
		state.phase = phaseConverged
	}
	return state
}

// knownPhase guards against a value written by a newer release (a downgrade) or
// by hand.
func knownPhase(p migrationPhase) bool {
	switch p {
	case phaseConverged, phaseProvisioning, phaseCopying, phaseVerifying,
		phaseCutover, phaseDraining, phaseDecommission,
		phaseResizeFreeze, phaseResizeStop, phaseResizePurge, phaseResizeSync:
		return true
	}
	return false
}

// frozenKey is one key's write access to one bucket, taken away by a freeze.
type frozenKey struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func parseFrozenKeys(value string) []frozenKey {
	if value == "" {
		return nil
	}
	var keys []frozenKey
	if err := json.Unmarshal([]byte(value), &keys); err != nil {
		// Unreadable means "none recorded": a key left without write access
		// gets it back from its next credentials request (a fresh key), which
		// is what a consumer does anyway when a key stops working.
		return nil
	}
	return keys
}

func formatFrozenKeys(keys []frozenKey) string {
	raw, err := json.Marshal(keys)
	if err != nil {
		return ""
	}
	return string(raw)
}

func parseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return t
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// saveMigrationState serializes phase changes with grants from all STS replicas.
func (d *Driver) saveMigrationState(ctx context.Context, store *backend.Store, state migrationState) error {
	data := map[string]string{
		migrationKeyServing: strconv.FormatInt(int64(state.serving), 10),
		migrationKeyPhase:   string(state.phase),
	}
	if state.other != 0 || state.phase.resizing() {
		if state.other != 0 {
			data[migrationKeyOther] = strconv.FormatInt(int64(state.other), 10)
		}
		data[migrationKeyOtherHA] = strconv.FormatBool(state.ha)
		data[migrationKeyOtherClass] = state.storageClass
	}
	if len(state.frozenKeys) > 0 {
		data[migrationKeyFrozenKeys] = formatFrozenKeys(state.frozenKeys)
	}
	if s := formatTime(state.repairAt); s != "" {
		data[migrationKeyRepairAt] = s
	}
	if s := formatTime(state.startedAt); s != "" {
		data[migrationKeyStartedAt] = s
	}
	if s := formatTime(state.frozenAt); s != "" {
		data[migrationKeyFrozenAt] = s
	}
	if s := formatTime(state.cutoverAt); s != "" {
		data[migrationKeyCutoverAt] = s
	}
	if state.writeFence != "" {
		data[migrationKeyWriteFence] = state.writeFence
	}
	if len(state.fencePods) > 0 {
		data[migrationKeyFencePods] = strings.Join(state.fencePods, ",")
	}
	if err := d.updateGrantState(ctx, store, func(cm *corev1.ConfigMap) error {
		next := make(map[string]string, len(data)+len(cm.Data))
		for key, value := range data {
			next[key] = value
		}
		for key, value := range cm.Data {
			if strings.HasPrefix(key, pendingWriteGrantPrefix) {
				next[key] = value
			}
		}
		cm.Data = next
		return nil
	}); err != nil {
		return fmt.Errorf("write the migration state: %w", err)
	}
	return nil
}

func (d *Driver) updateGrantState(ctx context.Context, store *backend.Store, change func(*corev1.ConfigMap) error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: migrationStateName(store)}, cm)
		missing := apierrors.IsNotFound(err)
		if err != nil && !missing {
			return err
		}
		if missing {
			serving, named, err := d.servingFromEndpoint(ctx, store)
			if err != nil {
				return err
			}
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: migrationStateName(store), Namespace: d.namespace, Labels: commonLabels(store)},
				Data: map[string]string{migrationKeyServing: strconv.FormatInt(int64(serving), 10), migrationKeyPhase: string(phaseConverged), migrationKeyEndpointRecorded: strconv.FormatBool(named)}}
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		if err := change(cm); err != nil {
			return err
		}
		if missing {
			err := d.client.Create(ctx, cm)
			if apierrors.IsAlreadyExists(err) {
				return apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, err)
			}
			return err
		}
		return d.client.Update(ctx, cm)
	})
}

// planeOf returns the store description of one of its data planes: the same
// store, at that incarnation and in that plane's shape.
//
// Every builder in this package takes a store and reads the plane out of it, so
// reconciling the incoming plane is the same code as reconciling the serving one,
// called with a different description.
func planeOf(store *backend.Store, incarnation int32, ha bool, storageClass string) *backend.Store {
	plane := *store
	plane.Incarnation = incarnation
	plane.HighAvailability = ha
	plane.StorageClass = storageClass
	return &plane
}

// endpointSelector is what the endpoint Service points at.
//
// A store that has never migrated keeps the selector it already has: the store's
// labels, with no plane in them. That is not laziness — it is what is on the
// Service of every installation upgraded into this code, and narrowing a live
// selector to a label the pods only grow as they roll would empty the endpoint
// for the length of a rollout, on an upgrade that changes nothing else.
//
// From the first migration on it names a plane, and it keeps naming one
// afterwards. The obvious alternative — widen it again once the migration has
// converged — has a hole in it: the pods of the cluster being torn down still
// carry the store's labels while they terminate, so a wide selector would send
// reads to a cluster that is on its way out, for as long as it takes them to go.
func endpointSelector(store *backend.Store, state migrationState) map[string]string {
	if !state.recorded {
		return commonLabels(store)
	}
	return planeLabels(planeOf(store, state.serving, store.HighAvailability, store.StorageClass))
}

// servingPlane resolves a store description that does not say which data plane
// it means into the one that is serving.
//
// Every entry point of the driver starts here. The store the controller holds is
// built from the module settings and names no incarnation, because the settings
// do not have one: which Garage cluster a bucket is created on, a key is issued
// on or an object is deleted from is a fact about the cluster, not about the
// settings, and for the hours of a migration the two planes answer differently.
//
// The read is uncached on purpose. It costs one GET per call and it is what
// stands between the cutover and a pass that acts on the cluster that stopped
// serving a second ago.
func (d *Driver) servingPlane(ctx context.Context, store *backend.Store) (*backend.Store, migrationState, error) {
	state, err := d.loadMigrationState(ctx, store)
	if err != nil {
		return nil, migrationState{}, fmt.Errorf("read the migration state: %w", err)
	}
	return planeOf(store, state.serving, store.HighAvailability, store.StorageClass), state, nil
}

// reportedMigration is the migration as the outside should see it: the phase and
// when it started, or nothing at all when the store has one data plane.
//
// The state read at the START of the pass is what is reported, not the one the
// pass produced. What a metric should say is where the store was while this pass
// was working on it; a phase that changed in the last second of the pass is the
// next pass's news.
func reportedMigration(state migrationState) (string, time.Time) {
	if !state.migrating() && !state.phase.resizing() {
		return "", time.Time{}
	}
	return string(state.phase), state.startedAt
}

func parseList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
