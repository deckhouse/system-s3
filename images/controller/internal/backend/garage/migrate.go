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
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// The tolerance the pre-cutover comparison accepts before it freezes writes.
//
// The check before the cutover is not a proof that the two clusters are equal —
// they cannot be, because consumers are still writing to one of them. It is a
// bound on how much is left to copy while writes are refused. Equality is proved
// AFTER the freeze, inside the cutover, where it is meaningful.
const (
	cutoverObjectTolerance = 1000
	cutoverByteTolerance   = 1 << 30 // 1 GiB
)

// cutoverDeadline bounds the window in which consumers cannot write.
//
// The cutover refuses writes and then copies what is left. If that cannot be
// finished — the copier will not run, a cluster stops answering — the writes are
// given back and the migration returns to copying, to try the switch again later.
// Holding a platform's system storage read-only indefinitely because a phase got
// stuck is a worse outcome than a migration that takes another day.
const cutoverDeadline = 30 * time.Minute

// migrationPlan is what the settings ask of the store that runs.
//
// reason is empty when they ask for nothing. start is false for the one change
// the module will not make on its own — see migrationTrigger.
type migrationPlan struct {
	reason string
	start  bool
	// inPlace: the difference is the mode alone, made on the same cluster by
	// restarting it with the other replication factor (see resize.go).
	inPlace bool
}

// migrationTrigger says whether the running plane still matches the settings,
// and whether the difference is one to act on.
//
// The two differences it can find are the same kind of fact: something about the
// data plane that cannot be changed while it runs. Garage refuses a
// replication_factor change on a live cluster, and a bound claim cannot be
// re-ordered from another class. The mode alone is changed on the same cluster,
// restarted with the other factor; a class is a migration to a second one.
//
// One of them is not acted on by itself, and it is the reason this returns a
// decision rather than a string. The high-availability mode is DERIVED when
// nobody sets it — from whether the control plane has more than one master — so
// losing masters flips it to false on its own. Read literally that says "move the
// system storage to a single replica", which is a loss of redundancy nobody asked
// for, arrived at exactly when the cluster is least able to afford it. So a store
// only ever shrinks when the setting says so out loud.
//
// The other direction is not guarded. Masters coming back is good news, the store
// gains a copy, and the cost is a restart that runs while everything is healthy.
func migrationTrigger(desired, serving *backend.Store, boundClass string) migrationPlan {
	switch {
	case desired.HighAvailability != serving.HighAvailability:
		from, to := "one replica", "three replicas"
		if serving.HighAvailability {
			from, to = to, from
		}
		reason := fmt.Sprintf("the high-availability mode changed (%s -> %s)", from, to)

		shrinking := serving.HighAvailability && !desired.HighAvailability
		if shrinking && !desired.HighAvailabilityExplicit {
			return migrationPlan{
				reason: "the control plane is no longer highly available, so the platform's mode now reads as off — " +
					"but the store keeps its three replicas: dropping to one is a loss of redundancy, and losing masters " +
					"is not a request for it. Set highAvailability: false in the module settings to move it to a single replica",
				start: false,
			}
		}
		return migrationPlan{reason: reason, start: true, inPlace: resizeInPlace(desired, serving, boundClass)}
	case boundClass != "" && desired.StorageClass != "" && boundClass != desired.StorageClass:
		return migrationPlan{
			reason: fmt.Sprintf("the StorageClass changed (%q -> %q)", boundClass, desired.StorageClass),
			start:  true,
		}
	default:
		return migrationPlan{}
	}
}

// reconcileMigration advances the move between two Garage clusters by one step
// and returns what to report.
//
// It is called at the END of a pass that found the serving plane healthy, and
// that is the whole scheduling policy: a migration is never started, advanced or
// cut over while the cluster it is moving away from is not in a state to be moved
// away from. A phase does one step per pass and says where it is; the next
// reconcile picks it up. Nothing here blocks, and nothing here takes longer than
// a reconcile budget — the copy itself is a Job (see migrate_job.go).
func (d *Driver) reconcileMigration(
	ctx context.Context,
	desired, serving *backend.Store,
	boundClass string,
	state migrationState,
	sumBytes int64,
) (migrationState, string, error) {
	switch state.phase {
	case phaseConverged:
		return d.migrationStart(ctx, desired, serving, boundClass, state)
	case phaseProvisioning:
		return d.migrationProvision(ctx, desired, serving, state, sumBytes)
	case phaseCopying:
		return d.migrationCopy(ctx, desired, serving, state)
	case phaseVerifying:
		return d.migrationVerify(ctx, desired, serving, state)
	case phaseCutover:
		return d.migrationCutover(ctx, serving, state)
	case phaseDraining:
		return d.migrationDrain(ctx, serving, state)
	case phaseDecommission:
		return d.migrationDecommission(ctx, serving, state)
	case phaseResizeFreeze:
		return d.resizeFreeze(ctx, desired, serving, state)
	case phaseResizeSync:
		return d.resizeSync(ctx, serving, state)
	default:
		// ResizeStop and ResizePurge run before the workload is built (see
		// advanceResizeOffline); a healthy pass never finds them.
		return state, "", nil
	}
}

// migrationStart begins a move when the running plane no longer matches the
// settings.
func (d *Driver) migrationStart(
	ctx context.Context,
	desired, serving *backend.Store,
	boundClass string,
	state migrationState,
) (migrationState, string, error) {
	plan := migrationTrigger(desired, serving, boundClass)
	if plan.reason == "" {
		return state, "", nil
	}
	if !plan.start {
		// Reported on every pass and never acted on: the settings and the store
		// disagree, and the disagreement is one only an operator can resolve.
		return state, plan.reason, nil
	}
	reason := plan.reason
	if plan.inPlace {
		// The mode alone: the same cluster, restarted with the other factor.
		// No second cluster, no copy (see resize.go).
		return d.startResize(ctx, desired, serving, reason)
	}
	if d.copierImage == "" {
		// Said rather than started. A migration that provisioned a second cluster
		// and then discovered it had nothing to copy with would have doubled the
		// footprint to reach the same place this message reaches for free.
		return state, fmt.Sprintf("%s, but this controller has no image to copy objects with (CONTROLLER_IMAGE is unset), "+
			"so the data plane keeps running as it is", reason), nil
	}

	next := migrationState{
		serving:      serving.Incarnation,
		other:        planeIncarnation(serving) + 1,
		phase:        phaseProvisioning,
		ha:           desired.HighAvailability,
		storageClass: desired.StorageClass,
		startedAt:    time.Now(),
	}
	if err := d.saveMigrationState(ctx, serving, next); err != nil {
		return state, "", err
	}
	d.log.Info("starting a migration to a new Garage cluster",
		"store", serving.Name, "reason", reason, "from", next.serving, "to", next.other)
	return next, fmt.Sprintf("%s: migrating to a new Garage cluster, which the consumers do not see "+
		"(the endpoint and their credentials do not change)", reason), nil
}

// migrationProvision builds the incoming cluster and brings it up with the same
// buckets and quotas as the serving one.
//
// Nothing is copied here and nothing is at stake: the serving cluster is not
// touched, and a provisioning that cannot finish — no room in the class, a class
// that binds nothing — leaves the store exactly as it was, saying what it is
// waiting for.
func (d *Driver) migrationProvision(
	ctx context.Context,
	desired, serving *backend.Store,
	state migrationState,
	sumBytes int64,
) (migrationState, string, error) {
	if next, msg, done := d.abortIfSettingsMovedOn(ctx, desired, serving, state); done {
		return next, msg, nil
	}
	incoming := d.incomingPlane(serving, state)

	if err := d.ensureSecret(ctx, incoming); err != nil {
		return state, "", fmt.Errorf("the secret of the incoming cluster: %w", err)
	}
	rf := initialReplicationFactor(incoming)
	cfgHash := configHash(renderConfig(rf))
	for _, obj := range []client.Object{
		buildConfigMap(incoming, d.namespace, rf),
		buildServiceAccount(incoming, d.namespace),
		buildPlaneService(incoming, d.namespace),
		buildRPCService(incoming, d.namespace),
	} {
		if err := d.apply(ctx, obj); err != nil {
			return state, "", fmt.Errorf("the objects of the incoming cluster: %w", err)
		}
	}

	volumeSize := volumeSizeFor(sumBytes)
	ready, msg, err := d.ensureWorkload(ctx, incoming, cfgHash, volumeSize)
	if err != nil {
		return state, "", fmt.Errorf("the workload of the incoming cluster: %w", err)
	}

	// The budget belongs to the cluster from the moment it has replicas, not from
	// the moment it starts serving. Between here and the cutover the incoming
	// cluster is three pods already holding a copy of the data, and a node drain
	// in that window would take all three at once and send the copy back to the
	// beginning.
	if err := d.ensureDisruptionBudget(ctx, incoming); err != nil {
		return state, "", err
	}

	// The layout before the readiness, and not the other way round. A pod becomes
	// ready BECAUSE the layout reached it — that is what the readiness probe
	// reports — so waiting for readiness first waits for what this call is what
	// causes. Left in the other order, an e2e run watched a migration sit at
	// "0/3 pods ready" for twenty-one minutes with three healthy pods in it.
	mesh, err := d.ensureMeshAndLayout(ctx, incoming, volumeSize)
	if err != nil {
		return state, "", fmt.Errorf("the layout of the incoming cluster: %w", err)
	}
	if !ready {
		return state, d.waitingNote(state, appendNote(msg, mesh.msg)), nil
	}
	if !mesh.ready {
		return state, d.waitingNote(state, mesh.msg), nil
	}

	if err := d.mirrorBuckets(ctx, serving, incoming); err != nil {
		return state, "", err
	}

	state.phase = phaseCopying
	if err := d.saveMigrationState(ctx, serving, state); err != nil {
		return state, "", err
	}
	return state, "the new Garage cluster is up; copying the objects into it", nil
}

// mirrorBuckets creates every bucket of the source cluster in the target, with
// the same quota.
//
// It runs on every pass of the copy, not once at provisioning: a SystemBucket
// created while a migration is in flight is created on the cluster that serves,
// and a bucket that exists on one side only is a consumer that loses its storage
// at cutover.
func (d *Driver) mirrorBuckets(ctx context.Context, source, target *backend.Store) error {
	from, ready, err := d.adminClientFor(ctx, source)
	if err != nil || !ready {
		return errOrNotReady(err, source, "mirror the buckets")
	}
	to, ready, err := d.adminClientFor(ctx, target)
	if err != nil || !ready {
		return errOrNotReady(err, target, "mirror the buckets")
	}

	buckets, err := from.listBuckets(ctx)
	if err != nil {
		return fmt.Errorf("list the buckets of the serving cluster: %w", err)
	}
	for _, b := range buckets {
		info, err := from.getBucketByID(ctx, b.ID)
		if err != nil {
			return fmt.Errorf("read bucket %s: %w", b.ID, err)
		}
		for _, alias := range b.GlobalAliases {
			created, found, err := to.getBucketByAlias(ctx, alias)
			if err != nil {
				return fmt.Errorf("look up bucket %q in the incoming cluster: %w", alias, err)
			}
			if !found {
				created, err = to.createBucket(ctx, alias)
				if err != nil {
					return fmt.Errorf("create bucket %q in the incoming cluster: %w", alias, err)
				}
			}
			if info.Quotas.MaxSize != nil && (created.Quotas.MaxSize == nil || *created.Quotas.MaxSize != *info.Quotas.MaxSize) {
				if err := to.setBucketQuota(ctx, created.ID, *info.Quotas.MaxSize, objectQuotaFor(*info.Quotas.MaxSize)); err != nil {
					return fmt.Errorf("set the quota of bucket %q in the incoming cluster: %w", alias, err)
				}
			}
		}
	}
	return nil
}

// migrationCopy runs the bulk copy: the pass that moves everything while
// consumers keep reading and writing the cluster it is copying from.
func (d *Driver) migrationCopy(
	ctx context.Context,
	desired, serving *backend.Store,
	state migrationState,
) (migrationState, string, error) {
	if next, msg, done := d.abortIfSettingsMovedOn(ctx, desired, serving, state); done {
		return next, msg, nil
	}
	incoming := d.incomingPlane(serving, state)

	if err := d.mirrorBuckets(ctx, serving, incoming); err != nil {
		return state, "", err
	}
	buckets, err := d.bucketNames(ctx, serving)
	if err != nil {
		return state, "", err
	}
	if len(buckets) == 0 {
		// Nothing to copy: the store has no buckets at all. The comparison below
		// would agree trivially, and going straight to the cutover keeps the empty
		// case out of the code that measures progress.
		state.phase = phaseCutover
		state.frozenAt = time.Now()
		return state, "the store holds no buckets; switching to the new cluster", d.saveMigrationState(ctx, serving, state)
	}

	outcome, msg, err := d.runCopyPass(ctx, serving, incoming, copyPassBulk, buckets)
	if err != nil {
		return state, "", err
	}
	return d.recordCopyOutcome(ctx, serving, state, outcome, msg)
}

func (d *Driver) recordCopyOutcome(ctx context.Context, serving *backend.Store, state migrationState, outcome copyOutcome, msg string) (migrationState, string, error) {
	if outcome != copyDone {
		if outcome == copyQuotaBlocked {
			state.phase = phaseCutover
			state.frozenAt = time.Now()
			return state, "bulk copy reached the destination quota; entering bounded frozen synchronization", d.saveMigrationState(ctx, serving, state)
		}
		return state, d.waitingNote(state, msg), nil
	}

	state.phase = phaseVerifying
	if err := d.saveMigrationState(ctx, serving, state); err != nil {
		return state, "", err
	}
	return state, "the objects are copied; comparing the two clusters", nil
}

// migrationVerify compares the two clusters and decides whether the remaining
// difference is small enough to be closed inside a write freeze.
func (d *Driver) migrationVerify(
	ctx context.Context,
	desired, serving *backend.Store,
	state migrationState,
) (migrationState, string, error) {
	if next, msg, done := d.abortIfSettingsMovedOn(ctx, desired, serving, state); done {
		return next, msg, nil
	}
	incoming := d.incomingPlane(serving, state)

	behind, err := d.compareClusters(ctx, serving, incoming, false)
	if err != nil {
		return state, "", err
	}
	if behind.objects > cutoverObjectTolerance || behind.bytes > cutoverByteTolerance {
		state.phase = phaseCopying
		if err := d.saveMigrationState(ctx, serving, state); err != nil {
			return state, "", err
		}
		return state, fmt.Sprintf("the new cluster is behind by %d object(s) and %d byte(s); copying again",
			behind.objects, behind.bytes), nil
	}

	state.phase = phaseCutover
	state.frozenAt = time.Now()
	if err := d.saveMigrationState(ctx, serving, state); err != nil {
		return state, "", err
	}
	return state, "the two clusters agree; switching over, writes are briefly refused", nil
}

// difference is the aggregate mismatch between clusters. Before the freeze only
// missing data matters; the final copy synchronizes deletions, so cutover also
// checks for excess objects and bytes.
type difference struct {
	objects int64
	bytes   int64
	// bucket names the first bucket the two clusters disagree about, for the
	// message. A number alone does not tell an operator where to look.
	bucket string
}

// compareClusters compares aggregate counts, not object checksums or key sets.
// Excess destination data is tolerated only before the write freeze, because
// pruning a changing source would race its writers. The final pass prunes first.
func (d *Driver) compareClusters(ctx context.Context, source, target *backend.Store, exact bool) (difference, error) {
	from, ready, err := d.adminClientFor(ctx, source)
	if err != nil || !ready {
		return difference{}, errOrNotReady(err, source, "compare the clusters")
	}
	to, ready, err := d.adminClientFor(ctx, target)
	if err != nil || !ready {
		return difference{}, errOrNotReady(err, target, "compare the clusters")
	}

	buckets, err := from.listBuckets(ctx)
	if err != nil {
		return difference{}, fmt.Errorf("list the buckets of the serving cluster: %w", err)
	}

	var diff difference
	for _, b := range buckets {
		src, err := from.getBucketByID(ctx, b.ID)
		if err != nil {
			return difference{}, fmt.Errorf("read bucket %s: %w", b.ID, err)
		}
		for _, alias := range b.GlobalAliases {
			dst, found, err := to.getBucketByAlias(ctx, alias)
			if err != nil {
				return difference{}, fmt.Errorf("read bucket %q of the incoming cluster: %w", alias, err)
			}
			if !found {
				diff.objects += src.Objects
				diff.bytes += src.Bytes
				if diff.bucket == "" {
					diff.bucket = alias
				}
				continue
			}
			if missing := aggregateDifference(src.Objects, dst.Objects, exact); missing > 0 {
				diff.objects += missing
				if diff.bucket == "" {
					diff.bucket = alias
				}
			}
			if missing := aggregateDifference(src.Bytes, dst.Bytes, exact); missing > 0 {
				diff.bytes += missing
				if diff.bucket == "" {
					diff.bucket = alias
				}
			}
		}
	}
	return diff, nil
}

func aggregateDifference(source, target int64, exact bool) int64 {
	if source >= target {
		return source - target
	}
	if exact {
		return target - source
	}
	return 0
}

// migrationCutover is the one window in which consumers cannot write.
//
// The steps are ordered so that the window is as short as the delta allows, and
// so that re-entering the phase after a controller restart repeats work rather
// than skipping it: write access is revoked (idempotent), the delta is copied
// (the copier skips what is already there), the two clusters are compared
// by aggregate counts after synchronizing deletions, and only then does the
// endpoint move. Temporary consumer keys are not imported into the new cluster.
//
// The point of no return is the endpoint. Everything before it can be undone by
// deleting the incoming cluster; after it, the old cluster is kept read-only only
// until the new one is confirmed to hold the data (see migrationDrain).
func (d *Driver) migrationCutover(
	ctx context.Context,
	serving *backend.Store,
	state migrationState,
) (migrationState, string, error) {
	// There is no abort here, which is why this phase does not take the settings
	// at all: writes are already refused. Going
	// back from this phase means restoring write access on the serving cluster,
	// which is the same work as finishing — and finishing leaves the store where
	// the settings ask for it to be.
	incoming := d.incomingPlane(serving, state)

	stopped, err := d.stopCopyPass(ctx, serving, incoming, copyPassBulk)
	if err != nil {
		return state, "", err
	}
	if !stopped {
		return state, "stopping the bulk copier before final synchronization", nil
	}

	if !state.frozenAt.IsZero() && time.Since(state.frozenAt) > cutoverDeadline {
		// Final synchronization may delete destination keys. Do not unfreeze
		// its source until the Job and all of its pods have actually disappeared.
		// Also discard completed delta Jobs: their success cannot authorize a
		// later cutover after consumers have resumed writes.
		stopped, err := d.stopDeltaPass(ctx, serving, incoming)
		if err != nil {
			return state, "", err
		}
		if !stopped {
			return state, "the cutover timed out; stopping the final copier before allowing writes again", nil
		}
		// The switch is not going to happen this time round. Write access comes
		// back on its own: the ordinary pass grants it on every SystemBucket as
		// soon as the phase is no longer the cutover.
		state.phase = phaseCopying
		state.frozenAt = time.Time{}
		state.writeFence = ""
		state.fencePods = nil
		if err := d.saveMigrationState(ctx, serving, state); err != nil {
			return state, "", err
		}
		d.log.Error("the cutover did not complete in time; writes are allowed again and the copy continues",
			"store", serving.Name, "deadline", cutoverDeadline.String())
		return state, fmt.Sprintf("the switch to the new Garage cluster did not complete within %s; "+
			"writes are allowed again and the copy continues", cutoverDeadline), nil
	}

	svc, ready, err := d.adminClientFor(ctx, serving)
	if err != nil || !ready {
		return state, "", errOrNotReady(err, serving, "drain pending write grants")
	}
	drained, err := d.writeGrantsDrained(ctx, serving, svc)
	if err != nil {
		return state, "", err
	}
	if !drained {
		return state, "waiting for pending STS write grants before freezing writes", nil
	}
	if state.writeFence == "" {
		stopped, err := d.stopDeltaPass(ctx, serving, incoming)
		if err != nil {
			return state, "", err
		}
		if !stopped {
			return state, "stopping any previous final copier before fencing the source", nil
		}
	}

	frozen, err := d.freezeWrites(ctx, serving)
	if err != nil {
		return state, "", err
	}
	if state.writeFence != writeFenceReady {
		if state.writeFence != "" && state.writeFence != writeFenceRestarting && state.writeFence != writeFenceRolling {
			return state, "", fmt.Errorf("unexpected write fence stage %q", state.writeFence)
		}
		if state.writeFence == writeFenceRolling {
			if _, msg, err := d.fencePodsSettled(ctx, serving); err != nil || msg != "" {
				return state, msg, err
			}
		}
		synced, why, err := clusterInSync(ctx, svc, desiredReplicas(serving), d.blocksOnDisk(serving))
		if err != nil {
			return state, "", err
		}
		if !synced {
			return state, fmt.Sprintf("waiting for the frozen source to synchronize (%s)", why), nil
		}
		if state.writeFence == "" && desiredReplicas(serving) > 1 {
			pods, err := d.planePods(ctx, serving)
			if err != nil {
				return state, "", err
			}
			state.writeFence, state.fencePods = writeFenceRolling, podUIDs(pods)
			return state, "restarting the source Garage pods one at a time to finish in-flight requests",
				d.saveMigrationState(ctx, serving, state)
		}
		if state.writeFence == "" {
			state.writeFence = writeFenceStopping
			return state, "stopping the source Garage on its existing PVCs to finish in-flight requests", d.saveMigrationState(ctx, serving, state)
		}
		if state.writeFence == writeFenceRolling {
			done, msg, err := d.rollFencePods(ctx, serving, state.fencePods)
			if err != nil || !done {
				return state, msg, err
			}
			state.fencePods = nil
		}
		state.writeFence = writeFenceReady
		return state, "the restarted source is synchronized; final copy can start", d.saveMigrationState(ctx, serving, state)
	}

	buckets, err := d.bucketNames(ctx, serving)
	if err != nil {
		return state, "", err
	}
	if len(buckets) > 0 {
		if err := d.mirrorBuckets(ctx, serving, incoming); err != nil {
			return state, "", err
		}
		outcome, msg, err := d.runCopyPass(ctx, serving, incoming, copyPassDelta, buckets)
		if err != nil {
			return state, "", err
		}
		if outcome != copyDone {
			return state, fmt.Sprintf("switching over: writes are refused while the last objects are copied (%s)", msg), nil
		}

		behind, err := d.compareClusters(ctx, serving, incoming, true)
		if err != nil {
			return state, "", err
		}
		if behind.objects > 0 || behind.bytes > 0 {
			// Nothing is written to the source any more, so a difference that is
			// still there is a copy that did not finish. Run the delta again.
			if err := d.restartDeltaPass(ctx, serving, incoming); err != nil {
				return state, "", err
			}
			return state, fmt.Sprintf("switching over: bucket %q differs by %d object(s); synchronizing again",
				behind.bucket, behind.objects), nil
		}
	}

	// Nothing is carried across for the consumers, and there is nothing to carry:
	// the only keys a consumer ever holds live at most thirty minutes; the credentials
	// endpoint minted, and the module does not keep their secret halves — the
	// backend hands a secret back once and never again. A client holding one when
	// the endpoint moves gets 403 on its next request. Its application must handle
	// early invalidation and renew credentials; ordinary expiry-based SDK refresh
	// alone does not guarantee recovery before that key's advertised expiry.
	//
	// What does not change is the address and the right to ask: the Service keeps
	// its name, and the RBAC grant that lets a ServiceAccount exchange its token
	// knows nothing about clusters.

	// The endpoint. One write, and every consumer is talking to the new cluster,
	// at the same address.
	switched := state
	switched.serving, switched.other = state.other, state.serving
	switched.phase = phaseDraining
	switched.fencePods = nil
	switched.cutoverAt = time.Now()
	if err := d.apply(ctx, buildEndpointService(serving, d.namespace, endpointSelector(serving, switched))); err != nil {
		return state, "", fmt.Errorf("switch the endpoint to the new cluster: %w", err)
	}
	if err := d.saveMigrationState(ctx, serving, switched); err != nil {
		return state, "", err
	}
	d.log.Info("the endpoint now points at the new Garage cluster",
		"store", serving.Name, "serving", switched.serving, "retiring", switched.other,
		"writesFrozenOn", frozen)
	return switched, "the store now runs on the new Garage cluster; the old one is kept read-only " +
		"until the new one is confirmed to hold the data", nil
}

// freezeWrites revokes write access from every key of every bucket of a cluster,
// and reports how many buckets it froze.
//
// Reads are left alone. The fence that follows restarts the source pods: one at
// a time in the highly available mode (rollFencePods), where a read can stall
// while the replica it asks first is away; all at once for a single replica,
// whose readers see that restart. A consumer that writes gets a 403 for the length of
// the delta copy. There is no way to
// pause an S3 consumer, and two clusters taking writes at once is the one outcome
// a migration must not produce.
func (d *Driver) freezeWrites(ctx context.Context, plane *backend.Store) (int, error) {
	svc, ready, err := d.adminClientFor(ctx, plane)
	if err != nil || !ready {
		return 0, errOrNotReady(err, plane, "freeze the writes")
	}
	buckets, err := svc.listBuckets(ctx)
	if err != nil {
		return 0, fmt.Errorf("list the buckets: %w", err)
	}
	for _, b := range buckets {
		info, err := svc.getBucketByID(ctx, b.ID)
		if err != nil {
			return 0, fmt.Errorf("read bucket %s: %w", b.ID, err)
		}
		for _, k := range info.Keys {
			if !k.Permissions.Write {
				continue
			}
			if err := svc.deny(ctx, b.ID, k.AccessKeyID, permissions{Write: true}); err != nil {
				return 0, fmt.Errorf("revoke write access of key %s on bucket %s: %w", k.AccessKeyID, b.ID, err)
			}
		}
	}
	return len(buckets), nil
}

// restartDeltaPass removes the finished delta Job so the next pass runs it again.
func (d *Driver) restartDeltaPass(ctx context.Context, source, target *backend.Store) error {
	job := &batchv1.Job{}
	job.Name, job.Namespace = copyJobName(source, target.Incarnation, copyPassDelta), d.namespace
	return d.deleteCopyJob(ctx, job)
}

// migrationDrain removes the migrated-from cluster as soon as the one now
// serving is confirmed to hold the data.
//
// The content was confirmed before the switch: the final pass compared the
// frozen source with the new cluster, and nothing has been written to the source
// since. What is left is that the new cluster keeps it — every replica holds
// every block it counts — which is what a replica that died right after the
// switch, or a copy that landed on fewer replicas than it should, would show.
// Until then the old cluster stays, read-only, as the copy to recover from.
//
// The pass that calls this has already found the serving cluster healthy, so
// the wait is for its replicas to catch up, not for it to come up.
func (d *Driver) migrationDrain(ctx context.Context, serving *backend.Store, state migrationState) (migrationState, string, error) {
	svc, ready, err := d.adminClientFor(ctx, serving)
	if err != nil {
		return state, "", err
	}
	if !ready {
		return state, "the previous Garage cluster is kept read-only until the new one can be checked", nil
	}
	synced, why, err := clusterInSync(ctx, svc, desiredReplicas(serving), d.blocksOnDisk(serving))
	if err != nil {
		return state, "", err
	}
	if !synced {
		return state, fmt.Sprintf("the previous Garage cluster is kept read-only until the new one holds every block (%s)", why), nil
	}
	state.phase = phaseDecommission
	if err := d.saveMigrationState(ctx, serving, state); err != nil {
		return state, "", err
	}
	return state, "the new Garage cluster holds the data; removing the previous one", nil
}

// migrationDecommission removes the plane that is not serving and closes the
// migration.
func (d *Driver) migrationDecommission(ctx context.Context, serving *backend.Store, state migrationState) (migrationState, string, error) {
	if state.other != 0 && state.other != state.serving {
		retiring := planeOf(serving, state.other, state.ha, state.storageClass)
		if err := d.teardownPlane(ctx, retiring); err != nil {
			return state, "", err
		}
		d.log.Info("the previous Garage cluster is removed", "store", serving.Name, "incarnation", state.other)
	}

	if err := d.removeCopyJobs(ctx, serving); err != nil {
		return state, "", err
	}
	if err := d.removeCopierCredentials(ctx, serving); err != nil {
		return state, "", err
	}

	done := migrationState{serving: state.serving, phase: phaseConverged}
	if err := d.saveMigrationState(ctx, serving, done); err != nil {
		return state, "", err
	}
	return done, "", nil
}

// abortIfSettingsMovedOn calls the migration off when the settings no longer ask
// for the cluster being built.
//
// Before the cutover this is free: nothing has been switched, so the incoming
// cluster is simply removed and the store is left where it already was. It covers
// both the operator who changed their mind and the one who changed the setting
// twice, and it is also what stops a copy from finishing into a cluster of the
// wrong shape.
func (d *Driver) abortIfSettingsMovedOn(
	ctx context.Context,
	desired, serving *backend.Store,
	state migrationState,
) (migrationState, string, bool) {
	// An unfinished shrink whose authority has gone. A migration to a single
	// replica runs only because somebody wrote highAvailability: false; if that
	// setting is taken away mid-flight, the cluster being built is one nobody is
	// asking for any more — and the derived mode agreeing with it means only that
	// the control plane is still short of masters, which is what the guard exists
	// to ignore. Without this the shape comparison below would find the target and
	// the settings in agreement and carry on shrinking.
	unauthorisedShrink := !state.ha && serving.HighAvailability && !desired.HighAvailabilityExplicit

	if desired.HighAvailability == state.ha && desired.StorageClass == state.storageClass && !unauthorisedShrink {
		return state, "", false
	}
	aborted := state
	aborted.phase = phaseDecommission
	if err := d.saveMigrationState(ctx, serving, aborted); err != nil {
		d.log.Error("could not record the abort of a migration", logger.Err(err))
		return state, "", false
	}
	d.log.Info("the settings changed while a migration was in flight; calling it off",
		"store", serving.Name, "incoming", state.other)
	return aborted, "the settings changed while the migration was in flight; the half-built cluster is being removed " +
		"and the store keeps running on the cluster it is on", true
}

// incomingPlane is the description of the cluster being built.
func (d *Driver) incomingPlane(serving *backend.Store, state migrationState) *backend.Store {
	return planeOf(serving, state.other, state.ha, state.storageClass)
}

// waitingNote prefixes a phase's progress with how long the migration has been
// running. An operation measured in hours has to say how long it has been at it,
// or "provisioning" reads the same at minute two and at hour six.
func (d *Driver) waitingNote(state migrationState, msg string) string {
	if state.startedAt.IsZero() {
		return msg
	}
	return fmt.Sprintf("migrating to a new Garage cluster for %s: %s",
		time.Since(state.startedAt).Round(time.Minute), msg)
}

// errOrNotReady turns "the admin API is not usable yet" into an error the phase
// can report, so a phase never carries on against a cluster it cannot talk to.
func errOrNotReady(err error, plane *backend.Store, what string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return fmt.Errorf("%s: the admin token of %s is not provisioned yet", what, resourceName(plane))
}

// teardownPlane removes every object of one data plane, including the volumes.
//
// The claims are named rather than selected by label: a StatefulSet's claims are
// created by the controller from a template, and on an installation that predates
// the incarnation label they carry only the store's labels — which the other
// plane's claims carry too. A prefix is not enough either ("data-system-garage-"
// is a prefix of "data-system-garage-i2-0"), so the ordinal is parsed and the
// parent name compared.
func (d *Driver) teardownPlane(ctx context.Context, plane *backend.Store) error {
	sts := &appsv1.StatefulSet{}
	sts.Name, sts.Namespace = resourceName(plane), d.namespace
	if err := d.client.Delete(ctx, sts); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove the StatefulSet of the retired cluster: %w", err)
	}

	claims := &corev1.PersistentVolumeClaimList{}
	if err := d.apiReader.List(ctx, claims, client.InNamespace(d.namespace)); err != nil {
		return fmt.Errorf("list the volumes of the retired cluster: %w", err)
	}
	for i := range claims.Items {
		pvc := &claims.Items[i]
		if !claimBelongsTo(pvc.Name, resourceName(plane)) {
			continue
		}
		if err := d.client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("remove volume %q of the retired cluster: %w", pvc.Name, err)
		}
	}

	for _, obj := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName(plane), Namespace: d.namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName(plane), Namespace: d.namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: nodeIdentitySecretName(plane), Namespace: d.namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: resourceName(plane), Namespace: d.namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: planeSvcName(plane), Namespace: d.namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: rpcSvcName(plane), Namespace: d.namespace}},
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: resourceName(plane), Namespace: d.namespace}},
	} {
		if err := d.client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("remove %s of the retired cluster: %w", obj.GetName(), err)
		}
	}
	return nil
}

// removeCopyJobs removes the copier Jobs of a finished or abandoned migration.
//
// By prefix rather than by name because the two ends of a migration name them
// differently — the incarnation in the name is the one being copied INTO, which
// after a cutover is the plane that now serves and after an abort is the plane
// being thrown away. One rule covers both.
func (d *Driver) removeCopyJobs(ctx context.Context, store *backend.Store) error {
	jobs := &batchv1.JobList{}
	if err := d.apiReader.List(ctx, jobs, client.InNamespace(d.namespace), client.MatchingLabels(commonLabels(store))); err != nil {
		return fmt.Errorf("list the copier Jobs: %w", err)
	}
	prefix := stableName(store) + "-copy-"
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !strings.HasPrefix(job.Name, prefix) {
			continue
		}
		if err := d.deleteCopyJob(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

// claimBelongsTo reports whether a claim was created by the StatefulSet of the
// given name, from the "data" volumeClaimTemplate.
func claimBelongsTo(claim, setName string) bool {
	const prefix = "data-"
	if len(claim) <= len(prefix) || claim[:len(prefix)] != prefix {
		return false
	}
	parent, ok := podParent(claim[len(prefix):])
	return ok && parent == setName
}

// pendingMigrationNote is what a pass that did not get as far as the migration
// machine says about it.
//
// It is deliberately vaguer than what the phases say: this is the note of a pass
// that has not looked at the migration this time round, and it must not claim
// progress it did not observe.
func pendingMigrationNote(plan migrationPlan, state migrationState) string {
	if state.migrating() {
		return fmt.Sprintf("a migration to a new Garage cluster is in progress (%s); "+
			"the endpoint and the consumers' credentials do not change", state.phase)
	}
	if state.phase.resizing() {
		return fmt.Sprintf("the replica count is being changed in place (%s); "+
			"the endpoint and the consumers' credentials do not change", state.phase)
	}
	if !plan.start {
		// A disagreement the module will not resolve on its own. It is said whole,
		// without the "starts as soon as" that would be a promise it is not making.
		return plan.reason
	}
	if plan.inPlace {
		return fmt.Sprintf("%s, which cannot be applied to a running Garage cluster: "+
			"the cluster is restarted with the new replica count as soon as it is healthy", plan.reason)
	}
	return fmt.Sprintf("%s, which cannot be applied to a running Garage cluster: "+
		"a migration to a new one starts as soon as the current one is healthy", plan.reason)
}
