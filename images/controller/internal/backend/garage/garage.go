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

// Package garage drives the Garage instance that backs the platform's system
// object storage.
//
// It reconciles the data plane — the ServiceAccount, RPC/admin secret,
// garage.toml ConfigMap, a StatefulSet with a fixed replica count on
// control-plane nodes over node-sticky local PersistentVolumes, and the S3/RPC
// Services — then connects the RPC peers and assigns the Garage cluster layout
// via the admin API (see mesh.go), and provisions buckets and access keys
// (buckets.go, access.go).
//
// There is one instance per Kubernetes cluster and it has no custom resource of
// its own: the desired state arrives as a backend.Store built from the module
// settings.
package garage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// garageVersion is the Garage release this driver targets. Keep it in sync with
// the upstream tag pinned in images/garage/werf.inc.yaml.
const garageVersion = "v2.4.1"

// Keys in the Garage secret.
const (
	secretKeyRPC   = "rpc-secret"
	secretKeyAdmin = "admin-token"
)

// Driver reconciles the system Garage store. It is constructed once at startup
// with the manager's client; a non-cached reader (for listing pods without
// caching every pod in the cluster); the module namespace; and the Garage image
// reference.
type Driver struct {
	client     client.Client
	apiReader  client.Reader
	restConfig *rest.Config
	log        logger.Logger
	namespace  string
	image      string
	// copierImage is the controller's own image, which the migration Job runs
	// with a different argument (see migrator.Command). The controller cannot
	// derive it — a pod does not know the reference it was pulled from once it is
	// running — so the chart passes it in, the same way it passes the Garage one.
	copierImage   string
	clusterDomain string

	// The exec clientset is built once. Capturing node identities execs into
	// every replica on every pass, and a clientset per exec is a set of REST
	// clients and rate limiters rebuilt for a call that lasts milliseconds. The
	// transports are cached globally by client-go, so this is not about TLS
	// handshakes — just about not constructing the rest of it six times a minute.
	execOnce   sync.Once
	execClient *kubernetes.Clientset
	execErr    error
}

// clientset returns the shared exec clientset, building it on first use.
func (d *Driver) clientset() (*kubernetes.Clientset, error) {
	d.execOnce.Do(func() {
		if d.restConfig == nil {
			d.execErr = fmt.Errorf("no rest config configured for exec")
			return
		}
		d.execClient, d.execErr = kubernetes.NewForConfig(d.restConfig)
	})
	return d.execClient, d.execErr
}

// New builds a Garage Driver.
func New(c client.Client, apiReader client.Reader, restConfig *rest.Config, log logger.Logger, namespace, image, copierImage, clusterDomain string) *Driver {
	return &Driver{
		client:        c,
		apiReader:     apiReader,
		restConfig:    restConfig,
		log:           log.Named("garage"),
		namespace:     namespace,
		image:         image,
		copierImage:   copierImage,
		clusterDomain: clusterDomain,
	}
}

// EnsureStore reconciles the Garage data plane and reports its state.
//
// The results are named because of the deferred note below: a mode change that
// is not applied has to reach the operator from EVERY exit of this function, and
// there are a dozen of them.
func (d *Driver) EnsureStore(ctx context.Context, store *backend.Store) (state backend.StoreState, err error) {
	state = backend.StoreState{Version: garageVersion}

	if d.image == "" {
		state.Message = "GARAGE_IMAGE is not configured on the controller"
		return state, nil
	}

	// No StorageClass, no data plane. The module orders its volumes like any other
	// workload and has nothing of its own to fall back to: the previous
	// implementation provisioned hostPath volumes on the control-plane nodes,
	// which is exactly what a platform with storage modules should not be doing
	// behind their back.
	//
	// Nothing is created in this state — not even the Secret above is followed by
	// a workload — and the reason is on every SystemBucket rather than in the log
	// alone.
	if store.StorageClass == "" {
		state.Message = "no StorageClass is configured for the system object storage: " +
			"set storageClass in the module settings, or global.modules.storageClass, " +
			"or mark a StorageClass as the cluster default"
		return state, nil
	}

	// Which data plane is being reconciled is the first thing to settle, because
	// every object below is named after it. There is one plane on all but the
	// hours a migration takes; while a migration runs there are two, and this pass
	// belongs to the one the endpoint points at — the incoming plane is built by
	// the migration itself (see reconcileMigration), on its own terms.
	migration, err := d.loadMigrationState(ctx, store)
	if err != nil {
		return state, fmt.Errorf("read the migration state: %w", err)
	}
	store = planeOf(store, migration.serving, store.HighAvailability, store.StorageClass)

	if err := d.ensureSecret(ctx, store); err != nil {
		return state, fmt.Errorf("ensure secret: %w", err)
	}
	// The RPC/admin secret exists from here on, so the state can name it.
	state.AdminSecretName = secretName(store)
	pinned, err := d.pinnedState(ctx, store)
	if err != nil {
		return state, fmt.Errorf("read pinned store state: %w", err)
	}

	// The high-availability mode and the StorageClass cannot be changed where the
	// data plane stands: Garage refuses a replication_factor change on a live
	// cluster, and a bound claim cannot be re-ordered from another class. Either
	// change is therefore a migration to a second Garage cluster, which this pass
	// starts and advances at the end, once it has found the cluster it is moving
	// away from healthy (see reconcileMigration).
	//
	// What the configured class is compared against is the class the claims were
	// actually created with, which is the only record of it — the module keeps no
	// pinned copy of the class the way it pins the replication factor.
	boundClass, err := d.boundStorageClass(ctx, store)
	if err != nil {
		return state, fmt.Errorf("read the StorageClass of the data plane volumes: %w", err)
	}

	// From here on the pass works on the store as it RUNS, not as it is asked to
	// run. The pinned replication factor is the shape of the live cluster — the
	// replica count, the layout, the placement rules and the volumes all follow it
	// — and a mode that was edited must not move any of them: what moves the store
	// to the new mode is the migration, in its own time, and this cluster keeps
	// serving unchanged until the endpoint is switched.
	//
	// Without this the refusal was only half true: the message said the factor
	// stays at 1, and the workload was scaled to the three replicas the new mode
	// asked for, at factor 1. An e2e run caught it as a StatefulSet rolling out
	// underneath a store that had just been told nothing would change.
	desired := store
	store = storeAsPinned(store, pinned)

	// A migration is advanced at the END of this function, and only by a pass that
	// found the serving cluster healthy. That leaves every earlier return — a
	// cluster still rolling out, a volume still growing — with nothing to say
	// about a mode or a class that was edited hours ago, which is exactly the
	// moment an operator goes looking. So the pending move is reported from every
	// exit, and suppressed on the one path that has already reported it in more
	// detail.
	migrationAdvanced := false
	if plan := migrationTrigger(desired, store, boundClass); plan.reason != "" || migration.migrating() {
		note := pendingMigrationNote(plan, migration)
		defer func() {
			if err != nil || migrationAdvanced {
				return // the caller reports the error; the phase reported itself
			}
			state.Message = appendNote(state.Message, note)
		}()
	}

	rf := pinned.rf
	cfgHash := configHash(renderConfig(rf))
	if err := d.apply(ctx, buildConfigMap(store, d.namespace, rf)); err != nil {
		return state, fmt.Errorf("ensure configmap: %w", err)
	}
	if err := d.apply(ctx, buildServiceAccount(store, d.namespace)); err != nil {
		return state, fmt.Errorf("ensure serviceaccount: %w", err)
	}
	if err := d.apply(ctx, buildPlaneService(store, d.namespace)); err != nil {
		return state, fmt.Errorf("ensure the plane service: %w", err)
	}
	if err := d.apply(ctx, buildEndpointService(store, d.namespace, endpointSelector(store, migration))); err != nil {
		return state, fmt.Errorf("ensure the endpoint service: %w", err)
	}
	if err := d.apply(ctx, buildRPCService(store, d.namespace)); err != nil {
		return state, fmt.Errorf("ensure rpc service: %w", err)
	}

	state.Endpoint = s3Endpoint(store, d.namespace, d.clusterDomain)
	state.Region = "garage"

	// Persist each replica's Garage node identity before any placement move, so a
	// replica that comes back on an empty volume (a move of a node-pinned volume)
	// rejoins with its stable node ID instead of generating a new one (which
	// strands the old ID as a dead node and churns the layout beyond recovery on
	// consolidate).
	identitiesMissing, err := d.ensureNodeIdentities(ctx, store)
	if err != nil {
		return state, fmt.Errorf("ensure node identities: %w", err)
	}
	// Converge replica placement onto the target topology as the master count
	// changes (spread across 3 masters, consolidate onto 1). One health-gated
	// PVC recycle per reconcile; when it acts, report progress and requeue so
	// the move settles before the next one.
	acted, placementMsg, err := d.reconcilePlacement(ctx, store)
	if err != nil {
		return state, fmt.Errorf("reconcile placement: %w", err)
	}
	if acted {
		state.Message = placementMsg
		return state, nil
	}
	// Not acted, but a message: placement gave up after repeating the same move
	// (see maxPlacementAttempts). Reconciliation continues — the store serves with
	// the replicas where they are — and the message is appended at the end so it
	// reaches the condition instead of being overwritten by the mesh state.
	if placementMsg != "" {
		d.log.Error("placement is not converging", "store", store.Name, "reason", placementMsg)
	}

	// What the store is for decides how big it is: the sum of what its consumers
	// asked for, plus the room a sum of quotas does not account for.
	sumBytes, err := d.desiredBucketBytes(ctx)
	if err != nil {
		return state, fmt.Errorf("sum the SystemBucket sizes: %w", err)
	}
	volumeSize := volumeSizeFor(sumBytes)

	// Claims first, workload second. The claims are what actually hold the space;
	// the template is brought in line below so a replica created later asks for the
	// same size.
	_, growMsg := d.ensureVolumeCapacity(ctx, store, volumeSize)
	if growMsg != "" {
		d.log.Error("the data plane volumes could not be grown", "store", store.Name, "reason", growMsg)
		defer func() {
			if err != nil {
				return
			}
			state.Message = appendNote(state.Message, growMsg)
		}()
	}

	workloadReady, workloadMsg, err := d.ensureWorkload(ctx, store, cfgHash, volumeSize)
	if err != nil {
		return state, err
	}

	// The disruption budget belongs to a cluster that has a quorum to lose, so it
	// is created in the highly available mode and removed outside it — a single
	// replica has nothing to protect, and a budget over one pod would only stand
	// in the way of a drain.
	if err := d.ensureDisruptionBudget(ctx, store); err != nil {
		return state, err
	}

	// The mesh is reconciled whether or not the pods are ready, and the order is
	// the point: a pod becomes ready BECAUSE the layout reached it (see the
	// readiness probe), so waiting for readiness first would be waiting for
	// something this call is what causes.
	mesh, err := d.ensureMeshAndLayout(ctx, store, volumeSize)
	if err != nil {
		return state, err
	}
	if !workloadReady {
		state.Message = appendNote(workloadMsg, mesh.msg)
		return state, nil
	}
	state.TotalCapacity = mesh.total
	state.Ready = mesh.ready
	state.Message = mesh.msg

	// Said on the object, not only in the log. A replica with no stored identity
	// works fine until the next placement move, and then rejoins under a new node
	// ID and leaves the old one in the layout as a dead node. The store stays
	// Ready — nothing is broken yet — but an operator has to be able to see that
	// the protection is off before a rebalance turns it into layout churn.
	if placementMsg != "" {
		state.Message = fmt.Sprintf("%s; %s", state.Message, placementMsg)
	}

	if identitiesMissing > 0 {
		state.Message = fmt.Sprintf(
			"%s; node identity not captured for %d replica(s): a placement move would give them new node IDs",
			state.Message, identitiesMissing)
	}

	// Last, and only on a pass that got this far. A migration is never started,
	// advanced or cut over while the cluster it is moving away from is not in a
	// state to be moved away from — every return above this point is either an
	// error or a cluster that is still converging, and both are answers of the
	// form "not now".
	migrationMsg, err := d.advanceMigration(ctx, desired, store, boundClass, migration, sumBytes)
	if err != nil {
		return state, err
	}
	migrationAdvanced = true
	state.Message = appendNote(state.Message, migrationMsg)
	state.MigrationPhase, state.MigrationStartedAt = reportedMigration(migration)
	return state, nil
}

// ensureDisruptionBudget creates the data plane's PodDisruptionBudget in the
// highly available mode and removes it outside that mode.
//
// Removal matters as much as creation: a budget left behind over a single
// replica allows no disruption at all, and the first drain of the node it sits
// on would wait forever on a store that has nothing to protect.
func (d *Driver) ensureDisruptionBudget(ctx context.Context, store *backend.Store) error {
	if desiredReplicas(store) > 1 {
		if err := d.apply(ctx, buildPodDisruptionBudget(store, d.namespace)); err != nil {
			return fmt.Errorf("ensure the disruption budget: %w", err)
		}
		return nil
	}

	pdb := &policyv1.PodDisruptionBudget{}
	pdb.Name, pdb.Namespace = resourceName(store), d.namespace
	if err := d.client.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove the disruption budget: %w", err)
	}
	return nil
}

// storeAsPinned returns the store as the running cluster actually is: the
// high-availability mode implied by the plane's pinned replication factor rather
// than the one the settings ask for.
//
// A plane that was never initialised is returned unchanged — there is nothing
// pinned yet, and the settings are exactly what it should be built from.
func storeAsPinned(store *backend.Store, pinned pinnedState) *backend.Store {
	if !pinned.exists {
		return store
	}
	running := *store
	running.HighAvailability = pinned.rf >= replicasHA
	return &running
}

// appendNote joins a note to whatever the pass reported, keeping both readable
// when either side has nothing to say.
//
// The empty note is not a theoretical case: the migration machine has passes
// that do nothing and say nothing, and a message ending in a bare "; " is a
// message an operator reads as truncated.
func appendNote(message, note string) string {
	switch {
	case note == "":
		return message
	case message == "":
		return note
	default:
		return fmt.Sprintf("%s; %s", message, note)
	}
}

// boundStorageClass is the StorageClass the data plane's volumes were actually
// created with, read from the first replica's claim. Empty when the store has no
// claims yet (a fresh installation), which is the case where there is nothing to
// disagree with.
func (d *Driver) boundStorageClass(ctx context.Context, store *backend.Store) (string, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{Namespace: d.namespace, Name: fmt.Sprintf("data-%s-0", resourceName(store))}
	if err := d.apiReader.Get(ctx, key, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	if pvc.Spec.StorageClassName == nil {
		return "", nil
	}
	return *pvc.Spec.StorageClassName, nil
}

// pinnedState is the state one data plane pins at init and the controller reads
// back afterwards instead of recomputing: the Garage replication factor. exists
// reports whether the garage.toml ConfigMap holding it was found, i.e. whether
// this plane was ever initialized.
type pinnedState struct {
	rf     int32
	exists bool
}

// pinnedState returns the replication_factor to bake into garage.toml. Garage
// cannot change replication_factor on a live
// cluster, so it is decided ONCE at cluster init and then pinned: the value is
// read back from the running garage.toml on every subsequent reconcile and never
// recomputed from the current node count. This keeps the factor stable across
// control-plane node-count changes (e.g. masters going 3->1->3) — every node
// keeps the same factor, no data-losing rf change is ever attempted, and the
// cluster merely degrades to read-only (data intact) if the live node count
// drops below it.
//
// There is no way out of a pinned factor in this release: a change of the
// high-availability mode is reported and not applied (see EnsureStore). Moving a
// store to the other mode means migrating its data to a second Garage cluster,
// which a later release performs and which bumps the incarnation.
func (d *Driver) pinnedState(ctx context.Context, store *backend.Store) (pinnedState, error) {
	existing := &corev1.ConfigMap{}
	err := d.client.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: configName(store)}, existing)
	switch {
	case apierrors.IsNotFound(err):
		// First init: no pinned factor yet, compute it once.
		return pinnedState{rf: initialReplicationFactor(store)}, nil
	case err != nil:
		return pinnedState{}, err
	}
	// The ConfigMap exists — the factor is already pinned and must be read back,
	// never recomputed. If it is missing/unparseable, fail closed rather than
	// recompute: recomputing could differ from the value the live cluster was
	// initialized with and trigger a forbidden replication_factor change.
	rf := replicationFactorFromConfigMap(existing)
	if rf <= 0 {
		return pinnedState{}, fmt.Errorf("garage.toml ConfigMap %q exists but its replication_factor is absent or unparseable; refusing to recompute (would risk a replication_factor change on a live cluster)", configName(store))
	}
	return pinnedState{rf: rf, exists: true}, nil
}

// initialReplicationFactor computes the factor to pin at first init: the
// redundancy the module settings ask for, clamped to the desired replica count.
// That is three replicas with factor 3 by default, or a single replica with
// factor 1 when singleReplica is set; the replica count is fixed either way, so
// the factor never depends on the master count.
func initialReplicationFactor(store *backend.Store) int32 {
	return clampRF(replicationFactor(store), desiredReplicas(store))
}

// EnsureBucket and DeleteBucket are implemented in buckets.go.

// ensureSecret creates the Garage secret (rpc secret + admin token) on first
// reconcile and never overwrites existing values.
func (d *Driver) ensureSecret(ctx context.Context, store *backend.Store) error {
	key := client.ObjectKey{Namespace: d.namespace, Name: secretName(store)}
	existing := &corev1.Secret{}
	err := d.client.Get(ctx, key, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	rpcSecret, err := randomHex(32)
	if err != nil {
		return err
	}
	adminToken, err := randomHex(32)
	if err != nil {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName(store),
			Namespace: d.namespace,
			Labels:    commonLabels(store),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			secretKeyRPC:   []byte(rpcSecret),
			secretKeyAdmin: []byte(adminToken),
		},
	}
	return d.client.Create(ctx, secret)
}

// ensureWorkload creates/updates the StatefulSet — a fixed replica count on
// control-plane nodes over node-sticky local volumes — and reports whether its
// pods are ready.
func (d *Driver) ensureWorkload(ctx context.Context, store *backend.Store, cfgHash string, volumeSize resource.Quantity) (bool, string, error) {
	// A volumeClaimTemplate cannot be updated, so a workload asking for the old
	// size is deleted with its pods left running and created again below. The data
	// plane keeps serving across the gap; what it goes without, for the time
	// between the two calls, is the controller that would replace a pod that died.
	current, err := d.statefulSetVolumeSize(ctx, store)
	if err != nil {
		return false, "", fmt.Errorf("read the statefulset volume size: %w", err)
	}
	if !current.IsZero() && current.Cmp(volumeSize) != 0 {
		d.log.Info("recreating the data-plane workload for a new volume size",
			"store", store.Name, "from", current.String(), "to", volumeSize.String())
		if err := d.dropStatefulSetKeepingPods(ctx, store); err != nil {
			return false, "", fmt.Errorf("drop the statefulset for a volume resize: %w", err)
		}
	}

	sts := buildStatefulSet(store, d.namespace, d.image, cfgHash, volumeSize)
	if err := d.apply(ctx, sts); err != nil {
		return false, "", fmt.Errorf("ensure statefulset: %w", err)
	}
	desired := int32(0)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}

	ready, msg := workloadRolledOut(sts, desired)
	return ready, msg, nil
}

// workloadRolledOut reports whether the StatefulSet is serving the revision that
// was just applied, and what to say while it is not.
//
// Revision matters as much as count, and readyReplicas cannot tell them apart.
// Garage reads garage.toml once, at startup. When the config hash changes the pod
// template changes with it, and for a moment the old pods are still Ready while
// the StatefulSet has not even begun rolling — observedGeneration still behind
// generation. A gate that counts only ready pods opens right there, and what runs
// immediately after it is mesh and layout: peers connected and a layout applied
// against a Garage cluster running the previous config, while consumers hold
// credentials for an endpoint reported Ready.
//
// Hence the order: generation, then revision, then count. The same lesson as
// "pods Ready" not meaning "pods of the revision you just applied".
func workloadRolledOut(sts *appsv1.StatefulSet, desired int32) (bool, string) {
	if sts.Status.ObservedGeneration < sts.Generation {
		return false, "Garage StatefulSet update not observed yet"
	}
	if sts.Status.UpdateRevision != "" && sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		return false, fmt.Sprintf("Garage StatefulSet rolling out (%d/%d pods on the new revision)",
			sts.Status.UpdatedReplicas, desired)
	}
	if sts.Status.ReadyReplicas < desired {
		return false, fmt.Sprintf("Garage StatefulSet rolling out (%d/%d pods ready)",
			sts.Status.ReadyReplicas, desired)
	}
	return true, ""
}

// apply creates or updates obj. On update it overwrites the spec-bearing fields
// with the desired object.
//
// Nothing here carries an ownerReference: the store has no custom resource to be
// owned by. The data plane is identified by commonLabels instead and torn down
// explicitly — by the recreate path (recreate.go) and, when the module is
// disabled, together with the namespace that holds it.
//
// That last sentence holds for what is namespaced, which is everything applied
// here. It does not hold for the local PV pool: PersistentVolumes are
// cluster-scoped, Helm does not own them because the controller creates them, and
// there is no custom resource to own them either. Deleting the namespace leaves
// them, so the module-delete hook removes them explicitly
// (hooks/go/030-remove-finalizers-on-module-delete). The directories they point at
// on the masters are left behind — the module has no node agent — and
// CONFIGURATION documents that.
func (d *Driver) apply(ctx context.Context, obj client.Object) error {
	desired := obj.DeepCopyObject().(client.Object)
	_, err := controllerutil.CreateOrUpdate(ctx, d.client, obj, func() error {
		return mergeDesired(obj, desired)
	})
	return err
}

// mergeDesired copies the desired spec/data onto the live object fetched by
// CreateOrUpdate, preserving server-managed metadata. Immutable fields
// (StatefulSet selector/volumeClaimTemplates, headless ClusterIP) are not
// changed once set.
// mergeDesired copies the spec-bearing fields of desired onto live.
//
// An unknown type is an error rather than a silent no-op, and the difference is
// not academic. Silently doing nothing here means an object created once, on the
// pass where CreateOrUpdate took the create branch, and never updated again — so
// a changed spec would apply the first time the module is installed and be
// ignored for the rest of its life. That failure is invisible: no error, no
// condition, and an object in the cluster that simply does not follow the code
// that owns it. Adding a type to apply() and forgetting this switch is an easy
// mistake, so it is made a loud one.
func mergeDesired(live, desired client.Object) error {
	switch l := live.(type) {
	case *corev1.ConfigMap:
		// Desired keys win, placement bookkeeping survives. buildConfigMap
		// describes the desired state and knows nothing about the attempt counter
		// the placement reconcile keeps in the same object; a plain assignment
		// would wipe it on every pass, which is every 30 seconds, and the counter
		// would never reach its cap.
		data := desired.(*corev1.ConfigMap).Data
		merged := make(map[string]string, len(data)+2)
		for k, v := range data {
			merged[k] = v
		}
		for k, v := range l.Data {
			if isPlacementBookkeepingKey(k) {
				merged[k] = v
			}
		}
		l.Data = merged
		l.Labels = desired.GetLabels()
	case *corev1.ServiceAccount:
		l.AutomountServiceAccountToken = desired.(*corev1.ServiceAccount).AutomountServiceAccountToken
		l.Labels = desired.GetLabels()
	case *policyv1.PodDisruptionBudget:
		l.Labels = desired.(*policyv1.PodDisruptionBudget).Labels
		// The selector is immutable on a PodDisruptionBudget in the API versions
		// this module runs against, so it is set at creation and left alone; what
		// can change is the budget itself.
		l.Spec.MaxUnavailable = desired.(*policyv1.PodDisruptionBudget).Spec.MaxUnavailable
		l.Spec.MinAvailable = desired.(*policyv1.PodDisruptionBudget).Spec.MinAvailable
	case *corev1.Secret:
		// Data only. Type is immutable once the Secret exists, and the one Secret
		// applied this way — the copier's credentials — is written whole on every
		// pass anyway: its content is two key pairs that are re-checked against
		// their clusters before they are stored.
		l.Data = desired.(*corev1.Secret).Data
	case *corev1.Service:
		d := desired.(*corev1.Service)
		l.Labels = d.Labels
		l.Spec.Selector = d.Spec.Selector
		l.Spec.Ports = d.Spec.Ports
		l.Spec.PublishNotReadyAddresses = d.Spec.PublishNotReadyAddresses
	case *appsv1.StatefulSet:
		d := desired.(*appsv1.StatefulSet)
		l.Labels = d.Labels
		l.Spec.Replicas = d.Spec.Replicas
		l.Spec.Template = d.Spec.Template
	default:
		return fmt.Errorf("mergeDesired has no case for %T: it would be created once and never updated", live)
	}
	return nil
}

// randomHex returns n random bytes hex-encoded.
func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
