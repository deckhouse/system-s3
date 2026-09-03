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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
const garageVersion = "v2.3.0"

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
	client        client.Client
	apiReader     client.Reader
	restConfig    *rest.Config
	log           logger.Logger
	namespace     string
	image         string
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
func New(c client.Client, apiReader client.Reader, restConfig *rest.Config, log logger.Logger, namespace, image, clusterDomain string) *Driver {
	return &Driver{
		client:        c,
		apiReader:     apiReader,
		restConfig:    restConfig,
		log:           log.Named("garage"),
		namespace:     namespace,
		image:         image,
		clusterDomain: clusterDomain,
	}
}

// EnsureStore reconciles the Garage data plane and reports its state.
func (d *Driver) EnsureStore(ctx context.Context, store *backend.Store) (backend.StoreState, error) {
	state := backend.StoreState{Version: garageVersion}

	if d.image == "" {
		state.Message = "GARAGE_IMAGE is not configured on the controller"
		return state, nil
	}

	if err := d.ensureSecret(ctx, store); err != nil {
		return state, fmt.Errorf("ensure secret: %w", err)
	}
	// The RPC/admin secret exists from here on, so the state can name it.
	state.AdminSecretName = secretName(store)
	pinned, err := d.pinnedState(ctx, store)
	if err != nil {
		return state, fmt.Errorf("read pinned store state: %w", err)
	}

	// A replica-count switch (the singleReplica module setting toggled) cannot be
	// applied in place: it changes the replication factor, which Garage refuses
	// to do on a live cluster. Tear the data plane down instead and let the rest
	// of this reconcile rebuild it empty at the next incarnation. This DESTROYS
	// the stored objects — documented on the setting.
	if want := initialReplicationFactor(store); pinned.exists && pinned.rf != want {
		// And not before the operator has said so in the same change.
		//
		// The module declares this as a CEL rule on its settings too, which is the
		// better place to refuse: the edit never lands. But that rule cannot be the
		// guarantee. Deckhouse skips transition rules when there is no previous
		// value to compare with, so a ModuleConfig that carried no settings until
		// now — everything on defaults, the common case — gets the flip through
		// unexamined; and the rule is not evaluated at all before DKP 1.77. An e2e
		// run flipped singleReplica on such a ModuleConfig and the API server took
		// it without a word.
		//
		// The teardown happens here, so the last word is here.
		if store.RecreateConfirmation != backend.RecreateConfirmationValue {
			state.Message = fmt.Sprintf(
				"refusing to recreate the data plane: storage.singleReplica changed (replication factor %d -> %d), "+
					"which destroys every object in the system object storage. Set storage.recreateConfirmation to %q "+
					"in the same ModuleConfig to proceed, or put storage.singleReplica back",
				pinned.rf, want, backend.RecreateConfirmationValue)
			d.log.Error("refusing a replica-count switch with no acknowledgement",
				"store", store.Name, "pinnedReplicationFactor", pinned.rf, "wantedReplicationFactor", want)
			return state, nil
		}

		done, msg, err := d.teardownDataPlane(ctx, store)
		if err != nil {
			return state, fmt.Errorf("recreate data plane: %w", err)
		}
		if !done {
			state.Message = msg
			return state, nil
		}
		pinned = pinnedState{rf: want, incarnation: pinned.incarnation + 1, exists: true}
		d.log.Info("store torn down for a replica-count switch; rebuilding empty",
			"store", store.Name, "incarnation", pinned.incarnation, "replicationFactor", pinned.rf)
	}

	rf := pinned.rf
	cfgHash := configHash(renderConfig(rf))
	if err := d.apply(ctx, buildConfigMap(store, d.namespace, rf, pinned.incarnation)); err != nil {
		return state, fmt.Errorf("ensure configmap: %w", err)
	}
	if err := d.apply(ctx, buildServiceAccount(store, d.namespace)); err != nil {
		return state, fmt.Errorf("ensure serviceaccount: %w", err)
	}
	if err := d.apply(ctx, buildS3Service(store, d.namespace)); err != nil {
		return state, fmt.Errorf("ensure s3 service: %w", err)
	}
	if err := d.apply(ctx, buildRPCService(store, d.namespace)); err != nil {
		return state, fmt.Errorf("ensure rpc service: %w", err)
	}

	state.Endpoint = s3Endpoint(store, d.namespace, d.clusterDomain)
	state.Region = "garage"

	// The store is backed by node-sticky local PVs the controller provisions
	// itself; the pool must exist before the StatefulSet's PVCs try to bind.
	if err := d.ensureLocalPVs(ctx, store, pinned.incarnation); err != nil {
		return state, fmt.Errorf("ensure local PVs: %w", err)
	}
	// Persist each replica's Garage node identity before any placement move, so
	// a recycled replica (fresh empty PV on another master) rejoins with its
	// stable node ID instead of generating a new one (which strands the old ID
	// as a dead node and churns the layout beyond recovery on consolidate).
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

	workloadReady, workloadMsg, err := d.ensureWorkload(ctx, store, cfgHash)
	if err != nil {
		return state, err
	}
	if !workloadReady {
		state.Message = workloadMsg
		return state, nil
	}

	// Workloads are up: connect the RPC peers and assign the Garage cluster
	// layout.
	mesh, err := d.ensureMeshAndLayout(ctx, store)
	if err != nil {
		return state, err
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
	return state, nil
}

// pinnedState is the store state the controller pins at init and reads back
// afterwards instead of recomputing: the Garage replication factor and the
// data-plane incarnation. exists reports whether the garage.toml ConfigMap
// holding it was found, i.e. whether the store was ever initialized.
type pinnedState struct {
	rf          int32
	incarnation int32
	exists      bool
}

// pinnedState returns the replication_factor to bake into garage.toml plus the
// data-plane incarnation. Garage cannot change replication_factor on a live
// cluster, so it is decided ONCE at cluster init and then pinned: the value is
// read back from the running garage.toml on every subsequent reconcile and never
// recomputed from the current node count. This keeps the factor stable across
// control-plane node-count changes (e.g. masters going 3->1->3) — every node
// keeps the same factor, no data-losing rf change is ever attempted, and the
// cluster merely degrades to read-only (data intact) if the live node count
// drops below it.
//
// The only sanctioned way out of a pinned factor is an explicit recreate of the
// data plane (a replica-count switch), which EnsureStore performs by tearing
// everything down and bumping the incarnation.
func (d *Driver) pinnedState(ctx context.Context, store *backend.Store) (pinnedState, error) {
	existing := &corev1.ConfigMap{}
	err := d.client.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: configName(store)}, existing)
	switch {
	case apierrors.IsNotFound(err):
		// First init: no pinned factor yet, compute it once.
		return pinnedState{rf: initialReplicationFactor(store), incarnation: firstIncarnation}, nil
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
	return pinnedState{rf: rf, incarnation: incarnationFromConfigMap(existing), exists: true}, nil
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
func (d *Driver) ensureWorkload(ctx context.Context, store *backend.Store, cfgHash string) (bool, string, error) {
	sts := buildStatefulSet(store, d.namespace, d.image, cfgHash)
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
