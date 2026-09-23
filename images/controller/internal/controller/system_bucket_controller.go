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
	"fmt"
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/monitoring"
	"github.com/deckhouse/system-s3/images/controller/internal/sts"
	"github.com/deckhouse/system-s3/images/controller/pkg/config"
)

// SystemBucketCtrlName names the controller in its logs, in the
// controller-runtime registry and in the controller label on the reconcile
// metrics; one spelling keeps the three joinable.
const SystemBucketCtrlName = "system-bucket"

// SystemBucketReconciler drives every SystemBucket through a short FSM:
//
//		StorageReady -> BucketReady -> ConnectionReady -> Ready
//
//	  - StorageReady reports the module's single data plane (see StoreManager). It
//	    gates everything else: no bucket can be created while Garage is not
//	    serving.
//	  - BucketReady creates the bucket in the backend.
//	  - ConnectionReady writes the ConfigMap that tells a consumer where the
//	    storage is, what its bucket is called and how to ask for credentials.
//
// No credentials are issued here, and none are stored anywhere: a consumer
// exchanges its ServiceAccount token at the credentials endpoint for a key the
// backend stops honouring an hour later (see internal/sts). Who may do that for
// which bucket is an ordinary RBAC grant, so this controller has no authorization
// stage and no key lifecycle at all — nothing to rotate, nothing to revoke,
// nothing to leak.
type SystemBucketReconciler struct {
	Client client.Client
	// APIReader is an uncached reader used to look up the namespace: a stale
	// cached read would refuse a legitimate one.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Log       logger.Logger
	Cfg       *config.Options
	Store     StoreConverger
	Driver    Driver
}

// StoreConverger is the data-plane surface the reconciler needs: the store it
// provisions buckets in, and a way to drive it towards the desired shape.
//
// An interface for the same reason Driver is one — StoreManager talks to a real
// Garage and holds a mutex around a full convergence, so a reconciler that takes
// it concretely cannot be driven in a test. Which is how the whole of
// reconcileNormal came to have no coverage at all.
type StoreConverger interface {
	Store() *backend.Store
	Ensure(ctx context.Context) (backend.StoreState, error)
}

// Driver is the backend surface the reconciler needs. It is an interface so the
// reconciler can be tested without a Garage instance.
type Driver interface {
	EnsureBucket(ctx context.Context, store *backend.Store, name, owner string, size int64) (backend.BucketState, backend.BucketUsage, error)
	DeleteBucket(ctx context.Context, store *backend.Store, name, owner string) error
}

var systemBucketStageOrder = []string{
	v1alpha1.SystemBucketConditionStorageReady,
	v1alpha1.SystemBucketConditionBucketReady,
	v1alpha1.SystemBucketConditionConnectionReady,
}

// bucketObserved accumulates the status fields written back onto the resource.
type bucketObserved struct {
	endpoint       string
	bucketName     string
	connectionName string

	// usage is what the backend says the bucket holds and is allowed to hold. It
	// is published on every pass, including the ones that end early, because a
	// consumer reading "how full am I" wants the last known answer rather than an
	// empty field.
	usage backend.BucketUsage

	// sizeApplied is the SizeApplied condition, when this pass could work it out
	// (see sizeCondition). It sits outside the stage builder: it is not a stage,
	// and a bucket whose volumes are still growing is Ready all the same.
	sizeApplied *metav1.Condition
}

// AddSystemBucketReconcilerToManager wires the SystemBucket reconciler.
func AddSystemBucketReconcilerToManager(mgr manager.Manager, cfg *config.Options, log logger.Logger, store StoreConverger, driver Driver) error {
	r := &SystemBucketReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		Log:       log.Named(SystemBucketCtrlName),
		Cfg:       cfg,
		Store:     store,
		Driver:    driver,
	}

	// Reconcile on spec changes and on rotation-annotation changes.
	bucketPredicate := predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(SystemBucketCtrlName).
		For(&v1alpha1.SystemBucket{}, builder.WithPredicates(bucketPredicate)).
		// The credentials Secret is owned by the SystemBucket, so a consumer (or an
		// operator) deleting it re-drives the bucket and a fresh key pair is issued.
		Owns(&corev1.ConfigMap{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: cfg.MaxConcurrentReconciles}).
		Complete(r)
}

func (r *SystemBucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	// Bound the reconcile: a backend that stops answering must not take the
	// worker with it (see withReconcileTimeout).
	ctx, cancel := withReconcileTimeout(ctx, r.Cfg.ReconcileTimeout)
	defer cancel()

	r.Log.Info("reconcile started", "systemBucket", req.String())

	bucket := &v1alpha1.SystemBucket{}
	if err := r.Client.Get(ctx, req.NamespacedName, bucket); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if bucket.DeletionTimestamp != nil {
		return ctrl.Result{}, r.reconcileDelete(ctx, bucket)
	}

	if !controllerutil.ContainsFinalizer(bucket, Finalizer) {
		controllerutil.AddFinalizer(bucket, Finalizer)
		if err := r.Client.Update(ctx, bucket); err != nil {
			return ctrl.Result{}, err
		}
	}

	return r.reconcileNormal(ctx, bucket)
}

// reconcileDelete revokes the access key and deletes the bucket, then releases
// the finalizer. The credentials Secret is owned by the SystemBucket, so it goes
// with it.
//
// A SystemBucket has no reclaim policy: deleting it deletes the data. That is the
// contract — a platform component owns its system bucket outright — so the
// teardown is unconditional, and it is why the finalizer is kept on any failure
// rather than released best-effort: a bucket left behind in Garage with no
// resource naming it is storage nobody is accountable for.
func (r *SystemBucketReconciler) reconcileDelete(ctx context.Context, bucket *v1alpha1.SystemBucket) error {
	if !controllerutil.ContainsFinalizer(bucket, Finalizer) {
		return nil
	}

	store := r.Store.Store()
	name := effectiveBucketName(bucket)

	// No key is revoked here because none was issued for this resource. The
	// temporary keys handed to consumers of this bucket are grants on the bucket
	// and go with it — they are the only kind of key this module ever mints.
	if err := r.Driver.DeleteBucket(ctx, store, name, bucketOwnerRef(bucket)); err != nil {
		return fmt.Errorf("delete bucket %q: %w", name, err)
	}

	// The series go when the bucket goes. A deleted SystemBucket that keeps
	// publishing its last usage is an alert waiting to fire about something that
	// does not exist.
	monitoring.ForgetBucket(bucket.Namespace, bucket.Name, name)

	controllerutil.RemoveFinalizer(bucket, Finalizer)
	return r.Client.Update(ctx, bucket)
}

func (r *SystemBucketReconciler) reconcileNormal(ctx context.Context, bucket *v1alpha1.SystemBucket) (ctrl.Result, error) {
	status := newStatusBuilder(systemBucketStageOrder, bucket.Generation)
	observed := &bucketObserved{}

	// Re-check the namespace the admission webhook already checked. The webhook
	// can be bypassed — a failurePolicy that lets writes through while the
	// webhook pod is down, or a CRD applied without it — and this is the gate
	// that keeps the system storage out of tenant namespaces, so it is enforced
	// where the credentials are actually issued as well.
	platform, err := r.platformNamespace(ctx, bucket.Namespace)
	if err != nil {
		status.Set(v1alpha1.SystemBucketConditionStorageReady, metav1.ConditionFalse, conditions.ReasonReconcileFailed, err.Error())
		status.GateAfter(v1alpha1.SystemBucketConditionStorageReady)
		return r.finish(ctx, bucket, status, observed, err)
	}
	if !platform {
		// ReconcileFailed, not a reason of its own, and the reason is the phase.
		//
		// Stages.Phase reports Error only when some stage carries the failed reason;
		// anything else leaves it InProgress, and the gated stages below get
		// WaitingForDependency. So a custom reason here made the one genuinely
		// terminal refusal in the whole FSM read as "still working": nothing moves
		// until somebody relabels the namespace, and no reconcile will change that.
		// An operator waiting for a terminal state — or a script that distinguishes
		// Ready from Error — waits forever.
		//
		// The text keeps saying what is actually wrong; only the reason is the one
		// the phase derivation understands.
		status.Set(v1alpha1.SystemBucketConditionStorageReady, metav1.ConditionFalse, conditions.ReasonReconcileFailed,
			fmt.Sprintf("namespace %q is not a Deckhouse platform namespace (label %s=%s); the system object storage is not available to it",
				bucket.Namespace, v1alpha1.LabelHeritage, v1alpha1.HeritageDeckhouse))
		status.GateAfter(v1alpha1.SystemBucketConditionStorageReady)
		return r.finish(ctx, bucket, status, observed, nil)
	}

	// Drive the data plane. Every consumer's reconcile converges it, so the first
	// SystemBucket in a fresh cluster does not wait for the background tick.
	state, err := r.Store.Ensure(ctx)
	observed.endpoint = state.Endpoint
	if !status.Advance(v1alpha1.SystemBucketConditionStorageReady, state.Ready, state.Message, err) {
		return r.finish(ctx, bucket, status, observed, err)
	}

	store := r.Store.Store()
	name := effectiveBucketName(bucket)

	// Re-check the bucket name the admission webhook already checked, for the
	// reason the namespace gate is re-checked above: the webhook can be bypassed.
	//
	// This is a terminal refusal, so it carries the reason the phase derivation
	// understands rather than one of its own — see the namespace gate for why.
	// Nothing moves until the resource is given a spec.bucketName S3 accepts, and
	// the resource cannot be renamed, so an operator reading InProgress here would
	// be waiting for a reconcile that will keep making the same decision.
	if problem := bucketNameProblem(name); problem != "" {
		status.Set(v1alpha1.SystemBucketConditionBucketReady, metav1.ConditionFalse, conditions.ReasonReconcileFailed,
			fmt.Sprintf("the bucket name %q cannot be used: %s. With spec.bucketName unset it is derived as <namespace>-<name>; set spec.bucketName to something S3 accepts",
				name, problem))
		status.GateAfter(v1alpha1.SystemBucketConditionBucketReady)
		return r.finish(ctx, bucket, status, observed, nil)
	}

	size, sizeErr := requestedSize(bucket)
	if sizeErr != nil {
		// A quantity the schema's pattern let through but resource.ParseQuantity
		// does not accept. Terminal for this resource — nothing moves until the
		// field is fixed — so it is reported where the bucket's own failures are
		// reported, and the reconcile stops here rather than creating a bucket with
		// no quota.
		status.Advance(v1alpha1.SystemBucketConditionBucketReady, false,
			fmt.Sprintf("spec.size %q is not a Kubernetes quantity: %v", bucket.Spec.Size, sizeErr), nil)
		status.GateAfter(v1alpha1.SystemBucketConditionBucketReady)
		return r.finish(ctx, bucket, status, observed, nil)
	}

	bstate, usage, err := r.Driver.EnsureBucket(ctx, store, name, bucketOwnerRef(bucket), size)
	if bstate.BucketName != "" {
		observed.bucketName = bstate.BucketName
	}
	observed.usage = usage
	if bstate.BucketName != "" {
		monitoring.BucketUsage(bucket.Namespace, bucket.Name, bstate.BucketName, usage.UsedBytes, usage.QuotaBytes)
	}
	if !status.Advance(v1alpha1.SystemBucketConditionBucketReady, bstate.Ready, bstate.Message, err) {
		return r.finish(ctx, bucket, status, observed, err)
	}
	observed.sizeApplied = sizeCondition(size, usage, state.Capacity)

	// Nothing is minted here and nothing secret is written. What the consumer
	// gets is a ConfigMap saying where the storage is, what its bucket is called
	// and how to ask for credentials — the asking is done with the ServiceAccount
	// token it already has, and RBAC decides whether it may (see internal/sts).
	//
	// That is the whole of the credentials story: there is no key with this
	// bucket's name on it, so there is none to rotate, none to revoke and none to
	// leak.
	connectionName, err := r.ensureConnectionConfigMap(ctx, bucket, &state, name)
	if !status.Advance(v1alpha1.SystemBucketConditionConnectionReady, err == nil,
		"connection parameters written", err) {
		return r.finish(ctx, bucket, status, observed, err)
	}
	observed.connectionName = connectionName

	status.SetReady("All stages reconciled")
	return r.finish(ctx, bucket, status, observed, nil)
}

// platformNamespace reports whether the namespace is one a Deckhouse module
// owns, which is what makes it eligible for the system object storage. The check
// is the heritage: deckhouse label helm_lib stamps on every module namespace —
// not a d8-* name pattern, which a tenant can imitate by naming its own
// namespace that way.
func (r *SystemBucketReconciler) platformNamespace(ctx context.Context, name string) (bool, error) {
	ns := &corev1.Namespace{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: name}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read namespace %q: %w", name, err)
	}
	return ns.Labels[v1alpha1.LabelHeritage] == v1alpha1.HeritageDeckhouse, nil
}

// requestedSize is spec.size in bytes. An empty field reads as zero, which the
// driver takes as "do not touch the quota": the schema defaults the field, so an
// empty one is a resource created before the field existed or written by a client
// that stripped it, and neither is a reason to remove a quota that is in force.
func requestedSize(bucket *v1alpha1.SystemBucket) (int64, error) {
	if bucket.Spec.Size == "" {
		return 0, nil
	}
	q, err := resource.ParseQuantity(bucket.Spec.Size)
	if err != nil {
		return 0, err
	}
	return q.Value(), nil
}

// bucketOwnerRef identifies a SystemBucket in the backend's ownership record.
// "namespace/name" — the object's identity, which is what has to be compared when
// deciding whether a bucket may be handed to this resource.
func bucketOwnerRef(bucket *v1alpha1.SystemBucket) string {
	return bucket.Namespace + "/" + bucket.Name
}

// What a bucket name has to look like. Mirrors the schema of spec.bucketName in
// crds/systembucket.yaml: the point of checking here is that a derived name is
// held to exactly what an explicit one is held to.
const (
	bucketNameMinLength = 3
	bucketNameMaxLength = 63
)

var bucketNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// bucketNameProblem says what is wrong with a bucket name, or returns the empty
// string when nothing is.
//
// The schema checks spec.bucketName, and only spec.bucketName. Left unset the
// name is derived as <namespace>-<name>, and nothing checked that: two labels of
// up to 63 characters each compose to as much as 127, against an S3 limit of 63.
//
// Mirrors bucketNameProblem in images/webhooks; the two must agree, for the same
// reason the two copies of effectiveBucketName must.
func bucketNameProblem(name string) string {
	switch {
	case len(name) < bucketNameMinLength:
		return fmt.Sprintf("it is %d characters long, and S3 bucket names start at %d",
			len(name), bucketNameMinLength)
	case len(name) > bucketNameMaxLength:
		return fmt.Sprintf("it is %d characters long, and S3 bucket names stop at %d",
			len(name), bucketNameMaxLength)
	case !bucketNamePattern.MatchString(name):
		return "it is not a valid S3 bucket name: lower-case letters, digits and " +
			"dashes only, starting and ending with a letter or a digit"
	default:
		return ""
	}
}

// effectiveBucketName is spec.bucketName, or <namespace>-<name> when unset. The
// backend bucket namespace is flat while SystemBuckets are namespaced, so the
// default carries the namespace: two components asking for a "backups" bucket
// must not land on the same one.
func effectiveBucketName(bucket *v1alpha1.SystemBucket) string {
	if bucket.Spec.BucketName != "" {
		return bucket.Spec.BucketName
	}
	return bucket.Namespace + "-" + bucket.Name
}

// connectionConfigMapName is where a consumer finds how to reach its bucket.
// Not configurable: there is nothing in it worth naming differently, and a name
// the resource does not derive is a name something has to look up.
func connectionConfigMapName(bucket *v1alpha1.SystemBucket) string {
	return bucket.Name + "-s3-connection"
}

// ensureConnectionConfigMap creates or updates the ConfigMap owned by the
// SystemBucket that says where the storage is, what the bucket is called and how
// to exchange a ServiceAccount token for credentials. It returns its name.
//
// A ConfigMap and not a Secret: nothing here is secret. That is the point of the
// change it came from — the module mints no long-lived key for anyone, so there
// is no half of a key pair to protect, to rotate, or to lose.
func (r *SystemBucketReconciler) ensureConnectionConfigMap(
	ctx context.Context,
	bucket *v1alpha1.SystemBucket,
	state *backend.StoreState,
	bucketName string,
) (string, error) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      connectionConfigMapName(bucket),
			Namespace: bucket.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels["storage.deckhouse.io/system-bucket"] = bucket.Name
		// The label the controller's cache is scoped to. Without it this object
		// would be invisible to the cached client and to the Owns() watch, so a
		// consumer deleting it by hand would go unnoticed.
		cm.Labels[v1alpha1.ManagedByLabel] = v1alpha1.ManagedByValue
		cm.Data = map[string]string{
			v1alpha1.ConnectionKeyS3Endpoint: state.Endpoint,
			v1alpha1.ConnectionKeyS3Region:   state.Region,
			v1alpha1.ConnectionKeyS3Bucket:   bucketName,
		}
		// The CA the S3 endpoint is verified with. The endpoint is https and its
		// certificate comes from the module's own CA, so a consumer that is not
		// given it cannot talk to the store at all.
		if ca := r.garageCA(ctx); len(ca) > 0 {
			cm.Data[v1alpha1.ConnectionKeyS3CACert] = string(ca)
		}
		// Where to ask for credentials, what audience the token has to carry, and
		// the CA to verify the endpoint with (the module's own, so nothing trusts
		// it by default). Added only while the module actually serves the endpoint,
		// so this never advertises something that does not answer.
		if endpoint := r.stsEndpoint(); endpoint != "" {
			cm.Data[v1alpha1.ConnectionKeySTSEndpoint] = endpoint
			cm.Data[v1alpha1.ConnectionKeySTSAudience] = sts.TokenAudience
			if ca := r.stsCA(ctx); len(ca) > 0 {
				cm.Data[v1alpha1.ConnectionKeySTSCACert] = string(ca)
			}
		}
		return controllerutil.SetControllerReference(bucket, cm, r.Scheme)
	})
	if err != nil {
		return "", err
	}
	return cm.Name, nil
}

// stsEndpoint is the address of the credentials endpoint, or empty when the
// module is not serving one.
//
// Written as an accessor because the configuration may legitimately be absent —
// every test that exercises one method of this reconciler would otherwise have to
// construct a whole config to avoid a nil dereference, and one of them did not,
// which is how this was found.
func (r *SystemBucketReconciler) stsEndpoint() string {
	if r.Cfg == nil {
		return ""
	}
	return r.Cfg.STSEndpoint
}

// stsCA is the CA that signs the credentials endpoint's certificate, read from
// the module's own certificate Secret.
//
// Empty is not an error here: the endpoint's address is still worth publishing,
// and a consumer that cannot verify it will say so far more clearly than a
// reconcile that failed on a Secret it could not read.
// garageCA is the CA of the data plane's certificate, read from the Secret the
// Garage pods serve it from.
func (r *SystemBucketReconciler) garageCA(ctx context.Context) []byte {
	if r.Cfg == nil || r.Cfg.GarageTLSSecret == "" {
		return nil
	}
	return r.secretCA(ctx, r.Cfg.GarageTLSSecret, "the CA of the S3 endpoint")
}

func (r *SystemBucketReconciler) secretCA(ctx context.Context, name, what string) []byte {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: r.Cfg.ControllerNamespace, Name: name}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		r.Log.Error("unable to read "+what, logger.Err(err), "secret", name)
		return nil
	}
	return secret.Data["ca.crt"]
}

func (r *SystemBucketReconciler) stsCA(ctx context.Context) []byte {
	if r.Cfg == nil || r.Cfg.STSCertSecret == "" {
		return nil
	}
	return r.secretCA(ctx, r.Cfg.STSCertSecret, "the CA of the credentials endpoint")
}

func (r *SystemBucketReconciler) finish(
	ctx context.Context,
	bucket *v1alpha1.SystemBucket,
	status *conditions.Builder,
	observed *bucketObserved,
	reconcileErr error,
) (ctrl.Result, error) {
	if err := r.updateStatus(ctx, bucket, status, observed); err != nil {
		r.Log.Error("unable to update the status", logger.Err(err))
		if reconcileErr == nil {
			reconcileErr = err
		}
	}
	// Published from here because this is the one exit every pass goes through,
	// including the ones that stop early: a bucket that never gets past its own
	// failure is exactly the one worth a series.
	monitoring.BucketReady(bucket.Namespace, bucket.Name, status.Ready())

	if reconcileErr != nil {
		return ctrl.Result{}, reconcileErr
	}
	if status.Ready() && observed.sizeApplied != nil && observed.sizeApplied.Status != metav1.ConditionTrue {
		// Ready, but a raised size has not taken effect yet. Growing volumes is
		// minutes, not the resync interval: come back at the plain cadence, so
		// SizeApplied turns True close to when it does.
		return ctrl.Result{RequeueAfter: r.Cfg.RequeueInterval}, nil
	}
	if status.Ready() {
		// Re-drive even when Ready, so a bucket or key removed behind the
		// controller's back (directly in Garage) is restored within minutes rather
		// than at the next informer resync — and so the scheduled rotation happens
		// close to when it is due rather than at whatever the resync lands on.
		return ctrl.Result{RequeueAfter: r.nextWakeup(bucket)}, nil
	}
	return ctrl.Result{RequeueAfter: r.Cfg.RequeueInterval}, nil
}

// nextWakeup is how long to wait before driving a Ready bucket again: the resync
// cadence, unless something with a deadline comes first.
//
// The two deadlines are the scheduled rotation and the expiry of a replaced key.
// Neither is urgent to the minute — the rotation may as well happen a few minutes
// late, and the expired key is deleted by the backend's clock either way — but
// waiting a full resync past them makes the status read wrong for that long
// (nextRotationTime in the past, a superseded key that is expired and still
// recorded), and a status that is routinely stale is a status nobody trusts.
func (r *SystemBucketReconciler) nextWakeup(_ *v1alpha1.SystemBucket) time.Duration {
	wait := r.Cfg.ReadyResyncInterval
	// Never shorter than the plain requeue interval: the two are configured
	// independently and a resync shorter than the requeue would be a spin.
	if wait < r.Cfg.RequeueInterval {
		wait = r.Cfg.RequeueInterval
	}
	return wait
}

func (r *SystemBucketReconciler) updateStatus(
	ctx context.Context,
	bucket *v1alpha1.SystemBucket,
	sb *conditions.Builder,
	observed *bucketObserved,
) error {
	// Read through APIReader, write through Client.
	//
	// UpdateStatus re-reads with whatever client it is given, and this one was
	// given the cached one. RetryOnConflict re-reads on every attempt precisely to
	// get the version the conflict was about; a cached read returns the same stale
	// copy each time, so the retry re-mutates the object that already lost and
	// conflicts again until the informer catches up. UpdateStatusVia exists to
	// separate the two.
	read := func(ctx context.Context) (*v1alpha1.SystemBucket, error) {
		fresh := &v1alpha1.SystemBucket{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(bucket), fresh); err != nil {
			return nil, err
		}
		return fresh, nil
	}
	write := func(ctx context.Context, obj *v1alpha1.SystemBucket) error {
		return r.Client.Status().Update(ctx, obj)
	}

	return conditions.UpdateStatusVia(ctx, read, write, func(latest *v1alpha1.SystemBucket) {
		if latest.Status == nil {
			latest.Status = &v1alpha1.SystemBucketStatus{}
		}

		for _, cond := range sb.Conditions() {
			conditions.Set(&latest.Status.Conditions, cond)
		}
		if observed.sizeApplied != nil {
			cond := *observed.sizeApplied
			cond.ObservedGeneration = bucket.Generation
			conditions.Set(&latest.Status.Conditions, cond)
		}
		latest.Status.ObservedGeneration = bucket.Generation
		latest.Status.Phase = derivePhase(latest.Status.Conditions, systemBucketStageOrder)

		if observed.endpoint != "" {
			latest.Status.Endpoint = observed.endpoint
		}
		if observed.bucketName != "" {
			latest.Status.BucketName = observed.bucketName
		}
		if observed.usage != (backend.BucketUsage{}) {
			latest.Status.QuotaBytes = observed.usage.QuotaBytes
			latest.Status.UsedBytes = observed.usage.UsedBytes
			latest.Status.Objects = observed.usage.Objects
		}
		if observed.connectionName != "" {
			latest.Status.ConnectionRef = &v1alpha1.LocalObjectReference{Name: observed.connectionName}
		}
	})
}
