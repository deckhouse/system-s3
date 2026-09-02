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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/deckhouse/system-s3/images/controller/pkg/config"
)

// SystemBucketCtrlName names the controller in its logs, in the
// controller-runtime registry and in the controller label on the reconcile
// metrics; one spelling keeps the three joinable.
const SystemBucketCtrlName = "system-bucket"

// SystemBucketReconciler drives every SystemBucket through a short FSM:
//
//		StorageReady -> BucketReady -> CredentialsReady -> Ready
//
//	  - StorageReady reports the module's single data plane (see StoreManager). It
//	    gates everything else: no bucket can be created while Garage is not
//	    serving.
//	  - BucketReady creates the bucket in the backend.
//	  - CredentialsReady mints the access key and writes the S3 credentials Secret
//	    in the SystemBucket's namespace. A change to the
//	    storage.deckhouse.io/rotate annotation issues a fresh key pair.
//
// There is no authorization stage: a SystemBucket is admitted only in a platform
// namespace (see platformNamespace) and grants exactly one consumer — the
// component that created it — full access to its own bucket.
type SystemBucketReconciler struct {
	Client client.Client
	// APIReader is an uncached reader used to look up the credentials Secret and
	// the namespace: a stale cached read of either would make the controller
	// rotate a live key or refuse a legitimate namespace.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Log       logger.Logger
	Cfg       *config.Options
	Store     *StoreManager
	Driver    Driver
}

// Driver is the backend surface the reconciler needs. It is an interface so the
// reconciler can be tested without a Garage instance.
type Driver interface {
	EnsureBucket(ctx context.Context, store *backend.Store, name string) (backend.BucketState, error)
	DeleteBucket(ctx context.Context, store *backend.Store, name string) error
	EnsureAccess(ctx context.Context, store *backend.Store, bucketName, keyName, prevKeyID string, mintFresh bool) (backend.AccessState, error)
	DeleteAccess(ctx context.Context, store *backend.Store, keyID string) error
}

var systemBucketStageOrder = []string{
	v1alpha1.SystemBucketConditionStorageReady,
	v1alpha1.SystemBucketConditionBucketReady,
	v1alpha1.SystemBucketConditionCredentialsReady,
}

// bucketObserved accumulates the status fields written back onto the resource.
type bucketObserved struct {
	endpoint         string
	bucketName       string
	accessKeyID      string
	secretName       string
	observedRotation string
	rotated          bool
}

// AddSystemBucketReconcilerToManager wires the SystemBucket reconciler.
func AddSystemBucketReconcilerToManager(mgr manager.Manager, cfg *config.Options, log logger.Logger, store *StoreManager, driver Driver) error {
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
		Owns(&corev1.Secret{}).
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

	if bucket.Status != nil && bucket.Status.AccessKeyID != "" {
		if err := r.Driver.DeleteAccess(ctx, store, bucket.Status.AccessKeyID); err != nil {
			return fmt.Errorf("revoke access key for SystemBucket %s/%s: %w", bucket.Namespace, bucket.Name, err)
		}
	}
	if err := r.Driver.DeleteBucket(ctx, store, name); err != nil {
		return fmt.Errorf("delete bucket %q: %w", name, err)
	}

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
		status.Set(v1alpha1.SystemBucketConditionStorageReady, metav1.ConditionFalse, "NamespaceNotPlatform",
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

	bstate, err := r.Driver.EnsureBucket(ctx, store, name)
	if bstate.BucketName != "" {
		observed.bucketName = bstate.BucketName
	}
	if !status.Advance(v1alpha1.SystemBucketConditionBucketReady, bstate.Ready, bstate.Message, err) {
		return r.finish(ctx, bucket, status, observed, err)
	}

	// Decide whether to issue a fresh key: on a rotation-annotation change, when
	// no key has been issued yet, or when the credentials Secret is missing.
	rotationValue := bucket.Annotations[v1alpha1.RotateAnnotation]
	observedRotation, recordedKey := "", ""
	if bucket.Status != nil {
		observedRotation = bucket.Status.ObservedRotation
		recordedKey = bucket.Status.AccessKeyID
	}
	existing, secretExists, err := r.getSecret(ctx, bucket)
	if err != nil {
		return r.finish(ctx, bucket, status, observed, err)
	}
	mintFresh := rotationValue != observedRotation || recordedKey == "" || !secretExists

	astate, err := r.Driver.EnsureAccess(ctx, store, name,
		backend.AccessResourceName(bucket.Namespace, bucket.Name), recordedKey, mintFresh)
	if astate.AccessKeyID != "" {
		observed.accessKeyID = astate.AccessKeyID
	} else {
		observed.accessKeyID = recordedKey
	}
	if err != nil || !astate.Ready {
		status.Advance(v1alpha1.SystemBucketConditionCredentialsReady, false, astate.Message, err)
		return r.finish(ctx, bucket, status, observed, err)
	}

	// Write the credentials Secret. A fresh key comes with its secret key; a
	// reused key keeps the secret key already stored in the Secret.
	secretName, err := r.ensureCredentialsSecret(ctx, bucket, &state, &astate, name, existing)
	if !status.Advance(v1alpha1.SystemBucketConditionCredentialsReady, err == nil, "credentials Secret written", err) {
		return r.finish(ctx, bucket, status, observed, err)
	}
	observed.secretName = secretName
	observed.observedRotation = rotationValue
	observed.rotated = astate.SecretAccessKey != ""

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

// credentialsSecretName is spec.credentialsSecretName or <name>-s3-credentials.
func credentialsSecretName(bucket *v1alpha1.SystemBucket) string {
	if bucket.Spec.CredentialsSecretName != "" {
		return bucket.Spec.CredentialsSecretName
	}
	return bucket.Name + "-s3-credentials"
}

// getSecret reads the credentials Secret (nil, false when it does not exist).
// It reads through the uncached APIReader: the result drives the mintFresh
// decision, and a stale cached read (Secret just written but not yet in the
// informer cache) would make it look missing and trigger an unnecessary key
// rotation — revoking a live key.
func (r *SystemBucketReconciler) getSecret(ctx context.Context, bucket *v1alpha1.SystemBucket) (*corev1.Secret, bool, error) {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: bucket.Namespace, Name: credentialsSecretName(bucket)}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return secret, true, nil
}

// ensureCredentialsSecret creates or updates the credentials Secret owned by the
// SystemBucket and returns its name.
func (r *SystemBucketReconciler) ensureCredentialsSecret(
	ctx context.Context,
	bucket *v1alpha1.SystemBucket,
	state *backend.StoreState,
	access *backend.AccessState,
	bucketName string,
	existing *corev1.Secret,
) (string, error) {
	accessKeyID := access.AccessKeyID
	secretKey := access.SecretAccessKey
	if secretKey == "" && existing != nil {
		// Reused key: keep the secret key already stored. Garage cannot recover a
		// secret key after creation, so the Secret is the only copy.
		secretKey = string(existing.Data[v1alpha1.SecretKeySecretAccessID])
		if accessKeyID == "" {
			accessKeyID = string(existing.Data[v1alpha1.SecretKeyAccessKeyID])
		}
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      credentialsSecretName(bucket),
			Namespace: bucket.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Labels == nil {
			secret.Labels = map[string]string{}
		}
		secret.Labels["storage.deckhouse.io/system-bucket"] = bucket.Name
		secret.Type = corev1.SecretTypeOpaque
		secret.StringData = map[string]string{
			v1alpha1.SecretKeyS3Endpoint:     state.Endpoint,
			v1alpha1.SecretKeyS3Region:       state.Region,
			v1alpha1.SecretKeyS3Bucket:       bucketName,
			v1alpha1.SecretKeyAccessKeyID:    accessKeyID,
			v1alpha1.SecretKeySecretAccessID: secretKey,
		}
		return controllerutil.SetControllerReference(bucket, secret, r.Scheme)
	})
	if err != nil {
		return "", err
	}
	return secret.Name, nil
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
	if reconcileErr != nil {
		return ctrl.Result{}, reconcileErr
	}
	if status.Ready() {
		// Re-drive even when Ready, so a bucket or key removed behind the
		// controller's back (directly in Garage) is restored within minutes rather
		// than at the next informer resync.
		return ctrl.Result{RequeueAfter: r.Cfg.ReadyResyncInterval}, nil
	}
	return ctrl.Result{RequeueAfter: r.Cfg.RequeueInterval}, nil
}

func (r *SystemBucketReconciler) updateStatus(
	ctx context.Context,
	bucket *v1alpha1.SystemBucket,
	sb *conditions.Builder,
	observed *bucketObserved,
) error {
	return conditions.UpdateStatus(ctx, r.Client, bucket, func(latest *v1alpha1.SystemBucket) {
		if latest.Status == nil {
			latest.Status = &v1alpha1.SystemBucketStatus{}
		}

		for _, cond := range sb.Conditions() {
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
		if observed.accessKeyID != "" {
			latest.Status.AccessKeyID = observed.accessKeyID
		}
		if observed.secretName != "" {
			latest.Status.SecretRef = &v1alpha1.LocalSecretReference{Name: observed.secretName}
			latest.Status.ObservedRotation = observed.observedRotation
			if observed.rotated {
				now := metav1.Now()
				latest.Status.LastRotationTime = &now
			}
		}
	})
}
