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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-common-lib/logger"
	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/pkg/config"
)

// The reconciler had no tests at all, and the review found three defects inside
// it — the rotation ordering among them. These cover the decisions that are
// conditional, which is where those defects lived.

// fakeConverger stands in for StoreManager: it talks to a real Garage and holds a
// mutex around a whole convergence, so the reconciler could not be driven without
// this seam.
type fakeConverger struct {
	store *backend.Store
	state backend.StoreState
	err   error
}

func (f *fakeConverger) Store() *backend.Store { return f.store }
func (f *fakeConverger) Ensure(context.Context) (backend.StoreState, error) {
	return f.state, f.err
}

// fakeDriver records what the reconciler asked the backend to do, in order.
type fakeDriver struct {
	calls []string

	// mintedKeys is handed out in order, one per EnsureAccess that mints.
	mintedKeys []string
	minted     int

	deletedKeys []string
	ensureErr   error
}

func (f *fakeDriver) EnsureBucket(_ context.Context, _ *backend.Store, name string) (backend.BucketState, error) {
	f.calls = append(f.calls, "EnsureBucket:"+name)
	return backend.BucketState{Ready: true, BucketName: name}, nil
}

func (f *fakeDriver) DeleteBucket(_ context.Context, _ *backend.Store, name string) error {
	f.calls = append(f.calls, "DeleteBucket:"+name)
	return nil
}

func (f *fakeDriver) EnsureAccess(
	_ context.Context, _ *backend.Store, _, _, prevKeyID string, mintFresh bool,
) (backend.AccessState, error) {
	if f.ensureErr != nil {
		return backend.AccessState{}, f.ensureErr
	}
	if !mintFresh && prevKeyID != "" {
		f.calls = append(f.calls, "EnsureAccess:reuse:"+prevKeyID)
		return backend.AccessState{Ready: true, AccessKeyID: prevKeyID}, nil
	}
	key := "AK-minted"
	if f.minted < len(f.mintedKeys) {
		key = f.mintedKeys[f.minted]
	}
	f.minted++
	f.calls = append(f.calls, "EnsureAccess:mint:"+key)
	return backend.AccessState{Ready: true, AccessKeyID: key, SecretAccessKey: "secret-of-" + key}, nil
}

func (f *fakeDriver) DeleteAccess(_ context.Context, _ *backend.Store, keyID string) error {
	f.calls = append(f.calls, "DeleteAccess:"+keyID)
	f.deletedKeys = append(f.deletedKeys, keyID)
	return nil
}

func reconcilerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("client-go scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("api scheme: %v", err)
	}
	return s
}

// platformNS is a namespace the module accepts: the gate is the heritage label,
// not a d8-* name, which a tenant could imitate.
func platformNS(name string) *corev1.Namespace { //nolint:unparam // one caller today; a second namespace is a plausible next test
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{"heritage": "deckhouse"},
	}}
}

func newReconciler(t *testing.T, drv Driver, objs ...client.Object) (*SystemBucketReconciler, client.Client) {
	t.Helper()
	s := reconcilerScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.SystemBucket{}).
		Build()
	return &SystemBucketReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    s,
		Log:       logger.NewNop(),
		Cfg:       &config.Options{ReconcileTimeout: time.Minute, RequeueInterval: time.Second, ReadyResyncInterval: time.Minute},
		Store: &fakeConverger{
			store: &backend.Store{Name: "system"},
			state: backend.StoreState{Ready: true, Endpoint: "http://system-garage:3900", Region: "garage"},
		},
		Driver: drv,
	}, c
}

func bucketFor(ns, name string) *v1alpha1.SystemBucket {
	return &v1alpha1.SystemBucket{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: ns, Finalizers: []string{Finalizer},
	}}
}

// The rotation contract across two passes, which is what !5 changed and could not
// assert: the replaced key is revoked on a later pass, never in the one that
// minted its replacement.
func TestRotationRevokesTheOldKeyOnlyOnTheNextPass(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	drv := &fakeDriver{mintedKeys: []string{"AK1", "AK2"}}
	bucket := bucketFor(ns, name)
	r, c := newReconciler(t, drv, platformNS(ns), bucket)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}

	// First pass: nothing recorded yet, so a key is minted and nothing revoked.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if len(drv.deletedKeys) != 0 {
		t.Fatalf("first pass revoked %v; there was nothing to revoke", drv.deletedKeys)
	}

	// Ask for a rotation.
	live := &v1alpha1.SystemBucket{}
	if err := c.Get(ctx, req.NamespacedName, live); err != nil {
		t.Fatalf("get: %v", err)
	}
	live.Annotations = map[string]string{v1alpha1.RotateAnnotation: "2"}
	if err := c.Update(ctx, live); err != nil {
		t.Fatalf("annotate: %v", err)
	}

	// Second pass: mints the replacement and must NOT revoke yet — the new secret
	// has to be durable first.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("rotation reconcile: %v", err)
	}
	if len(drv.deletedKeys) != 0 {
		t.Fatalf("the replaced key was revoked in the same pass that minted its replacement (%v); "+
			"a crash there leaves the consumer holding a revoked key and loses the new secret", drv.deletedKeys)
	}
	if err := c.Get(ctx, req.NamespacedName, live); err != nil {
		t.Fatalf("get after rotation: %v", err)
	}
	if live.Status == nil || live.Status.SupersededAccessKeyID != "AK1" {
		t.Fatalf("supersededAccessKeyID = %v, want AK1 recorded for a later revoke",
			live.Status)
	}

	// Third pass: now it is safe, and it happens.
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("third reconcile: %v", err)
	}
	if len(drv.deletedKeys) != 1 || drv.deletedKeys[0] != "AK1" {
		t.Fatalf("revoked %v, want exactly [AK1] on the pass after the secret became durable", drv.deletedKeys)
	}
	if err := c.Get(ctx, req.NamespacedName, live); err != nil {
		t.Fatalf("get after revoke: %v", err)
	}
	if live.Status.SupersededAccessKeyID != "" {
		t.Fatalf("supersededAccessKeyID = %q after a successful revoke, want it cleared",
			live.Status.SupersededAccessKeyID)
	}
}

// A namespace without the heritage label is refused where the credentials are
// issued, not only at admission: the webhook can be bypassed.
func TestANonPlatformNamespaceGetsNoCredentials(t *testing.T) {
	const ns, name = "tenant-a", "sneaky"
	drv := &fakeDriver{}
	tenantNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	r, c := newReconciler(t, drv, tenantNS, bucketFor(ns, name))
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, call := range drv.calls {
		t.Fatalf("the backend was touched for a non-platform namespace: %v", call)
	}

	secret := &corev1.Secret{}
	err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: credentialsSecretName(bucketFor(ns, name))}, secret)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("a credentials Secret exists in a tenant namespace (err=%v)", err)
	}
}

// Deletion order: the key is revoked, then the bucket removed, and only then the
// finalizer released. A finalizer dropped first leaves storage in Garage that no
// resource names.
func TestDeleteRevokesThenRemovesThenReleases(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	drv := &fakeDriver{}
	bucket := bucketFor(ns, name)
	bucket.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	bucket.Status = &v1alpha1.SystemBucketStatus{AccessKeyID: "AK1", BucketName: "d8-monitoring-loki"}

	r, c := newReconciler(t, drv, platformNS(ns), bucket)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}

	want := []string{"DeleteAccess:AK1", "DeleteBucket:d8-monitoring-loki"}
	if len(drv.calls) != len(want) || drv.calls[0] != want[0] || drv.calls[1] != want[1] {
		t.Fatalf("calls = %v, want %v", drv.calls, want)
	}

	gone := &v1alpha1.SystemBucket{}
	err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, gone)
	if err == nil && len(gone.Finalizers) > 0 {
		t.Fatalf("the finalizer is still there after a successful teardown: %v", gone.Finalizers)
	}
}

// A backend that fails during teardown must keep the finalizer: releasing it
// would leave a bucket in Garage with no resource accountable for it.
func TestAFailedTeardownKeepsTheFinalizer(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	drv := &fakeDriver{ensureErr: nil}
	bucket := bucketFor(ns, name)
	bucket.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	bucket.Status = &v1alpha1.SystemBucketStatus{AccessKeyID: "AK1"}

	r, c := newReconciler(t, &failingDeleteDriver{fakeDriver: drv}, platformNS(ns), bucket)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err == nil {
		t.Fatal("a failing teardown has to report an error, or nothing retries it")
	}

	still := &v1alpha1.SystemBucket{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, still); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(still.Finalizers) == 0 {
		t.Fatal("the finalizer was released even though the backend teardown failed")
	}
}

type failingDeleteDriver struct{ *fakeDriver }

func (f *failingDeleteDriver) DeleteBucket(_ context.Context, _ *backend.Store, _ string) error {
	return apierrors.NewServiceUnavailable("garage is down")
}

// A Secret that exists but has lost its secret key is the same case as no Secret.
//
// Garage returns a key's secret half once and never again, so reusing a key whose
// only copy is gone writes credentials that cannot work — and then reports Ready
// over them. The consumer gets 403 on every request while the resource says Ready,
// and it never heals: the next pass makes the same decision.
func TestAnEmptySecretKeyForcesAFreshKey(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"

	for _, tc := range []struct {
		name     string
		secret   *corev1.Secret
		wantMint bool
	}{
		{
			name:     "no Secret at all",
			secret:   nil,
			wantMint: true,
		},
		{
			name: "a Secret with a usable secret key",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-s3-credentials", Namespace: ns},
				Data: map[string][]byte{
					v1alpha1.SecretKeyAccessKeyID:    []byte("AK1"),
					v1alpha1.SecretKeySecretAccessID: []byte("secret-of-AK1"),
				},
			},
			wantMint: false,
		},
		{
			// Hand-edited, restored from a backup without data, or a partial first
			// write. Existence alone used to be enough to reuse the key.
			name: "a Secret whose secret key is empty",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-s3-credentials", Namespace: ns},
				Data: map[string][]byte{
					v1alpha1.SecretKeyAccessKeyID:    []byte("AK1"),
					v1alpha1.SecretKeySecretAccessID: []byte(""),
				},
			},
			wantMint: true,
		},
		{
			name: "a Secret with no secret key at all",
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-s3-credentials", Namespace: ns},
				Data:       map[string][]byte{v1alpha1.SecretKeyAccessKeyID: []byte("AK1")},
			},
			wantMint: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drv := &fakeDriver{mintedKeys: []string{"AK-new"}}
			bucket := bucketFor(ns, name)
			bucket.Status = &v1alpha1.SystemBucketStatus{AccessKeyID: "AK1"}

			objs := []client.Object{platformNS(ns), bucket}
			if tc.secret != nil {
				objs = append(objs, tc.secret)
			}
			r, _ := newReconciler(t, drv, objs...)

			if _, err := r.Reconcile(context.Background(),
				ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			minted := false
			reused := false
			for _, c := range drv.calls {
				switch c {
				case "EnsureAccess:mint:AK-new":
					minted = true
				case "EnsureAccess:reuse:AK1":
					reused = true
				}
			}
			if minted != tc.wantMint {
				t.Fatalf("minted a fresh key = %v, want %v (calls %v); reusing a key whose secret "+
					"half is gone writes credentials that cannot work", minted, tc.wantMint, drv.calls)
			}
			if reused == tc.wantMint && tc.wantMint {
				t.Fatalf("the key was reused as well as minted: %v", drv.calls)
			}
		})
	}
}
