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
	"strings"
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

	"github.com/deckhouse/sds-common-lib/conditions"
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

	// sizes records the quota each EnsureBucket was asked for, so a spec.size that
	// never reaches the backend fails a test rather than a cluster.
	sizes []int64
	// usage is what the driver reports back about the bucket.
	usage backend.BucketUsage
}

func (f *fakeDriver) EnsureBucket(_ context.Context, _ *backend.Store, name, owner string, size int64) (backend.BucketState, backend.BucketUsage, error) {
	f.calls = append(f.calls, "EnsureBucket:"+name+":"+owner)
	f.sizes = append(f.sizes, size)
	return backend.BucketState{Ready: true, BucketName: name}, f.usage, nil
}

func (f *fakeDriver) DeleteBucket(_ context.Context, _ *backend.Store, name, owner string) error {
	f.calls = append(f.calls, "DeleteBucket:"+name+":"+owner)
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
		Cfg: &config.Options{
			ReconcileTimeout:    time.Minute,
			RequeueInterval:     time.Second,
			ReadyResyncInterval: time.Minute,
		},
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

func TestBucketStatusReportsUsageAndEmptyBucket(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	bucket := bucketFor(ns, name)
	bucket.Spec.Size = "1Gi"
	drv := &fakeDriver{usage: backend.BucketUsage{
		QuotaBytes: 1 << 30, QuotaObjects: 16384, UsedBytes: 1024, Objects: 3,
	}}
	r, c := newReconciler(t, drv, platformNS(ns), bucket)
	ctx := context.Background()
	key := client.ObjectKey{Namespace: ns, Name: name}
	for _, empty := range []bool{false, true} {
		if empty {
			drv.usage.UsedBytes, drv.usage.Objects = 0, 0
		}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		live := &v1alpha1.SystemBucket{}
		if err := c.Get(ctx, key, live); err != nil {
			t.Fatal(err)
		}
		if live.Status == nil || live.Status.QuotaBytes != drv.usage.QuotaBytes ||
			live.Status.QuotaObjects != drv.usage.QuotaObjects || live.Status.UsedBytes != drv.usage.UsedBytes ||
			live.Status.Objects != drv.usage.Objects {
			t.Fatalf("status does not reflect backend counters: %+v", live.Status)
		}
	}
}

func TestANonPlatformNamespaceGetsNothing(t *testing.T) {
	heritage := map[string]string{"heritage": "deckhouse"}
	for _, tc := range []struct {
		name   string
		ns     string
		labels map[string]string
	}{
		{name: "neither the label nor the name", ns: "tenant-a"},
		{name: "the label without a platform name", ns: "tenant-a", labels: heritage},
		{name: "a platform name without the label", ns: "d8-tenant-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const name = "sneaky"
			drv := &fakeDriver{}
			tenantNS := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tc.ns, Labels: tc.labels}}
			r, c := newReconciler(t, drv, tenantNS, bucketFor(tc.ns, name))
			ctx := context.Background()

			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: tc.ns, Name: name}}); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			for _, call := range drv.calls {
				t.Fatalf("the backend was touched for a non-platform namespace: %v", call)
			}

			cm := &corev1.ConfigMap{}
			err := c.Get(ctx, client.ObjectKey{Namespace: tc.ns, Name: connectionConfigMapName(bucketFor(tc.ns, name))}, cm)
			if !apierrors.IsNotFound(err) {
				t.Fatalf("a connection ConfigMap exists in a tenant namespace (err=%v)", err)
			}
			checkRefusal(t, c, tc.ns, name)
		})
	}
}

// checkRefusal asserts the terminal refusal names both halves of the gate, so
// an operator knows what a namespace is missing.
func checkRefusal(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	live := &v1alpha1.SystemBucket{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, live); err != nil {
		t.Fatal(err)
	}
	cond := findCondition(live.Status.Conditions, v1alpha1.SystemBucketConditionBucketReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != conditions.ReasonReconcileFailed {
		t.Fatalf("BucketReady = %+v, want False/%s", cond, conditions.ReasonReconcileFailed)
	}
	for _, want := range []string{"d8-*", "kube-*", "heritage=deckhouse"} {
		if !strings.Contains(cond.Message, want) {
			t.Errorf("the refusal does not mention %q: %s", want, cond.Message)
		}
	}
}

// Deletion order: the bucket is removed and only then the finalizer released. A
// finalizer dropped first leaves storage in Garage that no resource names.
func TestDeleteRemovesTheBucketThenReleasesTheFinalizer(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	drv := &fakeDriver{}
	bucket := bucketFor(ns, name)
	bucket.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	bucket.Status = &v1alpha1.SystemBucketStatus{BucketName: "d8-monitoring-loki"}

	r, c := newReconciler(t, drv, platformNS(ns), bucket)
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	}

	// The owner is part of the call: the driver refuses to destroy a bucket
	// recorded against a different SystemBucket.
	want := []string{"DeleteBucket:d8-monitoring-loki:d8-monitoring/loki"}
	if len(drv.calls) != len(want) || drv.calls[0] != want[0] {
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
	drv := &fakeDriver{}
	bucket := bucketFor(ns, name)
	bucket.DeletionTimestamp = &metav1.Time{Time: time.Now()}
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

func (f *failingDeleteDriver) DeleteBucket(_ context.Context, _ *backend.Store, _, _ string) error {
	return apierrors.NewServiceUnavailable("garage is down")
}

// Nothing refuses a derived bucket name S3 will not take at admission — the
// module runs no webhook — so the controller is the guard, next to the namespace
// gate, and it has to hold before the bucket is created. Without it the backend
// answered instead, on the first reconcile, and the operator got the refusal from
// Garage rather than a condition naming the field to set.
func TestADerivedBucketNameOverTheS3LimitIsRefusedBeforeTheBackend(t *testing.T) {
	ns := "d8-" + strings.Repeat("n", 40)
	name := strings.Repeat("b", 30)
	drv := &fakeDriver{}
	r, c := newReconciler(t, drv, platformNS(ns), bucketFor(ns, name))
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	for _, call := range drv.calls {
		if strings.HasPrefix(call, "EnsureBucket") || strings.HasPrefix(call, "EnsureAccess") {
			t.Fatalf("the backend was asked to create a bucket that cannot exist: %v", drv.calls)
		}
	}

	got := &v1alpha1.SystemBucket{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, got); err != nil {
		t.Fatalf("get SystemBucket: %v", err)
	}
	if got.Status == nil {
		t.Fatal("no status was written")
	}

	// Terminal, so the phase has to say Error: nothing moves until the resource is
	// given a spec.bucketName, and the resource cannot be renamed. A reason of its
	// own would leave the phase at InProgress — the trap the namespace gate
	// already fell into once.
	if got.Status.Phase != v1alpha1.PhaseError {
		t.Errorf("phase = %q, want %q: the refusal is terminal", got.Status.Phase, v1alpha1.PhaseError)
	}

	var bucketReady *metav1.Condition
	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == v1alpha1.SystemBucketConditionBucketReady {
			bucketReady = &got.Status.Conditions[i]
		}
	}
	if bucketReady == nil {
		t.Fatalf("no %s condition", v1alpha1.SystemBucketConditionBucketReady)
	}
	if bucketReady.Status != metav1.ConditionFalse {
		t.Errorf("%s = %s, want False", bucketReady.Type, bucketReady.Status)
	}
	if !strings.Contains(bucketReady.Message, "spec.bucketName") {
		t.Errorf("the message should say what to set instead, got: %s", bucketReady.Message)
	}
}

// The same resource with a name S3 accepts goes through, so the guard above is
// keyed on the name rather than on the length of the namespace.
func TestAnExplicitBucketNameGetsPastTheNameGuard(t *testing.T) {
	ns := "d8-" + strings.Repeat("n", 40)
	name := strings.Repeat("b", 30)
	drv := &fakeDriver{}
	bucket := bucketFor(ns, name)
	bucket.Spec.BucketName = "loki"
	r, _ := newReconciler(t, drv, platformNS(ns), bucket)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var created bool
	for _, call := range drv.calls {
		if strings.HasPrefix(call, "EnsureBucket") {
			created = true
		}
	}
	if !created {
		t.Errorf("the bucket was not created despite a valid spec.bucketName: %v", drv.calls)
	}
}

// What a consumer is handed now: a ConfigMap saying where to go, and nothing
// that has to be kept secret.
//
// The assertion that matters is the second one. The change this came from was
// "no long-lived credentials anywhere", and the way that regresses is not a
// missing ConfigMap — it is a Secret quietly appearing next to it again.
func TestTheConsumerGetsConnectionParametersAndNoCredentials(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	drv := &fakeDriver{}
	r, c := newReconciler(t, drv, platformNS(ns), bucketFor(ns, name))
	ctx := context.Background()

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: ns, Name: connectionConfigMapName(bucketFor(ns, name))}
	if err := c.Get(ctx, key, cm); err != nil {
		t.Fatalf("the connection ConfigMap was not written: %v", err)
	}
	for _, k := range []string{
		v1alpha1.ConnectionKeyS3Endpoint,
		v1alpha1.ConnectionKeyS3Region,
		v1alpha1.ConnectionKeyS3Bucket,
	} {
		if cm.Data[k] == "" {
			t.Errorf("connection ConfigMap has no %s: %v", k, cm.Data)
		}
	}
	if cm.Data[v1alpha1.ConnectionKeyS3Bucket] != ns+"-"+name {
		t.Errorf("bucket = %q, want the derived name", cm.Data[v1alpha1.ConnectionKeyS3Bucket])
	}

	secrets := &corev1.SecretList{}
	if err := c.List(ctx, secrets, client.InNamespace(ns)); err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Errorf("a Secret was written for the consumer: %v — the module hands out no long-lived credentials",
			secrets.Items[0].Name)
	}

	live := &v1alpha1.SystemBucket{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, live); err != nil {
		t.Fatalf("get: %v", err)
	}
	if live.Status == nil || live.Status.ConnectionRef == nil || live.Status.ConnectionRef.Name != cm.Name {
		t.Errorf("status.connectionRef = %v, want %s", live.Status.ConnectionRef, cm.Name)
	}
}

// TestTheConnectionSpeaksTheSDKsNames: an SDK with the web identity flow needs
// the role, the two endpoints and the region under its own names, and must not
// be handed a role while there is no endpoint to exchange it at.
func TestTheConnectionSpeaksTheSDKsNames(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	for _, sts := range []string{"", "https://system-s3-sts.d8-system-s3.svc"} {
		t.Run("sts="+sts, func(t *testing.T) {
			r, c := newReconciler(t, &fakeDriver{}, platformNS(ns), bucketFor(ns, name))
			r.Cfg.STSEndpoint = sts
			ctx := context.Background()
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			cm := &corev1.ConfigMap{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: connectionConfigMapName(bucketFor(ns, name))}, cm); err != nil {
				t.Fatalf("get: %v", err)
			}

			want := map[string]string{}
			if sts != "" {
				want = map[string]string{
					v1alpha1.ConnectionKeyAWSRoleARN:     "arn:deckhouse:system-s3:::systembucket/d8-monitoring/loki",
					v1alpha1.ConnectionKeyAWSEndpointSTS: sts,
					v1alpha1.ConnectionKeyAWSEndpointS3:  "http://system-garage:3900",
					v1alpha1.ConnectionKeyAWSRegion:      "garage",
				}
			}
			for _, k := range []string{
				v1alpha1.ConnectionKeyAWSRoleARN, v1alpha1.ConnectionKeyAWSEndpointSTS,
				v1alpha1.ConnectionKeyAWSEndpointS3, v1alpha1.ConnectionKeyAWSRegion,
			} {
				if got := cm.Data[k]; got != want[k] {
					t.Errorf("%s=%q, want %q", k, got, want[k])
				}
			}
		})
	}
}

// TestTheConnectionKeepsItsCAWhileTheSecretIsAway: a pass that cannot read the
// certificate Secret — deleted and not yet re-created by the hook that re-issues
// it — must not strip the CA out of every consumer's connection ConfigMap.
func TestTheConnectionKeepsItsCAWhileTheSecretIsAway(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	tlsSecret := func(n, ca string) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "d8-system-s3", Name: n},
			Data:       map[string][]byte{"ca.crt": []byte(ca)},
		}
	}
	r, c := newReconciler(t, &fakeDriver{}, platformNS(ns), bucketFor(ns, name),
		tlsSecret("garage-tls", "garage-ca"), tlsSecret("webhooks-https-certs", "sts-ca"))
	r.Cfg.ControllerNamespace = "d8-system-s3"
	r.Cfg.GarageTLSSecret = "garage-tls"
	r.Cfg.STSCertSecret = "webhooks-https-certs"
	r.Cfg.STSEndpoint = "https://system-s3-sts.d8-system-s3.svc"
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}
	key := client.ObjectKey{Namespace: ns, Name: connectionConfigMapName(bucketFor(ns, name))}
	read := func() map[string]string {
		t.Helper()
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, key, cm); err != nil {
			t.Fatalf("get: %v", err)
		}
		return cm.Data
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if d := read(); d[v1alpha1.ConnectionKeyS3CACert] != "garage-ca" || d[v1alpha1.ConnectionKeySTSCACert] != "sts-ca" {
		t.Fatalf("the CAs were not published: %v", d)
	}

	for _, n := range []string{"garage-tls", "webhooks-https-certs"} {
		if err := c.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "d8-system-s3", Name: n}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile without the Secrets: %v", err)
	}
	if d := read(); d[v1alpha1.ConnectionKeyS3CACert] != "garage-ca" || d[v1alpha1.ConnectionKeySTSCACert] != "sts-ca" {
		t.Errorf("a pass that could not read the Secrets dropped the CAs: %v", d)
	}

	if err := c.Create(ctx, tlsSecret("garage-tls", "new-garage-ca")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if d := read(); d[v1alpha1.ConnectionKeyS3CACert] != "new-garage-ca" {
		t.Errorf("a CA that can be read again is not the one published: %v", d)
	}
}
