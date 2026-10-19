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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/sds-common-lib/logger"
)

// testGarageLogger routes the driver's log into the test's output.
func testGarageLogger(t *testing.T) logger.Logger {
	t.Helper()
	l, err := logger.NewLoggerToWriter(&garageTestWriter{t: t}, logger.InfoLevel)
	if err != nil {
		t.Fatalf("build logger: %v", err)
	}
	return l
}

type garageTestWriter struct{ t *testing.T }

func (w *garageTestWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// Two SystemBuckets must not both be given the same bucket.
//
// They would write to the same storage unaware of each other, and — a SystemBucket
// owning its data outright, with no reclaim policy — deleting either would take
// the other's objects. Nothing rejects the second resource at admission — the
// module runs no webhook, and admission is not serialised anyway (two
// simultaneous CREATEs each see the other missing) — so the claim has to hold
// here.
func TestBucketOwnership(t *testing.T) {
	ns := "d8-system-s3"
	store := systemStore()
	ctx := context.Background()

	newDriver := func(t *testing.T, objs ...client.Object) (*Driver, client.Client) {
		t.Helper()
		c := fake.NewClientBuilder().WithScheme(attemptScheme(t)).WithObjects(objs...).Build()
		return &Driver{client: c, apiReader: c, namespace: ns}, c
	}

	t.Run("first claim wins, a second owner is told who holds it", func(t *testing.T) {
		d, _ := newDriver(t)

		other, err := d.claimBucket(ctx, store, "loki", "d8-monitoring/loki")
		if err != nil || other != "" {
			t.Fatalf("first claim: other=%q err=%v, want \"\"/nil", other, err)
		}
		// The same owner reconciling again is the normal case.
		other, err = d.claimBucket(ctx, store, "loki", "d8-monitoring/loki")
		if err != nil || other != "" {
			t.Fatalf("re-claim by the owner: other=%q err=%v, want \"\"/nil", other, err)
		}
		other, err = d.claimBucket(ctx, store, "loki", "d8-other/logs")
		if err != nil {
			t.Fatalf("competing claim: %v", err)
		}
		if other != "d8-monitoring/loki" {
			t.Fatalf("competing claim: other=%q, want the first owner", other)
		}
	})

	t.Run("release only by the holder", func(t *testing.T) {
		d, c := newDriver(t)
		if _, err := d.claimBucket(ctx, store, "loki", "d8-monitoring/loki"); err != nil {
			t.Fatalf("claim: %v", err)
		}

		// A SystemBucket that was refused the bucket must not free somebody else's
		// claim on its way out.
		if err := d.releaseBucket(ctx, store, "loki", "d8-other/logs"); err != nil {
			t.Fatalf("release by a non-holder: %v", err)
		}
		owner, err := d.bucketOwner(ctx, store, "loki")
		if err != nil || owner != "d8-monitoring/loki" {
			t.Fatalf("owner=%q err=%v, want the claim intact", owner, err)
		}

		if err := d.releaseBucket(ctx, store, "loki", "d8-monitoring/loki"); err != nil {
			t.Fatalf("release by the holder: %v", err)
		}
		owner, err = d.bucketOwner(ctx, store, "loki")
		if err != nil || owner != "" {
			t.Fatalf("owner=%q err=%v, want the claim gone", owner, err)
		}

		// The record itself stays (other buckets may be recorded in it).
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: bucketOwnersName(store)}, cm); err != nil {
			t.Fatalf("get ownership record: %v", err)
		}
	})

	t.Run("no record: nothing is owned", func(t *testing.T) {
		d, _ := newDriver(t)
		owner, err := d.bucketOwner(ctx, store, "loki")
		if err != nil || owner != "" {
			t.Fatalf("owner=%q err=%v, want \"\"/nil on a store that claimed nothing", owner, err)
		}
		// Releasing what was never claimed is not an error either.
		if err := d.releaseBucket(ctx, store, "loki", "d8-monitoring/loki"); err != nil {
			t.Fatalf("release with no record: %v", err)
		}
	})

	t.Run("EnsureBucket refuses a bucket held by another SystemBucket", func(t *testing.T) {
		// The claim is checked before anything reaches Garage, which is also why
		// this needs no admin endpoint: the refusal happens first.
		d, _ := newDriver(t, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: bucketOwnersName(store), Namespace: ns},
			Data:       map[string]string{"loki": "d8-monitoring/loki"},
		})

		state, _, err := d.EnsureBucket(ctx, store, "loki", "d8-other/logs", 1<<30)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if state.Ready {
			t.Fatal("Ready=true for a bucket owned by another SystemBucket")
		}
		if !strings.Contains(state.Message, "d8-monitoring/loki") {
			t.Errorf("message %q does not name the owner", state.Message)
		}
	})

	t.Run("DeleteBucket leaves another owner's bucket alone", func(t *testing.T) {
		// Same reasoning in reverse: the refused resource has to be able to go away,
		// so this returns success — but it must not touch the data plane, which the
		// absence of an admin endpoint here proves (destroying would have to talk to
		// Garage, and the test would fail on the attempt).
		// Reaching the data plane is what must not happen, so the test watches for
		// the read that would start it: adminClientFor's fetch of the admin token.
		adminTokenReads := 0
		c := fake.NewClientBuilder().WithScheme(attemptScheme(t)).
			WithObjects(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: bucketOwnersName(store), Namespace: ns},
				Data:       map[string]string{"loki": "d8-monitoring/loki"},
			}).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*corev1.Secret); ok {
						adminTokenReads++
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			}).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

		if err := d.DeleteBucket(ctx, store, "loki", "d8-other/logs"); err != nil {
			t.Fatalf("delete by a non-owner: %v", err)
		}
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: bucketOwnersName(store)}, cm); err != nil {
			t.Fatalf("get ownership record: %v", err)
		}
		if cm.Data["loki"] != "d8-monitoring/loki" {
			t.Errorf("ownership record = %v, want the real owner's claim intact", cm.Data)
		}
		if adminTokenReads != 0 {
			t.Errorf("the delete reached for the admin token %d time(s): a non-owner's teardown must not touch the bucket at all", adminTokenReads)
		}
	})
}
