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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

const firstBucketNS = "d8-system-s3"

func firstBucket(name string) *v1alpha1.SystemBucket {
	return &v1alpha1.SystemBucket{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "d8-monitoring", Finalizers: []string{"test"}},
		Spec:       v1alpha1.SystemBucketSpec{Size: "1Gi"},
	}
}

// nothingBuilt fails the test when the namespace holds any object the
// controller creates for a data plane.
func nothingBuilt(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	in := client.InNamespace(firstBucketNS)
	for what, list := range map[string]client.ObjectList{
		"StatefulSet": &appsv1.StatefulSetList{},
		"Secret":      &corev1.SecretList{},
		"ConfigMap":   &corev1.ConfigMapList{},
		"Service":     &corev1.ServiceList{},
		"PVC":         &corev1.PersistentVolumeClaimList{},
	} {
		if err := c.List(ctx, list, in); err != nil {
			t.Fatal(err)
		}
		items, err := apimeta.ExtractList(list)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			obj, _ := apimeta.Accessor(item)
			t.Errorf("a %s %q was created with no SystemBucket asking for storage", what, obj.GetName())
		}
	}
}

// TestNoBucketsBuildsNothing: an enabled module with no SystemBucket creates no
// data plane, and says so without claiming that anything is wrong with one.
// Settings changed meanwhile — the mode, the class — start no migration and no
// resize: they only change what the first bucket gets.
func TestNoBucketsBuildsNothing(t *testing.T) {
	ctx := context.Background()
	deleting := firstBucket("going")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(deleting).Build()
	d := testDriver(t, c, firstBucketNS)

	for _, store := range []*backend.Store{
		systemStore(),
		nonHAStore(),
		{Name: "system", HighAvailability: true, StorageClass: "another"},
	} {
		state, err := d.EnsureStore(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		if !state.AwaitingFirstBucket || state.Ready {
			t.Fatalf("awaiting=%v ready=%v, want a store waiting for its first bucket", state.AwaitingFirstBucket, state.Ready)
		}
		storage := storeCondition(t, state, backend.StorageReady)
		if storage.Status != metav1.ConditionFalse || storage.Reason != WaitingForFirstBucketReason ||
			!strings.Contains(storage.Message, "first one is created") {
			t.Errorf("StorageReady %+v", storage)
		}
		for _, kind := range backend.StoreConditionTypes[1:] {
			if c := storeCondition(t, state, kind); c.Status != metav1.ConditionTrue || c.Reason != NoDataPlaneReason {
				t.Errorf("%s %+v, want True/%s", kind, c, NoDataPlaneReason)
			}
		}
		if state.MigrationPhase != "" {
			t.Errorf("a migration phase %q is reported with no data plane", state.MigrationPhase)
		}
	}
	nothingBuilt(t, c)
}

// TestNoBucketsAndNoClassSaysBoth: the missing class is a note on the wait, not
// a failure of a store nobody asked for.
func TestNoBucketsAndNoClassSaysBoth(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).Build()
	d := testDriver(t, c, firstBucketNS)
	state, err := d.EnsureStore(context.Background(), &backend.Store{Name: "system", HighAvailability: true})
	if err != nil {
		t.Fatal(err)
	}
	storage := storeCondition(t, state, backend.StorageReady)
	if storage.Reason != WaitingForFirstBucketReason || !strings.Contains(storage.Message, "storageClass") {
		t.Errorf("StorageReady %+v does not name both the wait and the setting", storage)
	}
	nothingBuilt(t, c)
}

// TestFirstBucketBuildsAndLastBucketKeepsTheDataPlane: the first SystemBucket
// builds the data plane, and deleting every SystemBucket afterwards leaves it
// where it is.
func TestFirstBucketBuildsAndLastBucketKeepsTheDataPlane(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).Build()
	d := testDriver(t, c, firstBucketNS)
	store := systemStore()

	if state, err := d.EnsureStore(ctx, store); err != nil || !state.AwaitingFirstBucket {
		t.Fatalf("before the first bucket: awaiting=%v err=%v", state.AwaitingFirstBucket, err)
	}

	bucket := firstBucket("loki")
	if err := c.Create(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	state, err := d.EnsureStore(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if state.AwaitingFirstBucket {
		t.Fatal("the first SystemBucket did not start the data plane")
	}
	if c := storeCondition(t, state, backend.StorageReady); c.Reason == WaitingForFirstBucketReason {
		t.Errorf("StorageReady still waits for a bucket: %+v", c)
	}
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: firstBucketNS, Name: resourceName(store)}, sts); err != nil {
		t.Fatalf("the data plane was not built: %v", err)
	}

	// The finalizer keeps it as the controller would, on its way out.
	if err := c.Delete(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	state, err = d.EnsureStore(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if state.AwaitingFirstBucket {
		t.Fatal("deleting the last SystemBucket put the store back to waiting")
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: firstBucketNS, Name: resourceName(store)}, sts); err != nil {
		t.Fatalf("the data plane went with the last bucket: %v", err)
	}
}

// TestLeftoverClaimsAreADataPlane: claims with no StatefulSet — one deleted by
// hand, say — are a store with data in it, and the pass builds it again rather
// than waiting for a bucket to ask.
func TestLeftoverClaimsAreADataPlane(t *testing.T) {
	store := systemStore()
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-" + resourceName(store) + "-0", Namespace: firstBucketNS, Labels: planeLabels(store),
	}}
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(claim).Build()
	d := testDriver(t, c, firstBucketNS)
	awaiting, err := d.awaitingFirstBucket(context.Background(), store)
	if err != nil || awaiting {
		t.Fatalf("awaiting=%v err=%v with the store's claims in the namespace", awaiting, err)
	}
}
