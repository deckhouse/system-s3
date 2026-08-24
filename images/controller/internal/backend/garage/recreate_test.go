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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-object-system/images/controller/internal/backend"
)

// teardownScheme carries the object kinds the System teardown touches.
func teardownScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	return s
}

// dataPlane is the full set of objects a running store owns that the teardown has
// to remove.
func dataPlane(store *backend.Store, ns string) []client.Object {
	return []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(store), Namespace: ns, Labels: commonLabels(store),
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + resourceName(store) + "-0", Namespace: ns, Labels: commonLabels(store),
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + resourceName(store) + "-1", Namespace: ns, Labels: commonLabels(store),
		}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: nodeIdentitySecretName(store), Namespace: ns, Labels: commonLabels(store),
		}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
			Name:   localPVName(store, "m0", firstIncarnation, 0),
			Labels: map[string]string{storeLabel: store.Name, labelSystemLocalNode: "m0"},
		}},
	}
}

func TestTeardownDataPlane(t *testing.T) {
	ns := "d8-sds-object-system"
	store := systemStore()
	ctx := context.Background()

	c := fake.NewClientBuilder().WithScheme(teardownScheme(t)).
		WithObjects(dataPlane(store, ns)...).Build()
	d := &Driver{client: c, apiReader: c, namespace: ns}

	// First pass: everything is still there, so it reports "not done" and says what
	// it is waiting for (the caller surfaces this as the cluster message).
	done, msg, err := d.teardownDataPlane(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if done {
		t.Errorf("done=true on the pass that issued the deletes, want false")
	}
	if msg == "" {
		t.Errorf("expected a progress message while objects remain")
	}

	// The fake client deletes synchronously (no pods, no protection finalizers), so
	// the next pass finds an empty data plane.
	done, _, err = d.teardownDataPlane(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error on the second pass: %v", err)
	}
	if !done {
		t.Fatalf("done=false, want the teardown to complete once nothing is left")
	}

	for _, obj := range dataPlane(store, ns) {
		key := client.ObjectKeyFromObject(obj)
		if err := c.Get(ctx, key, obj); err == nil {
			t.Errorf("%T %q survived the teardown", obj, key.Name)
		}
	}

	// Idempotent on an already-empty data plane.
	if done, _, err := d.teardownDataPlane(ctx, store); err != nil || !done {
		t.Errorf("teardown on an empty data plane: done=%v err=%v, want true/nil", done, err)
	}
}

// TestTeardownDataPlaneKeepsForeignObjects guards the label selectors: objects
// labelled for a different store name must not be swept up by the recreate.
func TestTeardownDataPlaneKeepsForeignObjects(t *testing.T) {
	ns := "d8-sds-object-system"
	system := systemStore()
	other := &backend.Store{Name: "other", StorageClass: localSC}
	ctx := context.Background()

	objs := append(dataPlane(system, ns), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + resourceName(other) + "-0", Namespace: ns, Labels: commonLabels(other),
		},
	})
	c := fake.NewClientBuilder().WithScheme(teardownScheme(t)).WithObjects(objs...).Build()
	d := &Driver{client: c, apiReader: c, namespace: ns}

	if _, _, err := d.teardownDataPlane(ctx, system); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key := client.ObjectKey{Namespace: ns, Name: "data-" + resourceName(other) + "-0"}
	if err := c.Get(ctx, key, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Errorf("the Lightweight store's PVC was deleted: %v", err)
	}
}
