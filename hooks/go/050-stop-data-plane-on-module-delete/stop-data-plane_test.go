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

package stopdataplane

import (
	"context"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/hooks/go/consts"
)

func garageLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/managed-by": "system-s3", "app.kubernetes.io/name": "garage"}
}

// TestDisablingStopsGarageAndKeepsItsData: the Garage StatefulSets go to zero
// replicas and stay, with their claims; the copier Jobs go; nothing else in the
// namespace, and nothing of anybody else's, is touched.
func TestDisablingStopsGarageAndKeepsItsData(t *testing.T) {
	three, one := int32(3), int32(1)
	objs := []client.Object{
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "system-garage-i2", Namespace: consts.ModuleNamespace, Labels: garageLabels()},
			Spec:       appsv1.StatefulSetSpec{Replicas: &three},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: consts.ModuleNamespace},
			Spec:       appsv1.StatefulSetSpec{Replicas: &one},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "system-garage-i1", Namespace: "elsewhere", Labels: garageLabels()},
			Spec:       appsv1.StatefulSetSpec{Replicas: &one},
		},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "system-copy-i3-bulk", Namespace: consts.ModuleNamespace, Labels: garageLabels()}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-system-garage-i2-0", Namespace: consts.ModuleNamespace, Labels: garageLabels()}},
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{appsv1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	ctx := context.Background()

	if err := stopDataPlane(ctx, cl, log.NewNop()); err != nil {
		t.Fatal(err)
	}

	replicas := func(ns, name string) int32 {
		sts := &appsv1.StatefulSet{}
		if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, sts); err != nil {
			t.Fatalf("StatefulSet %s/%s: %v", ns, name, err)
		}
		return *sts.Spec.Replicas
	}
	if got := replicas(consts.ModuleNamespace, "system-garage-i2"); got != 0 {
		t.Errorf("Garage still has %d replicas", got)
	}
	if got := replicas(consts.ModuleNamespace, "unrelated"); got != 1 {
		t.Errorf("a StatefulSet that is not Garage's was scaled to %d", got)
	}
	if got := replicas("elsewhere", "system-garage-i1"); got != 1 {
		t.Errorf("a StatefulSet outside the module namespace was scaled to %d", got)
	}
	err := cl.Get(ctx, client.ObjectKey{Namespace: consts.ModuleNamespace, Name: "system-copy-i3-bulk"}, &batchv1.Job{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("the copier Job is still there: %v", err)
	}
	if err := cl.Get(ctx, client.ObjectKey{Namespace: consts.ModuleNamespace, Name: "data-system-garage-i2-0"}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Errorf("the data plane's claim is gone: %v", err)
	}

	// A second run — Deckhouse may run the hook again — changes nothing and fails nothing.
	if err := stopDataPlane(ctx, cl, log.NewNop()); err != nil {
		t.Fatal(err)
	}
}
