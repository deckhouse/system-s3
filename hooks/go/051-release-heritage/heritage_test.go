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

package releaseheritage

import (
	"context"
	"maps"
	"testing"

	"github.com/deckhouse/deckhouse/pkg/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/hooks/go/consts"
)

const rulesWatcher = "prometheus.deckhouse.io/rules-watcher-enabled"

func moduleLabels() map[string]string {
	return map[string]string{heritageLabel: heritageDeckhouse, "module": "system-s3", rulesWatcher: "true"}
}

func garageLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/managed-by": "system-s3", "app.kubernetes.io/name": "garage"}
}

func fakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func labelsOf(t *testing.T, cl client.Client, obj client.Object) map[string]string {
	t.Helper()
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("%T %s: %v", obj, obj.GetName(), err)
	}
	return obj.GetLabels()
}

// TestDisablingReleasesAndEnablingRestoresHeritage: disabling the module takes
// heritage off the namespace and everything in it that had it, and nothing
// else; enabling it puts the label back on exactly those.
func TestDisablingReleasesAndEnablingRestoresHeritage(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: consts.ModuleNamespace, Labels: moduleLabels()}}
	ca := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "system-s3-ca", Namespace: consts.ModuleNamespace, Labels: moduleLabels()}}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-system-garage-0", Namespace: consts.ModuleNamespace,
		Labels: map[string]string{heritageLabel: heritageDeckhouse, "app.kubernetes.io/name": "garage"}}}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "system-garage", Namespace: consts.ModuleNamespace, Labels: garageLabels()}}
	// Somebody else's: another heritage, or another namespace.
	foreign := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: consts.ModuleNamespace,
		Labels: map[string]string{heritageLabel: "upmeter"}}}
	elsewhere := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "d8-monitoring", Labels: moduleLabels()}}

	cl := fakeClient(t, ns, ca, claim, sts, foreign, elsewhere)
	ctx := context.Background()

	if err := releaseHeritage(ctx, cl, log.NewNop(), "test"); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []client.Object{&corev1.Namespace{ObjectMeta: ns.ObjectMeta}, &corev1.Secret{ObjectMeta: ca.ObjectMeta}, &corev1.PersistentVolumeClaim{ObjectMeta: claim.ObjectMeta}} {
		labels := labelsOf(t, cl, obj)
		if _, ok := labels[heritageLabel]; ok {
			t.Errorf("%T %s still has heritage after the module was disabled", obj, obj.GetName())
		}
		if labels[releasedLabel] != heritageDeckhouse {
			t.Errorf("%T %s is not marked as released: %v", obj, obj.GetName(), labels)
		}
	}
	if labels := labelsOf(t, cl, &corev1.Namespace{ObjectMeta: ns.ObjectMeta}); labels[rulesWatcher] != "true" || labels["module"] != "system-s3" {
		t.Errorf("the namespace lost labels other than heritage: %v", labels)
	}
	if labels := labelsOf(t, cl, &appsv1.StatefulSet{ObjectMeta: sts.ObjectMeta}); !maps.Equal(labels, garageLabels()) {
		t.Errorf("an object without heritage was changed: %v", labels)
	}
	if labels := labelsOf(t, cl, &corev1.ConfigMap{ObjectMeta: foreign.ObjectMeta}); labels[heritageLabel] != "upmeter" || labels[releasedLabel] != "" {
		t.Errorf("an object of another heritage was changed: %v", labels)
	}
	if labels := labelsOf(t, cl, &corev1.Secret{ObjectMeta: elsewhere.ObjectMeta}); labels[heritageLabel] != heritageDeckhouse {
		t.Errorf("an object outside the module namespace was released: %v", labels)
	}

	// Deckhouse may run the hook again.
	if err := releaseHeritage(ctx, cl, log.NewNop(), "test"); err != nil {
		t.Fatal(err)
	}

	// Created by somebody while the module was away: no mark, so not the
	// module's to label.
	users := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "users", Namespace: consts.ModuleNamespace}}
	if err := cl.Create(ctx, users); err != nil {
		t.Fatal(err)
	}

	if err := restoreHeritage(ctx, cl, log.NewNop(), "test"); err != nil {
		t.Fatal(err)
	}
	for _, obj := range []client.Object{&corev1.Namespace{ObjectMeta: ns.ObjectMeta}, &corev1.Secret{ObjectMeta: ca.ObjectMeta}, &corev1.PersistentVolumeClaim{ObjectMeta: claim.ObjectMeta}} {
		labels := labelsOf(t, cl, obj)
		if labels[heritageLabel] != heritageDeckhouse {
			t.Errorf("%T %s did not get heritage back: %v", obj, obj.GetName(), labels)
		}
		if _, ok := labels[releasedLabel]; ok {
			t.Errorf("%T %s still carries the release mark: %v", obj, obj.GetName(), labels)
		}
	}
	if labels := labelsOf(t, cl, &corev1.Namespace{ObjectMeta: ns.ObjectMeta}); !maps.Equal(labels, moduleLabels()) {
		t.Errorf("the namespace is not as it was before the module was disabled: %v", labels)
	}
	if labels := labelsOf(t, cl, &corev1.ConfigMap{ObjectMeta: users.ObjectMeta}); len(labels) != 0 {
		t.Errorf("an object created while the module was away was labelled: %v", labels)
	}

	// And again, as before every Helm run.
	if err := restoreHeritage(ctx, cl, log.NewNop(), "test"); err != nil {
		t.Fatal(err)
	}
}

// TestNoNamespaceNothingToDo: the administrator deleted the namespace, which
// is what releasing it was for. Neither direction fails on that.
func TestNoNamespaceNothingToDo(t *testing.T) {
	cl := fakeClient(t)
	if err := releaseHeritage(context.Background(), cl, log.NewNop(), "test"); err != nil {
		t.Fatal(err)
	}
	if err := restoreHeritage(context.Background(), cl, log.NewNop(), "test"); err != nil {
		t.Fatal(err)
	}
}
