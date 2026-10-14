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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
)

// TestMetricsTokenIsCreatedOnceAndKept: an installation upgraded into this code
// gets the Secret on its first pass, and a Secret that exists is never rewritten —
// the running pods hold its token and would answer the next scrape with a 403.
func TestMetricsTokenIsCreatedOnceAndKept(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	if err := d.ensureSecret(ctx, systemStore()); err != nil {
		t.Fatalf("ensureSecret: %v", err)
	}
	got := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "d8-system-s3", Name: metricsTokenSecret}, got); err != nil {
		t.Fatalf("the metrics token Secret was not created: %v", err)
	}
	token := string(got.Data[metricsTokenKey])
	if len(token) != 64 {
		t.Errorf("token=%q, want 32 random bytes in hex", token)
	}
	// The controller's cache sees only what carries the label; without it the
	// next pass would not find the Secret and try to create it again.
	if got.Labels[v1alpha1.ManagedByLabel] != v1alpha1.ManagedByValue {
		t.Errorf("labels=%v, want %s=%s", got.Labels, v1alpha1.ManagedByLabel, v1alpha1.ManagedByValue)
	}

	plane := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "d8-system-s3", Name: secretName(systemStore())}, plane); err != nil {
		t.Fatalf("plane Secret: %v", err)
	}
	if string(plane.Data[secretKeyAdmin]) == token {
		t.Errorf("the metrics token is the admin token")
	}

	if err := d.ensureSecret(ctx, systemStore()); err != nil {
		t.Fatalf("second ensureSecret: %v", err)
	}
	again := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "d8-system-s3", Name: metricsTokenSecret}, again); err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(again.Data[metricsTokenKey]) != token {
		t.Errorf("the token was rewritten on the second pass")
	}
}

// TestExistingMetricsTokenIsNotReplaced covers the Secret written by a previous
// controller, which this one must take as it is.
func TestExistingMetricsTokenIsNotReplaced(t *testing.T) {
	ctx := context.Background()
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: metricsTokenSecret, Namespace: "d8-system-s3"},
		Data:       map[string][]byte{metricsTokenKey: []byte("kept")},
	}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(existing).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	if err := d.ensureMetricsToken(ctx); err != nil {
		t.Fatalf("ensureMetricsToken: %v", err)
	}
	got := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "d8-system-s3", Name: metricsTokenSecret}, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Data[metricsTokenKey]) != "kept" {
		t.Errorf("token=%q, want the existing one", got.Data[metricsTokenKey])
	}
}

// TestGarageReadsTheMetricsTokenFromItsOwnSecret: the env var is what closes
// /metrics, and it must point at the shared Secret the monitor reads, not at the
// plane's — podSpec fills in the plane's name only where none is set.
func TestGarageReadsTheMetricsTokenFromItsOwnSecret(t *testing.T) {
	for _, store := range []string{"first", "second"} {
		s := systemStore()
		if store == "second" {
			s.Incarnation = 2
		}
		spec := podSpec(s, "garage:v1")
		refs := map[string]*corev1.SecretKeySelector{}
		for _, e := range spec.Containers[0].Env {
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				refs[e.Name] = e.ValueFrom.SecretKeyRef
			}
		}
		m := refs["GARAGE_METRICS_TOKEN"]
		if m == nil || m.Name != metricsTokenSecret || m.Key != metricsTokenKey {
			t.Errorf("%s plane: GARAGE_METRICS_TOKEN=%+v, want %s/%s", store, m, metricsTokenSecret, metricsTokenKey)
		}
		a := refs["GARAGE_ADMIN_TOKEN"]
		if a == nil || a.Name != secretName(s) {
			t.Errorf("%s plane: GARAGE_ADMIN_TOKEN=%+v, want the plane's Secret %s", store, a, secretName(s))
		}
	}
}
