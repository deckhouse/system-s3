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

package handlers

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func listKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		systemBucketGVR: "SystemBucketList",
		namespaceGVR:    "NamespaceList",
	}
}

func sbObj(namespace, name, bucketName string) *unstructured.Unstructured {
	spec := map[string]interface{}{}
	if bucketName != "" {
		spec["bucketName"] = bucketName
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SystemBucket",
		"metadata":   map[string]interface{}{"name": name, "namespace": namespace},
		"spec":       spec,
	}}
}

func nsObj(name string, labels map[string]string) *unstructured.Unstructured {
	meta := map[string]interface{}{"name": name}
	if labels != nil {
		l := map[string]interface{}{}
		for k, v := range labels {
			l[k] = v
		}
		meta["labels"] = l
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   meta,
	}}
}

func platformNS(name string) *unstructured.Unstructured {
	return nsObj(name, map[string]string{labelHeritage: heritageDeckhouse})
}

func newFakeValidator(objs ...runtime.Object) *Validator {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds(), objs...)
	return NewValidator(dyn)
}

func TestEffectiveBucketName(t *testing.T) {
	// The default carries the namespace: the backend bucket namespace is flat, so
	// "backups" in two module namespaces must not resolve to one bucket.
	if got := effectiveBucketName(sbObj("d8-monitoring", "backups", "")); got != "d8-monitoring-backups" {
		t.Errorf("effectiveBucketName(default)=%q, want d8-monitoring-backups", got)
	}
	if got := effectiveBucketName(sbObj("d8-monitoring", "backups", "custom")); got != "custom" {
		t.Errorf("effectiveBucketName(explicit)=%q, want custom", got)
	}
}

func TestSystemBucketValidateNamespaceGate(t *testing.T) {
	ctx := context.Background()

	t.Run("platform namespace is admitted", func(t *testing.T) {
		v := newFakeValidator(platformNS("d8-monitoring"))
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("d8-monitoring", "loki", ""))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Valid {
			t.Errorf("platform namespace rejected: %s", res.Message)
		}
	})

	// A tenant naming its own namespace d8-something is exactly the case a name
	// pattern would have admitted, which is why the check is on the label.
	t.Run("unlabelled namespace is rejected even when named like a module one", func(t *testing.T) {
		v := newFakeValidator(nsObj("d8-not-really", nil))
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("d8-not-really", "loki", ""))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Valid {
			t.Errorf("a namespace without the heritage label must be rejected")
		}
	})

	t.Run("missing namespace is rejected", func(t *testing.T) {
		v := newFakeValidator()
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("gone", "loki", ""))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Valid {
			t.Errorf("a SystemBucket in a namespace that does not exist must be rejected")
		}
	})
}

func TestSystemBucketValidateNameCollision(t *testing.T) {
	ctx := context.Background()

	t.Run("another resource already maps to the bucket name", func(t *testing.T) {
		v := newFakeValidator(
			platformNS("d8-monitoring"), platformNS("d8-logging"),
			sbObj("d8-logging", "shared", "shared-bucket"),
		)
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("d8-monitoring", "own", "shared-bucket"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Valid {
			t.Errorf("two SystemBuckets must not map to one backend bucket")
		}
	})

	t.Run("the same resource updating itself is not a collision", func(t *testing.T) {
		v := newFakeValidator(
			platformNS("d8-monitoring"),
			sbObj("d8-monitoring", "loki", "loki-bucket"),
		)
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("d8-monitoring", "loki", "loki-bucket"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Valid {
			t.Errorf("updating a SystemBucket must not collide with itself: %s", res.Message)
		}
	})

	t.Run("default names in different namespaces do not collide", func(t *testing.T) {
		v := newFakeValidator(
			platformNS("d8-monitoring"), platformNS("d8-logging"),
			sbObj("d8-logging", "backups", ""),
		)
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("d8-monitoring", "backups", ""))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Valid {
			t.Errorf("same name in two namespaces must be allowed: %s", res.Message)
		}
	})
}

// terminating marks the object as being deleted, the way the API server does
// once a delete has been issued on an object that still carries a finalizer.
func terminating(u *unstructured.Unstructured) *unstructured.Unstructured {
	out := u.DeepCopy()
	meta, _, _ := unstructured.NestedMap(out.Object, "metadata")
	if meta == nil {
		meta = map[string]interface{}{}
	}
	meta["deletionTimestamp"] = "2026-09-02T17:12:06Z"
	meta["finalizers"] = []interface{}{"storage.deckhouse.io/system-s3-controller"}
	_ = unstructured.SetNestedMap(out.Object, meta, "metadata")
	return out
}

// A SystemBucket the webhook would refuse must still be deletable.
//
// Deleting one does not remove the object: the controller holds a finalizer, and
// removing that finalizer is an UPDATE that comes back through this webhook. So a
// resource this webhook refuses — a bucket-name collision, or a namespace that
// lost its heritage label — used to become immortal: the controller's finalizer
// patch was rejected on every pass, and the object sat in Terminating for as long
// as the module existed. An e2e run produced exactly that: hundreds of reconciles,
// each logging "denied the request: bucket name ... is already used by".
func TestSystemBucketValidateAdmitsTerminatingObjects(t *testing.T) {
	ctx := context.Background()

	t.Run("a colliding name does not block the finalizer patch", func(t *testing.T) {
		v := newFakeValidator(
			platformNS("d8-monitoring"), platformNS("d8-logging"),
			sbObj("d8-logging", "shared", "shared-bucket"),
		)
		res, err := v.SystemBucketValidate(ctx, nil, terminating(sbObj("d8-monitoring", "own", "shared-bucket")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Valid {
			t.Errorf("a terminating SystemBucket was refused (%s); it can never be deleted then", res.Message)
		}
	})

	t.Run("a namespace that is no longer a platform namespace does not block it either", func(t *testing.T) {
		// The label can be removed after the resource was admitted, and then the
		// namespace gate would trap the object the same way.
		v := newFakeValidator(nsObj("tenant", nil))
		res, err := v.SystemBucketValidate(ctx, nil, terminating(sbObj("tenant", "left-behind", "")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Valid {
			t.Errorf("a terminating SystemBucket was refused (%s); it can never be deleted then", res.Message)
		}
	})

	t.Run("the same object, not terminating, is still refused", func(t *testing.T) {
		v := newFakeValidator(nsObj("tenant", nil))
		res, err := v.SystemBucketValidate(ctx, nil, sbObj("tenant", "left-behind", ""))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Valid {
			t.Error("the exemption must apply to terminating objects only")
		}
	})
}
