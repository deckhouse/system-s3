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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// vpaScheme is the core scheme plus the VerticalPodAutoscaler as an unstructured
// kind, which is how a cluster with the autoscaler module looks to the client.
func vpaScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := coreScheme(t)
	s.AddKnownTypeWithName(vpaGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(vpaGVK.GroupVersion().WithKind(vpaGVK.Kind+"List"), &unstructured.UnstructuredList{})
	return s
}

func getVPA(ctx context.Context, c client.Client, name string) (*unstructured.Unstructured, error) {
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(vpaGVK)
	err := c.Get(ctx, client.ObjectKey{Namespace: "d8-system-s3", Name: name}, vpa)
	return vpa, err
}

// TestVPAFollowsItsPlane: the autoscaler of a plane targets that plane's
// StatefulSet, names its containers as they are in the pod, and never lets a
// recommendation go below what the pod asks for without one.
func TestVPAFollowsItsPlane(t *testing.T) {
	for _, incarnation := range []int32{1, 2} {
		store := systemStore()
		store.Incarnation = incarnation
		sts := buildStatefulSet(store, "d8-system-s3", "garage:img", "hash", testVolumeSize)
		withTLSTerminator(sts, "controller:img", "garage-tls", "system-garage-bucket-owners")
		vpa := buildVPA(store, "d8-system-s3")

		if vpa.GetName() != sts.Name {
			t.Errorf("incarnation %d: VPA %q, want it named after the StatefulSet %q", incarnation, vpa.GetName(), sts.Name)
		}
		if vpa.GetLabels()[incarnationLabel] != sts.Spec.Template.Labels[incarnationLabel] {
			t.Errorf("incarnation %d: VPA labels %v do not name the plane", incarnation, vpa.GetLabels())
		}
		target, _, _ := unstructured.NestedStringMap(vpa.Object, "spec", "targetRef")
		if target["kind"] != "StatefulSet" || target["apiVersion"] != "apps/v1" || target["name"] != sts.Name {
			t.Errorf("incarnation %d: targetRef=%v, want the StatefulSet %q", incarnation, target, sts.Name)
		}
		mode, _, _ := unstructured.NestedString(vpa.Object, "spec", "updatePolicy", "updateMode")
		if mode != "Initial" {
			t.Errorf("incarnation %d: updateMode=%q; any other mode may evict a storage replica", incarnation, mode)
		}

		policies, _, _ := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
		if len(policies) != len(sts.Spec.Template.Spec.Containers) {
			t.Fatalf("incarnation %d: %d container policies for %d containers", incarnation, len(policies), len(sts.Spec.Template.Spec.Containers))
		}
		for _, raw := range policies {
			p := raw.(map[string]any)
			name := p["containerName"].(string)
			var container *corev1.Container
			for i := range sts.Spec.Template.Spec.Containers {
				if sts.Spec.Template.Spec.Containers[i].Name == name {
					container = &sts.Spec.Template.Spec.Containers[i]
				}
			}
			if container == nil {
				t.Errorf("incarnation %d: policy for %q, which is not a container of the pod", incarnation, name)
				continue
			}
			if p["controlledValues"] != "RequestsOnly" {
				t.Errorf("%s: controlledValues=%v, want RequestsOnly", name, p["controlledValues"])
			}
			minAllowed := p["minAllowed"].(map[string]any)
			maxAllowed := p["maxAllowed"].(map[string]any)
			for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
				req := container.Resources.Requests[res]
				lo := resource.MustParse(minAllowed[string(res)].(string))
				hi := resource.MustParse(maxAllowed[string(res)].(string))
				if lo.Cmp(req) != 0 {
					t.Errorf("%s: minAllowed %s=%s, want the static request %s", name, res, lo.String(), req.String())
				}
				if hi.Cmp(lo) <= 0 {
					t.Errorf("%s: maxAllowed %s=%s is not above minAllowed %s", name, res, hi.String(), lo.String())
				}
			}
		}
	}
}

// TestGarageRequestsStayOnTheTemplate: the template is the same with the
// autoscaler and without it, so the requests are the pod's floor until a
// recommendation exists and switching the module does not roll the data plane.
func TestGarageRequestsStayOnTheTemplate(t *testing.T) {
	c := garageContainer("garage:img")
	if c.Resources.Requests.Cpu().Cmp(resource.MustParse("100m")) != 0 ||
		c.Resources.Requests.Memory().Cmp(resource.MustParse("256Mi")) != 0 {
		t.Errorf("garage requests=%v, want 100m/256Mi", c.Resources.Requests)
	}
}

func ensureWorkloadWithVPA(t *testing.T, enabled bool, store *backend.Store) client.Client {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(vpaScheme(t)).Build()
	d := (&Driver{client: c, apiReader: c, namespace: "d8-system-s3", image: "garage:img",
		copierImage: "controller:img", tlsSecret: "garage-tls", log: testGarageLogger(t)}).WithVPA(enabled)
	if _, _, err := d.ensureWorkload(context.Background(), store, "hash", testVolumeSize); err != nil {
		t.Fatalf("ensureWorkload: %v", err)
	}
	return c
}

// TestEnsureWorkloadCreatesTheVPAOnlyWhenEnabled covers both planes of a
// migration, which are built by the same call.
func TestEnsureWorkloadCreatesTheVPAOnlyWhenEnabled(t *testing.T) {
	ctx := context.Background()
	incoming := systemStore()
	incoming.Incarnation = 2

	for _, store := range []*backend.Store{systemStore(), incoming} {
		c := ensureWorkloadWithVPA(t, true, store)
		if _, err := getVPA(ctx, c, resourceName(store)); err != nil {
			t.Errorf("%s: no VPA with the autoscaler enabled: %v", resourceName(store), err)
		}
	}

	c := ensureWorkloadWithVPA(t, false, systemStore())
	if _, err := getVPA(ctx, c, resourceName(systemStore())); !apierrors.IsNotFound(err) {
		t.Errorf("a VPA was created with the autoscaler disabled (err=%v)", err)
	}
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "d8-system-s3", Name: resourceName(systemStore())}, sts); err != nil {
		t.Fatalf("the StatefulSet itself is missing: %v", err)
	}
	if sts.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().IsZero() {
		t.Error("without the autoscaler the pod has no memory request")
	}
}

// TestApplyUpdatesTheVPASpec: an autoscaler created by an older release is
// brought to the current spec, not left as it was first written.
func TestApplyUpdatesTheVPASpec(t *testing.T) {
	ctx := context.Background()
	store := systemStore()
	stale := buildVPA(store, "d8-system-s3")
	if err := unstructured.SetNestedField(stale.Object, "Recreate", "spec", "updatePolicy", "updateMode"); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(vpaScheme(t)).WithObjects(stale).Build()
	d := (&Driver{client: c, apiReader: c, namespace: "d8-system-s3"}).WithVPA(true)

	if err := d.ensureVPA(ctx, store); err != nil {
		t.Fatalf("ensureVPA: %v", err)
	}
	got, err := getVPA(ctx, c, resourceName(store))
	if err != nil {
		t.Fatal(err)
	}
	if mode, _, _ := unstructured.NestedString(got.Object, "spec", "updatePolicy", "updateMode"); mode != vpaUpdateMode {
		t.Errorf("updateMode=%q after apply, want %q", mode, vpaUpdateMode)
	}
}

// TestTeardownPlaneRemovesOnlyItsVPA: the retired plane's autoscaler goes with
// it, and the serving plane's stays.
func TestTeardownPlaneRemovesOnlyItsVPA(t *testing.T) {
	ctx := context.Background()
	ns := "d8-system-s3"
	serving := systemStore()
	serving.Incarnation = 2
	retiring := planeOf(systemStore(), 1, true, localSC)

	c := fake.NewClientBuilder().WithScheme(vpaScheme(t)).
		WithObjects(buildVPA(serving, ns), buildVPA(retiring, ns)).Build()
	d := (&Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}).WithVPA(true)

	if err := d.teardownPlane(ctx, retiring); err != nil {
		t.Fatalf("teardown: %v", err)
	}
	if _, err := getVPA(ctx, c, resourceName(serving)); err != nil {
		t.Errorf("the decommission took the serving cluster's VPA: %v", err)
	}
	if _, err := getVPA(ctx, c, resourceName(retiring)); !apierrors.IsNotFound(err) {
		t.Errorf("the retired cluster's VPA survived its decommission (err=%v)", err)
	}
}

// TestTeardownPlaneWithoutTheVPAAPI: with the autoscaler off the kind may not be
// served at all, and the teardown must not try to delete one.
func TestTeardownPlaneWithoutTheVPAAPI(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}
	if err := d.teardownPlane(context.Background(), systemStore()); err != nil {
		t.Fatalf("teardown without the VPA API: %v", err)
	}
}
