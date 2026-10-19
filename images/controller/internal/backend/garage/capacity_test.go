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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// The sizing rule has three jobs, and the table names each: never go below what
// Garage needs to start, always leave room above the quotas, and never change the
// answer for a bucket that fits in the rounding.
func TestVolumeSizeFor(t *testing.T) {
	gi := func(n int64) int64 { return n << 30 }

	cases := []struct {
		name string
		sum  int64
		want int64
	}{
		{"no buckets at all: the floor", 0, gi(10)},
		{"one default bucket: still the floor", gi(1), gi(10)},
		{"a sum that fits under the floor with its headroom", gi(5), gi(10)},
		// 20Gi + 10% + 4Gi = 26Gi, rounded up to the 5Gi step.
		{"past the floor: headroom, reserve, and the step", gi(20), gi(30)},
		// 100Gi + 10% + 4Gi = 114Gi -> 115Gi.
		{"a large sum", gi(100), gi(115)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := volumeSizeFor(tc.sum)
			if got.Value() != tc.want {
				t.Errorf("volumeSizeFor(%d)=%s (%d), want %d", tc.sum, got.String(), got.Value(), tc.want)
			}
		})
	}

	// The property the step exists for: adding a bucket that fits in the slack
	// must not resize anything.
	if a, b := volumeSizeFor(gi(21)), volumeSizeFor(gi(22)); a.Cmp(b) != 0 {
		t.Errorf("a 1Gi bucket changed the volume size from %s to %s; the step is supposed to absorb it", a.String(), b.String())
	}
}

func capacityScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := rfScheme(t)
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add module scheme: %v", err)
	}
	return s
}

// What counts towards the store's size, and what does not.
func TestDesiredBucketBytes(t *testing.T) {
	bucket := func(ns, name, size string, deleting bool) *v1alpha1.SystemBucket {
		b := &v1alpha1.SystemBucket{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       v1alpha1.SystemBucketSpec{Size: size},
		}
		if deleting {
			now := metav1.Now()
			b.DeletionTimestamp = &now
			b.Finalizers = []string{"storage.deckhouse.io/system-s3-controller"}
		}
		return b
	}

	c := fake.NewClientBuilder().WithScheme(capacityScheme(t)).WithObjects(
		bucket("d8-monitoring", "loki", "20Gi", false),
		bucket("d8-monitoring", "prometheus", "5Gi", false),
		// On its way out: its space is about to come back, and sizing the store for
		// a consumer that no longer exists keeps the volume big forever.
		bucket("d8-old", "leaving", "100Gi", true),
		// Written by a client that stripped the defaulted field.
		bucket("d8-weird", "no-size", "", false),
		// Past the schema's pattern somehow: counted as nothing, and the resource's
		// own reconcile is what reports it.
		bucket("d8-weird", "bad-size", "twenty gigabytes", false),
	).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}

	got, err := d.desiredBucketBytes(context.Background())
	if err != nil {
		t.Fatalf("desiredBucketBytes: %v", err)
	}
	if want := int64(25) << 30; got != want {
		t.Errorf("sum=%d, want %d (20Gi + 5Gi)", got, want)
	}
}

// Claims grow and never shrink, and a claim that is already big enough is not
// written to at all — a store whose sum went down must not churn its volumes.
func TestEnsureVolumeCapacityGrowsOnly(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system", HighAvailability: false}
	ctx := context.Background()

	claim := func(name, size string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: corev1.PersistentVolumeClaimSpec{
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
				},
			},
		}
	}

	t.Run("grows a claim that is too small", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(capacityScheme(t)).
			WithObjects(claim("data-system-garage-0", "10Gi")).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

		grown, capacity := d.ensureVolumeCapacity(ctx, store, resource.MustParse("30Gi"))
		msg := capacity.Problem
		if msg != "" {
			t.Fatalf("ensureVolumeCapacity: msg=%q", msg)
		}
		if !grown {
			t.Error("grown=false, want true")
		}
		live := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "data-system-garage-0"}, live); err != nil {
			t.Fatalf("read claim: %v", err)
		}
		if got := live.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("30Gi")) != 0 {
			t.Errorf("claim request=%s, want 30Gi", got.String())
		}
	})

	t.Run("reports how big the volumes are and why they could not grow", func(t *testing.T) {
		pvc := claim("data-system-garage-0", "10Gi")
		pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}
		c := fake.NewClientBuilder().WithScheme(capacityScheme(t)).WithObjects(pvc).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
					return errors.New("only dynamically provisioned pvc can be resized and the storageclass that provisions the pvc must support resize")
				},
			}).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

		grown, capacity := d.ensureVolumeCapacity(ctx, store, resource.MustParse("30Gi"))
		if grown {
			t.Error("grown=true although the claim could not be patched")
		}
		if capacity.WantBytes != 30<<30 || capacity.HaveBytes != 10<<30 {
			t.Errorf("capacity want=%d have=%d, want 30Gi and 10Gi", capacity.WantBytes, capacity.HaveBytes)
		}
		if !strings.Contains(capacity.Problem, "allowVolumeExpansion") {
			t.Errorf("problem %q does not say what to change", capacity.Problem)
		}
	})

	t.Run("leaves a claim that is already larger alone", func(t *testing.T) {
		writes := 0
		c := fake.NewClientBuilder().WithScheme(capacityScheme(t)).
			WithObjects(claim("data-system-garage-0", "50Gi")).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					writes++
					return cl.Update(ctx, obj, opts...)
				},
			}).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

		grown, capacity := d.ensureVolumeCapacity(ctx, store, resource.MustParse("30Gi"))
		msg := capacity.Problem
		if msg != "" {
			t.Fatalf("ensureVolumeCapacity: msg=%q", msg)
		}
		if grown {
			t.Error("grown=true for a claim that was already big enough")
		}
		if writes != 0 {
			t.Errorf("%d write(s) to a claim that needed none: a shrinking sum must not touch the volumes", writes)
		}
	})
}

// A volumeClaimTemplate cannot be updated, so the workload is deleted with its
// pods left running and created again. Both halves matter: if the delete were not
// orphaning, the data plane would go down to change a number.
func TestWorkloadIsRecreatedForANewVolumeSizeWithoutTakingThePodsDown(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system", HighAvailability: false, StorageClass: "ceph-csi-rbd"}
	ctx := context.Background()

	existing := buildStatefulSet(store, ns, "garage:test", "hash", resource.MustParse("10Gi"))
	var policies []metav1.DeletionPropagation
	c := fake.NewClientBuilder().WithScheme(capacityScheme(t)).WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					o := &client.DeleteOptions{}
					for _, opt := range opts {
						opt.ApplyToDelete(o)
					}
					if o.PropagationPolicy != nil {
						policies = append(policies, *o.PropagationPolicy)
					} else {
						policies = append(policies, "")
					}
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).Build()
	d := testDriver(t, c, ns)

	if _, _, err := d.ensureWorkload(ctx, store, "hash", resource.MustParse("30Gi")); err != nil {
		t.Fatalf("ensureWorkload: %v", err)
	}

	if len(policies) != 1 || policies[0] != metav1.DeletePropagationOrphan {
		t.Fatalf("delete propagation policies=%v, want exactly one Orphan: anything else takes the data plane down", policies)
	}

	live := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)}, live); err != nil {
		t.Fatalf("the workload was not created again: %v", err)
	}
	got := live.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	if got.Cmp(resource.MustParse("30Gi")) != 0 {
		t.Errorf("recreated template asks for %s, want 30Gi", got.String())
	}
}

// And the opposite: the same size must not delete anything. The recreate is
// cheap but not free — it is a window with no controller behind the pods — so it
// has to happen only when the template actually changes.
func TestWorkloadIsNotRecreatedWhenTheSizeIsUnchanged(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system", HighAvailability: false, StorageClass: "ceph-csi-rbd"}
	size := resource.MustParse("30Gi")

	deletes := 0
	c := fake.NewClientBuilder().WithScheme(capacityScheme(t)).
		WithObjects(buildStatefulSet(store, ns, "garage:test", "hash", size)).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deletes++
				return cl.Delete(ctx, obj, opts...)
			},
		}).Build()
	d := testDriver(t, c, ns)

	if _, _, err := d.ensureWorkload(context.Background(), store, "hash", size); err != nil {
		t.Fatalf("ensureWorkload: %v", err)
	}
	if deletes != 0 {
		t.Errorf("%d delete(s) for an unchanged volume size", deletes)
	}
}

// TestResizeStateReadsWhereAGrowthIsStuck: "growing" alone does not tell an
// operator whether to wait or to act. The claim says which, and a refusal is a
// problem to report, not a growth to wait for.
func TestResizeStateReadsWhereAGrowthIsStuck(t *testing.T) {
	claim := func(status corev1.ClaimResourceStatus, conds ...corev1.PersistentVolumeClaimCondition) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-system-garage-0"}}
		if status != "" {
			pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{corev1.ResourceStorage: status}
		}
		pvc.Status.Conditions = conds
		return pvc
	}
	cond := func(t corev1.PersistentVolumeClaimConditionType, msg string) corev1.PersistentVolumeClaimCondition {
		return corev1.PersistentVolumeClaimCondition{Type: t, Status: corev1.ConditionTrue, Message: msg}
	}

	cases := []struct {
		name        string
		pvc         *corev1.PersistentVolumeClaim
		wantDetail  string
		wantRefused string
	}{
		{name: "nothing reported", pvc: claim("")},
		{name: "resizer at work", pvc: claim("", cond(corev1.PersistentVolumeClaimResizing, "")), wantDetail: "resizer is growing"},
		{name: "file system pending", pvc: claim("", cond(corev1.PersistentVolumeClaimFileSystemResizePending, "")), wantDetail: "next mounted"},
		{name: "node pending", pvc: claim(corev1.PersistentVolumeClaimNodeResizePending), wantDetail: "file system"},
		{name: "infeasible", pvc: claim(corev1.PersistentVolumeClaimControllerResizeInfeasible), wantRefused: "infeasible"},
		{
			name:        "controller error",
			pvc:         claim("", cond(corev1.PersistentVolumeClaimControllerResizeError, "array refused: no space")),
			wantRefused: "no space",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			detail, refused := resizeState(tc.pvc)
			if (tc.wantDetail == "") != (detail == "") || !strings.Contains(detail, tc.wantDetail) {
				t.Errorf("detail=%q, want it to mention %q", detail, tc.wantDetail)
			}
			if (tc.wantRefused == "") != (refused == "") || !strings.Contains(refused, tc.wantRefused) {
				t.Errorf("refused=%q, want it to mention %q", refused, tc.wantRefused)
			}
		})
	}
}

func TestExhaustedVolumes(t *testing.T) {
	const gi = uint64(1) << 30
	role := json.RawMessage(`{"zone":"system-s3"}`)
	host := func(s string) *string { return &s }
	space := func(avail, total uint64) *partitionSpace { return &partitionSpace{Available: avail, Total: total} }
	node := func(name string, data, meta *partitionSpace) clusterNode {
		return clusterNode{ID: name + "0123456789abcdef", Hostname: host(name), IsUp: true, Role: role, DataPartition: data, MetadataPartition: meta}
	}

	for _, tc := range []struct {
		name  string
		nodes []clusterNode
		want  []string
	}{
		{"room everywhere", []clusterNode{node("i2-0", space(9*gi, 15*gi), space(9*gi, 15*gi))}, nil},
		{"one volume full", []clusterNode{
			node("i2-0", space(9*gi, 15*gi), space(9*gi, 15*gi)),
			node("i2-1", space(0, 15*gi), space(0, 15*gi)),
		}, []string{"i2-1 has 0 of 15Gi free"}},
		{"only the metadata partition is short", []clusterNode{node("i2-1", space(9*gi, 15*gi), space(gi/4, 15*gi))}, []string{"i2-1 has 256Mi of 15Gi free"}},
		{"just above the threshold", []clusterNode{node("i2-1", space(gi, 15*gi), space(gi, 15*gi))}, nil},
		{"a node without a role is not a replica", []clusterNode{func() clusterNode {
			n := node("gateway", space(0, 15*gi), space(0, 15*gi))
			n.Role = json.RawMessage("null")
			return n
		}()}, nil},
		{"a node that is down reports nothing current", []clusterNode{func() clusterNode {
			n := node("i2-2", space(0, 15*gi), space(0, 15*gi))
			n.IsUp = false
			return n
		}()}, nil},
		{"no figures yet", []clusterNode{node("i2-0", nil, nil)}, nil},
		{"named by id without a hostname", []clusterNode{func() clusterNode {
			n := node("i2-1", space(0, 15*gi), nil)
			n.Hostname = nil
			return n
		}()}, []string{"i2-10123456789ab has 0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := exhaustedVolumes(tc.nodes)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("want nothing, got %q", got)
				}
				return
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Fatalf("%q does not mention %q", got, w)
				}
			}
		})
	}
}

// TestAFullReplicaGrowsTheVolumeOneStepAtATime: a replica found nearly full asks
// for one step above the volume it has; the target is never lowered, does not
// leak into another plane, and gives way to a larger size the quotas ask for.
func TestAFullReplicaGrowsTheVolumeOneStepAtATime(t *testing.T) {
	gi := func(n int64) int64 { return n << 30 }
	q := func(n int64) resource.Quantity { return *resource.NewQuantity(n, resource.BinarySI) }
	d := &Driver{}
	plane2 := &backend.Store{Name: backend.StoreName, Incarnation: 2}
	plane3 := &backend.Store{Name: backend.StoreName, Incarnation: 3}

	if got := d.growForFullness(plane2, q(gi(15))); got.Value() != gi(15) {
		t.Fatalf("nothing reported full, yet the size is %s", got.String())
	}
	if target := d.recordFullness(plane2, gi(15)); target != gi(20) {
		t.Fatalf("a full 15Gi volume asks for %d, want 20Gi", target)
	}
	if got := d.growForFullness(plane2, q(gi(15))); got.Value() != gi(20) {
		t.Errorf("the plane is not grown to its target: %s", got.String())
	}
	if got := d.growForFullness(plane3, q(gi(15))); got.Value() != gi(15) {
		t.Errorf("another plane picked up the target: %s", got.String())
	}
	// Still reporting full while the resize is under way, with the old size: the
	// target does not run away.
	if target := d.recordFullness(plane2, gi(15)); target != gi(20) {
		t.Errorf("the target moved while the growth was under way: %d", target)
	}
	// Grown and still full: one more step.
	if target := d.recordFullness(plane2, gi(20)); target != gi(25) {
		t.Errorf("a full 20Gi volume asks for %d, want 25Gi", target)
	}
	if got := d.growForFullness(plane2, q(gi(40))); got.Value() != gi(40) {
		t.Errorf("the quotas asked for more than the target and got %s", got.String())
	}
}
