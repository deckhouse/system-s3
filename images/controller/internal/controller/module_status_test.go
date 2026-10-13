// Copyright 2026 Flant JSC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-common-lib/logger"
	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func moduleFixture() *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "deckhouse.io/v1alpha1", "kind": "Module",
		"metadata": map[string]interface{}{"name": systemS3ModuleName},
		"status": map[string]interface{}{"phase": "Ready", "hooksState": "ready", "conditions": []interface{}{
			map[string]interface{}{"type": "IsReady", "status": "True", "reason": "Ready", "message": "managed by Deckhouse"},
		}},
	}}
	return u
}

func readModule(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(moduleGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Name: systemS3ModuleName}, u); err != nil {
		t.Fatal(err)
	}
	return u
}

func moduleCondition(t *testing.T, u *unstructured.Unstructured, kind string) map[string]interface{} {
	t.Helper()
	cs, _, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range cs {
		c := item.(map[string]interface{})
		if c["type"] == kind {
			return c
		}
	}
	t.Fatalf("missing %s in %v", kind, cs)
	return nil
}

func TestModuleConditionsPreserveDeckhouseStatusAndTransitionTimes(t *testing.T) {
	u := moduleFixture()
	c := fake.NewClientBuilder().WithScheme(reconcilerScheme(t)).WithObjects(u).WithStatusSubresource(u).Build()
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	p := &moduleStatusPublisher{client: c, reader: c, now: func() time.Time { return now }}
	state := backend.StoreState{}
	state.SetCondition(backend.StorageReady, metav1.ConditionTrue, "Serving", "healthy")
	state.SetCondition(backend.CapacityReady, metav1.ConditionFalse, "CannotGrow", "CSI resize failed")
	if err := p.Publish(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	live := readModule(t, c)
	phase, _, _ := unstructured.NestedString(live.Object, "status", "phase")
	if phase != "Ready" || moduleCondition(t, live, "IsReady")["message"] != "managed by Deckhouse" {
		t.Fatal("Deckhouse status overwritten")
	}
	for _, kind := range backend.StoreConditionTypes {
		cond := moduleCondition(t, live, kind)
		if _, present := cond["observedGeneration"]; present {
			t.Fatal("v1alpha1 Module does not support observedGeneration")
		}
	}
	initial := moduleCondition(t, live, backend.CapacityReady)["lastTransitionTime"]
	rv := live.GetResourceVersion()
	now = now.Add(time.Minute)
	if err := p.Publish(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if readModule(t, c).GetResourceVersion() != rv {
		t.Fatal("unchanged conditions caused a write")
	}
	state.SetCondition(backend.CapacityReady, metav1.ConditionFalse, "Growing", "CSI is resizing")
	if err := p.Publish(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if moduleCondition(t, readModule(t, c), backend.CapacityReady)["lastTransitionTime"] != initial {
		t.Fatal("reason-only change reset transition time")
	}
	now = now.Add(time.Minute)
	state.SetCondition(backend.CapacityReady, metav1.ConditionTrue, "Available", "volumes fit")
	if err := p.Publish(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if moduleCondition(t, readModule(t, c), backend.CapacityReady)["lastTransitionTime"] == initial {
		t.Fatal("status transition did not reset transition time")
	}
}

type conflictModuleClient struct {
	client.Client
	patches int
	t       *testing.T
}
type conflictModuleWriter struct {
	client.SubResourceWriter
	owner *conflictModuleClient
}

func (c *conflictModuleClient) Status() client.SubResourceWriter {
	return &conflictModuleWriter{SubResourceWriter: c.Client.Status(), owner: c}
}
func (w *conflictModuleWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	w.owner.patches++
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	if !strings.Contains(string(data), "resourceVersion") {
		w.owner.t.Fatal("patch has no optimistic lock")
	}
	if w.owner.patches == 1 {
		live := readModule(w.owner.t, w.owner.Client)
		_ = unstructured.SetNestedField(live.Object, "Reconciling", "status", "phase")
		cs, _, _ := unstructured.NestedSlice(live.Object, "status", "conditions")
		cs = append(cs, map[string]interface{}{"type": "EnabledByModuleConfig", "status": "True"})
		_ = unstructured.SetNestedSlice(live.Object, cs, "status", "conditions")
		if err := w.owner.Client.Status().Update(ctx, live); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Group: "deckhouse.io", Resource: "modules"}, obj.GetName(), errors.New("concurrent Deckhouse update"))
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func TestModuleConditionPatchRetriesWithoutLosingConcurrentDeckhouseChanges(t *testing.T) {
	u := moduleFixture()
	base := fake.NewClientBuilder().WithScheme(reconcilerScheme(t)).WithObjects(u).WithStatusSubresource(u).Build()
	c := &conflictModuleClient{Client: base, t: t}
	p := &moduleStatusPublisher{client: c, reader: base}
	if err := p.Publish(context.Background(), backend.StoreState{}); err != nil {
		t.Fatal(err)
	}
	live := readModule(t, base)
	phase, _, _ := unstructured.NestedString(live.Object, "status", "phase")
	if phase != "Reconciling" || moduleCondition(t, live, "EnabledByModuleConfig")["status"] != "True" || c.patches != 2 {
		t.Fatal("concurrent changes were lost or no retry happened")
	}
}

func TestModulePublisherDoesNotCreateMissingModule(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(reconcilerScheme(t)).Build()
	p := &moduleStatusPublisher{client: c, reader: c}
	if err := p.Publish(context.Background(), backend.StoreState{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGarageConditionsDoNotLeakIntoSystemBucketAndLegacyConditionsAreRemoved(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	bucket := bucketFor(ns, name)
	bucket.Status = &v1alpha1.SystemBucketStatus{Conditions: []metav1.Condition{
		{Type: backend.StorageReady, Status: metav1.ConditionFalse, Reason: "Converging", Message: "old Garage layout details"},
		{Type: backend.MigrationReady, Status: metav1.ConditionFalse, Reason: "Copying", Message: "old migration details"},
		{Type: "SizeApplied", Status: metav1.ConditionFalse, Reason: "CannotGrow", Message: "old CSI error"},
	}}
	drv := &fakeDriver{}
	r, c := newReconciler(t, drv, platformNS(ns), bucket)
	store := r.Store.(*fakeConverger)
	store.state = backend.StoreState{Message: "Garage quorum lost; PVC CSI exploded; ResizePurge"}
	store.err = errors.New("private cluster error details")
	key := client.ObjectKey{Namespace: ns, Name: name}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err == nil {
		t.Fatal("driver error was not returned for retry")
	}
	live := &v1alpha1.SystemBucket{}
	if err := c.Get(context.Background(), key, live); err != nil {
		t.Fatal(err)
	}
	for _, cond := range live.Status.Conditions {
		if strings.Contains(cond.Message, "quorum") || strings.Contains(cond.Message, "CSI") || strings.Contains(cond.Message, "private") || strings.Contains(cond.Message, "old ") {
			t.Fatalf("cluster details leaked: %+v", cond)
		}
		for _, kind := range backend.StoreConditionTypes {
			if cond.Type == kind {
				t.Fatalf("store condition remains on bucket: %+v", cond)
			}
		}
	}
	if findCondition(live.Status.Conditions, v1alpha1.SystemBucketConditionBucketReady).Reason != "WaitingForDependency" {
		t.Fatal("bucket does not report a dependency wait")
	}
	if len(drv.calls) != 0 {
		t.Fatal("backend was touched while store is unavailable")
	}
}

type reportingStoreDriver struct {
	state backend.StoreState
	err   error
}

func TestSizeUpToDateReplacesAllLegacySizeAppliedStatuses(t *testing.T) {
	for _, legacy := range []struct {
		status metav1.ConditionStatus
		reason string
	}{
		{metav1.ConditionTrue, "Applied"},
		{metav1.ConditionFalse, "QuotaPending"},
		{metav1.ConditionFalse, "Growing"},
		{metav1.ConditionFalse, "CannotGrow"},
		{metav1.ConditionUnknown, "NotEvaluated"},
	} {
		for _, ready := range []bool{true, false} {
			name := legacy.reason + "/store-unavailable"
			if ready {
				name = legacy.reason + "/store-ready"
			}
			t.Run(name, func(t *testing.T) {
				const ns, name = "d8-monitoring", "loki"
				bucket := bucketFor(ns, name)
				bucket.Generation = 7
				bucket.Spec.Size = "1Gi"
				bucket.Status = &v1alpha1.SystemBucketStatus{Conditions: []metav1.Condition{
					{Type: "SizeApplied", Status: legacy.status, Reason: legacy.reason, Message: "legacy quota condition"},
				}}
				r, c := newReconciler(t, &fakeDriver{usage: backend.BucketUsage{QuotaBytes: 1 << 30}}, platformNS(ns), bucket)
				r.Store.(*fakeConverger).state.Ready = ready
				key := client.ObjectKey{Namespace: ns, Name: name}
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				live := &v1alpha1.SystemBucket{}
				if err := c.Get(context.Background(), key, live); err != nil {
					t.Fatal(err)
				}
				if old := findCondition(live.Status.Conditions, "SizeApplied"); old != nil {
					t.Fatalf("legacy condition was not removed: %+v", old)
				}
				current := findCondition(live.Status.Conditions, v1alpha1.SystemBucketConditionSizeUpToDate)
				if ready {
					if current == nil || current.Status != metav1.ConditionTrue || current.Reason != v1alpha1.SizeReasonApplied || current.ObservedGeneration != 7 {
						t.Fatalf("new quota condition was not published: %+v", current)
					}
				} else if current != nil {
					t.Fatalf("quota was not observed but the new condition was published: %+v", current)
				}
			})
		}
	}
}

func (d *reportingStoreDriver) EnsureStore(context.Context, *backend.Store) (backend.StoreState, error) {
	return d.state, d.err
}
func (*reportingStoreDriver) TemporaryKeys(context.Context, *backend.Store) ([]backend.TemporaryKey, error) {
	return nil, nil
}

func (*reportingStoreDriver) SweepExpiredTemporaryAccess(context.Context, *backend.Store) (int, error) {
	return 0, nil
}
func (*reportingStoreDriver) SweepIncompleteUploads(context.Context, *backend.Store) (int, error) {
	return 0, nil
}

func TestStoreManagerReportsFailedReconciliationWithoutAnyBuckets(t *testing.T) {
	u := moduleFixture()
	c := fake.NewClientBuilder().WithScheme(reconcilerScheme(t)).WithObjects(u).WithStatusSubresource(u).Build()
	d := &reportingStoreDriver{err: errors.New("replica-count change failed")}
	d.state.SetCondition(backend.ReplicationReady, metav1.ConditionFalse, "ReconcileFailed", d.err.Error())
	m := &StoreManager{driver: d, store: &backend.Store{Name: "system"}, log: logger.NewNop(),
		state: backend.StoreState{Ready: true}, moduleStatus: &moduleStatusPublisher{client: c, reader: c}}
	state, err := m.Ensure(context.Background())
	if err == nil || !state.Ready {
		t.Fatal("last good cached state or retry error was lost")
	}
	cond := moduleCondition(t, readModule(t, c), backend.ReplicationReady)
	if cond["status"] != "False" || cond["message"] != d.err.Error() {
		t.Fatalf("failed state was not published: %v", cond)
	}
}

func TestReportingFailureDoesNotBlockServingStorage(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(reconcilerScheme(t)).Build()
	m := &StoreManager{driver: &reportingStoreDriver{state: backend.StoreState{Ready: true}},
		store: &backend.Store{Name: "system"}, log: logger.NewNop(),
		moduleStatus: &moduleStatusPublisher{client: c, reader: c}}
	state, err := m.Ensure(context.Background())
	if err != nil || !state.Ready {
		t.Fatalf("publication failure blocked S3: %+v %v", state, err)
	}
}
