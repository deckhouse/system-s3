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

package controller

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

const gi = int64(1) << 30

func TestSizeCondition(t *testing.T) {
	cases := []struct {
		name       string
		requested  int64
		quota      int64
		wantStatus metav1.ConditionStatus
		wantReason string
		wantInMsg  string
	}{
		{
			name:      "bucket quota applied",
			requested: 5 * gi, quota: 5 * gi,
			wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.SizeReasonApplied, wantInMsg: "5Gi",
		},
		{
			name:      "the backend still enforces the old quota",
			requested: 6 * gi, quota: 5 * gi,
			wantStatus: metav1.ConditionFalse, wantReason: v1alpha1.SizeReasonQuotaPending, wantInMsg: "5Gi",
		},
		{
			name:      "larger bucket quota applied",
			requested: 20 * gi, quota: 20 * gi,
			wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.SizeReasonApplied, wantInMsg: "20Gi",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sizeCondition(tc.requested, backend.BucketUsage{QuotaBytes: tc.quota})
			if got == nil {
				t.Fatal("got no condition")
			}
			if got.Type != v1alpha1.SystemBucketConditionSizeUpToDate || got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Errorf("got %s=%s (%s), want %s (%s)", got.Type, got.Status, got.Reason, tc.wantStatus, tc.wantReason)
			}
			if !strings.Contains(got.Message, tc.wantInMsg) {
				t.Errorf("message %q does not mention %q", got.Message, tc.wantInMsg)
			}
		})
	}
}

// TestSizeUpToDateReachesTheResource: the condition lands on the SystemBucket
// next to the stage conditions, independently of shared PVC growth or failure.
func TestSizeUpToDateReachesTheResource(t *testing.T) {
	const ns, name = "d8-monitoring", "loki"
	bucket := bucketFor(ns, name)
	bucket.Spec.Size = "20Gi"
	drv := &fakeDriver{usage: backend.BucketUsage{QuotaBytes: 20 * gi}}
	r, c := newReconciler(t, drv, platformNS(ns), bucket)
	store := r.Store.(*fakeConverger)
	store.state.Capacity = backend.StoreCapacity{WantBytes: 25 * gi, HaveBytes: 15 * gi}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}

	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &v1alpha1.SystemBucket{}
	if err := c.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != v1alpha1.PhaseReady {
		t.Errorf("phase=%s while the volumes grow, want Ready: the bucket serves as before", got.Status.Phase)
	}
	cond := findCondition(got.Status.Conditions, v1alpha1.SystemBucketConditionSizeUpToDate)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != v1alpha1.SizeReasonApplied {
		t.Fatalf("SizeUpToDate=%+v, want True/Applied independently of shared PVC growth", cond)
	}
	if cond.ObservedGeneration != got.Generation {
		t.Errorf("observedGeneration=%d, want %d", cond.ObservedGeneration, got.Generation)
	}
	if res.RequeueAfter != r.nextWakeup(bucket) {
		t.Errorf("requeue after %s, want the ready bucket cadence %s", res.RequeueAfter, r.nextWakeup(bucket))
	}

	store.state.Capacity.HaveBytes = 25 * gi
	store.state.Capacity.Problem = "CSI expansion failed: this belongs on Module, not the bucket"
	res, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond = findCondition(got.Status.Conditions, v1alpha1.SystemBucketConditionSizeUpToDate)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != v1alpha1.SizeReasonApplied {
		t.Fatalf("SizeUpToDate=%+v, want True/Applied once the volumes fit", cond)
	}
	if res.RequeueAfter != r.Cfg.ReadyResyncInterval {
		t.Errorf("requeue after %s, want the resync interval %s once applied", res.RequeueAfter, r.Cfg.ReadyResyncInterval)
	}
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
