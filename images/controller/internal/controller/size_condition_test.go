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
		capacity   backend.StoreCapacity
		wantNil    bool
		wantStatus metav1.ConditionStatus
		wantReason string
		wantInMsg  string
	}{
		{
			// The store's convergence stopped before sizing the volumes: no
			// answer, rather than a guess that replaces the last one.
			name:      "volumes not sized this pass",
			requested: 5 * gi, quota: 5 * gi,
			wantNil: true,
		},
		{
			name:      "quota and volumes both in place",
			requested: 5 * gi, quota: 5 * gi,
			capacity:   backend.StoreCapacity{WantBytes: 15 * gi, HaveBytes: 20 * gi},
			wantStatus: metav1.ConditionTrue, wantReason: v1alpha1.SizeReasonApplied, wantInMsg: "5Gi",
		},
		{
			name:      "the backend still enforces the old quota",
			requested: 6 * gi, quota: 5 * gi,
			capacity:   backend.StoreCapacity{WantBytes: 15 * gi, HaveBytes: 15 * gi},
			wantStatus: metav1.ConditionFalse, wantReason: v1alpha1.SizeReasonQuotaPending, wantInMsg: "5Gi",
		},
		{
			name:      "volumes growing",
			requested: 20 * gi, quota: 20 * gi,
			capacity:   backend.StoreCapacity{WantBytes: 25 * gi, HaveBytes: 15 * gi},
			wantStatus: metav1.ConditionFalse, wantReason: v1alpha1.SizeReasonGrowing, wantInMsg: "15Gi to 25Gi (no claim reports",
		},
		{
			name:      "volumes growing, the node has yet to grow the file system",
			requested: 20 * gi, quota: 20 * gi,
			capacity: backend.StoreCapacity{
				WantBytes: 25 * gi, HaveBytes: 15 * gi,
				Detail: "claim data-system-garage-0: the volume is grown and the node has yet to grow its file system",
			},
			wantStatus: metav1.ConditionFalse, wantReason: v1alpha1.SizeReasonGrowing, wantInMsg: "yet to grow its file system",
		},
		{
			// A problem wins over "growing": the volumes are smaller AND nothing is
			// going to make them bigger, and the message says what to change.
			name:      "volumes cannot grow",
			requested: 20 * gi, quota: 20 * gi,
			capacity: backend.StoreCapacity{
				WantBytes: 25 * gi, HaveBytes: 15 * gi,
				Problem: "a StorageClass with allowVolumeExpansion is required",
			},
			wantStatus: metav1.ConditionFalse, wantReason: v1alpha1.SizeReasonCannotGrow, wantInMsg: "allowVolumeExpansion",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sizeCondition(tc.requested, backend.BucketUsage{QuotaBytes: tc.quota}, tc.capacity)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("got %+v, want no condition", got)
				}
				return
			}
			if got == nil {
				t.Fatal("got no condition")
			}
			if got.Type != v1alpha1.SystemBucketConditionSizeApplied || got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Errorf("got %s=%s (%s), want %s (%s)", got.Type, got.Status, got.Reason, tc.wantStatus, tc.wantReason)
			}
			if !strings.Contains(got.Message, tc.wantInMsg) {
				t.Errorf("message %q does not mention %q", got.Message, tc.wantInMsg)
			}
		})
	}
}

// TestSizeAppliedReachesTheResource: the condition lands on the SystemBucket
// next to the stage conditions, does not take Ready down while the volumes grow,
// and brings the next pass forward so it turns True close to when it does.
func TestSizeAppliedReachesTheResource(t *testing.T) {
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
	cond := findCondition(got.Status.Conditions, v1alpha1.SystemBucketConditionSizeApplied)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != v1alpha1.SizeReasonGrowing {
		t.Fatalf("SizeApplied=%+v, want False/Growing", cond)
	}
	if cond.ObservedGeneration != got.Generation {
		t.Errorf("observedGeneration=%d, want %d", cond.ObservedGeneration, got.Generation)
	}
	if res.RequeueAfter != r.Cfg.RequeueInterval {
		t.Errorf("requeue after %s, want the plain interval %s while the size has not taken effect",
			res.RequeueAfter, r.Cfg.RequeueInterval)
	}

	store.state.Capacity.HaveBytes = 25 * gi
	res, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := c.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	cond = findCondition(got.Status.Conditions, v1alpha1.SystemBucketConditionSizeApplied)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != v1alpha1.SizeReasonApplied {
		t.Fatalf("SizeApplied=%+v, want True/Applied once the volumes fit", cond)
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
