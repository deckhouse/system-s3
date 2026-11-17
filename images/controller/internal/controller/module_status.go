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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

const systemS3ModuleName = "system-s3"

// Module's v1alpha1 condition schema is not metav1.Condition: it has
// lastProbeTime but no observedGeneration. Unstructured access avoids importing
// all of Deckhouse and lets us preserve its status fields and foreign conditions.
var moduleGVK = schema.GroupVersionKind{Group: "deckhouse.io", Version: "v1alpha1", Kind: "Module"}

type moduleStatusPublisher struct {
	client client.Client
	reader client.Reader
	now    func() time.Time
}

func (p *moduleStatusPublisher) Publish(ctx context.Context, state backend.StoreState) error {
	return backend.RetryOnConflict(retry.DefaultRetry, func() error {
		module := &unstructured.Unstructured{}
		module.SetGroupVersionKind(moduleGVK)
		if err := p.reader.Get(ctx, client.ObjectKey{Name: systemS3ModuleName}, module); err != nil {
			return fmt.Errorf("read Module/system-s3: %w", err)
		}
		before := module.DeepCopy()
		current, _, err := unstructured.NestedSlice(module.Object, "status", "conditions")
		if err != nil {
			return err
		}
		now := time.Now()
		if p.now != nil {
			now = p.now()
		}
		stamp := now.UTC().Format(time.RFC3339)
		changed := false
		for _, observed := range moduleConditions(state) {
			kind := observed.Type
			idx := -1
			var old map[string]interface{}
			for i, item := range current {
				if c, ok := item.(map[string]interface{}); ok && c["type"] == kind {
					idx, old = i, c
					break
				}
			}
			if old != nil && old["status"] == string(observed.Status) && old["reason"] == observed.Reason && old["message"] == observed.Message {
				continue
			}
			transition := stamp
			if old != nil && old["status"] == string(observed.Status) && old["lastTransitionTime"] != nil {
				transition, _ = old["lastTransitionTime"].(string)
			}
			updated := map[string]interface{}{
				"type": kind, "status": string(observed.Status), "reason": observed.Reason,
				"message": observed.Message, "lastTransitionTime": transition, "lastProbeTime": stamp,
			}
			if idx < 0 {
				current = append(current, updated)
			} else {
				current[idx] = updated
			}
			changed = true
		}
		if !changed {
			return nil
		}
		if err := unstructured.SetNestedSlice(module.Object, current, "status", "conditions"); err != nil {
			return err
		}
		// The conditions list is replaced by a JSON merge patch. Optimistic locking
		// is essential: without it a concurrent Deckhouse update would be lost.
		patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
		return p.client.Status().Patch(ctx, module, patch)
	})
}

// moduleConditions is what Module/system-s3 says for each of its conditions
// after a pass: what the pass observed, or Unknown/NotEvaluated for a condition
// it did not reach. Publish writes it and the condition metric exports it, from
// this one place, so that the two cannot disagree about a condition the pass
// left out.
func moduleConditions(state backend.StoreState) []metav1.Condition {
	out := make([]metav1.Condition, 0, len(backend.StoreConditionTypes))
	for _, kind := range backend.StoreConditionTypes {
		observed := metav1.Condition{Type: kind, Status: metav1.ConditionUnknown, Reason: "NotEvaluated", Message: "not evaluated in this reconciliation"}
		for _, c := range state.Conditions {
			if c.Type == kind {
				observed = c
				break
			}
		}
		out = append(out, observed)
	}
	return out
}

// clearStoreConditions removes legacy data-plane conditions on an upgraded
// SystemBucket without dropping any conditions owned by another controller.
func clearStoreConditions(conditions []metav1.Condition) []metav1.Condition {
	filtered := make([]metav1.Condition, 0, len(conditions))
	for _, c := range conditions {
		// SizeApplied was renamed to SizeUpToDate. Remove every legacy status,
		// even when this pass cannot evaluate the quota and publish its new name.
		if c.Type == "SizeApplied" {
			continue
		}
		owned := false
		for _, kind := range backend.StoreConditionTypes {
			if c.Type == kind {
				owned = true
				break
			}
		}
		if !owned {
			filtered = append(filtered, c)
		}
	}
	return filtered
}
