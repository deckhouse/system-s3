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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/conditions"
	v1alpha1 "github.com/deckhouse/sds-object-system/api/v1alpha1"
)

var testStages = []string{"A", "B"}

func cond(t string, s metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: t, Status: s, Reason: reason}
}

func TestDerivePhase(t *testing.T) {
	cases := []struct {
		name  string
		conds []metav1.Condition
		want  string
	}{
		{"empty", nil, v1alpha1.PhasePending},
		// Both stages are present in every case below, because that is what the
		// FSM leaves behind: gateAfter follows a stage that does not pass and
		// writes the ones under it. A set that is missing a stage is Pending,
		// which is the case right after these.
		{"error", []metav1.Condition{
			cond("A", metav1.ConditionFalse, conditions.ReasonReconcileFailed),
			cond("B", metav1.ConditionFalse, conditions.ReasonWaitingForDependency),
		}, v1alpha1.PhaseError},
		{"inprogress", []metav1.Condition{
			cond("A", metav1.ConditionFalse, conditions.ReasonPending),
			cond("B", metav1.ConditionFalse, conditions.ReasonWaitingForDependency),
		}, v1alpha1.PhaseInProgress},
		{"a stage with no verdict", []metav1.Condition{
			cond("A", metav1.ConditionTrue, conditions.ReasonReconciled),
		}, v1alpha1.PhasePending},
		{"ready", []metav1.Condition{cond("A", metav1.ConditionTrue, conditions.ReasonReconciled), cond("B", metav1.ConditionTrue, conditions.ReasonReconciled)}, v1alpha1.PhaseReady},
		{"error wins over inprogress", []metav1.Condition{cond("A", metav1.ConditionFalse, conditions.ReasonPending), cond("B", metav1.ConditionFalse, conditions.ReasonReconcileFailed)}, v1alpha1.PhaseError},
	}
	for _, c := range cases {
		if got := derivePhase(c.conds, testStages); got != c.want {
			t.Errorf("%s: derivePhase=%q, want %q", c.name, got, c.want)
		}
	}
}

// The FSM itself — advancing a stage, gating what comes after it, folding the
// aggregate — belongs to conditions.Stages and conditions.Builder in
// sds-common-lib, and is tested there. What this module still owns is which
// phase strings those verdicts map to (its own Phase* constants) and the
// reconcile bound.

// TestWithReconcileTimeout pins the bound that keeps one stuck backend call from
// taking a controller with it: a configured timeout must reach the context, and an
// unset one (tests, or a deliberately disabled bound) must leave it alone rather
// than expire immediately.
func TestWithReconcileTimeout(t *testing.T) {
	bounded, cancel := withReconcileTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deadline, ok := bounded.Deadline()
	if !ok {
		t.Fatalf("a configured timeout must put a deadline on the context")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 30*time.Second {
		t.Errorf("remaining=%v, want (0, 30s]", remaining)
	}

	unbounded, cancel := withReconcileTimeout(context.Background(), 0)
	defer cancel()
	if _, ok := unbounded.Deadline(); ok {
		t.Errorf("an unset timeout must not bound the context")
	}
	if err := unbounded.Err(); err != nil {
		t.Errorf("an unset timeout must leave the context usable, got %v", err)
	}
}
