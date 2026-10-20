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

package garage

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func storeCondition(t *testing.T, state backend.StoreState, kind string) metav1.Condition {
	t.Helper()
	for _, c := range state.Conditions {
		if c.Type == kind {
			return c
		}
	}
	t.Fatalf("missing %s in %+v", kind, state.Conditions)
	return metav1.Condition{}
}

func TestCapacityConditionsReportGrowthAndCSIFailureIndependentlyOfServing(t *testing.T) {
	for _, tc := range []struct {
		name            string
		capacity        backend.StoreCapacity
		status          metav1.ConditionStatus
		reason, message string
	}{
		{"not observed", backend.StoreCapacity{}, metav1.ConditionUnknown, "NotEvaluated", "not evaluated"},
		{"available", backend.StoreCapacity{WantBytes: 15 << 30, HaveBytes: 20 << 30}, metav1.ConditionTrue, "Available", "20Gi"},
		{"growing", backend.StoreCapacity{WantBytes: 20 << 30, HaveBytes: 15 << 30, Detail: "filesystem resize pending"}, metav1.ConditionFalse, "Growing", "filesystem resize pending"},
		{"CSI failure", backend.StoreCapacity{WantBytes: 20 << 30, HaveBytes: 15 << 30, Problem: "CSI rejected expansion"}, metav1.ConditionFalse, "CannotGrow", "CSI rejected expansion"},
		{"volume full", backend.StoreCapacity{WantBytes: 15 << 30, HaveBytes: 15 << 30, Exhausted: "the volume of system-garage-i2-1 has 0 of 14Gi free"}, metav1.ConditionFalse, "Exhausted", "system-garage-i2-1"},
		{"volume full while growing", backend.StoreCapacity{WantBytes: 20 << 30, HaveBytes: 15 << 30, Problem: "CSI rejected expansion", Exhausted: "the volume of system-garage-i2-1 has 0 of 14Gi free"}, metav1.ConditionFalse, "Exhausted", "CSI rejected expansion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := backend.StoreState{Ready: true, Capacity: tc.capacity}
			state.SetCondition(backend.CapacityReady, metav1.ConditionUnknown, "NotEvaluated", "not evaluated")
			storageCondition(&state, true, "healthy")
			capacityCondition(&state)
			c := storeCondition(t, state, backend.CapacityReady)
			if c.Status != tc.status || c.Reason != tc.reason || !strings.Contains(c.Message, tc.message) {
				t.Fatalf("unexpected condition: %+v", c)
			}
			if storeCondition(t, state, backend.StorageReady).Status != metav1.ConditionTrue {
				t.Fatal("capacity problem took serving down")
			}
		})
	}
}

func TestEveryMigrationAndHAStageHasItsOwnCondition(t *testing.T) {
	for _, phase := range []migrationPhase{phaseConverged, phaseProvisioning, phaseCopying, phaseVerifying, phaseCutover, phaseDraining, phaseDecommission, phaseResizeFreeze, phaseResizeStop, phaseResizePurge, phaseResizeSync} {
		t.Run(string(phase), func(t *testing.T) {
			state := backend.StoreState{}
			kind := migrationConditions(&state, migrationState{phase: phase, serving: 1, other: 2}, migrationPlan{}, "")
			if phase == phaseConverged {
				if kind != "" {
					t.Fatalf("converged operation is pending: %s", kind)
				}
			} else {
				want := backend.MigrationReady
				if phase.resizing() {
					want = backend.ReplicationReady
				}
				c := storeCondition(t, state, want)
				if kind != want || c.Status != metav1.ConditionFalse || c.Reason != string(phase) {
					t.Fatalf("incorrect group or phase: %+v", c)
				}
				other := backend.ReplicationReady
				if phase.resizing() {
					other = backend.MigrationReady
				}
				if storeCondition(t, state, other).Status != metav1.ConditionTrue {
					t.Fatal("unrelated condition is not stable")
				}
			}
		})
	}
}

func TestPendingChangesAndCompletionStayInTheirOwnGroup(t *testing.T) {
	for _, tc := range []struct {
		name string
		plan migrationPlan
		kind string
	}{
		{"HA change", migrationPlan{reason: "HA changed", start: true, inPlace: true}, backend.ReplicationReady},
		{"automatic shrink refused", migrationPlan{reason: "explicit false required"}, backend.ReplicationReady},
		{"StorageClass change", migrationPlan{reason: "StorageClass changed", start: true}, backend.MigrationReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := backend.StoreState{}
			kind := migrationConditions(&state, migrationState{phase: phaseConverged}, tc.plan, "")
			if kind != tc.kind || storeCondition(t, state, kind).Reason != "Pending" {
				t.Fatalf("incorrect pending group: %s", kind)
			}
			migrationConditions(&state, migrationState{phase: phaseConverged}, migrationPlan{}, "completed")
			if c := storeCondition(t, state, tc.kind); c.Status != metav1.ConditionTrue || c.Message != "completed" {
				t.Fatalf("completion did not clear pending state: %+v", c)
			}
		})
	}
}

func TestBootstrapFailureDoesNotInventHealthyPlacementOrCapacity(t *testing.T) {
	d := &Driver{}
	state, err := d.EnsureStore(context.Background(), systemStore())
	if err != nil {
		t.Fatal(err)
	}
	if c := storeCondition(t, state, backend.StorageReady); c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "GARAGE_IMAGE") {
		t.Fatalf("missing bootstrap failure: %+v", c)
	}
	for _, kind := range backend.StoreConditionTypes[1:] {
		if c := storeCondition(t, state, kind); c.Status != metav1.ConditionUnknown {
			t.Fatalf("unobserved concern reported healthy: %+v", c)
		}
	}
}

func TestInsufficientNodesAreReportedWithoutRecyclingPVCs(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(rfScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}
	acted, msg, err := d.reconcilePlacement(context.Background(), systemStore())
	if err != nil || acted || !strings.Contains(msg, "needs 3 eligible nodes") {
		t.Fatalf("acted=%v, message=%q, error=%v", acted, msg, err)
	}
}

type failingStateReader struct{ client.Reader }

func (f failingStateReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("cannot read migration state from Kubernetes")
}

func TestDriverFailureIsAttachedToTheConcernBeingReconciled(t *testing.T) {
	// A built data plane, so the pass gets as far as the migration state.
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-system-garage-0", Namespace: "d8-system-s3", Labels: commonLabels(systemStore())}}
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(claim).Build()
	d := testDriver(t, c, "d8-system-s3")
	d.apiReader = failingStateReader{Reader: c}
	state, err := d.EnsureStore(context.Background(), systemStore())
	if err == nil {
		t.Fatal("expected API failure")
	}
	if c := storeCondition(t, state, backend.MigrationReady); c.Status != metav1.ConditionFalse || c.Reason != "ReconcileFailed" || !strings.Contains(c.Message, "migration state") {
		t.Fatalf("failed concern was not reported: %+v", c)
	}
	if storeCondition(t, state, backend.PlacementReady).Status != metav1.ConditionUnknown {
		t.Fatal("unprobed placement was reported healthy")
	}
}
