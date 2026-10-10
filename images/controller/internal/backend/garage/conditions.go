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
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func storageCondition(state *backend.StoreState, ready bool, message string) {
	status, reason := metav1.ConditionFalse, "Converging"
	if ready {
		status, reason = metav1.ConditionTrue, "Serving"
	}
	state.SetCondition(backend.StorageReady, status, reason, message)
}

func capacityCondition(state *backend.StoreState) {
	c := state.Capacity
	if c.WantBytes == 0 {
		return
	}
	quantity := func(n int64) string { return resource.NewQuantity(n, resource.BinarySI).String() }
	status, reason := metav1.ConditionTrue, "Available"
	msg := fmt.Sprintf("each replica volume has %s; required %s", quantity(c.HaveBytes), quantity(c.WantBytes))
	switch {
	case c.Problem != "":
		status, reason = metav1.ConditionFalse, "CannotGrow"
		msg = appendNote(msg, c.Problem)
	case c.HaveBytes < c.WantBytes:
		status, reason = metav1.ConditionFalse, "Growing"
		msg = appendNote(msg, c.Detail)
	}
	state.SetCondition(backend.CapacityReady, status, reason, msg)
}

// ReplicationReady describes HA changes; MigrationReady describes StorageClass
// moves, including combined StorageClass/HA changes that use a second plane.
func migrationConditions(state *backend.StoreState, migration migrationState, plan migrationPlan, message string) string {
	completedKind := backend.MigrationReady
	for _, c := range state.Conditions {
		if c.Type == backend.ReplicationReady && c.Status == metav1.ConditionFalse {
			completedKind = backend.ReplicationReady
		}
	}
	for _, c := range state.Conditions {
		if c.Type == backend.MigrationReady && c.Status == metav1.ConditionFalse {
			completedKind = backend.MigrationReady
		}
	}
	state.SetCondition(backend.ReplicationReady, metav1.ConditionTrue, "Stable", "the replica count matches the configured mode")
	state.SetCondition(backend.MigrationReady, metav1.ConditionTrue, "Converged", "no StorageClass migration is pending")
	state.MigrationPhase, state.MigrationStartedAt = reportedMigration(migration)
	kind, reason := "", "Pending"
	switch {
	case migration.phase.resizing():
		kind, reason = backend.ReplicationReady, string(migration.phase)
	case migration.migrating():
		kind, reason = backend.MigrationReady, string(migration.phase)
	case plan.reason != "":
		kind = backend.MigrationReady
		if plan.inPlace || !plan.start {
			kind = backend.ReplicationReady
		}
	}
	if kind != "" {
		if message == "" {
			message = pendingMigrationNote(plan, migration)
		}
		state.SetCondition(kind, metav1.ConditionFalse, reason, message)
	} else if message != "" {
		// Keep the completion message on the operation that just finished.
		state.SetCondition(completedKind, metav1.ConditionTrue, "Converged", message)
	}
	return kind
}
