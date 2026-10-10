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

package backend

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	StorageReady     = "StorageReady"
	PlacementReady   = "PlacementReady"
	CapacityReady    = "CapacityReady"
	ReplicationReady = "ReplicationReady"
	MigrationReady   = "MigrationReady"
)

var StoreConditionTypes = []string{StorageReady, PlacementReady, CapacityReady, ReplicationReady, MigrationReady}

func (s *StoreState) SetCondition(kind string, status metav1.ConditionStatus, reason, message string) {
	c := metav1.Condition{Type: kind, Status: status, Reason: reason, Message: message}
	for i := range s.Conditions {
		if s.Conditions[i].Type == kind {
			s.Conditions[i] = c
			return
		}
	}
	s.Conditions = append(s.Conditions, c)
}
