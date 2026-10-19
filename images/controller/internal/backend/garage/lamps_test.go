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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func TestPlacedMessageNamesTheNodes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas []planeReplica
		want     string
	}{
		{"three", []planeReplica{{"g-0", "debian-0", true}, {"g-1", "ubuntu-1", true}, {"g-2", "ubuntu-0", true}}, "one replica per node: debian-0, ubuntu-1, ubuntu-0"},
		{"one", []planeReplica{{"g-0", "master-0", true}}, "the replica runs on node master-0"},
		{"none scheduled", []planeReplica{{"g-0", "", false}}, "replica placement matches the eligible nodes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := placedMessage(tc.replicas); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNotReadyNoteNamesTheReplicaAndItsNode(t *testing.T) {
	if got := notReadyNote([]planeReplica{{"g-0", "a", true}, {"g-1", "b", true}}); got != "" {
		t.Fatalf("all ready: %q", got)
	}
	got := notReadyNote([]planeReplica{{"g-0", "a", true}, {"g-1", "b", false}, {"g-2", "", false}})
	if got != "not ready: g-1 (on b), g-2 (not scheduled)" {
		t.Fatalf("got %q", got)
	}
}

// TestSettledShapeIsDescribedAndOperationsAreLeftAlone: Stable and Converged say
// what the store runs as; a condition that reports an operation keeps its own
// message, which says more.
func TestSettledShapeIsDescribedAndOperationsAreLeftAlone(t *testing.T) {
	state := backend.StoreState{}
	state.SetCondition(backend.ReplicationReady, metav1.ConditionTrue, "Stable", "the replica count matches the configured mode")
	state.SetCondition(backend.MigrationReady, metav1.ConditionTrue, "Converged", "no StorageClass migration is pending")
	describeSettledShape(&state, 3, false, "huawei-storage-stable")

	repl := storeCondition(t, state, backend.ReplicationReady)
	if repl.Message != "3 replicas, high availability on; the highly available mode follows the number of control-plane nodes" {
		t.Errorf("ReplicationReady: %q", repl.Message)
	}
	mig := storeCondition(t, state, backend.MigrationReady)
	if !strings.Contains(mig.Message, "StorageClass huawei-storage-stable") || !strings.Contains(mig.Message, "no migration is pending") {
		t.Errorf("MigrationReady: %q", mig.Message)
	}

	busy := backend.StoreState{}
	busy.SetCondition(backend.ReplicationReady, metav1.ConditionFalse, "ResizeFreeze", "writes are frozen while the replica count changes")
	busy.SetCondition(backend.MigrationReady, metav1.ConditionFalse, "Copying", "copying 3 bucket(s)")
	describeSettledShape(&busy, 1, true, "local")
	if c := storeCondition(t, busy, backend.ReplicationReady); c.Message != "writes are frozen while the replica count changes" {
		t.Errorf("an operation lost its message: %q", c.Message)
	}
	if c := storeCondition(t, busy, backend.MigrationReady); c.Message != "copying 3 bucket(s)" {
		t.Errorf("an operation lost its message: %q", c.Message)
	}
}

func TestReplicationMessageSaysWhereTheModeComesFrom(t *testing.T) {
	if got := replicationMessage(1, true); got != "1 replica, high availability off; the highly available mode is set in the module or global settings" {
		t.Fatalf("got %q", got)
	}
}
