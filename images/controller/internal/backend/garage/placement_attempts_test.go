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
	"fmt"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func attemptScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return s
}

// TestPlacementAttemptBookkeeping covers the counter that bounds the migration
// loop: it is read back, restarted when the target changes, and cleared on a
// converged pass.
func TestPlacementAttemptBookkeeping(t *testing.T) {
	s := attemptScheme(t)
	store := &backend.Store{Name: backend.StoreName}
	ns := "d8-system-s3"
	ctx := context.Background()

	cm := func(data map[string]string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configName(store), Namespace: ns},
			Data:       data,
		}
	}
	read := func(t *testing.T, c client.Client) map[string]string {
		t.Helper()
		got := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: configName(store)}, got); err != nil {
			t.Fatalf("get configmap: %v", err)
		}
		return got.Data
	}

	t.Run("no ConfigMap yet: nothing attempted, and recording is a no-op", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns}

		target, attempts, err := d.placementAttempt(ctx, store)
		if err != nil || target != "" || attempts != 0 {
			t.Fatalf("target=%q attempts=%d err=%v, want \"\"/0/nil", target, attempts, err)
		}
		// The config apply creates the ConfigMap later in the same pass; recording
		// into a ConfigMap that does not exist must not fail the reconcile.
		if err := d.recordPlacementAttempt(ctx, store, "0/spread", 1); err != nil {
			t.Fatalf("record into a missing ConfigMap: %v", err)
		}
	})

	t.Run("recorded attempt is read back", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm(map[string]string{
			configFileName: "replication_factor = 3\n",
		})).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns}

		if err := d.recordPlacementAttempt(ctx, store, "1/spread: replica co-located on master-0", 2); err != nil {
			t.Fatalf("record: %v", err)
		}
		target, attempts, err := d.placementAttempt(ctx, store)
		if err != nil || target != "1/spread: replica co-located on master-0" || attempts != 2 {
			t.Fatalf("target=%q attempts=%d err=%v, want the recorded pair", target, attempts, err)
		}
		// The desired config keys are untouched by the bookkeeping write.
		if got := read(t, c)[configFileName]; got != "replication_factor = 3\n" {
			t.Errorf("garage.toml = %q, want it unchanged by the bookkeeping write", got)
		}
	})

	t.Run("unparseable count reads as zero", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm(map[string]string{
			placementAttemptTargetKey: "0/spread",
			placementAttemptCountKey:  "not a number",
		})).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns}

		target, attempts, err := d.placementAttempt(ctx, store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if target != "0/spread" || attempts != 0 {
			t.Errorf("target=%q attempts=%d, want the target kept and the count restarted", target, attempts)
		}
	})

	t.Run("clear drops both keys and keeps the config", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm(map[string]string{
			configFileName:            "replication_factor = 3\n",
			incarnationKey:            "2",
			placementAttemptTargetKey: "0/spread",
			placementAttemptCountKey:  "3",
		})).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns}

		if err := d.clearPlacementAttempts(ctx, store); err != nil {
			t.Fatalf("clear: %v", err)
		}
		data := read(t, c)
		if _, ok := data[placementAttemptTargetKey]; ok {
			t.Errorf("target key survived the clear: %v", data)
		}
		if _, ok := data[placementAttemptCountKey]; ok {
			t.Errorf("count key survived the clear: %v", data)
		}
		if data[incarnationKey] != "2" || data[configFileName] != "replication_factor = 3\n" {
			t.Errorf("clear touched the desired state: %v", data)
		}
	})

	t.Run("clear on an already clean ConfigMap issues no write", func(t *testing.T) {
		// The converged path calls clear on every pass — every 30 seconds. Writing
		// each time would churn the ConfigMap's resourceVersion and, through the
		// config-hash annotation, risk rolling the data plane for nothing.
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm(map[string]string{
			configFileName: "replication_factor = 3\n",
		})).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns}

		before := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: configName(store)}, before); err != nil {
			t.Fatalf("get: %v", err)
		}
		if err := d.clearPlacementAttempts(ctx, store); err != nil {
			t.Fatalf("clear: %v", err)
		}
		after := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: configName(store)}, after); err != nil {
			t.Fatalf("get: %v", err)
		}
		if before.ResourceVersion != after.ResourceVersion {
			t.Errorf("resourceVersion changed %s -> %s: clear wrote when there was nothing to clear",
				before.ResourceVersion, after.ResourceVersion)
		}
	})

	t.Run("recording the same pair twice issues no write", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(cm(map[string]string{
			placementAttemptTargetKey: "0/spread",
			placementAttemptCountKey:  "1",
		})).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns}

		before := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: configName(store)}, before); err != nil {
			t.Fatalf("get: %v", err)
		}
		if err := d.recordPlacementAttempt(ctx, store, "0/spread", 1); err != nil {
			t.Fatalf("record: %v", err)
		}
		after := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: configName(store)}, after); err != nil {
			t.Fatalf("get: %v", err)
		}
		if before.ResourceVersion != after.ResourceVersion {
			t.Errorf("resourceVersion changed %s -> %s: recording an unchanged pair wrote anyway",
				before.ResourceVersion, after.ResourceVersion)
		}
	})
}

// TestMergeDesiredKeepsPlacementBookkeeping is the regression test for the trap
// this feature walks into: the garage.toml ConfigMap is re-applied on every pass,
// so a plain Data assignment resets the attempt counter and the cap never binds.
func TestMergeDesiredKeepsPlacementBookkeeping(t *testing.T) {
	live := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "system-garage", Namespace: "d8-system-s3"},
		Data: map[string]string{
			configFileName:            "replication_factor = 3\n",
			incarnationKey:            "1",
			placementAttemptTargetKey: "2/spread: replica co-located on master-1",
			placementAttemptCountKey:  strconv.Itoa(maxPlacementAttempts - 1),
			"stale-key":               "gone",
		},
	}
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "system-garage", Namespace: "d8-system-s3"},
		Data: map[string]string{
			configFileName: "replication_factor = 3\n",
			incarnationKey: "1",
		},
	}

	if err := mergeDesired(live, desired); err != nil {
		t.Fatalf("mergeDesired: %v", err)
	}

	if live.Data[placementAttemptTargetKey] != "2/spread: replica co-located on master-1" {
		t.Errorf("attempt target lost: %v", live.Data)
	}
	if live.Data[placementAttemptCountKey] != strconv.Itoa(maxPlacementAttempts-1) {
		t.Errorf("attempt count lost: %v", live.Data)
	}
	if _, ok := live.Data["stale-key"]; ok {
		t.Errorf("a key that is neither desired nor bookkeeping survived: %v", live.Data)
	}
}

// TestPlacementGivesUpAfterRepeatedIdenticalMoves drives the loop the cap exists
// for: the replica is recycled and comes back on the same wrong node, over and
// over. After maxPlacementAttempts the reconcile stops recycling and reports why,
// instead of discarding a copy of the data every pass forever.
func TestPlacementGivesUpAfterRepeatedIdenticalMoves(t *testing.T) {
	ns := "d8-system-s3"
	ctx := context.Background()
	store := systemStore()

	// Exercise the execution budget independently of health/topology discovery.
	// The production caller only selects this action with three eligible hosts.
	spots := map[int32]replicaSpot{0: {nodePinned: true}, 1: {nodePinned: true}}
	action := placementAction{act: true, ordinal: 0, reason: "spread: replica off an eligible node"}
	pv := pinnedPV("replica-pv", "gone")
	cfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configName(store), Namespace: ns},
		Data:       map[string]string{configFileName: "replication_factor = 3\n"},
	}
	c := fake.NewClientBuilder().WithScheme(attemptScheme(t)).WithObjects(pv, cfg).Build()
	d := &Driver{client: c, apiReader: c, namespace: ns}

	// The scheduler putting the replica back where it was: the PVC reappears bound
	// to a PV on the same dead node.
	putReplicaBack := func(t *testing.T, ordinal int32) {
		t.Helper()
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("data-%s-%d", resourceName(store), ordinal),
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
		}
		if err := c.Create(ctx, pvc); err != nil {
			t.Fatalf("recreate pvc: %v", err)
		}
	}

	for pass := 1; pass <= maxPlacementAttempts; pass++ {
		putReplicaBack(t, 0)
		acted, msg, err := d.executePlacementAction(ctx, store, spots, action)
		if err != nil {
			t.Fatalf("pass %d: unexpected error: %v", pass, err)
		}
		if !acted {
			t.Fatalf("pass %d: acted=false (%q), want the migration to be attempted", pass, msg)
		}
	}

	// The cap is reached: this pass must not recycle.
	putReplicaBack(t, 0)
	acted, msg, err := d.executePlacementAction(ctx, store, spots, action)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if acted {
		t.Fatalf("acted=true after %d identical attempts, want the reconcile to give up", maxPlacementAttempts)
	}
	if msg == "" {
		t.Fatal("gave up silently: the operator has to be told why the placement is wrong")
	}
	if !strings.Contains(msg, "gave up") || !strings.Contains(msg, "spread") {
		t.Errorf("message %q does not name the give-up and the move it gave up on", msg)
	}
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: ns, Name: fmt.Sprintf("data-%s-0", resourceName(store)),
	}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Errorf("the replica's PVC was recycled on the give-up pass: %v", err)
	}

	// A different move is a different target: the count restarts, so giving up on
	// one replica does not freeze the others.
	if err := c.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: fmt.Sprintf("data-%s-0", resourceName(store)),
	}}); err != nil {
		t.Fatalf("delete replica 0 pvc: %v", err)
	}
	putReplicaBack(t, 1)
	action.ordinal = 1
	acted, msg, err = d.executePlacementAction(ctx, store, spots, action)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acted {
		t.Errorf("acted=false (%q), want a move for another replica to be attempted after the count restarts", msg)
	}
}
