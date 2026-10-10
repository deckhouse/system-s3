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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCutoverFenceWaitsForPodsAndRetainsClaimsAcrossControllerRestart(t *testing.T) {
	ctx := context.Background()
	store := planeOf(systemStore(), 1, true, localSC)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(store), Namespace: "d8-system-s3"}, Spec: appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: resourceName(store) + "-0", Namespace: sts.Namespace, Labels: planeLabels(store)}}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + pod.Name, Namespace: sts.Namespace, UID: "original-data"}}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(sts, pod, claim).Build()
	d := testDriver(t, c, sts.Namespace)
	state := migrationState{serving: 1, other: 2, phase: phaseCutover, frozenAt: time.Now(), writeFence: writeFenceStopping}
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	next, _, err := d.advanceCutoverOffline(ctx, store, state)
	if err != nil || next.writeFence != writeFenceStopping {
		t.Fatalf("fence crossed live pod: %+v %v", next, err)
	}
	current := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(sts), current); err != nil || *current.Spec.Replicas != 0 {
		t.Fatalf("workload not stopped: %v", err)
	}
	if policy := current.Spec.PersistentVolumeClaimRetentionPolicy; policy == nil || policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || policy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("source PVC retention not enforced: %+v", policy)
	}
	observed, err := d.EnsureStore(ctx, store)
	if err != nil || observed.Ready {
		t.Fatalf("offline fence did not stop the normal reconcile: %+v %v", observed, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(sts), current); err != nil || *current.Spec.Replicas != 0 {
		t.Fatalf("normal reconcile reopened the source before termination: %v", err)
	}
	// A terminating Pod is still a writer. No force removal or timeout bypass.
	if err := c.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: ptr.To(int64(30))}); err != nil {
		t.Fatal(err)
	}
	// Fake deletion is immediate; restore a terminating Pod with a finalizer.
	pod.ResourceVersion = ""
	pod.Finalizers = []string{"test/termination"}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	restarted := testDriver(t, c, sts.Namespace)
	loaded, err := restarted.loadMigrationState(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err = restarted.advanceCutoverOffline(ctx, store, loaded)
	if err != nil || next.writeFence != writeFenceStopping {
		t.Fatalf("fence crossed terminating pod: %+v %v", next, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = nil
	if err := c.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	next, _, err = restarted.advanceCutoverOffline(ctx, store, loaded)
	if err != nil || next.writeFence != writeFenceRestarting || !next.writesFrozen() {
		t.Fatalf("restart released write fence: %+v %v", next, err)
	}
	loaded, err = restarted.loadMigrationState(ctx, store)
	if err != nil || loaded.writeFence != writeFenceRestarting {
		t.Fatalf("restart stage not persisted: %+v %v", loaded, err)
	}
	kept := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(claim), kept); err != nil || kept.UID != claim.UID {
		t.Fatalf("source PVC changed: %v", err)
	}
}

func TestExpiredCutoverFenceStopsOldPodsBeforeReturningToCopying(t *testing.T) {
	ctx := context.Background()
	store := planeOf(systemStore(), 1, true, localSC)
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := testDriver(t, c, "d8-system-s3")
	state := migrationState{serving: 1, other: 2, phase: phaseCutover, writeFence: writeFenceStopping, frozenAt: time.Now().Add(-cutoverDeadline - time.Minute)}
	next, _, err := d.advanceCutoverOffline(ctx, store, state)
	if err != nil || next.phase != phaseCopying || next.writeFence != "" || !next.frozenAt.IsZero() {
		t.Fatalf("timeout did not safely thaw: %+v %v", next, err)
	}
}
