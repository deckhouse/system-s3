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
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func TestCutoverFenceWaitsForPodsAndRetainsClaimsAcrossControllerRestart(t *testing.T) {
	ctx := context.Background()
	store := planeOf(systemStore(), 1, true, localSC)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(store), Namespace: "d8-system-s3", Labels: commonLabels(store)}, Spec: appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))}}
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

func fencePod(store *backend.Store, ordinal int, uid string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", resourceName(store), ordinal), Namespace: "d8-system-s3", Labels: planeLabels(store), UID: types.UID(uid)},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func TestRollingFenceRestartsOneSourcePodAtATime(t *testing.T) {
	ctx := context.Background()
	store := planeOf(systemStore(), 1, true, localSC)
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).
		WithObjects(fencePod(store, 0, "old-0", true), fencePod(store, 1, "old-1", true), fencePod(store, 2, "old-2", true)).Build()
	d := testDriver(t, c, "d8-system-s3")
	old := []string{"old-0", "old-1", "old-2"}

	present := func() []string {
		pods, err := d.planePods(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		return podUIDs(pods)
	}

	for step, ordinal := range []int{0, 1, 2} {
		done, msg, err := d.rollFencePods(ctx, store, old)
		if err != nil || done {
			t.Fatalf("step %d: %v %v", step, done, err)
		}
		if got := present(); len(got) != 2 {
			t.Fatalf("step %d: want exactly one pod restarted at a time, have %v (%s)", step, got, msg)
		}
		// While the restarted pod is missing or not ready nothing else is touched.
		if _, _, err := d.rollFencePods(ctx, store, old); err != nil {
			t.Fatal(err)
		}
		if got := present(); len(got) != 2 {
			t.Fatalf("step %d: a second pod went down while one was missing: %v", step, got)
		}
		back := fencePod(store, ordinal, fmt.Sprintf("new-%d", ordinal), false)
		if err := c.Create(ctx, back); err != nil {
			t.Fatal(err)
		}
		if _, msg, _ := d.rollFencePods(ctx, store, old); len(present()) != 3 || msg == "" {
			t.Fatalf("step %d: a pod went down while the restarted one was not ready: %v", step, present())
		}
		back.Status.Conditions[0].Status = corev1.ConditionTrue
		if err := c.Status().Update(ctx, back); err != nil {
			t.Fatal(err)
		}
	}
	done, _, err := d.rollFencePods(ctx, store, old)
	if err != nil || !done {
		t.Fatalf("fence not done after every pod restarted: %v %v", done, err)
	}
	if got := present(); len(got) != 3 || got[0] != "new-0" || got[2] != "new-2" {
		t.Fatalf("unexpected pods after the fence: %v", got)
	}
}

func TestRollingFencePodsSurviveControllerRestart(t *testing.T) {
	ctx := context.Background()
	store := planeOf(systemStore(), 1, true, localSC)
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := testDriver(t, c, "d8-system-s3")
	state := migrationState{serving: 1, other: 2, phase: phaseCutover, frozenAt: time.Now(), writeFence: writeFenceRolling, fencePods: []string{"a", "b", "c"}}
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := testDriver(t, c, "d8-system-s3").loadMigrationState(ctx, store)
	if err != nil || loaded.writeFence != writeFenceRolling || strings.Join(loaded.fencePods, ",") != "a,b,c" || !loaded.writesFrozen() {
		t.Fatalf("rolling fence not persisted: %+v %v", loaded, err)
	}
}
