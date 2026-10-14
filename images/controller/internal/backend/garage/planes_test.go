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
	"reflect"
	"strconv"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

const planesTestNamespace = "d8-system-s3"

// planePod is a pod of one plane as its StatefulSet creates it.
func planePod(plane *backend.Store, ordinal int, node string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(plane) + "-" + strconv.Itoa(ordinal),
			Namespace: planesTestNamespace,
			Labels:    planeLabels(plane),
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

// In the middle of a migration both clusters are reported, each with its own
// shape and its own pods, and only the one behind the endpoint as serving. The
// shape is what the cluster was built in — the bound class and the pinned mode
// of the serving plane, the recorded target of the incoming one — not what the
// settings say now.
func TestDataPlanesDuringAMigration(t *testing.T) {
	ctx := context.Background()
	ns := "d8-system-s3"
	// The settings already ask for the shape the migration moves to.
	store := &backend.Store{Name: "system", StorageClass: "replicated", HighAvailability: false}
	serving := planeOf(store, 1, true, "local")
	incoming := planeOf(store, 2, false, "replicated")

	servingSTS := buildStatefulSet(serving, ns, "garage:test", "h", resource.MustParse("10Gi"))
	incomingSTS := buildStatefulSet(incoming, ns, "garage:test", "h", resource.MustParse("10Gi"))
	boundClaim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(serving) + "-0", Namespace: ns},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: ptr.To("local")},
	}
	state := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: migrationStateName(store), Namespace: ns},
		Data: map[string]string{
			migrationKeyServing: "1", migrationKeyOther: "2", migrationKeyPhase: string(phaseCopying),
			migrationKeyOtherHA: "false", migrationKeyOtherClass: "replicated",
		},
	}
	// A copy job's pod carries the store's labels and no incarnation, so it reads
	// as the first plane's; it is not a replica of anything.
	copier := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName(serving) + "-copy-abcde", Namespace: ns, Labels: commonLabels(store),
	}}

	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(
		state, servingSTS, incomingSTS, boundClaim,
		buildConfigMap(serving, ns, 3),
		planePod(serving, 0, "node-a", true),
		planePod(serving, 1, "node-b", false),
		planePod(serving, 2, "", false),
		planePod(incoming, 0, "node-c", true),
		copier,
	).Build()
	d := testDriver(t, c, ns)

	got, err := d.DataPlanes(ctx, store)
	if err != nil {
		t.Fatalf("DataPlanes: %v", err)
	}
	want := []backend.DataPlane{
		{
			Incarnation: 1, Serving: true, StorageClass: "local", HighAvailability: true, Replicas: 3,
			Pods: []backend.DataPlanePod{
				{Name: "system-garage-0", Node: "node-a", Ready: true},
				{Name: "system-garage-1", Node: "node-b"},
				{Name: "system-garage-2"},
			},
		},
		{
			// No claim bound yet and no pinned factor: the class is the template's
			// and the mode the one the migration recorded.
			Incarnation: 2, StorageClass: "replicated", Replicas: 1,
			Pods: []backend.DataPlanePod{{Name: "system-garage-i2-0", Node: "node-c", Ready: true}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("planes:\n got %+v\nwant %+v", got, want)
	}
}

// A plane that was torn down is gone from the report even though the migration
// state still remembers its number: nothing of it exists to show.
func TestDataPlanesLeavesOutAPlaneWithNothingLeft(t *testing.T) {
	ctx := context.Background()
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system", StorageClass: "replicated"}
	serving := planeOf(store, 2, false, "replicated")
	state := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: migrationStateName(store), Namespace: ns},
		Data: map[string]string{
			migrationKeyServing: "2", migrationKeyOther: "1", migrationKeyPhase: string(phaseDecommission),
		},
	}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(
		state,
		buildStatefulSet(serving, ns, "garage:test", "h", resource.MustParse("10Gi")),
		planePod(serving, 0, "node-a", true),
	).Build()
	d := testDriver(t, c, ns)

	got, err := d.DataPlanes(ctx, store)
	if err != nil {
		t.Fatalf("DataPlanes: %v", err)
	}
	if len(got) != 1 || got[0].Incarnation != 2 || !got[0].Serving {
		t.Fatalf("planes = %+v, want only the serving plane 2", got)
	}

	// And a store that was never built has no planes at all.
	if err := c.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(serving), Namespace: ns}}); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if got, err := d.DataPlanes(ctx, store); err != nil || len(got) != 0 {
		t.Errorf("planes = %+v, %v; want none", got, err)
	}
}

func TestIsReplicaOf(t *testing.T) {
	for pod, want := range map[string]bool{
		"system-garage-0":          true,
		"system-garage-12":         true,
		"system-garage-":           false,
		"system-garage-i2-0":       false,
		"system-garage-copy-abcde": false,
	} {
		if got := isReplicaOf(pod, "system-garage"); got != want {
			t.Errorf("isReplicaOf(%q) = %v, want %v", pod, got, want)
		}
	}
}
