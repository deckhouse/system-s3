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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func coreScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return s
}

// TestLoadMigrationStateOfAnInstallationThatNeverMigrated: no state ConfigMap
// means the first plane serves and nothing is in flight. This is every
// installation that exists when this code ships, so it must not need a write to
// be read correctly.
func TestLoadMigrationStateOfAnInstallationThatNeverMigrated(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	got, err := d.loadMigrationState(context.Background(), systemStore())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.serving != firstIncarnation || got.phase != phaseConverged || got.migrating() {
		t.Errorf("state=%+v, want the first plane, converged, not migrating", got)
	}
}

// TestLostMigrationStateFallsBackToTheEndpoint: the Service is not a record of
// which plane serves, it IS which plane serves. A state ConfigMap deleted by hand
// must not make the module reconcile a plane that nothing is pointing at.
func TestLostMigrationStateFallsBackToTheEndpoint(t *testing.T) {
	store := systemStore()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: s3SvcName(store), Namespace: "d8-system-s3"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{incarnationLabel: "4"}},
	}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(svc).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	got, err := d.loadMigrationState(context.Background(), store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.serving != 4 {
		t.Errorf("serving=%d, want the 4 the endpoint selects", got.serving)
	}
	if got.phase != phaseConverged {
		t.Errorf("phase=%q, want %q: a migration that is not recorded is not running", got.phase, phaseConverged)
	}
}

// TestMigrationStateRoundTrip: what a phase writes is what the next pass reads.
// Every phase transition is a write followed by a reconcile that re-reads it, so
// a field that does not survive the round trip is a phase that repeats itself.
func TestMigrationStateRoundTrip(t *testing.T) {
	store := systemStore()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}
	ctx := context.Background()

	started := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	want := migrationState{
		serving:      1,
		other:        2,
		phase:        phaseCopying,
		ha:           false,
		storageClass: "huawei-storage-stable",
		startedAt:    started,
	}
	if err := d.saveMigrationState(ctx, store, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := d.loadMigrationState(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.serving != want.serving || got.other != want.other || got.phase != want.phase {
		t.Errorf("state=%+v, want %+v", got, want)
	}
	if got.ha != want.ha || got.storageClass != want.storageClass {
		t.Errorf("target shape=%v/%q, want %v/%q", got.ha, got.storageClass, want.ha, want.storageClass)
	}
	if !got.startedAt.Equal(started) {
		t.Errorf("startedAt=%v, want %v", got.startedAt, started)
	}
	if !got.migrating() {
		t.Errorf("state=%+v does not read as a migration in flight", got)
	}
}

// TestSaveMigrationStateForgetsTheFinishedMigration: the write is a replacement,
// not a merge. A converged state that kept the previous target would read as a
// migration in flight forever.
func TestSaveMigrationStateForgetsTheFinishedMigration(t *testing.T) {
	store := systemStore()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}
	ctx := context.Background()

	inFlight := migrationState{serving: 1, other: 2, phase: phaseCutover, storageClass: "old", cutoverAt: time.Now()}
	if err := d.saveMigrationState(ctx, store, inFlight); err != nil {
		t.Fatalf("save: %v", err)
	}
	converged := migrationState{serving: 2, phase: phaseConverged}
	if err := d.saveMigrationState(ctx, store, converged); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := d.loadMigrationState(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.serving != 2 || got.other != 0 || got.migrating() {
		t.Errorf("state=%+v, want plane 2 serving with nothing in flight", got)
	}
	if !got.cutoverAt.IsZero() {
		t.Errorf("cutoverAt=%v survived the converged write", got.cutoverAt)
	}
}

// TestParseMigrationStateRefusesNonsense: a phase written by a newer release, or
// by hand, or half-written, is read as "nothing is happening". The alternative is
// a state machine driving a plane that was never named.
func TestParseMigrationStateRefusesNonsense(t *testing.T) {
	cases := map[string]map[string]string{
		"a phase from the future":       {migrationKeyPhase: "Teleporting", migrationKeyOther: "2"},
		"a phase with no other plane":   {migrationKeyPhase: string(phaseCopying)},
		"another plane below the first": {migrationKeyPhase: string(phaseCopying), migrationKeyOther: "0"},
		"the other plane is this plane": {migrationKeyPhase: string(phaseCopying), migrationKeyServing: "1", migrationKeyOther: "1"},
		"an unreadable other plane":     {migrationKeyPhase: string(phaseCopying), migrationKeyOther: "soon"},
		"nothing written at all":        {},
		"an unreadable serving plane":   {migrationKeyServing: "the one on the left"},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			got := parseMigrationState(data)
			if got.phase != phaseConverged || got.migrating() {
				t.Errorf("state=%+v, want converged and not migrating", got)
			}
			if got.serving != firstIncarnation {
				t.Errorf("serving=%d, want %d", got.serving, firstIncarnation)
			}
		})
	}
}

// TestPlaneOf: the same store, described as one of its planes. Every builder
// takes a store, so this is how the incoming plane is reconciled by the code that
// reconciles the serving one.
func TestPlaneOf(t *testing.T) {
	store := systemStore() // high availability, the local class
	plane := planeOf(store, 2, false, "huawei-storage-stable")

	if plane.Incarnation != 2 || plane.HighAvailability || plane.StorageClass != "huawei-storage-stable" {
		t.Errorf("plane=%+v, want incarnation 2, non-HA, on huawei-storage-stable", plane)
	}
	if plane.Name != store.Name {
		t.Errorf("plane name=%q, want the store's %q", plane.Name, store.Name)
	}
	if store.Incarnation != 0 || !store.HighAvailability || store.StorageClass != localSC {
		t.Errorf("the store was modified in place: %+v", store)
	}
}

// TestStateWrittenByACutoverSurvivesTheRoundTrip: after the endpoint moves, the
// plane that is not serving is the one being retired — and on a store's first
// migration that is plane 1. Reading 1 as "there is no other plane" made the
// state the cutover had just written read back as converged: the drain never
// started, the decommission never came, and the cluster meant to be kept for a
// day and then removed was kept for good. Found on a stand, half an hour after
// a cutover that had otherwise gone perfectly.
func TestStateWrittenByACutoverSurvivesTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := systemStore()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	cutover := migrationState{
		serving:   2,
		other:     1, // the plane just retired
		phase:     phaseDraining,
		cutoverAt: time.Now(),
	}
	if err := d.saveMigrationState(ctx, store, cutover); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := d.loadMigrationState(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.phase != phaseDraining || got.other != 1 || got.serving != 2 {
		t.Errorf("state=%+v, want plane 2 serving and plane 1 draining", got)
	}
	if !got.migrating() {
		t.Error("a store with a cluster still to decommission does not read as having two planes")
	}
}
