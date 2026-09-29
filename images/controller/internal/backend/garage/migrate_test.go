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
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func TestMigrationTrigger(t *testing.T) {
	ha := func() *backend.Store { s := systemStore(); s.HighAvailability = true; return s }
	single := func() *backend.Store { s := systemStore(); s.HighAvailability = false; return s }
	asked := func(s *backend.Store) *backend.Store { s.HighAvailabilityExplicit = true; return s }

	cases := []struct {
		name       string
		desired    *backend.Store
		serving    *backend.Store
		boundClass string
		want       string // substring of the reason; "" means nothing to report
		start      bool
	}{
		{name: "nothing changed", desired: ha(), serving: ha(), boundClass: localSC},
		{
			name: "the mode was turned on", desired: ha(), serving: single(), boundClass: localSC,
			want: "high-availability mode", start: true,
		},
		{
			// Masters came back and the derived mode says so. Growing needs no
			// asking: the store gains a copy, while everything is healthy.
			name: "the mode grew on its own", desired: ha(), serving: single(), boundClass: localSC,
			want: "high-availability mode", start: true,
		},
		{
			// The one the module refuses. A cluster that lost masters reads as not
			// highly available, and that is not a request to drop a replica.
			name: "the mode shrank on its own", desired: single(), serving: ha(), boundClass: localSC,
			want: "not a request for it", start: false,
		},
		{
			name: "the mode was turned off by hand", desired: asked(single()), serving: ha(), boundClass: localSC,
			want: "high-availability mode", start: true,
		},
		{
			name: "the class was changed", desired: ha(), serving: ha(), boundClass: "sds-local-volume-lvm",
			want: "StorageClass", start: true,
		},
		{
			// A store with no claims yet has no class to disagree with: the first
			// cluster is built from the setting, it is not migrated to it.
			name: "no claims exist yet", desired: ha(), serving: ha(), boundClass: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := migrationTrigger(tc.desired, tc.serving, tc.boundClass)
			if tc.want == "" {
				if got.reason != "" {
					t.Errorf("reason=%q, want nothing to report", got.reason)
				}
				return
			}
			if !strings.Contains(got.reason, tc.want) {
				t.Errorf("reason=%q, want it to mention %q", got.reason, tc.want)
			}
			if got.start != tc.start {
				t.Errorf("start=%v, want %v — %q", got.start, tc.start, got.reason)
			}
		})
	}
}

// TestAShrinkingClusterDoesNotShrinkTheStore is the rule spelled out on its own,
// because it is the one that costs redundancy if it is ever quietly changed.
//
// The high-availability mode is derived when nobody sets it, so a cluster that
// loses masters starts reporting "not highly available". Acting on that would
// move the system storage to a single replica at exactly the moment the cluster
// is least able to afford it — and it would do so without anybody asking.
func TestAShrinkingClusterDoesNotShrinkTheStore(t *testing.T) {
	serving := systemStore()
	serving.HighAvailability = true

	derived := systemStore()
	derived.HighAvailability = false // the control plane lost masters

	plan := migrationTrigger(derived, serving, localSC)
	if plan.start {
		t.Error("losing masters started a migration to a single replica: that is a loss of redundancy nobody asked for")
	}
	if !strings.Contains(plan.reason, "highAvailability: false") {
		t.Errorf("reason=%q, want it to name the setting that would make it happen", plan.reason)
	}

	asked := systemStore()
	asked.HighAvailability = false
	asked.HighAvailabilityExplicit = true
	if !migrationTrigger(asked, serving, localSC).start {
		t.Error("an operator who wrote highAvailability: false was not obeyed")
	}
}

// TestClaimBelongsTo is the trap this function exists for: the name of the first
// plane is a PREFIX of the name of the second, so a prefix match would delete the
// volumes of the cluster that serves while decommissioning the one that does not.
func TestClaimBelongsTo(t *testing.T) {
	cases := []struct {
		claim   string
		setName string
		want    bool
	}{
		{"data-system-garage-0", "system-garage", true},
		{"data-system-garage-11", "system-garage", true},
		{"data-system-garage-i2-0", "system-garage-i2", true},
		{"data-system-garage-i2-0", "system-garage", false},
		{"data-system-garage-0", "system-garage-i2", false},
		{"data-system-garage", "system-garage", false},
		{"system-garage-0", "system-garage", false},
		{"data-", "system-garage", false},
	}
	for _, tc := range cases {
		if got := claimBelongsTo(tc.claim, tc.setName); got != tc.want {
			t.Errorf("claimBelongsTo(%q, %q)=%v, want %v", tc.claim, tc.setName, got, tc.want)
		}
	}
}

// TestEndpointSelectorNarrowsOnlyWithTwoPlanes: narrowing the live Service's
// selector on an installation that has one plane would point it at a label its
// pods only grow as they roll — an outage for the length of a rollout, on an
// upgrade that changes nothing else.
func TestEndpointSelectorNarrowsOnlyWithTwoPlanes(t *testing.T) {
	store := systemStore()

	neverMigrated := endpointSelector(store, migrationState{serving: 1, phase: phaseConverged})
	if _, found := neverMigrated[incarnationLabel]; found {
		t.Errorf("the endpoint selector names a plane on a store that has never migrated: %v", neverMigrated)
	}

	// Once a store has migrated the selector keeps naming a plane, including after
	// the migration has converged: the pods of the cluster being torn down still
	// carry the store's labels while they terminate.
	settled := endpointSelector(store, migrationState{serving: 2, phase: phaseConverged, recorded: true})
	if settled[incarnationLabel] != "2" {
		t.Errorf("the endpoint selector=%v, want it to keep naming plane 2", settled)
	}

	migrating := endpointSelector(store, migrationState{serving: 1, other: 2, phase: phaseCopying, recorded: true})
	if migrating[incarnationLabel] != "1" {
		t.Errorf("the endpoint selector=%v, want the serving plane 1", migrating)
	}

	afterCutover := endpointSelector(store, migrationState{serving: 2, other: 1, phase: phaseDraining, recorded: true})
	if afterCutover[incarnationLabel] != "2" {
		t.Errorf("the endpoint selector=%v, want the new plane 2", afterCutover)
	}
}

// TestMigrationStartRecordsWhatItIsBuilding: the shape is fixed when the move
// starts, so an edit made while it runs cannot reshape a cluster mid-copy.
func TestMigrationStartRecordsWhatItIsBuilding(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", copierImage: "controller:test", log: testGarageLogger(t)}

	// The mode and the class together: the class cannot change in place, so this
	// is a move to a second cluster (the mode alone is an in-place change, see
	// TestAModeChangeAloneIsMadeInPlace).
	const otherSC = "huawei-storage-stable"
	serving := systemStore() // high availability, on the local class
	serving.Incarnation = 1
	desired := systemStore()
	desired.HighAvailability = false
	desired.StorageClass = otherSC
	// Written by an operator, not derived from a cluster that lost masters: the
	// derived version of this change is refused (see
	// TestAShrinkingClusterDoesNotShrinkTheStore).
	desired.HighAvailabilityExplicit = true

	got, msg, err := d.migrationStart(ctx, desired, serving, localSC, migrationState{serving: 1, phase: phaseConverged})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.phase != phaseProvisioning || got.other != 2 || got.serving != 1 {
		t.Errorf("state=%+v, want plane 2 being provisioned while 1 serves", got)
	}
	if got.ha || got.storageClass != otherSC {
		t.Errorf("the recorded shape is %v/%q, want the settings' non-HA on %q", got.ha, got.storageClass, otherSC)
	}
	if !strings.Contains(msg, "high-availability mode") {
		t.Errorf("message=%q, want the reason in it", msg)
	}

	// And it is on the object, not only in the returned value: the next pass reads
	// the ConfigMap, not this.
	reloaded, err := d.loadMigrationState(ctx, serving)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.phase != phaseProvisioning || reloaded.other != 2 {
		t.Errorf("the reloaded state=%+v, want the migration that was just started", reloaded)
	}
}

// TestMigrationStartRefusesWithNothingToCopyWith: a migration that provisioned a
// second cluster and then found it had no copier would have doubled the footprint
// to reach the same place the message reaches for nothing.
func TestMigrationStartRefusesWithNothingToCopyWith(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)} // no copierImage

	serving := systemStore()
	serving.Incarnation = 1
	desired := systemStore()
	desired.StorageClass = "huawei-storage-stable" // a class change needs the copier

	got, msg, err := d.migrationStart(ctx, desired, serving, localSC, migrationState{serving: 1, phase: phaseConverged})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.phase != phaseConverged {
		t.Errorf("phase=%q, want no migration started", got.phase)
	}
	if !strings.Contains(msg, "CONTROLLER_IMAGE") {
		t.Errorf("message=%q, want it to name what is missing", msg)
	}
}

// TestAbortWhenTheSettingsMoveOn: before the cutover an abort is free, and it is
// what stops a copy from finishing into a cluster of a shape nobody asks for any
// more.
func TestAbortWhenTheSettingsMoveOn(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}

	serving := systemStore()
	serving.Incarnation = 1
	inFlight := migrationState{serving: 1, other: 2, phase: phaseCopying, ha: false, storageClass: localSC}

	t.Run("the settings still ask for the cluster being built", func(t *testing.T) {
		desired := systemStore()
		desired.HighAvailability = false
		desired.HighAvailabilityExplicit = true // somebody asked for one replica
		_, _, aborted := d.abortIfSettingsMovedOn(ctx, desired, serving, inFlight)
		if aborted {
			t.Error("a migration was called off while the settings still ask for it")
		}
	})

	t.Run("the setting that authorised the shrink was taken away", func(t *testing.T) {
		// The value still reads false, because the control plane is still short of
		// masters — which is the thing the guard exists to ignore. What is gone is
		// the setting, and with it the only reason this cluster was being built.
		derived := systemStore()
		derived.HighAvailability = false
		got, msg, aborted := d.abortIfSettingsMovedOn(ctx, derived, serving, inFlight)
		if !aborted {
			t.Fatal("a shrink carried on after the setting authorising it was removed")
		}
		if got.phase != phaseDecommission {
			t.Errorf("phase=%q, want the half-built plane removed", got.phase)
		}
		if msg == "" {
			t.Error("the abort was silent")
		}
	})

	t.Run("the settings changed their mind", func(t *testing.T) {
		desired := systemStore() // back to the highly available mode
		got, msg, aborted := d.abortIfSettingsMovedOn(ctx, desired, serving, inFlight)
		if !aborted {
			t.Fatal("the migration carried on into a cluster nobody asks for")
		}
		if got.phase != phaseDecommission || got.other != 2 {
			t.Errorf("state=%+v, want the half-built plane 2 being removed", got)
		}
		if got.serving != 1 {
			t.Errorf("serving=%d, want the store to stay on the cluster it is on", got.serving)
		}
		if msg == "" {
			t.Error("the abort was silent")
		}
	})
}

// TestDrainKeepsThePreviousClusterForADay: the way back from a migration whose
// damage shows up after the switch.
func TestDrainKeepsThePreviousClusterForADay(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}
	serving := systemStore()

	fresh := migrationState{serving: 2, other: 1, phase: phaseDraining, cutoverAt: time.Now().Add(-time.Hour)}
	got, msg, err := d.migrationDrain(ctx, serving, fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.phase != phaseDraining {
		t.Errorf("phase=%q an hour after the cutover, want the old cluster kept", got.phase)
	}
	if !strings.Contains(msg, "read-only") {
		t.Errorf("message=%q, want it to say what the old cluster is for", msg)
	}

	old := migrationState{serving: 2, other: 1, phase: phaseDraining, cutoverAt: time.Now().Add(-drainPeriod - time.Minute)}
	got, _, err = d.migrationDrain(ctx, serving, old)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.phase != phaseDecommission {
		t.Errorf("phase=%q after the drain period, want %q", got.phase, phaseDecommission)
	}
}

// TestTeardownPlaneLeavesTheServingClusterAlone is the test this whole file is
// for. Decommission runs on a namespace holding two Garage clusters, one of which
// is serving every consumer of the platform.
func TestTeardownPlaneLeavesTheServingClusterAlone(t *testing.T) {
	ctx := context.Background()
	ns := "d8-system-s3"
	serving := systemStore()
	serving.Incarnation = 2
	retiring := planeOf(systemStore(), 1, true, localSC)

	objects := []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(serving), Namespace: ns}},
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(retiring), Namespace: ns}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName(serving), Namespace: ns}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName(retiring), Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: s3SvcName(serving), Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: planeSvcName(serving), Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: planeSvcName(retiring), Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(serving) + "-0", Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(retiring) + "-0", Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(retiring) + "-1", Namespace: ns}},
	}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(objects...).Build()
	d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

	if err := d.teardownPlane(ctx, retiring); err != nil {
		t.Fatalf("teardown: %v", err)
	}

	mustExist := []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(serving), Namespace: ns}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName(serving), Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: s3SvcName(serving), Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: planeSvcName(serving), Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(serving) + "-0", Namespace: ns}},
	}
	for _, obj := range mustExist {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Errorf("the decommission took %s from the serving cluster: %v", obj.GetName(), err)
		}
	}

	mustBeGone := []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(retiring), Namespace: ns}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configName(retiring), Namespace: ns}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: planeSvcName(retiring), Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(retiring) + "-0", Namespace: ns}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(retiring) + "-1", Namespace: ns}},
	}
	for _, obj := range mustBeGone {
		err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		if err == nil {
			t.Errorf("%s survived the decommission of its cluster", obj.GetName())
		} else if !apierrors.IsNotFound(err) {
			t.Errorf("read %s: %v", obj.GetName(), err)
		}
	}
}

// TestCopyJobIsPowerless: the copier is given two S3 endpoints and nothing else.
// It runs the controller's image, and the controller's image can do a great deal
// more than copy objects if it is ever handed a token.
func TestCopyJobIsPowerless(t *testing.T) {
	d := &Driver{namespace: "d8-system-s3", copierImage: "controller:test", clusterDomain: "cluster.local"}
	source := planeOf(systemStore(), 1, true, localSC)
	target := planeOf(systemStore(), 2, false, "huawei-storage-stable")

	job := d.buildCopyJob("copy", source, target, []string{"a", "b"}, "stamp")
	spec := job.Spec.Template.Spec

	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Error("the copier mounts a ServiceAccount token")
	}
	if spec.ServiceAccountName != "" {
		t.Errorf("the copier runs as %q, want the namespace default with no token", spec.ServiceAccountName)
	}
	if got := spec.Containers[0].Image; got != "controller:test" {
		t.Errorf("image=%q, want the controller's own", got)
	}

	env := map[string]string{}
	for _, e := range spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if !strings.HasPrefix(env["MIGRATE_SOURCE_ENDPOINT"], planeSvcName(source)+".") {
		t.Errorf("the copier reads from %q, want the serving plane's own Service", env["MIGRATE_SOURCE_ENDPOINT"])
	}
	if !strings.HasPrefix(env["MIGRATE_TARGET_ENDPOINT"], planeSvcName(target)+".") {
		t.Errorf("the copier writes to %q, want the incoming plane's own Service", env["MIGRATE_TARGET_ENDPOINT"])
	}
	if env["MIGRATE_SOURCE_ENDPOINT"] == env["MIGRATE_TARGET_ENDPOINT"] {
		t.Error("the copier reads and writes the same cluster")
	}
	if job.Annotations[annCopyBuckets] != "stamp" {
		t.Errorf("the Job does not record the buckets it was started for: %v", job.Annotations)
	}
}

// TestCopyJobIsReplacedWhenTheBucketsChange: a bucket created while the bulk copy
// runs is not in the running Job's list, and a Job's pod template cannot be
// edited.
func TestCopyJobIsReplacedWhenTheBucketsChange(t *testing.T) {
	ctx := context.Background()
	ns := "d8-system-s3"
	source := planeOf(systemStore(), 1, true, localSC)
	target := planeOf(systemStore(), 2, false, localSC)

	name := copyJobName(source, target.Incarnation, copyPassBulk)
	started := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: ns,
		Annotations: map[string]string{annCopyBuckets: bucketsStamp([]string{"one"})},
	}}
	started.Status.Succeeded = 1

	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(started).Build()
	d := &Driver{client: c, apiReader: c, namespace: ns, copierImage: "controller:test", log: testGarageLogger(t)}

	outcome, msg, err := d.runCopyPass(ctx, source, target, copyPassBulk, []string{"one", "two"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome == copyDone {
		t.Error("a copy that never looked at the second bucket was accepted as finished")
	}
	if msg == "" {
		t.Error("the restart was silent")
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Errorf("the stale copier Job was not removed: %v", err)
	}
}

// TestBucketsStampIgnoresNothingButOrder: the stamp is what decides whether the
// copier is replaced, so it must change when the buckets do and not when they are
// merely listed differently.
func TestBucketsStampIgnoresNothingButOrder(t *testing.T) {
	if bucketsStamp([]string{"a", "b"}) == bucketsStamp([]string{"a", "b", "c"}) {
		t.Error("a bucket can be added without the copier noticing")
	}
	same := []string{"a", "b"}
	if bucketsStamp(same) != bucketsStamp([]string{"a", "b"}) {
		t.Error("the stamp is not stable")
	}
}

// TestCutoverGivesTheWritesBackIfItCannotFinish: the freeze is the only part of a
// migration a consumer feels, and it is the one part that must not be open-ended.
// A switch that cannot complete returns to copying, and write access comes back
// with the next ordinary pass.
func TestCutoverGivesTheWritesBackIfItCannotFinish(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}

	serving := planeOf(systemStore(), 1, true, localSC)
	stuck := migrationState{
		serving:   1,
		other:     2,
		phase:     phaseCutover,
		frozenAt:  time.Now().Add(-cutoverDeadline - time.Minute),
		startedAt: time.Now().Add(-4 * time.Hour),
	}

	got, msg, err := d.migrationCutover(ctx, serving, stuck)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.phase != phaseCopying {
		t.Errorf("phase=%q, want the cutover to stand down to %q", got.phase, phaseCopying)
	}
	if !got.frozenAt.IsZero() {
		t.Errorf("frozenAt=%v, want the freeze forgotten so the next attempt bounds itself", got.frozenAt)
	}
	if !strings.Contains(msg, "writes are allowed again") {
		t.Errorf("message=%q, want it to say the writes are back", msg)
	}
	if got.serving != 1 {
		t.Errorf("serving=%d, want the store still on the cluster it was on", got.serving)
	}
}

// TestApplyKnowsEveryObjectTheDriverApplies: apply() refuses a type mergeDesired
// has no case for, loudly and on purpose — an object created once and never
// updated again is the failure that switch exists to prevent. The refusal is only
// useful if it is found here rather than in the middle of a migration, which is
// where it was found the first time: the copier's credentials Secret was a type
// the switch had never seen.
func TestApplyKnowsEveryObjectTheDriverApplies(t *testing.T) {
	store := planeOf(systemStore(), 1, true, localSC)
	objects := []client.Object{
		buildConfigMap(store, "d8-system-s3", 3),
		buildServiceAccount(store, "d8-system-s3"),
		buildPlaneService(store, "d8-system-s3"),
		buildEndpointService(store, "d8-system-s3", commonLabels(store)),
		buildRPCService(store, "d8-system-s3"),
		buildStatefulSet(store, "d8-system-s3", "garage:test", "hash", testVolumeSize),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: migrationKeysName(store), Namespace: "d8-system-s3"},
			Data: map[string][]byte{migrationSecretSourceID: []byte("GK1")}},
	}
	for _, desired := range objects {
		t.Run(fmt.Sprintf("%T", desired), func(t *testing.T) {
			live := desired.DeepCopyObject().(client.Object)
			if err := mergeDesired(live, desired); err != nil {
				t.Errorf("the driver applies this object and apply() cannot: %v", err)
			}
		})
	}
}

// TestDisruptionBudgetOnlyWhereThereIsAQuorumToLose: the budget is what makes a
// rolling drain of the control plane ask whether the previous replica came back.
// Over a single replica it would only stand in the way — nothing to protect, and
// a drain that waits forever.
func TestDisruptionBudgetOnlyWhereThereIsAQuorumToLose(t *testing.T) {
	ctx := context.Background()
	ns := "d8-system-s3"

	t.Run("highly available: the budget exists and allows one at a time", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(modeScheme(t)).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

		store := planeOf(systemStore(), 1, true, localSC)
		if err := d.ensureDisruptionBudget(ctx, store); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		pdb := &policyv1.PodDisruptionBudget{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)}, pdb); err != nil {
			t.Fatalf("no disruption budget for a three-replica store: %v", err)
		}
		if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
			t.Errorf("maxUnavailable=%v, want 1", pdb.Spec.MaxUnavailable)
		}
		if pdb.Spec.Selector == nil || pdb.Spec.Selector.MatchLabels[incarnationLabel] != "1" {
			t.Errorf("selector=%v, want the pods of this plane only", pdb.Spec.Selector)
		}
	})

	t.Run("a single replica: the budget is removed", func(t *testing.T) {
		store := planeOf(systemStore(), 1, false, localSC)
		existing := buildPodDisruptionBudget(planeOf(systemStore(), 1, true, localSC), ns)
		c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(existing).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, log: testGarageLogger(t)}

		if err := d.ensureDisruptionBudget(ctx, store); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)}, &policyv1.PodDisruptionBudget{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("the budget survived a store that has one replica: %v", err)
		}
	})
}

// TestTheIncomingClusterIsProtectedWhileItIsBeingBuilt: the budget used to be
// created only for the cluster that serves, which left the one being built —
// three pods already holding a copy of the data — with nothing between it and a
// node drain. An e2e run found the same hole from the other side: right after a
// cutover the cluster that now served had no budget either, because nothing had
// ever made one for it.
//
// The test reads the source rather than the behaviour, because provisioning needs
// a Garage cluster to be visible before it reaches anything worth asserting on.
func TestTheIncomingClusterIsProtectedWhileItIsBeingBuilt(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(src)

	section := func(fn string) string {
		start := strings.Index(body, fn)
		if start < 0 {
			t.Fatalf("%s is gone; this guard needs rewriting", fn)
		}
		end := strings.Index(body[start+len(fn):], "\nfunc ")
		if end < 0 {
			return body[start:]
		}
		return body[start : start+len(fn)+end]
	}

	if !strings.Contains(section("func (d *Driver) migrationProvision("), "ensureDisruptionBudget(ctx, incoming)") {
		t.Error("the cluster being built gets no disruption budget; a drain during the copy takes all of its replicas at once")
	}
	if !strings.Contains(section("func (d *Driver) teardownPlane("), "PodDisruptionBudget") {
		t.Error("a retired cluster leaves its disruption budget behind")
	}
}

// TestProvisionAssignsTheLayoutBeforeItWaitsForReadiness is a guard on an order
// that looks harmless and is not.
//
// A pod of the incoming cluster becomes ready because the layout reached it —
// that is what the readiness probe reports. Provisioning that waits for readiness
// before assigning the layout therefore waits for what it is itself supposed to
// cause: an e2e run watched a migration sit at "0/3 pods ready" for twenty-one
// minutes with three healthy pods in it.
//
// The test reads the source rather than the behaviour, because the behaviour
// needs a Garage cluster to be visible at all. It is a crude check and it is the
// one that would have caught this.
func TestProvisionAssignsTheLayoutBeforeItWaitsForReadiness(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(src)

	start := strings.Index(body, "func (d *Driver) migrationProvision(")
	if start < 0 {
		t.Fatal("migrationProvision is gone; this guard needs rewriting")
	}
	mesh := strings.Index(body[start:], "ensureMeshAndLayout")
	wait := strings.Index(body[start:], "if !ready {")
	if mesh < 0 || wait < 0 {
		t.Fatalf("migrationProvision no longer both assigns the layout and waits for readiness (mesh=%d wait=%d)", mesh, wait)
	}
	if wait < mesh {
		t.Error("provisioning waits for the pods to be ready before it assigns the layout; " +
			"they become ready because of it, so this is a deadlock")
	}
}
