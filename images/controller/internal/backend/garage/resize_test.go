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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func TestResizeInPlace(t *testing.T) {
	const otherSC = "huawei-storage-stable"
	cases := []struct {
		name       string
		desiredHA  bool
		desiredSC  string
		boundClass string
		want       bool
	}{
		{"the mode alone", false, localSC, localSC, true},
		{"the mode, the class unset in the settings", false, "", localSC, true},
		{"the mode, the bound class unknown", false, localSC, "", true},
		{"the mode and the class", false, otherSC, localSC, false},
		{"the class alone", true, otherSC, localSC, false},
		{"nothing", true, localSC, localSC, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serving := systemStore()
			desired := systemStore()
			desired.HighAvailability = tc.desiredHA
			desired.StorageClass = tc.desiredSC
			if got := resizeInPlace(desired, serving, tc.boundClass); got != tc.want {
				t.Errorf("resizeInPlace=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestAModeChangeAloneIsMadeInPlace: no second plane is provisioned and no
// copier is needed. A shrink starts by freezing the writes, a growth by stopping
// the one replica.
func TestAModeChangeAloneIsMadeInPlace(t *testing.T) {
	cases := []struct {
		name      string
		servingHA bool
		wantPhase migrationPhase
		wantWord  string
	}{
		{"3 -> 1", true, phaseResizeFreeze, "writes are refused"},
		{"1 -> 3", false, phaseResizeStop, "three replicas"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
			d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)} // no copierImage

			serving := systemStore()
			serving.HighAvailability = tc.servingHA
			serving.Incarnation = 1
			desired := systemStore()
			desired.HighAvailability = !tc.servingHA
			desired.HighAvailabilityExplicit = true

			got, msg, err := d.migrationStart(ctx, desired, serving, localSC, migrationState{serving: 1, phase: phaseConverged})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.phase != tc.wantPhase || got.other != 0 || got.serving != 1 {
				t.Errorf("state=%+v, want %q on plane 1 with no second plane", got, tc.wantPhase)
			}
			if got.ha != desired.HighAvailability || got.storageClass != localSC {
				t.Errorf("the recorded shape is %v/%q, want %v/%q", got.ha, got.storageClass, desired.HighAvailability, localSC)
			}
			if !strings.Contains(msg, tc.wantWord) {
				t.Errorf("message=%q, want it to say %q", msg, tc.wantWord)
			}

			reloaded, err := d.loadMigrationState(ctx, serving)
			if err != nil {
				t.Fatalf("reload: %v", err)
			}
			if reloaded.phase != tc.wantPhase || reloaded.other != 0 || reloaded.ha != desired.HighAvailability {
				t.Errorf("the reloaded state=%+v, want the change that was just started", reloaded)
			}
		})
	}
}

// TestResizeStateRoundTrip: what a shrink has to give back is on the object, not
// in memory. A controller restarted under a freeze that forgot whose writes it
// took would leave them read-only for good.
func TestResizeStateRoundTrip(t *testing.T) {
	store := systemStore()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}
	ctx := context.Background()

	at := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	want := migrationState{
		serving:      1,
		phase:        phaseResizeSync,
		ha:           false,
		storageClass: localSC,
		startedAt:    at,
		frozenAt:     at.Add(time.Minute),
		repairAt:     at.Add(2 * time.Minute),
		frozenKeys:   []frozenKey{{Bucket: "b1", Key: "GK1"}, {Bucket: "b2", Key: "GK2"}},
	}
	if err := d.saveMigrationState(ctx, store, want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := d.loadMigrationState(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.phase != want.phase || got.serving != 1 || got.other != 0 {
		t.Errorf("state=%+v, want %+v", got, want)
	}
	if got.ha != want.ha || got.storageClass != want.storageClass {
		t.Errorf("target shape=%v/%q, want %v/%q", got.ha, got.storageClass, want.ha, want.storageClass)
	}
	if !got.frozenAt.Equal(want.frozenAt) || !got.repairAt.Equal(want.repairAt) {
		t.Errorf("frozenAt=%v repairAt=%v, want %v %v", got.frozenAt, got.repairAt, want.frozenAt, want.repairAt)
	}
	if len(got.frozenKeys) != 2 || got.frozenKeys[0] != want.frozenKeys[0] || got.frozenKeys[1] != want.frozenKeys[1] {
		t.Errorf("frozenKeys=%v, want %v", got.frozenKeys, want.frozenKeys)
	}
	if !got.writesFrozen() {
		t.Errorf("state=%+v does not read as frozen", got)
	}
	if !got.phase.resizing() || got.migrating() {
		t.Errorf("state=%+v, want a change in place, with no second plane", got)
	}
}

// TestWritesFrozen: a temporary key with write access is refused exactly while
// something holds the writes.
func TestWritesFrozen(t *testing.T) {
	now := time.Now()
	cases := []struct {
		state migrationState
		want  bool
	}{
		{migrationState{phase: phaseConverged}, false},
		{migrationState{phase: phaseCopying}, false},
		{migrationState{phase: phaseCutover}, true},
		{migrationState{phase: phaseResizeFreeze}, false},
		{migrationState{phase: phaseResizeFreeze, frozenAt: now}, true},
		{migrationState{phase: phaseResizeStop, frozenAt: now}, true},
		{migrationState{phase: phaseResizeStop}, false}, // a growth freezes nothing
		{migrationState{phase: phaseResizeSync, frozenAt: now}, true},
	}
	for _, tc := range cases {
		if got := tc.state.writesFrozen(); got != tc.want {
			t.Errorf("%s frozen=%v: writesFrozen=%v, want %v", tc.state.phase, !tc.state.frozenAt.IsZero(), got, tc.want)
		}
	}
}

func TestStatisticsInSync(t *testing.T) {
	node := func(queue, errs, merkle int64) nodeStatistics {
		var s nodeStatistics
		s.BlockManagerStats = &struct {
			RCEntries      int64 `json:"rcEntries"`
			ResyncQueueLen int64 `json:"resyncQueueLen"`
			ResyncErrors   int64 `json:"resyncErrors"`
		}{RCEntries: 100, ResyncQueueLen: queue, ResyncErrors: errs}
		s.TableStats = append(s.TableStats, struct {
			TableName      string `json:"tableName"`
			MerkleQueueLen int64  `json:"merkleQueueLen"`
			InsertQueueLen int64  `json:"insertQueueLen"`
		}{TableName: "object", MerkleQueueLen: merkle})
		return s
	}
	cases := []struct {
		name     string
		stats    nodeStatisticsResponse
		want     int32
		wantSync bool
		wantWhy  string
	}{
		{"three in sync", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": node(0, 0, 0), "b": node(0, 0, 0), "c": node(0, 0, 0)}}, 3, true, ""},
		{"a node that did not answer", nodeStatisticsResponse{
			Success: map[string]nodeStatistics{"a": node(0, 0, 0), "b": node(0, 0, 0)},
			Error:   map[string]string{"c": "timeout"}}, 3, false, "did not answer"},
		{"fewer nodes than replicas", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": node(0, 0, 0)}}, 3, false, "1 of 3"},
		// A queue alone is a store being written to, not one out of sync.
		{"blocks queued", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": node(0, 0, 0), "b": node(2488, 0, 0), "c": node(0, 0, 0)}}, 3, true, ""},
		{"table work", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": node(0, 0, 5), "b": node(0, 0, 0), "c": node(0, 0, 0)}}, 3, false, "5 table update(s)"},
		{"resync errors", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": node(0, 1, 0), "b": node(0, 0, 0), "c": node(0, 0, 0)}}, 3, false, "failing to resync"},
		{"a node without statistics", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": {}, "b": node(0, 0, 0), "c": node(0, 0, 0)}}, 3, false, "no statistics"},
		{"one of one", nodeStatisticsResponse{Success: map[string]nodeStatistics{
			"a": node(0, 0, 0)}}, 1, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synced, why := statisticsInSync(&tc.stats, tc.want)
			if synced != tc.wantSync || !strings.Contains(why, tc.wantWhy) {
				t.Errorf("synced=%v why=%q, want %v and %q", synced, why, tc.wantSync, tc.wantWhy)
			}
		})
	}
}

// TestResizeStopScalesToZeroBeforeItPurges: the layout is removed only from a
// volume no Garage process has open.
func TestResizeStopScalesToZeroBeforeItPurges(t *testing.T) {
	ctx := context.Background()
	store := planeOf(nonHAStore(), 1, false, localSC)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(store), Namespace: "d8-system-s3"}}
	sts.Spec.Replicas = ptr.To(int32(1))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName(store) + "-0", Namespace: "d8-system-s3", Labels: commonLabels(store)}}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(sts, pod).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}
	state := migrationState{serving: 1, phase: phaseResizeStop, ha: true, storageClass: localSC, startedAt: time.Now()}
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatalf("save: %v", err)
	}

	msg, err := d.advanceResizeOffline(ctx, store, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(msg, "stopping") {
		t.Errorf("message=%q, want it to wait for the pod", msg)
	}
	got := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(sts), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Errorf("replicas=%v, want 0", got.Spec.Replicas)
	}
	if reloaded, _ := d.loadMigrationState(ctx, store); reloaded.phase != phaseResizeStop {
		t.Errorf("phase=%q, want %q while a pod runs", reloaded.phase, phaseResizeStop)
	}

	if err := c.Delete(ctx, pod); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	if _, err := d.advanceResizeOffline(ctx, store, state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reloaded, _ := d.loadMigrationState(ctx, store); reloaded.phase != phaseResizePurge {
		t.Errorf("phase=%q, want %q once the pod is gone", reloaded.phase, phaseResizePurge)
	}
}

// TestResizePurgeWritesTheNewFactorOnlyAfterTheJob: the factor in garage.toml
// and the layout on the volume change together or not at all. A factor written
// before the layout is gone is a replica that refuses to start.
func TestResizePurgeWritesTheNewFactorOnlyAfterTheJob(t *testing.T) {
	ctx := context.Background()
	store := planeOf(nonHAStore(), 1, false, localSC)
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).
		WithObjects(buildConfigMap(store, "d8-system-s3", 1)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", image: "garage:test", log: testGarageLogger(t)}
	state := migrationState{serving: 1, phase: phaseResizePurge, ha: true, storageClass: localSC, startedAt: time.Now()}
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := d.advanceResizeOffline(ctx, store, state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: "d8-system-s3", Name: resourceName(store) + purgeJobSuffix}
	if err := c.Get(ctx, key, job); err != nil {
		t.Fatalf("the purge Job was not created: %v", err)
	}
	if rf := factorOf(ctx, t, d, store); rf != 1 {
		t.Errorf("replication_factor=%d while the Job runs, want the old 1", rf)
	}

	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(ctx, job); err != nil {
		t.Fatalf("complete the Job: %v", err)
	}
	if _, err := d.advanceResizeOffline(ctx, store, state); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rf := factorOf(ctx, t, d, store); rf != 3 {
		t.Errorf("replication_factor=%d after the purge, want 3", rf)
	}
	if reloaded, _ := d.loadMigrationState(ctx, store); reloaded.phase != phaseResizeSync {
		t.Errorf("phase=%q, want %q", reloaded.phase, phaseResizeSync)
	}
	if err := c.Get(ctx, key, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Errorf("the finished purge Job is still there (err=%v)", err)
	}
}

func factorOf(ctx context.Context, t *testing.T, d *Driver, store *backend.Store) int32 {
	t.Helper()
	pinned, err := d.pinnedState(ctx, store)
	if err != nil {
		t.Fatalf("read the pinned factor: %v", err)
	}
	return pinned.rf
}

// TestPurgeJobTouchesOnlyTheLayout: the Job mounts replica 0's volume and
// removes one file; it has no API access to do anything else with.
func TestPurgeJobTouchesOnlyTheLayout(t *testing.T) {
	store := planeOf(systemStore(), 2, true, localSC)
	d := &Driver{namespace: "d8-system-s3", image: "garage:test"}
	job := d.buildPurgeJob(store, "purge")
	spec := job.Spec.Template.Spec

	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Errorf("the purge pod mounts a ServiceAccount token")
	}
	if spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy=%q, want Never", spec.RestartPolicy)
	}
	if len(spec.Volumes) != 1 || spec.Volumes[0].PersistentVolumeClaim == nil ||
		spec.Volumes[0].PersistentVolumeClaim.ClaimName != "data-"+resourceName(store)+"-0" {
		t.Errorf("volumes=%+v, want replica 0's data volume only", spec.Volumes)
	}
	if len(spec.Containers) != 1 || spec.Containers[0].Image != "garage:test" {
		t.Fatalf("containers=%+v, want one in the Garage image", spec.Containers)
	}
	if labels.SelectorFromSet(commonLabels(store)).Matches(labels.Set(job.Spec.Template.Labels)) {
		t.Errorf("pod labels=%v match the replicas': planePods would count the purge pod as one", job.Spec.Template.Labels)
	}
	script := strings.Join(spec.Containers[0].Command, " ")
	if !strings.Contains(script, "rm -f "+dataMountPath+"/meta/cluster_layout") {
		t.Errorf("script=%q, want it to remove the saved layout", script)
	}
}

// TestResizeFreezeIsCalledOffWhileNothingIsFrozen: before the freeze a shrink
// costs nothing to abandon, and the settings going back is a reason to.
func TestResizeFreezeIsCalledOffWhileNothingIsFrozen(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3", log: testGarageLogger(t)}
	store := planeOf(systemStore(), 1, true, localSC)
	state := migrationState{serving: 1, phase: phaseResizeFreeze, ha: false, storageClass: localSC, startedAt: time.Now()}

	got, msg, err := d.resizeFreeze(ctx, systemStore(), store, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.phase != phaseConverged {
		t.Errorf("phase=%q, want the change called off", got.phase)
	}
	if !strings.Contains(msg, "called off") {
		t.Errorf("message=%q", msg)
	}
}

// TestReleaseRemovedReplicas: after a shrink the volumes of the replicas that
// are gone are released, and the one carrying the data is not.
func TestReleaseRemovedReplicas(t *testing.T) {
	ctx := context.Background()
	store := planeOf(nonHAStore(), 1, false, localSC)
	var objs []client.Object
	for _, ord := range []string{"0", "1", "2"} {
		objs = append(objs, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + resourceName(store) + "-" + ord, Namespace: "d8-system-s3"}})
	}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(objs...).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	if err := d.releaseRemovedReplicas(ctx, store, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, obj := range objs {
		err := c.Get(ctx, client.ObjectKeyFromObject(obj), &corev1.PersistentVolumeClaim{})
		if gone := apierrors.IsNotFound(err); gone != (i > 0) {
			t.Errorf("%s gone=%v (err=%v), want %v", obj.GetName(), gone, err, i > 0)
		}
	}
}

// TestResizeOfflineReadsPastTheCache: the Job and the StatefulSet are read with
// the APIReader. Through the cached client the first read of a Job starts a
// cluster-wide informer the controller's RBAC does not allow to list, and the
// read waits for it until the pass times out — which is how the first e2e run
// of this change sat in ResizePurge for 25 minutes.
func TestResizeOfflineReadsPastTheCache(t *testing.T) {
	ctx := context.Background()
	store := planeOf(nonHAStore(), 1, false, localSC)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(store), Namespace: "d8-system-s3"}}
	sts.Spec.Replicas = ptr.To(int32(1))
	base := fake.NewClientBuilder().WithScheme(coreScheme(t)).
		WithObjects(sts, buildConfigMap(store, "d8-system-s3", 1)).Build()
	cached := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			switch obj.(type) {
			case *batchv1.Job, *appsv1.StatefulSet:
				return fmt.Errorf("read of %T through the cache", obj)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	d := &Driver{client: cached, apiReader: base, namespace: "d8-system-s3", image: "garage:test", log: testGarageLogger(t)}

	for _, phase := range []migrationPhase{phaseResizeStop, phaseResizePurge} {
		state := migrationState{serving: 1, phase: phase, ha: true, storageClass: localSC, startedAt: time.Now()}
		if _, err := d.advanceResizeOffline(ctx, store, state); err != nil {
			t.Errorf("%s: %v", phase, err)
		}
	}
}

// TestBlocksHeldWithDeletedBlocksANodeNeverHad: the stand of 14.10. A replica
// restarted while objects were written and deleted counts five blocks it never
// had and never will; Garage keeps their entries for good. It is as full as its
// peers, and the gate must open.
func TestBlocksHeldWithDeletedBlocksANodeNeverHad(t *testing.T) {
	rc := func(n int64) nodeStatistics {
		var s nodeStatistics
		s.BlockManagerStats = &struct {
			RCEntries      int64 `json:"rcEntries"`
			ResyncQueueLen int64 `json:"resyncQueueLen"`
			ResyncErrors   int64 `json:"resyncErrors"`
		}{RCEntries: n}
		return s
	}
	stats := &nodeStatisticsResponse{Success: map[string]nodeStatistics{"a": rc(5006), "b": rc(5011), "c": rc(5006)}}
	if got, why := blocksHeld(stats, map[string]int64{"a": 5006, "b": 5006, "c": 5006}); !got {
		t.Errorf("blocksHeld=false %q for a node as full as its peers", why)
	}
	// Its peers may still have the files of those blocks until Garage collects
	// them, ten minutes after the deletion; five of five thousand is within the
	// slack.
	if got, why := blocksHeld(stats, map[string]int64{"a": 5011, "b": 5006, "c": 5011}); !got {
		t.Errorf("blocksHeld=false %q while the peers wait to collect five deleted blocks", why)
	}
}

// TestBlocksHeld: a node is full when it has a file for every block it counts.
// Its reference counts arrive with the table sync, long before the blocks do,
// which is exactly the window this has to see.
func TestBlocksHeld(t *testing.T) {
	rc := func(n int64) nodeStatistics {
		var s nodeStatistics
		s.BlockManagerStats = &struct {
			RCEntries      int64 `json:"rcEntries"`
			ResyncQueueLen int64 `json:"resyncQueueLen"`
			ResyncErrors   int64 `json:"resyncErrors"`
		}{RCEntries: n}
		return s
	}
	stats := &nodeStatisticsResponse{Success: map[string]nodeStatistics{"a": rc(2102), "b": rc(2102), "c": rc(2103)}}
	cases := []struct {
		name    string
		files   map[string]int64
		want    bool
		wantWhy string
	}{
		{"every node full", map[string]int64{"a": 2102, "b": 2102, "c": 2103}, true, ""},
		{"a block written between the two reads", map[string]int64{"a": 2103, "b": 2103, "c": 2103}, true, ""},
		{"a node that started empty", map[string]int64{"a": 2102, "b": 2102, "c": 40}, false, "holds 40 of 2103"},
		{"a node whose volume was not read", map[string]int64{"a": 2102, "b": 2102}, false, "could not be read"},
		{"a node nearly filled is still being filled", map[string]int64{"a": 2102, "b": 2102, "c": 2000}, false, "holds 2000 of 2103"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, why := blocksHeld(stats, tc.files)
			if got != tc.want || !strings.Contains(why, tc.wantWhy) {
				t.Errorf("blocksHeld=%v %q, want %v and %q", got, why, tc.want, tc.wantWhy)
			}
		})
	}
}
