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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

func rfScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return s
}

func rfStore() *backend.Store {
	return &backend.Store{Name: "system", StorageClass: localSC}
}

func TestPinnedState(t *testing.T) {
	s := rfScheme(t)
	store := rfStore()
	ns := "d8-system-s3"

	configMap := func(data map[string]string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configName(store), Namespace: ns},
			Data:       data,
		}
	}

	t.Run("no ConfigMap: compute initial factor, store not initialized yet", func(t *testing.T) {
		d := &Driver{client: fake.NewClientBuilder().WithScheme(s).Build(), namespace: ns}
		got, err := d.pinnedState(context.Background(), store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.rf != initialReplicationFactor(store) {
			t.Errorf("rf=%d, want initial %d", got.rf, initialReplicationFactor(store))
		}
		if got.exists {
			t.Errorf("exists=true, want false (no ConfigMap means never initialized)")
		}
	})

	t.Run("ConfigMap with valid factor: read it back", func(t *testing.T) {
		d := &Driver{client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(configMap(map[string]string{configFileName: "replication_factor = 3\n"})).Build(), namespace: ns}
		got, err := d.pinnedState(context.Background(), store)
		if err != nil || got.rf != 3 || !got.exists {
			t.Errorf("state=%+v err=%v, want rf 3/exists true/nil", got, err)
		}
	})

	t.Run("ConfigMap exists but factor unparseable: fail closed", func(t *testing.T) {
		d := &Driver{client: fake.NewClientBuilder().WithScheme(s).
			WithObjects(configMap(map[string]string{configFileName: "db_engine = \"lmdb\"\n"})).Build(), namespace: ns}
		_, err := d.pinnedState(context.Background(), store)
		if err == nil {
			t.Errorf("expected fail-closed error when replication_factor is absent, got nil")
		}
	})
}

// TestConfigMapRecordsItsOwnPlane: the config of a plane says which plane it is,
// so a namespace holding two Garage clusters mid-migration can be read without
// decoding names.
func TestConfigMapRecordsItsOwnPlane(t *testing.T) {
	if cm := buildConfigMap(systemStore(), "d8-system-s3", 1); cm.Data[incarnationKey] != "1" {
		t.Errorf("ConfigMap incarnation=%q, want 1", cm.Data[incarnationKey])
	}
	second := systemStore()
	second.Incarnation = 3
	if cm := buildConfigMap(second, "d8-system-s3", 1); cm.Data[incarnationKey] != "3" {
		t.Errorf("ConfigMap incarnation=%q, want 3", cm.Data[incarnationKey])
	}
}

// scheme with the workload kinds and this module's own, for the paths that read
// the StatefulSet and sum the SystemBucket sizes.
func modeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := rfScheme(t)
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add module scheme: %v", err)
	}
	if err := policyv1.AddToScheme(s); err != nil {
		t.Fatalf("add policy scheme: %v", err)
	}
	return s
}

// A change of the high-availability mode never touches the cluster that runs.
//
// This used to be the acknowledgement guard: the switch tore the data plane down
// and rebuilt it empty, and an acknowledgement string in the ModuleConfig was
// what stood between an edit and the loss of every object. The setting is gone,
// the teardown is gone, and the change is now made by copying the data into a
// second Garage cluster (see migrate.go). What has to hold on THIS cluster is
// unchanged by that and is what this pins: nothing of it is destroyed, it keeps
// serving on its pinned factor, and the pending change is visible.
//
// The last part is the one worth a test of its own: reporting the store as not
// Ready would take every consumer's bucket down over a setting somebody edited,
// hours before the migration that setting asked for could finish.
func TestModeChangeDoesNotTouchTheRunningCluster(t *testing.T) {
	s := modeScheme(t)
	ns := "d8-system-s3"
	ctx := context.Background()

	// A store already pinned at 3, now asked for one replica.
	pinnedAtThree := func() *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configName(rfStore()), Namespace: ns, Labels: commonLabels(rfStore())},
			Data: map[string]string{
				configFileName: "replication_factor = 3\n",
				incarnationKey: "1",
			},
		}
	}
	// The data plane a recreate would have destroyed.
	statefulSet := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(rfStore()), Namespace: ns, Labels: commonLabels(rfStore()),
		}}
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(pinnedAtThree(), statefulSet()).Build()
	d := testDriver(t, c, ns)

	store := rfStore() // HighAvailability=false against a store pinned at 3

	state, err := d.EnsureStore(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The StatefulSet is what holds the data.
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)},
		&appsv1.StatefulSet{}); err != nil {
		t.Errorf("the data plane was torn down by a mode change: %v", err)
	}
	// And the pinned factor is still the old one: nothing recomputed it.
	got, err := d.pinnedState(ctx, store)
	if err != nil {
		t.Fatalf("read pinned state: %v", err)
	}
	if got.rf != 3 {
		t.Errorf("pinned replication factor is %d, want the original 3", got.rf)
	}
	// The garage.toml the pods read still says 3.
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: configName(store)}, cm); err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(cm.Data[configFileName], "replication_factor = 3") {
		t.Errorf("garage.toml no longer pins factor 3: %q", cm.Data[configFileName])
	}
	// This pass stops before the store can be called healthy (no pods answer in a
	// fake client), so Ready is not what this asserts -- what it asserts is that
	// the mode disagreement is what the operator is told about.
	//
	// The fixture is a store pinned at three replicas whose settings now say one,
	// with nobody having written that: the derived mode, i.e. a cluster that lost
	// masters. So what the operator is told is the refusal to shrink on its own,
	// and the setting that would make it happen.
	if !strings.Contains(state.Message, "highAvailability: false") {
		t.Errorf("message %q does not name the setting that would move the store to one replica", state.Message)
	}
	if !strings.Contains(state.Message, "keeps its three replicas") {
		t.Errorf("message %q does not say the store is keeping its replicas", state.Message)
	}
}

// A mode change that is not applied must not move the workload either.
//
// The first version of this reported the refusal and then built the StatefulSet
// from the mode the settings asked for: the message said the factor stays at 1
// while the replica count went to 3, so the store ran three replicas at
// replication factor 1 — a shape nobody asked for and the one the refusal exists
// to prevent. An e2e run caught it as a rollout underneath a store that had just
// been told nothing would change.
func TestARefusedModeChangeDoesNotResizeTheWorkload(t *testing.T) {
	ns := "d8-system-s3"
	ctx := context.Background()

	// Pinned at one replica, asked for the highly available mode.
	cfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configName(rfStore()), Namespace: ns, Labels: commonLabels(rfStore())},
		Data: map[string]string{
			configFileName: "replication_factor = 1\n",
			incarnationKey: "1",
		},
	}
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(cfg).Build()
	d := testDriver(t, c, ns)

	store := rfStore()
	store.HighAvailability = true

	if _, err := d.EnsureStore(ctx, store); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)}, sts); err != nil {
		t.Fatalf("read the statefulset: %v", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != replicasNonHA {
		t.Errorf("replicas=%v, want %d: the workload followed a mode change that was refused",
			sts.Spec.Replicas, replicasNonHA)
	}

	// And the store description the caller passed in is untouched: the override is
	// for the pass, not a rewrite of what the settings say.
	if !store.HighAvailability {
		t.Error("the store passed in was modified; the desired mode is the caller's, not the driver's")
	}
}

// Without a StorageClass there is no data plane, and the reason has to be on the
// object rather than in the log alone.
//
// The module used to provision hostPath volumes on the control-plane nodes when
// it had nothing else, which is precisely what a platform with storage modules
// should not be doing behind their back. What replaced it is a refusal, and a
// refusal is only useful if it names what to set.
func TestNoStorageClassBringsUpNoDataPlane(t *testing.T) {
	ns := "d8-system-s3"
	// A bucket asks for storage: without one nothing is built anyway, and the
	// missing class is only a note (see TestNoBucketsAndNoClassSaysBoth).
	bucket := &v1alpha1.SystemBucket{ObjectMeta: metav1.ObjectMeta{Name: "loki", Namespace: "d8-monitoring"}}
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(bucket).Build()
	d := testDriver(t, c, ns)

	store := &backend.Store{Name: "system", HighAvailability: true} // no StorageClass

	state, err := d.EnsureStore(context.Background(), store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Ready {
		t.Error("Ready=true with no StorageClass configured")
	}
	if !strings.Contains(state.Message, "storageClass") {
		t.Errorf("message %q does not name the setting to add", state.Message)
	}

	sts := &appsv1.StatefulSet{}
	if err := c.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: resourceName(store)}, sts); err == nil {
		t.Error("a StatefulSet was created for a store with no StorageClass")
	}
}

// A StorageClass edited under a running store leaves its volumes alone — the same
// contract as the high-availability mode, for the same reason: the volumes of a
// running store cannot be re-ordered from another class in place, so the change
// is made by a migration and not here.
//
// What makes this worth its own test is where the "current" class comes from. It
// is not pinned anywhere: the only record of it is the claim the data plane
// actually holds, so the comparison is against that.
func TestStorageClassChangeDoesNotTouchTheRunningVolumes(t *testing.T) {
	ns := "d8-system-s3"
	ctx := context.Background()

	store := &backend.Store{Name: "system", HighAvailability: true, StorageClass: "ceph-csi-rbd"}
	bound := "sds-local-volume-lvm"

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(store) + "-0", Namespace: ns, Labels: commonLabels(store)},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &bound},
	}
	cfg := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configName(store), Namespace: ns},
		Data: map[string]string{
			configFileName: "replication_factor = 3\n",
			incarnationKey: "1",
		},
	}
	c := fake.NewClientBuilder().WithScheme(modeScheme(t)).WithObjects(claim, cfg).Build()
	d := testDriver(t, c, ns)

	state, err := d.EnsureStore(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(state.Message, "StorageClass") {
		t.Errorf("message %q does not mention the class that was not applied", state.Message)
	}

	// The claim is the data plane's hold on its volume; a "report" that deleted it
	// would be the change applied in the most destructive way available.
	live := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: claim.Name}, live); err != nil {
		t.Fatalf("the claim was removed by a class change that is not applied: %v", err)
	}
	if live.DeletionTimestamp != nil {
		t.Error("the claim was marked for deletion by a class change that is not applied")
	}
	if got := *live.Spec.StorageClassName; got != bound {
		t.Errorf("the claim's class is now %q, want the original %q", got, bound)
	}
}
