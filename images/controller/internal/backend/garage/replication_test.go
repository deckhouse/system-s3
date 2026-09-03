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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
		if got.incarnation != firstIncarnation {
			t.Errorf("incarnation=%d, want %d", got.incarnation, firstIncarnation)
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

	t.Run("incarnation is read back", func(t *testing.T) {
		d := &Driver{client: fake.NewClientBuilder().WithScheme(s).WithObjects(configMap(map[string]string{
			configFileName: "replication_factor = 1\n",
			incarnationKey: "4",
		})).Build(), namespace: ns}
		got, err := d.pinnedState(context.Background(), store)
		if err != nil || got.incarnation != 4 {
			t.Errorf("incarnation=%d err=%v, want 4/nil", got.incarnation, err)
		}
	})
}

func TestIncarnationFromConfigMap(t *testing.T) {
	sys := systemStore()
	cases := map[string]struct {
		cm   *corev1.ConfigMap
		want int32
	}{
		"nil": {nil, firstIncarnation},
		"absent (legacy)": {&corev1.ConfigMap{Data: map[string]string{configFileName: "replication_factor = 3\n"}},
			firstIncarnation},
		"unparseable": {&corev1.ConfigMap{Data: map[string]string{incarnationKey: "later"}}, firstIncarnation},
		"below floor": {&corev1.ConfigMap{Data: map[string]string{incarnationKey: "0"}}, firstIncarnation},
		"recorded":    {&corev1.ConfigMap{Data: map[string]string{incarnationKey: "7"}}, 7},
	}
	for name, c := range cases {
		if got := incarnationFromConfigMap(c.cm); got != c.want {
			t.Errorf("incarnationFromConfigMap(%s)=%d, want %d", name, got, c.want)
		}
	}

	// The ConfigMap always records the counter: it is what keeps a recreated
	// store off the previous incarnation's data directories.
	if cm := buildConfigMap(sys, "d8-system-s3", 1, 3); cm.Data[incarnationKey] != "3" {
		t.Errorf("ConfigMap incarnation=%q, want 3", cm.Data[incarnationKey])
	}
}

// A replica-count switch without the acknowledgement must not tear anything down.
//
// The module declares the same rule on its settings, and that is the better place
// to refuse — the edit never lands. But it cannot be the guarantee: Deckhouse
// skips transition rules when there is no previous value to compare with, so a
// ModuleConfig that carried no settings until now (everything on defaults) gets
// the flip through unexamined, and the rule is not evaluated at all before DKP
// 1.77. An e2e run flipped singleReplica on such a ModuleConfig and the API
// server accepted it without a word. The teardown happens in the driver, so the
// driver has the last word.
func TestReplicaSwitchNeedsTheAcknowledgement(t *testing.T) {
	// teardownScheme, not rfScheme: this exercises the recreate path, which
	// touches the StatefulSet.
	s := teardownScheme(t)
	ns := "d8-system-s3"
	ctx := context.Background()

	// A store already pinned at 3, now asked for one replica.
	pinnedAtThree := func() *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configName(rfStore()), Namespace: ns},
			Data: map[string]string{
				configFileName: "replication_factor = 3\n",
				incarnationKey: "1",
			},
		}
	}
	// The data plane that a recreate would destroy.
	statefulSet := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(rfStore()), Namespace: ns, Labels: commonLabels(rfStore()),
		}}
	}

	t.Run("no acknowledgement: refused, and the data plane is untouched", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(pinnedAtThree(), statefulSet()).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, image: "garage:test", log: testGarageLogger(t)}

		store := singleReplicaStore()
		store.RecreateConfirmation = ""

		state, err := d.EnsureStore(ctx, store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if state.Ready {
			t.Error("Ready=true while refusing to converge")
		}
		if !strings.Contains(state.Message, "recreateConfirmation") {
			t.Errorf("message %q does not say what to set", state.Message)
		}

		// The StatefulSet is what holds the data; a refusal that still deleted it
		// would be the worst outcome of the three.
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)},
			&appsv1.StatefulSet{}); err != nil {
			t.Errorf("the data plane was torn down by a refused switch: %v", err)
		}
		// And the pinned factor is still the old one.
		got, err := d.pinnedState(ctx, store)
		if err != nil {
			t.Fatalf("read pinned state: %v", err)
		}
		if got.rf != 3 {
			t.Errorf("pinned replication factor is %d, want the original 3", got.rf)
		}
	})

	t.Run("wrong acknowledgement text: still refused", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(pinnedAtThree(), statefulSet()).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, image: "garage:test", log: testGarageLogger(t)}

		store := singleReplicaStore()
		store.RecreateConfirmation = "yes"

		state, err := d.EnsureStore(ctx, store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if state.Ready || !strings.Contains(state.Message, "recreateConfirmation") {
			t.Errorf("state=%+v, want a refusal naming the setting", state)
		}
	})

	t.Run("acknowledged: the teardown proceeds", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(pinnedAtThree(), statefulSet()).Build()
		d := &Driver{client: c, apiReader: c, namespace: ns, image: "garage:test", log: testGarageLogger(t)}

		store := singleReplicaStore()
		store.RecreateConfirmation = backend.RecreateConfirmationValue

		state, err := d.EnsureStore(ctx, store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// The first pass issues the deletes and reports progress rather than
		// finishing, which is the teardown's contract.
		if state.Ready {
			t.Error("Ready=true on the pass that started the teardown")
		}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: resourceName(store)},
			&appsv1.StatefulSet{}); err == nil {
			t.Error("the StatefulSet survived an acknowledged recreate")
		}
	})
}
