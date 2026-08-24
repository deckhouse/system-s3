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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-object-system/images/controller/internal/backend"
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
	ns := "d8-sds-object-system"

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
	if cm := buildConfigMap(sys, "d8-sds-object-system", 1, 3); cm.Data[incarnationKey] != "3" {
		t.Errorf("ConfigMap incarnation=%q, want 3", cm.Data[incarnationKey])
	}
}
