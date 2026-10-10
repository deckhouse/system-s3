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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWriteGrantSurvivesPhaseChangeAndControllerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := testDriver(t, c, "d8-system-s3")
	store := planeOf(systemStore(), 1, true, localSC)
	state := migrationState{serving: 1, other: 2, phase: phaseCopying}
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/AllowBucketKey":
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case "/v2/ListKeys":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "GK1", "expired": false}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	svc := newAdminClient(srv.URL, "token")
	result := make(chan error, 1)
	go func() { result <- d.grantTemporaryKey(ctx, store, svc, "bucket", "GK1", true) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("grant did not reach RPC")
	}
	state.phase = phaseCutover
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	restarted := testDriver(t, c, "d8-system-s3")
	if drained, err := restarted.writeGrantsDrained(ctx, store, svc); err != nil || drained {
		t.Fatalf("freeze crossed an unfinished grant: drained=%v err=%v", drained, err)
	}
	if err := restarted.registerWriteGrant(ctx, store, "GK2"); err == nil {
		t.Fatal("write grant admitted after Cutover")
	}
	release <- struct{}{}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if drained, err := restarted.writeGrantsDrained(ctx, store, svc); err != nil || !drained {
		t.Fatalf("completed grant did not release barrier: drained=%v err=%v", drained, err)
	}
}

type grantConflictClient struct {
	client.Client
	once         sync.Once
	beforeUpdate func()
}

func (c *grantConflictClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.once.Do(c.beforeUpdate)
	return c.Client.Update(ctx, obj, opts...)
}

func TestGrantRegistrationRechecksPhaseAfterResourceVersionConflict(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	leader := testDriver(t, c, "d8-system-s3")
	store := planeOf(systemStore(), 1, true, localSC)
	state := migrationState{serving: 1, other: 2, phase: phaseCopying}
	if err := leader.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	issuer := testDriver(t, &grantConflictClient{Client: c, beforeUpdate: func() {
		state.phase = phaseCutover
		if err := leader.saveMigrationState(ctx, store, state); err != nil {
			t.Fatal(err)
		}
	}}, "d8-system-s3")
	if err := issuer.registerWriteGrant(ctx, store, "GK1"); err == nil {
		t.Fatal("conflicting registration bypassed Cutover")
	}
}

func TestPhaseWritePreservesGrantRegisteredDuringResourceVersionConflict(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	issuer := testDriver(t, c, "d8-system-s3")
	store := planeOf(systemStore(), 1, true, localSC)
	state := migrationState{serving: 1, other: 2, phase: phaseCopying}
	if err := issuer.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	leader := testDriver(t, &grantConflictClient{Client: c, beforeUpdate: func() {
		if err := issuer.registerWriteGrant(ctx, store, "GK1"); err != nil {
			t.Fatal(err)
		}
	}}, "d8-system-s3")
	state.phase = phaseCutover
	if err := leader.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: leader.namespace, Name: migrationStateName(store)}, cm); err != nil {
		t.Fatal(err)
	}
	if cm.Data[pendingWriteGrantPrefix+"GK1"] != "1" || cm.Data[migrationKeyPhase] != string(phaseCutover) {
		t.Fatalf("conflict lost ticket or phase: %v", cm.Data)
	}
}

func TestMissingKeyDoesNotReleasePendingGrant(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := testDriver(t, c, "d8-system-s3")
	store := planeOf(systemStore(), 1, true, localSC)
	if err := d.registerWriteGrant(ctx, store, "GK1"); err != nil {
		t.Fatal(err)
	}
	state := migrationState{serving: 1, other: 2, phase: phaseCutover}
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode([]keyListItem{}) }))
	defer srv.Close()
	drained, err := d.writeGrantsDrained(ctx, store, newAdminClient(srv.URL, "token"))
	if err != nil || drained {
		t.Fatalf("missing metadata released a potentially live grant: drained=%v err=%v", drained, err)
	}
}

func TestWriteGateAllowsUnfrozenReplicaGrowthButClosesBeforeShrinkFreeze(t *testing.T) {
	for _, tc := range []struct {
		phase  migrationPhase
		frozen bool
		want   bool
	}{
		{phaseResizeSync, false, true}, {phaseResizeSync, true, false}, {phaseResizeFreeze, false, false}, {phaseCutover, false, false},
	} {
		t.Run(string(tc.phase)+map[bool]string{true: "-frozen", false: "-unfrozen"}[tc.frozen], func(t *testing.T) {
			ctx := context.Background()
			c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
			d := testDriver(t, c, "d8-system-s3")
			store := planeOf(systemStore(), 1, true, localSC)
			state := migrationState{serving: 1, other: 2, phase: tc.phase}
			if tc.frozen {
				state.frozenAt = time.Now()
			}
			if err := d.saveMigrationState(ctx, store, state); err != nil {
				t.Fatal(err)
			}
			err := d.registerWriteGrant(ctx, store, "GK1")
			if (err == nil) != tc.want {
				t.Fatalf("allowed=%v want=%v err=%v", err == nil, tc.want, err)
			}
		})
	}
}

func TestAmbiguousGrantKeepsTicketUntilBackendExpiry(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "expired"}[expired], func(t *testing.T) {
			ctx := context.Background()
			c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
			d := testDriver(t, c, "d8-system-s3")
			store := planeOf(systemStore(), 1, false, localSC)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/ListKeys" {
					_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "GK1", "expired": expired}})
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer srv.Close()
			svc := newAdminClient(srv.URL, "token")
			if err := d.grantTemporaryKey(ctx, store, svc, "bucket", "GK1", true); err == nil {
				t.Fatal("expected RPC failure")
			}
			state, err := d.loadMigrationState(ctx, store)
			if err != nil || state.recorded {
				t.Fatalf("ticket narrowed legacy endpoint: %+v %v", state, err)
			}
			state.phase, state.other = phaseCutover, 2
			if err := d.saveMigrationState(ctx, store, state); err != nil {
				t.Fatal(err)
			}
			drained, err := d.writeGrantsDrained(ctx, store, svc)
			if err != nil || drained != expired {
				t.Fatalf("drained=%v expired=%v err=%v", drained, expired, err)
			}
			cm := &corev1.ConfigMap{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: migrationStateName(store)}, cm); err != nil {
				t.Fatal(err)
			}
			_, pending := cm.Data[pendingWriteGrantPrefix+"GK1"]
			if pending == expired {
				t.Fatalf("ticket pending=%v expired=%v", pending, expired)
			}
		})
	}
}
