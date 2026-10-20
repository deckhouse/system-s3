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

package tlsproxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/deckhouse/sds-common-lib/logger"
)

// fakeGarage answers /health and GetNodeStatistics the way Garage v2.4 does,
// with item counts the test changes as it goes.
type fakeGarage struct {
	mu     sync.Mutex
	health int
	self   string
	items  map[string]map[string]uint64
	token  string
}

func (g *fakeGarage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.URL.Path == "/health" {
		w.WriteHeader(g.health)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+g.token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	type table struct {
		TableName string `json:"tableName"`
		Items     uint64 `json:"items"`
	}
	type stats struct {
		TableStats []table `json:"tableStats"`
	}
	success := map[string]stats{}
	for id, tables := range g.items {
		if r.URL.Query().Get("node") == "self" && id != g.self {
			continue
		}
		var s stats
		for name, n := range tables {
			s.TableStats = append(s.TableStats, table{TableName: name, Items: n})
		}
		success[id] = s
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"success": success, "error": map[string]string{}})
}

func (g *fakeGarage) set(node, table string, n uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.items[node][table] = n
}

func testLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.InfoLevel)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

func probe(t *testing.T, r *Readiness) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ReadyPath, nil))
	return rec.Code, rec.Body.String()
}

// TestANodeOnAnEmptyVolumeWaitsForItsTables is the case that sent S3 requests to
// a replica that answered NoSuchBucket: healthy, but with empty tables.
func TestANodeOnAnEmptyVolumeWaitsForItsTables(t *testing.T) {
	full := map[string]uint64{"bucket_v2": 5, "bucket_alias": 5, "key": 40}
	g := &fakeGarage{
		health: http.StatusOK, self: "aaaa", token: "secret",
		items: map[string]map[string]uint64{
			"aaaa": {"bucket_v2": 0, "bucket_alias": 0, "key": 0},
			"bbbb": full,
			"cccc": full,
		},
	}
	srv := httptest.NewServer(g)
	defer srv.Close()
	r := &Readiness{Backend: strings.TrimPrefix(srv.URL, "http://"), Token: "secret", Log: testLogger(t)}

	if code, body := probe(t, r); code != http.StatusServiceUnavailable || !strings.Contains(body, "bucket_v2") {
		t.Fatalf("an empty node is ready: %d %s", code, body)
	}

	g.set("aaaa", "bucket_v2", 5)
	g.set("aaaa", "bucket_alias", 5)
	g.set("aaaa", "key", 39)
	if code, body := probe(t, r); code != http.StatusServiceUnavailable || !strings.Contains(body, "table key") {
		t.Fatalf("a node one key short is ready: %d %s", code, body)
	}

	g.set("aaaa", "key", 40)
	if code, body := probe(t, r); code != http.StatusOK {
		t.Fatalf("a caught-up node is not ready: %d %s", code, body)
	}

	// The key table moves with every temporary key; a node that served stays
	// in the Service over a key issued on its peers a moment ago.
	g.set("bbbb", "key", 41)
	if code, body := probe(t, r); code != http.StatusOK {
		t.Fatalf("a node that caught up dropped out over one new key: %d %s", code, body)
	}

	// /health is still asked every time.
	g.mu.Lock()
	g.health = http.StatusServiceUnavailable
	g.mu.Unlock()
	if code, _ := probe(t, r); code != http.StatusServiceUnavailable {
		t.Fatalf("an unhealthy node is ready: %d", code)
	}
}

func TestASingleNodeIsReadyOnHealthAlone(t *testing.T) {
	g := &fakeGarage{
		health: http.StatusOK, self: "aaaa", token: "secret",
		items: map[string]map[string]uint64{"aaaa": {"bucket_v2": 0, "bucket_alias": 0, "key": 0}},
	}
	srv := httptest.NewServer(g)
	defer srv.Close()
	r := &Readiness{Backend: strings.TrimPrefix(srv.URL, "http://"), Token: "secret", Log: testLogger(t)}
	if code, body := probe(t, r); code != http.StatusOK {
		t.Fatalf("a single node has no peer to wait for: %d %s", code, body)
	}
}

func TestAFreshClusterIsReady(t *testing.T) {
	empty := func() map[string]uint64 { return map[string]uint64{"bucket_v2": 0, "bucket_alias": 0, "key": 0} }
	g := &fakeGarage{
		health: http.StatusOK, self: "aaaa", token: "secret",
		items: map[string]map[string]uint64{"aaaa": empty(), "bbbb": empty(), "cccc": empty()},
	}
	srv := httptest.NewServer(g)
	defer srv.Close()
	r := &Readiness{Backend: strings.TrimPrefix(srv.URL, "http://"), Token: "secret", Log: testLogger(t)}
	if code, body := probe(t, r); code != http.StatusOK {
		t.Fatalf("a fresh cluster, every node empty, must come up: %d %s", code, body)
	}
}
