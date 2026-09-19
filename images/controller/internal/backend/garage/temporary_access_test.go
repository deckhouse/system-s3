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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestLiveTemporaryKeysCountsOnlyThisBucketsUnexpiredOnes: the limit exists to
// stop a client that asks faster than its credentials expire, so what it counts
// has to be exactly that — not the consumer's long-lived key, and not the dead
// records the sweeper has not got to yet.
func TestLiveTemporaryKeysCountsOnlyThisBucketsUnexpiredOnes(t *testing.T) {
	bucket := &bucketInfo{
		ID:            "b1",
		GlobalAliases: []string{"loki"},
		Keys: []bucketKeyPerm{
			{AccessKeyID: "GK1", Name: "sts-loki-d8-monitoring.agent"},
			{AccessKeyID: "GK2", Name: "sts-loki-d8-monitoring.other"},
			{AccessKeyID: "GK3", Name: "sts-loki-d8-monitoring.gone"},
			{AccessKeyID: "GK5", Name: "d8-monitoring.loki"}, // the consumer's own, long-lived
		},
	}
	keys := []map[string]any{
		{"id": "GK1", "name": "sts-loki-d8-monitoring.agent", "expired": false},
		{"id": "GK2", "name": "sts-loki-d8-monitoring.other", "expired": false},
		{"id": "GK3", "name": "sts-loki-d8-monitoring.gone", "expired": true},
		{"id": "GK4", "name": "sts-registry-d8-system.puller", "expired": false},
		{"id": "GK5", "name": "d8-monitoring.loki", "expired": false},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/ListKeys" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(keys)
	}))
	defer srv.Close()

	d := &Driver{}
	got, err := d.liveTemporaryKeys(context.Background(), newAdminClient(srv.URL, "token"), bucket)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 2 {
		t.Errorf("live=%d, want 2: this bucket's unexpired temporary keys and nothing else", got)
	}
}

// TestLiveTemporaryKeysDoesNotConfuseBucketsWithASharedPrefix: "loki" and
// "loki-staging" are two buckets, and the name of a temporary key is
// "sts-<bucket>-<subject>" where both halves can contain a hyphen. Counting by
// name prefix charges one bucket's keys to the other and refuses credentials to a
// consumer that has asked for none — which is why the count comes from the
// bucket's own key list.
func TestLiveTemporaryKeysDoesNotConfuseBucketsWithASharedPrefix(t *testing.T) {
	loki := &bucketInfo{ID: "b1", GlobalAliases: []string{"loki"}}
	keys := []map[string]any{
		{"id": "GK1", "name": "sts-loki-staging-ns.sa", "expired": false},
		{"id": "GK2", "name": "sts-loki-staging-ns.other", "expired": false},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(keys)
	}))
	defer srv.Close()

	d := &Driver{}
	got, err := d.liveTemporaryKeys(context.Background(), newAdminClient(srv.URL, "token"), loki)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Errorf("live=%d for bucket %q, want 0: those keys belong to %q", got, "loki", "loki-staging")
	}
}

// TestTemporaryKeyNameCarriesBucketAndSubject pins the shape the count and the
// sweeper both read.
func TestTemporaryKeyNameCarriesBucketAndSubject(t *testing.T) {
	name := temporaryKeyName("d8-monitoring-loki", "system:serviceaccount:d8-monitoring:agent")
	want := fmt.Sprintf("%sd8-monitoring-loki-d8-monitoring.agent", temporaryKeyPrefix)
	if name != want {
		t.Errorf("temporaryKeyName=%q, want %q", name, want)
	}
}
