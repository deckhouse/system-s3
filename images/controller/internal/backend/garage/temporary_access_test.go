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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
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

// TestTemporaryKeysListsTheLiveOnesWithTheirBucket: the list an administrator
// sees is the unexpired temporary keys only, each charged to the bucket that
// granted it — not to a bucket whose name happens to prefix the key name — with
// the subject cut after that bucket's own name and the bucket's own permission.
func TestTemporaryKeysListsTheLiveOnesWithTheirBucket(t *testing.T) {
	buckets := map[string]bucketInfo{
		"b1": {ID: "b1", GlobalAliases: []string{"loki"}, Keys: []bucketKeyPerm{
			{AccessKeyID: "GK1", Name: "sts-loki-d8-monitoring.agent", Permissions: permissions{Read: true, Write: true}},
			{AccessKeyID: "GK3", Name: "sts-loki-d8-monitoring.gone", Permissions: permissions{Read: true, Write: true}},
			{AccessKeyID: "GK5", Name: "d8-monitoring.loki", Permissions: permissions{Read: true, Write: true, Owner: true}},
		}},
		"b2": {ID: "b2", GlobalAliases: []string{"loki-staging"}, Keys: []bucketKeyPerm{
			{AccessKeyID: "GK2", Name: "sts-loki-staging-d8-monitoring.reader", Permissions: permissions{Read: true}},
		}},
	}
	keys := []map[string]any{
		{"id": "GK1", "name": "sts-loki-d8-monitoring.agent", "expired": false, "expiration": "2026-10-13T10:30:00Z"},
		{"id": "GK2", "name": "sts-loki-staging-d8-monitoring.reader", "expired": false, "expiration": "2026-10-13T10:15:00.5Z"},
		{"id": "GK3", "name": "sts-loki-d8-monitoring.gone", "expired": true, "expiration": "2026-10-13T09:00:00Z"},
		{"id": "GK5", "name": "d8-monitoring.loki", "expired": false},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/ListKeys":
			_ = json.NewEncoder(w).Encode(keys)
		case "/v2/ListBuckets":
			_ = json.NewEncoder(w).Encode([]bucketListItem{{ID: "b1"}, {ID: "b2"}})
		case "/v2/GetBucketInfo":
			_ = json.NewEncoder(w).Encode(buckets[r.URL.Query().Get("id")])
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	got, err := temporaryKeysOf(context.Background(), newAdminClient(srv.URL, "token"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []backend.TemporaryKey{
		{Bucket: "loki", AccessKeyID: "GK1", Subject: "d8-monitoring.agent", Write: true, ExpiresAt: time.Date(2026, 10, 13, 10, 30, 0, 0, time.UTC)},
		{Bucket: "loki-staging", AccessKeyID: "GK2", Subject: "d8-monitoring.reader", Write: false, ExpiresAt: time.Date(2026, 10, 13, 10, 15, 0, 5e8, time.UTC)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Bucket != want[i].Bucket || got[i].AccessKeyID != want[i].AccessKeyID || got[i].Subject != want[i].Subject ||
			got[i].Write != want[i].Write || !got[i].ExpiresAt.Equal(want[i].ExpiresAt) {
			t.Errorf("key %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestTemporaryKeysWithoutLiveKeysDoesNotReadBuckets: no live temporary key, no
// bucket reads — the list is published every minute and most minutes it is empty.
func TestTemporaryKeysWithoutLiveKeysDoesNotReadBuckets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/ListKeys" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "GK5", "name": "d8-monitoring.loki", "expired": false}})
	}))
	defer srv.Close()

	got, err := temporaryKeysOf(context.Background(), newAdminClient(srv.URL, "token"))
	if err != nil || len(got) != 0 {
		t.Fatalf("got %+v, %v; want no keys", got, err)
	}
}

// TestUnreachableTellsAPassingFailureFromAFault: a Garage that did not answer in
// time, could not be reached or answered 5xx will be back; a 4xx or anything
// else is a fault, and the endpoint must not tell the consumer to come back for
// it.
func TestUnreachableTellsAPassingFailureFromAFault(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"timed out", fmt.Errorf("look up bucket: %w", context.DeadlineExceeded), true},
		{"transport failure", fmt.Errorf("look up bucket: %w", &url.Error{Op: "Get", URL: "https://garage", Err: errors.New("connection refused")}), true},
		{"garage 503", &apiError{Method: "GET", Path: "/v2/GetBucketInfo", StatusCode: 503}, true},
		{"garage 400", &apiError{Method: "GET", Path: "/v2/GetBucketInfo", StatusCode: 400}, false},
		{"anything else", errors.New("decode: unexpected end of JSON input"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, got := backend.AsUnavailable(unreachable(tc.err))
			if got != tc.want {
				t.Fatalf("unavailable=%v, want %v", got, tc.want)
			}
			if got && u.RetryAfter != retryAfterUnreachable {
				t.Errorf("RetryAfter=%v, want %v", u.RetryAfter, retryAfterUnreachable)
			}
		})
	}
}
