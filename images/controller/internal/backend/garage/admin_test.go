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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIsNotFound(t *testing.T) {
	if !isNotFound(&apiError{StatusCode: http.StatusNotFound}) {
		t.Errorf("isNotFound(404)=false, want true")
	}
	if isNotFound(&apiError{StatusCode: http.StatusInternalServerError}) {
		t.Errorf("isNotFound(500)=true, want false")
	}
	// Garage reports a missing key/bucket as 400 with a "not found" message.
	if !isNotFound(&apiError{StatusCode: http.StatusBadRequest, Body: `{"message":"Bad request: Access key not found: GK123"}`}) {
		t.Errorf("isNotFound(400 not-found)=false, want true")
	}
	// A 400 for any other reason must NOT be treated as not-found.
	if isNotFound(&apiError{StatusCode: http.StatusBadRequest, Body: `{"message":"invalid type: map, expected a sequence"}`}) {
		t.Errorf("isNotFound(400 other)=true, want false")
	}
	if isNotFound(errors.New("plain error")) {
		t.Errorf("isNotFound(plain)=true, want false")
	}
	if isNotFound(nil) {
		t.Errorf("isNotFound(nil)=true, want false")
	}
}

// The keys of a bucket are identified by their display name, so a change to how
// that is parsed out of ListKeys silently stops the collector from collecting.
func TestListKeysDecodesNameAndID(t *testing.T) {
	const body = `[{"id":"GK1","name":"d8-monitoring.loki"},{"id":"GK2","name":"d8-other.backups"}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/ListKeys" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	keys, err := newAdminClient(srv.URL, "token").listKeys(context.Background())
	if err != nil {
		t.Fatalf("listKeys: %v", err)
	}
	if len(keys) != 2 || keys[0].ID != "GK1" || keys[0].Name != "d8-monitoring.loki" {
		t.Fatalf("decoded %+v", keys)
	}
}

// The quota is what makes spec.size mean anything to a writer, and the wire shape
// is the whole of it: a request Garage accepts but reads differently would look
// like a quota that was set and is not enforced.
func TestSetBucketQuotaWireShape(t *testing.T) {
	var (
		gotPath   string
		gotMethod string
		gotBody   map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.RequestURI()
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newAdminClient(srv.URL, "token")
	if err := c.setBucketQuota(context.Background(), "bucket-id-1", 30<<30, 491520); err != nil {
		t.Fatalf("setBucketQuota: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method=%s, want POST", gotMethod)
	}
	if gotPath != "/v2/UpdateBucket?id=bucket-id-1" {
		t.Errorf("path=%s, want /v2/UpdateBucket?id=bucket-id-1", gotPath)
	}
	quotas, ok := gotBody["quotas"].(map[string]any)
	if !ok {
		t.Fatalf("body has no quotas object: %v", gotBody)
	}
	if got := quotas["maxSize"]; got != float64(30<<30) {
		t.Errorf("maxSize=%v, want %v", got, float64(30<<30))
	}
	// Both halves are sent: UpdateBucket replaces the quota object, so a half
	// left out would be a limit removed.
	if got := quotas["maxObjects"]; got != float64(491520) {
		t.Errorf("maxObjects=%v, want 491520", got)
	}
}

// What comes back is what the SystemBucket's status publishes, so the decoding
// is worth pinning: a renamed field would show every bucket as empty with no
// quota, which reads exactly like a bucket that was never used.
func TestBucketInfoDecodesUsageAndQuota(t *testing.T) {
	const body = `{"id":"abc","globalAliases":["loki"],"bytes":12345,"objects":7,` +
		`"quotas":{"maxSize":32212254720,"maxObjects":null}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	c := newAdminClient(srv.URL, "token")
	b, found, err := c.getBucketByAlias(context.Background(), "loki")
	if err != nil || !found {
		t.Fatalf("getBucketByAlias: found=%v err=%v", found, err)
	}
	if b.Bytes != 12345 || b.Objects != 7 {
		t.Errorf("usage=(%d bytes, %d objects), want (12345, 7)", b.Bytes, b.Objects)
	}
	if b.Quotas.MaxSize == nil || *b.Quotas.MaxSize != 32212254720 {
		t.Errorf("maxSize=%v, want 30Gi in bytes", b.Quotas.MaxSize)
	}
	if b.Quotas.MaxObjects != nil {
		t.Errorf("maxObjects=%v, want nil", *b.Quotas.MaxObjects)
	}
}

// The expiry is what closes the rotation window on time even when nothing is
// reconciling, so the moment has to reach Garage in the format it parses — RFC
// 3339, on the key named in the query.
func TestSetKeyExpirationWireShape(t *testing.T) {
	var (
		gotPath string
		gotBody map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RequestURI()
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	at := time.Date(2026, 9, 16, 12, 30, 0, 0, time.FixedZone("MSK", 3*60*60))
	c := newAdminClient(srv.URL, "token")
	if err := c.setKeyExpiration(context.Background(), "GK123", at); err != nil {
		t.Fatalf("setKeyExpiration: %v", err)
	}

	if gotPath != "/v2/UpdateKey?id=GK123" {
		t.Errorf("path=%s, want /v2/UpdateKey?id=GK123", gotPath)
	}
	// UTC, not the local zone it was handed: the same instant either way, but one
	// of the two is readable in a cluster whose nodes disagree about zones.
	if got := gotBody["expiration"]; got != "2026-09-16T09:30:00Z" {
		t.Errorf("expiration=%v, want 2026-09-16T09:30:00Z", got)
	}
}

// A key that is already gone is not an error to expire: the rotation's next pass
// would otherwise retry forever over a key nobody can use anyway.
func TestSetKeyExpirationToleratesAMissingKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newAdminClient(srv.URL, "token")
	if err := c.setKeyExpiration(context.Background(), "GK-gone", time.Now()); err != nil {
		t.Errorf("setKeyExpiration on a missing key: %v, want nil", err)
	}
}

// TestSkipDeadNodesNeverClaimsTheDataIsThere: the call has two halves and only
// one of them is a controller's to make. Advancing the ACK tracker stops the
// cluster waiting for a node that is gone; advancing the SYNC tracker says the
// data that node held has been accounted for, which is an operator's decision to
// accept a loss.
func TestSkipDeadNodesNeverClaimsTheDataIsThere(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/GetClusterLayoutHistory":
			_, _ = w.Write([]byte(`{"currentVersion":7,"minAck":5}`))
		case "/v2/ClusterLayoutSkipDeadNodes":
			_ = json.NewDecoder(r.Body).Decode(&body)
			_, _ = w.Write([]byte(`{"ackUpdated":["dead-node"],"syncUpdated":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := newAdminClient(srv.URL, "token")
	ctx := context.Background()

	hist, err := c.layoutHistory(ctx)
	if err != nil {
		t.Fatalf("layoutHistory: %v", err)
	}
	if hist.CurrentVersion != 7 || hist.MinAck != 5 {
		t.Fatalf("history=%+v, want version 7 acknowledged only up to 5", hist)
	}

	res, err := c.skipDeadNodes(ctx, hist.CurrentVersion, false)
	if err != nil {
		t.Fatalf("skipDeadNodes: %v", err)
	}
	if body["allowMissingData"] != false {
		t.Errorf("allowMissingData=%v: the controller must never declare missing data accounted for", body["allowMissingData"])
	}
	if v, ok := body["version"].(float64); !ok || int64(v) != 7 {
		t.Errorf("version=%v, want the current layout version", body["version"])
	}
	if len(res.AckUpdated) != 1 || res.AckUpdated[0] != "dead-node" {
		t.Errorf("ackUpdated=%v, want the node that was skipped", res.AckUpdated)
	}
}

// TestCleanupIncompleteUploadsWireShape: the call takes an age, and passing the
// wrong unit would either delete uploads that are still running (seconds read as
// hours) or never delete anything.
func TestCleanupIncompleteUploadsWireShape(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/CleanupIncompleteUploads" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"uploadsDeleted":3}`))
	}))
	defer srv.Close()

	got, err := newAdminClient(srv.URL, "token").
		cleanupIncompleteUploads(context.Background(), "bucket-id", 24*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 3 {
		t.Errorf("uploadsDeleted=%d, want 3", got)
	}
	if body["bucketId"] != "bucket-id" {
		t.Errorf("bucketId=%v", body["bucketId"])
	}
	secs, ok := body["olderThanSecs"].(float64)
	if !ok || int64(secs) != 86400 {
		t.Errorf("olderThanSecs=%v, want 86400 — the API takes seconds", body["olderThanSecs"])
	}
}

// TestObjectQuotaFollowsTheSize: 64 KiB per object on average is what the
// volume headroom allows for; the count follows spec.size, and never reaches
// zero, which Garage would read as a bucket that cannot hold anything.
func TestObjectQuotaFollowsTheSize(t *testing.T) {
	for _, tc := range []struct{ size, want int64 }{
		{1 << 30, 16384},
		{30 << 30, 491520},
		{64 << 10, 1},
		{1, 1},
	} {
		if got := objectQuotaFor(tc.size); got != tc.want {
			t.Errorf("objectQuotaFor(%d)=%d, want %d", tc.size, got, tc.want)
		}
	}
}
