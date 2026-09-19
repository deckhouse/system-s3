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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
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
