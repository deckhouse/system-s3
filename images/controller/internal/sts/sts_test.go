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

package sts

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-common-lib/logger"
	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/garage"
)

// fakeAuthorizer answers the two questions the endpoint asks the API server.
type fakeAuthorizer struct {
	user    authnv1.UserInfo
	ok      bool
	allowed map[string]bool // subresource -> allowed
	err     error
}

func (f *fakeAuthorizer) Authenticate(_ context.Context, token string) (authnv1.UserInfo, bool, error) {
	if f.err != nil {
		return authnv1.UserInfo{}, false, f.err
	}
	if token == "" || !f.ok {
		return authnv1.UserInfo{}, false, nil
	}
	return f.user, true, nil
}

func (f *fakeAuthorizer) Authorize(_ context.Context, _ authnv1.UserInfo, _, _, subresource string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.allowed[subresource], nil
}

// fakeIssuer records what the endpoint asked the backend for.
type fakeIssuer struct {
	calls     int
	write     bool
	bucket    string
	subject   string
	expiresAt time.Time
	err       error
}

func (f *fakeIssuer) IssueTemporaryAccess(
	_ context.Context, _ *backend.Store, bucketName, subject string, write bool, expiresAt time.Time,
) (garage.TemporaryCredentials, error) {
	f.calls++
	f.bucket, f.subject, f.write, f.expiresAt = bucketName, subject, write, expiresAt
	if f.err != nil {
		return garage.TemporaryCredentials{}, f.err
	}
	return garage.TemporaryCredentials{
		AccessKeyID:     "GK-temp",
		SecretAccessKey: "secret-half",
		ExpiresAt:       expiresAt,
	}, nil
}

func testServer(t *testing.T, auth *fakeAuthorizer, issuer *fakeIssuer, objs ...*v1alpha1.SystemBucket) *Server {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(s)
	for _, o := range objs {
		builder = builder.WithObjects(o)
	}
	return &Server{
		Reader:     builder.Build(),
		Issuer:     issuer,
		Authorizer: auth,
		Store:      &backend.Store{Name: "system"},
		Log:        logger.NewNop(),
		Now:        func() time.Time { return time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC) },
	}
}

// readyBucket is the SystemBucket the specs ask for, already provisioned. Only
// the backend bucket name varies between them — everything else about the
// resource is the same, and parameters that never vary read like they matter.
const (
	testBucketNamespace = "d8-monitoring"
	testBucketName      = "loki"
)

func readyBucket(backendName string) *v1alpha1.SystemBucket {
	return &v1alpha1.SystemBucket{
		ObjectMeta: metav1.ObjectMeta{Namespace: testBucketNamespace, Name: testBucketName},
		Status:     &v1alpha1.SystemBucketStatus{BucketName: backendName},
	}
}

func post(t *testing.T, srv *Server, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// The permission decides what the credentials can do, and nothing else does: a
// caller allowed to read gets a read-only key, and a caller allowed to write gets
// one that can write. Getting this backwards would be invisible until something
// wrote to a bucket it was only supposed to read.
func TestPermissionsFollowTheSubjectAccessReview(t *testing.T) {
	cases := []struct {
		name      string
		allowed   map[string]bool
		wantCode  int
		wantWrite bool
	}{
		{
			name:      "allowed to write: a key that can write",
			allowed:   map[string]bool{subresourceWrite: true},
			wantCode:  http.StatusOK,
			wantWrite: true,
		},
		{
			name:      "allowed to read only: a read-only key",
			allowed:   map[string]bool{subresourceRead: true},
			wantCode:  http.StatusOK,
			wantWrite: false,
		},
		{
			name:     "allowed nothing: refused, and no key is minted",
			allowed:  map[string]bool{},
			wantCode: http.StatusForbidden,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth := &fakeAuthorizer{
				user:    authnv1.UserInfo{Username: "system:serviceaccount:d8-monitoring:loki"},
				ok:      true,
				allowed: tc.allowed,
			}
			issuer := &fakeIssuer{}
			srv := testServer(t, auth, issuer, readyBucket("d8-monitoring-loki"))

			rec := post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"a-token"}})

			if rec.Code != tc.wantCode {
				t.Fatalf("status=%d, want %d (body %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantCode != http.StatusOK {
				if issuer.calls != 0 {
					t.Errorf("a key was minted for a caller that was refused")
				}
				return
			}
			if issuer.calls != 1 {
				t.Fatalf("issuer called %d times, want 1", issuer.calls)
			}
			if issuer.write != tc.wantWrite {
				t.Errorf("write=%v, want %v", issuer.write, tc.wantWrite)
			}
			if issuer.bucket != "d8-monitoring-loki" {
				t.Errorf("bucket=%q, want the name from the resource's status", issuer.bucket)
			}
			if issuer.subject != auth.user.Username {
				t.Errorf("subject=%q, want %q: the key's name is the only audit trail it has",
					issuer.subject, auth.user.Username)
			}

			var resp assumeRoleResponse
			if err := xml.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("the response is not the XML an S3 client expects: %v (%s)", err, rec.Body.String())
			}
			if resp.Result.Credentials.AccessKeyID != "GK-temp" ||
				resp.Result.Credentials.SecretAccessKey != "secret-half" {
				t.Errorf("credentials in the response: %+v", resp.Result.Credentials)
			}
			if resp.Result.Credentials.Expiration != "2026-09-16T12:30:00Z" {
				t.Errorf("expiration=%q, want thirty minutes from now in RFC 3339",
					resp.Result.Credentials.Expiration)
			}
		})
	}
}

// A token the API server does not accept gets nothing, and learns nothing about
// the bucket it asked for.
func TestAnUnacceptedTokenIsRefusedWithoutTouchingTheBackend(t *testing.T) {
	issuer := &fakeIssuer{}
	srv := testServer(t, &fakeAuthorizer{ok: false}, issuer, readyBucket("b"))

	rec := post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"stale"}})

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "InvalidIdentityToken") {
		t.Errorf("body does not carry the AWS error code: %s", rec.Body.String())
	}
	if issuer.calls != 0 {
		t.Error("credentials were minted for a token that was not accepted")
	}
}

// A bucket that does not exist, or has no bucket in the backend yet, is a
// request that cannot be served — and the caller is told which, because they
// were authorised for that resource.
func TestABucketThatCannotBeServedIsRefused(t *testing.T) {
	auth := &fakeAuthorizer{
		user:    authnv1.UserInfo{Username: "system:serviceaccount:d8-monitoring:loki"},
		ok:      true,
		allowed: map[string]bool{subresourceWrite: true},
	}

	t.Run("no such SystemBucket", func(t *testing.T) {
		issuer := &fakeIssuer{}
		srv := testServer(t, auth, issuer)
		rec := post(t, srv, "/d8-monitoring/absent", url.Values{"WebIdentityToken": {"t"}})
		if rec.Code != http.StatusBadRequest || issuer.calls != 0 {
			t.Fatalf("status=%d calls=%d, want 400 and no key", rec.Code, issuer.calls)
		}
	})

	t.Run("not provisioned yet", func(t *testing.T) {
		issuer := &fakeIssuer{}
		srv := testServer(t, auth, issuer, &v1alpha1.SystemBucket{
			ObjectMeta: metav1.ObjectMeta{Namespace: "d8-monitoring", Name: "loki"},
		})
		rec := post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"t"}})
		if rec.Code != http.StatusBadRequest || issuer.calls != 0 {
			t.Fatalf("status=%d calls=%d, want 400 and no key", rec.Code, issuer.calls)
		}
	})
}

// The bucket may be named in the path or in a RoleArn, because some clients
// insist on sending one and the path is what a Kubernetes consumer already knows.
func TestTheBucketCanBeNamedInThePathOrInARoleArn(t *testing.T) {
	auth := &fakeAuthorizer{
		user:    authnv1.UserInfo{Username: "system:serviceaccount:d8-monitoring:loki"},
		ok:      true,
		allowed: map[string]bool{subresourceWrite: true},
	}

	t.Run("RoleArn", func(t *testing.T) {
		issuer := &fakeIssuer{}
		srv := testServer(t, auth, issuer, readyBucket("d8-monitoring-loki"))
		rec := post(t, srv, "/", url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"WebIdentityToken": {"t"},
			"RoleArn":          {RoleARNPrefix + "d8-monitoring/loki"},
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200 (%s)", rec.Code, rec.Body.String())
		}
		if issuer.bucket != "d8-monitoring-loki" {
			t.Errorf("bucket=%q", issuer.bucket)
		}
	})

	t.Run("neither", func(t *testing.T) {
		issuer := &fakeIssuer{}
		srv := testServer(t, auth, issuer, readyBucket("d8-monitoring-loki"))
		rec := post(t, srv, "/", url.Values{"WebIdentityToken": {"t"}})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status=%d, want 400", rec.Code)
		}
	})
}

// DurationSeconds is clamped rather than refused: the SDKs pick a duration of
// their own, and failing a request over a number the caller never chose is how an
// integration becomes "works with these three clients".
func TestRequestedDurationIsClamped(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want time.Duration
	}{
		"unset":     {"", DefaultDuration},
		"in range":  {"1800", 30 * time.Minute},
		"too short": {"60", MinDuration},
		"too long":  {"86400", MaxDuration},
		"SDK hour":  {"3600", 30 * time.Minute},
		"overflow":  {"9223372036854775807", 30 * time.Minute},
	}
	for name, tc := range cases {
		got, err := requestedDuration(tc.raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
	if _, err := requestedDuration("soon"); err == nil {
		t.Error("a non-numeric DurationSeconds was accepted")
	}
}

// Only POST, and only this action: a GET that returned credentials would put
// them in every proxy log between the caller and here.
func TestOnlyPostIsAccepted(t *testing.T) {
	srv := testServer(t, &fakeAuthorizer{ok: true}, &fakeIssuer{})
	req := httptest.NewRequest(http.MethodGet, "/d8-monitoring/loki?WebIdentityToken=t", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d, want 405", rec.Code)
	}
}

// A store that will take the request again shortly answers 503 with
// Retry-After, which an SDK retries on its own; anything else stays a 500.
func TestATemporaryRefusalIsServiceUnavailableWithRetryAfter(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantCode       int
		wantRetryAfter string
	}{
		{
			name:           "the backend will be back: 503 and when to ask again",
			err:            backend.Unavailable(20*time.Second, errors.New("look up bucket: context deadline exceeded")),
			wantCode:       http.StatusServiceUnavailable,
			wantRetryAfter: "20",
		},
		{
			name:     "a fault: 500, no hint",
			err:      errors.New("garage admin GET /v2/GetBucketInfo: status 400"),
			wantCode: http.StatusInternalServerError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth := &fakeAuthorizer{
				user:    authnv1.UserInfo{Username: "system:serviceaccount:d8-monitoring:loki"},
				ok:      true,
				allowed: map[string]bool{subresourceWrite: true},
			}
			srv := testServer(t, auth, &fakeIssuer{err: tc.err}, readyBucket("d8-monitoring-loki"))

			rec := post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"a-token"}})

			if rec.Code != tc.wantCode {
				t.Fatalf("status=%d, want %d (body %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			if got := rec.Header().Get("Retry-After"); got != tc.wantRetryAfter {
				t.Errorf("Retry-After=%q, want %q", got, tc.wantRetryAfter)
			}
		})
	}
}

// A bucket at its limit of live temporary keys is the caller's doing: the
// answer is 400 LimitExceeded, which an SDK does not retry, and it is counted
// as such — not as InternalFailure, which reads as a fault of the store.
func TestTheKeyLimitIsLimitExceeded(t *testing.T) {
	auth := &fakeAuthorizer{
		user:    authnv1.UserInfo{Username: "system:serviceaccount:d8-monitoring:loki"},
		ok:      true,
		allowed: map[string]bool{subresourceWrite: true},
	}
	err := fmt.Errorf("%w: bucket \"d8-monitoring-loki\" already has 50", backend.ErrLimitExceeded)
	srv := testServer(t, auth, &fakeIssuer{err: err}, readyBucket("d8-monitoring-loki"))
	before := stsCount(t, "d8-monitoring", "loki", "LimitExceeded")

	rec := post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"a-token"}})

	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "<Code>LimitExceeded</Code>") {
		t.Fatalf("status=%d body=%s, want 400 LimitExceeded", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Errorf("Retry-After=%q: the limit does not pass by waiting a fixed time", rec.Header().Get("Retry-After"))
	}
	if got := stsCount(t, "d8-monitoring", "loki", "LimitExceeded") - before; got != 1 {
		t.Errorf("LimitExceeded counted %v times, want 1", got)
	}
}

// stsCount reads one series of system_s3_sts_requests_total, 0 when it does not
// exist.
func stsCount(t *testing.T, namespace, name, result string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "system_s3_sts_requests_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["namespace"] == namespace && labels["name"] == name && labels["result"] == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// A request is counted against its SystemBucket only once the caller's token
// was accepted and the bucket exists. A path is whatever the caller typed, and
// counting by it would let anybody who reaches the endpoint add series; a
// refusal of an authenticated caller for a bucket that exists is exactly the
// one an operator wants to find by name.
func TestRequestsAreCountedAgainstABucketOnlyWhenItExists(t *testing.T) {
	auth := &fakeAuthorizer{
		user:    authnv1.UserInfo{Username: "system:serviceaccount:d8-monitoring:loki"},
		ok:      true,
		allowed: map[string]bool{subresourceWrite: true},
	}
	srv := testServer(t, auth, &fakeIssuer{}, readyBucket("d8-monitoring-loki"))

	granted := stsCount(t, testBucketNamespace, testBucketName, "Granted")
	if rec := post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"t"}}); rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if got := stsCount(t, testBucketNamespace, testBucketName, "Granted"); got != granted+1 {
		t.Errorf("Granted for the bucket = %v, want %v", got, granted+1)
	}

	denied := stsCount(t, testBucketNamespace, testBucketName, "AccessDenied")
	auth.allowed = map[string]bool{}
	post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"t"}})
	if got := stsCount(t, testBucketNamespace, testBucketName, "AccessDenied"); got != denied+1 {
		t.Errorf("AccessDenied for the bucket = %v, want %v", got, denied+1)
	}

	auth.allowed = map[string]bool{subresourceWrite: true}
	unknown := stsCount(t, "", "", "InvalidRequest")
	post(t, srv, "/d8-monitoring/absent", url.Values{"WebIdentityToken": {"t"}})
	if got := stsCount(t, "", "", "InvalidRequest"); got != unknown+1 {
		t.Errorf("InvalidRequest for a bucket that does not exist = %v, want %v under empty labels", got, unknown+1)
	}
	if got := stsCount(t, "d8-monitoring", "absent", "InvalidRequest"); got != 0 {
		t.Errorf("a bucket that does not exist became a label: %v", got)
	}

	auth.ok = false
	rejected := stsCount(t, "", "", "InvalidIdentityToken")
	post(t, srv, "/d8-monitoring/loki", url.Values{"WebIdentityToken": {"stale"}})
	if got := stsCount(t, "", "", "InvalidIdentityToken"); got != rejected+1 {
		t.Errorf("a refused token = %v, want %v under empty labels", got, rejected+1)
	}
}
