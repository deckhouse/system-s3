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

package migrator

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/deckhouse/sds-common-lib/logger"
)

func TestNeedsCopy(t *testing.T) {
	written := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	src := func(size int64, etag string, at time.Time) minio.ObjectInfo {
		return minio.ObjectInfo{Key: "k", Size: size, ETag: etag, LastModified: at}
	}

	cases := []struct {
		name    string
		src     minio.ObjectInfo
		dst     minio.ObjectInfo
		statErr error
		want    bool
	}{
		{
			name:    "the destination does not have it",
			src:     src(10, "abc", written),
			statErr: errors.New("the specified key does not exist"),
			want:    true,
		},
		{
			name:    "the destination cannot be read",
			src:     src(10, "abc", written),
			statErr: errors.New("connection reset by peer"),
			want:    true,
		},
		{
			name: "the same object",
			src:  src(10, "abc", written),
			dst:  src(10, "abc", written.Add(time.Minute)),
			want: false,
		},
		{
			name: "a different size",
			src:  src(11, "abc", written),
			dst:  src(10, "abc", written.Add(time.Minute)),
			want: true,
		},
		{
			name: "a multipart object copied after it was written",
			src:  src(1<<30, "abc-8", written),
			dst:  src(1<<30, "def-4", written.Add(2*time.Minute)),
			want: true,
		},
		{
			name: "the source was written again after the copy",
			src:  src(1<<30, "abc-8", written.Add(2*time.Minute)),
			dst:  src(1<<30, "def-4", written),
			want: true,
		},
		{
			name: "the same object, timestamps of different precision",
			src:  src(10, "abc-2", written.Add(700*time.Millisecond)),
			dst:  src(10, "def-1", written),
			want: true,
		},
		{
			name: "same-length overwrite during upload of the old copy",
			src:  src(10, "new", written.Add(time.Second)),
			dst:  src(10, "old", written.Add(time.Minute)),
			want: true,
		},
		{
			name: "missing checksums do not prove equality",
			src:  src(10, "", written),
			dst:  src(10, "", written),
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NeedsCopy(tc.src, tc.dst, tc.statErr); got != tc.want {
				t.Errorf("NeedsCopy=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestConfigFromEnvRefusesHalfAConfiguration: a copier that started without one
// of its endpoints would exit successfully over buckets it never looked at, and
// the phase above it would read that as "copied".
func TestConfigFromEnvRefusesHalfAConfiguration(t *testing.T) {
	full := map[string]string{
		EnvSourceEndpoint:  "system-garage.d8-system-s3.svc:3900",
		EnvSourceAccessKey: "GK1",
		EnvSourceSecretKey: "s1",
		EnvTargetEndpoint:  "system-garage-i2.d8-system-s3.svc:3900",
		EnvTargetAccessKey: "GK2",
		EnvTargetSecretKey: "s2",
		EnvBuckets:         "one, two ,three",
		EnvCAFile:          "/etc/garage-tls/ca.crt",
		EnvServerName:      "system-garage.d8-system-s3.svc",
	}

	t.Run("complete", func(t *testing.T) {
		setEnv(t, full)
		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Buckets) != 3 || cfg.Buckets[1] != "two" {
			t.Errorf("buckets=%v, want the three names trimmed", cfg.Buckets)
		}
		if cfg.Workers != defaultWorkers {
			t.Errorf("workers=%d, want the default %d", cfg.Workers, defaultWorkers)
		}
	})

	for _, raw := range []string{"", "false", "true", "invalid"} {
		t.Run("final sync="+raw, func(t *testing.T) {
			setEnv(t, full)
			t.Setenv(EnvFinalSync, raw)
			cfg, err := ConfigFromEnv()
			if raw == "invalid" {
				if err == nil {
					t.Fatal("invalid final-sync mode accepted")
				}
				return
			}
			if err != nil || cfg.FinalSync != (raw == "true") {
				t.Fatalf("finalSync=%v err=%v", cfg.FinalSync, err)
			}
		})
	}

	for name := range full {
		t.Run("without "+name, func(t *testing.T) {
			partial := map[string]string{}
			for k, v := range full {
				if k != name {
					partial[k] = v
				}
			}
			setEnv(t, partial)
			if _, err := ConfigFromEnv(); err == nil {
				t.Errorf("ConfigFromEnv accepted a configuration with no %s", name)
			}
		})
	}
}

func setEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for _, name := range []string{
		EnvSourceEndpoint, EnvSourceAccessKey, EnvSourceSecretKey,
		EnvTargetEndpoint, EnvTargetAccessKey, EnvTargetSecretKey,
		EnvBuckets, EnvWorkers, EnvCAFile, EnvServerName, EnvFinalSync,
	} {
		t.Setenv(name, values[name])
	}
}

type fakeObjectStore struct {
	objects   []minio.ObjectInfo
	stat      map[string]error
	removeErr error
	removed   []string
}

func (f *fakeObjectStore) ListObjects(_ context.Context, _ string, _ minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, len(f.objects))
	for _, obj := range f.objects {
		ch <- obj
	}
	close(ch)
	return ch
}

func (f *fakeObjectStore) StatObject(_ context.Context, _, key string, _ minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{Key: key}, f.stat[key]
}

func (f *fakeObjectStore) RemoveObject(_ context.Context, _, key string, _ minio.RemoveObjectOptions) error {
	if f.removeErr != nil {
		return f.removeErr
	}
	f.removed = append(f.removed, key)
	return nil
}

func TestFinalSyncPrunesOnlyDefinitivelyDeletedObjects(t *testing.T) {
	source := &fakeObjectStore{stat: map[string]error{"deleted": minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}}}
	target := &fakeObjectStore{objects: []minio.ObjectInfo{{Key: "present"}, {Key: "deleted"}}}
	n, err := pruneDeletedObjects(context.Background(), source, target, "bucket")
	if err != nil || n != 1 || len(target.removed) != 1 || target.removed[0] != "deleted" {
		t.Fatalf("deleted=%d removed=%v err=%v", n, target.removed, err)
	}
}

func TestFinalSyncFailsClosed(t *testing.T) {
	for _, cause := range []error{
		errors.New("connection reset"),
		minio.ErrorResponse{Code: "AccessDenied", StatusCode: 403},
		minio.ErrorResponse{Code: "NoSuchBucket", StatusCode: 404},
		minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 500},
		minio.ErrorResponse{StatusCode: 404},
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			source := &fakeObjectStore{stat: map[string]error{"key": cause}}
			target := &fakeObjectStore{objects: []minio.ObjectInfo{{Key: "key"}}}
			if _, err := pruneDeletedObjects(context.Background(), source, target, "bucket"); err == nil {
				t.Fatal("uncertain source accepted")
			}
			if len(target.removed) != 0 {
				t.Fatal("destination deleted despite uncertain source")
			}
		})
	}
}

func TestFinalSyncReportsListingAndDeletionErrors(t *testing.T) {
	cause := errors.New("backend unavailable")
	for _, target := range []*fakeObjectStore{
		{objects: []minio.ObjectInfo{{Err: cause}}},
		{objects: []minio.ObjectInfo{{Key: "key"}}, removeErr: cause},
	} {
		source := &fakeObjectStore{stat: map[string]error{"key": minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}}}
		if _, err := pruneDeletedObjects(context.Background(), source, target, "bucket"); !errors.Is(err, cause) {
			t.Fatalf("err=%v, want underlying failure", err)
		}
	}
}

func TestFinalSyncCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := &fakeObjectStore{stat: map[string]error{"key": minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}}}
	target := &fakeObjectStore{objects: []minio.ObjectInfo{{Key: "key"}}}
	if _, err := pruneDeletedObjects(ctx, source, target, "bucket"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want cancellation", err)
	}
	if len(target.removed) != 0 {
		t.Fatal("cancelled pass deleted objects")
	}
}

// Exercise Run, not only pruning: copying first used to exhaust the object's
// quota before the stale destination key could be removed.
func TestRunFinalSyncFreesQuotaBeforeCopyingReplacements(t *testing.T) {
	for _, tc := range []struct {
		name              string
		final, sameLength bool
	}{
		{"bulk quota", false, false}, {"final quota", true, false}, {"same-length newer stale destination", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			final := tc.final
			var removed, copied atomic.Bool
			list := func(w http.ResponseWriter, key string) {
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><IsTruncated>false</IsTruncated><Contents><Key>%s</Key><LastModified>2026-10-07T00:00:00Z</LastModified><ETag>22af645d1859cb5ca6da0c484f1f37ea</ETag><Size>3</Size></Contents></ListBucketResult>`, key)
			}
			location := func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Query().Has("location") {
					fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
					return true
				}
				return false
			}
			source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if location(w, r) {
					return
				}
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/bucket/":
					list(w, "new")
				case r.Method == http.MethodHead && r.URL.Path == "/bucket/old":
					w.WriteHeader(http.StatusNotFound)
				case (r.Method == http.MethodGet || r.Method == http.MethodHead) && r.URL.Path == "/bucket/new":
					w.Header().Set("Content-Length", "3")
					w.Header().Set("Last-Modified", "Wed, 07 Oct 2026 00:00:00 GMT")
					w.Header().Set("ETag", `"22af645d1859cb5ca6da0c484f1f37ea"`)
					if r.Method == http.MethodGet {
						fmt.Fprint(w, "new")
					}
				default:
					http.Error(w, "unexpected source request", http.StatusBadRequest)
				}
			}))
			defer source.Close()
			target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if location(w, r) {
					return
				}
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/bucket/":
					if tc.sameLength {
						list(w, "new")
					} else {
						list(w, "old")
					}
				case r.Method == http.MethodHead && r.URL.Path == "/bucket/new":
					if tc.sameLength {
						w.Header().Set("Content-Length", "3")
						w.Header().Set("ETag", `"149603e6c03516362a8da23f624db945"`)
						w.Header().Set("Last-Modified", "Wed, 07 Oct 2026 01:00:00 GMT")
						return
					}
					w.WriteHeader(http.StatusNotFound)
				case r.Method == http.MethodDelete && r.URL.Path == "/bucket/old":
					removed.Store(true)
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodPut && r.URL.Path == "/bucket/new":
					if !removed.Load() && !tc.sameLength {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `<Error><Code>QuotaExceeded</Code><Message>object quota reached</Message></Error>`)
						return
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if !strings.Contains(string(body), "new") || strings.Contains(string(body), "old") {
						http.Error(w, "wrong version copied", http.StatusBadRequest)
						return
					}
					copied.Store(true)
					w.Header().Set("ETag", `"22af645d1859cb5ca6da0c484f1f37ea"`)
				default:
					http.Error(w, "unexpected target request", http.StatusBadRequest)
				}
			}))
			defer target.Close()
			caFile := filepath.Join(t.TempDir(), "ca.pem")
			if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: source.Certificate().Raw}), 0600); err != nil {
				t.Fatal(err)
			}
			log, err := logger.NewLoggerToWriter(io.Discard, logger.ErrorLevel)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = Run(ctx, Config{
				Source:  Endpoint{Endpoint: strings.TrimPrefix(source.URL, "https://"), AccessKey: "source", SecretKey: "source-secret"},
				Target:  Endpoint{Endpoint: strings.TrimPrefix(target.URL, "https://"), AccessKey: "target", SecretKey: "target-secret"},
				Buckets: []string{"bucket"}, Workers: 1, FinalSync: final, CAFile: caFile, ServerName: "example.com",
			}, log)
			if final {
				if err != nil || removed.Load() == tc.sameLength || !copied.Load() {
					t.Fatalf("final pass: removed=%v copied=%v err=%v", removed.Load(), copied.Load(), err)
				}
			} else if !errors.Is(err, ErrQuotaExceeded) || removed.Load() || copied.Load() {
				t.Fatalf("bulk pass pruned or bypassed quota: removed=%v copied=%v err=%v", removed.Load(), copied.Load(), err)
			}
		})
	}
}

func TestQuotaClassificationDoesNotTreatAuthenticationAsQuota(t *testing.T) {
	for _, tc := range []struct {
		code, message string
		want          bool
	}{
		{"QuotaExceeded", "quota", true},
		{"InsufficientStorage", "quota", true},
		{"AccessDenied", "Bucket size quota is reached", true},
		{"AccessDenied", "Object quota is reached", true},
		{"AccessDenied", "Access denied", false},
		{"SignatureDoesNotMatch", "Bucket size quota is reached", false},
		{"InternalError", "Object quota is reached", false},
	} {
		t.Run(tc.code+tc.message, func(t *testing.T) {
			if got := quotaExceeded(minio.ErrorResponse{Code: tc.code, Message: tc.message}); got != tc.want {
				t.Fatalf("quota=%v want %v", got, tc.want)
			}
		})
	}
}
