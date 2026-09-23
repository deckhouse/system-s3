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
	"errors"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
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
			// A multipart object is stored under an ETag that is not its MD5, so the
			// two clusters disagree about it even when the bytes are identical. It is
			// the copy time that settles it, or every large object would be re-copied
			// on the delta pass — in the window where writes are refused.
			name: "a multipart object copied after it was written",
			src:  src(1<<30, "abc-8", written),
			dst:  src(1<<30, "def-4", written.Add(2*time.Minute)),
			want: false,
		},
		{
			name: "the source was written again after the copy",
			src:  src(1<<30, "abc-8", written.Add(2*time.Minute)),
			dst:  src(1<<30, "def-4", written),
			want: true,
		},
		{
			// Sub-second timestamps survive one hop and not the other: the source
			// keeps milliseconds, the destination is stored to the second. Comparing
			// them as they come would copy every object on every pass, forever.
			name: "the same object, timestamps of different precision",
			src:  src(10, "abc-2", written.Add(700*time.Millisecond)),
			dst:  src(10, "def-1", written),
			want: false,
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
		EnvBuckets, EnvWorkers, EnvCAFile, EnvServerName,
	} {
		t.Setenv(name, values[name])
	}
}
