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
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

// The bucket allowlist: which buckets the per-bucket metrics may name.
//
// A bucket label taken from the request would let any client that can reach the
// Service mint a time series per request — a made-up bucket name is free to send
// and gets a 404 from Garage only after it has been counted. So the label is only
// ever a bucket the controller knows: the bucket-owner record it keeps in the
// module namespace (bucket name -> "namespace/name" of the SystemBucket), mounted
// into the pod as a directory, one file per bucket. Everything else is counted
// as OtherBucket, and a request that names no bucket at all — ListBuckets on "/"
// — as NoBucket. Neither can collide with a real bucket: S3 bucket names do not
// begin with "_".
//
// The directory is a ConfigMap volume and not a subPath mount, so the kubelet
// replaces its files when the record changes, and the terminator reads it again
// every BucketsRefresh. A bucket is counted as OtherBucket from its first request
// until both have happened, a minute or two at most.
const (
	OtherBucket = "_other"
	NoBucket    = "_none"

	// BucketsRefresh is how often the allowlist is read again.
	BucketsRefresh = 30 * time.Second
)

type bucketOwner struct{ namespace, name string }

// Buckets is the allowlist, read from a directory with one file per bucket that
// holds "namespace/name" of its SystemBucket.
type Buckets struct {
	dir    string
	owners atomic.Pointer[map[string]bucketOwner]
}

// NewBuckets is the allowlist in dir, empty until Load.
func NewBuckets(dir string) *Buckets {
	return &Buckets{dir: dir}
}

// Load reads the directory again. On an error the previous list stays in force.
//
// A directory that does not exist is an empty list: the pod mounts the record
// as optional, because no bucket has been claimed before the first SystemBucket.
func (b *Buckets) Load() error {
	entries, err := os.ReadDir(b.dir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	owners := make(map[string]bucketOwner, len(entries))
	for _, e := range entries {
		// The kubelet's own entries in a ConfigMap volume (..data and the
		// timestamped directory it points to) begin with dots; keys do not.
		if strings.HasPrefix(e.Name(), ".") || e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(b.dir, e.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			// Removed between the listing and the read: the kubelet swapped the
			// volume's contents under us. The next refresh sees the new set.
			continue
		}
		if err != nil {
			return err
		}
		namespace, name, ok := strings.Cut(strings.TrimSpace(string(raw)), "/")
		if !ok || namespace == "" || name == "" {
			continue
		}
		owners[e.Name()] = bucketOwner{namespace: namespace, name: name}
	}
	b.owners.Store(&owners)
	return nil
}

// Run reads the directory every BucketsRefresh until ctx is done.
func (b *Buckets) Run(ctx context.Context, log logger.Logger) {
	t := time.NewTicker(BucketsRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := b.Load(); err != nil {
				log.Warn("unable to read the bucket allowlist; keeping the previous one", logger.Err(err), slog.String("dir", b.dir))
			}
		}
	}
}

// Labels are the bucket, namespace and name labels for a request to bucket.
// A nil allowlist knows no bucket.
func (b *Buckets) Labels(bucket string) (string, string, string) {
	if bucket == "" {
		return NoBucket, "", ""
	}
	if b != nil {
		if owners := b.owners.Load(); owners != nil {
			if o, ok := (*owners)[bucket]; ok {
				return bucket, o.namespace, o.name
			}
		}
	}
	return OtherBucket, "", ""
}
