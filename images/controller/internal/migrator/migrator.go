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

// Package migrator copies the contents of one Garage cluster's buckets into
// another's.
//
// It runs as a Job rather than inside the controller's reconcile loop, and it is
// the same binary started with a different argument: copying a terabyte fits in
// neither the reconcile budget nor the controller's memory, but it needs exactly
// the S3 client the controller already carries. The controller creates the Job,
// watches it, and moves the migration phase on when it finishes (see
// garage.reconcileMigration).
//
// The copy is restartable by construction: it compares each object with its
// counterpart at the destination and copies the ones that are missing or stale.
// A Job that is killed halfway and rerun picks up where it left off without
// keeping a cursor, and the delta pass at cutover is not a different mode — it is
// the same pass, run again over a source that has stopped changing.
package migrator

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/s3util"
)

// Command is the argument that turns the controller binary into the copier.
const Command = "copy-objects"

// Environment variables the Job is configured with. They are the whole
// interface: the copier talks to no Kubernetes API, holds no ServiceAccount
// token, and cannot touch anything but the two S3 endpoints it is given.
const (
	EnvSourceEndpoint  = "MIGRATE_SOURCE_ENDPOINT"
	EnvSourceAccessKey = "MIGRATE_SOURCE_ACCESS_KEY"
	EnvSourceSecretKey = "MIGRATE_SOURCE_SECRET_KEY"
	EnvTargetEndpoint  = "MIGRATE_TARGET_ENDPOINT"
	EnvTargetAccessKey = "MIGRATE_TARGET_ACCESS_KEY"
	EnvTargetSecretKey = "MIGRATE_TARGET_SECRET_KEY"
	EnvBuckets         = "MIGRATE_BUCKETS"
	EnvWorkers         = "MIGRATE_WORKERS"
)

// defaultWorkers is how many objects of one bucket are copied at a time.
//
// The copy is bounded by the network and by Garage's own write path, not by this
// process, so the number is a compromise rather than a tuning knob: high enough
// that a bucket of many small objects is not copied one round trip at a time, low
// enough that a bucket of large ones does not hold eight multipart buffers in the
// memory of a Job that runs next to the data plane it is writing into.
const defaultWorkers = 4

// Endpoint is one side of the copy.
type Endpoint struct {
	Endpoint  string // host:port, no scheme
	AccessKey string
	SecretKey string
}

// Config is what the Job is started with.
type Config struct {
	Source  Endpoint
	Target  Endpoint
	Buckets []string
	Workers int
}

// ConfigFromEnv reads the Job's configuration, rejecting anything incomplete:
// a copier that started with half its configuration would report success over
// buckets it never looked at.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		Source: Endpoint{
			Endpoint:  os.Getenv(EnvSourceEndpoint),
			AccessKey: os.Getenv(EnvSourceAccessKey),
			SecretKey: os.Getenv(EnvSourceSecretKey),
		},
		Target: Endpoint{
			Endpoint:  os.Getenv(EnvTargetEndpoint),
			AccessKey: os.Getenv(EnvTargetAccessKey),
			SecretKey: os.Getenv(EnvTargetSecretKey),
		},
		Workers: defaultWorkers,
	}
	for _, name := range strings.Split(os.Getenv(EnvBuckets), ",") {
		if name = strings.TrimSpace(name); name != "" {
			cfg.Buckets = append(cfg.Buckets, name)
		}
	}
	if n, err := strconv.Atoi(os.Getenv(EnvWorkers)); err == nil && n > 0 {
		cfg.Workers = n
	}

	missing := []string{}
	for name, value := range map[string]string{
		EnvSourceEndpoint:  cfg.Source.Endpoint,
		EnvSourceAccessKey: cfg.Source.AccessKey,
		EnvSourceSecretKey: cfg.Source.SecretKey,
		EnvTargetEndpoint:  cfg.Target.Endpoint,
		EnvTargetAccessKey: cfg.Target.AccessKey,
		EnvTargetSecretKey: cfg.Target.SecretKey,
	} {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("the copier is missing its configuration: %s", strings.Join(missing, ", "))
	}
	if len(cfg.Buckets) == 0 {
		return Config{}, fmt.Errorf("%s is empty: there is nothing to copy", EnvBuckets)
	}
	return cfg, nil
}

// Result is what one bucket's pass did.
type Result struct {
	Bucket  string
	Copied  int64
	Skipped int64
	Bytes   int64
}

// Run copies every configured bucket and returns the first failure.
//
// A bucket that fails stops the run: the phases above treat a failed Job as "stay
// in Copying and try again", and a run that carried on past a failure would have
// to report success while leaving objects behind — which the verification step
// would then have to catch, one phase too late.
func Run(ctx context.Context, cfg Config, log logger.Logger) error {
	source, err := s3util.NewClient(cfg.Source.Endpoint, cfg.Source.AccessKey, cfg.Source.SecretKey)
	if err != nil {
		return fmt.Errorf("connect to the source cluster: %w", err)
	}
	target, err := s3util.NewClient(cfg.Target.Endpoint, cfg.Target.AccessKey, cfg.Target.SecretKey)
	if err != nil {
		return fmt.Errorf("connect to the target cluster: %w", err)
	}

	for _, bucket := range cfg.Buckets {
		start := time.Now()
		res, err := copyBucket(ctx, source, target, bucket, cfg.Workers)
		if err != nil {
			return fmt.Errorf("copy bucket %q: %w", bucket, err)
		}
		log.Info("bucket copied",
			"bucket", bucket, "copied", res.Copied, "skipped", res.Skipped,
			"bytes", res.Bytes, "seconds", int64(time.Since(start).Seconds()))
	}
	return nil
}

// copyBucket copies one bucket with a fixed number of workers.
func copyBucket(ctx context.Context, source, target *minio.Client, bucket string, workers int) (Result, error) {
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu  sync.Mutex
		res = Result{Bucket: bucket}
	)
	objects := source.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithMetadata: true})

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		failure error
	)
	fail := func(err error) {
		errOnce.Do(func() {
			failure = err
			// Stop the listing and the other workers: the run is over either way,
			// and draining a bucket of a million objects to find that out again
			// serves nobody.
			cancel()
		})
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range objects {
				if obj.Err != nil {
					fail(fmt.Errorf("list: %w", obj.Err))
					return
				}
				copied, err := copyObject(ctx, source, target, bucket, obj)
				if err != nil {
					fail(err)
					return
				}
				mu.Lock()
				if copied {
					res.Copied++
					res.Bytes += obj.Size
				} else {
					res.Skipped++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if failure != nil {
		return res, failure
	}
	return res, nil
}

// copyObject copies one object unless the destination already holds it, and
// reports whether it copied.
//
// What the object IS — its type, its encoding, the metadata a consumer put on it
// — is read from the GET rather than from the listing. It costs nothing (those
// are the response headers of the read the copy needs anyway) and it is the only
// place it can be read: a bucket listing carries an object's size, its ETag and
// its time, and not one x-amz-meta header. Copying from the listing would move
// every byte and quietly drop everything the bytes are about.
func copyObject(ctx context.Context, source, target *minio.Client, bucket string, obj minio.ObjectInfo) (bool, error) {
	dst, statErr := target.StatObject(ctx, bucket, obj.Key, minio.StatObjectOptions{})
	if !NeedsCopy(obj, dst, statErr) {
		return false, nil
	}

	reader, err := source.GetObject(ctx, bucket, obj.Key, minio.GetObjectOptions{})
	if err != nil {
		return false, fmt.Errorf("read %q: %w", obj.Key, err)
	}
	defer func() { _ = reader.Close() }()

	info, err := reader.Stat()
	if err != nil {
		return false, fmt.Errorf("read the metadata of %q: %w", obj.Key, err)
	}

	opts := minio.PutObjectOptions{
		ContentType:        info.ContentType,
		ContentEncoding:    info.ContentEncoding,
		ContentDisposition: info.Metadata.Get("Content-Disposition"),
		CacheControl:       info.Metadata.Get("Cache-Control"),
		UserMetadata:       userMetadata(info),
	}
	if _, err := target.PutObject(ctx, bucket, obj.Key, reader, info.Size, opts); err != nil {
		return false, fmt.Errorf("write %q: %w", obj.Key, err)
	}
	return true, nil
}

// NeedsCopy decides whether one object has to be transferred.
//
// The destination is copied over when it is absent, when it is a different size,
// or when the source has been written since the copy was made. Sizes and times
// rather than checksums, because Garage stores a multipart object under an ETag
// that is not the object's MD5 — comparing ETags would re-copy every large object
// on every pass, including the delta pass at cutover, where re-copying is the one
// thing that costs a consumer its write window.
//
// A source and a destination written within the same second are treated as the
// same object. That is only reachable while the source is still being written,
// which is precisely when the copy is NOT the one being trusted: the pass that
// decides the migration runs with writes already refused, so nothing can be
// written in the second the object is compared.
func NeedsCopy(src, dst minio.ObjectInfo, statErr error) bool {
	if statErr != nil {
		// Anything the destination cannot answer — not found, and equally an error
		// we do not recognise — is copied. The write is idempotent and a needless
		// copy is cheap; a skipped object is silent data loss.
		return true
	}
	if dst.Size != src.Size {
		return true
	}
	if src.ETag != "" && src.ETag == dst.ETag {
		return false
	}
	return dst.LastModified.Before(src.LastModified.Truncate(time.Second))
}

// userMetadata carries the object's user metadata across. minio returns it with
// the x-amz-meta- prefix already stripped on ListObjects with metadata, and
// PutObject adds it back.
func userMetadata(obj minio.ObjectInfo) map[string]string {
	if len(obj.UserMetadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(obj.UserMetadata))
	for k, v := range obj.UserMetadata {
		out[k] = v
	}
	return out
}
