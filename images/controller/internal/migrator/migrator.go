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
// keeping a cursor. The final delta pass also prunes destination objects absent
// from the frozen source; the bulk pass must never delete while writers run.
package migrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/s3util"
	"github.com/deckhouse/system-s3/images/controller/internal/tlsclient"
)

// Command is the argument that turns the controller binary into the copier.
const Command = "copy-objects"

// QuotaExitCode is the machine-readable outcome consumed by the controller.
const QuotaExitCode = 75

var ErrQuotaExceeded = errors.New("destination bucket quota exceeded")

func quotaExceeded(err error) bool {
	r := minio.ToErrorResponse(err)
	return r.Code == "QuotaExceeded" || r.Code == "InsufficientStorage" ||
		(r.Code == "AccessDenied" && (r.Message == "Bucket size quota is reached" || r.Message == "Object quota is reached"))
}

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
	EnvFinalSync       = "MIGRATE_FINAL_SYNC"

	// EnvCAFile is the CA both clusters' certificates are verified against, and
	// EnvServerName the name they are verified for. Both clusters present the
	// same certificate — it is issued for every Service in the namespace — so one
	// pair covers both sides of the copy.
	EnvCAFile     = "MIGRATE_CA_FILE"
	EnvServerName = "MIGRATE_TLS_SERVER_NAME"
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
	Source     Endpoint
	Target     Endpoint
	Buckets    []string
	Workers    int
	CAFile     string
	ServerName string
	FinalSync  bool // only against a write-frozen source, before endpoint cutover
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
		Workers:    defaultWorkers,
		CAFile:     os.Getenv(EnvCAFile),
		ServerName: os.Getenv(EnvServerName),
	}
	for _, name := range strings.Split(os.Getenv(EnvBuckets), ",") {
		if name = strings.TrimSpace(name); name != "" {
			cfg.Buckets = append(cfg.Buckets, name)
		}
	}
	if n, err := strconv.Atoi(os.Getenv(EnvWorkers)); err == nil && n > 0 {
		cfg.Workers = n
	}
	if raw := os.Getenv(EnvFinalSync); raw != "" {
		var err error
		cfg.FinalSync, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a boolean: %w", EnvFinalSync, err)
		}
	}

	missing := []string{}
	for name, value := range map[string]string{
		EnvSourceEndpoint:  cfg.Source.Endpoint,
		EnvSourceAccessKey: cfg.Source.AccessKey,
		EnvSourceSecretKey: cfg.Source.SecretKey,
		EnvTargetEndpoint:  cfg.Target.Endpoint,
		EnvTargetAccessKey: cfg.Target.AccessKey,
		EnvTargetSecretKey: cfg.Target.SecretKey,
		EnvCAFile:          cfg.CAFile,
		EnvServerName:      cfg.ServerName,
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
	ca := tlsclient.NewCA(cfg.CAFile)
	source, err := s3util.NewClient(cfg.Source.Endpoint, cfg.Source.AccessKey, cfg.Source.SecretKey, ca, cfg.ServerName)
	if err != nil {
		return fmt.Errorf("connect to the source cluster: %w", err)
	}
	target, err := s3util.NewClient(cfg.Target.Endpoint, cfg.Target.AccessKey, cfg.Target.SecretKey, ca, cfg.ServerName)
	if err != nil {
		return fmt.Errorf("connect to the target cluster: %w", err)
	}

	for _, bucket := range cfg.Buckets {
		start := time.Now()
		if cfg.FinalSync {
			// Free quota occupied by keys deleted since the bulk pass before
			// copying their replacements. Otherwise a full destination can never
			// accept the final copy and the prune is never reached.
			deleted, err := pruneDeletedObjects(ctx, source, target, bucket)
			if err != nil {
				return fmt.Errorf("synchronize deletions in bucket %q: %w", bucket, err)
			}
			log.Info("bucket deletions synchronized", "bucket", bucket, "deleted", deleted)
		}
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
	if err := ctx.Err(); err != nil {
		return res, err
	}
	return res, nil
}

type objectStore interface {
	ListObjects(context.Context, string, minio.ListObjectsOptions) <-chan minio.ObjectInfo
	StatObject(context.Context, string, string, minio.StatObjectOptions) (minio.ObjectInfo, error)
	RemoveObject(context.Context, string, string, minio.RemoveObjectOptions) error
}

// pruneDeletedObjects runs only before the final copy against a frozen source.
// A listing on the destination plus one HEAD per key avoids retaining an entire
// bucket inventory in memory. Only a definitive NoSuchKey authorizes deletion:
// an unavailable source, missing bucket or denied HEAD must fail closed.
func pruneDeletedObjects(ctx context.Context, source, target objectStore, bucket string) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var deleted int64
	for obj := range target.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if obj.Err != nil {
			return deleted, fmt.Errorf("list destination: %w", obj.Err)
		}
		if _, err := source.StatObject(ctx, bucket, obj.Key, minio.StatObjectOptions{}); err != nil {
			response := minio.ToErrorResponse(err)
			if response.Code != "NoSuchKey" || response.StatusCode != 404 {
				return deleted, fmt.Errorf("check source object %q: %w", obj.Key, err)
			}
			if err := target.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
				return deleted, fmt.Errorf("delete destination object %q: %w", obj.Key, err)
			}
			deleted++
		}
	}
	return deleted, ctx.Err()
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
		if quotaExceeded(err) {
			return false, fmt.Errorf("write %q: %w: %w", obj.Key, ErrQuotaExceeded, err)
		}
		return false, fmt.Errorf("write %q: %w", obj.Key, err)
	}
	return true, nil
}

// NeedsCopy decides whether one object has to be transferred.
//
// Copy time does not identify the version read from the source. Ambiguous ETags
// (including different multipart layouts) must be copied again, even if that
// lengthens the bounded final synchronization window.
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
	return true
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
