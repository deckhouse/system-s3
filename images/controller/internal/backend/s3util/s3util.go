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

// Package s3util holds the two bucket operations the Garage driver has to perform
// over the S3 protocol rather than the admin API, and keeps the minio dependency
// out of the driver.
package s3util

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// NewClient builds a minio S3 client for the given endpoint (host:port, no
// scheme) over plain HTTP.
func NewClient(endpoint, accessKey, secretKey string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
}

// EmptyBucket removes all objects from the bucket (best effort, idempotent). No
// error if the bucket does not exist. Some backends (Garage) refuse to delete a
// non-empty bucket, so callers empty it over S3 before removing it.
func EmptyBucket(ctx context.Context, mc *minio.Client, name string) error {
	exists, err := mc.BucketExists(ctx, name)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", name, err)
	}
	if !exists {
		return nil
	}

	objects := mc.ListObjects(ctx, name, minio.ListObjectsOptions{Recursive: true})
	for rerr := range mc.RemoveObjects(ctx, name, objects, minio.RemoveObjectsOptions{}) {
		if rerr.Err != nil {
			return fmt.Errorf("empty bucket %q: %w", name, rerr.Err)
		}
	}
	return nil
}
