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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/deckhouse/sds-object-system/images/controller/internal/backend"
	"github.com/deckhouse/sds-object-system/images/controller/internal/backend/s3util"
)

// EnsureBucket creates the named bucket in Garage. Idempotent: an existing
// bucket with the same alias is reused.
//
// Credentials are not issued here — EnsureAccess mints the key pair the
// SystemBucket hands to its consumer.
func (d *Driver) EnsureBucket(ctx context.Context, store *backend.Store, name string) (backend.BucketState, error) {
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil {
		return backend.BucketState{}, err
	}
	if !ready {
		return backend.BucketState{Message: "Garage admin token is not provisioned yet"}, nil
	}

	if _, found, err := svc.getBucketByAlias(ctx, name); err != nil {
		return backend.BucketState{}, fmt.Errorf("look up bucket %q: %w", name, err)
	} else if !found {
		if _, err := svc.createBucket(ctx, name); err != nil {
			return backend.BucketState{}, fmt.Errorf("create bucket %q: %w", name, err)
		}
	}

	return backend.BucketState{
		Ready:      true,
		Message:    "bucket provisioned",
		BucketName: name,
	}, nil
}

// DeleteBucket removes the named bucket and everything in it. It is idempotent
// and tolerates a data plane that is already gone. Access keys are removed
// separately by DeleteAccess.
//
// There is no reclaim policy to consult: a SystemBucket owns its data outright,
// so deleting the resource deletes the objects.
func (d *Driver) DeleteBucket(ctx context.Context, store *backend.Store, name string) error {
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil // store secret gone: nothing to clean up
		}
		return err
	}
	if !ready {
		return nil
	}

	b, found, err := svc.getBucketByAlias(ctx, name)
	if err != nil {
		return fmt.Errorf("look up bucket %q: %w", name, err)
	}
	if !found {
		return nil
	}

	// Garage's admin DELETE /v1/bucket refuses a non-empty bucket (409
	// BucketNotEmpty), so empty it over S3 first via a short-lived owner key.
	if err := d.emptyBucket(ctx, store, svc, b.ID, name); err != nil {
		return err
	}

	if err := svc.deleteBucket(ctx, b.ID); err != nil {
		return fmt.Errorf("delete bucket %q: %w", name, err)
	}
	return nil
}

// emptyBucket removes all objects from the bucket using a temporary owner key
// that is revoked afterwards. The bucket has no long-lived credentials of its
// own (those belong to the SystemBucket's key, which may already be gone by the
// time the bucket is reclaimed).
func (d *Driver) emptyBucket(ctx context.Context, store *backend.Store, svc *adminClient, bucketID, name string) error {
	key, err := svc.createKey(ctx, "sds-object-system-reclaim-"+name)
	if err != nil {
		return fmt.Errorf("create temporary key to empty bucket %q: %w", name, err)
	}
	defer func() { _ = svc.deleteKey(ctx, key.AccessKeyID) }()

	if err := svc.allow(ctx, bucketID, key.AccessKeyID, permissions{Read: true, Write: true, Owner: true}); err != nil {
		return fmt.Errorf("grant temporary key on bucket %q: %w", name, err)
	}
	mc, err := s3util.NewClient(s3HostPort(store, d.namespace, d.clusterDomain), key.AccessKeyID, key.SecretAccessKey)
	if err != nil {
		return fmt.Errorf("build S3 client to empty bucket %q: %w", name, err)
	}
	if err := s3util.EmptyBucket(ctx, mc, name); err != nil {
		return fmt.Errorf("empty bucket %q before delete: %w", name, err)
	}
	return nil
}

// adminClientFor resolves the admin token and returns a client. ready is false
// when the admin token has not been provisioned yet.
func (d *Driver) adminClientFor(ctx context.Context, store *backend.Store) (*adminClient, bool, error) {
	token, err := d.adminToken(ctx, store)
	if err != nil {
		return nil, false, err
	}
	if token == "" {
		return nil, false, nil
	}
	return newAdminClient(adminEndpoint(store, d.namespace, d.clusterDomain), token), true, nil
}
