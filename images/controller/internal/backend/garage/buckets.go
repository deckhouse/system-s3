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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/backend/s3util"
)

// EnsureBucket creates the named bucket in Garage on behalf of owner
// ("namespace/name" of the SystemBucket), applies quota as the bucket's quota and
// reports what the bucket holds. Idempotent: a bucket this owner already holds is
// reused.
//
// A bucket another SystemBucket holds is not: the call reports not-ready with the
// current owner and touches nothing. See bucket_owners.go for why this is decided
// here and not only in the admission webhook.
//
// Credentials are not issued here — EnsureAccess mints the key pair the
// SystemBucket hands to its consumer.
func (d *Driver) EnsureBucket(ctx context.Context, store *backend.Store, name, owner string, quota int64) (backend.BucketState, backend.BucketUsage, error) {
	store, _, err := d.servingPlane(ctx, store)
	if err != nil {
		return backend.BucketState{}, backend.BucketUsage{}, err
	}

	// The claim comes before anything reaches Garage: nothing about this bucket
	// should be created for an owner that is not going to be given it.
	if other, err := d.claimBucket(ctx, store, name, owner); err != nil {
		return backend.BucketState{}, backend.BucketUsage{}, fmt.Errorf("claim bucket %q for %s: %w", name, owner, err)
	} else if other != "" {
		return backend.BucketState{Message: bucketTakenMessage(name, other)}, backend.BucketUsage{}, nil
	}

	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil {
		return backend.BucketState{}, backend.BucketUsage{}, err
	}
	if !ready {
		return backend.BucketState{Message: "Garage admin token is not provisioned yet"}, backend.BucketUsage{}, nil
	}

	b, found, err := svc.getBucketByAlias(ctx, name)
	if err != nil {
		return backend.BucketState{}, backend.BucketUsage{}, fmt.Errorf("look up bucket %q: %w", name, err)
	}
	if !found {
		created, err := svc.createBucket(ctx, name)
		if err != nil {
			return backend.BucketState{}, backend.BucketUsage{}, fmt.Errorf("create bucket %q: %w", name, err)
		}
		b = created
	}

	// The quota is applied on every pass, not only at creation. It is the one
	// piece of the bucket an operator can change on a live resource, and a quota
	// that only followed spec.quota at creation would mean raising it did nothing
	// until somebody deleted the bucket.
	if quota > 0 && (b.Quotas.MaxSize == nil || *b.Quotas.MaxSize != quota) {
		if err := svc.setBucketQuota(ctx, b.ID, quota); err != nil {
			return backend.BucketState{}, backend.BucketUsage{}, fmt.Errorf("set the quota of bucket %q: %w", name, err)
		}
		// Re-read rather than assume: what the status publishes has to be what the
		// backend holds, and this is the call that just changed it.
		if refreshed, ok, err := svc.getBucketByAlias(ctx, name); err != nil {
			return backend.BucketState{}, backend.BucketUsage{}, fmt.Errorf("read back the quota of bucket %q: %w", name, err)
		} else if ok {
			b = refreshed
		}
	}

	usage := backend.BucketUsage{UsedBytes: b.Bytes, Objects: b.Objects}
	if b.Quotas.MaxSize != nil {
		usage.QuotaBytes = *b.Quotas.MaxSize
	}

	return backend.BucketState{
		Ready:      true,
		Message:    "bucket provisioned",
		BucketName: name,
	}, usage, nil
}

// DeleteBucket removes the named bucket and everything in it. It is idempotent
// and tolerates a data plane that is already gone. Access keys are removed
// separately by DeleteAccess.
//
// There is no reclaim policy to consult: a SystemBucket owns its data outright,
// so deleting the resource deletes the objects.
//
// owner is the SystemBucket being deleted ("namespace/name"). A bucket recorded
// against a different owner is left alone and the call succeeds: the resource
// going away never held that bucket (it was refused credentials for it), so its
// finalizer must be released without destroying the actual owner's data.
func (d *Driver) DeleteBucket(ctx context.Context, store *backend.Store, name, owner string) error {
	store, _, err := d.servingPlane(ctx, store)
	if err != nil {
		return err
	}

	recorded, err := d.bucketOwner(ctx, store, name)
	if err != nil {
		return fmt.Errorf("read the owner of bucket %q: %w", name, err)
	}
	if recorded != "" && recorded != owner {
		d.log.Info("not deleting a bucket owned by another SystemBucket",
			"bucket", name, "owner", recorded, "requestedBy", owner)
		return nil
	}

	if err := d.destroyBucket(ctx, store, name); err != nil {
		return err
	}

	// The claim is released last, and only on success: dropping it while the data
	// is still there would let the next SystemBucket of the same name adopt
	// somebody else's objects.
	return d.releaseBucket(ctx, store, name, owner)
}

// destroyBucket empties and removes the bucket, tolerating a data plane that is
// already gone or not yet up.
func (d *Driver) destroyBucket(ctx context.Context, store *backend.Store, name string) error {
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
	key, err := svc.createKey(ctx, "system-s3-reclaim-"+name)
	if err != nil {
		return fmt.Errorf("create temporary key to empty bucket %q: %w", name, err)
	}
	defer func() {
		// Its own context, not the one being unwound.
		//
		// The common way to reach this defer is the reconcile timeout expiring
		// inside the emptying loop below — and on a cancelled context the revoke
		// does not even get sent. What stays behind is an owner key on a bucket
		// that is being deleted: full read and write, no resource naming it, and
		// nothing that will ever come back for it. Emptying a bucket is exactly
		// when a leftover owner key matters most.
		//
		// WithoutCancel keeps the values (they carry the logger and request
		// metadata) and drops the cancellation; the short deadline is there so a
		// backend that stopped answering cannot hold the worker either.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := svc.deleteKey(cleanupCtx, key.AccessKeyID); err != nil {
			// Logged, not swallowed. It cannot be returned — the function may be
			// returning a more important error — but an owner key nobody revoked is
			// not something to drop on the floor.
			d.log.Error("unable to revoke the temporary key used to empty a bucket",
				logger.Err(err), "bucket", name, "accessKeyID", key.AccessKeyID)
		}
	}()

	if err := svc.allow(ctx, bucketID, key.AccessKeyID, permissions{Read: true, Write: true, Owner: true}); err != nil {
		return fmt.Errorf("grant temporary key on bucket %q: %w", name, err)
	}
	mc, err := s3util.NewClient(planeS3HostPort(store, d.namespace, d.clusterDomain), key.AccessKeyID, key.SecretAccessKey)
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
