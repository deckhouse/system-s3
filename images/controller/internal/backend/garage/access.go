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

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// EnsureAccess provisions a Garage access key scoped to the bucket. keyName is
// the backend-facing display name of the key (backend.AccessResourceName) and
// prevKeyID is the key id the SystemBucket already recorded, if any.
//
// mintFresh forces a brand-new key and revokes prevKeyID — the reconciler passes
// it on rotation or when the credentials Secret is missing. Otherwise prevKeyID
// is reused, and its secret is NOT returned: Garage cannot recover a secret key
// after creation, so the existing Secret is the only copy.
//
// The key is granted read and write on the bucket. There is no permission knob:
// a system bucket has exactly one consumer, and it is the component that asked
// for the bucket in the first place.
func (d *Driver) EnsureAccess(ctx context.Context, store *backend.Store, bucketName, keyName, prevKeyID string, mintFresh bool) (backend.AccessState, error) {
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil {
		return backend.AccessState{}, err
	}
	if !ready {
		return backend.AccessState{Message: "Garage admin token is not provisioned yet"}, nil
	}

	b, found, err := svc.getBucketByAlias(ctx, bucketName)
	if err != nil {
		return backend.AccessState{}, fmt.Errorf("look up bucket %q: %w", bucketName, err)
	}
	if !found {
		return backend.AccessState{Message: fmt.Sprintf("bucket %q does not exist yet", bucketName)}, nil
	}

	accessKey, secretKey := prevKeyID, ""

	if !mintFresh && accessKey != "" {
		exists, err := svc.keyExists(ctx, accessKey)
		if err != nil {
			return backend.AccessState{}, fmt.Errorf("check access key: %w", err)
		}
		if !exists {
			mintFresh = true // recorded key vanished; re-issue
		}
	}
	minted := false
	if mintFresh || accessKey == "" {
		key, err := svc.createKey(ctx, keyName)
		if err != nil {
			return backend.AccessState{}, fmt.Errorf("create access key: %w", err)
		}
		accessKey, secretKey = key.AccessKeyID, key.SecretAccessKey
		minted = true

		// Collect the keys of rotations past.
		//
		// The resource records ONE superseded key, so two rotations in quick
		// succession overwrite that record and the first replaced key is left in the
		// backend for good: a live read-write credential on a platform bucket that
		// nothing names and nothing will ever revoke. An e2e run found three of them
		// on one bucket after two rotations.
		//
		// Every key this module mints for a bucket carries the same display name
		// (<namespace>.<systembucket>), so the keys under that name are exactly this
		// bucket's history. The one just minted and the one being replaced stay; the
		// rest are ancestors, and this is the moment their number is known to be
		// growing. Failure is logged and not returned: the rotation itself has
		// succeeded, and a leftover key must not cost the consumer its credentials.
		if err := d.collectAncestorKeys(ctx, svc, keyName, accessKey, prevKeyID); err != nil {
			d.log.Error("unable to collect the access keys replaced by earlier rotations",
				logger.Err(err), "keyName", keyName)
		}
	}

	if err := svc.allow(ctx, b.ID, accessKey, permissions{Read: true, Write: true}); err != nil {
		if minted {
			// Roll back the key we just created: its id/secret were not persisted
			// to status, so leaving it would leak an orphan key and the next
			// reconcile would mint yet another one.
			_ = svc.deleteKey(ctx, accessKey)
		}
		return backend.AccessState{}, fmt.Errorf("grant key on bucket: %w", err)
	}

	// The superseded key is NOT revoked here, and that is the point.
	//
	// Garage hands back a key's secret half once, at creation, and never again.
	// Revoking the old key at this moment would put that secret — which so far
	// exists only in this function's return value — on the wrong side of every
	// ordinary interruption: a conflict on the Secret write, the reconcile
	// timeout, an evicted controller Pod. The consumer would be left holding a
	// key the backend no longer knows, while the key that replaced it became
	// unusable to everyone, permanently.
	//
	// So the old key is left alone here. The reconciler can tell a rotation
	// happened — it passed prevKeyID in and gets a different AccessKeyID back
	// together with a non-empty SecretAccessKey — and it revokes the old one on a
	// later pass, once the new secret is in etcd.
	return backend.AccessState{
		Ready:           true,
		Message:         "access key provisioned",
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}, nil
}

// collectAncestorKeys deletes every key with this display name except the one in
// use and the one being retired.
func (d *Driver) collectAncestorKeys(ctx context.Context, svc *adminClient, keyName, inUse, retiring string) error {
	keys, err := svc.listKeys(ctx)
	if err != nil {
		return fmt.Errorf("list access keys: %w", err)
	}
	for _, k := range keys {
		if k.Name != keyName || k.ID == inUse || (retiring != "" && k.ID == retiring) {
			continue
		}
		if err := svc.deleteKey(ctx, k.ID); err != nil {
			return fmt.Errorf("delete the orphaned access key %s: %w", k.ID, err)
		}
		d.log.Info("deleted an access key left by an earlier rotation",
			"keyName", keyName, "accessKeyID", k.ID)
	}
	return nil
}

// DeleteAccess revokes the given access key. Idempotent and tolerant of a data
// plane that is already gone.
func (d *Driver) DeleteAccess(ctx context.Context, store *backend.Store, keyID string) error {
	if keyID == "" {
		return nil
	}
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !ready {
		return nil
	}
	if err := svc.deleteKey(ctx, keyID); err != nil {
		return fmt.Errorf("delete access key: %w", err)
	}
	return nil
}
