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

	// Revoke the superseded key after the new one is granted.
	if mintFresh && prevKeyID != "" && prevKeyID != accessKey {
		if err := svc.deleteKey(ctx, prevKeyID); err != nil {
			return backend.AccessState{}, fmt.Errorf("revoke previous access key: %w", err)
		}
	}

	return backend.AccessState{
		Ready:           true,
		Message:         "access key provisioned",
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}, nil
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
