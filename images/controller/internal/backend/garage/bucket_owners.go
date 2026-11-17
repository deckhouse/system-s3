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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// Bucket ownership: which SystemBucket a Garage bucket belongs to.
//
// Two SystemBuckets resolving to one bucket name must not both get credentials
// for it. They would write to the same storage without knowing about each other,
// and — since a SystemBucket owns its data outright and there is no reclaim
// policy — deleting either one would take the other's objects with it.
//
// Nothing rejects the second resource at admission: the module runs no webhook,
// and one could not have held the invariant anyway — admission is not
// serialised, so two simultaneous CREATEs each see the other missing from the
// List and both pass. The controller is the only place the decision can be made
// once, so the claim lives here, and the second resource is accepted as an
// object and refused in its status.
//
// The record is a ConfigMap in the module namespace: bucket name -> "namespace/
// name" of the owning SystemBucket. A ConfigMap rather than the bucket itself
// because Garage has no place to put an arbitrary tag on a bucket, and
// read-modify-write on it is safe — the controller runs under leader election, so
// there is one writer, and the write is retried on conflict regardless.
// It belongs to the store and not to the data plane running it (see
// stableName): which SystemBucket a bucket was made for is the same answer
// before and after a migration moves the data to another Garage cluster.
func bucketOwnersName(store *backend.Store) string {
	return stableName(store) + "-bucket-owners"
}

// claimBucket records owner as the owner of name and returns "". When the bucket
// is already claimed by a different SystemBucket, it changes nothing and returns
// that owner.
func (d *Driver) claimBucket(ctx context.Context, store *backend.Store, name, owner string) (string, error) {
	var conflict string
	err := backend.RetryOnConflict(retry.DefaultRetry, func() error {
		conflict = ""

		cm := &corev1.ConfigMap{}
		key := client.ObjectKey{Namespace: d.namespace, Name: bucketOwnersName(store)}
		err := d.apiReader.Get(ctx, key, cm)
		switch {
		case apierrors.IsNotFound(err):
			// First claim in a fresh installation.
			return d.client.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      key.Name,
					Namespace: key.Namespace,
					Labels:    commonLabels(store),
				},
				Data: map[string]string{name: owner},
			})
		case err != nil:
			return err
		}

		if recorded := cm.Data[name]; recorded != "" {
			if recorded != owner {
				conflict = recorded
			}
			return nil
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[name] = owner
		return d.client.Update(ctx, cm)
	})
	if err != nil {
		// A Create racing another Create is the same situation as a conflicting
		// Update: re-read on the next reconcile rather than reporting an error.
		if apierrors.IsAlreadyExists(err) {
			return "", nil
		}
		return "", err
	}
	return conflict, nil
}

// bucketOwner returns the SystemBucket recorded as the owner of name, or "" when
// nothing is recorded (including a store that has never claimed anything).
func (d *Driver) bucketOwner(ctx context.Context, store *backend.Store, name string) (string, error) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: d.namespace, Name: bucketOwnersName(store)}
	if err := d.apiReader.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return cm.Data[name], nil
}

// releaseBucket drops the claim on name, but only if owner still holds it: a
// SystemBucket that never owned the bucket must not release somebody else's
// claim on its way out.
func (d *Driver) releaseBucket(ctx context.Context, store *backend.Store, name, owner string) error {
	return backend.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		key := client.ObjectKey{Namespace: d.namespace, Name: bucketOwnersName(store)}
		if err := d.apiReader.Get(ctx, key, cm); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if cm.Data[name] != owner {
			return nil
		}
		delete(cm.Data, name)
		return d.client.Update(ctx, cm)
	})
}

// bucketTakenMessage is what a SystemBucket is told when the bucket name it
// resolves to belongs to another one.
func bucketTakenMessage(name, owner string) string {
	return fmt.Sprintf(
		"bucket %q already belongs to SystemBucket %s: refusing to issue credentials for it, because both owners would write to the same storage and deleting either would destroy the other's objects. Give this SystemBucket a different spec.bucketName",
		name, owner)
}
