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
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// Garage node identity (ed25519) file layout inside the metadata dir and the
// expected raw key sizes, used to validate a captured identity before persisting
// it.
const (
	nodeKeyPath      = dataMountPath + "/meta/node_key"
	nodeKeyPubPath   = nodeKeyPath + ".pub"
	nodeKeyPrivBytes = 64
	nodeKeyPubBytes  = 32
)

// nodeIdentityDataKey is the identity-Secret key holding the private node_key for
// the given replica ordinal; the public key is that plus ".pub". These match the
// filenames the restore-node-key initContainer reads from the mounted Secret.
func nodeIdentityDataKey(ord int32) string {
	return fmt.Sprintf("node-%d", ord)
}

// podOrdinal parses the StatefulSet ordinal from a pod name (the integer after
// the last "-"), returning false when the suffix is not a number.
func podOrdinal(name string) (int32, bool) {
	_, ord, ok := splitOrdinal(name)
	return ord, ok
}

// podParent is the name of the workload a pod or a claim belongs to: everything
// before the ordinal. It is what tells the objects of two data planes apart when
// one name is a prefix of the other ("system-garage" of "system-garage-i2").
func podParent(name string) (string, bool) {
	parent, _, ok := splitOrdinal(name)
	return parent, ok
}

func splitOrdinal(name string) (string, int32, bool) {
	i := strings.LastIndex(name, "-")
	if i <= 0 || i == len(name)-1 {
		return "", 0, false
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil || n < 0 {
		return "", 0, false
	}
	return name[:i], int32(n), true
}

// ensureNodeIdentities persists each System replica's Garage node identity so a
// recycled replica keeps a stable node ID (see restoreNodeKeyScript). It ensures
// the identity Secret exists, then captures the node_key/node_key.pub of every
// Running replica whose identity is not yet stored. Keys are stable, so an
// already-captured ordinal is never overwritten. Capture is best-effort: a pod
// not answering yet is retried on the next reconcile; only genuine API errors are
// returned.
//
// It returns how many Running replicas still have no stored identity, and the
// count is the point. This mechanism protects data placement, and it used to fail
// silently: a Warning in the log, a successful reconcile, no condition touched,
// and an identity Secret that stays empty forever. The next rebalance then
// recreates the replica's PVC, Garage generates a new node ID because there was
// nothing to restore, and the old ID stays in the layout as a dead node —
// accumulating exactly the churn the mechanism exists to prevent.
//
// A pod that has not answered yet is normal and transient. A count that stays
// above zero is not, so the caller reports it where an operator will see it.
func (d *Driver) ensureNodeIdentities(ctx context.Context, store *backend.Store) (int, error) {
	if d.restConfig == nil {
		return 0, nil // no exec transport (e.g. in tests) — nothing to capture
	}

	secret, err := d.ensureNodeIdentitySecret(ctx, store)
	if err != nil {
		return 0, err
	}

	pods, err := d.runningGaragePods(ctx, store)
	if err != nil {
		return 0, err
	}

	captured := map[string][]byte{}
	uncaptured := 0
	for ord, pod := range pods {
		privKey := nodeIdentityDataKey(ord)
		pubKey := privKey + ".pub"
		if len(secret.Data[privKey]) == nodeKeyPrivBytes && len(secret.Data[pubKey]) == nodeKeyPubBytes {
			continue // already captured; the identity is stable, do not overwrite
		}
		priv, err := d.captureNodeKey(ctx, pod, nodeKeyPath)
		if err != nil {
			// Error, not Warning: a replica whose identity is never captured loses
			// its node ID on the next PVC recycle, and that is a data-placement
			// problem rather than a slow pod.
			d.log.Error("unable to read node_key; this replica would rejoin under a new node ID",
				logger.Err(err), slog.String("pod", pod))
			uncaptured++
			continue
		}
		pub, err := d.captureNodeKey(ctx, pod, nodeKeyPubPath)
		if err != nil {
			d.log.Error("unable to read node_key.pub; this replica would rejoin under a new node ID",
				logger.Err(err), slog.String("pod", pod))
			uncaptured++
			continue
		}
		if len(priv) != nodeKeyPrivBytes || len(pub) != nodeKeyPubBytes {
			d.log.Error("unexpected node identity sizes; refusing to store them",
				"pod", pod,
				"privBytes", len(priv), "wantPrivBytes", nodeKeyPrivBytes,
				"pubBytes", len(pub), "wantPubBytes", nodeKeyPubBytes)
			uncaptured++
			continue
		}
		captured[privKey] = priv
		captured[pubKey] = pub
	}

	if len(captured) == 0 {
		return uncaptured, nil
	}
	if err := d.storeNodeIdentities(ctx, store, captured); err != nil {
		return uncaptured, err
	}
	return uncaptured, nil
}

// ensureNodeIdentitySecret returns the identity Secret, creating an empty
// owned one if it does not exist yet.
func (d *Driver) ensureNodeIdentitySecret(ctx context.Context, store *backend.Store) (*corev1.Secret, error) {
	key := client.ObjectKey{Namespace: d.namespace, Name: nodeIdentitySecretName(store)}
	secret := &corev1.Secret{}
	err := d.client.Get(ctx, key, secret)
	if err == nil {
		return secret, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeIdentitySecretName(store),
			Namespace: d.namespace,
			Labels:    commonLabels(store),
		},
		Type: corev1.SecretTypeOpaque,
	}
	if err := d.client.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	// apiReader, not the cache, for the read-back. The object was created a
	// microsecond ago and the informer has not seen it yet, so the cached read
	// returns NotFound and the whole pass fails on an object that exists. It
	// corrects itself on the next pass, but it puts an error in the log every time
	// a data plane is created — which a migration now does routinely.
	if err := d.apiReader.Get(ctx, key, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// storeNodeIdentities merges the captured node identities into the identity
// Secret, never overwriting a key that is already present (identities are
// stable). Retries on write conflict.
func (d *Driver) storeNodeIdentities(ctx context.Context, store *backend.Store, captured map[string][]byte) error {
	key := client.ObjectKey{Namespace: d.namespace, Name: nodeIdentitySecretName(store)}
	return backend.RetryOnConflict(retry.DefaultBackoff, func() error {
		secret := &corev1.Secret{}
		// apiReader, not the cache. RetryOnConflict re-reads on every attempt, and
		// the whole point of re-reading is to get the version the conflict was
		// about. A cached read hands back the same stale copy each time, so the
		// retry mutates the object that already lost and conflicts again until the
		// informer happens to catch up — the retry becoming a spin rather than a
		// resolution.
		if err := d.apiReader.Get(ctx, key, secret); err != nil {
			return err
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		changed := false
		for k, v := range captured {
			if len(secret.Data[k]) != 0 {
				continue // keep the already-persisted identity
			}
			secret.Data[k] = v
			changed = true
		}
		if !changed {
			return nil
		}
		return d.client.Update(ctx, secret)
	})
}

// captureNodeKey reads a Garage identity file from a Running pod and returns its
// raw bytes. The file is transported base64-encoded (binary-safe over the exec
// stream) and any wrapping whitespace is stripped before decoding.
func (d *Driver) captureNodeKey(ctx context.Context, pod, path string) ([]byte, error) {
	out, err := d.execInPod(ctx, pod, "garage", []string{"base64", path})
	if err != nil {
		return nil, err
	}
	enc := strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r', ' ', '\t':
			return -1
		}
		return r
	}, string(out))
	return base64.StdEncoding.DecodeString(enc)
}
