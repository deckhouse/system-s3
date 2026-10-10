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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

const pendingWriteGrantPrefix = "pending-write-grant."

var errWriteGrantPending = errors.New("write grant outcome is not fully acknowledged")

// Tickets contain only key IDs, never credentials. They survive controller
// failover and are written atomically against the phase in the same ConfigMap.
// An ambiguous grant RPC must keep its ticket until Garage expires the key:
// a lease timeout would let a delayed RPC reopen writes behind the freeze.
func (d *Driver) registerWriteGrant(ctx context.Context, store *backend.Store, key string) error {
	return d.updateGrantState(ctx, store, func(cm *corev1.ConfigMap) error {
		phase := migrationPhase(cm.Data[migrationKeyPhase])
		serving, err := strconv.ParseInt(cm.Data[migrationKeyServing], 10, 32)
		closed := phase == phaseCutover || phase == phaseResizeFreeze || phase == phaseResizeStop || phase == phaseResizePurge ||
			(phase.resizing() && cm.Data[migrationKeyFrozenAt] != "")
		if err != nil || serving != int64(store.Incarnation) || !knownPhase(phase) || closed {
			return fmt.Errorf("write credentials are refused while the serving plane is changing")
		}
		cm.Data[pendingWriteGrantPrefix+key] = strconv.FormatInt(int64(store.Incarnation), 10)
		return nil
	})
}

func (d *Driver) completeWriteGrant(ctx context.Context, store *backend.Store, key string) error {
	return d.updateGrantState(ctx, store, func(cm *corev1.ConfigMap) error {
		delete(cm.Data, pendingWriteGrantPrefix+key)
		return nil
	})
}

func (d *Driver) grantTemporaryKey(ctx context.Context, store *backend.Store, svc *adminClient, bucket, key string, write bool) error {
	return d.grantBucketKey(ctx, store, svc, bucket, key, permissions{Read: true, Write: write})
}

func (d *Driver) grantBucketKey(ctx context.Context, store *backend.Store, svc *adminClient, bucket, key string, perms permissions) error {
	if perms.Write {
		if err := d.registerWriteGrant(ctx, store, key); err != nil {
			return err
		}
	}
	if err := svc.allow(ctx, bucket, key, perms); err != nil {
		if perms.Write {
			return fmt.Errorf("%w: %w", errWriteGrantPending, err)
		}
		return err
	}
	if perms.Write {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := d.completeWriteGrant(cleanup, store, key); err != nil {
			return fmt.Errorf("%w: %w", errWriteGrantPending, err)
		}
	}
	return nil
}

func (d *Driver) writeGrantsDrained(ctx context.Context, store *backend.Store, svc *adminClient) (bool, error) {
	cm := &corev1.ConfigMap{}
	if err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: migrationStateName(store)}, cm); err != nil {
		return false, err
	}
	var pending []string
	for name := range cm.Data {
		if strings.HasPrefix(name, pendingWriteGrantPrefix) {
			pending = append(pending, strings.TrimPrefix(name, pendingWriteGrantPrefix))
		}
	}
	if len(pending) == 0 {
		return true, nil
	}
	keys, err := svc.listKeys(ctx)
	if err != nil {
		return false, err
	}
	expired := map[string]bool{}
	for _, key := range keys {
		expired[key.ID] = key.Expired
	}
	for _, key := range pending {
		// Absence in a list is not proof of expiration: metadata may not have
		// propagated to the node answering this request yet.
		if !expired[key] {
			return false, nil
		}
		if err := d.completeWriteGrant(ctx, store, key); err != nil {
			return false, err
		}
	}
	return true, nil
}
