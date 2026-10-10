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
	"time"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// advanceCutoverOffline must run before ensureWorkload: otherwise that builder
// would restore replicas while the termination barrier is still waiting.
// PVCs, node identities and the layout are retained; no purge or force deletion.
func (d *Driver) advanceCutoverOffline(ctx context.Context, store *backend.Store, state migrationState) (migrationState, string, error) {
	if state.writeFence == writeFenceStopping {
		stopped, err := d.scaleToZero(ctx, store)
		if err != nil {
			return state, "", err
		}
		if !stopped {
			return state, "waiting for all source Garage pods to terminate; reads and writes are unavailable", nil
		}
		state.writeFence = writeFenceRestarting
	}
	if !state.frozenAt.IsZero() && time.Since(state.frozenAt) > cutoverDeadline {
		incoming := d.incomingPlane(store, state)
		for _, pass := range []string{copyPassBulk, copyPassDelta} {
			stopped, err := d.stopCopyPass(ctx, store, incoming, pass)
			if err != nil {
				return state, "", err
			}
			if !stopped {
				return state, "stopping copier pods before restarting the source with writes enabled", nil
			}
		}
		state.phase, state.frozenAt, state.writeFence = phaseCopying, time.Time{}, ""
		return state, "the cutover timed out; restoring the source and returning to bulk copy", d.saveMigrationState(ctx, store, state)
	}
	return state, "all source Garage pods terminated; restarting on the same PVCs with writes still refused", d.saveMigrationState(ctx, store, state)
}
