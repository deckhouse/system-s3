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
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
		state.phase, state.frozenAt, state.writeFence, state.fencePods = phaseCopying, time.Time{}, "", nil
		return state, "the cutover timed out; restoring the source and returning to bulk copy", d.saveMigrationState(ctx, store, state)
	}
	return state, "all source Garage pods terminated; restarting on the same PVCs with writes still refused", d.saveMigrationState(ctx, store, state)
}

// rollFencePods is the write fence of a highly available source: the same
// barrier as stopping the whole plane — every request a pod admitted before the
// freeze has ended — reached one pod at a time, so the other two keep a quorum
// and the store keeps serving. Reads are not guaranteed: Garage asks the
// replicas of a block one after another and waits for a peer that vanished until
// it misses four pings 15 s apart, so a read can stall for about a minute while
// the replica it asks first is away.
//
// old holds the UIDs of the pods that ran when writes were frozen. A pod is
// deleted only while every replica is present and ready; the StatefulSet brings
// it back on the same PVC under a new UID. done is true once no pod of the plane
// carries an old UID. The caller checks clusterInSync between the steps.
func (d *Driver) rollFencePods(ctx context.Context, store *backend.Store, old []string) (bool, string, error) {
	pods, msg, err := d.fencePodsSettled(ctx, store)
	if err != nil || msg != "" {
		return false, msg, err
	}
	restarted := 0
	for i := range pods {
		if !slices.Contains(old, string(pods[i].UID)) {
			restarted++
		}
	}
	for i := range pods {
		pod := &pods[i]
		if !slices.Contains(old, string(pod.UID)) {
			continue
		}
		err := d.client.Delete(ctx, pod, client.Preconditions{UID: &pod.UID})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, "", fmt.Errorf("restart source pod %s: %w", pod.Name, err)
		}
		return false, fmt.Sprintf("restarting source pod %s (%d of %d) to finish its in-flight requests",
			pod.Name, restarted+1, len(pods)), nil
	}
	return true, "", nil
}

// fencePodsSettled returns the plane's pods sorted by name when every replica is
// present, ready and not terminating, and otherwise what is being waited for.
func (d *Driver) fencePodsSettled(ctx context.Context, store *backend.Store) ([]corev1.Pod, string, error) {
	pods, err := d.planePods(ctx, store)
	if err != nil {
		return nil, "", err
	}
	want := int(desiredReplicas(store))
	if len(pods) < want {
		return nil, fmt.Sprintf("waiting for the restarted source pod to come back (%d of %d running)", len(pods), want), nil
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	for i := range pods {
		if pods[i].DeletionTimestamp != nil || !podReady(&pods[i]) {
			return nil, fmt.Sprintf("waiting for source pod %s to become ready", pods[i].Name), nil
		}
	}
	return pods, "", nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podUIDs(pods []corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for i := range pods {
		out = append(out, string(pods[i].UID))
	}
	sort.Strings(out)
	return out
}
