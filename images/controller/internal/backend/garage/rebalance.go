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
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// placementAction is the single migration step the placement reconcile decides
// to take this pass (recycle one replica's PVC), or none.
type placementAction struct {
	act     bool
	ordinal int32
	reason  string
}

// nextPlacementAction decides the one replica to relocate to converge the System
// replicas onto the target topology, given the live control-plane hostnames, the
// node each replica's local PV is pinned to (replicaNode[ordinal]; "" when the
// replica's PVC is unbound — still binding, e.g. during initial bring-up) and
// whether Garage is currently healthy. An "" (settling) replica is never
// migrated; only a replica bound to a wrong or removed node is moved. A replica
// left Pending after its master was removed keeps its PVC bound to the old PV
// (Retain), so it reads back as that removed node — not "" — and is migrated.
//
// Topology rules (the 2-master state is intentionally left untouched — a
// mid-shrink transient we neither spread across nor consolidate from):
//   - control-plane nodes >= replicas: SPREAD — one replica per master. A
//     replica that is co-located (shares its node) or pinned to a node that is
//     no longer a control-plane node is relocated onto an empty master. Gated on
//     Garage being healthy so the other replicas have re-replicated before the
//     next move (re-replication only completes once the StatefulSet is whole).
//   - exactly one control-plane node: CONSOLIDATE — every replica onto that
//     node. Replicas not already pinned there (Pending because their master was
//     removed, or on a wrong node) are relocated; the surviving replica on the
//     target is the data anchor and is never recycled. No health gate: the
//     replicas being moved hold no reachable data (their node is gone), so
//     recycling them only restores quorum.
//   - otherwise (0 or 2 nodes): no action.
//
// It returns at most one action; the caller performs it and requeues, so the
// rebalance advances one replica per reconcile.
func nextPlacementAction(cpNodes []string, replicaNode map[int32]string, replicas int32, healthy bool) placementAction {
	cpSet := make(map[string]struct{}, len(cpNodes))
	for _, n := range cpNodes {
		cpSet[n] = struct{}{}
	}

	switch {
	case int32(len(cpNodes)) >= replicas: // SPREAD
		if !healthy {
			return placementAction{} // wait: let the previous move re-replicate
		}
		perNode := map[string]int{}
		for _, n := range replicaNode {
			if _, ok := cpSet[n]; ok {
				perNode[n]++
			}
		}
		for ord := int32(0); ord < replicas; ord++ {
			n := replicaNode[ord]
			if n == "" {
				continue // PVC still binding — let it settle, do not disturb
			}
			if _, onCP := cpSet[n]; !onCP {
				return placementAction{act: true, ordinal: ord, reason: "spread: replica off a control-plane node"}
			}
			if perNode[n] > 1 {
				return placementAction{act: true, ordinal: ord, reason: "spread: replica co-located on " + n}
			}
		}
		return placementAction{}

	case len(cpNodes) == 1: // CONSOLIDATE
		target := cpNodes[0]
		for ord := int32(0); ord < replicas; ord++ {
			n := replicaNode[ord]
			if n == "" {
				continue // PVC still binding (e.g. initial bring-up) — leave it
			}
			if n != target {
				return placementAction{act: true, ordinal: ord, reason: "consolidate onto " + target}
			}
		}
		return placementAction{}

	default: // 0 or 2 control-plane nodes: ignore
		return placementAction{}
	}
}

// reconcilePlacement performs at most one placement migration per reconcile: it
// reads the live topology, decides the next
// action (nextPlacementAction) and, when one is due, recycles that replica's PVC
// so the StatefulSet re-homes it onto the correct master. It returns a
// human-readable message and whether it acted (the caller then reports "not
// ready" and requeues so the move can settle before the next one).
//
// acted=false with a non-empty message is the third outcome: the same migration
// has been attempted maxPlacementAttempts times and is not being attempted
// again. The store is left as it is — co-located replicas still serve — and the
// caller surfaces the message so an operator sees why the placement is wrong
// instead of watching one replica rebuild itself forever.
func (d *Driver) reconcilePlacement(ctx context.Context, store *backend.Store) (acted bool, msg string, err error) {
	// A single-replica store never migrates. A relocation recycles the replica's
	// PVC and lets it re-replicate from the surviving copies — with one replica
	// there are none, so the move would silently discard the whole store. The
	// replica therefore stays on the master its local PV bound to, and goes
	// Pending (data intact on that node's disk) if the master is removed: bringing
	// it back or recreating the store empty is an operator decision.
	if store.SingleReplica {
		return false, "", nil
	}

	cpNodes, err := d.controlPlaneHostnames(ctx)
	if err != nil {
		return false, "", fmt.Errorf("list control-plane nodes: %w", err)
	}
	replicaNode, err := d.systemReplicaNodes(ctx, store)
	if err != nil {
		return false, "", fmt.Errorf("read replica PV placement: %w", err)
	}

	// The health gate only matters for SPREAD; querying it is skipped otherwise.
	replicas := desiredReplicas(store)
	healthy := false
	if int32(len(cpNodes)) >= replicas {
		healthy = d.garageHealthy(ctx, store)
	}

	action := nextPlacementAction(cpNodes, replicaNode, replicas, healthy)
	if !action.act {
		// Converged (or nothing to do): forget the bookkeeping, so a later,
		// legitimate migration starts from a clean count.
		if err := d.clearPlacementAttempts(ctx, store); err != nil {
			return false, "", fmt.Errorf("clear placement attempts: %w", err)
		}
		return false, "", nil
	}

	// Same migration asked for over and over means the previous ones did not
	// achieve it: the replica keeps coming back to a node it was moved off.
	// Attempting it forever costs a copy of the data each time, so stop and say so.
	target := fmt.Sprintf("%d/%s", action.ordinal, action.reason)
	recorded, attempts, err := d.placementAttempt(ctx, store)
	if err != nil {
		return false, "", fmt.Errorf("read placement attempts: %w", err)
	}
	if recorded != target {
		attempts = 0
	}
	if attempts >= maxPlacementAttempts {
		return false, fmt.Sprintf(
			"placement of replica %d gave up after %d attempts (%s): the replica keeps returning to the same node — check node taints, resource pressure and cluster-wide topology constraints",
			action.ordinal, maxPlacementAttempts, action.reason), nil
	}
	if err := d.recordPlacementAttempt(ctx, store, target, attempts+1); err != nil {
		return false, "", fmt.Errorf("record placement attempt: %w", err)
	}

	if err := d.recycleReplica(ctx, store, action.ordinal); err != nil {
		return false, "", fmt.Errorf("recycle replica %d (%s): %w", action.ordinal, action.reason, err)
	}
	return true, fmt.Sprintf("rebalancing System placement: recycling %s-%d (%s)", resourceName(store), action.ordinal, action.reason), nil
}

// placementAttempt reads the recorded migration target and how many times it has
// been attempted in a row. A store whose ConfigMap is missing the keys (or the
// ConfigMap itself, on first bring-up) has attempted nothing.
func (d *Driver) placementAttempt(ctx context.Context, store *backend.Store) (target string, attempts int, err error) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: d.namespace, Name: configName(store)}
	if err := d.apiReader.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return "", 0, nil
		}
		return "", 0, err
	}
	target = cm.Data[placementAttemptTargetKey]
	// An unparseable count reads as "none": the cap is a safety net, and refusing
	// to place replicas because a hand-edited ConfigMap holds garbage would be
	// worse than starting the count over.
	n, convErr := strconv.Atoi(cm.Data[placementAttemptCountKey])
	if convErr != nil || n < 0 {
		return target, 0, nil
	}
	return target, n, nil
}

// recordPlacementAttempt persists the migration about to be attempted and its
// running count.
func (d *Driver) recordPlacementAttempt(ctx context.Context, store *backend.Store, target string, attempts int) error {
	return d.mutatePlacementAttempt(ctx, store, func(data map[string]string) bool {
		if data[placementAttemptTargetKey] == target &&
			data[placementAttemptCountKey] == strconv.Itoa(attempts) {
			return false
		}
		data[placementAttemptTargetKey] = target
		data[placementAttemptCountKey] = strconv.Itoa(attempts)
		return true
	})
}

// clearPlacementAttempts drops the bookkeeping. Called on every converged pass,
// hence the "nothing to do" short-circuit: this must not write to the ConfigMap
// once every 30 seconds for a store that is simply in place.
func (d *Driver) clearPlacementAttempts(ctx context.Context, store *backend.Store) error {
	return d.mutatePlacementAttempt(ctx, store, func(data map[string]string) bool {
		_, hadTarget := data[placementAttemptTargetKey]
		_, hadCount := data[placementAttemptCountKey]
		if !hadTarget && !hadCount {
			return false
		}
		delete(data, placementAttemptTargetKey)
		delete(data, placementAttemptCountKey)
		return true
	})
}

// mutatePlacementAttempt applies mutate to the garage.toml ConfigMap's data and
// writes it back, retrying on conflict against a fresh read (the same ConfigMap
// is rewritten by the config apply on every pass). mutate reports whether it
// changed anything; when it did not, no write is issued. A missing ConfigMap is
// not an error: the config apply creates it later in the same pass, and the
// bookkeeping is re-derived on the next one.
func (d *Driver) mutatePlacementAttempt(ctx context.Context, store *backend.Store, mutate func(map[string]string) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		key := client.ObjectKey{Namespace: d.namespace, Name: configName(store)}
		if err := d.apiReader.Get(ctx, key, cm); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		if !mutate(cm.Data) {
			return nil
		}
		return d.client.Update(ctx, cm)
	})
}

// systemReplicaNodes maps each replica ordinal to the control-plane node its
// data PVC is bound to (via the bound local PV's node label), or "" when the PVC
// is unbound / the PV is missing (e.g. the replica is Pending because its master
// was removed).
func (d *Driver) systemReplicaNodes(ctx context.Context, store *backend.Store) (map[int32]string, error) {
	replicas := desiredReplicas(store)
	out := make(map[int32]string, replicas)
	for ord := int32(0); ord < replicas; ord++ {
		pvc := &corev1.PersistentVolumeClaim{}
		key := client.ObjectKey{Namespace: d.namespace, Name: fmt.Sprintf("data-%s-%d", resourceName(store), ord)}
		if err := d.apiReader.Get(ctx, key, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				out[ord] = ""
				continue
			}
			return nil, err
		}
		if pvc.DeletionTimestamp != nil {
			// A move is already under way: the PVC is waiting for pvc-protection to
			// let go. Reading it as "still on the wrong node" is what turns one
			// recycle into a loop — nextPlacementAction would ask for the same move,
			// recycleReplica would delete the pod again, and the pod is the consumer
			// keeping the finalizer on. Reported as unplaced instead, which asks for
			// nothing.
			out[ord] = ""
			continue
		}
		if pvc.Spec.VolumeName == "" {
			out[ord] = ""
			continue
		}
		pv := &corev1.PersistentVolume{}
		if err := d.apiReader.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			if apierrors.IsNotFound(err) {
				out[ord] = ""
				continue
			}
			return nil, err
		}
		out[ord] = pv.Labels[labelSystemLocalNode]
	}
	return out, nil
}

// recycleReplica relocates one replica by deleting its pod and PVC: the
// StatefulSet then recreates both, and the fresh PVC (WaitForFirstConsumer)
// binds a pool PV on the master the scheduler picks — an empty master (soft
// anti-affinity) when spreading, or the sole survivor when consolidating. The
// replica boots empty and Garage re-replicates once the cluster is whole again.
// Retain means the old PV is left Released for gcLocalPVs to reap.
func (d *Driver) recycleReplica(ctx context.Context, store *backend.Store, ordinal int32) error {
	name := fmt.Sprintf("%s-%d", resourceName(store), ordinal)
	pvcName := "data-" + name

	// The pod first, the PVC second, and the order is the whole difference.
	//
	// Deleting the PVC while its pod still mounts it only stamps a
	// deletionTimestamp: pvc-protection holds the object as long as a consumer
	// exists. Then the pod goes, and the StatefulSet controller immediately
	// recreates it under the same name with the same claim — so whether this
	// works comes down to a race between pvc-protection dropping the finalizer
	// and the new pod appearing. Lose that race and the new pod references a
	// terminating PVC, stays Pending on "persistentvolumeclaim is being deleted",
	// and by existing it keeps the finalizer from ever being dropped.
	//
	// Removing the pod first means that when the PVC delete lands there is no
	// consumer to hold it, so the finalizer goes immediately and the claim is
	// gone before the StatefulSet has recreated anything.
	pod := &corev1.Pod{}
	pod.Namespace, pod.Name = d.namespace, name
	if err := d.client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod %s: %w", name, err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	pvc.Namespace, pvc.Name = d.namespace, pvcName
	if err := d.client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pvc %s: %w", pvcName, err)
	}
	return nil
}

// garageHealthy reports whether the Garage admin API considers the cluster
// healthy (all storage nodes ok / data re-replicated). Any error (token missing,
// endpoint not answering, degraded) yields false, so a placement move is only
// started from a known-good state.
func (d *Driver) garageHealthy(ctx context.Context, store *backend.Store) bool {
	token, err := d.adminToken(ctx, store)
	if err != nil || token == "" {
		return false
	}
	svc := newAdminClient(adminEndpoint(store, d.namespace, d.clusterDomain), token)
	h, err := svc.health(ctx)
	if err != nil {
		return false
	}
	return h.Status == garageStatusHealthy
}
