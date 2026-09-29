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
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// replicas onto the target topology, given the live eligible hostnames, the
// node each replica's local PV is pinned to (replicaNode[ordinal]; "" when the
// replica's PVC is unbound — still binding, e.g. during initial bring-up) and
// whether Garage is currently healthy. An "" (settling) replica is never
// migrated; only a replica bound to a wrong or removed node is moved. A replica
// left Pending after its node was removed keeps its PVC bound to the old PV
// (Retain), so it reads back as that removed node — not "" — and is migrated.
//
// Topology rules (the 2-node state is intentionally left untouched — a
// mid-shrink transient we neither spread across nor consolidate from):
//   - eligible nodes >= replicas: SPREAD — one replica per node. A replica that
//     is co-located (shares its node) or sits on a node that is no longer
//     eligible is relocated onto an empty eligible node. Gated on
//     Garage being healthy so the other replicas have re-replicated before the
//     next move (re-replication only completes once the StatefulSet is whole).
//     The gate is kept even where the volume follows the pod and no
//     re-replication is needed: a move is still one replica down, and taking a
//     second one down while the cluster is degraded is what the gate prevents.
//   - exactly one eligible node: CONSOLIDATE — every replica onto that
//     node. Replicas not already pinned there (Pending because their node was
//     removed, or on a node that is no longer eligible) are relocated; a replica
//     already on the target is never recycled. A replica whose node is gone is
//     moved without a health gate: it holds no reachable data, so recycling it
//     only restores quorum. A replica whose node still exists holds a live copy
//     — the case a node role produces, when the role is given to a node the
//     replicas are not on — and is moved only while Garage is healthy, one at a
//     time, exactly like SPREAD; moving them all at once would recycle every
//     copy of the data on a node-pinned class.
//   - otherwise (0 or 2 nodes): no action.
//
// It returns at most one action; the caller performs it and requeues, so the
// rebalance advances one replica per reconcile.
func nextPlacementAction(eligible []string, present map[string]bool, spots map[int32]replicaSpot, replicas int32, healthy bool) placementAction {
	eligibleSet := make(map[string]struct{}, len(eligible))
	for _, n := range eligible {
		eligibleSet[n] = struct{}{}
	}

	switch {
	case int32(len(eligible)) >= replicas: // SPREAD
		if !healthy {
			return placementAction{} // wait: let the previous move re-replicate
		}
		perNode := map[string]int{}
		for _, spot := range spots {
			if _, ok := eligibleSet[spot.node]; ok {
				perNode[spot.node]++
			}
		}
		for ord := int32(0); ord < replicas; ord++ {
			n := spots[ord].node
			if n == "" {
				continue // PVC still binding — let it settle, do not disturb
			}
			if _, onCP := eligibleSet[n]; !onCP {
				return placementAction{act: true, ordinal: ord, reason: "spread: replica off an eligible node"}
			}
			if perNode[n] > 1 {
				return placementAction{act: true, ordinal: ord, reason: "spread: replica co-located on " + n}
			}
		}
		return placementAction{}

	case len(eligible) == 1: // CONSOLIDATE
		target := eligible[0]
		for ord := int32(0); ord < replicas; ord++ {
			n := spots[ord].node
			if n == "" {
				continue // PVC still binding (e.g. initial bring-up) — leave it
			}
			if n == target {
				continue
			}
			if present[n] && !healthy {
				return placementAction{} // a live copy: wait for the previous move to settle
			}
			return placementAction{act: true, ordinal: ord, reason: "consolidate onto " + target}
		}
		return placementAction{}

	default: // 0 or 2 eligible nodes: ignore
		return placementAction{}
	}
}

// reconcilePlacement performs at most one placement migration per reconcile: it
// reads the live topology, decides the next
// action (nextPlacementAction) and, when one is due, recycles that replica's PVC
// so the StatefulSet re-homes it onto the correct node. It returns a
// human-readable message and whether it acted (the caller then reports "not
// ready" and requeues so the move can settle before the next one).
//
// acted=false with a non-empty message is the third outcome: the same migration
// has been attempted maxPlacementAttempts times and is not being attempted
// again. The store is left as it is — co-located replicas still serve — and the
// caller surfaces the message so an operator sees why the placement is wrong
// instead of watching one replica rebuild itself forever.
func (d *Driver) reconcilePlacement(ctx context.Context, store *backend.Store) (acted bool, msg string, err error) {
	// A store outside the high-availability mode never migrates. A relocation
	// recycles the replica's PVC and lets it re-replicate from the surviving
	// copies — with one replica there are none, so the move would silently discard
	// the whole store. The replica therefore stays on the node its local PV bound
	// to, and goes Pending (data intact on that node's disk) if the node is
	// removed: bringing it back or recreating the store empty is an operator
	// decision.
	if !store.HighAvailability {
		return false, "", nil
	}

	candidates, err := d.candidateHostnames(ctx)
	if err != nil {
		return false, "", fmt.Errorf("list the nodes the data plane may run on: %w", err)
	}
	spots, err := d.replicaPlacement(ctx, store)
	if err != nil {
		return false, "", fmt.Errorf("read replica placement: %w", err)
	}

	present, err := d.presentNodes(ctx)
	if err != nil {
		return false, "", fmt.Errorf("list nodes: %w", err)
	}

	// The health gate matters for SPREAD and for CONSOLIDATE off a live node;
	// with no eligible node or two there is nothing to decide, and it is skipped.
	replicas := desiredReplicas(store)
	ready, why := false, ""
	if int32(len(candidates)) >= replicas || len(candidates) == 1 {
		ready, why = d.garageReadyToMove(ctx, store, replicas)
	}

	action := nextPlacementAction(candidates, present, spots, replicas, ready)
	if !action.act && why != "" && nextPlacementAction(candidates, present, spots, replicas, true).act {
		// A move is due and the cluster is up, but a replica is still being
		// filled. Said, so an operator watching a store that will not spread
		// knows it is waiting and on what.
		return false, "rebalancing System placement: the next move waits for the replicas to be in sync (" + why + ")", nil
	}
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

	nodePinned := spots[action.ordinal].nodePinned
	if err := d.moveReplica(ctx, store, action.ordinal, nodePinned); err != nil {
		return false, "", fmt.Errorf("move replica %d (%s): %w", action.ordinal, action.reason, err)
	}
	how := "rescheduling"
	if nodePinned {
		// Worth spelling out on the object: this one throws the replica's copy away
		// and rebuilds it, which is minutes of re-replication and not a restart.
		how = "recycling the node-pinned volume of"
	}
	return true, fmt.Sprintf("rebalancing System placement: %s %s-%d (%s)",
		how, resourceName(store), action.ordinal, action.reason), nil
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

// replicaSpot is where one replica currently sits, and whether its volume can
// follow it elsewhere.
type replicaSpot struct {
	// node is the node the replica occupies, or "" when it is still settling (no
	// PVC yet, a claim being deleted, an unbound claim, or a pod with no node).
	node string
	// nodePinned reports that the replica's volume is tied to that node — the
	// bound PersistentVolume carries a nodeAffinity, which is what a local
	// StorageClass produces. It decides what "move this replica" means: with a
	// pinned volume the move discards the replica's copy of the data and Garage
	// rebuilds it from the others; without one the volume follows the pod and the
	// move costs a restart.
	nodePinned bool
}

// replicaPlacement maps each replica ordinal to its spot.
//
// The node comes from the pod, not from the volume: with a class whose volumes
// follow the pod the volume has no node of its own to read. The volume is still
// consulted, for two things — whether it is pinned, and, when the pod has no node
// (Pending, because the node it was pinned to is gone), which node it is pinned
// to. That second case is the one that has to keep working: a replica left
// Pending after its node was removed must read back as that removed node, or
// nothing would ever move it.
func (d *Driver) replicaPlacement(ctx context.Context, store *backend.Store) (map[int32]replicaSpot, error) {
	replicas := desiredReplicas(store)
	out := make(map[int32]replicaSpot, replicas)
	for ord := int32(0); ord < replicas; ord++ {
		name := fmt.Sprintf("%s-%d", resourceName(store), ord)

		pvc := &corev1.PersistentVolumeClaim{}
		err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: "data-" + name}, pvc)
		switch {
		case apierrors.IsNotFound(err):
			out[ord] = replicaSpot{}
			continue
		case err != nil:
			return nil, err
		}
		if pvc.DeletionTimestamp != nil {
			// A move is already under way: the PVC is waiting for pvc-protection to
			// let go. Reading it as "still on the wrong node" is what turns one move
			// into a loop — nextPlacementAction would ask for the same move, the move
			// would delete the pod again, and the pod is the consumer keeping the
			// finalizer on. Reported as unplaced instead, which asks for nothing.
			out[ord] = replicaSpot{}
			continue
		}
		if pvc.Spec.VolumeName == "" {
			out[ord] = replicaSpot{}
			continue
		}

		pv := &corev1.PersistentVolume{}
		if err := d.apiReader.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			if apierrors.IsNotFound(err) {
				out[ord] = replicaSpot{}
				continue
			}
			return nil, err
		}
		spot := replicaSpot{nodePinned: pv.Spec.NodeAffinity != nil}

		pod := &corev1.Pod{}
		err = d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: name}, pod)
		switch {
		case err == nil && pod.Spec.NodeName != "":
			spot.node = pod.Spec.NodeName
		case err == nil || apierrors.IsNotFound(err):
			// No pod, or a pod the scheduler has not placed. A pinned volume still
			// says where the replica belongs; a volume that follows the pod says
			// nothing, and the replica is reported as settling.
			spot.node = pvNodeName(pv)
		default:
			return nil, err
		}
		out[ord] = spot
	}
	return out, nil
}

// pvNodeName reads the single node a volume is pinned to, or "" when it is not
// pinned to exactly one.
//
// Anything more elaborate than one term with one "in" expression naming one
// value is not a volume tied to a node — it is a topology constraint (a zone, a
// set of racks), and the placement reconcile has nothing to say about it.
//
// Which key names the node is not fixed. kubernetes.io/hostname is the one a
// cluster always has, but a CSI driver publishes its own per-node topology and
// pins its volumes with that: sds-local-volume uses
// topology.sds-local-volume-csi/node, and reading only the hostname made its
// volumes look unpinned — on the one class where being pinned is the whole
// point. So a key ending in /node counts too, which is the convention those
// drivers follow and is narrow enough to exclude a zone.
func pvNodeName(pv *corev1.PersistentVolume) string {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return ""
	}
	terms := pv.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) != 1 {
		return ""
	}
	for _, expr := range terms[0].MatchExpressions {
		if !namesANode(expr.Key) || expr.Operator != corev1.NodeSelectorOpIn || len(expr.Values) != 1 {
			continue
		}
		return expr.Values[0]
	}
	return ""
}

// namesANode reports whether a topology key identifies a node rather than a
// wider failure domain.
func namesANode(key string) bool {
	return key == hostnameTopologyKey || strings.HasSuffix(key, "/node")
}

// candidateHostnames lists the nodes the data plane may run on — the ones the
// placement admits — and returns their names.
//
// This is the set the replicas are spread across, and it has to be the set the
// scheduler works from, which is why it is the same Placement the pods carry. A
// placement reconcile that
// counted nodes the pods may not have would keep moving a replica towards a node
// the scheduler will never give it, recycling a replica's data on every attempt
// until the attempt cap stops it.
//
// Uncached, on a path that runs every 30 seconds and only for a store in the
// highly available mode: a cached Node informer would have to hold every Node in
// the cluster to answer one filtered question. The selector narrows the list on
// the server; taints and cordons are checked here, since no field selector can.
func (d *Driver) candidateHostnames(ctx context.Context) ([]string, error) {
	nodes := &corev1.NodeList{}
	if err := d.apiReader.List(ctx, nodes, client.MatchingLabels(d.placement.NodeSelector)); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(nodes.Items))
	for i := range nodes.Items {
		if d.placement.admits(&nodes.Items[i]) {
			out = append(out, nodes.Items[i].Name)
		}
	}
	return out, nil
}

// presentNodes returns the names of every node that exists, eligible or not. A
// replica on a node in this set holds a copy the store can still read; one on a
// node outside it holds nothing reachable.
func (d *Driver) presentNodes(ctx context.Context) (map[string]bool, error) {
	nodes := &metav1.PartialObjectMetadataList{}
	nodes.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("NodeList"))
	if err := d.apiReader.List(ctx, nodes); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(nodes.Items))
	for i := range nodes.Items {
		out[nodes.Items[i].Name] = true
	}
	return out, nil
}

// moveReplica relocates one replica, and what it deletes depends on whether the
// replica's volume can travel.
//
//   - A volume that follows the pod: only the pod is deleted. The StatefulSet
//     recreates it, the scheduler places it elsewhere (soft anti-affinity), and
//     the same volume is attached there. No data is lost and nothing has to be
//     re-replicated — the move is a restart.
//   - A volume pinned to a node: the pod AND the claim are deleted, because the
//     claim is what holds the replica to the node it must leave. The replacement
//     claim binds a volume on the node the scheduler picks, the replica boots
//     empty, and Garage re-replicates into it once the cluster is whole. That is
//     the expensive path, and it is the one the health gate and the attempt cap
//     exist for.
//
// The order matters in the second case. Deleting the claim while its pod still
// mounts it only stamps a deletionTimestamp: pvc-protection holds the object as
// long as a consumer exists. Then the pod goes, and the StatefulSet controller
// immediately recreates it under the same name with the same claim — so whether
// this works comes down to a race between pvc-protection dropping the finalizer
// and the new pod appearing. Lose that race and the new pod references a
// terminating claim, stays Pending on "persistentvolumeclaim is being deleted",
// and by existing it keeps the finalizer from ever being dropped. Removing the
// pod first means that when the delete lands there is no consumer to hold it.
func (d *Driver) moveReplica(ctx context.Context, store *backend.Store, ordinal int32, nodePinned bool) error {
	name := fmt.Sprintf("%s-%d", resourceName(store), ordinal)

	pod := &corev1.Pod{}
	pod.Namespace, pod.Name = d.namespace, name
	if err := d.client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod %s: %w", name, err)
	}

	if !nodePinned {
		return nil
	}

	pvcName := "data-" + name
	pvc := &corev1.PersistentVolumeClaim{}
	pvc.Namespace, pvc.Name = d.namespace, pvcName
	if err := d.client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pvc %s: %w", pvcName, err)
	}
	return nil
}

// garageReadyToMove reports whether the next placement move may start: the
// Garage admin API considers the cluster healthy and every node holds what it
// should. Any error (token missing, endpoint not answering, degraded) yields
// false, so a placement move is only started from a known-good state.
//
// Healthy is not enough to move the next replica. Garage reads healthy as soon
// as every node is connected and the layout is whole, and a replica that has
// just come back on an empty volume is connected long before it holds its copy
// of the data: its blocks are resynced with a delay of 2 × rpc_timeout (ten
// minutes, block/manager.rs). Recycling the next one inside that window leaves
// one full copy where the gate was meant to leave two. So the move also waits
// for the cluster to be in sync (clusterInSync, the same check the in-place
// change of the replica count uses), and why reports what it waits on when the
// cluster is healthy but not yet in sync.
func (d *Driver) garageReadyToMove(ctx context.Context, store *backend.Store, replicas int32) (ready bool, why string) {
	token, err := d.adminToken(ctx, store)
	if err != nil || token == "" {
		return false, ""
	}
	return readyToMove(ctx, d.admin(store, adminEndpoint(store, d.namespace, d.clusterDomain), token), replicas)
}

// readyToMove is the judgement half of garageReadyToMove, on a client.
func readyToMove(ctx context.Context, svc *adminClient, replicas int32) (ready bool, why string) {
	h, err := svc.health(ctx)
	if err != nil || h.Status != garageStatusHealthy {
		return false, ""
	}
	synced, why, err := clusterInSync(ctx, svc, replicas)
	if err != nil {
		return false, err.Error()
	}
	return synced, why
}
