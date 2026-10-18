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
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// layoutZone is the Garage layout zone assigned to every node. A single zone is
// deliberate: the store must keep a fixed replica count even on a single master
// (replicas co-located there), which a per-host zone would forbid.
// The trade-off is deliberate and worth stating: co-located replicas share a
// physical disk, so on a single master the redundancy is at the process level
// only — a lost disk loses the store. It buys a replica count that does not
// change when masters are added or removed, which is what keeps the pinned
// replication factor and the Garage quorum stable (see replicasHA); the placement
// reconcile spreads the replicas as soon as there are masters to spread them
// across.
const layoutZone = "dc1"

// garageStatusHealthy is the value Garage's admin GET /v2/GetClusterHealth
// reports in `status` when the cluster is fully healthy (the other values are
// "degraded" and "unavailable").
const garageStatusHealthy = "healthy"

// meshResult is the outcome of a meshing+layout reconcile pass.
type meshResult struct {
	ready     bool
	msg       string
	total     *resource.Quantity
	exhausted string
}

// nodePeer is a discovered Garage pod with its node identity.
type nodePeer struct {
	id string
	ip string
}

// ensureMeshAndLayout connects the Garage RPC peers and assigns the cluster
// layout via the admin API, returning whether the cluster is healthy and
// serving. It is idempotent: connect is a no-op for already-connected peers and
// layout is only staged for nodes not yet present in it.
func (d *Driver) ensureMeshAndLayout(ctx context.Context, store *backend.Store, capacity resource.Quantity) (meshResult, error) {
	token, err := d.adminToken(ctx, store)
	if err != nil {
		return meshResult{}, err
	}
	if token == "" {
		return meshResult{msg: "Garage admin token is not provisioned yet"}, nil
	}

	peers, err := d.discoverPeers(ctx, store, token)
	if err != nil {
		return meshResult{}, err
	}
	if len(peers) == 0 {
		return meshResult{msg: "waiting for Garage pods to report a node identity"}, nil
	}

	// Addressed at a pod, not at the Service — and that is what lets the readiness
	// probe mean something.
	//
	// Garage answers /health only once a node holds a role in the layout, and the
	// layout is assigned by these very calls. Made through the Service, which
	// routes to READY endpoints only, they cannot be made until a pod is ready and
	// a pod cannot be ready until they are made: a fresh installation sits at 0/3
	// forever (measured once at 35 minutes). Made at a pod's address, they work
	// while every pod is still unready, the layout lands, and readiness becomes
	// free to mean "this node is in the cluster and can serve" — which is what the
	// disruption budget needs it to mean.
	//
	// Any live peer will do: connect, layout and health are cluster-wide facts,
	// and discoverPeers only returns nodes that just answered.
	svc := d.admin(store, podAdminURL(peers[0].ip), token)

	// 1. Connect peers (idempotent gossip seed).
	peerSpecs := make([]string, 0, len(peers))
	for _, p := range peers {
		peerSpecs = append(peerSpecs, fmt.Sprintf("%s@%s:%d", p.id, p.ip, rpcPort))
	}
	if err := svc.connect(ctx, peerSpecs); err != nil {
		return meshResult{msg: fmt.Sprintf("connecting Garage peers: %v", err)}, nil
	}

	// 2. Reconcile the layout onto the live peers: assign roles for new nodes
	// and drop roles for nodes that are gone (once the full replica complement
	// is live, so a transient reschedule does not churn the layout).
	layout, err := svc.layout(ctx)
	if err != nil {
		return meshResult{msg: fmt.Sprintf("reading layout: %v", err)}, nil
	}

	changes := layoutRoleChanges(layout, peers, desiredReplicas(store), capacity.Value())
	if len(changes) > 0 {
		if err := svc.stageLayout(ctx, changes); err != nil {
			return meshResult{msg: fmt.Sprintf("staging layout: %v", err)}, nil
		}
		cur, err := svc.layout(ctx)
		if err != nil {
			return meshResult{msg: fmt.Sprintf("reading staged layout: %v", err)}, nil
		}
		if err := svc.applyLayout(ctx, cur.Version+1); err != nil {
			return meshResult{msg: fmt.Sprintf("applying layout: %v", err)}, nil
		}
	}

	// 3. Health gate. The free space is read first and reported either way: a
	// replica that has filled its volume counts as up in the cluster health.
	exhausted := ""
	if status, err := svc.clusterStatus(ctx); err != nil {
		d.log.Warn("unable to read the free space of the Garage nodes", "store", store.Name, "error", err.Error())
	} else {
		exhausted = exhaustedVolumes(status.Nodes)
	}
	health, err := svc.health(ctx)
	if err != nil {
		return meshResult{msg: fmt.Sprintf("reading health: %v", err), exhausted: exhausted}, nil
	}
	total := layoutTotalCapacity(layout)
	if health.Status != garageStatusHealthy {
		return meshResult{msg: fmt.Sprintf("Garage store health is %q (%d/%d storage nodes ok)", health.Status, health.StorageNodesOk, health.StorageNodes), total: total, exhausted: exhausted}, nil
	}

	// 4. A node that is never coming back must stop holding the cluster on an old
	// layout version.
	d.skipDeadNodesIfStuck(ctx, store, svc, len(peers), desiredReplicas(store))

	return meshResult{ready: true, msg: "Garage store is healthy", total: total, exhausted: exhausted}, nil
}

// skipDeadNodesIfStuck advances the layout trackers past nodes that are gone, and
// only when it is safe to call them gone.
//
// Garage keeps a layout version alive until every node has acknowledged it. A
// node that is removed for good — a master taken out of the cluster — never will,
// and the cluster then sits on an old version indefinitely. The admin API has a
// call for exactly this; what it needs is somebody to decide that the silent node
// is dead rather than restarting.
//
// The decision here is deliberately narrow: every replica the store expects is
// live and healthy, and the layout is still waiting on somebody. Whoever that is,
// it is not one of ours. During a rolling restart the count is short and nothing
// is called dead.
//
// Failures are logged and swallowed. This tidies up a layout; it is not a reason
// to fail a pass over a cluster that has just been found healthy.
func (d *Driver) skipDeadNodesIfStuck(ctx context.Context, store *backend.Store, svc *adminClient, livePeers int, want int32) {
	if int32(livePeers) < want {
		return
	}
	hist, err := svc.layoutHistory(ctx)
	if err != nil {
		d.log.Error("unable to read the cluster layout history", logger.Err(err), "store", store.Name)
		return
	}
	if hist.MinAck >= hist.CurrentVersion {
		return
	}

	// allowMissingData stays false: the ACK tracker moves, the SYNC tracker does
	// not. The cluster stops waiting for the dead node without anyone pretending
	// the data it held has been accounted for.
	res, err := svc.skipDeadNodes(ctx, hist.CurrentVersion, false)
	if err != nil {
		d.log.Error("unable to advance the layout past nodes that are gone", logger.Err(err),
			"store", store.Name, "version", hist.CurrentVersion, "minAck", hist.MinAck)
		return
	}
	if len(res.AckUpdated) > 0 {
		d.log.Info("the layout no longer waits for nodes that are gone",
			"store", store.Name, "version", hist.CurrentVersion, "nodes", res.AckUpdated)
	}
}

// adminToken reads the admin token from the per-cluster secret.
func (d *Driver) adminToken(ctx context.Context, store *backend.Store) (string, error) {
	secret := &corev1.Secret{}
	if err := d.client.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: secretName(store)}, secret); err != nil {
		return "", err
	}
	return string(secret.Data[secretKeyAdmin]), nil
}

// discoverPeers lists the Garage pods and queries each pod's admin API for its
// node identity. Pods without an IP or not yet answering are skipped.
func (d *Driver) discoverPeers(ctx context.Context, store *backend.Store, token string) ([]nodePeer, error) {
	pods, err := d.planePods(ctx, store)
	if err != nil {
		return nil, err
	}

	peers := make([]nodePeer, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		pc := d.admin(store, podAdminURL(pod.Status.PodIP), token)
		st, err := pc.status(ctx)
		if err != nil || st.Node == "" {
			// Pod not answering yet; it will be picked up on requeue.
			continue
		}
		peers = append(peers, nodePeer{id: st.Node, ip: pod.Status.PodIP})
	}
	return peers, nil
}

// layoutRoleChanges computes the layout mutations converging the assigned roles
// onto the currently-live Garage peers:
//
//   - every live peer not yet in the layout is assigned a role (single zone,
//     per-node capacity), so a freshly-scheduled pod starts holding data and
//     Garage re-replicates onto it;
//   - a role whose node is no longer live is removed — but only once the full
//     replica complement (expected) is live. With stable node identity a System
//     pod recycled onto another master rejoins with its ORIGINAL node ID (the
//     restore-node-key initContainer restores node_key), so the common case adds
//     and removes nothing and Garage re-syncs the empty PV via anti-entropy. This
//     removal path is the defensive fallback for a node ID that genuinely never
//     returns (e.g. an identity lost before it was captured); gating it on a full
//     complement avoids churning the layout during a transient reschedule.
//
// capacity is the per-node storage capacity in bytes.
func layoutRoleChanges(layout *clusterLayout, peers []nodePeer, expected int32, capacity int64) []roleChange {
	assigned := map[string]struct{}{}
	for _, r := range layout.Roles {
		assigned[r.ID] = struct{}{}
	}
	live := make(map[string]struct{}, len(peers))
	for _, p := range peers {
		live[p.id] = struct{}{}
	}

	var changes []roleChange
	for _, p := range peers {
		if _, ok := assigned[p.id]; ok {
			continue
		}
		c := capacity
		changes = append(changes, roleChange{ID: p.id, Zone: layoutZone, Capacity: &c, Tags: []string{}})
	}
	if int32(len(peers)) >= expected {
		for _, r := range layout.Roles {
			if _, ok := live[r.ID]; ok {
				continue
			}
			changes = append(changes, roleChange{ID: r.ID, Remove: true, Tags: []string{}})
		}
	}
	return changes
}

// layoutTotalCapacity sums the assigned role capacities (bytes) into a Quantity.
func layoutTotalCapacity(layout *clusterLayout) *resource.Quantity {
	var sum int64
	for _, r := range layout.Roles {
		if r.Capacity != nil {
			sum += *r.Capacity
		}
	}
	if sum == 0 {
		return nil
	}
	q := resource.NewQuantity(sum, resource.BinarySI)
	return q
}
