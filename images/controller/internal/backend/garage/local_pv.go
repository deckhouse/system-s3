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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// ensureLocalPVs maintains the static pool of node-sticky local PVs that backs
// the data-plane StatefulSet. For every control-plane node it ensures
// desiredReplicas PVs of the current incarnation exist (each
// hostPath-backed and nodeAffinity-pinned to that node), so the scheduler — under
// WaitForFirstConsumer — always has an available PV on whichever node it places a
// replica, and a bound replica returns to its node (hence its data) after a
// restart.
//
// It is create-only and idempotent: PVs have deterministic names and existing
// ones are left untouched. The pool carries no ownerReference — there is no CR to
// own it — so stale PVs are reaped here instead (gcLocalPVs), and Retain keeps
// the on-disk data regardless. Over-provisioning (a full pool per node, most of it Available) is
// intentional and cheap: an Available PV allocates no directory until a pod
// mounts it.
func (d *Driver) ensureLocalPVs(ctx context.Context, store *backend.Store, incarnation int32) error {
	hostnames, err := d.controlPlaneHostnames(ctx)
	if err != nil {
		return err
	}

	// Cached: the manager's PersistentVolume informer is restricted to this very
	// pool (see the cache options in main), and both users of the list tolerate a
	// stale one. A missing entry means a Create that comes back AlreadyExists,
	// which is ignored below; an entry that is already gone means a Delete that
	// comes back NotFound, which gcLocalPVs ignores. The teardown path in
	// recreate.go deliberately keeps reading through the APIReader instead —
	// there, under-reporting would declare the previous data plane gone while it
	// is not.
	existing := &corev1.PersistentVolumeList{}
	if err := d.client.List(ctx, existing,
		client.MatchingLabels{storeLabel: store.Name},
		client.HasLabels{labelSystemLocalNode},
	); err != nil {
		return err
	}
	have := make(map[string]struct{}, len(existing.Items))
	for i := range existing.Items {
		have[existing.Items[i].Name] = struct{}{}
	}

	desired := desiredLocalPVs(store, hostnames, incarnation)
	wanted := make(map[string]struct{}, len(desired))
	for _, pv := range desired {
		wanted[pv.Name] = struct{}{}
		if _, ok := have[pv.Name]; ok {
			continue
		}
		if err := d.client.Create(ctx, pv); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	return d.gcLocalPVs(ctx, existing, hostnames, wanted)
}

// gcLocalPVs reaps stale pool PVs: any Released PV (its PVC was recycled
// during a rebalance — Retain left the PV behind), and any Available PV that is
// pinned to a node which is no longer a control-plane node (a removed master) or
// is not part of the wanted pool (a leftover of a previous incarnation, or of a
// replica count the cluster no longer runs). Bound PVs are never touched (a live
// replica, or a Pending replica still owning it — the placement reconcile
// recycles the latter's PVC, after which the PV goes Released and is reaped on a
// later pass).
func (d *Driver) gcLocalPVs(ctx context.Context, pool *corev1.PersistentVolumeList, hostnames []string, wanted map[string]struct{}) error {
	live := make(map[string]struct{}, len(hostnames))
	for _, h := range hostnames {
		live[h] = struct{}{}
	}
	for i := range pool.Items {
		pv := &pool.Items[i]
		_, onLiveNode := live[pv.Labels[labelSystemLocalNode]]
		_, inPool := wanted[pv.Name]
		stale := pv.Status.Phase == corev1.VolumeReleased ||
			(pv.Status.Phase == corev1.VolumeAvailable && (!onLiveNode || !inPool))
		if !stale {
			continue
		}
		if err := d.client.Delete(ctx, pv); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// controlPlaneHostnames lists control-plane nodes and returns their hostnames
// (the kubernetes.io/hostname label value, falling back to the node name), which
// key both the PV nodeAffinity and its deterministic name. Read from the cache,
// whose Node informer is restricted to the same label (see the cache options in
// main), so this is still not a cluster-wide Node informer: control-plane
// membership changes on the timescale of a cluster edit, and this runs every 30
// seconds.
func (d *Driver) controlPlaneHostnames(ctx context.Context) ([]string, error) {
	nodes := &corev1.NodeList{}
	if err := d.client.List(ctx, nodes, client.HasLabels{controlPlaneNodeLabel}); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(nodes.Items))
	for i := range nodes.Items {
		h := nodes.Items[i].Labels[hostnameTopologyKey]
		if h == "" {
			h = nodes.Items[i].Name
		}
		out = append(out, h)
	}
	return out, nil
}

// desiredLocalPVs is the full pool the controller wants for the given
// incarnation: desiredReplicas PVs per control-plane hostname (one per hostname in
// single-replica mode, so the scheduler can still place the sole replica on any
// master).
func desiredLocalPVs(store *backend.Store, hostnames []string, incarnation int32) []*corev1.PersistentVolume {
	replicas := desiredReplicas(store)
	pvs := make([]*corev1.PersistentVolume, 0, len(hostnames)*int(replicas))
	for _, h := range hostnames {
		for i := int32(0); i < replicas; i++ {
			pvs = append(pvs, buildLocalPV(store, h, incarnation, i))
		}
	}
	return pvs
}
