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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// spotsOn turns the table's "ordinal -> node" fixtures into placement spots. The
// decision does not depend on whether the volume is pinned — that only decides
// what the move deletes — so the fixtures stay as plain nodes.
func spotsOn(nodes map[int32]string) map[int32]replicaSpot {
	out := make(map[int32]replicaSpot, len(nodes))
	for ord, node := range nodes {
		out[ord] = replicaSpot{node: node}
	}
	return out
}

// pinnedPV builds a PersistentVolume tied to one node, which is what a local
// StorageClass produces and what the placement reconcile reads to decide whether
// moving a replica costs it its copy of the data.
func pinnedPV(name, node string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      hostnameTopologyKey,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{node},
						}},
					}},
				},
			},
		},
	}
}

func TestNextPlacementAction(t *testing.T) {
	const replicas = int32(3)

	cases := []struct {
		name    string
		cpNodes []string
		replica map[int32]string
		healthy bool
		wantAct bool
		wantOrd int32
	}{
		// SPREAD (>=3 masters)
		{
			name:    "spread: already 1-per-master -> no action",
			cpNodes: []string{"m0", "m1", "m2"},
			replica: map[int32]string{0: "m0", 1: "m1", 2: "m2"},
			healthy: true,
			wantAct: false,
		},
		{
			name:    "spread: co-located + healthy -> recycle the co-located replica",
			cpNodes: []string{"m0", "m1", "m2"},
			replica: map[int32]string{0: "m0", 1: "m0", 2: "m0"},
			healthy: true,
			wantAct: true,
			wantOrd: 0, // first replica sharing m0
		},
		{
			name:    "spread: co-located but NOT healthy -> gated, no action",
			cpNodes: []string{"m0", "m1", "m2"},
			replica: map[int32]string{0: "m0", 1: "m0", 2: "m0"},
			healthy: false,
			wantAct: false,
		},
		{
			name:    "spread: replica bound to a removed node + healthy -> recycle it",
			cpNodes: []string{"m0", "m1", "m2"},
			replica: map[int32]string{0: "m0", 1: "m1", 2: "gone"},
			healthy: true,
			wantAct: true,
			wantOrd: 2,
		},
		{
			name:    "spread: unbound replica (still binding) -> left alone",
			cpNodes: []string{"m0", "m1", "m2"},
			replica: map[int32]string{0: "m0", 1: "m1", 2: ""},
			healthy: true,
			wantAct: false,
		},
		// CONSOLIDATE (exactly 1 master)
		{
			name:    "consolidate: all on target -> no action",
			cpNodes: []string{"m0"},
			replica: map[int32]string{0: "m0", 1: "m0", 2: "m0"},
			healthy: false,
			wantAct: false,
		},
		{
			name:    "consolidate: replicas bound to removed masters -> recycle first (anchor untouched)",
			cpNodes: []string{"m0"},
			replica: map[int32]string{0: "m0", 1: "gone1", 2: "gone2"},
			healthy: false,
			wantAct: true,
			wantOrd: 1,
		},
		{
			name:    "consolidate: unbound replica (still binding) -> left alone",
			cpNodes: []string{"m0"},
			replica: map[int32]string{0: "m0", 1: "", 2: "m0"},
			healthy: false,
			wantAct: false,
		},
		{
			name:    "consolidate: replica on a stale node -> recycle it",
			cpNodes: []string{"m0"},
			replica: map[int32]string{0: "m0", 1: "m0", 2: "m2"},
			healthy: false,
			wantAct: true,
			wantOrd: 2,
		},
		// IGNORE (2 or 0 masters)
		{
			name:    "two masters -> ignored even when replicas are messy",
			cpNodes: []string{"m0", "m1"},
			replica: map[int32]string{0: "m0", 1: "", 2: "gone"},
			healthy: true,
			wantAct: false,
		},
		{
			name:    "zero masters -> no action",
			cpNodes: []string{},
			replica: map[int32]string{0: "", 1: "", 2: ""},
			healthy: false,
			wantAct: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nextPlacementAction(tc.cpNodes, spotsOn(tc.replica), replicas, tc.healthy)
			if got.act != tc.wantAct {
				t.Fatalf("act=%v, want %v (reason=%q)", got.act, tc.wantAct, got.reason)
			}
			if got.act && got.ordinal != tc.wantOrd {
				t.Errorf("ordinal=%d, want %d (reason=%q)", got.ordinal, tc.wantOrd, got.reason)
			}
		})
	}
}

// TestReconcilePlacementNonHA pins the promise made outside the
// high-availability mode: the sole replica is never relocated. The fixture is a
// consolidate case the highly available store acts on without a health gate (one
// master left, the replica's PV pinned to a master that is gone), so the two
// modes are compared on identical input.
func TestReconcilePlacementNonHA(t *testing.T) {
	ns := "d8-system-s3"

	newDriver := func(t *testing.T, store *backend.Store) (*Driver, client.Client) {
		t.Helper()
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "m0",
			Labels: map[string]string{controlPlaneNodeLabel: "", hostnameTopologyKey: "m0"},
		}}
		pv := pinnedPV("replica-pv", "gone")
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(store) + "-0", Namespace: ns},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
		}
		c := fake.NewClientBuilder().WithScheme(rfScheme(t)).WithObjects(node, pv, pvc).Build()
		return &Driver{client: c, apiReader: c, namespace: ns}, c
	}

	pvcGone := func(t *testing.T, c client.Client, store *backend.Store) bool {
		t.Helper()
		err := c.Get(context.Background(), client.ObjectKey{
			Namespace: ns, Name: "data-" + resourceName(store) + "-0",
		}, &corev1.PersistentVolumeClaim{})
		return apierrors.IsNotFound(err)
	}

	t.Run("outside the HA mode: never migrates", func(t *testing.T) {
		store := nonHAStore()
		d, c := newDriver(t, store)
		acted, msg, err := d.reconcilePlacement(context.Background(), store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acted {
			t.Errorf("acted=true (%q), want no placement action for a single replica", msg)
		}
		if pvcGone(t, c, store) {
			t.Errorf("the replica's PVC was recycled; single-replica data must never be discarded")
		}
	})

	t.Run("default profile: consolidates onto the surviving master", func(t *testing.T) {
		store := systemStore()
		d, c := newDriver(t, store)
		acted, _, err := d.reconcilePlacement(context.Background(), store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acted {
			t.Fatalf("acted=false, want the default profile to recycle the stale replica")
		}
		if !pvcGone(t, c, store) {
			t.Errorf("expected the replica's PVC to be recycled")
		}
	})
}

// A replica whose PVC is terminating is mid-move, not misplaced.
//
// Reading it as "still on the wrong node" is what turns one recycle into a loop:
// nextPlacementAction asks for the same move, recycleReplica deletes the pod
// again, and the pod is the consumer that keeps pvc-protection from dropping the
// finalizer. The replica never leaves Pending and the message repeats every
// requeue interval.
func TestATerminatingPVCReadsAsMidMoveNotMisplaced(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system"}

	pv := pinnedPV("replica-pv", "m0")
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-" + resourceName(store) + "-0",
			Namespace: ns,
			// A finalizer, so deleting it leaves the object terminating rather than
			// gone — which is exactly the state pvc-protection produces.
			Finalizers: []string{"kubernetes.io/pvc-protection"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
	}
	c := fake.NewClientBuilder().WithScheme(rfScheme(t)).WithObjects(pv, pvc).Build()
	d := &Driver{client: c, apiReader: c, namespace: ns}
	ctx := context.Background()

	// Before the delete the replica is placed, and on the node the PV names.
	placed, err := d.replicaPlacement(ctx, store)
	if err != nil {
		t.Fatalf("replicaPlacement: %v", err)
	}
	if placed[0].node != "m0" {
		t.Fatalf("a bound PVC must report its node, got %q", placed[0].node)
	}

	if err := c.Delete(ctx, pvc); err != nil {
		t.Fatalf("delete pvc: %v", err)
	}

	moving, err := d.replicaPlacement(ctx, store)
	if err != nil {
		t.Fatalf("replicaPlacement after delete: %v", err)
	}
	if moving[0].node != "" {
		t.Fatalf("a terminating PVC reported node %q; it has to read as unplaced, "+
			"or the placement loop asks for the same move again and deletes the pod that "+
			"is holding the finalizer", moving[0].node)
	}
}

// The pod goes before the PVC, and the order is the whole difference.
//
// Deleting the PVC while its pod still mounts it only stamps a deletionTimestamp;
// pvc-protection holds the object while a consumer exists. Then the pod goes and
// the StatefulSet recreates it immediately under the same name with the same
// claim — so the outcome is a race, and losing it leaves the new pod Pending on a
// terminating PVC whose finalizer it is itself keeping alive.
func TestRecycleDeletesThePodBeforeTheClaim(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system"}
	name := resourceName(store) + "-0"

	var order []string
	c := fake.NewClientBuilder().
		WithScheme(rfScheme(t)).
		WithObjects(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + name, Namespace: ns}},
		).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				switch obj.(type) {
				case *corev1.Pod:
					order = append(order, "pod")
				case *corev1.PersistentVolumeClaim:
					order = append(order, "pvc")
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	d := &Driver{client: c, apiReader: c, namespace: ns}
	if err := d.moveReplica(context.Background(), store, 0, true); err != nil {
		t.Fatalf("moveReplica: %v", err)
	}

	want := []string{"pod", "pvc"}
	if len(order) != len(want) || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("delete order = %v, want %v: the claim must not be left terminating "+
			"while its consumer is still alive", order, want)
	}
}

// The rollout gate has to close on a StatefulSet that has not caught up, not
// only on one that is short of ready pods.
//
// What sits immediately behind this gate is mesh and layout: RPC peers connected
// and a cluster layout applied. Letting it open while the old pods are still
// serving means doing that against a Garage running the previous garage.toml,
// with consumers already holding credentials for an endpoint reported Ready.
func TestTheRolloutGateChecksRevisionNotJustCount(t *testing.T) {
	const desired = 3

	sts := func(mutate func(*appsv1.StatefulSet)) *appsv1.StatefulSet {
		s := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "system-garage", Generation: 7},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 7,
				ReadyReplicas:      desired,
				UpdatedReplicas:    desired,
				CurrentRevision:    "rev-2",
				UpdateRevision:     "rev-2",
			},
		}
		if mutate != nil {
			mutate(s)
		}
		return s
	}

	for _, tc := range []struct {
		name      string
		mutate    func(*appsv1.StatefulSet)
		wantReady bool
		wantMsg   string
	}{
		{"settled on the applied revision", nil, true, ""},
		{
			// The window this fix exists for: the update was accepted, the old pods
			// are all still Ready, and the controller has not begun rolling.
			name:    "the update has not been observed yet",
			mutate:  func(s *appsv1.StatefulSet) { s.Status.ObservedGeneration = 6 },
			wantMsg: "not observed yet",
		},
		{
			name: "rolling: pods still on the previous revision",
			mutate: func(s *appsv1.StatefulSet) {
				s.Status.CurrentRevision = "rev-1"
				s.Status.UpdatedReplicas = 1
			},
			wantMsg: "on the new revision",
		},
		{
			name:    "right revision, not enough ready",
			mutate:  func(s *appsv1.StatefulSet) { s.Status.ReadyReplicas = 2 },
			wantMsg: "pods ready",
		},
		{
			// A StatefulSet that has never rolled reports no UpdateRevision; the
			// revision check must not read that as "mid-rollout" forever.
			name:      "no revision reported at all",
			mutate:    func(s *appsv1.StatefulSet) { s.Status.UpdateRevision = ""; s.Status.CurrentRevision = "" },
			wantReady: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ready, msg := workloadRolledOut(sts(tc.mutate), desired)
			if ready != tc.wantReady {
				t.Fatalf("ready = %v, want %v (msg %q)", ready, tc.wantReady, msg)
			}
			if tc.wantMsg != "" && !strings.Contains(msg, tc.wantMsg) {
				t.Fatalf("msg = %q, want it to mention %q", msg, tc.wantMsg)
			}
		})
	}
}

// A volume that follows the pod must not have its claim deleted to move the
// replica: the claim IS the data. Deleting it discards a copy that did not need
// discarding and makes Garage re-replicate for nothing — minutes of traffic and,
// while it runs, one replica's worth of redundancy less than the store had.
func TestMovingAReplicaOnAFollowingVolumeOnlyDeletesThePod(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system"}
	name := resourceName(store) + "-0"

	var deleted []string
	c := fake.NewClientBuilder().
		WithScheme(rfScheme(t)).
		WithObjects(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + name, Namespace: ns}},
		).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				switch obj.(type) {
				case *corev1.Pod:
					deleted = append(deleted, "pod")
				case *corev1.PersistentVolumeClaim:
					deleted = append(deleted, "pvc")
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	d := &Driver{client: c, apiReader: c, namespace: ns}
	if err := d.moveReplica(context.Background(), store, 0, false); err != nil {
		t.Fatalf("moveReplica: %v", err)
	}

	if len(deleted) != 1 || deleted[0] != "pod" {
		t.Fatalf("deleted %v, want the pod only", deleted)
	}
	if err := c.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: "data-" + name}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Errorf("the claim was removed with a volume that can travel: %v", err)
	}
}

// Where a replica IS, and where it BELONGS, come from two different objects, and
// the reconcile needs both to be right.
//
//   - A running replica is where its pod is. With a class whose volumes follow
//     the pod there is nothing else to ask — the volume has no node of its own.
//   - A replica whose pod is not scheduled is where its volume is pinned, if it is
//     pinned at all. That is the case the placement rules exist for: a replica left
//     Pending because the node its volume lives on was removed. Reading it as
//     "settling" would leave it Pending forever.
func TestReplicaPlacementReadsThePodThenTheVolume(t *testing.T) {
	ns := "d8-system-s3"
	store := &backend.Store{Name: "system"}
	ctx := context.Background()
	claim := func(pvName string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data-" + resourceName(store) + "-0", Namespace: ns},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
		}
	}
	pod := func(node string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName(store) + "-0", Namespace: ns},
			Spec:       corev1.PodSpec{NodeName: node},
		}
	}
	followingPV := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "net-pv"}}

	cases := []struct {
		name       string
		objects    []client.Object
		wantNode   string
		wantPinned bool
	}{
		{
			name:     "running on a volume that travels: the pod names the node",
			objects:  []client.Object{followingPV, claim("net-pv"), pod("m1")},
			wantNode: "m1",
		},
		{
			name:       "running on a pinned volume: still the pod, and pinned",
			objects:    []client.Object{pinnedPV("local-pv", "m2"), claim("local-pv"), pod("m2")},
			wantNode:   "m2",
			wantPinned: true,
		},
		{
			name:       "pod not scheduled, volume pinned to a node that is gone",
			objects:    []client.Object{pinnedPV("local-pv", "gone"), claim("local-pv"), pod("")},
			wantNode:   "gone",
			wantPinned: true,
		},
		{
			name:     "pod not scheduled, volume travels: nothing to say yet",
			objects:  []client.Object{followingPV, claim("net-pv"), pod("")},
			wantNode: "",
		},
		{
			name:     "no pod at all, volume travels: settling",
			objects:  []client.Object{followingPV, claim("net-pv")},
			wantNode: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(rfScheme(t)).WithObjects(tc.objects...).Build()
			d := &Driver{client: c, apiReader: c, namespace: ns}

			spots, err := d.replicaPlacement(ctx, store)
			if err != nil {
				t.Fatalf("replicaPlacement: %v", err)
			}
			if spots[0].node != tc.wantNode {
				t.Errorf("node=%q, want %q", spots[0].node, tc.wantNode)
			}
			if spots[0].nodePinned != tc.wantPinned {
				t.Errorf("nodePinned=%v, want %v", spots[0].nodePinned, tc.wantPinned)
			}
		})
	}
}

// pvNodeName answers one question — which single node is this volume tied to —
// and must not answer it for topology that merely narrows placement.
func TestPVNodeName(t *testing.T) {
	zonal := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
		NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "topology.kubernetes.io/zone",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"zone-a"},
				}},
			}},
		}},
	}}
	twoNodes := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
		NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      hostnameTopologyKey,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"m0", "m1"},
				}},
			}},
		}},
	}}

	cases := map[string]struct {
		pv   *corev1.PersistentVolume
		want string
	}{
		"no affinity at all":      {&corev1.PersistentVolume{}, ""},
		"pinned to one node":      {pinnedPV("pv", "m0"), "m0"},
		"a zone is not a node":    {zonal, ""},
		"two nodes is not a node": {twoNodes, ""},
	}
	for name, tc := range cases {
		if got := pvNodeName(tc.pv); got != tc.want {
			t.Errorf("%s: pvNodeName=%q, want %q", name, got, tc.want)
		}
	}
}

// TestCandidateHostnamesAreTheMasters: the replicas are spread across the
// masters and nothing else. The set has to be the one the scheduler is given in
// dataPlanePlacement — count a node the pods may not have, and the reconcile
// moves a replica towards a node it will never be scheduled on, throwing away
// that replica's data on every attempt.
func TestCandidateHostnamesAreTheMasters(t *testing.T) {
	// The label the data plane used to prefer. It exists here and nowhere in the
	// package: a node carrying it is a node like any other, and the only thing
	// that makes one eligible is being a master.
	const systemNodeLabel = "node-role.deckhouse.io/system"

	ctx := context.Background()
	node := func(name string, labels ...string) *corev1.Node {
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
		for _, l := range labels {
			n.Labels[l] = ""
		}
		return n
	}
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(
		node("sys-0", systemNodeLabel),
		node("sys-1", systemNodeLabel),
		node("master-0", controlPlaneNodeLabel),
		// Labelled both: it is a master, and a master is a place to put a replica.
		node("both-0", systemNodeLabel, controlPlaneNodeLabel),
		node("worker-0"),
	).Build()
	d := &Driver{client: c, apiReader: c, namespace: "d8-system-s3"}

	got, err := d.candidateHostnames(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := map[string]bool{"master-0": false, "both-0": false}
	for _, name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%q is a candidate for the data plane and should not be", name)
			continue
		}
		if want[name] {
			t.Errorf("%q is counted twice: a node that is both is still one place to put a replica", name)
		}
		want[name] = true
	}
	for name, found := range want {
		if !found {
			t.Errorf("%q is not a candidate and should be", name)
		}
	}
}

// TestAVolumePinnedByACSIDriversOwnTopologyIsStillPinned: the placement reconcile
// reads which node a replica's volume is on out of the volume itself, and the key
// that names the node depends on who provisioned it. sds-local-volume pins with
// topology.sds-local-volume-csi/node, and while only kubernetes.io/hostname was
// recognised its volumes read as not pinned at all — on the one class where being
// pinned to a node is the entire point.
func TestAVolumePinnedByACSIDriversOwnTopologyIsStillPinned(t *testing.T) {
	pinned := func(key, value string) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      key,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{value},
					}},
				}},
			}},
		}}
	}

	cases := map[string]struct {
		pv   *corev1.PersistentVolume
		want string
	}{
		"the hostname every cluster has":  {pinned(hostnameTopologyKey, "worker-1"), "worker-1"},
		"sds-local-volume's own topology": {pinned("topology.sds-local-volume-csi/node", "worker-2"), "worker-2"},
		"another driver's node topology":  {pinned("topology.hostpath.csi/node", "worker-3"), "worker-3"},
		"a zone is not a node":            {pinned("topology.kubernetes.io/zone", "eu-central-1a"), ""},
		"a region is not a node":          {pinned("topology.kubernetes.io/region", "eu-central-1"), ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := pvNodeName(tc.pv); got != tc.want {
				t.Errorf("pvNodeName = %q, want %q", got, tc.want)
			}
		})
	}
}
