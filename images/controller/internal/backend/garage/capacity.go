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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/monitoring"
)

// The store sizes itself from what its consumers asked for: the sum of every
// SystemBucket's spec.size, plus room for the things a sum of quotas does not
// count.
//
//   - headroomPercent covers what a bucket occupies beyond the bytes of its
//     objects — Garage's block store rounds, multipart uploads leave parts behind
//     until they are completed or swept, and a bucket sitting exactly at its quota
//     must not be sitting exactly at the end of the disk. Ten per cent: the quota
//     is the ceiling a bucket cannot write past, so this covers the overhead on
//     top of it and not a second ceiling.
//   - metadataReserve is the metadata engine's own space (LMDB, the block index,
//     the layout). It does not scale with the objects in any way this module can
//     predict, and it must not be squeezed out by them.
//   - floorSize keeps a store with one tiny bucket — or none at all — on a volume
//     Garage can actually start on.
//   - stepSize is what keeps this from being a resize per bucket: the answer is
//     rounded up to a multiple, so adding a 1Gi bucket to a store that already has
//     room changes nothing at all.
//
// The numbers are a starting point, and deliberately generous in the direction
// that is cheap to be wrong in: an oversized volume costs space, an undersized one
// costs the platform its writes.
const (
	headroomPercent = 10
	metadataReserve = 2 << 30  // 2Gi
	floorSize       = 10 << 30 // 10Gi
	stepSize        = 5 << 30  // 5Gi
)

// volumeSizeFor is the size of one replica's volume for a given sum of bucket
// sizes.
//
// Per replica, not divided between them: with a replication factor equal to the
// replica count every node holds a full copy of everything, so each volume has to
// fit the whole sum. The day that stops being true — more replicas than the
// factor — this is the function that has to learn about partitions.
func volumeSizeFor(sumBytes int64) resource.Quantity {
	want := sumBytes + sumBytes*headroomPercent/100 + metadataReserve
	if want < floorSize {
		want = floorSize
	}
	if rem := want % stepSize; rem != 0 {
		want += stepSize - rem
	}
	return *resource.NewQuantity(want, resource.BinarySI)
}

// desiredBucketBytes sums spec.size over every SystemBucket in the cluster.
//
// A resource whose size does not parse is counted as zero and named in the
// returned error's stead — it is not fatal here, because the reconciler that owns
// that resource reports it on the resource itself, and one malformed field must
// not stop the store from sizing itself for the other consumers.
func (d *Driver) desiredBucketBytes(ctx context.Context) (int64, error) {
	list := &v1alpha1.SystemBucketList{}
	if err := d.client.List(ctx, list); err != nil {
		return 0, err
	}
	var sum int64
	for i := range list.Items {
		b := &list.Items[i]
		if b.DeletionTimestamp != nil {
			// Already going away: its space is about to be returned, and counting it
			// would keep the volume sized for a consumer that no longer exists.
			continue
		}
		if b.Spec.Size == "" {
			continue
		}
		q, err := resource.ParseQuantity(b.Spec.Size)
		if err != nil {
			d.log.Warn("ignoring a SystemBucket whose size is not a quantity when sizing the store",
				"systemBucket", b.Namespace+"/"+b.Name, "size", b.Spec.Size)
			continue
		}
		sum += q.Value()
	}
	return sum, nil
}

// ensureVolumeCapacity grows the data plane's volumes to want when the sum of the
// buckets has outgrown them. It reports whether the workload has to be recreated
// for the new size to take (the caller does that as part of its normal apply) and
// a message when the growth could not be performed.
//
// Shrinking is never attempted. A PersistentVolumeClaim cannot shrink, and a
// smaller sum means somebody deleted a bucket — the volume stays the size it
// reached, which is documented on spec.size.
//
// The claims are patched one by one rather than through the StatefulSet, because
// a StatefulSet's volumeClaimTemplates are immutable: the template is brought up
// to date by deleting the workload with the pods left running (orphan propagation)
// and letting the caller create it again. The pods keep serving throughout; what
// they lose is the controller that would restart them, for the seconds between the
// two calls.
// It returns no error, and that is the contract rather than an oversight: every
// way this can fail is a fact about the cluster an operator has to be told —
// a class that does not allow expansion, a quota — and none of them is a reason
// to stop reconciling a store that is serving. The failure comes back in the
// capacity's Problem, next to how big the volumes are and should be.
func (d *Driver) ensureVolumeCapacity(ctx context.Context, store *backend.Store, want resource.Quantity) (recreate bool, capacity backend.StoreCapacity) {
	replicas := desiredReplicas(store)
	capacity.WantBytes = want.Value()
	for ord := int32(0); ord < replicas; ord++ {
		name := fmt.Sprintf("data-%s-%d", resourceName(store), ord)
		grown, err := d.growClaim(ctx, name, want)
		if err != nil {
			// The usual cause is a class that does not allow expansion, and that is
			// an operator-visible fact rather than a controller failure: the store
			// keeps serving at the size it has, and the buckets that do not fit stay
			// unable to write.
			capacity.Problem = fmt.Sprintf(
				"unable to grow the data plane's volume %s to %s (the sum of the SystemBucket sizes): %v — "+
					"a StorageClass with allowVolumeExpansion is required to grow the system object storage",
				name, want.String(), err)
			break
		}
		recreate = recreate || grown
	}
	// On every exit, including the failing one: a store that cannot grow is
	// precisely the store whose sizes somebody needs to be able to see.
	have, detail, refused := d.publishVolumeSizes(ctx, store, want, replicas)
	capacity.HaveBytes, capacity.Detail = have, detail
	if capacity.Problem == "" {
		capacity.Problem = refused
	}
	return recreate && capacity.Problem == "", capacity
}

// resizeState reads what a claim says about its own growth: a refusal the
// resizer or the kubelet reported, or where a growth under way has got to. Both
// are empty for a claim that is not growing.
func resizeState(pvc *corev1.PersistentVolumeClaim) (detail, refused string) {
	switch pvc.Status.AllocatedResourceStatuses[corev1.ResourceStorage] {
	case corev1.PersistentVolumeClaimControllerResizeInfeasible, corev1.PersistentVolumeClaimNodeResizeInfeasible:
		refused = fmt.Sprintf("the resize of claim %s was refused as infeasible", pvc.Name)
	case corev1.PersistentVolumeClaimNodeResizePending:
		detail = fmt.Sprintf("claim %s: the volume is grown and the node has yet to grow its file system", pvc.Name)
	case corev1.PersistentVolumeClaimControllerResizeInProgress, corev1.PersistentVolumeClaimNodeResizeInProgress:
		detail = fmt.Sprintf("claim %s: the resize is in progress", pvc.Name)
	}
	for _, c := range pvc.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case corev1.PersistentVolumeClaimControllerResizeError, corev1.PersistentVolumeClaimNodeResizeError:
			refused = fmt.Sprintf("the resize of claim %s failed: %s", pvc.Name, c.Message)
		case corev1.PersistentVolumeClaimFileSystemResizePending:
			if detail == "" {
				detail = fmt.Sprintf("claim %s: the volume is grown and the node has yet to grow its file system "+
					"(a driver that cannot do it online does it when the volume is next mounted)", pvc.Name)
			}
		case corev1.PersistentVolumeClaimResizing:
			if detail == "" {
				detail = fmt.Sprintf("claim %s: the resizer is growing the volume", pvc.Name)
			}
		}
	}
	return detail, refused
}

// publishVolumeSizes reports what one replica's volume should be, what its claim
// asks for and what it actually has. It returns the last of the three, together
// with what the claims say about a growth under way or refused (see
// resizeState).
//
// The smallest across the replicas rather than an average: the store is only as
// grown as its least grown replica, and an average would hide one claim stuck at
// the old size behind two that moved.
func (d *Driver) publishVolumeSizes(ctx context.Context, store *backend.Store, want resource.Quantity, replicas int32) (have int64, detail, refused string) {
	var requested, capacity int64
	seen := false
	for ord := int32(0); ord < replicas; ord++ {
		pvc := &corev1.PersistentVolumeClaim{}
		name := fmt.Sprintf("data-%s-%d", resourceName(store), ord)
		if err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: name}, pvc); err != nil {
			// A claim that is not there yet (the store is still being built) is not
			// a claim at size zero, so it is left out rather than counted as the
			// smallest.
			continue
		}
		req := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		got := pvc.Status.Capacity[corev1.ResourceStorage]
		if !seen || req.Value() < requested {
			requested = req.Value()
		}
		if !seen || got.Value() < capacity {
			capacity = got.Value()
		}
		seen = true
		if got.Value() < want.Value() {
			claimDetail, claimRefused := resizeState(pvc)
			if detail == "" {
				detail = claimDetail
			}
			if refused == "" {
				refused = claimRefused
			}
		}
	}
	if !seen {
		return 0, "", ""
	}
	monitoring.StoreVolumes(store.Name, want.Value(), requested, capacity)
	return capacity, detail, refused
}

// growClaim raises one claim's request to want, and reports whether it changed
// anything. A claim that is missing (the store is not built yet) or already at or
// above want is left alone.
func (d *Driver) growClaim(ctx context.Context, name string, want resource.Quantity) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: name}, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if current.Cmp(want) >= 0 {
			return nil
		}
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = want
		if err := d.client.Update(ctx, pvc); err != nil {
			return err
		}
		changed = true
		return nil
	})
	return changed, err
}

// dropStatefulSetKeepingPods deletes the workload and leaves its pods running, so
// it can be created again with a volumeClaimTemplate the API server would refuse
// as an update.
//
// Orphan propagation is the whole trick, and it is the reason this is not simply
// a delete: the pods are the data plane, and taking them down to change a number
// in a template would be an outage of the platform's object storage for the
// duration of a full StatefulSet rollout. The re-created workload adopts them by
// label selector on its first reconcile.
func (d *Driver) dropStatefulSetKeepingPods(ctx context.Context, store *backend.Store) error {
	sts := &appsv1.StatefulSet{}
	err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: resourceName(store)}, sts)
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	}
	if sts.DeletionTimestamp != nil {
		return nil
	}
	return d.client.Delete(ctx, sts, client.PropagationPolicy(metav1.DeletePropagationOrphan))
}

// statefulSetVolumeSize is the size the workload's claim template asks for, or
// the zero quantity when there is no workload (or no template, which cannot
// happen for one this module built).
func (d *Driver) statefulSetVolumeSize(ctx context.Context, store *backend.Store) (resource.Quantity, error) {
	sts := &appsv1.StatefulSet{}
	err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: resourceName(store)}, sts)
	switch {
	case apierrors.IsNotFound(err):
		return resource.Quantity{}, nil
	case err != nil:
		return resource.Quantity{}, err
	}
	for i := range sts.Spec.VolumeClaimTemplates {
		if sts.Spec.VolumeClaimTemplates[i].Name != "data" {
			continue
		}
		return sts.Spec.VolumeClaimTemplates[i].Spec.Resources.Requests[corev1.ResourceStorage], nil
	}
	return resource.Quantity{}, nil
}

// exhaustedFraction is how little free space on a replica's volume counts as
// none. Garage does not slow down as its volume fills: LMDB keeps working until
// the write that does not fit, and from then on the node refuses every write to
// its metadata while still answering as a healthy member of the cluster. Five
// per cent of a 15Gi volume is about what one burst of garbage collection on a
// bloated metadata file takes in a minute, which is the warning there is.
const exhaustedFraction = 0.05

// exhaustedVolumes names the replicas whose data or metadata partition has less
// than exhaustedFraction of it free, or "" when none has.
//
// A full replica does not show anywhere else. It stays up, in the layout and
// ready; the cluster health counts it among the storage nodes that are fine. But
// a key, a bucket or an object written by the others never reaches it, and the
// requests it answers fail on what it does not have: the S3 API looks an access
// key up in the node's own copy of the key table, so a key the endpoint has just
// issued is unknown to it ("No such key").
func exhaustedVolumes(nodes []clusterNode) string {
	var full []string
	for _, n := range nodes {
		if len(n.Role) == 0 || string(n.Role) == "null" || !n.IsUp {
			continue
		}
		name := n.ID
		if len(name) > 16 {
			name = name[:16]
		}
		if n.Hostname != nil && *n.Hostname != "" {
			name = *n.Hostname
		}
		// On one volume the two partitions are the same file system and report the
		// same numbers; the lower of the two is what matters either way.
		var worst *partitionSpace
		for _, p := range []*partitionSpace{n.DataPartition, n.MetadataPartition} {
			if p == nil || p.Total == 0 {
				continue
			}
			if worst == nil || float64(p.Available)/float64(p.Total) < float64(worst.Available)/float64(worst.Total) {
				worst = p
			}
		}
		if worst == nil || float64(worst.Available) >= exhaustedFraction*float64(worst.Total) {
			continue
		}
		full = append(full, fmt.Sprintf("%s has %s of %s free",
			name,
			resource.NewQuantity(int64(worst.Available), resource.BinarySI).String(),
			resource.NewQuantity(int64(worst.Total), resource.BinarySI).String()))
	}
	if len(full) == 0 {
		return ""
	}
	return fmt.Sprintf("the volume of %s; Garage cannot write there, and requests that reach the replica fail",
		strings.Join(full, ", "))
}
