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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// The data plane is built for its first consumer, not for the module.
//
// The module is enabled on every cluster and most of them never hold a
// SystemBucket, and a Garage cluster nobody uses still costs its volumes, its
// pods and a share of the nodes it runs on. So nothing of the data plane exists
// until a SystemBucket asks for storage, and once it does, the data plane stays:
// deleting the last SystemBucket deletes that bucket, not the cluster the next
// one will be created in. A store that comes and goes with its consumers would
// rebuild Garage, its layout and its certificates every time a component is
// reinstalled.
//
// What is held back is exactly what the controller creates for a plane — the
// Secrets, the ConfigMap, the Services, the StatefulSet and its claims. What the
// chart renders (the controller, the credentials endpoint, the CA) is there from
// the start: it does not cost storage, and the credentials endpoint has to be
// reachable to say that a bucket does not exist.

// NoDataPlaneReason is the reason every store condition carries while the store
// waits for its first SystemBucket.
const NoDataPlaneReason = "NoDataPlane"

// WaitingForFirstBucketReason is StorageReady's reason in that state.
const WaitingForFirstBucketReason = "WaitingForFirstBucket"

// awaitingFirstBucket reports whether the data plane is still to be built:
// nothing of a data plane in the namespace, and no SystemBucket that is not on
// its way out.
//
// The data plane is asked first because it is the answer for good once it
// exists: a store whose last SystemBucket was deleted keeps its data plane, so
// the buckets only matter while there is none. It is asked for anything of it
// rather than for its StatefulSet alone: claims left behind by a StatefulSet
// somebody deleted, or the ConfigMap that pins the replication factor and
// records a migration, are a store with data in it, and reading them as "never
// built" would leave that data unserved until somebody created a bucket.
func (d *Driver) awaitingFirstBucket(ctx context.Context, store *backend.Store) (bool, error) {
	built, err := d.dataPlaneBuilt(ctx, store)
	if err != nil || built {
		return false, err
	}
	buckets := &v1alpha1.SystemBucketList{}
	if err := d.client.List(ctx, buckets); err != nil {
		return false, fmt.Errorf("list the SystemBuckets: %w", err)
	}
	for i := range buckets.Items {
		if buckets.Items[i].DeletionTimestamp == nil {
			return false, nil
		}
	}
	return true, nil
}

// dataPlaneBuilt reports whether anything of any plane of the store exists: a
// StatefulSet (scaled to zero by a disabled module, too), a claim, or one of the
// store's ConfigMaps — the pinned replication factor of a plane, the migration
// state of a move or an in-place resize, the bucket owners.
//
// Read past the cache: the cache is not what holds these objects for the
// controller, and a wrong "no" here is a store built a second time on top of
// the first.
func (d *Driver) dataPlaneBuilt(ctx context.Context, store *backend.Store) (bool, error) {
	selector := []client.ListOption{client.InNamespace(d.namespace), client.MatchingLabels(commonLabels(store)), client.Limit(1)}
	for _, probe := range []struct {
		what string
		list client.ObjectList
		len  func(client.ObjectList) int
	}{
		{"StatefulSets", &appsv1.StatefulSetList{}, func(l client.ObjectList) int { return len(l.(*appsv1.StatefulSetList).Items) }},
		{"PersistentVolumeClaims", &corev1.PersistentVolumeClaimList{}, func(l client.ObjectList) int { return len(l.(*corev1.PersistentVolumeClaimList).Items) }},
		{"ConfigMaps", &corev1.ConfigMapList{}, func(l client.ObjectList) int { return len(l.(*corev1.ConfigMapList).Items) }},
	} {
		if err := d.apiReader.List(ctx, probe.list, selector...); err != nil {
			return false, fmt.Errorf("list the data plane %s: %w", probe.what, err)
		}
		if probe.len(probe.list) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// noDataPlaneConditions is what the store says while it waits for its first
// SystemBucket.
//
// StorageReady is False — there is no S3 to reach — under a reason of its own, so
// that it is not read as a store that is failing. The other conditions are True:
// a placement, a capacity, a replication factor and a migration are properties of
// a data plane, and there is none to have a problem with. Unknown would say the
// pass could not look, which is not the case.
//
// A setting that would stop the first bucket from being served is said now
// rather than when somebody waits on it.
func noDataPlaneConditions(state *backend.StoreState, store *backend.Store) {
	msg := "no SystemBucket asks for the system object storage yet; the data plane is built when the first one is created"
	if store.StorageClass == "" {
		msg = appendNote(msg, "no StorageClass is configured for it either: set storageClass in the module settings, "+
			"or global.modules.storageClass, or mark a StorageClass as the cluster default")
	}
	state.AwaitingFirstBucket = true
	state.Message = msg
	state.SetCondition(backend.StorageReady, metav1.ConditionFalse, WaitingForFirstBucketReason, msg)
	for _, kind := range []string{backend.PlacementReady, backend.CapacityReady, backend.ReplicationReady, backend.MigrationReady} {
		state.SetCondition(kind, metav1.ConditionTrue, NoDataPlaneReason, "the data plane is not built yet")
	}
}
