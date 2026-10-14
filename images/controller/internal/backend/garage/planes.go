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
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// DataPlanes reports every Garage cluster the store has, with its applied shape
// and its pods.
//
// It is what the console draws the data plane from, through the controller's
// metrics, instead of reading the StatefulSets and pods of the module namespace
// itself: what a plane is and which one serves are decisions this package makes,
// and a reader that rebuilt them from object names and labels would have to
// keep up with every change to how they are made.
//
// A plane is reported when its StatefulSet or any of its pods exists. A plane
// the migration state names but that has neither has nothing to show yet.
func (d *Driver) DataPlanes(ctx context.Context, store *backend.Store) ([]backend.DataPlane, error) {
	migration, err := d.loadMigrationState(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("read the migration state: %w", err)
	}

	pods := &corev1.PodList{}
	if err := d.apiReader.List(ctx, pods,
		client.InNamespace(d.namespace),
		client.MatchingLabels(commonLabels(store)),
	); err != nil {
		return nil, fmt.Errorf("list the data plane pods: %w", err)
	}
	byPlane := map[int32][]corev1.Pod{}
	for _, pod := range pods.Items {
		n := incarnationOfPod(&pod)
		byPlane[n] = append(byPlane[n], pod)
	}

	incarnations := map[int32]bool{migration.serving: true}
	if migration.other != 0 {
		incarnations[migration.other] = true
	}
	for n := range byPlane {
		incarnations[n] = true
	}

	var planes []backend.DataPlane
	for n := range incarnations {
		ha := store.HighAvailability
		if n == migration.other {
			ha = migration.ha
		}
		plane := planeOf(store, n, ha, store.StorageClass)

		sts := &appsv1.StatefulSet{}
		err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: resourceName(plane)}, sts)
		switch {
		case apierrors.IsNotFound(err):
			sts = nil
		case err != nil:
			return nil, fmt.Errorf("read the statefulset of plane %d: %w", n, err)
		}

		// The pods of a plane are the ones its StatefulSet would claim, by name.
		// Copy jobs carry the store's labels too, and a pod of the first plane
		// that predates the incarnation label reads as the first plane's.
		var members []backend.DataPlanePod
		for i := range byPlane[n] {
			pod := &byPlane[n][i]
			if !isReplicaOf(pod.Name, resourceName(plane)) {
				continue
			}
			members = append(members, backend.DataPlanePod{
				Name: pod.Name, Node: pod.Spec.NodeName, Ready: podReady(pod),
			})
		}
		if sts == nil && len(members) == 0 {
			continue
		}
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })

		pinned, err := d.pinnedState(ctx, plane)
		if err != nil {
			return nil, fmt.Errorf("read the pinned state of plane %d: %w", n, err)
		}
		plane = storeAsPinned(plane, pinned)
		class, err := d.boundStorageClass(ctx, plane)
		if err != nil {
			return nil, fmt.Errorf("read the StorageClass of plane %d: %w", n, err)
		}
		// The first claim goes away for a moment when placement recycles that
		// replica; the template is what the claim is created again from.
		if class == "" && sts != nil && len(sts.Spec.VolumeClaimTemplates) > 0 &&
			sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName != nil {
			class = *sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName
		}

		planes = append(planes, backend.DataPlane{
			Incarnation:      n,
			Serving:          n == migration.serving,
			StorageClass:     class,
			HighAvailability: plane.HighAvailability,
			// The count the pinned mode asks for rather than spec.replicas: an
			// in-place change of the replica count holds the set at zero while it
			// purges the layout, and that is not a shape the store has.
			Replicas: desiredReplicas(plane),
			Pods:     members,
		})
	}
	sort.Slice(planes, func(i, j int) bool { return planes[i].Incarnation < planes[j].Incarnation })
	return planes, nil
}

// isReplicaOf reports whether a pod name is "<set>-<ordinal>".
func isReplicaOf(pod, set string) bool {
	ordinal, ok := strings.CutPrefix(pod, set+"-")
	if !ok || ordinal == "" {
		return false
	}
	for _, r := range ordinal {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
