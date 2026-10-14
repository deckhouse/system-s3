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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// vpaGVK is the VerticalPodAutoscaler of the platform's vertical-pod-autoscaler
// module. It is handled as unstructured: the module needs four fields of it, and
// the autoscaler's Go module would be a dependency for a type the controller only
// ever writes.
var vpaGVK = schema.GroupVersionKind{Group: "autoscaling.k8s.io", Version: "v1", Kind: "VerticalPodAutoscaler"}

// vpaUpdateMode is "Initial": a recommendation reaches a replica when the pod is
// created, and never by the autoscaler taking a running one away.
//
// Every other mode can end in an eviction, and an eviction here is a storage
// node going down. A single-replica store has nothing to serve from while its
// pod restarts, and a highly available one runs without one of its copies
// until the replica is back. InPlaceOrRecreate, which the platform uses for its
// own controllers, is no exception: it needs Kubernetes 1.33 for the in-place
// half, the platform still supports 1.32, and wherever the resize cannot be done
// in place it falls back to evicting the pod.
//
// The pods are recreated often enough for that to be enough: every rollout of
// the data plane (a new module release, a config change, a change of the volume
// size), every placement move and every node drain creates them again, and each
// time with what the autoscaler has learnt by then.
const vpaUpdateMode = "Initial"

// garageRequests is what the Garage container asks for, with or without the
// autoscaler.
//
// They stay on the pod template when the autoscaler is on. The platform's own
// controllers drop them there, which leaves a pod with no requests at all until
// the autoscaler has a recommendation for it — and the autoscaler is created in
// the same pass as the first pods. A storage node scheduled as best-effort is
// the first thing a node under memory pressure evicts. Kept, they are what the
// pod starts with until a recommendation exists and are overridden after.
//
// They are also the floor of the recommendation (see buildVPA), and not raised
// to what the data plane uses under load. A load test measured up to 2.6 cores
// and 1.7 GiB per replica, and nothing about a cluster that is not under such
// load: a request sized for it would rest unused on a small cluster, roll every
// existing data plane once, and leave a replica that no longer fits its node
// Pending. Sizing to the actual load is what the autoscaler is for.
func garageRequests() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100m"),
		corev1.ResourceMemory: resource.MustParse("256Mi"),
	}
}

// terminatorRequests is what the TLS terminator asks for.
func terminatorRequests() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("20m"),
		corev1.ResourceMemory: resource.MustParse("32Mi"),
	}
}

// garageMaxAllowed caps the recommendation for Garage.
//
// The cap is not a limit on what Garage may use — the pod has no limits — but on
// what it reserves, and a request is something a node has to have free. 2 cores
// and 4 GiB still fit a modest 4-core, 8 GiB node next to the platform's own
// pods, and they cover what the load test measured: the memory peak of 1.7 GiB
// with room above it, and most of the 2.6-core CPU peak, of which the
// recommender targets a percentile rather than the maximum anyway. A cap above
// the node is worse than one below the load: the recreated replica would wait
// Pending for a node that does not exist.
func garageMaxAllowed() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("2"),
		corev1.ResourceMemory: resource.MustParse("4Gi"),
	}
}

// terminatorMaxAllowed caps the recommendation for the TLS terminator, which the
// same load test drove to 0.7 core and about 15 MiB.
func terminatorMaxAllowed() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("128Mi"),
	}
}

// buildVPA returns the VerticalPodAutoscaler of one data plane: per plane, like
// the StatefulSet it targets, so a migration's incoming cluster is sized by what
// it does and not by what the serving one did.
//
// RequestsOnly because the containers have no limits for it to scale.
//
// No workload-resource-policy.deckhouse.io label. The platform uses it to say
// that pods run on the masters or on every node, and Garage does neither: it
// runs where the module's placement puts it.
func buildVPA(store *backend.Store, namespace string) *unstructured.Unstructured {
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(vpaGVK)
	vpa.SetName(resourceName(store))
	vpa.SetNamespace(namespace)
	vpa.SetLabels(planeLabels(store))
	vpa.Object["spec"] = map[string]any{
		"targetRef": map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "StatefulSet",
			"name":       resourceName(store),
		},
		"updatePolicy": map[string]any{
			"updateMode": vpaUpdateMode,
		},
		"resourcePolicy": map[string]any{
			"containerPolicies": []any{
				containerPolicy("garage", garageRequests(), garageMaxAllowed()),
				containerPolicy(terminatorContainer, terminatorRequests(), terminatorMaxAllowed()),
			},
		},
	}
	return vpa
}

func containerPolicy(name string, minAllowed, maxAllowed corev1.ResourceList) map[string]any {
	return map[string]any{
		"containerName":    name,
		"controlledValues": "RequestsOnly",
		"minAllowed":       resourceMap(minAllowed),
		"maxAllowed":       resourceMap(maxAllowed),
	}
}

func resourceMap(list corev1.ResourceList) map[string]any {
	out := make(map[string]any, len(list))
	for name, q := range list {
		out[string(name)] = q.String()
	}
	return out
}

// WithVPA tells the driver whether the cluster has the VerticalPodAutoscaler
// API. The chart knows from the list of enabled modules; the controller would
// otherwise have to find out from discovery on every pass.
func (d *Driver) WithVPA(enabled bool) *Driver {
	d.vpaEnabled = enabled
	return d
}

// ensureVPA puts the autoscaler on one data plane, when the cluster has one.
//
// Without it the pods keep the static requests and nothing else changes: the
// pod template is the same either way, so turning the autoscaler module on or
// off does not roll the data plane.
func (d *Driver) ensureVPA(ctx context.Context, store *backend.Store) error {
	if !d.vpaEnabled {
		return nil
	}
	if err := d.apply(ctx, buildVPA(store, d.namespace)); err != nil {
		return fmt.Errorf("ensure the vertical pod autoscaler: %w", err)
	}
	return nil
}

// removeVPA deletes the autoscaler of a retired data plane. An API that is gone
// took the object with it, so a missing kind is as good as a missing object.
func (d *Driver) removeVPA(ctx context.Context, plane *backend.Store) error {
	if !d.vpaEnabled {
		return nil
	}
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(vpaGVK)
	vpa.SetName(resourceName(plane))
	vpa.SetNamespace(d.namespace)
	if err := d.client.Delete(ctx, vpa); err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return fmt.Errorf("remove the vertical pod autoscaler of the retired cluster: %w", err)
	}
	return nil
}
