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
	"maps"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Placement is which nodes the data plane may run on: a node selector and the
// taints it tolerates.
//
// The chart decides it, with the platform's "system" strategy: the module's own
// node role (node-role.deckhouse.io/system-s3) when a cluster has nodes with it,
// the system role when it has those instead, and the masters otherwise. The
// first two steps are the rule every Deckhouse component placed on system nodes
// follows, and an operator who wants the store somewhere in particular does it
// the way they do it for everything else — by giving a NodeGroup the role.
//
// The masters are the last step, not the first. They used to be the only place
// the data plane ran, and that sized the control plane for object storage; a
// dedicated role puts the store on nodes an operator provisioned for it,
// including their disks. Without a role, though, the masters are the one set of
// nodes every cluster has and keeps: the worker nodes a NodeGroup grows, shrinks
// and replaces for other workloads would cost a replica its copy on a local
// class each time, so the store falls back to the masters and tolerates their
// taints rather than to any node.
//
// The controller does not work the rule out itself: the chart has what it needs
// (the per-role node counts from discovery) and renders the result, so the
// driver only has to apply it. A zero Placement — no selector, no tolerations —
// is "any node without a taint"; the chart does not render it.
type Placement struct {
	NodeSelector map[string]string
	Tolerations  []corev1.Toleration
}

// WithPlacement tells the driver where the data plane may run.
func (d *Driver) WithPlacement(p Placement) *Driver {
	d.placement = p
	return d
}

// applyTo puts the placement on a pod spec. The spec keeps whatever affinity it
// has: the data plane's spreading rules live there and are the StatefulSet's
// business, not the placement's.
func (p Placement) applyTo(spec *corev1.PodSpec) {
	spec.NodeSelector = maps.Clone(p.NodeSelector)
	spec.Tolerations = slices.Clone(p.Tolerations)
}

// withPlacement puts the placement on the data plane's pod template.
func withPlacement(sts *appsv1.StatefulSet, p Placement) {
	p.applyTo(&sts.Spec.Template.Spec)
}

// admits reports whether the scheduler could put a data-plane pod on the node,
// as far as this placement decides it: the selector matches the node's labels,
// every taint that keeps pods off is tolerated, and the node is not cordoned.
//
// It has to agree with the scheduler. The placement reconcile spreads replicas
// across the nodes this admits, and a node it counts that the scheduler would
// refuse is a node a replica is moved towards and never lands on.
func (p Placement) admits(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	if !labels.SelectorFromSet(p.NodeSelector).Matches(labels.Set(node.Labels)) {
		return false
	}
	for i := range node.Spec.Taints {
		taint := &node.Spec.Taints[i]
		if taint.Effect == corev1.TaintEffectPreferNoSchedule {
			continue
		}
		if !slices.ContainsFunc(p.Tolerations, func(t corev1.Toleration) bool { return t.ToleratesTaint(taint) }) {
			return false
		}
	}
	return true
}
