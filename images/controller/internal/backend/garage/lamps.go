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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// The conditions of Module/system-s3 say what the store runs as, not only that
// it agrees with the settings.
//
// "the replica count matches the configured mode" and "replica placement matches
// the eligible nodes" are true and answer nothing an operator asks: how many
// replicas, in which mode and why that mode, on which StorageClass, on which
// nodes, and which replica is the one that is not ready. Those answers were in
// the controller's metrics and nowhere in `kubectl get module system-s3`, so the
// module read as a black box whose lamps only said "fine". The messages below
// carry them.

// planeReplica is one Garage pod of a plane, as the conditions report it.
type planeReplica struct {
	name  string
	node  string
	ready bool
}

// planeReplicas lists the pods of the plane store describes, by name.
func (d *Driver) planeReplicas(ctx context.Context, store *backend.Store) ([]planeReplica, error) {
	pods := &corev1.PodList{}
	if err := d.apiReader.List(ctx, pods,
		client.InNamespace(d.namespace),
		client.MatchingLabels(commonLabels(store)),
	); err != nil {
		return nil, fmt.Errorf("list the data plane pods: %w", err)
	}
	var replicas []planeReplica
	for i := range pods.Items {
		pod := &pods.Items[i]
		if incarnationOfPod(pod) != store.Incarnation {
			continue
		}
		replicas = append(replicas, planeReplica{
			name:  pod.Name,
			node:  pod.Spec.NodeName,
			ready: pod.DeletionTimestamp == nil && podReady(pod),
		})
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].name < replicas[j].name })
	return replicas, nil
}

// placedMessage is PlacementReady=True/Placed: where the replicas run.
func placedMessage(replicas []planeReplica) string {
	var nodes []string
	for _, r := range replicas {
		if r.node != "" {
			nodes = append(nodes, r.node)
		}
	}
	switch len(nodes) {
	case 0:
		return "replica placement matches the eligible nodes"
	case 1:
		return "the replica runs on node " + nodes[0]
	default:
		return "one replica per node: " + strings.Join(nodes, ", ")
	}
}

// notReadyNote names the replicas that are not ready, or is "" when all are.
func notReadyNote(replicas []planeReplica) string {
	var names []string
	for _, r := range replicas {
		if r.ready {
			continue
		}
		where := "not scheduled"
		if r.node != "" {
			where = "on " + r.node
		}
		names = append(names, fmt.Sprintf("%s (%s)", r.name, where))
	}
	if len(names) == 0 {
		return ""
	}
	return "not ready: " + strings.Join(names, ", ")
}

// describeSettledShape rewrites the messages of a ReplicationReady and a
// MigrationReady that report nothing pending, so that they say what the store
// settled on: the replica count and the mode it follows, and the StorageClass
// its volumes are on. A condition that reports an operation keeps its message.
func describeSettledShape(state *backend.StoreState, rf int32, explicit bool, boundClass string) {
	for _, c := range state.Conditions {
		switch {
		case c.Type == backend.ReplicationReady && c.Status == metav1.ConditionTrue && c.Reason == "Stable":
			state.SetCondition(backend.ReplicationReady, c.Status, c.Reason, replicationMessage(rf, explicit))
		case c.Type == backend.MigrationReady && c.Status == metav1.ConditionTrue && c.Reason == "Converged" && boundClass != "":
			state.SetCondition(backend.MigrationReady, c.Status, c.Reason,
				fmt.Sprintf("the volumes are on StorageClass %s, the configured one; no migration is pending", boundClass))
		}
	}
}

// replicationMessage says how many replicas the store keeps and where the mode
// comes from: a mode that was set is the operator's, a mode that was not follows
// the number of control-plane nodes and can change with it.
func replicationMessage(rf int32, explicit bool) string {
	mode := "high availability on"
	if rf < replicasHA {
		mode = "high availability off"
	}
	source := "the highly available mode follows the number of control-plane nodes"
	if explicit {
		source = "the highly available mode is set in the module or global settings"
	}
	noun := "replicas"
	if rf == 1 {
		noun = "replica"
	}
	return fmt.Sprintf("%d %s, %s; %s", rf, noun, mode, source)
}
