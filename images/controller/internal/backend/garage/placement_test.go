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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// rolePlacement is what the chart renders for a cluster with nodes carrying the
// module's own role.
func rolePlacement() Placement {
	return Placement{
		NodeSelector: map[string]string{"node-role.deckhouse.io/system-s3": ""},
		Tolerations: []corev1.Toleration{
			{Key: "dedicated.deckhouse.io", Operator: corev1.TolerationOpEqual, Value: "system-s3"},
			{Key: "dedicated.deckhouse.io", Operator: corev1.TolerationOpEqual, Value: "system"},
		},
	}
}

// TestPlacementReachesTheDataPlane: the StatefulSet carries exactly the
// placement it was given, alongside its own spreading rules — the placement
// decides the nodes, and must not take the anti-affinity away with it.
func TestPlacementReachesTheDataPlane(t *testing.T) {
	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:v1", "hash", testVolumeSize)
	p := rolePlacement()
	withPlacement(sts, p)
	spec := sts.Spec.Template.Spec

	if !maps.Equal(spec.NodeSelector, p.NodeSelector) {
		t.Errorf("nodeSelector=%v, want %v", spec.NodeSelector, p.NodeSelector)
	}
	if !reflect.DeepEqual(spec.Tolerations, p.Tolerations) {
		t.Errorf("tolerations=%+v, want %+v", spec.Tolerations, p.Tolerations)
	}
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		t.Error("the spreading rules are gone")
	}

	// The pod spec holds copies: a later change to one must not show up in the
	// other, or two data planes built from one driver would share a map.
	spec.NodeSelector["changed"] = "yes"
	if _, ok := p.NodeSelector["changed"]; ok {
		t.Error("the pod spec shares its node selector with the placement")
	}
}

// TestCopierRunsWhereTheDataPlaneRuns: the migration copier is given the same
// placement as the data plane, and nothing that would pin it to the masters.
func TestCopierRunsWhereTheDataPlaneRuns(t *testing.T) {
	d := (&Driver{namespace: "d8-system-s3", copierImage: "controller:test", clusterDomain: "cluster.local"}).
		WithPlacement(rolePlacement())
	job := d.buildCopyJob("copy", planeOf(systemStore(), 1, true, localSC),
		planeOf(systemStore(), 2, false, localSC), []string{"a"}, "stamp")
	spec := job.Spec.Template.Spec

	if !maps.Equal(spec.NodeSelector, rolePlacement().NodeSelector) {
		t.Errorf("nodeSelector=%v, want the data plane's", spec.NodeSelector)
	}
	if !reflect.DeepEqual(spec.Tolerations, rolePlacement().Tolerations) {
		t.Errorf("tolerations=%+v, want the data plane's", spec.Tolerations)
	}
	if spec.Affinity != nil {
		t.Errorf("affinity=%+v: the copier has nothing to spread against and no other node rule", spec.Affinity)
	}
}
