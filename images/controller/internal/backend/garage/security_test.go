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
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestOnlyGarageContainersRunAsRoot(t *testing.T) {
	store := systemStore()
	sts := buildStatefulSet(store, "d8-system-s3", "garage:v1", "hash", testVolumeSize)
	withTLSTerminator(sts, "controller:v1", "garage-tls", "system-garage", "system-garage-bucket-owners")
	d := &Driver{namespace: "d8-system-s3", image: "garage:v1"}
	purge := d.buildPurgeJob(store, "purge")

	root := map[string]bool{"garage": true, "restore-node-key": true, "purge": true}
	var containers []corev1.Container
	containers = append(containers, sts.Spec.Template.Spec.InitContainers...)
	containers = append(containers, sts.Spec.Template.Spec.Containers...)
	containers = append(containers, purge.Spec.Template.Spec.Containers...)
	for _, c := range containers {
		sc := c.SecurityContext
		if sc == nil || sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" ||
			len(sc.Capabilities.Add) != 0 {
			t.Errorf("%s: capabilities=%+v, want drop ALL and nothing added", c.Name, sc)
			continue
		}
		if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
			t.Errorf("%s: privilege escalation is allowed", c.Name)
		}
		if sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
			t.Errorf("%s: seccompProfile=%+v, want RuntimeDefault", c.Name, sc.SeccompProfile)
		}
		asRoot := sc.RunAsUser != nil && *sc.RunAsUser == 0
		if asRoot != root[c.Name] {
			t.Errorf("%s: runAsUser=%v, root wanted: %v", c.Name, sc.RunAsUser, root[c.Name])
		}
		if !root[c.Name] && (sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot) {
			t.Errorf("%s: runAsNonRoot is not set", c.Name)
		}
	}

	for name, l := range map[string]map[string]string{
		"statefulset": sts.Spec.Template.Labels,
		"purge job":   purge.Spec.Template.Labels,
	} {
		if l[securityPolicyExceptionLabel] != garageSecurityPolicyException {
			t.Errorf("%s pod labels=%v, want %s=%s", name, l, securityPolicyExceptionLabel, garageSecurityPolicyException)
		}
	}
	for k := range planeLabels(store) {
		if k == securityPolicyExceptionLabel {
			t.Errorf("planeLabels carries %s: every Service, PDB and anti-affinity would select on it", k)
		}
	}
}
