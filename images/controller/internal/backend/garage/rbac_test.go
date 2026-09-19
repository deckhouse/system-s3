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
	"os"
	"regexp"
	"strings"
	"testing"
)

// rbacTemplate is the chart's RBAC for the controller, read from the repository
// the package lives in.
const rbacTemplate = "../../../../../templates/controller/rbac-for-us.yaml"

var applyCase = regexp.MustCompile(`case \*\w+\.(\w+):`)

// TestEveryAppliedTypeIsReadableClusterWide is the guard on a coupling that has
// no compiler behind it: apply() goes through the manager's CACHED client, and
// the cache serves a type by starting an informer that LISTS it across the whole
// cluster.
//
// So the moment a type is added to apply(), the controller needs get/list/watch
// on it at cluster scope — in clusterRoleRules, not only the namespaced
// roleRules that cover the writes. Without it the informer is refused, the cache
// for that type never syncs, and the reconcile that touched it blocks on a cache
// that will never be ready: no crash, no condition, a controller that simply
// stops. That is how a migration spent twenty-five minutes doing nothing while
// the log repeated "poddisruptionbudgets.policy is forbidden".
func TestEveryAppliedTypeIsReadableClusterWide(t *testing.T) {
	src, err := os.ReadFile("garage.go")
	if err != nil {
		t.Fatalf("read garage.go: %v", err)
	}
	body := string(src)
	start := strings.Index(body, "func mergeDesired(")
	if start < 0 {
		t.Fatal("mergeDesired is gone; this guard needs rewriting")
	}
	end := strings.Index(body[start:], "\nfunc ")
	if end < 0 {
		end = len(body) - start
	}

	rules, err := os.ReadFile(rbacTemplate)
	if err != nil {
		t.Fatalf("read %s: %v", rbacTemplate, err)
	}
	clusterRules := string(rules)
	if i := strings.Index(clusterRules, `"clusterRoleRules"`); i >= 0 {
		clusterRules = clusterRules[i:]
	} else {
		t.Fatalf("%s has no clusterRoleRules; this guard needs rewriting", rbacTemplate)
	}

	matches := applyCase.FindAllStringSubmatch(body[start:start+end], -1)
	if len(matches) == 0 {
		t.Fatal("mergeDesired no longer switches on types; this guard needs rewriting")
	}
	for _, m := range matches {
		// Every type in this switch is a Kubernetes kind whose resource name is
		// the lower-cased plural of it: Secret -> secrets, StatefulSet ->
		// statefulsets, PodDisruptionBudget -> poddisruptionbudgets.
		resource := strings.ToLower(m[1]) + "s"
		if !strings.Contains(clusterRules, `"`+resource+`"`) {
			t.Errorf("apply() writes %s but clusterRoleRules does not allow reading %q cluster-wide; "+
				"the cache informer for it will be refused and the reconcile will block on a cache that never syncs",
				m[1], resource)
		}
	}
}
