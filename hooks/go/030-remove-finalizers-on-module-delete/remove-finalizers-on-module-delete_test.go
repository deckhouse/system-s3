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

package hooks_common

import (
	"testing"

	"github.com/deckhouse/system-s3/hooks/go/consts"
)

// The uninstall hook must only ever drop finalizers this module set.
//
// It used to clear the whole list. On the objects it touches there is normally
// nothing else, so nothing was observably broken — but the one object that does
// acquire a foreign finalizer, from a backup operator or a policy controller,
// would have it silently dropped, releasing whatever it was protecting.
func TestOnlyThisModulesFinalizersAreRemoved(t *testing.T) {
	// The controller's own finalizer has to match, or the hook stops doing its job.
	// It lives in another Go module (images/controller), so it is pinned here as a
	// literal: if it is ever renamed, this fails instead of the uninstall.
	const controllerFinalizer = "storage.deckhouse.io/system-s3-controller"
	if !isModuleFinalizer(controllerFinalizer) {
		t.Fatalf("%q does not match %q: the hook would leave SystemBuckets stuck in Terminating",
			controllerFinalizer, consts.ModuleFinalizerPrefix)
	}

	cases := []struct {
		finalizer string
		want      bool
	}{
		{controllerFinalizer, true},
		{consts.ModuleFinalizerPrefix, true},
		{"storage.deckhouse.io/sds-object-controller", false},
		{"storage.deckhouse.io/sds-node-configurator", false},
		{"kubernetes.io/pv-protection", false},
		{"velero.io/backup-protection", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isModuleFinalizer(tc.finalizer); got != tc.want {
			t.Errorf("isModuleFinalizer(%q) = %v, want %v", tc.finalizer, got, tc.want)
		}
	}
}
