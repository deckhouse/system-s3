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

package controller

import (
	"testing"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/system-s3/api/v1alpha1"
)

// The API module declares the phase vocabulary a consumer compares against; the
// controller writes the values sds-common-lib derives. The two sets have to stay
// identical, and neither module can see the other's constants at compile time —
// api deliberately does not depend on the helper library — so the check lives
// here, where both are in scope.
//
// Without it the duplication is synchronised by hand, and the failure it hides is
// quiet: a consumer waiting for "Ready" against a status that now says something
// else waits forever.
func TestPhaseVocabularyMatchesTheLibrary(t *testing.T) {
	cases := []struct{ api, lib string }{
		{v1alpha1.PhasePending, conditions.PhasePending},
		{v1alpha1.PhaseInProgress, conditions.PhaseInProgress},
		{v1alpha1.PhaseReady, conditions.PhaseReady},
		{v1alpha1.PhaseError, conditions.PhaseError},
	}
	for _, tc := range cases {
		if tc.api != tc.lib {
			t.Errorf("api phase %q != library phase %q", tc.api, tc.lib)
		}
	}
}
