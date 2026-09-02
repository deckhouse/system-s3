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

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
)

// A rotation replaces a key, and the replaced one may only be revoked once the
// replacement is durable. These two decisions are what order that: one says
// "remember this key for later", the other says "it is safe to revoke now".
//
// Both can be wrong in a way that costs data access rather than tidiness.
// Recording a key that is still in use schedules the consumer's own credentials
// for revocation; failing to record one leaves a usable key on a platform
// bucket that nothing will ever clean up.

func TestOnlyARealRotationRecordsASupersededKey(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recorded  string
		newKey    string
		newSecret string
		want      string
	}{
		{
			// The case the mechanism is for: a fresh key displaced the recorded one.
			name: "a fresh key replaced the recorded one", recorded: "K1", newKey: "K2", newSecret: "s3cret", want: "K1",
		},
		{
			// No secret came back, so nothing was minted: the driver reused the key
			// it was given. Recording K1 here would revoke the key in use.
			name: "the existing key was reused", recorded: "K1", newKey: "K1", newSecret: "", want: "",
		},
		{
			// First provisioning: there is nothing to supersede.
			name: "first key ever issued", recorded: "", newKey: "K1", newSecret: "s3cret", want: "",
		},
		{
			// A secret came back for the same key id. Whatever that is, it is not a
			// rotation, and revoking that id would take away what was just written.
			name: "a secret came back for the same key", recorded: "K1", newKey: "K1", newSecret: "s3cret", want: "",
		},
		{
			// Defensive: no secret and a different key means the driver reported a
			// key this controller cannot hand to anyone. Not a rotation either.
			name: "a different key with no secret", recorded: "K1", newKey: "K2", newSecret: "", want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := supersededAfterRotation(tc.recorded, tc.newKey, tc.newSecret); got != tc.want {
				t.Fatalf("supersededAfterRotation(%q,%q,secret=%v) = %q, want %q",
					tc.recorded, tc.newKey, tc.newSecret != "", got, tc.want)
			}
		})
	}
}

func TestTheKeyInUseIsNeverRevoked(t *testing.T) {
	withStatus := func(superseded, inUse string) *v1alpha1.SystemBucket {
		return &v1alpha1.SystemBucket{
			Status: &v1alpha1.SystemBucketStatus{
				SupersededAccessKeyID: superseded,
				AccessKeyID:           inUse,
			},
		}
	}

	for _, tc := range []struct {
		name       string
		superseded string
		inUse      string
		want       string
	}{
		{"a recorded key that is not in use", "K1", "K2", "K1"},
		{"nothing recorded", "", "K2", ""},
		{
			// The guard. A status that names the same key in both places must not
			// lead to revoking the credentials the consumer is holding.
			name: "the recorded key is the one in use", superseded: "K2", inUse: "K2", want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := revokeCandidate(withStatus(tc.superseded, tc.inUse), tc.inUse); got != tc.want {
				t.Fatalf("revokeCandidate(superseded=%q, inUse=%q) = %q, want %q",
					tc.superseded, tc.inUse, got, tc.want)
			}
		})
	}
}

// A resource with no status at all must not produce a revoke.
func TestNoStatusMeansNothingToRevoke(t *testing.T) {
	if got := revokeCandidate(&v1alpha1.SystemBucket{}, "K1"); got != "" {
		t.Fatalf("revokeCandidate on a statusless bucket = %q, want empty", got)
	}
	if got := supersededKeyOf(&v1alpha1.SystemBucket{}); got != "" {
		t.Fatalf("supersededKeyOf on a statusless bucket = %q, want empty", got)
	}
}

// Every Secret the module reads through the cached client must carry the label
// the cache is scoped to, or the read silently returns NotFound.
//
// This is not a hypothetical: the cache selector and the places that set labels
// are in different files, and adding a Secret without the label would fail only
// at runtime, only on the path that reads it. So the label is one constant and
// this asserts the constant is what the writers use.
func TestTheCacheLabelIsTheOneTheWritersSet(t *testing.T) {
	if v1alpha1.ManagedByLabel != "app.kubernetes.io/managed-by" {
		t.Fatalf("ManagedByLabel = %q; the RBAC comment and the cache selector name this key",
			v1alpha1.ManagedByLabel)
	}
	if v1alpha1.ManagedByValue != "system-s3" {
		t.Fatalf("ManagedByValue = %q; it has to match the module name the data plane is labelled with",
			v1alpha1.ManagedByValue)
	}
}
