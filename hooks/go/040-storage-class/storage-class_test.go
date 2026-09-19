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

package hooks_storageclass

import "testing"

// The resolution order is the whole hook, and getting it backwards is not
// something an installation would report clearly: the store would come up on a
// class nobody chose, which only shows up as a surprise when the volume fills or
// the node it is pinned to goes away.
func TestResolveStorageClass(t *testing.T) {
	sc := func(name string, isDefault bool) filteredStorageClass {
		return filteredStorageClass{Name: name, IsDefault: isDefault}
	}
	ptr := func(s string) *string { return &s }

	cases := []struct {
		name    string
		classes []filteredStorageClass
		global  *string
		module  *string
		want    string
	}{
		{
			name: "nothing at all: no class, and the caller has to say so",
			want: "",
		},
		{
			name:    "no default class and no settings: still nothing",
			classes: []filteredStorageClass{sc("ceph-csi-rbd", false), sc("localpath", false)},
			want:    "",
		},
		{
			name:    "the cluster default is the floor",
			classes: []filteredStorageClass{sc("ceph-csi-rbd", false), sc("localpath", true)},
			want:    "localpath",
		},
		{
			name:    "the platform-wide setting outranks the default",
			classes: []filteredStorageClass{sc("localpath", true)},
			global:  ptr("ceph-csi-rbd"),
			want:    "ceph-csi-rbd",
		},
		{
			name:    "the module's own setting outranks both",
			classes: []filteredStorageClass{sc("localpath", true)},
			global:  ptr("ceph-csi-rbd"),
			module:  ptr("sds-local-volume-lvm"),
			want:    "sds-local-volume-lvm",
		},
		{
			name:   "the module's setting alone is enough",
			module: ptr("sds-local-volume-lvm"),
			want:   "sds-local-volume-lvm",
		},
		{
			// A value that is set wins even when it is empty: "set to nothing" is an
			// operator statement, and turning it back into the default class would be
			// the hook overriding them.
			name:    "an explicitly empty setting is not a fallback to the default",
			classes: []filteredStorageClass{sc("localpath", true)},
			module:  ptr(""),
			want:    "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveStorageClass(tc.classes, tc.global, tc.module); got != tc.want {
				t.Errorf("resolveStorageClass() = %q, want %q", got, tc.want)
			}
		})
	}
}
