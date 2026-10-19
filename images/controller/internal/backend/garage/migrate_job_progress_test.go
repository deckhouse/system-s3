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

import "testing"

func TestCopyProgressNote(t *testing.T) {
	for _, tc := range []struct {
		name string
		diff difference
		want string
	}{
		{"nothing to copy", difference{}, ""},
		{"a third", difference{objects: 200, bytes: 2 << 30, totalObjects: 300, totalBytes: 3 << 30}, "1Gi of 3Gi, 100 of 300 objects (33%)"},
		{"done", difference{totalObjects: 10, totalBytes: 1 << 20}, "1Mi of 1Mi, 10 of 10 objects (100%)"},
		{"empty objects only", difference{objects: 1, totalObjects: 4}, "0 of 0, 3 of 4 objects (75%)"},
		{"the target ahead counts as done, not negative", difference{objects: -5, bytes: -1, totalObjects: 2, totalBytes: 2}, "2 of 2, 2 of 2 objects (100%)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := copyProgressNote(tc.diff); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
