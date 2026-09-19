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

package openapischema

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/itchyny/gojq"
	"sigs.k8s.io/yaml"
)

// The conversions are what keeps an existing ModuleConfig valid when the schema
// changes under it, and they are the one piece of the module nothing else
// exercises: they run inside Deckhouse, on an object written before the release
// that reads it. A conversion that is wrong is not a failed reconcile — it is a
// module that cannot be configured at all, which this module has managed before
// (see the comment in validations_test.go).
//
// So they are run here, against the same jq the ModuleRelease controller uses,
// over pairs of files under testdata: <from>-<case>.yaml in, <to>-<case>.yaml
// expected.
const conversionsDir = "../../../openapi/conversions"

type conversionFile struct {
	Version     int      `json:"version"`
	Conversions []string `json:"conversions"`
	Description struct {
		RU string `json:"ru"`
		EN string `json:"en"`
	} `json:"description"`
}

// TestConversions runs the whole chain, from the version a ModuleConfig is on up
// to the one the schema declares, exactly as Deckhouse does — one conversion is
// never applied alone in a cluster, and a field that one version leaves behind
// for the next to remove is only correct if the next one is also run.
func TestConversions(t *testing.T) {
	cases := []struct {
		name string
		from int
		in   string
		want string
	}{
		{
			// Everything the old section carried: the mode has to survive the move,
			// the acknowledgement string and the volume size have nowhere to go.
			name: "a store that was switched to a single replica, all the way to today",
			from: 1, in: "v1-1.yaml", want: "v2-1.yaml",
		},
		{
			// singleReplica: false is deliberately not turned into
			// highAvailability: true. The mode has to keep being derived from the
			// cluster, and a conversion that pinned it would decide for every
			// installation that never chose.
			name: "a default installation keeps choosing nothing",
			from: 1, in: "v1-2.yaml", want: "v2-2.yaml",
		},
		{
			name: "settings already on the new shape are left alone",
			from: 2, in: "v2-3.yaml", want: "v3-3.yaml",
		},
	}

	target := configVersion(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := readValues(t, tc.in)
			for v := tc.from + 1; v <= target; v++ {
				conv := readConversion(t, fmt.Sprintf("v%d.yaml", v))
				for i, expr := range conv.Conversions {
					got = applyJQ(t, expr, got, i)
				}
			}
			want := readValues(t, tc.want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("converted to:\n  %#v\nwant:\n  %#v", got, want)
			}
		})
	}
}

// TestConversionsAreContinuous: the files have to form an unbroken chain from
// version 2 up to the version the schema declares. A gap is not caught by
// anything at deploy time — Deckhouse applies the versions it finds, and a
// ModuleConfig on the missing version simply fails validation with nothing to
// convert it.
func TestConversionsAreContinuous(t *testing.T) {
	entries, err := os.ReadDir(conversionsDir)
	if err != nil {
		t.Fatalf("read %s: %v", conversionsDir, err)
	}
	seen := map[int]bool{}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
			continue
		}
		conv := readConversion(t, e.Name())
		if conv.Version == 0 {
			t.Errorf("%s declares no version", e.Name())
			continue
		}
		if len(conv.Conversions) == 0 {
			t.Errorf("%s has no conversions", e.Name())
		}
		if conv.Description.RU == "" || conv.Description.EN == "" {
			t.Errorf("%s has no description in one of the locales: this is what an operator is shown", e.Name())
		}
		if seen[conv.Version] {
			t.Errorf("two conversions claim version %d", conv.Version)
		}
		seen[conv.Version] = true
	}

	target := configVersion(t)
	for v := 2; v <= target; v++ {
		if !seen[v] {
			t.Errorf("no conversion to version %d, and config-values.yaml declares %d: "+
				"a ModuleConfig on version %d has nothing to move it forward", v, target, v-1)
		}
	}
}

func readConversion(t *testing.T, name string) conversionFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(conversionsDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var out conversionFile
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return out
}

func readValues(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(conversionsDir, "testdata", name))
	if err != nil {
		t.Fatalf("read testdata/%s: %v", name, err)
	}
	out := map[string]any{}
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse testdata/%s: %v", name, err)
	}
	return out
}

// applyJQ runs one conversion expression the way the ModuleRelease controller
// does: one input, exactly one output.
func applyJQ(t *testing.T, expr string, in map[string]any, i int) map[string]any {
	t.Helper()
	query, err := gojq.Parse(expr)
	if err != nil {
		t.Fatalf("conversion %d does not parse as jq: %v\n  %s", i, err, expr)
	}
	iter := query.Run(in)
	v, ok := iter.Next()
	if !ok {
		t.Fatalf("conversion %d produced nothing", i)
	}
	if err, isErr := v.(error); isErr {
		t.Fatalf("conversion %d failed: %v", i, err)
	}
	if _, more := iter.Next(); more {
		t.Fatalf("conversion %d produced more than one result; Deckhouse takes the first and the rest are silently lost", i)
	}
	out, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("conversion %d produced %T, want an object", i, v)
	}
	return out
}

// configVersion is the x-config-version the settings schema declares.
func configVersion(t *testing.T) int {
	t.Helper()
	raw, err := os.ReadFile("../../../openapi/config-values.yaml")
	if err != nil {
		t.Fatalf("read config-values.yaml: %v", err)
	}
	var schema struct {
		Version int `json:"x-config-version"`
	}
	if err := yaml.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse config-values.yaml: %v", err)
	}
	if schema.Version == 0 {
		t.Fatal("config-values.yaml declares no x-config-version")
	}
	return schema.Version
}
