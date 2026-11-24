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

// Package openapischema holds tests over the module's OpenAPI schemas. There is
// no code here: the schemas are data, and what needs checking is that Deckhouse
// can actually use them.
package openapischema

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/cel-go/cel"
	"gopkg.in/yaml.v3"
)

// schemaFiles are the module's OpenAPI documents, relative to this package.
var schemaFiles = []string{
	"../../../openapi/config-values.yaml",
	"../../../openapi/values.yaml",
}

// ruleKey is the extension Deckhouse reads validation rules from. The Kubernetes
// spelling (x-kubernetes-validations) is accepted by the YAML and evaluated by
// nobody, so this test also fails a schema that uses it.
const (
	ruleKey    = "x-deckhouse-validations"
	k8sRuleKey = "x-kubernetes-validations"
)

// TestEveryValidationRuleCompiles is the test the module was missing.
//
// Deckhouse compiles every x-deckhouse-validations rule before it applies any
// values patch, so a rule that does not compile does not merely fail to
// validate — it stops the module from configuring itself at all. The module
// shipped `has(oldSelf)`, which is not valid CEL (has() takes a field selection,
// not an identifier), and the result was a module that could not be installed:
// the webhook-certs hook failed with "cannot apply values patch for module
// values" and the ModuleConfig never became ready. Nothing in the repository
// looked at these expressions until a cluster did.
func TestEveryValidationRuleCompiles(t *testing.T) {
	env := celEnv(t)

	for _, path := range schemaFiles {
		doc := loadSchema(t, path)
		rules := collectRules(t, path, doc)
		if len(rules) == 0 {
			t.Logf("%s: no validation rules", path)
			continue
		}
		for _, r := range rules {
			if _, issues := env.Compile(r.expression); issues.Err() != nil {
				t.Errorf("%s (%s): rule does not compile: %v\nexpression:\n%s",
					path, r.where, issues.Err(), r.expression)
			}
			if r.message == "" {
				t.Errorf("%s (%s): rule has no message; a refusal with no explanation is not actionable",
					path, r.where)
			}
		}
		t.Logf("%s: %d rule(s) compiled", path, len(rules))
	}
}

// TestNoTransitionRulesInModuleValues forbids a rule that mentions oldSelf.
//
// This module had one — the singleReplica guard — and it wedged Deckhouse's main
// task queue on a live cluster. A transition rule is evaluated in two places with
// two different notions of "previous":
//
//   - admission compares against the stored ModuleConfig, so an edit that passes
//     there is accepted and stored;
//   - the ApplyKubeConfigValues task compares against the last values Deckhouse
//     successfully applied, and it retries forever when the rule refuses.
//
// So a sequence of accepted edits can leave a state the platform cannot apply:
// the task fails on every retry and everything behind it in the main queue stops.
// Measured — the queue held ModuleRun for this module and for upmeter behind a
// task with 38 failures and counting:
//
//	Queue 'main': length 4, status: 'sleep after fail for 32s'
//	 1. ApplyKubeConfigValues ... failures 38: Changing storage.singleReplica ...
//	 3. ModuleRun:main:system-s3:KubeConfig-Changed-Modules
//	 4. ModuleRun:main:upmeter:Schedule-Change-ModuleValues
//
// A guard that can jam the platform is worse than the mistake it prevents. The
// replica-count switch is refused by the driver instead, which reports it on
// Module/system-s3.ReplicationReady and blocks nothing (see TestReplicaSwitchNeedsTheAcknowledgement
// in the garage package).
func TestNoTransitionRulesInModuleValues(t *testing.T) {
	for _, path := range schemaFiles {
		doc := loadSchema(t, path)
		for _, r := range collectRules(t, path, doc) {
			if usesOldSelf(r.expression) {
				t.Errorf("%s (%s): the rule mentions oldSelf, which makes it a transition rule — "+
					"those are re-evaluated by ApplyKubeConfigValues against the last applied values and "+
					"retry forever when they refuse, which stops the whole main queue. Enforce the "+
					"transition in the controller instead.\nexpression:\n%s", path, r.where, r.expression)
			}
		}
	}
}

// celEnv builds the environment Deckhouse evaluates module-values rules in: two
// dynamic variables, nothing else.
func celEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(
		cel.Variable("self", cel.DynType),
		cel.Variable("oldSelf", cel.DynType),
	)
	if err != nil {
		t.Fatalf("build CEL env: %v", err)
	}
	return env
}

func loadSchema(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return doc
}

type foundRule struct {
	where      string
	expression string
	message    string
}

// collectRules walks the schema and returns every validation rule in it, so the
// test covers rules added later at any depth.
func collectRules(t *testing.T, path string, doc map[string]any) []foundRule {
	t.Helper()
	var out []foundRule

	var walk func(node any, where string)
	walk = func(node any, where string) {
		m, ok := node.(map[string]any)
		if !ok {
			return
		}
		if _, bad := m[k8sRuleKey]; bad {
			t.Errorf("%s (%s): uses %s; module values are validated by Deckhouse, which reads %s",
				path, where, k8sRuleKey, ruleKey)
		}
		if raw, found := m[ruleKey]; found {
			list, ok := raw.([]any)
			if !ok {
				t.Errorf("%s (%s): %s is not a list", path, where, ruleKey)
			}
			for _, entry := range list {
				e, ok := entry.(map[string]any)
				if !ok {
					t.Errorf("%s (%s): rule entry is not a mapping", path, where)
					continue
				}
				expr, _ := e["expression"].(string)
				msg, _ := e["message"].(string)
				if expr == "" {
					t.Errorf("%s (%s): rule has no expression", path, where)
					continue
				}
				out = append(out, foundRule{where: where, expression: expr, message: msg})
			}
		}
		props, ok := m["properties"].(map[string]any)
		if !ok {
			return
		}
		for name, sub := range props {
			child := name
			if where != "(root)" {
				child = where + "." + name
			}
			walk(sub, child)
		}
	}
	walk(doc, "(root)")
	return out
}

// usesOldSelf reports whether the expression mentions oldSelf at all, which is
// what makes Deckhouse treat it as a transition rule.
func usesOldSelf(expr string) bool {
	for i := 0; i+7 <= len(expr); i++ {
		if expr[i:i+7] == "oldSelf" {
			return true
		}
	}
	return false
}

// TestRenderedChartKeepsEveryRBACObject is a guard for a trap the chart fell into
// once: a Helm comment closed with "-}}" ate the newline before the "---" that
// follows it, merged two YAML documents into one, and silently dropped the
// module's own ClusterRoleBinding. The module then came up with a controller that
// could not list Secrets or SystemBuckets cluster-wide, and nothing in the
// template looked wrong.
//
// It parses the *rendered* chart, so any future document-separator mistake fails
// here rather than on a cluster. Rendering needs helm and the values the
// addon-operator normally supplies, so the test skips when they are absent — a
// skip says so, and CI has both.
func TestRenderedChartKeepsEveryRBACObject(t *testing.T) {
	out := renderChart(t)

	want := map[string]string{
		// The controller's own permissions, and the binding that grants them.
		"ClusterRole/d8:system-s3:controller":        "",
		"ClusterRoleBinding/d8:system-s3:controller": "d8:system-s3:controller",
		// What lets the controller authorize a metrics scrape.
		"ClusterRoleBinding/d8:system-s3:controller:rbac-proxy": "d8:rbac-proxy",
	}

	found := map[string]string{}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err != nil {
			break
		}
		if doc == nil {
			continue
		}
		kind, _ := doc["kind"].(string)
		meta, _ := doc["metadata"].(map[string]any)
		if meta == nil {
			continue
		}
		name, _ := meta["name"].(string)
		ref := ""
		if rr, ok := doc["roleRef"].(map[string]any); ok {
			ref, _ = rr["name"].(string)
		}
		found[kind+"/"+name] = ref
	}

	for key, wantRef := range want {
		gotRef, ok := found[key]
		if !ok {
			t.Errorf("%s is missing from the rendered chart", key)
			continue
		}
		if wantRef != "" && gotRef != wantRef {
			t.Errorf("%s points at %q, want %q", key, gotRef, wantRef)
		}
	}
}

// TestRenderedSecretsHoldBase64Data: the API server decodes every value under a
// Secret's data as base64 and refuses the whole object otherwise, with "illegal
// base64 data at input byte 0". The certificate hooks keep the PEM itself in the
// values, and so does hack/helm-render-values.yaml.
func TestRenderedSecretsHoldBase64Data(t *testing.T) {
	out := renderChart(t)

	secrets := 0
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc struct {
			Kind     string                `yaml:"kind"`
			Metadata struct{ Name string } `yaml:"metadata"`
			Data     map[string]string     `yaml:"data"`
		}
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode the rendered chart: %v", err)
		}
		if doc.Kind != "Secret" {
			continue
		}
		secrets++
		for key, value := range doc.Data {
			if _, err := base64.StdEncoding.DecodeString(value); err != nil {
				t.Errorf("Secret/%s: data.%s is not base64: %v", doc.Metadata.Name, key, err)
			}
		}
	}
	if secrets == 0 {
		t.Fatal("no Secret in the rendered chart")
	}
}

// renderChart renders the chart with plain helm and the values the
// addon-operator normally supplies. It skips when either is absent — a skip
// says so, and CI has both.
func renderChart(t *testing.T) []byte {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed; the rendered-chart guard needs it")
	}

	chart := filepath.Clean("../../..")
	values := filepath.Join(chart, "hack", "helm-render-values.yaml")
	if _, err := os.Stat(values); err != nil {
		t.Skipf("no %s to render with: %v", values, err)
	}

	out, err := exec.Command(helm, "template", "system-s3", chart, "-f", values).Output()
	if err != nil {
		t.Fatalf("helm template: %v", err)
	}
	return out
}
