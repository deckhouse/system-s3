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
	"os"
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

// TestRecreateConfirmationRule pins what the singleReplica guard actually does,
// case by case.
//
// The evaluation here mirrors the Deckhouse harness
// (deckhouse-controller/internal/packages/values/schema/cel): self and oldSelf
// are dynamic values, a rule that mentions oldSelf is a transition rule and is
// skipped when there is no previous value, and the rule lives at the root of the
// settings — which is what makes the "storage was not mentioned before" case fire
// rather than being skipped.
func TestRecreateConfirmationRule(t *testing.T) {
	const ack = "i-understand-this-destroys-all-system-object-data"

	doc := loadSchema(t, schemaFiles[0])
	rules := collectRules(t, schemaFiles[0], doc)
	var expr string
	for _, r := range rules {
		if r.where == "(root)" {
			expr = r.expression
		}
	}
	if expr == "" {
		t.Fatal("no root-level validation rule in config-values.yaml: the singleReplica guard has to be at the " +
			"root, or it is skipped for a ModuleConfig that never mentioned storage")
	}

	env := celEnv(t)
	ast, issues := env.Compile(expr)
	if issues.Err() != nil {
		t.Fatalf("compile: %v", issues.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("program: %v", err)
	}

	storage := func(kv map[string]any) map[string]any { return map[string]any{"storage": kv} }

	cases := []struct {
		name string
		self map[string]any
		old  map[string]any
		// want is the verdict: true = the change is allowed.
		// A nil old means a create, where the rule is skipped entirely.
		skipped bool
		want    bool
	}{
		{name: "create", self: storage(map[string]any{"singleReplica": true}), old: nil, skipped: true},
		{name: "no change", self: storage(map[string]any{"singleReplica": false}), old: storage(map[string]any{"singleReplica": false}), want: true},
		{name: "unrelated edit", self: storage(map[string]any{"singleReplica": true, "sizePerNode": "20Gi"}), old: storage(map[string]any{"singleReplica": true, "sizePerNode": "10Gi"}), want: true},
		{name: "flip on without the acknowledgement", self: storage(map[string]any{"singleReplica": true}), old: storage(map[string]any{"singleReplica": false}), want: false},
		{name: "flip on with the acknowledgement", self: storage(map[string]any{"singleReplica": true, "recreateConfirmation": ack}), old: storage(map[string]any{"singleReplica": false}), want: true},
		{name: "flip off without the acknowledgement", self: storage(map[string]any{"singleReplica": false}), old: storage(map[string]any{"singleReplica": true}), want: false},
		// Removing the flag is flipping it back: the default is false, and the
		// rebuild it causes is just as destructive.
		{name: "flip off by omission", self: storage(map[string]any{"sizePerNode": "10Gi"}), old: storage(map[string]any{"singleReplica": true}), want: false},
		// The case a rule attached to `storage` silently skipped: a running
		// installation whose ModuleConfig never mentioned storage at all.
		{name: "storage settings added to an existing config", self: storage(map[string]any{"singleReplica": true}), old: map[string]any{"logLevel": "INFO"}, want: false},
		{name: "wrong acknowledgement text", self: storage(map[string]any{"singleReplica": true, "recreateConfirmation": "yes"}), old: storage(map[string]any{"singleReplica": false}), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.old == nil {
				// Deckhouse skips transition rules with no previous value; asserting
				// that the rule mentions oldSelf is asserting that it is one.
				if !usesOldSelf(expr) {
					t.Fatal("the rule does not mention oldSelf, so it would also fire on create, " +
						"where there is nothing to destroy")
				}
				return
			}
			out, _, err := prg.Eval(map[string]any{"self": tc.self, "oldSelf": tc.old})
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			got, ok := out.Value().(bool)
			if !ok {
				t.Fatalf("rule returned %T, want bool", out.Value())
			}
			if got != tc.want {
				t.Errorf("allowed=%v, want %v", got, tc.want)
			}
		})
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
