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

package monitoring

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// rulesDir is the module's alert rules, read from the repository this package
// lives in.
const rulesDir = "../../../../monitoring/prometheus-rules"

var (
	metricNameInCode = regexp.MustCompile(`Name:\s*"([a-z_]+)"`)
	metricInExpr     = regexp.MustCompile(`system_s3_[a-z_]+`)
)

type ruleGroup struct {
	Name  string `json:"name"`
	Rules []struct {
		Alert string `json:"alert"`
		Expr  string `json:"expr"`
		For   string `json:"for"`
	} `json:"rules"`
}

// metricSources are the files the module's own metrics are declared in: the
// controller's, and the TLS terminator's in the Garage pods.
var metricSources = []string{"monitoring.go", "../tlsproxy/metrics.go"}

// publishedMetrics is every system_s3_* metric declared in metricSources.
func publishedMetrics(t *testing.T) map[string]bool {
	t.Helper()
	published := map[string]bool{}
	for _, f := range metricSources {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		found := metricNameInCode.FindAllStringSubmatch(string(src), -1)
		if len(found) == 0 {
			t.Fatalf("no metrics found in %s; this guard needs rewriting", f)
		}
		for _, m := range found {
			published["system_s3_"+m[1]] = true
		}
	}
	return published
}

// TestEveryAlertIsAboutAMetricThatExists is the guard on a coupling nothing else
// checks: an alert names a metric as a string, and a metric renamed or removed in
// this file leaves the rule syntactically perfect and permanently silent. A rule
// that never fires looks exactly like a system that never fails.
func TestEveryAlertIsAboutAMetricThatExists(t *testing.T) {
	published := publishedMetrics(t)

	files, err := filepath.Glob(filepath.Join(rulesDir, "*.yaml"))
	if err != nil {
		t.Fatalf("glob the rules: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no alert rules in %s; this guard needs rewriting", rulesDir)
	}

	alerts := 0
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var groups []ruleGroup
		if err := yaml.Unmarshal(raw, &groups); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, g := range groups {
			for _, r := range g.Rules {
				alerts++
				if r.Alert == "" {
					t.Errorf("%s: a rule in group %q has no alert name", filepath.Base(f), g.Name)
				}
				// Deckhouse names every module alert this way, and the name is what an
				// operator searches for when one fires.
				if !strings.HasPrefix(r.Alert, "D8SystemS3") {
					t.Errorf("%s: alert %q does not start with D8SystemS3", filepath.Base(f), r.Alert)
				}
				for _, metric := range metricInExpr.FindAllString(r.Expr, -1) {
					if !published[metric] {
						t.Errorf("alert %s watches %s, which this module does not publish: "+
							"the rule is valid and will never fire", r.Alert, metric)
					}
				}
			}
		}
	}
	t.Logf("%d alerts over %d metrics", alerts, len(published))
}

// dashboardsDir is the module's Grafana dashboards.
const dashboardsDir = "../../../../monitoring/grafana-dashboards"

// TestEveryDashboardPanelIsAboutAMetricThatExists is the same guard for the
// dashboards, and it matters for the same reason with a twist: a panel over a
// metric nobody publishes does not look broken, it looks like a system with
// nothing to report. "No data" and "nothing is wrong" are drawn identically.
func TestEveryDashboardPanelIsAboutAMetricThatExists(t *testing.T) {
	published := publishedMetrics(t)

	files, err := filepath.Glob(filepath.Join(dashboardsDir, "*", "*.json"))
	if err != nil {
		t.Fatalf("glob the dashboards: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no dashboards in %s; this guard needs rewriting", dashboardsDir)
	}

	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		// Parsed rather than grepped, so a metric named only in a panel's prose
		// (the descriptions name the alerts and the series they read) is not
		// mistaken for one a panel queries.
		var board struct {
			Title  string `json:"title"`
			UID    string `json:"uid"`
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					Expr string `json:"expr"`
				} `json:"targets"`
			} `json:"panels"`
		}
		if err := yaml.Unmarshal(raw, &board); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		if board.Title == "" || board.UID == "" {
			t.Errorf("%s: a dashboard needs a title and a uid", filepath.Base(f))
		}
		for _, p := range board.Panels {
			for _, target := range p.Targets {
				for _, metric := range metricInExpr.FindAllString(target.Expr, -1) {
					if !published[metric] {
						t.Errorf("%s: panel %q queries %s, which this module does not publish: "+
							"the panel will read \"No data\" forever", filepath.Base(f), p.Title, metric)
					}
				}
			}
		}
	}
}
