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
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/tlsproxy"
)

// testDriver is a driver with everything EnsureStore insists on before it
// builds anything: both images and the data plane's TLS.
func testDriver(t *testing.T, c client.Client, ns string) *Driver {
	t.Helper()
	d := &Driver{
		client: c, apiReader: c, namespace: ns,
		image: "garage:test", copierImage: "controller:test",
		log: testGarageLogger(t),
	}
	return d.WithTLS("garage-tls", "/nonexistent/ca.crt")
}

// TestGarageListensOnTheLoopbackOnly pins the half of the design that is easy
// to lose in a refactor: Garage's S3 and admin listeners must not be on the
// pod's address, or anything that can reach the pod has a plaintext way around
// the terminator.
func TestGarageListensOnTheLoopbackOnly(t *testing.T) {
	cfg := renderConfig(3)
	for _, want := range []string{
		`api_bind_addr = "127.0.0.1:3910"`,
		`api_bind_addr = "127.0.0.1:3913"`,
	} {
		if !strings.Contains(cfg, want) {
			t.Errorf("garage.toml has no %s:\n%s", want, cfg)
		}
	}
	for _, bad := range []string{`"[::]:3900"`, `"[::]:3903"`, `"0.0.0.0:`} {
		if strings.Contains(cfg, bad) {
			t.Errorf("garage.toml binds %s, which the terminator was supposed to own:\n%s", bad, cfg)
		}
	}
	// RPC is Garage's own and stays on every address: other nodes dial it, and
	// Garage encrypts it itself.
	if !strings.Contains(cfg, `rpc_bind_addr = "[::]:3901"`) {
		t.Errorf("RPC is no longer reachable by the other nodes:\n%s", cfg)
	}
}

// TestTerminatorIsInThePod checks what withTLSTerminator adds to the data
// plane's pod: the sidecar on the client-facing ports, forwarding to Garage's
// loopback listeners, with the certificate mounted.
func TestTerminatorIsInThePod(t *testing.T) {
	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:v1", "hash", testVolumeSize)
	withTLSTerminator(sts, "controller:v1", "garage-tls")
	spec := sts.Spec.Template.Spec

	var term *corev1.Container
	for i := range spec.Containers {
		if spec.Containers[i].Name == terminatorContainer {
			term = &spec.Containers[i]
		}
	}
	if term == nil {
		t.Fatal("the pod has no TLS terminator")
	}
	if term.Image != "controller:v1" || len(term.Args) != 1 || term.Args[0] != tlsproxy.Command {
		t.Errorf("terminator runs %s %v, want the controller image with %q", term.Image, term.Args, tlsproxy.Command)
	}
	env := map[string]string{}
	for _, e := range term.Env {
		env[e.Name] = e.Value
	}
	if env[tlsproxy.EnvRoutes] != "[::]:3900=127.0.0.1:3910,[::]:3903=127.0.0.1:3913" {
		t.Errorf("routes %q", env[tlsproxy.EnvRoutes])
	}
	if _, err := tlsproxy.ParseRoutes(env[tlsproxy.EnvRoutes]); err != nil {
		t.Errorf("the routes the driver writes do not parse: %v", err)
	}

	ports := map[int32]string{}
	for _, c := range spec.Containers {
		for _, p := range c.Ports {
			if owner, dup := ports[p.ContainerPort]; dup {
				t.Errorf("port %d is declared by both %s and %s", p.ContainerPort, owner, c.Name)
			}
			ports[p.ContainerPort] = c.Name
		}
	}
	if ports[s3Port] != terminatorContainer || ports[adminPort] != terminatorContainer {
		t.Errorf("client-facing ports belong to %q and %q, want the terminator", ports[s3Port], ports[adminPort])
	}
	if ports[rpcPort] != "garage" {
		t.Errorf("RPC port belongs to %q, want Garage", ports[rpcPort])
	}
	if ports[proxyMetricsPort] != terminatorContainer {
		t.Errorf("the terminator's metrics port belongs to %q", ports[proxyMetricsPort])
	}
	if env[tlsproxy.EnvMetricsListen] != "[::]:3904" {
		t.Errorf("metrics listen %q", env[tlsproxy.EnvMetricsListen])
	}
	var token *corev1.SecretKeySelector
	for _, e := range term.Env {
		if e.Name == tlsproxy.EnvMetricsToken && e.ValueFrom != nil {
			token = e.ValueFrom.SecretKeyRef
		}
	}
	if token == nil || token.Name != metricsTokenSecret || token.Key != metricsTokenKey {
		t.Errorf("the terminator's metrics token comes from %+v, want the Secret Garage's /metrics uses", token)
	}

	var mounted bool
	for _, v := range spec.Volumes {
		if v.Name == tlsVolume && v.Secret != nil && v.Secret.SecretName == "garage-tls" {
			mounted = true
		}
	}
	if !mounted {
		t.Error("the certificate Secret is not a volume of the pod")
	}
}

// TestReadinessGoesThroughTheTerminator: the probe is HTTPS on the admin port,
// which is the terminator's. A plaintext probe would never get an answer.
func TestReadinessGoesThroughTheTerminator(t *testing.T) {
	c := garageContainer("garage:v1")
	get := c.ReadinessProbe.HTTPGet
	if get == nil || get.Scheme != corev1.URISchemeHTTPS || get.Port.IntValue() != adminPort {
		t.Fatalf("readiness probe %+v, want HTTPS on %d", get, adminPort)
	}
}

// TestNoTLSMeansNoDataPlane: without a certificate the driver says so and
// builds nothing — it does not fall back to exposing Garage in plaintext.
func TestNoTLSMeansNoDataPlane(t *testing.T) {
	d := &Driver{namespace: "d8-system-s3", image: "garage:test", copierImage: "controller:test", log: testGarageLogger(t)}
	state, err := d.EnsureStore(context.Background(), systemStore())
	if err != nil {
		t.Fatal(err)
	}
	if state.Ready || !strings.Contains(state.Message, "GARAGE_TLS_SECRET") {
		t.Fatalf("state %+v, want not ready with the missing setting named", state)
	}
}

// TestAdminClientWithoutACAFailsClosed: an unreadable CA gives a client whose
// calls fail with the reason, never one that skips verification.
func TestAdminClientWithoutACAFailsClosed(t *testing.T) {
	d := (&Driver{namespace: "d8-system-s3"}).WithTLS("garage-tls", "/nonexistent/ca.crt")
	_, err := d.admin(systemStore(), "https://127.0.0.1:1", "token").health(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Garage CA") {
		t.Fatalf("got %v, want the CA read failure", err)
	}
}
