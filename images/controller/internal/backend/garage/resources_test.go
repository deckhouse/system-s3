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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// testVolumeSize is the volume size the fixtures build a StatefulSet with; the
// real one is computed from the sum of the SystemBucket sizes (see capacity.go),
// which is not what these tests are about.
var testVolumeSize = resource.MustParse("10Gi")

// localSC is the managed local StorageClass the module ships; the fixtures name
// it explicitly so a rename in config defaults cannot silently pass these tests.
const localSC = "system-s3-local"

// systemStore is the store as the module builds it from its default settings:
// the high-availability mode, which is what a cluster with a highly available
// control plane gets without anyone setting anything.
func systemStore() *backend.Store {
	return &backend.Store{
		Name:             "system",
		HighAvailability: true,
		StorageClass:     localSC,
	}
}

// nonHAStore is the store outside the high-availability mode: one replica,
// replication factor 1.
func nonHAStore() *backend.Store {
	s := systemStore()
	s.HighAvailability = false
	return s
}

func TestReplicationFactor(t *testing.T) {
	if got := replicationFactor(systemStore()); got != 3 {
		t.Errorf("replicationFactor(default)=%d, want 3", got)
	}
	if got := replicationFactor(nonHAStore()); got != 1 {
		t.Errorf("replicationFactor(nonHA)=%d, want 1", got)
	}
}

func TestRenderConfig(t *testing.T) {
	cfg := renderConfig(5)
	for _, want := range []string{
		"replication_factor = 5",
		"[s3_api]",
		"[admin]",
		`s3_region = "garage"`,
	} {
		if !strings.Contains(cfg, want) {
			t.Errorf("renderConfig missing %q in:\n%s", want, cfg)
		}
	}
}

func TestClampRF(t *testing.T) {
	cases := []struct{ desired, nodes, want int32 }{
		{1, 1, 1},
		{3, 1, 1}, // one master -> single copy
		{3, 2, 2}, // two nodes -> rf=2 (valid Garage mode)
		{3, 3, 3},
		{5, 3, 3}, // High on three masters
		{5, 4, 4}, // no odd rounding
		{5, 5, 5},
		{3, 0, 1}, // floor
	}
	for _, c := range cases {
		if got := clampRF(c.desired, c.nodes); got != c.want {
			t.Errorf("clampRF(%d, %d)=%d, want %d", c.desired, c.nodes, got, c.want)
		}
	}
}

func TestReplicationFactorFromConfigMap(t *testing.T) {
	cm := buildConfigMap(systemStore(), "d8-system-s3", 2)
	if got := replicationFactorFromConfigMap(cm); got != 2 {
		t.Errorf("replicationFactorFromConfigMap=%d, want 2", got)
	}
	if got := replicationFactorFromConfigMap(nil); got != 0 {
		t.Errorf("replicationFactorFromConfigMap(nil)=%d, want 0", got)
	}
}

func TestNamesAndEndpoints(t *testing.T) {
	c := systemStore()
	if got := resourceName(c); got != "system-garage" {
		t.Errorf("resourceName=%q", got)
	}
	if got := secretName(c); got != "system-garage-secrets" {
		t.Errorf("secretName=%q", got)
	}
	if got := s3Endpoint(c, "d8-system-s3", "internal.store.local"); got != "https://system-garage.d8-system-s3.svc.internal.store.local:3900" {
		t.Errorf("s3Endpoint=%q", got)
	}
	// The admin API is reached at the PLANE's Service, never at the endpoint: an
	// admin call is always made to a particular Garage cluster.
	if got := adminEndpoint(c, "d8-system-s3", "store.local"); got != "https://system-garage-api.d8-system-s3.svc.store.local:3903" {
		t.Errorf("adminEndpoint=%q", got)
	}
	if l := commonLabels(c); l["storage.deckhouse.io/system-store"] != "system" {
		t.Errorf("commonLabels missing store label: %v", l)
	}
}

func TestAccessResourceName(t *testing.T) {
	if got := backend.AccessResourceName("app", "data"); got != "app.data" {
		t.Errorf("AccessResourceName=%q, want app.data", got)
	}
}

func TestInitialReplicationFactorSystem(t *testing.T) {
	if got := initialReplicationFactor(systemStore()); got != replicasHA {
		t.Errorf("initialReplicationFactor(System)=%d, want %d", got, replicasHA)
	}
	// Single replica pins factor 1: a higher factor could never be satisfied by one
	// Garage node, so the store would stay read-only forever.
	if got := initialReplicationFactor(nonHAStore()); got != 1 {
		t.Errorf("initialReplicationFactor(System single)=%d, want 1", got)
	}
}

func TestDesiredReplicas(t *testing.T) {
	if got := desiredReplicas(systemStore()); got != replicasHA {
		t.Errorf("desiredReplicas(default)=%d, want %d (fixed, master-count independent)", got, replicasHA)
	}
	if got := desiredReplicas(nonHAStore()); got != replicasNonHA {
		t.Errorf("desiredReplicas(nonHA)=%d, want %d", got, replicasNonHA)
	}
}

func TestBuildSystemStatefulSet(t *testing.T) {
	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:v1", "hash", testVolumeSize)

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != replicasHA {
		t.Fatalf("replicas=%v, want %d (fixed, independent of master count)", sts.Spec.Replicas, replicasHA)
	}
	if sts.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
		t.Errorf("PodManagementPolicy=%q, want Parallel", sts.Spec.PodManagementPolicy)
	}
	if sts.Spec.ServiceName != rpcSvcName(systemStore()) {
		t.Errorf("ServiceName=%q, want %q", sts.Spec.ServiceName, rpcSvcName(systemStore()))
	}

	// Node-sticky storage: a per-ordinal PVC on the managed local StorageClass,
	// no inline hostPath volume.
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("System must use one volumeClaimTemplate (local PV), got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	vct := sts.Spec.VolumeClaimTemplates[0]
	if vct.Name != "data" {
		t.Errorf("volumeClaimTemplate name=%q, want data", vct.Name)
	}
	if vct.Spec.StorageClassName == nil || *vct.Spec.StorageClassName != localSC {
		t.Errorf("volumeClaimTemplate storageClass=%v, want %q", vct.Spec.StorageClassName, localSC)
	}

	spec := sts.Spec.Template.Spec
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == "data" {
			t.Errorf("System must not carry an inline data volume (PVC-backed): %+v", spec.Volumes[i])
		}
	}

	assertNoNodeSelection(t, spec)

	// HA replicas must run on separate eligible hosts, like platform components.
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity")
	}
	if len(spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("anti-affinity must be required, not a preference")
	}
	required := spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required) != 1 || required[0].TopologyKey != hostnameTopologyKey {
		t.Errorf("required anti-affinity term=%+v, want topologyKey %q", required, hostnameTopologyKey)
	}

	// Stable node identity: an optional identity-Secret volume plus a
	// restore-node-key initContainer that mounts both the data volume and the
	// identity Secret (so it can restore node_key before Garage starts).
	var idVol *corev1.Volume
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == "node-identity" {
			idVol = &spec.Volumes[i]
		}
	}
	if idVol == nil {
		t.Fatalf("expected a node-identity volume")
	}
	if idVol.Secret == nil {
		t.Fatalf("node-identity volume=%+v, want it backed by secret %q", idVol.VolumeSource, nodeIdentitySecretName(systemStore()))
	}
	if idVol.Secret.SecretName != nodeIdentitySecretName(systemStore()) {
		t.Errorf("node-identity volume=%+v, want secret %q", idVol.VolumeSource, nodeIdentitySecretName(systemStore()))
	}
	if idVol.Secret.Optional == nil || !*idVol.Secret.Optional {
		t.Errorf("node-identity secret must be optional (first boot has no identity yet)")
	}
	if len(spec.InitContainers) != 1 || spec.InitContainers[0].Name != "restore-node-key" {
		t.Fatalf("expected one restore-node-key initContainer, got %+v", spec.InitContainers)
	}
	init := spec.InitContainers[0]
	if init.Image != "garage:v1" {
		t.Errorf("initContainer image=%q, want garage:v1", init.Image)
	}
	mounts := map[string]string{}
	for _, m := range init.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	if mounts["data"] != dataMountPath {
		t.Errorf("initContainer must mount data at %q, got %q", dataMountPath, mounts["data"])
	}
	if mounts["node-identity"] != nodeIdentityMountPath {
		t.Errorf("initContainer must mount node-identity at %q, got %q", nodeIdentityMountPath, mounts["node-identity"])
	}
}

func TestPodOrdinal(t *testing.T) {
	cases := []struct {
		name string
		want int32
		ok   bool
	}{
		{"system-garage-0", 0, true},
		{"system-garage-12", 12, true},
		{"system-garage", 0, false},
		{"system-garage-", 0, false},
		{"system-garage-x", 0, false},
	}
	for _, c := range cases {
		got, ok := podOrdinal(c.name)
		if ok != c.ok || got != c.want {
			t.Errorf("podOrdinal(%q)=(%d,%v), want (%d,%v)", c.name, got, ok, c.want, c.ok)
		}
	}
}

func TestBuildNonHASystemStatefulSet(t *testing.T) {
	sts := buildStatefulSet(nonHAStore(), "d8-system-s3", "garage:v1", "hash", testVolumeSize)

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != replicasNonHA {
		t.Fatalf("replicas=%v, want %d", sts.Spec.Replicas, replicasNonHA)
	}
	// Everything else is as usual: no node selection of its own and one claim per
	// replica from the configured StorageClass.
	spec := sts.Spec.Template.Spec
	assertNoNodeSelection(t, spec)
	if spec.Affinity != nil && spec.Affinity.PodAntiAffinity != nil {
		t.Error("non-HA store must not require pod anti-affinity")
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 ||
		sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName == nil ||
		*sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName != localSC {
		t.Errorf("volumeClaimTemplates=%+v, want one PVC on %q", sts.Spec.VolumeClaimTemplates, localSC)
	}
}

func TestLayoutTotalCapacity(t *testing.T) {
	if got := layoutTotalCapacity(&clusterLayout{}); got != nil {
		t.Errorf("layoutTotalCapacity(empty)=%v, want nil", got)
	}
	a := int64(1 << 30)
	b := int64(2 << 30)
	total := layoutTotalCapacity(&clusterLayout{Roles: []layoutRole{
		{ID: "a", Capacity: &a},
		{ID: "b", Capacity: &b},
		{ID: "c", Capacity: nil}, // gateway node, no capacity
	}})
	if total == nil || total.Value() != a+b {
		t.Errorf("layoutTotalCapacity=%v, want %d", total, a+b)
	}
}

// The data-plane image comes from the module's own ModuleSource, so the pods
// must carry the module-scoped pull secret. `deckhouse-registry` mirrors the
// global modules registry and holds no credentials for a second ModuleSource:
// pulls then fail with 401 while the controller itself keeps running.
func TestPullSecretIsModuleScoped(t *testing.T) {
	const want = "system-s3-module-registry"

	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:v1", "hash", testVolumeSize)
	secrets := sts.Spec.Template.Spec.ImagePullSecrets
	if len(secrets) != 1 || secrets[0].Name != want {
		t.Errorf("imagePullSecrets=%v, want [%s]", secrets, want)
	}
}

// The readiness probe must not depend on the cluster being healthy.
//
// Garage answers /health on the admin port only once the node is in the layout,
// and the layout is assigned by this controller through the Service in front of
// these pods — which only routes to ready endpoints. Gating readiness on /health
// is therefore a deadlock: no pod becomes ready until the layout exists, and the
// layout cannot be written until a pod is ready. A fresh installation sat at 0/3
// ready for 35 minutes in an e2e run before this was understood.

// The readiness probe says "this node is in the cluster and can serve", and that
// is what the disruption budget rests on: a budget counting processes rather than
// members would let a second eviction through the moment the replacement pod
// accepted a connection, which is well before it has anything to serve.
//
// It used to be a TCP connect, because /health deadlocked the bootstrap: the
// layout was assigned through a Service, a Service routes to ready endpoints
// only, and no pod became ready until the layout was assigned (an e2e run sat at
// 0/3 for 35 minutes). The layout is now assigned at a pod's own address, so the
// deadlock is gone — and this test is what keeps the two halves of that from
// drifting apart again.
func TestReadinessProbeReportsClusterMembership(t *testing.T) {
	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:test", "cfghash", testVolumeSize)

	var garage *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == "garage" {
			garage = &sts.Spec.Template.Spec.Containers[i]
		}
	}
	if garage == nil {
		t.Fatal("no \"garage\" container in the data-plane pod")
	}

	probe := garage.ReadinessProbe
	if probe == nil {
		t.Fatal("the data plane has no readiness probe")
	}
	if probe.HTTPGet == nil {
		t.Fatalf("readiness probe is %+v, want an HTTP check on /health", probe.ProbeHandler)
	}
	if probe.HTTPGet.Path != "/health" {
		t.Errorf("readiness probes %q, want /health", probe.HTTPGet.Path)
	}
	if got := probe.HTTPGet.Port.IntValue(); got != int(adminPort) {
		t.Errorf("readiness probes port %d, want the admin port %d", got, adminPort)
	}
	// Becoming ready waits on the controller's next pass rather than on this
	// process, so the probe has to allow for it instead of restarting.
	if probe.FailureThreshold < 10 {
		t.Errorf("failureThreshold=%d: a pod that is waiting for the layout would be given up on",
			probe.FailureThreshold)
	}
}

// TestSecondIncarnationRenamesEverythingButTheEndpoint is the naming contract a
// migration rests on: the two data planes share a namespace, so every object of
// the incoming one is named apart from the serving one — except the Service the
// consumers hold, which is the same object throughout and merely changes which
// pods it selects.
func TestSecondIncarnationRenamesEverythingButTheEndpoint(t *testing.T) {
	first := systemStore()
	second := systemStore()
	second.Incarnation = 2

	if got := resourceName(second); got != "system-garage-i2" {
		t.Errorf("resourceName(incarnation 2)=%q, want system-garage-i2", got)
	}
	if got := configName(second); got != "system-garage-i2-config" {
		t.Errorf("configName(incarnation 2)=%q", got)
	}
	if got := secretName(second); got != "system-garage-i2-secrets" {
		t.Errorf("secretName(incarnation 2)=%q", got)
	}
	if got := rpcSvcName(second); got != "system-garage-i2-rpc" {
		t.Errorf("rpcSvcName(incarnation 2)=%q", got)
	}
	if got, want := s3SvcName(second), s3SvcName(first); got != want {
		t.Errorf("s3SvcName(incarnation 2)=%q, want the endpoint to stay %q", got, want)
	}
	if got := s3Endpoint(second, "d8-system-s3", "store.local"); got != s3Endpoint(first, "d8-system-s3", "store.local") {
		t.Errorf("s3Endpoint moved with the incarnation: %q", got)
	}
	// The module's own way in does move with it, which is the point: the two
	// planes are reached separately for as long as both exist.
	if got := planeSvcName(second); got != "system-garage-i2-api" {
		t.Errorf("planeSvcName(incarnation 2)=%q", got)
	}
	if planeSvcName(second) == planeSvcName(first) {
		t.Errorf("both planes answer at the same Service %q", planeSvcName(first))
	}
}

// TestUnsetIncarnationIsTheFirst covers the store description that predates
// migrations: it names the objects an existing installation already runs, rather
// than a plane that does not exist.
func TestUnsetIncarnationIsTheFirst(t *testing.T) {
	zero := systemStore() // Incarnation left at its zero value
	if got := resourceName(zero); got != "system-garage" {
		t.Errorf("resourceName(unset incarnation)=%q, want system-garage", got)
	}
	if got := planeLabels(zero)[incarnationLabel]; got != "1" {
		t.Errorf("planeLabels(unset incarnation)[%s]=%q, want 1", incarnationLabel, got)
	}
	first := systemStore()
	first.Incarnation = firstIncarnation
	if got := resourceName(first); got != resourceName(zero) {
		t.Errorf("resourceName(1)=%q differs from resourceName(unset)=%q", got, resourceName(zero))
	}
}

// TestIncarnationOfPod: a pod carrying no incarnation label belongs to the first
// plane. The rolling update that first adds the label must not make the pass lose
// sight of the replicas it has not rolled yet.
func TestIncarnationOfPod(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   int32
	}{
		{"no label at all", nil, firstIncarnation},
		{"the label of the first plane", map[string]string{incarnationLabel: "1"}, 1},
		{"the label of the second plane", map[string]string{incarnationLabel: "2"}, 2},
		{"an unparseable label", map[string]string{incarnationLabel: "later"}, firstIncarnation},
		{"a label below the first", map[string]string{incarnationLabel: "0"}, firstIncarnation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{}
			pod.Labels = tc.labels
			if got := incarnationOfPod(pod); got != tc.want {
				t.Errorf("incarnationOfPod=%d, want %d", got, tc.want)
			}
		})
	}
}

// TestStatefulSetSelectorStaysWideAndPodsCarryThePlane pins the asymmetry that
// makes the rename possible on a cluster that already runs: the immutable
// selector is untouched, and the labels that tell the planes apart are on the
// pods.
func TestStatefulSetSelectorStaysWideAndPodsCarryThePlane(t *testing.T) {
	store := systemStore()
	store.Incarnation = 2
	sts := buildStatefulSet(store, "d8-system-s3", "garage:v2", "hash", testVolumeSize)

	if _, found := sts.Spec.Selector.MatchLabels[incarnationLabel]; found {
		t.Errorf("the StatefulSet selector carries the incarnation; it is immutable and must not: %v", sts.Spec.Selector.MatchLabels)
	}
	if got := sts.Spec.Template.Labels[incarnationLabel]; got != "2" {
		t.Errorf("pod template label %s=%q, want 2", incarnationLabel, got)
	}
	if got := sts.Spec.VolumeClaimTemplates[0].Labels[incarnationLabel]; got != "2" {
		t.Errorf("claim template label %s=%q, want 2", incarnationLabel, got)
	}
	term := sts.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if got := term.LabelSelector.MatchLabels[incarnationLabel]; got != "2" {
		t.Errorf("the anti-affinity spreads against the other plane too: %v", term.LabelSelector.MatchLabels)
	}
}

// TestEachPlaneSpreadsOnlyAgainstItself: both spreading rules select on the
// plane, not on the store.
//
// A migration runs two Garage clusters on the same masters until the copy is
// done. A rule that selected on the store alone would have the second plane read
// every master as taken — it would pile onto one node, and with a hard rule it
// would not schedule at all, so the module could never replace a data plane. The
// HA replicas require different hosts, but the incoming incarnation is not
// blocked by the serving plane's pods on those same hosts.
func TestEachPlaneSpreadsOnlyAgainstItself(t *testing.T) {
	store := systemStore()
	store.Incarnation = 2
	sts := buildStatefulSet(store, "d8-system-s3", "garage:v2", "hash", testVolumeSize)
	spec := sts.Spec.Template.Spec

	anti := spec.Affinity.PodAntiAffinity
	if anti == nil || len(anti.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("pod anti-affinity=%+v, want one required term", anti)
	}
	if len(anti.PreferredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("preferred anti-affinity must not replace the required separation")
	}
	if got := anti.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey; got != hostnameTopologyKey {
		t.Errorf("anti-affinity topology key=%q, want %q", got, hostnameTopologyKey)
	}

	if len(spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("topology spread constraints=%+v, want one", spec.TopologySpreadConstraints)
	}
	tsc := spec.TopologySpreadConstraints[0]
	if tsc.MaxSkew != 1 || tsc.TopologyKey != hostnameTopologyKey {
		t.Errorf("spread constraint=%+v, want maxSkew 1 over %q", tsc, hostnameTopologyKey)
	}
	if tsc.WhenUnsatisfiable != corev1.ScheduleAnyway {
		t.Errorf("spread constraint is %q: a cluster with fewer masters than replicas would leave replicas Pending", tsc.WhenUnsatisfiable)
	}
	if got := tsc.LabelSelector.MatchLabels[incarnationLabel]; got != "2" {
		t.Errorf("the spread constraint counts the other plane's replicas too: %v", tsc.LabelSelector.MatchLabels)
	}
	// Without these the skew is measured over every node in the cluster, which
	// makes the constraint satisfied by workers the data plane may never have.
	if tsc.NodeAffinityPolicy == nil || *tsc.NodeAffinityPolicy != corev1.NodeInclusionPolicyHonor {
		t.Errorf("nodeAffinityPolicy=%v, want Honor", tsc.NodeAffinityPolicy)
	}
	if tsc.NodeTaintsPolicy == nil || *tsc.NodeTaintsPolicy != corev1.NodeInclusionPolicyHonor {
		t.Errorf("nodeTaintsPolicy=%v, want Honor", tsc.NodeTaintsPolicy)
	}
}

// assertNoNodeSelection pins that buildStatefulSet decides nothing about nodes:
// no selector, no node affinity, no tolerations. Where the data plane runs is
// the chart's placement, put on by the driver (see withPlacement); a rule left
// in here would be ANDed with it and quietly narrow what the chart allowed.
func assertNoNodeSelection(t *testing.T, spec corev1.PodSpec) {
	t.Helper()
	if spec.NodeSelector != nil {
		t.Errorf("nodeSelector=%v: node selection is the placement's, not the StatefulSet's", spec.NodeSelector)
	}
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil {
		t.Errorf("node affinity=%+v: node selection is the placement's, not the StatefulSet's", spec.Affinity.NodeAffinity)
	}
	if len(spec.Tolerations) != 0 {
		t.Errorf("tolerations=%+v: they come with the placement", spec.Tolerations)
	}
}
