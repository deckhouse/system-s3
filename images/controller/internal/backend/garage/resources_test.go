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
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// localSC is the managed local StorageClass the module ships; the fixtures name
// it explicitly so a rename in config defaults cannot silently pass these tests.
const localSC = "system-s3-local"

// systemStore is the store as the module builds it from its default settings.
func systemStore() *backend.Store {
	return &backend.Store{
		Name:         "system",
		SizePerNode:  resource.MustParse("10Gi"),
		StorageClass: localSC,
	}
}

// singleReplicaStore is the store as the module builds it when the
// singleReplica setting is on.
func singleReplicaStore() *backend.Store {
	s := systemStore()
	s.SingleReplica = true
	return s
}

func TestReplicationFactor(t *testing.T) {
	if got := replicationFactor(systemStore()); got != 3 {
		t.Errorf("replicationFactor(default)=%d, want 3", got)
	}
	if got := replicationFactor(singleReplicaStore()); got != 1 {
		t.Errorf("replicationFactor(singleReplica)=%d, want 1", got)
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
	cm := buildConfigMap(systemStore(), "d8-system-s3", 2, firstIncarnation)
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
	if got := s3Endpoint(c, "d8-system-s3", "internal.store.local"); got != "http://system-garage.d8-system-s3.svc.internal.store.local:3900" {
		t.Errorf("s3Endpoint=%q", got)
	}
	if got := adminEndpoint(c, "d8-system-s3", "store.local"); got != "http://system-garage.d8-system-s3.svc.store.local:3903" {
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
	if got := initialReplicationFactor(singleReplicaStore()); got != 1 {
		t.Errorf("initialReplicationFactor(System single)=%d, want 1", got)
	}
}

func TestDesiredReplicas(t *testing.T) {
	if got := desiredReplicas(systemStore()); got != replicasHA {
		t.Errorf("desiredReplicas(default)=%d, want %d (fixed, master-count independent)", got, replicasHA)
	}
	if got := desiredReplicas(singleReplicaStore()); got != replicasSingle {
		t.Errorf("desiredReplicas(singleReplica)=%d, want %d", got, replicasSingle)
	}
}

func TestBuildSystemStatefulSet(t *testing.T) {
	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:v1", "hash")

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

	// Forced onto control-plane, tolerating master taints.
	if _, ok := spec.NodeSelector[controlPlaneNodeLabel]; !ok {
		t.Errorf("nodeSelector missing %q: %v", controlPlaneNodeLabel, spec.NodeSelector)
	}
	if len(spec.Tolerations) == 0 {
		t.Errorf("expected control-plane tolerations")
	}

	// Soft (preferred, not required) anti-affinity so all replicas still run on a
	// single master.
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		t.Fatalf("expected pod anti-affinity")
	}
	if len(spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
		t.Errorf("anti-affinity must be soft, found required terms (would block single-master scheduling)")
	}
	pref := spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(pref) != 1 || pref[0].PodAffinityTerm.TopologyKey != hostnameTopologyKey {
		t.Errorf("preferred anti-affinity term=%+v, want topologyKey %q", pref, hostnameTopologyKey)
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

func TestBuildSingleReplicaSystemStatefulSet(t *testing.T) {
	sts := buildStatefulSet(singleReplicaStore(), "d8-system-s3", "garage:v1", "hash")

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != replicasSingle {
		t.Fatalf("replicas=%v, want %d", sts.Spec.Replicas, replicasSingle)
	}
	// Everything else is the System profile as usual: control-plane placement and
	// node-sticky storage on the managed local StorageClass.
	spec := sts.Spec.Template.Spec
	if _, ok := spec.NodeSelector[controlPlaneNodeLabel]; !ok {
		t.Errorf("nodeSelector missing %q: %v", controlPlaneNodeLabel, spec.NodeSelector)
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 ||
		sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName == nil ||
		*sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName != localSC {
		t.Errorf("volumeClaimTemplates=%+v, want one PVC on %q", sts.Spec.VolumeClaimTemplates, localSC)
	}
}

func TestSystemLocalPVIncarnation(t *testing.T) {
	c := systemStore()

	// A never-recreated cluster keeps the original names and directories, so
	// upgrading a running store moves nothing.
	legacyName := localPVName(c, "master-0", firstIncarnation, 2)
	legacyPath := localPVPath(c, "master-0", firstIncarnation, 2)
	if want := fmt.Sprintf("%s-local-%s-2", resourceName(c), shortHash("master-0")); legacyName != want {
		t.Errorf("legacy PV name=%q, want %q", legacyName, want)
	}
	if want := fmt.Sprintf("%s/%s/%s-2", hostPathBase, c.Name, shortHash("master-0")); legacyPath != want {
		t.Errorf("legacy PV path=%q, want %q", legacyPath, want)
	}

	// A recreated cluster gets both a distinct PV name (so it cannot bind a
	// leftover PV of the previous incarnation) and a distinct directory (so it
	// starts from empty storage rather than incompatible Garage metadata).
	name := localPVName(c, "master-0", 2, 2)
	path := localPVPath(c, "master-0", 2, 2)
	if name == legacyName {
		t.Errorf("incarnation 2 PV name must differ from %q", legacyName)
	}
	if path == legacyPath {
		t.Errorf("incarnation 2 PV path must differ from %q", legacyPath)
	}
	if !strings.HasPrefix(path, fmt.Sprintf("%s/%s/", hostPathBase, c.Name)) {
		t.Errorf("PV path=%q, want it under the store's hostPath base", path)
	}
	if pv := buildLocalPV(c, "master-0", 2, 2); pv.Name != name || pv.Spec.HostPath.Path != path {
		t.Errorf("buildLocalPV=(%q,%q), want (%q,%q)", pv.Name, pv.Spec.HostPath.Path, name, path)
	}
}

func TestBuildSystemLocalPV(t *testing.T) {
	pv := buildLocalPV(systemStore(), "master-0", firstIncarnation, 2)

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("reclaimPolicy=%q, want Retain (never wipe data)", pv.Spec.PersistentVolumeReclaimPolicy)
	}
	if pv.Spec.StorageClassName != localSC {
		t.Errorf("storageClass=%q, want %q", pv.Spec.StorageClassName, localSC)
	}
	if pv.Spec.HostPath == nil || pv.Spec.HostPath.Type == nil || *pv.Spec.HostPath.Type != corev1.HostPathDirectoryOrCreate {
		t.Fatalf("expected hostPath source with DirectoryOrCreate: %+v", pv.Spec.HostPath)
	}
	if pv.Labels[labelSystemLocalNode] != "master-0" {
		t.Errorf("node label=%q, want master-0", pv.Labels[labelSystemLocalNode])
	}

	// nodeAffinity pins the PV (hence the replica) to its node.
	na := pv.Spec.NodeAffinity
	if na == nil || na.Required == nil || len(na.Required.NodeSelectorTerms) != 1 {
		t.Fatalf("expected required nodeAffinity: %+v", na)
	}
	me := na.Required.NodeSelectorTerms[0].MatchExpressions
	if len(me) != 1 || me[0].Key != hostnameTopologyKey || me[0].Operator != corev1.NodeSelectorOpIn ||
		len(me[0].Values) != 1 || me[0].Values[0] != "master-0" {
		t.Errorf("nodeAffinity term=%+v, want %s In [master-0]", me, hostnameTopologyKey)
	}
}

func TestDesiredSystemLocalPVs(t *testing.T) {
	hostnames := []string{"master-0", "master-1", "master-2"}
	pvs := desiredLocalPVs(systemStore(), hostnames, firstIncarnation)

	want := len(hostnames) * int(replicasHA)
	if len(pvs) != want {
		t.Fatalf("pool size=%d, want %d (systemReplicas per node)", len(pvs), want)
	}
	// Names are unique and deterministic.
	names := map[string]struct{}{}
	for _, pv := range pvs {
		if _, dup := names[pv.Name]; dup {
			t.Errorf("duplicate PV name %q", pv.Name)
		}
		names[pv.Name] = struct{}{}
	}
	// Every node carries systemReplicas PVs.
	perNode := map[string]int{}
	for _, pv := range pvs {
		perNode[pv.Labels[labelSystemLocalNode]]++
	}
	for _, h := range hostnames {
		if perNode[h] != int(replicasHA) {
			t.Errorf("node %q has %d PVs, want %d", h, perNode[h], replicasHA)
		}
	}

	// Single-replica mode still stocks one PV per master, so the scheduler can
	// place the sole replica on any of them (once bound, nodeAffinity keeps it
	// there for good).
	single := desiredLocalPVs(singleReplicaStore(), hostnames, firstIncarnation)
	if len(single) != len(hostnames) {
		t.Fatalf("single-replica pool size=%d, want %d (one per master)", len(single), len(hostnames))
	}
	for _, pv := range single {
		if !strings.HasSuffix(pv.Name, "-0") {
			t.Errorf("single-replica pool PV %q must be the ordinal-0 slot", pv.Name)
		}
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

	sts := buildStatefulSet(systemStore(), "d8-system-s3", "garage:v1", "hash")
	secrets := sts.Spec.Template.Spec.ImagePullSecrets
	if len(secrets) != 1 || secrets[0].Name != want {
		t.Errorf("imagePullSecrets=%v, want [%s]", secrets, want)
	}
}
