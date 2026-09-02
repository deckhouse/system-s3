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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// Port numbers used by the Garage processes.
const (
	s3Port    = 3900
	rpcPort   = 3901
	adminPort = 3903
)

// Fixed paths inside the Garage container.
const (
	dataMountPath  = "/var/lib/garage"
	configMount    = "/etc/garage"
	configFileName = "garage.toml"

	// hostPathBase is where Garage data lives on control-plane nodes (the
	// backing directory of each node-sticky local PV).
	hostPathBase = "/var/lib/deckhouse/system-s3/garage"

	// registryPullSecret is the module-scoped pull secret rendered by
	// templates/registry-secret.yaml. The Garage image comes from the module's
	// own ModuleSource (helm_lib_module_image), so its credentials live here —
	// not in `deckhouse-registry`, which mirrors the global modules registry and
	// carries no credentials for a separate ModuleSource.
	registryPullSecret = "system-s3-module-registry"

	// controlPlaneNodeLabel marks control-plane nodes; the StatefulSet is
	// scheduled there (spread across them when several exist, co-located on the
	// single master otherwise).
	controlPlaneNodeLabel = backend.ControlPlaneNodeLabel

	// hostnameTopologyKey is the node label carrying the hostname; used both for
	// the soft pod anti-affinity and as the local-PV nodeAffinity key that pins a
	// replica to its node.
	hostnameTopologyKey = "kubernetes.io/hostname"

	// nodeIdentityMountPath is where the per-ordinal Garage node identity Secret
	// is mounted into the pods; the restore-node-key initContainer copies the
	// matching key into the metadata dir before Garage starts.
	nodeIdentityMountPath = "/etc/garage-node-identity"

	// annConfigHash carries a short hash of garage.toml on the pod template so
	// the workload rolls (and every node converges to the same config) when the
	// config changes.
	annConfigHash = "storage.deckhouse.io/config-hash"
)

// replicasHA is the number of Garage replicas the default (highly available)
// store runs, independent of the control-plane node count. Keeping it constant
// (rather than one pod per master) keeps the pinned replication factor and the
// Garage quorum stable across master-count changes: three replicas always run —
// spread across control-plane nodes when several exist, co-located on the single
// master otherwise.
//
// The master-count changes this has to survive, and what happens on each:
//   - 1 -> 3: the replicas stay where they are and the placement reconcile moves
//     them out one at a time, health-gated, so the store keeps serving.
//   - 3 -> 1: the two replicas whose masters are gone are consolidated onto the
//     survivor; they hold no reachable data, so recycling them restores quorum
//     rather than risking it.
//   - 3 -> 2 (and 2 -> 3): left alone. It is a transient of a shrink or a grow,
//     and neither spreading across two nor consolidating from two is a state
//     worth moving data for.
//
// See nextPlacementAction in rebalance.go, which implements exactly these rules.
const replicasHA int32 = 3

// replicasSingle is the replica count of the single-replica store (the
// singleReplica module setting): one Garage replica with replication_factor 1. It
// gives up all redundancy — the store is unavailable while its pod is down and
// lost with its master's disk — in exchange for a third of the footprint, and it
// never migrates between masters (see reconcilePlacement).
const replicasSingle int32 = 1

// desiredReplicas is the number of Garage data-plane pods: one in single-replica
// mode, replicasHA otherwise. Both counts are independent of the control-plane
// node count; switching between them is not an in-place change but a recreate of
// the data plane (see Driver.teardownDataPlane). It also drives the initial
// replication factor (clamped to this count) and the layout reconcile.
func desiredReplicas(store *backend.Store) int32 {
	if store.SingleReplica {
		return replicasSingle
	}
	return replicasHA
}

// incarnationKey is the ConfigMap key recording the data plane's incarnation: a
// counter bumped on every recreate (a replica-count switch) so the new, empty
// cluster binds local PVs backed by FRESH directories instead of mounting the
// previous incarnation's Garage metadata (which pins a different replication
// factor and a layout of node IDs that no longer exist — a state Garage cannot
// recover from).
const incarnationKey = "incarnation"

// placementAttemptTargetKey and placementAttemptCountKey record how many times
// in a row the placement reconcile has asked for the same migration, in the same
// ConfigMap as the incarnation (the store has no custom resource of its own to
// keep state on). The target is "<ordinal>/<reason>": when it changes the count
// starts over, so the cap bounds a repeating move and not a sequence of
// legitimate ones.
const (
	placementAttemptTargetKey = "placement-attempt-target"
	placementAttemptCountKey  = "placement-attempt-count"
)

// maxPlacementAttempts caps the identical migrations the placement reconcile
// will perform before it gives up and says so.
//
// A migration recycles the replica's PVC, which throws away that replica's copy
// of the data and makes Garage re-replicate it by anti-entropy. The move is only
// worth it because the scheduler is expected to place the replica on a different
// master — but that is a preference, not a guarantee: the pod anti-affinity is
// preferred (it has to be, or replicas could not run on a single master) and the
// pool holds a free PV on every node, so the scheduler may legally return the
// replica to the node it just left. Without a cap that is an unbounded retry
// (Nygard, Release It!, 2nd ed., ch. 4): every few minutes one replica loses its
// data and rebuilds it, forever, and the log looks like progress.
const maxPlacementAttempts = 3

// isPlacementBookkeepingKey reports whether a garage.toml ConfigMap key is
// runtime bookkeeping written by the placement reconcile rather than desired
// state produced by buildConfigMap. mergeDesired carries these across, so the
// apply that runs on every pass does not reset the attempt counter.
func isPlacementBookkeepingKey(key string) bool {
	return key == placementAttemptTargetKey || key == placementAttemptCountKey
}

// firstIncarnation is the incarnation of a store that was never recreated.
const firstIncarnation int32 = 1

// labelSystemLocalNode marks a controller-provisioned local PV and records the
// node (its kubernetes.io/hostname value) its nodeAffinity pins it to. Its
// presence distinguishes pool PVs from any other PV.
const labelSystemLocalNode = backend.LocalNodeLabel

// storeLabel carries the store name on every object the driver creates (used as
// a selector, e.g. to list the local-PV pool).
const storeLabel = backend.StoreLabel

// resourceName is the common name/prefix for every object backing the store.
func resourceName(store *backend.Store) string {
	return store.Name + "-garage"
}

func secretName(store *backend.Store) string {
	return resourceName(store) + "-secrets"
}
func configName(store *backend.Store) string {
	return resourceName(store) + "-config"
}
func rpcSvcName(store *backend.Store) string { return resourceName(store) + "-rpc" }
func s3SvcName(store *backend.Store) string  { return resourceName(store) }

// commonLabels are placed on every object the driver creates. With no owning
// custom resource they are also the only handle on the data plane, so the
// teardown paths select by them.
func commonLabels(store *backend.Store) map[string]string {
	return map[string]string{
		v1alpha1.ManagedByLabel:  v1alpha1.ManagedByValue,
		"app.kubernetes.io/name": "garage",
		storeLabel:               store.Name,
	}
}

// s3Endpoint is the in-cluster S3 URL of the store's Service.
func s3Endpoint(store *backend.Store, namespace, clusterDomain string) string {
	return fmt.Sprintf("http://%s.%s.svc.%s:%d", s3SvcName(store), namespace, clusterDomain, s3Port)
}

// s3HostPort is the in-cluster S3 endpoint as host:port (no scheme), for the
// minio/S3 client used to empty buckets before deletion.
func s3HostPort(store *backend.Store, namespace, clusterDomain string) string {
	return fmt.Sprintf("%s.%s.svc.%s:%d", s3SvcName(store), namespace, clusterDomain, s3Port)
}

// adminEndpoint is the in-cluster admin API URL of the store's Service.
func adminEndpoint(store *backend.Store, namespace, clusterDomain string) string {
	return fmt.Sprintf("http://%s.%s.svc.%s:%d", s3SvcName(store), namespace, clusterDomain, adminPort)
}

// replicationFactor is the Garage replication_factor the module settings ask
// for: 1 in single-replica mode, 3 otherwise. This is only the initial intent
// used at cluster creation; the effective factor is clamped to the node count and
// then PINNED for the cluster's lifetime (see Driver.pinnedState). Garage does
// not support changing replication_factor on a live cluster, so it must never be
// recomputed from the current node count afterwards.
func replicationFactor(store *backend.Store) int32 {
	if store.SingleReplica {
		return 1
	}
	return 3
}

// clampRF caps desired at the available node count, with a floor of 1. Garage
// accepts any replication factor in [1, nodeCount] (2 is a valid, supported
// mode: tolerates one node down, read-only while degraded), so no odd rounding
// is applied. E.g. clampRF(3, 1)=1, clampRF(3, 2)=2, clampRF(5, 4)=4.
func clampRF(desired, nodes int32) int32 {
	rf := desired
	if nodes < rf {
		rf = nodes
	}
	if rf < 1 {
		rf = 1
	}
	return rf
}

// replicationFactorRE extracts replication_factor from an existing garage.toml,
// so a running cluster's (pinned) factor can be read back rather than
// recomputed.
var replicationFactorRE = regexp.MustCompile(`(?m)^replication_factor\s*=\s*(\d+)`)

// replicationFactorFromConfigMap returns the replication_factor recorded in an
// existing garage.toml ConfigMap, or 0 when the ConfigMap is nil or the value is
// absent/unparseable.
func replicationFactorFromConfigMap(cm *corev1.ConfigMap) int32 {
	if cm == nil {
		return 0
	}
	m := replicationFactorRE.FindStringSubmatch(cm.Data[configFileName])
	if len(m) != 2 {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n < 1 {
		return 0
	}
	return int32(n)
}

// incarnationFromConfigMap returns the data-plane incarnation recorded in an
// existing garage.toml ConfigMap. A missing or unparseable value reads back as
// firstIncarnation, so a store whose ConfigMap predates the counter keeps the PV
// names and data directories it is already bound to.
func incarnationFromConfigMap(cm *corev1.ConfigMap) int32 {
	if cm == nil {
		return firstIncarnation
	}
	n, err := strconv.Atoi(cm.Data[incarnationKey])
	if err != nil || n < int(firstIncarnation) {
		return firstIncarnation
	}
	return int32(n)
}

// configHash is a short content hash of garage.toml, stamped onto the pod
// template so the workload rolls when the config changes (Garage reads the file
// only at startup, and every node must run the same replication_factor).
func configHash(cfg string) string {
	sum := sha256.Sum256([]byte(cfg))
	return hex.EncodeToString(sum[:])[:16]
}

// renderConfig produces the garage.toml content for the given (already clamped)
// replication factor. Secrets (rpc, admin token) are supplied via environment
// variables, not the file.
func renderConfig(rf int32) string {
	return fmt.Sprintf(`metadata_dir = "%s/meta"
data_dir = "%s/data"
db_engine = "lmdb"

replication_factor = %d

rpc_bind_addr = "[::]:%d"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:%d"
root_domain = ".s3.garage"

[admin]
api_bind_addr = "[::]:%d"
`, dataMountPath, dataMountPath, rf, rpcPort, s3Port, adminPort)
}

// buildConfigMap returns the ConfigMap holding garage.toml with the given
// replication factor. It also records the data-plane incarnation, the counter
// that keeps a recreated store off the previous incarnation's data directories;
// the key lives outside garage.toml so bumping it does not roll the pods by
// itself (the factor change already does).
func buildConfigMap(store *backend.Store, namespace string, rf, incarnation int32) *corev1.ConfigMap {
	data := map[string]string{
		configFileName: renderConfig(rf),
		incarnationKey: strconv.FormatInt(int64(incarnation), 10),
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configName(store),
			Namespace: namespace,
			Labels:    commonLabels(store),
		},
		Data: data,
	}
}

// buildServiceAccount returns the ServiceAccount the Garage pods run as.
func buildServiceAccount(store *backend.Store, namespace string) *corev1.ServiceAccount {
	automount := false
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(store),
			Namespace: namespace,
			Labels:    commonLabels(store),
		},
		AutomountServiceAccountToken: &automount,
	}
}

// buildS3Service returns the ClusterIP Service exposing the S3 and admin APIs.
func buildS3Service(store *backend.Store, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s3SvcName(store),
			Namespace: namespace,
			Labels:    commonLabels(store),
		},
		Spec: corev1.ServiceSpec{
			Selector: commonLabels(store),
			Ports: []corev1.ServicePort{
				{Name: "s3", Port: s3Port, TargetPort: intstr.FromInt32(s3Port)},
				{Name: "admin", Port: adminPort, TargetPort: intstr.FromInt32(adminPort)},
			},
		},
	}
}

// buildRPCService returns the headless Service used for stable per-pod DNS
// (RPC mesh, consumed by the meshing step in a later milestone).
func buildRPCService(store *backend.Store, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rpcSvcName(store),
			Namespace: namespace,
			Labels:    commonLabels(store),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 commonLabels(store),
			Ports: []corev1.ServicePort{
				{Name: "rpc", Port: rpcPort, TargetPort: intstr.FromInt32(rpcPort)},
			},
		},
	}
}

// garageContainer is the Garage container spec. The data volume is a per-pod PVC
// bound to a node-sticky local PV, mounted whole at the Garage data directory.
func garageContainer(image string) corev1.Container {
	return corev1.Container{
		Name:    "garage",
		Image:   image,
		Command: []string{"/garage", "server"},
		Env: []corev1.EnvVar{
			{Name: "GARAGE_CONFIG_FILE", Value: configMount + "/" + configFileName},
			{Name: "GARAGE_RPC_SECRET", ValueFrom: secretKeyRef(secretKeyRPC)},
			{Name: "GARAGE_ADMIN_TOKEN", ValueFrom: secretKeyRef(secretKeyAdmin)},
		},
		Ports: []corev1.ContainerPort{
			{Name: "s3", ContainerPort: s3Port},
			{Name: "rpc", ContainerPort: rpcPort},
			{Name: "admin", ContainerPort: adminPort},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: configMount},
			{Name: "data", MountPath: dataMountPath},
		},
		ReadinessProbe: &corev1.Probe{
			// /health on the admin port, not a TCP connect on the S3 one.
			//
			// Garage binds S3 at startup, long before it has joined the mesh or been
			// given a role in the layout, so an open socket says the process is up
			// and nothing about whether it can serve. A readiness probe that answers
			// "yes" there is what lets the rollout gate above it count a pod as
			// ready while it cannot take a request.
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/health",
					Port: intstr.FromInt32(adminPort),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		},
		// The container had no security context of its own at all: it inherited
		// root from the pod and kept the runtime's default capability set on top of
		// a hostPath mount. See hardenedRoot.
		SecurityContext: hardenedRoot(),
	}
}

// secretKeyRef builds an EnvVarSource reading a key from the cluster secret.
// The secret name is patched in by the caller via the closure in podSpec.
func secretKeyRef(key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			Key: key,
			// LocalObjectReference.Name is filled in podSpec once the
			// cluster (hence secret name) is known.
		},
	}
}

// podSpec assembles the shared PodSpec. The data volume comes from the
// StatefulSet's volumeClaimTemplates (a PVC named "data"), so no extra volume is
// added here beyond the config ConfigMap.
func podSpec(store *backend.Store, image string) corev1.PodSpec {
	c := garageContainer(image)
	// Patch the secret name into the env sources now that we know the cluster.
	for i := range c.Env {
		if c.Env[i].ValueFrom != nil && c.Env[i].ValueFrom.SecretKeyRef != nil {
			c.Env[i].ValueFrom.SecretKeyRef.Name = secretName(store)
		}
	}

	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configName(store)},
				},
			},
		},
	}

	return corev1.PodSpec{
		ServiceAccountName: resourceName(store),
		ImagePullSecrets:   []corev1.LocalObjectReference{{Name: registryPullSecret}},
		// Garage must own its metadata/data directory. The base image runs as a
		// non-root user, but a hostPath-backed local PV directory is created
		// root:root by the kubelet, and fsGroup does not cover hostPath — so the
		// data-plane pod runs as root, consistent with how Deckhouse storage data
		// planes run.
		SecurityContext: &corev1.PodSecurityContext{
			RunAsUser:  ptr.To[int64](0),
			RunAsGroup: ptr.To[int64](0),
			FSGroup:    ptr.To[int64](0),
		},
		Containers: []corev1.Container{c},
		Volumes:    volumes,
	}
}

// hardenedRoot is the container security context for the data plane.
//
// root stays, and it is not optional: the directory behind a hostPath-backed PV
// is created by the kubelet as root:root, and fsGroup does not apply to hostPath.
// But root with no capabilities and no escalation is a much smaller surface than
// root with the default set, and Garage needs none of them — it listens on 3900,
// 3901 and 3903, all unprivileged, and writes to a volume root already owns.
//
// ReadOnlyRootFilesystem is deliberately not set. It is the one part of this that
// could stop the data plane rather than merely constrain it, if Garage or the
// restore script writes anywhere outside its mounts, and the e2e run that would
// answer that is currently blocked on the stand. Worth adding once it can be
// verified rather than assumed.
func hardenedRoot() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsUser:                ptr.To[int64](0),
		RunAsGroup:               ptr.To[int64](0),
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// nodeIdentitySecretName is the Secret holding each replica's persisted Garage
// node identity (keys node-<ord> and node-<ord>.pub), so a replica
// recycled onto another master keeps its stable node ID instead of generating a
// fresh one (which would strand the old ID as a dead node and churn the layout).
func nodeIdentitySecretName(store *backend.Store) string {
	return resourceName(store) + "-node-identity"
}

// restoreNodeKeyScript runs in the restore-node-key initContainer.
// It restores this ordinal's persisted Garage node identity (node_key /
// node_key.pub) from the mounted identity Secret into the metadata dir before
// Garage starts, so a replica recycled onto another master (fresh empty PV)
// rejoins with its stable node ID and re-syncs via anti-entropy. The Secret is
// optional: on the very first boot no key exists yet, so Garage generates one
// (which the controller then captures into the Secret).
var restoreNodeKeyScript = fmt.Sprintf(`set -eu
ord="${HOSTNAME##*-}"
meta="%s/meta"
src="%s/node-${ord}"
mkdir -p "$meta"
if [ -s "$src" ] && [ -s "${src}.pub" ]; then
  cp "$src" "$meta/node_key"
  cp "${src}.pub" "$meta/node_key.pub"
  chmod 0600 "$meta/node_key"
  echo "restored garage node identity for ordinal ${ord}"
else
  echo "no persisted identity for ordinal ${ord}; garage will generate one"
fi
`, dataMountPath, nodeIdentityMountPath)

// buildStatefulSet returns the data-plane StatefulSet: a desiredReplicas count
// backed by node-sticky local PVs on control-plane nodes.
// The replica count is independent of the master count — replicas spread
// one-per-node when several control-plane nodes exist (soft anti-affinity) and
// co-locate on the single master otherwise; in single-replica mode the one pod
// simply lands on whichever master the scheduler picks and stays there. Pods
// start in parallel (no ordinal barrier). Storage is a per-ordinal PVC on the
// WaitForFirstConsumer local StorageClass: the controller pre-creates a pool of
// hostPath-backed, nodeAffinity-pinned PVs (ensureLocalPVs), so each
// replica sticks to its node and finds its data again after a restart. This keeps
// a constant replication factor and Garage quorum across master-count changes;
// the layout is reconciled in mesh.go as replicas move and rejoin.
func buildStatefulSet(store *backend.Store, namespace, image, cfgHash string) *appsv1.StatefulSet {
	spec := podSpec(store, image) // data comes from volumeClaimTemplates

	// The data plane runs on control-plane nodes and must tolerate their taints.
	spec.NodeSelector = map[string]string{controlPlaneNodeLabel: ""}
	spec.Tolerations = []corev1.Toleration{
		{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
		{Key: "node-role.kubernetes.io/master", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
	// Prefer (not require) spreading replicas one-per-control-plane-node, so every
	// replica still schedules and runs when only a single master exists.
	spec.Affinity = &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{MatchLabels: commonLabels(store)},
					TopologyKey:   hostnameTopologyKey,
				},
			}},
		},
	}

	// Persist each replica's Garage node identity. When placement recycles a
	// replica onto another master its PVC is recreated empty, so without this the
	// pod would generate a brand-new node ID — stranding the old one as a dead
	// node and churning the layout (multi-version migrations that Garage cannot
	// always recover from). The initContainer restores node_key from the
	// per-ordinal identity Secret before Garage starts, so the replica rejoins
	// with its stable ID and Garage re-syncs the data via anti-entropy. The
	// Secret is Optional so the first boot (before any identity is captured) just
	// lets Garage generate one, which the controller then captures.
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name: "node-identity",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: nodeIdentitySecretName(store),
				Optional:   ptr.To(true),
			},
		},
	})
	spec.InitContainers = []corev1.Container{{
		Name:    "restore-node-key",
		Image:   image,
		Command: []string{"sh", "-c", restoreNodeKeyScript},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: dataMountPath},
			{Name: "node-identity", MountPath: nodeIdentityMountPath, ReadOnly: true},
		},
		SecurityContext: hardenedRoot(),
	}}

	replicas := desiredReplicas(store)
	sc := store.StorageClass
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(store),
			Namespace: namespace,
			Labels:    commonLabels(store),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         rpcSvcName(store),
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: commonLabels(store)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      commonLabels(store),
					Annotations: map[string]string{annConfigHash: cfgHash},
				},
				Spec: spec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: commonLabels(store)},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						StorageClassName: &sc,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: store.SizePerNode},
						},
					},
				},
			},
		},
	}
}

// shortHash is a filesystem/label-safe short digest of s (used to derive PV
// names and data-directory paths from an arbitrary node hostname).
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:10]
}

// localPVName is the deterministic name of the index-th pool PV pinned to node
// for the given incarnation. Deterministic naming makes the pool reconcile
// idempotent; carrying the incarnation keeps a recreated store from binding a
// leftover PV of the previous one (whose directory holds incompatible data).
// firstIncarnation keeps the plain, incarnation-less name.
func localPVName(store *backend.Store, node string, incarnation, index int32) string {
	if incarnation <= firstIncarnation {
		return fmt.Sprintf("%s-local-%s-%d", resourceName(store), shortHash(node), index)
	}
	return fmt.Sprintf("%s-local-%s-i%d-%d", resourceName(store), shortHash(node), incarnation, index)
}

// localPVPath is the on-node directory backing that pool PV. A recreated store
// (incarnation > firstIncarnation) gets a directory of its own, so it starts from
// empty storage instead of the previous incarnation's Garage metadata; the old
// directories are left on disk untouched (Retain semantics).
func localPVPath(store *backend.Store, node string, incarnation, index int32) string {
	if incarnation <= firstIncarnation {
		return fmt.Sprintf("%s/%s/%s-%d", hostPathBase, store.Name, shortHash(node), index)
	}
	return fmt.Sprintf("%s/%s/i%d/%s-%d", hostPathBase, store.Name, incarnation, shortHash(node), index)
}

// buildLocalPV builds one node-sticky local PV for the pool: a hostPath-backed
// volume (DirectoryOrCreate, so the kubelet creates the directory on first mount
// — no node agent needed) pinned to node via nodeAffinity, on the
// WaitForFirstConsumer local StorageClass, with Retain so PVC/PV deletion never
// wipes data. The capacity is nominal (hostPath does not enforce it).
func buildLocalPV(store *backend.Store, node string, incarnation, index int32) *corev1.PersistentVolume {
	hostPathType := corev1.HostPathDirectoryOrCreate
	labels := commonLabels(store)
	labels[labelSystemLocalNode] = node
	sc := store.StorageClass
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   localPVName(store, node, incarnation, index),
			Labels: labels,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: store.SizePerNode},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              sc,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: localPVPath(store, node, incarnation, index),
					Type: &hostPathType,
				},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      hostnameTopologyKey,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{node},
						}},
					}},
				},
			},
		},
	}
}
