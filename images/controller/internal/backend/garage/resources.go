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
	"net"
	"regexp"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
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

// proxyMetricsPort is where the TLS terminator serves its own traffic counters,
// and proxyMetricsPortName the container port's name templates/garage-podmonitor.yaml
// scrapes by.
const (
	proxyMetricsPort     = 3904
	proxyMetricsPortName = "proxy-metrics"
)

// Fixed paths inside the Garage container.
const (
	dataMountPath  = "/var/lib/garage"
	configMount    = "/etc/garage"
	configFileName = "garage.toml"

	// registryPullSecret is the module-scoped pull secret rendered by
	// templates/registry-secret.yaml. The Garage image comes from the module's
	// own ModuleSource (helm_lib_module_image), so its credentials live here —
	// not in `deckhouse-registry`, which mirrors the global modules registry and
	// carries no credentials for a separate ModuleSource.
	registryPullSecret = "system-s3-module-registry"

	// hostnameTopologyKey is the node label carrying the hostname, used for the
	// required pod anti-affinity that separates HA replicas.
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

// replicasHA is the number of Garage replicas the highly available store runs,
// independent of how many nodes the placement allows. Keeping it constant
// (rather than one pod per eligible node) keeps the pinned replication factor and
// the Garage quorum stable as nodes come and go: three replicas are desired,
// with required anti-affinity on different eligible hosts. Fewer eligible hosts
// leave replicas Pending; placement does not recycle volumes in an attempt to
// consolidate those replicas. Reduce the replication factor only through the
// explicit highAvailability setting and its coordinated in-place transition.
//
// See nextPlacementAction in rebalance.go, which implements exactly these rules.
const replicasHA int32 = 3

// replicasNonHA is the replica count outside the high-availability mode: one
// Garage replica with replication_factor 1. It gives up all redundancy — the
// store is unavailable while its pod is down and lost with its node's disk —
// in exchange for a third of the footprint, and it never migrates between
// nodes (see reconcilePlacement).
const replicasNonHA int32 = 1

// desiredReplicas is the number of Garage data-plane pods: replicasHA in the
// high-availability mode, replicasNonHA otherwise. Both counts are independent of
// how many nodes the placement allows. It also drives the initial replication factor
// (clamped to this count) and the layout reconcile.
//
// This is desired state, and for an existing store it is not necessarily what
// runs: Garage refuses a replication_factor change on a live cluster, so a store
// built in one mode keeps its replica count until the change of the replica
// count (see resize.go) restarts it in the other.
func desiredReplicas(store *backend.Store) int32 {
	if store.HighAvailability {
		return replicasHA
	}
	return replicasNonHA
}

// incarnationKey is the ConfigMap key recording which data plane the config
// belongs to. It is a record, not a source of truth — the object's own name says
// the same thing — and it is there so that a namespace holding two Garage
// clusters mid-migration can be read without decoding names.
//
// Which plane SERVES is kept elsewhere entirely (see migrationStateName): it is a
// property of the store, and a per-plane ConfigMap cannot be asked a question
// about the store.
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
// node. Required anti-affinity separates live HA pods, but a node-pinned PV or
// other scheduling constraints may still prevent relocation. Without a cap
// that would be an unbounded retry
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

// planeIncarnation is the incarnation a store description refers to, with the
// zero value reading as the first — a store built before migrations existed has
// one data plane and it is that one.
func planeIncarnation(store *backend.Store) int32 {
	if store.Incarnation < firstIncarnation {
		return firstIncarnation
	}
	return store.Incarnation
}

// storeLabel carries the store name on every object the driver creates (used as
// a selector, e.g. to list the local-PV pool).
const storeLabel = backend.StoreLabel

// resourceName is the common name/prefix for the objects backing ONE data plane.
//
// It carries the incarnation from the second one on, because a migration runs two
// Garage clusters side by side in one namespace: the one serving and the one being
// filled. The first incarnation keeps the bare name, so an installation that has
// never migrated is not renamed underneath itself — and the endpoint consumers
// use is not this name at all (see s3SvcName).
func resourceName(store *backend.Store) string {
	if planeIncarnation(store) == firstIncarnation {
		return store.Name + "-garage"
	}
	return fmt.Sprintf("%s-garage-i%d", store.Name, planeIncarnation(store))
}

func secretName(store *backend.Store) string {
	return resourceName(store) + "-secrets"
}
func configName(store *backend.Store) string {
	return resourceName(store) + "-config"
}
func rpcSvcName(store *backend.Store) string { return resourceName(store) + "-rpc" }

// planeSvcName is the S3/admin Service of ONE data plane, and it is the module's
// own way in.
//
// Consumers never use it — they use the endpoint, which points at whichever plane
// serves. The module does, for everything it does TO a plane: a bucket created
// during a migration is created on the plane being filled, and an admin call that
// went through the endpoint instead would land on whichever plane the endpoint
// happened to be pointing at when the call was made.
func planeSvcName(store *backend.Store) string { return resourceName(store) + "-api" }

// stableName is the name prefix of the objects that belong to the STORE rather
// than to one of its data planes, and it therefore never carries the incarnation.
//
// There are few of them, and each is there because a migration must not lose it:
// what a bucket is for outlives the cluster the bucket currently lives on.
func stableName(store *backend.Store) string { return store.Name + "-garage" }

// s3SvcName is the endpoint every consumer holds, and it NEVER carries the
// incarnation.
//
// That is the whole reason a migration can be invisible to them: the Service
// keeps its name and its cluster IP, and what changes at cutover is which pods it
// selects. A consumer's S3_ENDPOINT is written into its credentials Secret once
// and stays true across a migration it never hears about.
func s3SvcName(store *backend.Store) string { return stableName(store) }

// incarnationLabel is on the pods of a data plane and in the selector of the
// endpoint Service. It is what makes the cutover a single write: the Service
// stops selecting the old cluster's pods and starts selecting the new one's.
const incarnationLabel = "storage.deckhouse.io/incarnation"

// planeLabels are the pod labels of one incarnation: the common ones the
// StatefulSet selects on, plus the incarnation the endpoint Service selects on.
//
// The two are separate on purpose. A StatefulSet's selector is immutable, so the
// labels it matches cannot grow a field later; the endpoint Service's selector is
// not, which is exactly what the cutover rewrites.
func planeLabels(store *backend.Store) map[string]string {
	labels := commonLabels(store)
	labels[incarnationLabel] = strconv.FormatInt(int64(planeIncarnation(store)), 10)
	return labels
}

// incarnationOfPod reads which data plane a pod belongs to.
//
// A pod with no such label belongs to the first incarnation, the same way a
// ConfigMap with no incarnation key does: an installation made before the label
// existed has exactly one data plane, and it is that one. Without this reading a
// rolling update would look like a pod of no plane at all, and the pass would
// stop finding the replicas it is meshing.
func incarnationOfPod(pod *corev1.Pod) int32 {
	n, err := strconv.Atoi(pod.Labels[incarnationLabel])
	if err != nil || n < int(firstIncarnation) {
		return firstIncarnation
	}
	return int32(n)
}

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

// s3Endpoint is the in-cluster S3 URL consumers are given: the endpoint Service,
// which survives a migration. https: the port is the TLS terminator's.
func s3Endpoint(store *backend.Store, namespace, clusterDomain string) string {
	return fmt.Sprintf("https://%s.%s.svc.%s:%d", s3SvcName(store), namespace, clusterDomain, s3Port)
}

// planeS3HostPort is the S3 endpoint of one plane as host:port (no scheme), for
// the minio/S3 client used to empty buckets before deletion and to copy between
// two planes.
func planeS3HostPort(store *backend.Store, namespace, clusterDomain string) string {
	return fmt.Sprintf("%s.%s.svc.%s:%d", planeSvcName(store), namespace, clusterDomain, s3Port)
}

// adminEndpoint is the admin API URL of one plane. Never the endpoint Service:
// an admin call is always made TO a particular Garage cluster.
func adminEndpoint(store *backend.Store, namespace, clusterDomain string) string {
	return fmt.Sprintf("https://%s.%s.svc.%s:%d", planeSvcName(store), namespace, clusterDomain, adminPort)
}

// podAdminURL is the admin API of one Garage node, at the pod's own address.
func podAdminURL(podIP string) string {
	return fmt.Sprintf("https://%s", net.JoinHostPort(podIP, strconv.Itoa(adminPort)))
}

// replicationFactor is the Garage replication_factor the module settings ask
// for: 3 in the high-availability mode, 1 otherwise. This is only the initial intent
// used at cluster creation; the effective factor is clamped to the node count and
// then PINNED for the cluster's lifetime (see Driver.pinnedState). Garage does
// not support changing replication_factor on a live cluster, so it must never be
// recomputed from the current node count afterwards.
func replicationFactor(store *backend.Store) int32 {
	if store.HighAvailability {
		return 3
	}
	return 1
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

// configHash is a short content hash of garage.toml, stamped onto the pod
// template so the workload rolls when the config changes (Garage reads the file
// only at startup, and every node must run the same replication_factor).
func configHash(cfg string) string {
	sum := sha256.Sum256([]byte(cfg))
	return hex.EncodeToString(sum[:])[:16]
}

// renderConfig produces the garage.toml content for the given (already clamped)
// replication factor. Secrets (rpc, admin and metrics tokens) are supplied via
// environment variables, not the file.
//
// The S3 and admin APIs listen on the loopback, on ports of their own: the TLS
// terminator in the same pod owns the ports clients dial (see tls.go). Garage
// has no TLS of its own, and a plaintext listener on the pod's address would be
// a way around the terminator for anything that can reach the pod.
func renderConfig(rf int32) string {
	return fmt.Sprintf(`metadata_dir = "%s/meta"
data_dir = "%s/data"
db_engine = "lmdb"

replication_factor = %d

rpc_bind_addr = "[::]:%d"

[s3_api]
s3_region = "garage"
api_bind_addr = "127.0.0.1:%d"
root_domain = ".s3.garage"

[admin]
api_bind_addr = "127.0.0.1:%d"
`, dataMountPath, dataMountPath, rf, rpcPort, garageS3Loopback, garageAdminLoopback)
}

// buildConfigMap returns the ConfigMap holding garage.toml with the given
// replication factor. It also records which data plane the config belongs to;
// that key lives outside garage.toml so it is not part of the hash the pods roll
// on.
func buildConfigMap(store *backend.Store, namespace string, rf int32) *corev1.ConfigMap {
	data := map[string]string{
		configFileName: renderConfig(rf),
		incarnationKey: strconv.FormatInt(int64(planeIncarnation(store)), 10),
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

// buildEndpointService returns the Service consumers hold: one object for the
// life of the store, whose selector is the whole of a cutover.
//
// The selector is passed in rather than derived here because it is the one thing
// about this Service that is not a property of the store. While one plane runs it
// is the store's labels — which is also what an installation upgraded into this
// code already has, so nothing is re-selected under a running cluster. While two
// planes run it names one of them.
func buildEndpointService(store *backend.Store, namespace string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s3SvcName(store),
			Namespace: namespace,
			Labels:    commonLabels(store),
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: "s3", Port: s3Port, TargetPort: intstr.FromInt32(s3Port)},
				{Name: "admin", Port: adminPort, TargetPort: intstr.FromInt32(adminPort)},
			},
		},
	}
}

// buildPodDisruptionBudget keeps a voluntary eviction from taking a second
// replica while the first is still coming back.
//
// maxUnavailable: 1 is the whole policy, and it is only worth having in the
// highly available mode: with three replicas at replication factor 3 the cluster
// keeps a quorum while one is away and loses it at two, and a rolling update of
// the control plane drains nodes one after another without asking whether the
// previous workload has recovered. The budget is what makes it ask.
//
// It rests on the readiness probe meaning "in the cluster and able to serve"
// rather than "listening" — a budget counting processes rather than members
// would let the second eviction through the moment the first pod accepted a
// connection, which is well before it has anything to serve.
//
// Per plane, because each plane is its own Garage cluster with its own quorum.
func buildPodDisruptionBudget(store *backend.Store, namespace string) *policyv1.PodDisruptionBudget {
	maxUnavailable := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(store),
			Namespace: namespace,
			Labels:    planeLabels(store),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector:       &metav1.LabelSelector{MatchLabels: planeLabels(store)},
		},
	}
}

// buildPlaneService returns the S3/admin Service of one data plane — the module's
// own way into a particular Garage cluster, alongside the endpoint that points at
// whichever one serves.
func buildPlaneService(store *backend.Store, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      planeSvcName(store),
			Namespace: namespace,
			Labels:    planeLabels(store),
		},
		Spec: corev1.ServiceSpec{
			Selector: planeLabels(store),
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
			Selector:                 planeLabels(store),
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
			// Setting it is what closes /metrics: without a metrics token Garage
			// serves the endpoint to anyone who reaches the admin port.
			{Name: "GARAGE_METRICS_TOKEN", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: metricsTokenSecret},
					Key:                  metricsTokenKey,
				},
			}},
		},
		// Only RPC. The S3 and admin ports clients dial belong to the TLS
		// terminator's container; Garage's own listeners are on the loopback.
		Ports: []corev1.ContainerPort{
			{Name: "rpc", ContainerPort: rpcPort},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: configMount},
			{Name: "data", MountPath: dataMountPath},
		},
		ReadinessProbe: &corev1.Probe{
			// /health on the admin port: 200 once this node holds a role in the
			// layout and can reach enough of its peers to serve.
			//
			// That is a stronger statement than "the process is listening", and the
			// disruption budget is what needs it. A drain evicts a replica, the
			// StatefulSet brings it back, and the budget must not let the next
			// eviction through until the replacement is back IN THE CLUSTER — not
			// merely accepting connections. On a rolling update of the control plane
			// that is the difference between one replica down at a time and a quorum
			// lost.
			//
			// It used to be a TCP connect because /health deadlocked the bootstrap:
			// the layout was assigned through a Service, a Service routes to ready
			// endpoints only, and no pod could become ready until the layout was
			// assigned. An e2e run sat at 0/3 for 35 minutes. The layout is now
			// assigned at a pod's own address (see ensureMeshAndLayout), so the
			// deadlock is gone and the probe can say what it should.
			//
			// No token: /health is the one admin endpoint Garage serves
			// unauthenticated, exactly so that a probe can use it.
			//
			// Through the TLS terminator, which is the only way to the admin API from
			// outside the pod. The kubelet does not verify the certificate of an HTTPS
			// probe, and does not need to: it is asking its own pod. A terminator that
			// is down fails the probe too, which is right — the node is not reachable.
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/health",
					Port:   intstr.FromInt32(adminPort),
					Scheme: corev1.URISchemeHTTPS,
				},
			},
			// Generous, because becoming ready waits on the controller's next pass
			// rather than on this process: the pod starts, answers 503 until the
			// layout reaches it, and that is a normal first minute rather than a
			// fault.
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    30,
		},
		Resources: corev1.ResourceRequirements{Requests: garageRequests()},
		// The container had no security context of its own at all: it inherited root
		// from the pod and kept the runtime's default capability set. See
		// hardenedRoot.
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
		if c.Env[i].ValueFrom != nil && c.Env[i].ValueFrom.SecretKeyRef != nil && c.Env[i].ValueFrom.SecretKeyRef.Name == "" {
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
		// Garage must own its metadata/data directory, and it runs as root to get
		// it. With a hostPath-backed volume that was forced — the kubelet creates
		// the directory root:root and fsGroup does not apply to hostPath. On a
		// PersistentVolumeClaim from a StorageClass fsGroup does apply, so the pod
		// could run as the image's non-root user instead. That is a change to the
		// data plane's identity on disk, not a comment fix: an existing store's
		// files are owned by root, so it needs a migration of its own and a run on
		// the stand. Left as it is, deliberately, and consistent with how Deckhouse
		// storage data planes run.
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
// root with no capabilities and no escalation is a much smaller surface than
// root with the default set, and Garage needs none of them — it listens on 3900,
// 3901 and 3903, all unprivileged, and writes to a volume root already owns. Why
// root at all is explained on the pod security context in podSpec.
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
// recycled onto another node keeps its stable node ID instead of generating a
// fresh one (which would strand the old ID as a dead node and churn the layout).
func nodeIdentitySecretName(store *backend.Store) string {
	return resourceName(store) + "-node-identity"
}

// restoreNodeKeyScript runs in the restore-node-key initContainer.
// It restores this ordinal's persisted Garage node identity (node_key /
// node_key.pub) from the mounted identity Secret into the metadata dir before
// Garage starts, so a replica recycled onto another node (fresh empty PV)
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
// of replicas, each with a PersistentVolumeClaim of its own from the module's
// StorageClass. Which nodes they may run on is not decided here: the driver puts
// the chart's placement on the result (see placement.go).
//
// The replica count is independent of how many nodes the placement allows.
// HA replicas require distinct eligible hosts; with fewer than three hosts
// some pods remain Pending rather than creating false redundancy on one node.
// Pods start in parallel (no ordinal barrier). This keeps a
// constant replication factor and Garage quorum as nodes come and go; the layout
// is reconciled in mesh.go as replicas move and rejoin.
//
// What the volume does when a replica moves is the StorageClass's business, not
// this module's: a class whose volumes follow the pod carries the replica's data
// with it, and a node-pinned class leaves it behind to be rebuilt by anti-entropy
// from the other replicas. The placement reconcile reads which of the two it is
// from the bound volume and moves replicas accordingly (see rebalance.go).
func buildStatefulSet(store *backend.Store, namespace, image, cfgHash string, volumeSize resource.Quantity) *appsv1.StatefulSet {
	spec := podSpec(store, image) // data comes from volumeClaimTemplates

	// Where the data plane may run is the placement's (see withPlacement); the
	// affinity carries only the spreading rules below.
	spec.Affinity = &corev1.Affinity{}

	// Spread the replicas across the eligible nodes, as far as the cluster allows.
	//
	// Both rules below select on planeLabels — the store's labels AND its
	// incarnation — and that scoping is the load-bearing part, not a detail. A
	// migration runs two Garage clusters side by side on the same nodes for as
	// long as the copy takes, and each has to spread over the same small set of
	// nodes. A rule that selected on the store alone would make the two planes
	// repel each other: the second plane's replicas would score every node as
	// occupied, pile onto one node, or with a hard rule not schedule at all —
	// and the migration, which is how this module replaces a data plane, would
	// deadlock on its own placement policy. Each plane spreads itself; neither
	// has an opinion about the other.
	//
	// Like helm_lib_pod_anti_affinity_for_ha, the HA anti-affinity is required.
	// The soft spread constraint remains a scoring aid, not an escape from it.
	//
	// The anti-affinity says "not on a node that already has one of mine" — a
	// repulsion, evaluated per pod, which is what keeps the first replicas apart.
	// The spread constraint says "keep the counts per node within one", which is
	// the same intent stated over the whole set: it is what stops three replicas
	// landing 2+1 on three nodes when the scheduler scores two of them equally.
	// Honouring node affinity and taints makes the skew count only the nodes this
	// pod could actually land on — the placement's — instead of every node in the
	// cluster, which would make the constraint satisfiable while doing nothing.
	if desiredReplicas(store) > 1 {
		spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: planeLabels(store)},
				TopologyKey:   hostnameTopologyKey,
			}},
		}
	}
	spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:            1,
		TopologyKey:        hostnameTopologyKey,
		WhenUnsatisfiable:  corev1.ScheduleAnyway,
		LabelSelector:      &metav1.LabelSelector{MatchLabels: planeLabels(store)},
		NodeAffinityPolicy: ptr.To(corev1.NodeInclusionPolicyHonor),
		NodeTaintsPolicy:   ptr.To(corev1.NodeInclusionPolicyHonor),
	}}

	// Persist each replica's Garage node identity. When placement recycles a
	// replica onto another node its PVC is recreated empty, so without this the
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
			// The selector deliberately does NOT carry the incarnation, and it is
			// the one place that does not. A StatefulSet's selector is immutable,
			// so narrowing it would mean recreating the workload of every existing
			// installation just to add a label. It is safe to leave wide: a
			// StatefulSet only ever claims pods whose NAME says they are its own
			// (the ordinal's parent must equal the set's name), and the two planes
			// have different names, so their sets do not reach for each other's
			// pods however alike their selectors look.
			Selector: &metav1.LabelSelector{MatchLabels: commonLabels(store)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      planeLabels(store),
					Annotations: map[string]string{annConfigHash: cfgHash},
				},
				Spec: spec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: planeLabels(store)},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						StorageClassName: &sc,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: volumeSize},
						},
					},
				},
			},
		},
	}
}
