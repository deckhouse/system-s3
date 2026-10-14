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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// The in-place change of the replica count.
//
// A change of the high-availability mode alone does not need a second Garage
// cluster. Garage refuses a replication_factor that differs from the one its
// saved layout was built for, and says itself how to get past it: remove
// meta/cluster_layout (rpc/layout/manager.rs). The node keeps its identity and
// its data; with a new layout the cluster starts over on the same metadata and
// the same blocks, and the replicas added or removed are filled or dropped by
// Garage's own synchronisation. Checked by hand on 28.09 before this was written:
// both directions, 2 GiB, every object compared by sha256, reads through a node
// that had just started empty all served whole.
//
// Growing (1 -> 3):
//
//	ResizeStop   the one replica is stopped — the store is down from here;
//	ResizePurge  cluster_layout is removed from its volume, garage.toml gets
//	             factor 3;
//	ResizeSync   three replicas start, the mesh builds a layout of three, the
//	             store serves again; the block repair is launched and the phase
//	             waits for every node to hold everything.
//
// The store is down only between the first two and the layout: there is nothing
// to freeze, the single replica is not running.
//
// Shrinking (3 -> 1) needs the replica that stays to hold everything, which it
// does only once nothing is being written and every node has caught up:
//
//	ResizeFreeze write access is taken from every key, and the phase waits for
//	             the synchronisation to be complete;
//	ResizeStop, ResizePurge as above, with factor 1;
//	ResizeSync   one replica, a layout of one; write access is given back and
//	             the volumes of the replicas that are gone are released.
//
// A change of the StorageClass, alone or together with the mode, is still a move
// to a second cluster (see migrate.go): the volumes cannot be re-ordered from
// another class in place.

// resizeFreezeLimit bounds how long the shrinking freeze waits for the cluster to
// be in sync. Past it, writes are given back and the phase tries again later: a
// platform's storage held read-only because a node is slow to catch up is worse
// than a shrink that happens an hour later.
const resizeFreezeLimit = 10 * time.Minute

// purgeJobSuffix names the Job that removes the layout from the carrying replica.
const purgeJobSuffix = "-purge-layout"

// resizeInPlace reports whether the change from the running plane to the
// settings can be made on the same cluster: the mode differs, and the class does
// not.
func resizeInPlace(desired, serving *backend.Store, boundClass string) bool {
	if desired.HighAvailability == serving.HighAvailability {
		return false
	}
	return boundClass == "" || desired.StorageClass == "" || boundClass == desired.StorageClass
}

// resizeTarget is the plane as it will run after the change.
func resizeTarget(store *backend.Store, state migrationState) *backend.Store {
	return planeOf(store, store.Incarnation, state.ha, store.StorageClass)
}

// startResize records the in-place change and names its first phase.
func (d *Driver) startResize(ctx context.Context, desired, serving *backend.Store, reason string) (migrationState, string, error) {
	growing := desired.HighAvailability && !serving.HighAvailability
	next := migrationState{
		serving:      serving.Incarnation,
		phase:        phaseResizeFreeze,
		ha:           desired.HighAvailability,
		storageClass: serving.StorageClass,
		startedAt:    time.Now(),
	}
	msg := fmt.Sprintf("%s: changing the replica count in place — writes are refused until the "+
		"replicas are in sync, then the store restarts with one replica", reason)
	if growing {
		next.phase = phaseResizeStop
		msg = fmt.Sprintf("%s: changing the replica count in place — the store restarts with three "+
			"replicas on the same data; it is unavailable until they are up", reason)
	}
	if err := d.saveMigrationState(ctx, serving, next); err != nil {
		return migrationState{}, "", err
	}
	d.log.Info("starting an in-place change of the replica count",
		"store", serving.Name, "reason", reason, "phase", string(next.phase))
	return next, msg, nil
}

// advanceResizeOffline runs the two phases in which the data plane is not
// running. They cannot wait for a healthy pass, since there is none to wait
// for, so EnsureStore calls this before it builds the workload and returns what
// it says.
func (d *Driver) advanceResizeOffline(ctx context.Context, store *backend.Store, state migrationState) (string, error) {
	switch state.phase {
	case phaseResizeStop:
		stopped, err := d.scaleToZero(ctx, store)
		if err != nil {
			return "", err
		}
		if !stopped {
			return "changing the replica count: stopping the data plane", nil
		}
		state.phase = phaseResizePurge
		if err := d.saveMigrationState(ctx, store, state); err != nil {
			return "", err
		}
		return "changing the replica count: the data plane is stopped", nil

	case phaseResizePurge:
		done, msg, err := d.purgeLayout(ctx, store)
		if err != nil || !done {
			return msg, err
		}
		// The one place the pinned factor changes. Everything below reads it
		// from here, so the next pass builds the workload, the layout and the
		// disruption budget for the new shape.
		target := resizeTarget(store, state)
		rf := initialReplicationFactor(target)
		if err := d.apply(ctx, buildConfigMap(target, d.namespace, rf)); err != nil {
			return "", fmt.Errorf("write the new replication factor: %w", err)
		}
		state.phase = phaseResizeSync
		if err := d.saveMigrationState(ctx, store, state); err != nil {
			return "", err
		}
		d.log.Info("the layout is purged; the data plane restarts with the new replication factor",
			"store", store.Name, "replicationFactor", rf)
		return fmt.Sprintf("changing the replica count: starting the data plane with replication factor %d", rf), nil
	}
	return "", nil
}

// resizeFreeze is the first phase of a shrink, run by a healthy pass.
//
// The freeze is taken only once the cluster reads as in sync, and the check is
// made again under it: a write that landed between the two is on two replicas
// of three, and possibly not on the one that stays.
func (d *Driver) resizeFreeze(ctx context.Context, desired, store *backend.Store, state migrationState) (migrationState, string, error) {
	if desired.HighAvailability == store.HighAvailability && state.frozenAt.IsZero() {
		// The settings went back before anything was touched.
		state = migrationState{serving: store.Incarnation, phase: phaseConverged}
		return state, "the change of the replica count is called off: the settings no longer ask for it",
			d.saveMigrationState(ctx, store, state)
	}
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil || !ready {
		return state, "", errOrNotReady(err, store, "check the synchronisation")
	}
	synced, why, err := clusterInSync(ctx, svc, desiredReplicas(store), d.blocksOnDisk(store))
	if err != nil {
		return state, "", err
	}

	if state.frozenAt.IsZero() {
		drained, err := d.writeGrantsDrained(ctx, store, svc)
		if err != nil {
			return state, "", err
		}
		if !drained {
			return state, "changing the replica count: waiting for pending STS write grants", nil
		}
		if !synced {
			return state, fmt.Sprintf("changing the replica count: waiting for the replicas to be in sync before writes are refused (%s)", why), nil
		}
		keys, err := d.freezeWritesRecording(ctx, svc)
		if err != nil {
			return state, "", err
		}
		state.frozenKeys, state.frozenAt = keys, time.Now()
		if err := d.saveMigrationState(ctx, store, state); err != nil {
			return state, "", err
		}
		return state, "changing the replica count: writes are refused while the replicas are checked once more", nil
	}

	if !synced {
		if time.Since(state.frozenAt) > resizeFreezeLimit {
			if err := d.giveBackWrites(ctx, svc, state.frozenKeys); err != nil {
				return state, "", err
			}
			state.frozenKeys, state.frozenAt = nil, time.Time{}
			if err := d.saveMigrationState(ctx, store, state); err != nil {
				return state, "", err
			}
			return state, fmt.Sprintf("changing the replica count: the replicas were not in sync within %s; "+
				"writes are allowed again and the change is tried later (%s)", resizeFreezeLimit, why), nil
		}
		return state, fmt.Sprintf("changing the replica count: writes are refused, waiting for the replicas to be in sync (%s)", why), nil
	}
	state.phase = phaseResizeStop
	if err := d.saveMigrationState(ctx, store, state); err != nil {
		return state, "", err
	}
	return state, "changing the replica count: the replicas are in sync; stopping the data plane", nil
}

// resizeSync is the last phase, run by a healthy pass of the new shape.
func (d *Driver) resizeSync(ctx context.Context, store *backend.Store, state migrationState) (migrationState, string, error) {
	svc, ready, err := d.adminClientFor(ctx, store)
	if err != nil || !ready {
		return state, "", errOrNotReady(err, store, "check the synchronisation")
	}
	replicas := desiredReplicas(store)

	if replicas > 1 && state.repairAt.IsZero() {
		if err := svc.repairBlocks(ctx); err != nil {
			return state, "", fmt.Errorf("launch the block repair: %w", err)
		}
		state.repairAt = time.Now()
		if err := d.saveMigrationState(ctx, store, state); err != nil {
			return state, "", err
		}
		return state, "changing the replica count: the store serves with three replicas; filling the new ones", nil
	}

	synced, why, err := clusterInSync(ctx, svc, replicas, d.blocksOnDisk(store))
	if err != nil {
		return state, "", err
	}
	if !synced {
		return state, fmt.Sprintf("changing the replica count: the store serves; filling the new replicas (%s)", why), nil
	}

	if len(state.frozenKeys) > 0 {
		if err := d.giveBackWrites(ctx, svc, state.frozenKeys); err != nil {
			return state, "", err
		}
	}
	if err := d.releaseRemovedReplicas(ctx, store, replicas); err != nil {
		return state, "", err
	}
	done := migrationState{serving: store.Incarnation, phase: phaseConverged}
	if err := d.saveMigrationState(ctx, store, done); err != nil {
		return state, "", err
	}
	d.log.Info("the replica count is changed", "store", store.Name, "replicas", replicas,
		"took", time.Since(state.startedAt).Round(time.Second).String())
	return done, fmt.Sprintf("the replica count is changed: %d replica(s), in sync", replicas), nil
}

// heldBlocks reads how many block files each node has on its volume, by node
// id.
type heldBlocks func(ctx context.Context) (map[string]int64, error)

// clusterInSync reports whether every node of the cluster holds everything it
// should: every node answers, no table has updates or inserts waiting, no block
// is failing to resync, and every node has a file for every block it counts a
// reference to.
//
// The last is what "holds everything" means, and it is read off the volumes
// because Garage has no counter for it. The resync queue is not that counter:
// every block written — by a consumer, not only by a sync — puts an entry on it
// that falls due 2 × rpc_timeout later (ten minutes, block/manager.rs) and is
// resolved as a no-op when the block is already there. Measured on the stand on
// 29.09: one 1 MiB object a second keeps some hundred entries queued on every
// node for as long as the writes go on. A gate that waited for an empty queue
// never opened on a store that is written to, which a platform's store always
// is. Every replica holds every block (the replication factor is the replica
// count), so a node with fewer block files than reference counts is a node that
// is still being filled.
func clusterInSync(ctx context.Context, svc *adminClient, want int32, held heldBlocks) (bool, string, error) {
	stats, err := svc.nodeStatistics(ctx)
	if err != nil {
		return false, "", fmt.Errorf("read the node statistics: %w", err)
	}
	if synced, why := statisticsInSync(stats, want); !synced {
		return false, why, nil
	}
	files, err := held(ctx)
	if err != nil {
		return false, "", fmt.Errorf("count the block files on the volumes: %w", err)
	}
	synced, why := blocksHeld(stats, files)
	return synced, why, nil
}

// blocksHeld compares each node's block files with the blocks it counts.
//
// A node that holds as many files as the fullest node holds everything too, even
// with fewer files than it counts. Garage keeps a reference count for a block
// whose last object was deleted (Deletable) until the block file is removed — and
// it removes that entry only while removing the file. A node that was away while
// an object was written and deleted learns the deletion from the table sync but
// never had the file, so the entry stays for good (block/resync.rs: the
// "offloading and deleting" branch is the only caller of
// clear_deleted_block_rc). On a store that consumers delete from, which a
// platform's store is, a replica that restarted at the wrong moment would
// otherwise never count as full: the cutover froze writes, gave up after its
// deadline and froze them again, the drain kept the old cluster and the replica
// change never finished — found on the load-test stand on 14.10.
//
// The fullest node is the yardstick because every replica holds every block (the
// replication factor is the replica count), and a node still being filled has
// fewer files than its peers whatever it counts. heldBlocksSlack allows for the
// blocks written between the reads of the volumes and for a peer that still has
// the files of deleted blocks waiting for Garage's collection delay.
func blocksHeld(stats *nodeStatisticsResponse, files map[string]int64) (bool, string) {
	ids := make([]string, 0, len(stats.Success))
	for id := range stats.Success {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var fullest int64
	for _, id := range ids {
		if have, ok := files[id]; ok && have > fullest {
			fullest = have
		}
	}
	slack := max(heldBlocksSlackMin, fullest/heldBlocksSlackFraction)
	var short []string
	for _, id := range ids {
		rc := stats.Success[id].BlockManagerStats.RCEntries
		have, ok := files[id]
		switch {
		case !ok:
			short = append(short, fmt.Sprintf("node %s: its volume could not be read", shortID(id)))
		case have >= rc, have >= fullest-slack:
		default:
			short = append(short, fmt.Sprintf("node %s holds %d of %d blocks", shortID(id), have, rc))
		}
	}
	if len(short) > 0 {
		return false, strings.Join(short, "; ")
	}
	return true, ""
}

// heldBlocksSlackMin and heldBlocksSlackFraction bound how far a node may fall
// behind the fullest one and still count as full: two blocks, or a thousandth of
// the fullest node's, whichever is more. Small enough that an empty replica of
// even a small store never passes.
const (
	heldBlocksSlackMin      = 2
	heldBlocksSlackFraction = 1000
)

// blocksOnDisk counts the block files on the volume of every running replica,
// keyed by the node id its admin API reports.
//
// A block is one file under data/<xx>/<yy>/; the depth leaves out the marker
// Garage keeps at the top of data/. The count is a find over the directory,
// which is cheap on a platform's store and is run only while a change waits on
// it.
func (d *Driver) blocksOnDisk(store *backend.Store) heldBlocks {
	return func(ctx context.Context) (map[string]int64, error) {
		token, err := d.adminToken(ctx, store)
		if err != nil {
			return nil, err
		}
		pods, err := d.planePods(ctx, store)
		if err != nil {
			return nil, err
		}
		out := map[string]int64{}
		for i := range pods {
			pod := &pods[i]
			if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
				continue
			}
			st, err := d.admin(store, podAdminURL(pod.Status.PodIP), token).status(ctx)
			if err != nil || st.Node == "" {
				continue // reported as "could not be read" against its node id
			}
			raw, err := d.execInPod(ctx, pod.Name, "garage", []string{"sh", "-c",
				"find " + dataMountPath + "/data -mindepth 3 -type f | wc -l"})
			if err != nil {
				return nil, err
			}
			n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("pod %s counted %q block files", pod.Name, strings.TrimSpace(string(raw)))
			}
			out[st.Node] = n
		}
		return out, nil
	}
}

// statisticsInSync is the half of clusterInSync the statistics alone decide.
func statisticsInSync(stats *nodeStatisticsResponse, want int32) (bool, string) {
	if len(stats.Error) > 0 {
		nodes := make([]string, 0, len(stats.Error))
		for id := range stats.Error {
			nodes = append(nodes, shortID(id))
		}
		sort.Strings(nodes)
		return false, fmt.Sprintf("node(s) %s did not answer", strings.Join(nodes, ", "))
	}
	if int32(len(stats.Success)) < want {
		return false, fmt.Sprintf("%d of %d nodes answered", len(stats.Success), want)
	}
	var errors, tableWork int64
	for id, s := range stats.Success {
		if s.BlockManagerStats == nil || s.TableStats == nil {
			return false, fmt.Sprintf("node %s reports no statistics to judge by", shortID(id))
		}
		errors += s.BlockManagerStats.ResyncErrors
		for _, t := range s.TableStats {
			tableWork += t.MerkleQueueLen + t.InsertQueueLen
		}
	}
	if errors > 0 {
		return false, fmt.Sprintf("%d block(s) failing to resync", errors)
	}
	if tableWork > 0 {
		return false, fmt.Sprintf("%d table update(s) pending", tableWork)
	}
	return true, ""
}

func shortID(id string) string {
	if len(id) > 16 {
		return id[:16]
	}
	return id
}

// freezeWritesRecording takes write access from every key of every bucket and
// returns which ones had it, so that exactly those get it back.
func (d *Driver) freezeWritesRecording(ctx context.Context, svc *adminClient) ([]frozenKey, error) {
	buckets, err := svc.listBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the buckets: %w", err)
	}
	var frozen []frozenKey
	for _, b := range buckets {
		info, err := svc.getBucketByID(ctx, b.ID)
		if err != nil {
			return nil, fmt.Errorf("read bucket %s: %w", b.ID, err)
		}
		for _, k := range info.Keys {
			if !k.Permissions.Write {
				continue
			}
			if err := svc.deny(ctx, b.ID, k.AccessKeyID, permissions{Write: true}); err != nil {
				return nil, fmt.Errorf("revoke write access of key %s on bucket %s: %w", k.AccessKeyID, b.ID, err)
			}
			frozen = append(frozen, frozenKey{Bucket: b.ID, Key: k.AccessKeyID})
		}
	}
	return frozen, nil
}

// giveBackWrites restores write access to the keys a freeze took it from. A key
// that is gone in the meantime — a temporary key that expired and was swept — is
// skipped: there is nothing to give back to.
func (d *Driver) giveBackWrites(ctx context.Context, svc *adminClient, keys []frozenKey) error {
	for _, k := range keys {
		exists, err := svc.keyExists(ctx, k.Key)
		if err != nil {
			return fmt.Errorf("look up key %s: %w", k.Key, err)
		}
		if !exists {
			continue
		}
		if err := svc.allow(ctx, k.Bucket, k.Key, permissions{Write: true}); err != nil {
			return fmt.Errorf("give write access back to key %s on bucket %s: %w", k.Key, k.Bucket, err)
		}
	}
	return nil
}

// scaleToZero stops the data plane and reports whether its pods are gone.
func (d *Driver) scaleToZero(ctx context.Context, store *backend.Store) (bool, error) {
	sts := &appsv1.StatefulSet{}
	err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: resourceName(store)}, sts)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return false, fmt.Errorf("read the StatefulSet: %w", err)
	case sts.Spec.Replicas == nil || *sts.Spec.Replicas != 0 || sts.Spec.PersistentVolumeClaimRetentionPolicy == nil ||
		sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType ||
		sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType:
		sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}
		sts.Spec.Replicas = ptr.To(int32(0))
		if err := d.client.Update(ctx, sts); err != nil {
			return false, fmt.Errorf("scale the data plane to zero: %w", err)
		}
	}
	pods, err := d.planePods(ctx, store)
	if err != nil {
		return false, err
	}
	return len(pods) == 0, nil
}

// purgeLayout removes cluster_layout from the volume of replica 0, the one that
// carries the data over, with a Job in the Garage image. It reports whether that
// is done, cleaning the Job up once it is.
func (d *Driver) purgeLayout(ctx context.Context, store *backend.Store) (bool, string, error) {
	name := resourceName(store) + purgeJobSuffix
	// Read past the cache, as the copier's Job is: a cached read would start a
	// cluster-wide Job informer, which the controller's namespaced RBAC does not
	// allow to list, and the Get would wait for it until the pass times out.
	job := &batchv1.Job{}
	err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: name}, job)
	switch {
	case apierrors.IsNotFound(err):
		if err := d.client.Create(ctx, d.buildPurgeJob(store, name)); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, "", fmt.Errorf("create the layout purge Job: %w", err)
		}
		return false, "changing the replica count: removing the saved layout from the data volume", nil
	case err != nil:
		return false, "", fmt.Errorf("read the layout purge Job: %w", err)
	}
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			if err := d.deleteCopyJob(ctx, job); err != nil {
				return false, "", err
			}
			return true, "", nil
		case batchv1.JobFailed:
			// Said and retried: a Job that could not run (the volume would not
			// attach, the image would not pull) is a fact about the cluster, and
			// the next pass starts a fresh one.
			if err := d.deleteCopyJob(ctx, job); err != nil {
				return false, "", err
			}
			return false, fmt.Sprintf("changing the replica count: removing the saved layout failed (%s); retrying", c.Message), nil
		}
	}
	return false, "changing the replica count: removing the saved layout from the data volume", nil
}

// buildPurgeJob is the Job that removes the saved layout from replica 0's volume.
//
// The Garage image carries a shell (the node-identity init container uses it);
// the volume is mounted where Garage mounts it. The Job lands where the volume
// can be attached: a node-pinned volume takes its pod to its node.
func (d *Driver) buildPurgeJob(store *backend.Store, name string) *batchv1.Job {
	layout := dataMountPath + "/meta/cluster_layout"
	script := fmt.Sprintf("set -e\nrm -f %s\ntest ! -e %s\necho removed %s", layout, layout, layout)
	spec := corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: ptr.To(false),
		ImagePullSecrets:             []corev1.LocalObjectReference{{Name: registryPullSecret}},
		Containers: []corev1.Container{{
			Name:            "purge",
			Image:           d.image,
			Command:         []string{"sh", "-c", script},
			SecurityContext: hardenedRoot(),
			VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: dataMountPath}},
		}},
		Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: fmt.Sprintf("data-%s-0", resourceName(store)),
			}},
		}},
	}
	d.placement.applyTo(&spec)
	// The Job carries the store's labels, so teardown finds it; its pod does not
	// claim to be Garage, so nothing that looks for the replicas (planePods) finds
	// it among them.
	podLabels := commonLabels(store)
	podLabels["app.kubernetes.io/name"] = "garage-purge-layout"
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: d.namespace, Labels: commonLabels(store)},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To(int32(2)),
			Template:     corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}, Spec: spec},
		},
	}
}

// releaseRemovedReplicas deletes the claims of replicas the new shape does not
// have. After a shrink those hold the copies of replicas that are gone; kept,
// they would be picked up again by the next growth with a stale copy on them.
func (d *Driver) releaseRemovedReplicas(ctx context.Context, store *backend.Store, replicas int32) error {
	for ord := replicas; ord < replicasHA; ord++ {
		pvc := &corev1.PersistentVolumeClaim{}
		pvc.Name, pvc.Namespace = fmt.Sprintf("data-%s-%d", resourceName(store), ord), d.namespace
		if err := d.client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("release the volume of replica %d: %w", ord, err)
		}
	}
	return nil
}
