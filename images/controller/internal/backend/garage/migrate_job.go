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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
	"github.com/deckhouse/system-s3/images/controller/internal/migrator"
)

// migrationKeyDisplayName is the display name of the key the copier uses in each
// cluster. It is not a consumer's key and never leaves the module: it is created
// with access to every bucket of its own cluster, and it dies with the migration.
const migrationKeyDisplayName = "system-s3-migration"

// Keys of the Secret holding the copier's credentials for both clusters.
const (
	migrationSecretSourceID     = "source-access-key-id"
	migrationSecretSourceSecret = "source-secret-access-key"
	migrationSecretTargetID     = "target-access-key-id"
	migrationSecretTargetSecret = "target-secret-access-key"
)

// annCopyBuckets stamps the copier Job with the buckets it was started for. A
// bucket created while the bulk copy runs is not in the running Job's list, and
// a Job cannot be edited — so the stamp is compared on every pass and a Job that
// no longer covers every bucket is replaced.
const annCopyBuckets = "storage.deckhouse.io/copy-buckets"

// migrationKeysName is the Secret holding the copier's credentials. One per
// store: a migration has one copier at a time, and the next migration overwrites
// it.
func migrationKeysName(store *backend.Store) string {
	return stableName(store) + "-migration-keys"
}

// copyJobName is the Job of one copy pass. The bulk pass and the delta pass at
// cutover are separate Jobs so that the delta is not confused with the bulk copy
// that has already succeeded — a completed Job is the signal each phase waits on,
// and one object cannot hold two answers.
func copyJobName(store *backend.Store, incarnation int32, pass string) string {
	return fmt.Sprintf("%s-copy-i%d-%s", stableName(store), incarnation, pass)
}

const (
	copyPassBulk  = "bulk"
	copyPassDelta = "delta"
)

// keyPair is an S3 credential.
type keyPair struct {
	AccessKeyID     string
	SecretAccessKey string
}

// ensureMigrationKeys puts the copier's credentials for both clusters in a
// Secret, creating the keys when they are missing and granting them access to
// every bucket.
//
// The pair is kept in a Secret because Garage hands a secret key back exactly
// once. Both halves are checked against their cluster on every pass: a key that
// was removed there — or a Secret left over from a previous migration, pointing
// at a cluster that no longer exists — is replaced rather than used to build a
// copier that cannot authenticate.
func (d *Driver) ensureMigrationKeys(ctx context.Context, source, target *backend.Store) error {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: d.namespace, Name: migrationKeysName(source)}
	if err := d.apiReader.Get(ctx, key, secret); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	src := keyPair{
		AccessKeyID:     string(secret.Data[migrationSecretSourceID]),
		SecretAccessKey: string(secret.Data[migrationSecretSourceSecret]),
	}
	dst := keyPair{
		AccessKeyID:     string(secret.Data[migrationSecretTargetID]),
		SecretAccessKey: string(secret.Data[migrationSecretTargetSecret]),
	}

	src, err := d.ensurePlaneMigrationKey(ctx, source, src, permissions{Read: true})
	if err != nil {
		return fmt.Errorf("the copier's key on the source cluster: %w", err)
	}
	dst, err = d.ensurePlaneMigrationKey(ctx, target, dst, permissions{Read: true, Write: true})
	if err != nil {
		return fmt.Errorf("the copier's key on the target cluster: %w", err)
	}

	stored := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: commonLabels(source)},
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			migrationSecretSourceID:     []byte(src.AccessKeyID),
			migrationSecretSourceSecret: []byte(src.SecretAccessKey),
			migrationSecretTargetID:     []byte(dst.AccessKeyID),
			migrationSecretTargetSecret: []byte(dst.SecretAccessKey),
		},
	}
	if err := d.apply(ctx, stored); err != nil {
		return fmt.Errorf("store the copier's credentials: %w", err)
	}
	return nil
}

// ensurePlaneMigrationKey makes sure the given key exists in the plane with the
// given access to every one of its buckets, creating it if it does not.
//
// The permissions are granted on every pass rather than at creation, because the
// set of buckets grows: a SystemBucket created while the copy runs must not be
// the one bucket the copier cannot read.
func (d *Driver) ensurePlaneMigrationKey(ctx context.Context, plane *backend.Store, have keyPair, perms permissions) (keyPair, error) {
	svc, ready, err := d.adminClientFor(ctx, plane)
	if err != nil {
		return keyPair{}, err
	}
	if !ready {
		return keyPair{}, fmt.Errorf("the admin token of %s is not provisioned yet", resourceName(plane))
	}

	usable := have.AccessKeyID != "" && have.SecretAccessKey != ""
	if usable {
		exists, err := svc.keyExists(ctx, have.AccessKeyID)
		if err != nil {
			return keyPair{}, err
		}
		usable = exists
	}
	if !usable {
		created, err := svc.createKey(ctx, migrationKeyDisplayName)
		if err != nil {
			return keyPair{}, fmt.Errorf("create the copier's key: %w", err)
		}
		have = keyPair{AccessKeyID: created.AccessKeyID, SecretAccessKey: created.SecretAccessKey}
	}

	buckets, err := svc.listBuckets(ctx)
	if err != nil {
		return keyPair{}, fmt.Errorf("list the buckets: %w", err)
	}
	for _, b := range buckets {
		if err := svc.allow(ctx, b.ID, have.AccessKeyID, perms); err != nil {
			return keyPair{}, fmt.Errorf("grant the copier access to bucket %s: %w", b.ID, err)
		}
	}
	return have, nil
}

// bucketNames returns the global aliases of every bucket of a cluster, sorted.
//
// Sorted because the list is hashed onto the Job that copies it: an order that
// depended on the API's would replace the copier on a pass that changed nothing.
func (d *Driver) bucketNames(ctx context.Context, plane *backend.Store) ([]string, error) {
	svc, ready, err := d.adminClientFor(ctx, plane)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, fmt.Errorf("the admin token of %s is not provisioned yet", resourceName(plane))
	}
	buckets, err := svc.listBuckets(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(buckets))
	for _, b := range buckets {
		names = append(names, b.GlobalAliases...)
	}
	sort.Strings(names)
	return names, nil
}

// copyOutcome is what one pass of the copier has got to.
type copyOutcome int

const (
	copyRunning copyOutcome = iota
	copyDone
	copyFailed
	copyQuotaBlocked
)

// runCopyPass drives one copy Job to completion and reports where it is.
//
// The Job is created if it is absent, replaced if it was started for a different
// set of buckets, and deleted-and-recreated once it has exhausted its retries —
// the phase calling this stays where it is either way, so a copy that cannot get
// through keeps trying while saying so, rather than stranding a migration that is
// holding a consumer's write access in the cutover case.
func (d *Driver) runCopyPass(
	ctx context.Context,
	source, target *backend.Store,
	pass string,
	buckets []string,
) (copyOutcome, string, error) {
	name := copyJobName(source, target.Incarnation, pass)
	stamp := bucketsStamp(buckets)

	job := &batchv1.Job{}
	err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: name}, job)
	switch {
	case apierrors.IsNotFound(err):
		if err := d.ensureMigrationKeys(ctx, source, target); err != nil {
			return copyRunning, "", err
		}
		job := d.buildCopyJob(name, source, target, buckets, stamp)
		configureCopyPass(job, pass)
		if err := d.client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return copyRunning, "", fmt.Errorf("start the copier: %w", err)
		}
		return copyRunning, fmt.Sprintf("copying %d bucket(s) into the new cluster", len(buckets)), nil
	case err != nil:
		return copyRunning, "", err
	}

	if job.Annotations[annCopyBuckets] != stamp || (pass == copyPassDelta && !copyJobHasFinalSync(job)) {
		// A bucket appeared or disappeared since this Job was started. A Job's pod
		// template cannot be edited, so the pass is restarted over the new list;
		// what was already copied is skipped by the copier itself.
		if err := d.deleteCopyJob(ctx, job); err != nil {
			return copyRunning, "", err
		}
		return copyRunning, "the bucket set or final synchronization mode changed; the copy is restarting", nil
	}

	switch {
	case job.Status.Succeeded > 0:
		return copyDone, "", nil
	case jobExhausted(job):
		if pass == copyPassBulk {
			blocked, err := d.copyHitQuota(ctx, job)
			if err != nil {
				return copyRunning, "", err
			}
			if blocked {
				return copyQuotaBlocked, "the destination quota prevents bulk copy", nil
			}
		}
		if err := d.deleteCopyJob(ctx, job); err != nil {
			return copyFailed, "", err
		}
		return copyFailed, "the copy failed and is starting again; see the logs of the previous copier pod", nil
	default:
		return copyRunning, appendNote(fmt.Sprintf("copying %d bucket(s) into the new cluster", len(buckets)),
			d.copyProgress(ctx, source, target)), nil
	}
}

// copyProgress says how much of the source the new cluster already holds, or ""
// when that cannot be read this pass.
//
// The copier knows exactly, but it is a Job: its counts are in its log, and a
// migration measured in hours that reports only "copying 3 bucket(s)" leaves
// an operator — and the console page that shows this message under the phase —
// with no idea whether it is at a tenth or nine tenths. The two clusters' bucket
// totals are what the verification compares anyway; compared while the copy
// runs, the part already there is the progress. It is an estimate in the same
// way the verification is: aggregate counts, not keys.
func (d *Driver) copyProgress(ctx context.Context, source, target *backend.Store) string {
	diff, err := d.compareClusters(ctx, source, target, false)
	if err != nil {
		d.log.Debug("unable to measure the copy's progress", "store", source.Name, "error", err.Error())
		return ""
	}
	return copyProgressNote(diff)
}

// copyProgressNote renders diff as the part of the source that has been copied.
func copyProgressNote(diff difference) string {
	if diff.totalObjects == 0 && diff.totalBytes == 0 {
		return ""
	}
	copiedObjects := min(max(diff.totalObjects-diff.objects, 0), diff.totalObjects)
	copiedBytes := min(max(diff.totalBytes-diff.bytes, 0), diff.totalBytes)
	percent := int64(100)
	switch {
	case diff.totalBytes > 0:
		percent = copiedBytes * 100 / diff.totalBytes
	case diff.totalObjects > 0:
		percent = copiedObjects * 100 / diff.totalObjects
	}
	quantity := func(n int64) string { return resource.NewQuantity(n, resource.BinarySI).String() }
	return fmt.Sprintf("%s of %s, %d of %d objects (%d%%)",
		quantity(copiedBytes), quantity(diff.totalBytes), copiedObjects, diff.totalObjects, percent)
}

func configureCopyPass(job *batchv1.Job, pass string) {
	if pass == copyPassDelta {
		job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: migrator.EnvFinalSync, Value: "true"})
	}
}

func copyJobHasFinalSync(job *batchv1.Job) bool {
	if len(job.Spec.Template.Spec.Containers) == 0 {
		return false
	}
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == migrator.EnvFinalSync {
			return env.Value == "true"
		}
	}
	return false
}

// jobExhausted reports whether the Job has given up: its backoff limit is spent
// and it will not start another pod by itself.
func jobExhausted(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// deleteCopyJob removes a Job together with its pods. Without the propagation
// policy the pods are orphaned and a Job of the same name cannot be created
// while they hold it.
func (d *Driver) deleteCopyJob(ctx context.Context, job *batchv1.Job) error {
	policy := metav1.DeletePropagationBackground
	err := d.client.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove the copier Job: %w", err)
	}
	return nil
}

// stopDeltaPass is a non-blocking termination barrier before thawing the source.
// Foreground deletion retains the Job until dependent pods are removed. The
// explicit pod check also covers a Job already background-deleted by an earlier
// reconcile or controller version.
func (d *Driver) stopDeltaPass(ctx context.Context, source, target *backend.Store) (bool, error) {
	return d.stopCopyPass(ctx, source, target, copyPassDelta)
}

func (d *Driver) stopCopyPass(ctx context.Context, source, target *backend.Store, pass string) (bool, error) {
	name := copyJobName(source, target.Incarnation, pass)
	job := &batchv1.Job{}
	key := client.ObjectKey{Namespace: d.namespace, Name: name}
	if err := d.apiReader.Get(ctx, key, job); err == nil {
		if job.DeletionTimestamp.IsZero() {
			policy := metav1.DeletePropagationForeground
			if err := d.client.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("stop the final copier: %w", err)
			}
		}
		return false, nil
	} else if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("read the final copier: %w", err)
	}
	pods := &corev1.PodList{}
	if err := d.apiReader.List(ctx, pods, client.InNamespace(d.namespace), client.MatchingLabels{"job-name": name}); err != nil {
		return false, fmt.Errorf("check final copier termination: %w", err)
	}
	return len(pods.Items) == 0, nil
}

func (d *Driver) copyHitQuota(ctx context.Context, job *batchv1.Job) (bool, error) {
	pods := &corev1.PodList{}
	if err := d.apiReader.List(ctx, pods, client.InNamespace(d.namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		return false, fmt.Errorf("read copier outcome: %w", err)
	}
	for _, pod := range pods.Items {
		owned := false
		for _, owner := range pod.OwnerReferences {
			if owner.UID == job.UID && owner.Kind == "Job" && owner.Name == job.Name && owner.Controller != nil && *owner.Controller {
				owned = true
			}
		}
		if !owned {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == "copier" && status.State.Terminated != nil && status.State.Terminated.ExitCode == migrator.QuotaExitCode {
				return true, nil
			}
		}
	}
	return false, nil
}

// bucketsStamp is a short hash of the bucket list, for the annotation the Job
// carries.
func bucketsStamp(buckets []string) string {
	sum := sha256.Sum256([]byte(strings.Join(buckets, "\n")))
	return hex.EncodeToString(sum[:])[:16]
}

// buildCopyJob builds the copier: the controller's own image, started with the
// copy argument, with no Kubernetes access of any kind.
//
// backoffLimit is small on purpose. A copy that fails for a reason of its own
// (a cluster that is not answering) is better restarted by the next reconcile,
// which re-checks that both clusters are healthy first, than by a Job retrying
// into the same wall.
// The credentials are NOT passed in: the Job reads them from the Secret
// ensureMigrationKeys has just written, so a key rotated between two passes
// reaches a restarted pod without the Job being rebuilt, and no secret half is
// spelled out in a pod spec that anyone with read access to the namespace can
// list.
func (d *Driver) buildCopyJob(
	name string,
	source, target *backend.Store,
	buckets []string,
	stamp string,
) *batchv1.Job {
	env := []corev1.EnvVar{
		{Name: migrator.EnvSourceEndpoint, Value: planeS3HostPort(source, d.namespace, d.clusterDomain)},
		{Name: migrator.EnvTargetEndpoint, Value: planeS3HostPort(target, d.namespace, d.clusterDomain)},
		{Name: migrator.EnvBuckets, Value: strings.Join(buckets, ",")},
		{Name: migrator.EnvSourceAccessKey, ValueFrom: migrationSecretRef(source, migrationSecretSourceID)},
		{Name: migrator.EnvSourceSecretKey, ValueFrom: migrationSecretRef(source, migrationSecretSourceSecret)},
		{Name: migrator.EnvTargetAccessKey, ValueFrom: migrationSecretRef(source, migrationSecretTargetID)},
		{Name: migrator.EnvTargetSecretKey, ValueFrom: migrationSecretRef(source, migrationSecretTargetSecret)},
		// Both clusters are behind the same kind of terminator with the same
		// certificate, so one CA and one name verify either side.
		{Name: migrator.EnvCAFile, Value: tlsMountPath + "/ca.crt"},
		{Name: migrator.EnvServerName, Value: garageServerName(source, d.namespace)},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   d.namespace,
			Labels:      commonLabels(source),
			Annotations: map[string]string{annCopyBuckets: stamp},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To(int32(2)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: commonLabels(source)},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					// The copier talks to two Services and to nothing else. It needs no
					// ServiceAccount token, and not mounting one is the difference
					// between a pod that copies objects and a pod that could act on the
					// cluster if the image it runs were ever wrong.
					AutomountServiceAccountToken: ptr.To(false),
					// Where the data plane may run, the copier may run: it talks to
					// both clusters over the cluster network and wants to be near
					// them rather than anywhere else. No spreading rules: there is one
					// copier and nothing for it to spread against.
					NodeSelector:     maps.Clone(d.placement.NodeSelector),
					Tolerations:      slices.Clone(d.placement.Tolerations),
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: registryPullSecret}},
					Containers: []corev1.Container{{
						Name:            "copier",
						Image:           d.copierImage,
						Args:            []string{migrator.Command},
						Env:             env,
						SecurityContext: copierSecurityContext(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: tlsVolume, MountPath: tlsMountPath, ReadOnly: true},
						},
					}},
					// The CA only. The copier verifies Garage; it has no business holding
					// the key Garage's certificate is served with.
					Volumes: []corev1.Volume{caOnlyVolume(d.tlsSecret)},
				},
			},
		},
	}
}

// migrationSecretRef reads one half of a copier credential out of the Secret.
func migrationSecretRef(store *backend.Store, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: migrationKeysName(store)},
			Key:                  key,
		},
	}
}

// copierSecurityContext is the copier's. Unlike the Garage container next to it,
// this one owns no files and needs no user of its own: it makes HTTP calls. So it
// runs as a nobody, which is the strictest thing that still starts a static Go
// binary.
func copierSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsUser:                ptr.To[int64](64535),
		RunAsGroup:               ptr.To[int64](64535),
		RunAsNonRoot:             ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// removeCopierCredentials takes away the key the copier was given, and the Secret
// holding it.
//
// The key has read and write on every bucket of its cluster: it is the one
// credential in this module that is not scoped to a single consumer, and the
// only reason it exists is a copy that is now over. Leaving it behind would
// leave a live all-buckets key sitting in a Secret for as long as the
// installation lasts.
//
// The keys are found by display name rather than read out of the Secret,
// because which half of the Secret belongs to the surviving cluster depends on
// how the migration ended: after a cutover the copier's writing key is on the
// cluster that now serves, after an abort its reading key is. One rule covers
// both, and it also collects a key left behind by a migration that was
// interrupted before this step.
func (d *Driver) removeCopierCredentials(ctx context.Context, serving *backend.Store) error {
	svc, ready, err := d.adminClientFor(ctx, serving)
	if err != nil {
		return fmt.Errorf("remove the copier's key: %w", err)
	}
	if !ready {
		return fmt.Errorf("remove the copier's key: the admin token of %s is not provisioned yet", resourceName(serving))
	}
	keys, err := svc.listKeys(ctx)
	if err != nil {
		return fmt.Errorf("list the keys of %s: %w", resourceName(serving), err)
	}
	for _, k := range keys {
		if k.Name != migrationKeyDisplayName {
			continue
		}
		if err := svc.deleteKey(ctx, k.ID); err != nil {
			return fmt.Errorf("remove the copier's key %s: %w", k.ID, err)
		}
	}

	secret := &corev1.Secret{}
	secret.Name, secret.Namespace = migrationKeysName(serving), d.namespace
	if err := d.client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove the copier's credentials: %w", err)
	}
	return nil
}
