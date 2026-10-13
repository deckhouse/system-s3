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

// Package monitoring holds the module's own metrics.
//
// Everything else the controller exports comes from controller-runtime
// (reconcile counts and durations, workqueue depth, client-go latency), which
// describes the controller. These two describe the thing the controller runs, and
// the data plane has no custom resource to publish it on: its state is visible
// only as a condition copied onto every SystemBucket, so a cluster with no
// SystemBucket yet has nowhere to show that its system storage never came up.
package monitoring

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const namespace = "system_s3"

var (
	// storeReady is the readiness of the single system data plane, 1 or 0. Same
	// value as Module/system-s3's StorageReady condition.
	storeReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_ready",
		Help:      "Whether the system object storage data plane is up and serving S3 (1) or not (0).",
	}, []string{"store"})

	// storeCapacityBytes is the capacity the backend reports for its assigned
	// layout — the sum of the per-replica capacities Garage actually accepted, not
	// the size the controller asked for. The difference is the point: a replica
	// missing from the layout shows up here as capacity that never arrived.
	storeCapacityBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_capacity_bytes",
		Help:      "Total capacity assigned in the data plane's layout, in bytes.",
	}, []string{"store"})

	// bucketUsedBytes and bucketQuotaBytes are per bucket, and they are the only
	// warning a platform component gets before its writes start failing: a bucket
	// at its quota refuses PUTs, and the component that owns it has no other way
	// to see that coming. Labelled by the SystemBucket rather than by the backend
	// bucket name, because the resource is what an operator would edit to fix it.
	bucketUsedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "bucket_used_bytes",
		Help:      "Bytes stored in a system bucket, as the backend counts them.",
	}, []string{"namespace", "name", "bucket"})

	bucketQuotaBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "bucket_quota_bytes",
		Help:      "Size quota in force on a system bucket, in bytes (0 when none is applied).",
	}, []string{"namespace", "name", "bucket"})

	// bucketObjects and bucketQuotaObjects are the other half of the same
	// warning: a bucket is refused writes at its object-count quota too
	// (spec.size / 64 KiB), with bytes to spare.
	bucketObjects = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "bucket_objects",
		Help:      "Objects stored in a system bucket, as the backend counts them.",
	}, []string{"namespace", "name", "bucket"})

	bucketQuotaObjects = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "bucket_quota_objects",
		Help:      "Object-count quota in force on a system bucket (0 when none is applied).",
	}, []string{"namespace", "name", "bucket"})

	// migrationPhase and migrationStartedSeconds describe the module's only
	// operation measured in hours: moving a store to a second Garage cluster.
	//
	// The phase is a label rather than a number because the phases are not on a
	// scale — Cutover is not "more" than Copying, it is the one where consumers
	// cannot write. An alert that has to distinguish them wants to name one, and a
	// number would make it guess.
	//
	// Both series exist only while a migration or an in-place change of the
	// replica count is in flight. A store that is doing neither publishes
	// neither, so a dashboard showing them shows a store that is changing shape,
	// and nothing else.
	migrationPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "migration_phase",
		Help:      "1 for the phase a migration between two Garage clusters, or an in-place change of the replica count, is currently in.",
	}, []string{"store", "phase"})

	migrationStartedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "migration_started_seconds",
		Help:      "Unix time the migration in flight began, so its age can be alerted on.",
	}, []string{"store"})

	// bucketReady is one consumer's whole answer to "is my storage working".
	//
	// store_ready says the data plane serves; it says nothing about a bucket that
	// the data plane refuses to provision — a name that collides with another
	// consumer's, a size that does not parse, a key that cannot be issued. Those
	// live only in the resource's conditions, which nothing watches.
	bucketReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "bucket_ready",
		Help:      "Whether a system bucket is fully provisioned (1) or not (0).",
	}, []string{"namespace", "name"})

	// bucketSizeUpToDate is the SizeUpToDate condition of one bucket: 1 when the
	// backend quota matches spec.size, 0 while it does not, with
	// the condition's reason as a label.
	//
	// Keep the published metric name for existing dashboards and alert rules.
	// Shared PVC expansion is reported independently by the store's metrics.
	bucketSizeUpToDate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "bucket_size_applied",
		Help:      "Whether a system bucket's spec.size has taken effect (1) or not (0), by the SizeUpToDate reason.",
	}, []string{"namespace", "name", "reason"})

	// The three sizes of one replica's volume, which are equal on a healthy store
	// and diverge in two different ways when growth is stuck:
	//
	//   - desired > requested: the claims could not even be patched, which is what
	//     a StorageClass without allowVolumeExpansion looks like;
	//   - requested > capacity: the claims were patched and the CSI driver has not
	//     finished (or cannot) resize the volumes behind them.
	//
	// Growth is automatic and silent by design — the store follows the sum of the
	// bucket sizes without anyone asking — so the failure is silent too. It went
	// unnoticed here for months: the controller had no permission to patch a claim
	// at all, and nothing said so.
	storeVolumeDesiredBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_volume_desired_bytes",
		Help:      "Size one replica's volume should have, from the sum of the SystemBucket sizes.",
	}, []string{"store"})

	storeVolumeRequestedBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_volume_requested_bytes",
		Help:      "Smallest size any replica's claim asks for, in bytes.",
	}, []string{"store"})

	storeVolumeCapacityBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "store_volume_capacity_bytes",
		Help:      "Smallest size any replica's volume actually has, in bytes.",
	}, []string{"store"})

	// stsRequestsTotal counts what the temporary-credentials endpoint answered.
	//
	// "Granted" and the refusal codes in one series: a consumer that starts being
	// refused — a RoleBinding removed, a client asking for a key on every request
	// and hitting the per-bucket cap — is invisible from the outside otherwise.
	// The error code is the label because it is what the client is told.
	// temporaryKeyExpiration is the list of temporary keys Garage still honours,
	// one series per key, valued with the Unix time it stops being honoured.
	//
	// It is the only place outside Garage an administrator can see who holds
	// access to a bucket right now: the module keeps no record of the keys it
	// mints. The access key ID is not a secret; the secret half never leaves
	// Garage after it is handed to the caller. A bucket holds at most 50
	// unexpired temporary keys, which bounds the series.
	temporaryKeyExpiration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "temporary_key_expiration_seconds",
		Help:      "Unix time an unexpired temporary key stops being honoured, by bucket, access key ID, the ServiceAccount it was issued to and whether it may write.",
	}, []string{"bucket", "access_key_id", "subject", "write"})

	stsRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "sts_requests_total",
		Help:      "Temporary-credentials requests by outcome (Granted, AccessDenied, LimitExceeded, ...).",
	}, []string{"result"})
)

// BucketReady records whether a bucket is fully provisioned.
func BucketReady(namespace, name string, ready bool) {
	v := 0.0
	if ready {
		v = 1
	}
	bucketReady.WithLabelValues(namespace, name).Set(v)
}

// BucketSizeUpToDate records a bucket's SizeUpToDate condition.
//
// The series of the previous reason is dropped rather than set to zero, for the
// same reason as in Migration: an alert on the reason must stop matching once
// the reason is gone.
func BucketSizeUpToDate(namespace, name, reason string, applied bool) {
	bucketSizeUpToDate.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "name": name})
	v := 0.0
	if applied {
		v = 1
	}
	bucketSizeUpToDate.WithLabelValues(namespace, name, reason).Set(v)
}

// StoreVolumes records the three sizes of a replica's volume: what it should be,
// what the claims ask for, and what they have.
func StoreVolumes(store string, desired, requested, capacity int64) {
	storeVolumeDesiredBytes.WithLabelValues(store).Set(float64(desired))
	storeVolumeRequestedBytes.WithLabelValues(store).Set(float64(requested))
	storeVolumeCapacityBytes.WithLabelValues(store).Set(float64(capacity))
}

// STSRequest counts one answer of the temporary-credentials endpoint.
func STSRequest(result string) {
	stsRequestsTotal.WithLabelValues(result).Inc()
}

// Migration records the phase a store's migration is in, or that there is none.
//
// The previous phase's series is dropped rather than set to zero: a phase that
// ended is not a phase at zero, and an alert written as
// migration_phase{phase="Cutover"} == 1 should stop matching when the cutover is
// over, not keep matching a series that says zero.
func Migration(store, phase string, startedAt time.Time) {
	migrationPhase.DeletePartialMatch(prometheus.Labels{"store": store})
	if phase == "" {
		migrationStartedSeconds.DeleteLabelValues(store)
		return
	}
	migrationPhase.WithLabelValues(store, phase).Set(1)
	migrationStartedSeconds.WithLabelValues(store).Set(float64(startedAt.Unix()))
}

// BucketUsage records what one bucket holds and what it is allowed to hold.
func BucketUsage(namespace, name, bucket string, used, quota, objects, quotaObjects int64) {
	bucketUsedBytes.WithLabelValues(namespace, name, bucket).Set(float64(used))
	bucketQuotaBytes.WithLabelValues(namespace, name, bucket).Set(float64(quota))
	bucketObjects.WithLabelValues(namespace, name, bucket).Set(float64(objects))
	bucketQuotaObjects.WithLabelValues(namespace, name, bucket).Set(float64(quotaObjects))
}

// ForgetBucket drops a bucket's series.
//
// Without it a deleted SystemBucket keeps publishing its last usage forever, and
// an alert on "bucket nearly full" would fire about a bucket that does not exist
// — the kind of alert that teaches people to ignore alerts.
func ForgetBucket(namespace, name, bucket string) {
	bucketUsedBytes.DeleteLabelValues(namespace, name, bucket)
	bucketQuotaBytes.DeleteLabelValues(namespace, name, bucket)
	bucketObjects.DeleteLabelValues(namespace, name, bucket)
	bucketQuotaObjects.DeleteLabelValues(namespace, name, bucket)
	bucketReady.DeleteLabelValues(namespace, name)
	bucketSizeUpToDate.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "name": name})
}

func init() {
	metrics.Registry.MustRegister(storeReady)
	metrics.Registry.MustRegister(storeCapacityBytes)
	metrics.Registry.MustRegister(migrationPhase)
	metrics.Registry.MustRegister(migrationStartedSeconds)
	metrics.Registry.MustRegister(bucketUsedBytes)
	metrics.Registry.MustRegister(bucketQuotaBytes)
	metrics.Registry.MustRegister(bucketObjects)
	metrics.Registry.MustRegister(bucketQuotaObjects)
	metrics.Registry.MustRegister(bucketReady)
	metrics.Registry.MustRegister(bucketSizeUpToDate)
	metrics.Registry.MustRegister(storeVolumeDesiredBytes)
	metrics.Registry.MustRegister(storeVolumeRequestedBytes)
	metrics.Registry.MustRegister(storeVolumeCapacityBytes)
	metrics.Registry.MustRegister(stsRequestsTotal)
	metrics.Registry.MustRegister(temporaryKeyExpiration)
}

// TemporaryKey is one series of temporary_key_expiration_seconds.
type TemporaryKey struct {
	Bucket, AccessKeyID, Subject string
	Write                        bool
	ExpiresAt                    time.Time
}

// TemporaryKeys replaces the published list of unexpired temporary keys. A key
// that expired or was swept since the last call drops out.
func TemporaryKeys(keys []TemporaryKey) {
	temporaryKeyExpiration.Reset()
	for _, k := range keys {
		temporaryKeyExpiration.WithLabelValues(k.Bucket, k.AccessKeyID, k.Subject, strconv.FormatBool(k.Write)).
			Set(float64(k.ExpiresAt.Unix()))
	}
}

// StoreReady records the readiness of the named store.
func StoreReady(store string, ready bool) {
	v := 0.0
	if ready {
		v = 1
	}
	storeReady.WithLabelValues(store).Set(v)
}

// StoreCapacityBytes records the store's assigned capacity. Capacity is optional
// — a store whose layout is not assigned yet has none to report — and an absent
// value publishes 0 rather than the previous reading, because a stale capacity is
// worse than none: it would keep an alert quiet about a store that has stopped
// answering.
func StoreCapacityBytes(store string, bytes *int64) {
	if bytes == nil {
		storeCapacityBytes.WithLabelValues(store).Set(0)
		return
	}
	storeCapacityBytes.WithLabelValues(store).Set(float64(*bytes))
}
