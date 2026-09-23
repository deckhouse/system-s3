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

// Package backend defines the contract between the system-s3
// reconcilers and the Garage data plane that implements the system object
// storage. The reconcilers own the Kubernetes-facing FSM (conditions, phases,
// finalizers, credentials Secret); the driver owns everything backend-specific
// (data-plane workloads, bucket and access-key lifecycle).
//
// Unlike the user-facing module, nothing here is keyed by a custom resource: the
// module owns exactly one data plane, described by Store — a value object built
// from the module settings. That is what lets the system storage exist without a
// store CR at all, which is the whole point of the split. It is also why StoreState
// stays here while the bucket and access values are shared: there is no store
// resource for a shared type to describe.
package backend

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-common-lib/objectstorage"
)

// StoreName is the store's fixed name. It prefixes every object the data plane
// owns, and it is a constant rather than a setting because there is exactly one
// store: a name to choose would be a name a consumer has to know, which is the
// coupling the split was meant to remove.
const StoreName = "system"

// StoreLabel carries the store name on every object the driver creates (used
// as a selector, e.g. to list the local-PV pool).
const StoreLabel = "storage.deckhouse.io/system-store"

// Store is the single system data plane the module manages. It stands in for the
// custom resource the System profile used to be, so the driver keeps taking a
// description of the desired store rather than reading module settings itself.
type Store struct {
	// Name prefixes every resource the data plane owns (StatefulSet, Services,
	// ConfigMap, Secret, PersistentVolumes). It is fixed rather than
	// user-chosen: there is only one store.
	Name string

	// HighAvailability is the platform's high-availability mode as the chart
	// resolved it: three Garage replicas with replication factor 3 when true, a
	// single replica with factor 1 when false.
	//
	// It is desired state only. The factor the live cluster runs with is pinned in
	// the data plane's own ConfigMap and read back from there, because Garage
	// cannot change its replication factor on a live cluster: a store built in one
	// mode keeps running in it until a migration moves the data to a cluster built
	// in the other. So this field says what the cluster asks for, and a
	// disagreement with the pinned factor is reported rather than acted on.
	HighAvailability bool

	// HighAvailabilityExplicit reports that HighAvailability was written by
	// somebody rather than derived from the number of masters the cluster has.
	//
	// It exists for one decision and is worth the field for it. The derived value
	// flips on its own when masters are lost, and "the cluster is down to one
	// master" must not be read as "please move the system storage to a single
	// replica": that is a loss of redundancy nobody asked for, arrived at exactly
	// when the cluster is least able to afford it. A store therefore only ever
	// shrinks when the setting says so out loud. Growing is not guarded — masters
	// coming back is good news, and acting on it costs nothing but a migration.
	HighAvailabilityExplicit bool

	// Incarnation is which of the store's data planes this description refers to.
	//
	// There is normally one, and it is the first. A migration is the exception: the
	// store runs two Garage clusters for as long as the copy takes — the one
	// serving and the one being filled — and every object of each is named after
	// its incarnation. The endpoint Service is the one thing that is not, because
	// it is what consumers hold.
	//
	// It is part of the store description rather than a parameter on every call
	// because "which data plane" is the same kind of fact as "which store": the
	// driver's whole surface is written against one, and a migration is two passes
	// over two descriptions.
	Incarnation int32

	// StorageClass is the class the data plane's volumes are ordered from. Empty
	// when the cluster offers none, in which case no data plane is created at all
	// — there is nothing for the module to fall back to, and inventing storage on
	// the control-plane nodes behind the platform's storage modules is what this
	// replaced.
	//
	// Like HighAvailability it is desired state: the claims of a running store
	// were created with whatever class was configured then, and changing it needs
	// the same migration.
	StorageClass string
}

// StoreState is the observed data-plane state the driver reports. The store
// reconciler surfaces it on every SystemBucket as the StorageReady condition —
// there is no store object of its own to carry it.
type StoreState struct {
	// Ready is true once the data plane is up and serving S3.
	Ready bool

	// Message is a human-readable explanation of the current state, used as the
	// condition message (especially while not Ready).
	Message string

	// Version is the running Garage version.
	Version string

	// Endpoint is the in-cluster S3 endpoint URL, once known.
	Endpoint string

	// Region is the S3 region the endpoint advertises.
	Region string

	// TotalCapacity is the cluster-wide capacity the backend reports, when
	// known. It has no CR field to land in; the reconciler logs it and it is
	// where a capacity metric will read from.
	TotalCapacity *resource.Quantity

	// AdminSecretName is the Secret in the module namespace holding the RPC
	// secret and admin token the controller manages the store with. Empty until
	// the Secret exists.
	AdminSecretName string

	// MigrationPhase is where a move between two Garage clusters has got to, and
	// MigrationStartedAt is when it began. Empty and zero when the store has one
	// data plane, which is almost always.
	//
	// They are reported rather than kept to themselves because a migration is the
	// module's only operation measured in hours: without a series to look at, "is
	// it still going, and since when" can only be answered by reading a
	// SystemBucket's condition and hoping somebody wrote down when it started.
	MigrationPhase     string
	MigrationStartedAt time.Time

	// Capacity is how the data plane's volumes compare with what the buckets ask
	// for. Zero when the pass did not get as far as sizing them.
	Capacity StoreCapacity
}

// StoreCapacity is the size one replica's volume should have for the sum of the
// SystemBucket sizes, the size the smallest of them has, and why they could not
// be grown when they could not. It is what tells a SystemBucket whether a raised
// spec.size has taken effect.
//
// Detail is what the claims say about a growth that is under way — the resizer
// working, or the node yet to grow the file system — so that "growing" comes
// with where it is stuck when it is.
type StoreCapacity struct {
	WantBytes int64
	HaveBytes int64
	Problem   string
	Detail    string
}

// The values that cross the driver seam — what the driver reports back about a
// bucket and its access key, and how that key is named in the backend — live in
// sds-common-lib. Both object-storage modules had their own copy, and the copies
// drifted: the user-facing module's BucketState grew UnsupportedFeatures (so a
// requested feature the backend cannot enforce is surfaced instead of silently
// no-opping) while this one's did not.
//
// Nothing here requests such a feature today — a SystemBucket has no quota and no
// access policy to ask for — so the field stays empty. That is the honest state
// of it, and it is now the same field the other module reports through rather
// than a struct that happens to look alike.
//
// The aliases keep the names this package's callers already use.
type (
	// BucketState is the observed state the driver reports for one bucket.
	BucketState = objectstorage.BucketState

	// AccessState is the observed state the driver reports for a bucket's access
	// key. When SecretAccessKey is non-empty a fresh key was issued and the
	// reconciler (re)writes the credentials Secret with it; when it is empty the
	// bucket already had a live key and the existing Secret is preserved — Garage
	// cannot recover a secret key after creation.
	AccessState = objectstorage.AccessState
)

// BucketUsage is what a bucket holds and what it is allowed to hold, as the
// backend counts it. It is reported alongside BucketState rather than inside it
// because BucketState is shared with the user-facing module through
// sds-common-lib, and usage is not something every driver there can answer.
type BucketUsage struct {
	// QuotaBytes is the size quota in force in the backend. Zero means the
	// backend reported none — which, once the controller has reconciled the
	// bucket, means the quota has not been applied yet rather than that the
	// bucket is unbounded.
	QuotaBytes int64

	// UsedBytes and Objects are what the bucket holds right now.
	UsedBytes int64
	Objects   int64
}

// AccessResourceName is the backend-facing identifier (the Garage key display
// name) for a SystemBucket's key, unique per (namespace, name).
func AccessResourceName(namespace, name string) string {
	return objectstorage.AccessResourceName(namespace, name)
}
