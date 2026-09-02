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
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-common-lib/objectstorage"
)

// StoreName is the store's fixed name. It prefixes every object the data plane
// owns, and it is a constant rather than a setting because there is exactly one
// store: a name to choose would be a name a consumer has to know, which is the
// coupling the split was meant to remove.
const StoreName = "system"

// Store is the single system data plane the module manages. It stands in for the
// custom resource the System profile used to be, so the driver keeps taking a
// description of the desired store rather than reading module settings itself.
type Store struct {
	// Name prefixes every resource the data plane owns (StatefulSet, Services,
	// ConfigMap, Secret, PersistentVolumes). It is fixed rather than
	// user-chosen: there is only one store.
	Name string

	// SingleReplica runs one Garage replica with replication factor 1 instead
	// of the default three. Flipping it forces a recreate of the data plane —
	// Garage cannot change its replication factor in place — which destroys the
	// stored objects.
	SingleReplica bool

	// SizePerNode is the capacity of the node-sticky local volume provisioned
	// per replica.
	SizePerNode resource.Quantity

	// StorageClass is the managed StorageClass those local volumes belong to.
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

// AccessResourceName is the backend-facing identifier (the Garage key display
// name) for a SystemBucket's key, unique per (namespace, name).
func AccessResourceName(namespace, name string) string {
	return objectstorage.AccessResourceName(namespace, name)
}
