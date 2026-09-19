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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SystemBucket is a namespaced CR with which a Deckhouse component asks for a
// bucket in the platform's system object storage. The controller creates the
// bucket in the system backend and writes a ConfigMap with the S3 connection
// parameters in the same namespace (status.connectionRef).
//
// No credentials are written anywhere, and none are long-lived: a consumer
// exchanges its ServiceAccount token at the credentials endpoint for a key that
// the backend stops honouring an hour later. Kubernetes RBAC decides who may do
// that, per bucket, through the virtual systembuckets/read and /write
// subresources.
//
// It is deliberately the dumbest resource the module could ship, and every
// omission is a decision:
//
//   - No access resource, and no credentials object of any kind. Who may reach a
//     bucket is an ordinary RBAC grant, which is the answer an administrator
//     already knows how to read, audit and revoke — and revoking it takes effect
//     without anything having to be rotated, because what was handed out expires
//     on its own.
//   - No reference to a store. The module owns exactly one system data plane,
//     configured through ModuleConfig, so there is nothing to point at and no
//     head resource to keep Ready.
//   - No reclaim policy. Deleting a SystemBucket deletes the bucket and its
//     objects in the backend: platform data lives as long as the CR that asked
//     for it, and an orphaned system bucket nobody is accountable for is worse
//     than an explicit deletion.
//   - Namespace-locked. It is admitted only in namespaces labelled
//     heritage: deckhouse (the label helm_lib puts on every DKP module
//     namespace) and carries no user-facing RBAC, which is what keeps the
//     system storage out of tenant hands without matching namespace names
//     against a pattern.
//
// The markers below are not rendered. crds/systembucket.yaml is written by
// hand, and hack/generate_code.sh generates deepcopy only — so this is a record
// of what the shipped manifest enforces, kept next to the fields it constrains,
// and the manifest is the thing that has to be edited with them. They were
// added because the type carried none of it: a reader of this package saw two
// unconstrained strings and no immutability at all, which is a laxer resource
// than the one a cluster actually has.
//
// The markers below are not rendered. crds/systembucket.yaml is written by
// hand, and hack/generate_code.sh generates deepcopy only — so this is a record
// of what the shipped manifest enforces, kept next to the fields it constrains,
// and the manifest is the thing that has to be edited with them. They were
// added because the type carried none of it: a reader of this package saw two
// unconstrained strings and no immutability at all, which is a laxer resource
// than the one a cluster actually has.
//
// +kubebuilder:resource:scope=Namespaced,shortName=sb
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=system-s3","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="Bucket",type="string",JSONPath=`.status.bucketName`
// +kubebuilder:printcolumn:name="Quota",type="string",JSONPath=`.spec.quota`
// +kubebuilder:printcolumn:name="Used",type="integer",JSONPath=`.status.usedBytes`
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Secret",type="string",JSONPath=`.status.secretRef.name`
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SystemBucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SystemBucketSpec    `json:"spec"`
	Status *SystemBucketStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type SystemBucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []SystemBucket `json:"items"`
}

// +k8s:deepcopy-gen=true
// +kubebuilder:validation:XValidation:rule="oldSelf == null || !has(self.bucketName) && !has(oldSelf.bucketName) || has(self.bucketName) && has(oldSelf.bucketName) && self.bucketName == oldSelf.bucketName",message="spec.bucketName is immutable after creation."
// +kubebuilder:validation:XValidation:rule="oldSelf == null || !has(self.credentialsSecretName) && !has(oldSelf.credentialsSecretName) || has(self.credentialsSecretName) && has(oldSelf.credentialsSecretName) && self.credentialsSecretName == oldSelf.credentialsSecretName",message="spec.credentialsSecretName is immutable after creation: renaming it would leave the old Secret behind — it is owned by this resource, so nothing else removes it while this one lives — and the new one starts empty, which the controller reads as a lost secret key and answers with a rotation. Delete the SystemBucket and create it with the name you want."
type SystemBucketSpec struct {
	// BucketName is the name of the bucket in S3. Defaults to
	// <metadata.namespace>-<metadata.name> when omitted, because the backend
	// bucket namespace is flat while SystemBuckets are namespaced. Immutable
	// after creation.
	//
	// The schema bounds it the way S3 does — 3 to 63 characters, lower-case
	// letters, digits and dashes, starting and ending with a letter or a digit.
	// The derived form is bounded too, but not by the schema, which cannot see
	// it: a namespace and a name compose to as much as 127 characters, so the
	// admission webhook and the controller both refuse a name S3 would not take.
	// +optional
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	BucketName string `json:"bucketName,omitempty"`

	// Quota is how much the bucket may hold, as a Kubernetes quantity. It becomes
	// the bucket's quota in the backend — a write past it is refused — and it is
	// what the data plane sizes its volumes from: the store provisions the sum of
	// every SystemBucket's quota, so asking for more here is what makes the
	// volumes grow.
	//
	// It is named after what it does. The field used to be called size, which
	// read as a reservation — something the bucket is given and holds — while
	// what it has always been is a ceiling the backend enforces per write.
	//
	// The default is deliberately small. A system bucket is a platform
	// component's working set, not a data lake, and a default that fits a
	// component which never says how much it needs is better than one that
	// reserves tens of gigabytes per consumer on a control-plane volume.
	//
	// It can be raised at any time. Lowering it is accepted but does not shrink
	// anything: the quota follows, so writes stop at the new value, while the
	// volumes the store already provisioned stay the size they are — a
	// PersistentVolumeClaim cannot shrink.
	// +optional
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?(E|P|T|G|M|k|Ei|Pi|Ti|Gi|Mi|Ki)?$`
	Quota string `json:"quota,omitempty"`
}

// +k8s:deepcopy-gen=true
type SystemBucketStatus struct {
	// ObservedGeneration is the most recent .metadata.generation reconciled
	// by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is a coarse-grained summary derived from Conditions.
	// +kubebuilder:validation:Enum=Pending;InProgress;Ready;Error
	// +optional
	Phase string `json:"phase,omitempty"`

	// Endpoint is the in-cluster S3 endpoint URL of the system storage.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// BucketName is the effective bucket name created in the backend.
	// +optional
	BucketName string `json:"bucketName,omitempty"`

	// ConnectionRef references the ConfigMap (in this bucket's namespace) holding
	// the S3 connection parameters: where the storage is, what the bucket is
	// called, and how to exchange a ServiceAccount token for credentials.
	//
	// A ConfigMap and not a Secret because nothing in it is secret. There is no
	// long-lived key to keep: credentials are minted per consumer, for an hour,
	// against a token Kubernetes issued and RBAC approved.
	// +optional
	ConnectionRef *LocalObjectReference `json:"connectionRef,omitempty"`

	// QuotaBytes is the quota the backend enforces on this bucket, derived from
	// spec.quota. Reported back rather than echoed from the spec: what stops a
	// write is what the backend holds, and the two differ while a reconcile is
	// still to come.
	// +optional
	QuotaBytes int64 `json:"quotaBytes,omitempty"`

	// UsedBytes and Objects are what the bucket currently holds, as the backend
	// counts it. They are the only place a consumer can see how close it is to
	// its quota, and the only input an operator has for raising spec.quota before
	// writes start failing.
	// +optional
	UsedBytes int64 `json:"usedBytes,omitempty"`
	// +optional
	Objects int64 `json:"objects,omitempty"`

	// Conditions hold the latest stage states. Known types: StorageReady,
	// BucketReady, ConnectionReady, Ready.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// LocalObjectReference references an object by name within a known namespace.
// +k8s:deepcopy-gen=true
type LocalObjectReference struct {
	// Name of the referenced object.
	Name string `json:"name"`
}

// Well-known condition types for SystemBucket.
const (
	// SystemBucketConditionStorageReady reports whether the system data plane
	// is up and serving S3. It gates everything else: no bucket can be created
	// while it is False.
	SystemBucketConditionStorageReady = "StorageReady"

	// SystemBucketConditionBucketReady reports whether the bucket exists in the
	// backend.
	SystemBucketConditionBucketReady = "BucketReady"

	// SystemBucketConditionConnectionReady reports whether the ConfigMap with
	// the connection parameters has been written. It is not a statement about
	// credentials — there are none to be ready — but about the consumer having
	// been told where to go and how to ask.
	SystemBucketConditionConnectionReady = "ConnectionReady"

	// SystemBucketConditionReady is the aggregate.
	SystemBucketConditionReady = "Ready"
)

// SystemBucketConditionTypes is every condition type a SystemBucket publishes.
//
// A condition type that is declared but never written is worse than one that
// does not exist: it documents an observation nobody can act on, and reads as
// "not yet evaluated" forever. Keeping the set in one list is what lets a test
// hold the controller to it.
var SystemBucketConditionTypes = []string{
	SystemBucketConditionStorageReady,
	SystemBucketConditionBucketReady,
	SystemBucketConditionConnectionReady,
	SystemBucketConditionReady,
}

// ManagedByLabel and ManagedByValue mark every object this module creates,
// including the connection ConfigMap it writes into a consumer's namespace.
//
// The label is not decoration: the controller's cache is scoped to it, because a
// cache over every Secret in the cluster would hold each Helm release in the
// controller's heap. Anything this module has to read through that cache must
// carry it.
const (
	ManagedByLabel = "app.kubernetes.io/managed-by"
	ManagedByValue = "system-s3"
)

// SystemBucketKind is the kind constant used for OwnerReferences and dynamic
// GVK lookups.
const SystemBucketKind = "SystemBucket"

// Namespace admission gate for SystemBucket. Every namespace a DKP module owns
// carries these through helm_lib_module_labels, so the label is an authoritative
// "this is a platform namespace" marker — unlike a d8-* name pattern, which is a
// convention a tenant can imitate.
const (
	LabelHeritage     = "heritage"
	HeritageDeckhouse = "deckhouse"
)

// Keys written into the connection ConfigMap referenced by
// SystemBucketStatus.ConnectionRef. Standardised so a consuming component can
// `envFrom` it directly, and named as the S3 world names them.
//
// There is no key pair here and there is no Secret next to it: access to a
// bucket is obtained by exchanging a ServiceAccount token at the credentials
// endpoint, which mints a key that lives for an hour. What a consumer needs to
// do that is the endpoint's address, the audience its token has to carry, and
// the CA that signs the endpoint (the module's own, so nothing in the cluster
// trusts it by default) — and all three are public.
const (
	ConnectionKeyS3Endpoint = "S3_ENDPOINT"
	ConnectionKeyS3Region   = "S3_REGION"
	ConnectionKeyS3Bucket   = "S3_BUCKET"

	ConnectionKeySTSEndpoint = "S3_STS_ENDPOINT"
	ConnectionKeySTSAudience = "S3_STS_AUDIENCE"
	ConnectionKeySTSCACert   = "S3_STS_CA_CERT"
)

// Status.phase values.
//
// Declared here on purpose, even though sds-common-lib's conditions package has
// the same four: this module is the public API a consumer imports, and it must not
// drag the controller's helper library in to name a value it reads off an object.
// The controller writes the library's values; a test in images/controller keeps
// the two sets identical, so a change on either side is caught rather than
// discovered by a consumer comparing against a string that no longer appears.
const (
	PhasePending    = "Pending"
	PhaseInProgress = "InProgress"
	PhaseReady      = "Ready"
	PhaseError      = "Error"
)
