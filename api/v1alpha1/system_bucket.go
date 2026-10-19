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

// SystemBucket is deliberately the dumbest resource the module could ship, and
// every omission is a decision:
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

// SystemBucket asks for a bucket in the platform's system object
// storage. The controller creates the bucket in the system backend and
// writes a ConfigMap in the same namespace (see
// `status.connectionRef`) with the S3 connection parameters.
//
// The resource is for Deckhouse components only. It is admitted
// exclusively in namespaces labelled `heritage: deckhouse`, and no
// user-facing RBAC is granted for it.
//
// There is no reclaim policy: **deleting a SystemBucket deletes the
// bucket and every object in it.**
//
// **No credentials are written anywhere, and none are long-lived.** A
// consumer exchanges its ServiceAccount token at the credentials
// endpoint (`S3_STS_ENDPOINT` in the ConfigMap) for a key pair the
// backend stops honouring at most thirty minutes later. Who may do that, for which
// bucket, is an ordinary Kubernetes RBAC grant on the virtual
// `systembuckets/read` and `systembuckets/write` subresources — so
// access is granted and revoked like any other permission, and nothing
// has to be rotated when it is taken away.
//
// **The consumer has to renew its credentials.** Ask for a key near the
// end of its lifetime rather than on every request — every request
// mints a key, and a bucket is allowed at most 50 unexpired ones at a
// time.
//
// +kubebuilder:resource:scope=Namespaced,shortName=sb
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels={"heritage=deckhouse","module=system-s3","backup.deckhouse.io/cluster-config=true"}
// +kubebuilder:printcolumn:name="Bucket",type="string",JSONPath=`.status.bucketName`
// +kubebuilder:printcolumn:name="Size",type="string",JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="UsedBytes",type="integer",JSONPath=`.status.usedBytes`
// +kubebuilder:printcolumn:name="TotalBytes",type="integer",JSONPath=`.status.quotaBytes`
// +kubebuilder:printcolumn:name="Objects",type="integer",JSONPath=`.status.objects`
// +kubebuilder:printcolumn:name="ObjectLimit",type="integer",JSONPath=`.status.quotaObjects`,priority=1
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Connection",type="string",JSONPath=`.status.connectionRef.name`
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +crd-enricher:crd:minimal=true
// +crd-enricher:crd:preserveUnknownFields=false
type SystemBucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Desired state of the bucket.
	Spec SystemBucketSpec `json:"spec"`

	// Observed state of the bucket.
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
// +kubebuilder:validation:XValidation:rule="oldSelf == null || !has(oldSelf.size) || !has(self.size) || quantity(self.size).compareTo(quantity(oldSelf.size)) >= 0",message="spec.size cannot be lowered: the volumes the store provisioned for it cannot shrink, and a quota below what the bucket holds would only make its writes fail."
type SystemBucketSpec struct {
	// The schema bounds it the way S3 does — 3 to 63 characters, lower-case
	// letters, digits and dashes, starting and ending with a letter or a digit.
	// The derived form is bounded too, but not by the schema, which cannot see
	// it: a namespace and a name compose to as much as 127 characters, so the
	// controller refuses a name S3 would not take. Nothing refuses it at
	// admission: the resource is created and reports BucketReady=False.

	// Name of the bucket in S3. Defaults to
	// `<metadata.namespace>-<metadata.name>` when omitted: the
	// backend bucket namespace is flat, while SystemBuckets are
	// namespaced, so the default keeps two components from
	// colliding on a bucket name. Immutable after creation.
	// +optional
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	BucketName string `json:"bucketName,omitempty"`

	// The default is small because a default that fits a component which never
	// says how much it needs is better than one that reserves tens of gigabytes
	// per consumer on a control-plane volume.

	// How much the bucket asks for, as a Kubernetes quantity.
	//
	// It becomes the bucket's quota in the backend — a write past
	// it is refused — and it is what the data plane sizes its
	// volumes from: the store provisions the sum of every
	// SystemBucket's size, so asking for more here is what makes
	// the volumes grow.
	//
	// The default is deliberately small: a system bucket is a
	// platform component's working set, not a data lake.
	//
	// It can be raised at any time; the `SizeUpToDate` condition
	// says when the backend enforces the new quota. Shared PVC capacity
	// is reported separately by Module/system-s3.CapacityReady. It
	// cannot be lowered: the volumes the store provisioned for it
	// cannot shrink (a PersistentVolumeClaim cannot), and a quota
	// below what the bucket holds would only make its writes
	// fail.
	// +optional
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?(E|P|T|G|M|k|Ei|Pi|Ti|Gi|Mi|Ki)?$`
	Size string `json:"size,omitempty"`
}

// +k8s:deepcopy-gen=true
type SystemBucketStatus struct {
	// Most recent `metadata.generation` reconciled by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Coarse-grained summary derived from Conditions.
	// +kubebuilder:validation:Enum=Pending;InProgress;Ready;Error
	// +optional
	Phase string `json:"phase,omitempty"`

	// In-cluster S3 endpoint URL of the system object storage.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Effective bucket name created in the backend.
	// +optional
	BucketName string `json:"bucketName,omitempty"`

	// Reference to the ConfigMap (in this bucket's namespace) with
	// the S3 connection parameters: `S3_ENDPOINT` (https),
	// `S3_REGION`, `S3_BUCKET`, `S3_CA_CERT` (the CA that verifies
	// the endpoint), and — for asking for credentials —
	// `S3_STS_ENDPOINT`, `S3_STS_AUDIENCE`, `S3_STS_CA_CERT`.
	//
	// A ConfigMap and not a Secret: there is no key pair in it,
	// because the module issues none. Credentials are minted per
	// consumer, for at most thirty minutes, against a ServiceAccount token.
	// +optional
	// +crd-enricher:unset:properties.name.description
	ConnectionRef *LocalObjectReference `json:"connectionRef,omitempty"`

	// Quota the backend enforces on this bucket, derived from
	// `spec.size`. Reported back rather than echoed from the spec:
	// what stops a write is what the backend holds.
	// +optional
	QuotaBytes int64 `json:"quotaBytes,omitempty"`

	// Number of objects the backend lets this bucket hold: `spec.size`
	// divided by 64 KiB. Each object costs disk space beyond its bytes
	// (metadata, rounding to the filesystem block), and the store's
	// volumes allow for that overhead only up to this count. A bucket
	// that reaches it has its writes refused with 403 `AccessDenied`
	// ("Object quota is reached") even below `quotaBytes`; raising
	// `spec.size` raises both.
	// +optional
	QuotaObjects int64 `json:"quotaObjects,omitempty"`

	// UsedBytes and Objects are the only place a consumer can see how close it is
	// to its quota, and the only input an operator has for raising spec.size
	// before writes start failing.

	// Bytes the bucket currently holds, as the backend counts them.
	// +optional
	UsedBytes int64 `json:"usedBytes"`
	// Number of objects in the bucket, as the backend counts them.
	// +optional
	Objects int64 `json:"objects"`

	// Per-stage conditions: `BucketReady`,
	// `ConnectionReady`, and the aggregate `Ready`. Besides them,
	// `SizeUpToDate` says whether `spec.size` has taken effect:
	// `Applied` or `QuotaPending`. Shared storage conditions, including PVC
	// growth and Garage migrations, are published on Module/system-s3.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +crd-enricher:raw:items.properties.lastTransitionTime.description="Timestamp of the last status transition for this condition."
	// +crd-enricher:raw:items.properties.message.description="Human-readable explanation of the current status."
	// +crd-enricher:raw:items.properties.observedGeneration.description="Value of `metadata.generation` the condition was last set against."
	// +crd-enricher:raw:items.properties.reason.description="Machine-readable reason for the current status, suitable for\nlog filtering."
	// +crd-enricher:raw:items.properties.status.description="Current status of the condition."
	// +crd-enricher:raw:items.properties.type.description="Condition type, for example `Ready`, `BucketReady`,\n`ConnectionReady`, or `SizeUpToDate`."
	// +crd-enricher:unset:items.description
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
	// Deprecated: StorageReady is now published on Module/system-s3 only.
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

	// SystemBucketConditionSizeUpToDate reports whether spec.size has taken
	// effect as the backend quota. Shared PVC growth is reported separately on
	// Module/system-s3 as CapacityReady. SizeUpToDate is not part of Ready.
	SystemBucketConditionSizeUpToDate = "SizeUpToDate"
)

// Reasons of the SizeUpToDate condition.
const (
	// SizeReasonApplied: the quota is spec.size.
	SizeReasonApplied = "Applied"
	// SizeReasonQuotaPending: the backend still enforces another quota.
	SizeReasonQuotaPending = "QuotaPending"
	// Deprecated: Growing is now a Module CapacityReady reason.
	SizeReasonGrowing = "Growing"
	// Deprecated: CannotGrow is now a Module CapacityReady reason.
	SizeReasonCannotGrow = "CannotGrow"
)

// SystemBucketConditionTypes is every condition type a SystemBucket publishes.
//
// A condition type that is declared but never written is worse than one that
// does not exist: it documents an observation nobody can act on, and reads as
// "not yet evaluated" forever. Keeping the set in one list is what lets a test
// hold the controller to it.
var SystemBucketConditionTypes = []string{
	SystemBucketConditionBucketReady,
	SystemBucketConditionConnectionReady,
	SystemBucketConditionReady,
	SystemBucketConditionSizeUpToDate,
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

// Namespace gate for SystemBucket, checked by the controller before it
// provisions anything. Every namespace a DKP module owns carries these through
// helm_lib_module_labels, so the label is an authoritative "this is a platform
// namespace" marker — unlike a d8-* name pattern, which is a convention a tenant
// can imitate.
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
// endpoint, which mints a key that lives for at most 30 minutes. What a consumer needs to
// do that is the endpoint's address, the audience its token has to carry, and
// the CA that signs the endpoint (the module's own, so nothing in the cluster
// trusts it by default) — and all three are public.
//
// S3_ENDPOINT is https as well, and S3_CA_CERT is what verifies it. Its value is
// the same CA as S3_STS_CA_CERT today — one CA signs both — but it is a key of its
// own, so a consumer configures "the CA of the S3 endpoint" by that name and does
// not come to depend on the two endpoints sharing one.
const (
	ConnectionKeyS3Endpoint = "S3_ENDPOINT"
	ConnectionKeyS3Region   = "S3_REGION"
	ConnectionKeyS3Bucket   = "S3_BUCKET"
	ConnectionKeyS3CACert   = "S3_CA_CERT"

	ConnectionKeySTSEndpoint = "S3_STS_ENDPOINT"
	ConnectionKeySTSAudience = "S3_STS_AUDIENCE"
	ConnectionKeySTSCACert   = "S3_STS_CA_CERT"
)

// The same connection, in the names the AWS SDKs read on their own.
//
// With these in the environment an SDK that implements the web identity flow —
// the AWS CLI, boto3, aws-sdk-go-v2, the Java and JavaScript SDKs — asks the
// credentials endpoint for a key and renews it without a line of code. What
// stays with the consumer is what depends on its Pod rather than on the bucket:
// AWS_WEB_IDENTITY_TOKEN_FILE, the path of a projected ServiceAccount token for
// the S3_STS_AUDIENCE audience, and AWS_CA_BUNDLE, the path the CA is mounted at.
//
// AWS_ROLE_ARN names the bucket in the one ARN spelling the endpoint accepts
// (see RoleARNPrefix); there is no role behind it. The two endpoint keys and the
// region are written only while the credentials endpoint is served, like the
// S3_STS_* keys: without it an SDK given AWS_ROLE_ARN would have nowhere to go.
const (
	ConnectionKeyAWSRoleARN     = "AWS_ROLE_ARN"
	ConnectionKeyAWSEndpointSTS = "AWS_ENDPOINT_URL_STS"
	ConnectionKeyAWSEndpointS3  = "AWS_ENDPOINT_URL_S3"
	ConnectionKeyAWSRegion      = "AWS_REGION"

	// RoleARNPrefix is what AWS_ROLE_ARN starts with; the rest is
	// "<namespace>/<name>" of the SystemBucket.
	RoleARNPrefix = "arn:deckhouse:system-s3:::systembucket/"
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
