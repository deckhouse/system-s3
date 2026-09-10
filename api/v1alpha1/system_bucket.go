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
// bucket in the system backend, mints a dedicated access key / secret key for
// it, and writes a Secret with the standard S3 connection variables in the same
// namespace (status.secretRef).
//
// It is deliberately the dumbest resource the module could ship, and every
// omission is a decision:
//
//   - No access resource. The consumer is a platform component, not a tenant
//     handing out scoped credentials, so the credentials come with the bucket.
//     Rotation is an annotation (see RotateAnnotation), not a second object.
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
// Key rotation: add or change the annotation storage.deckhouse.io/rotate to
// trigger issuance of a fresh key pair (the Secret is updated and the previous
// key is revoked). status.observedRotation records the last processed value.
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

	// CredentialsSecretName overrides the name of the credentials Secret
	// written in this bucket's namespace. Defaults to
	// <metadata.name>-s3-credentials.
	//
	// Immutable after creation: the Secret is owned by this resource, so renaming
	// it would leave the old one behind for as long as this resource lives, and
	// the new name starts with no Secret at all — which the controller treats as
	// a lost secret key and answers by rotating the access key. Delete the
	// SystemBucket and create it with the name you want.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CredentialsSecretName string `json:"credentialsSecretName,omitempty"`
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

	// AccessKeyID is the public access key id issued for this bucket (the
	// secret key is only written into the credentials Secret).
	// +optional
	AccessKeyID string `json:"accessKeyID,omitempty"`

	// SecretRef references the Secret (in this bucket's namespace) holding the
	// S3 connection variables and credentials.
	// +optional
	SecretRef *LocalSecretReference `json:"secretRef,omitempty"`

	// ObservedRotation is the last value of the rotation annotation processed
	// by the controller. When it differs from the current annotation value a
	// new key pair is issued.
	// +optional
	ObservedRotation string `json:"observedRotation,omitempty"`

	// SupersededAccessKeyID is a key that has been replaced and still
	// has to be revoked in the backend.
	//
	// It exists because the two halves of a rotation cannot be made
	// atomic: the backend mints a key and hands back its secret half
	// once and never again, so the secret has to reach the consumer's
	// Secret before the old key is taken away. Revoking first leaves a
	// window — a conflict on the Secret write, a reconcile timeout, an
	// evicted Pod — in which the consumer holds a key that no longer
	// works and the new secret exists only in the controller's memory.
	//
	// So the old key is recorded here instead and revoked on a later
	// pass, once the new one is durable. The field is what makes that
	// retryable: a revoke that fails is tried again rather than
	// forgotten, which is the difference between a stale key and an
	// orphan nobody will ever clean up.
	// +optional
	SupersededAccessKeyID string `json:"supersededAccessKeyID,omitempty"`

	// LastRotationTime is the timestamp of the most recent key issuance.
	// +optional
	LastRotationTime *metav1.Time `json:"lastRotationTime,omitempty"`

	// Conditions hold the latest stage states. Known types: StorageReady,
	// BucketReady, CredentialsReady, Ready.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// LocalSecretReference references a Secret by name within a known namespace.
// +k8s:deepcopy-gen=true
type LocalSecretReference struct {
	// Name of the referenced Secret.
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

	// SystemBucketConditionCredentialsReady reports whether the credentials
	// Secret holds a key pair that is valid for the bucket.
	SystemBucketConditionCredentialsReady = "CredentialsReady"

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
	SystemBucketConditionCredentialsReady,
	SystemBucketConditionReady,
}

// ManagedByLabel and ManagedByValue mark every object this module creates,
// including the credentials Secret it writes into a consumer's namespace.
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

// RotateAnnotation, when its value changes, triggers issuance of a fresh access
// key pair for the SystemBucket.
const RotateAnnotation = "storage.deckhouse.io/rotate"

// Namespace admission gate for SystemBucket. Every namespace a DKP module owns
// carries these through helm_lib_module_labels, so the label is an authoritative
// "this is a platform namespace" marker — unlike a d8-* name pattern, which is a
// convention a tenant can imitate.
const (
	LabelHeritage     = "heritage"
	HeritageDeckhouse = "deckhouse"
)

// Keys written into the credentials Secret referenced by
// SystemBucketStatus.SecretRef. Standardised so a consuming component can
// `envFrom` it directly, and identical to the keys the user-facing sds-object
// module writes.
const (
	SecretKeyS3Endpoint      = "S3_ENDPOINT"
	SecretKeyS3Region        = "S3_REGION"
	SecretKeyS3Bucket        = "S3_BUCKET"
	SecretKeyAccessKeyID     = "AWS_ACCESS_KEY_ID"
	SecretKeySecretAccessKey = "AWS_SECRET_ACCESS_KEY"
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
