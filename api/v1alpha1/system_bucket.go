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
// +kubebuilder:resource:scope=Namespaced,shortName=sb
// +kubebuilder:subresource:status
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
type SystemBucketSpec struct {
	// BucketName is the name of the bucket in S3. Defaults to
	// <metadata.namespace>-<metadata.name> when omitted, because the backend
	// bucket namespace is flat while SystemBuckets are namespaced. Immutable
	// after creation.
	// +optional
	BucketName string `json:"bucketName,omitempty"`

	// CredentialsSecretName overrides the name of the credentials Secret
	// written in this bucket's namespace. Defaults to
	// <metadata.name>-s3-credentials.
	// +optional
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
	SecretKeyS3Endpoint     = "S3_ENDPOINT"
	SecretKeyS3Region       = "S3_REGION"
	SecretKeyS3Bucket       = "S3_BUCKET"
	SecretKeyAccessKeyID    = "AWS_ACCESS_KEY_ID"
	SecretKeySecretAccessID = "AWS_SECRET_ACCESS_KEY"
)

// Status.phase values.
const (
	PhasePending    = "Pending"
	PhaseInProgress = "InProgress"
	PhaseReady      = "Ready"
	PhaseError      = "Error"
)
