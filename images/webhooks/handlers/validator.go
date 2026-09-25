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

package handlers

import (
	"fmt"
	"regexp"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// Validator carries the cross-resource lookup client the admission validator
// needs. Schema-level checks (shape, immutability, the pattern of an explicit
// spec.bucketName) are enforced by the CRD and its CEL rules; what is left here
// are the checks CEL cannot express, because they look at other resources — the
// namespace's labels and the other SystemBuckets — or at a value the schema
// cannot see, which is the bucket name derived from the resource's own
// coordinates.
type Validator struct {
	dyn dynamic.Interface
}

// NewValidator builds a Validator backed by the given dynamic client.
func NewValidator(dyn dynamic.Interface) *Validator {
	return &Validator{dyn: dyn}
}

// Contract constants mirrored from api/v1alpha1 (kept as local literals to avoid
// a build dependency on the API module).
const (
	// labelHeritage / heritageDeckhouse mark a namespace a Deckhouse module owns.
	// The label is what admits a SystemBucket, deliberately instead of a d8-*
	// name pattern: the API review banned regex namespace selection, and a name
	// is a convention a tenant can imitate while the label is stamped by
	// helm_lib on module namespaces only.
	labelHeritage     = "heritage"
	heritageDeckhouse = "deckhouse"
)

// GroupVersionResources the validator queries.
var (
	systemBucketGVR = schema.GroupVersionResource{
		Group: "storage.deckhouse.io", Version: "v1alpha1", Resource: "systembuckets",
	}
	namespaceGVR = schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "namespaces",
	}
)

// effectiveBucketName is the S3 bucket name a SystemBucket maps to:
// spec.bucketName when set, otherwise <namespace>-<name>. It mirrors
// effectiveBucketName in the controller; the two must agree, or the webhook
// would guard a name the controller does not create.
func effectiveBucketName(u *unstructured.Unstructured) string {
	if name, _, _ := unstructured.NestedString(u.Object, "spec", "bucketName"); name != "" {
		return name
	}
	return u.GetNamespace() + "-" + u.GetName()
}

// What a bucket name has to look like. Mirrors the schema of spec.bucketName (the
// markers on SystemBucketSpec.BucketName in api/v1alpha1, rendered into crds/),
// deliberately: the point is that a derived name is held to exactly what an
// explicit one is held to.
const (
	bucketNameMinLength = 3
	bucketNameMaxLength = 63
)

var bucketNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// bucketNameProblem says what is wrong with a bucket name, or returns the empty
// string when nothing is.
//
// The schema checks spec.bucketName, and only spec.bucketName. Left unset the
// name is derived from the resource's coordinates as <namespace>-<name>, and
// nothing checked that: two labels of up to 63 characters each compose to as
// much as 127, against an S3 limit of 63. The refusal then came from the backend
// on the first reconcile, as BucketReady=False, rather than from admission on
// the write that caused it.
func bucketNameProblem(name string) string {
	switch {
	case len(name) < bucketNameMinLength:
		return fmt.Sprintf("it is %d characters long, and S3 bucket names start at %d",
			len(name), bucketNameMinLength)
	case len(name) > bucketNameMaxLength:
		return fmt.Sprintf("it is %d characters long, and S3 bucket names stop at %d",
			len(name), bucketNameMaxLength)
	case !bucketNamePattern.MatchString(name):
		return "it is not a valid S3 bucket name: lower-case letters, digits and " +
			"dashes only, starting and ending with a letter or a digit"
	default:
		return ""
	}
}
