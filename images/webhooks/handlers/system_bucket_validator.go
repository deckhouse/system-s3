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
	"context"
	"fmt"

	"github.com/slok/kubewebhook/v2/pkg/model"
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// SystemBucketValidate admits a SystemBucket only when
//
//   - it lives in a namespace a Deckhouse module owns (label heritage: deckhouse),
//     which is what keeps the platform's system storage out of tenant hands,
//   - the bucket name it maps to is one S3 accepts, and
//   - that name is not already claimed by another SystemBucket.
//
// The last check exists because the backend bucket namespace is flat while
// SystemBuckets are namespaced: two resources mapping to one bucket would share
// storage silently, and the second one to be deleted would take the first one's
// data with it. The one before it exists because that mapping can produce a name
// S3 will not take, and the schema cannot see it — see bucketNameProblem.
func (v *Validator) SystemBucketValidate(ctx context.Context, _ *model.AdmissionReview, obj metav1.Object) (*kwhvalidating.ValidatorResult, error) {
	bucket, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return reject("unexpected object type: not a SystemBucket"), nil
	}

	// An object on its way out is admitted whatever it says.
	//
	// Deleting a SystemBucket does not delete the object: the controller has a
	// finalizer on it, and removing that finalizer is an UPDATE, which comes back
	// through this webhook. So a resource this webhook refuses — a bucket-name
	// collision, or a namespace that lost its heritage label — could be created
	// (or become invalid later), and then never deleted: the controller's
	// finalizer patch is rejected on every pass, and the object sits in
	// Terminating for as long as the module exists. Measured: an e2e run produced
	// a SystemBucket that survived hundreds of reconciles, each one logging
	// "denied the request: bucket name ... is already used by".
	//
	// There is nothing to protect at this point either. The only updates a
	// terminating object receives are finalizer and status writes, and the spec
	// is immutable in the CRD, so admitting them cannot introduce the state the
	// rules below are about.
	if bucket.GetDeletionTimestamp() != nil {
		return &kwhvalidating.ValidatorResult{Valid: true}, nil
	}

	platform, err := v.platformNamespace(ctx, bucket.GetNamespace())
	if err != nil {
		return nil, err
	}
	if !platform {
		return reject(fmt.Sprintf(
			"SystemBucket is only allowed in a Deckhouse platform namespace (one labelled %s=%s); namespace %q is not one. "+
				"The system object storage serves platform components; application workloads get buckets from the sds-object module instead",
			labelHeritage, heritageDeckhouse, bucket.GetNamespace())), nil
	}

	name := effectiveBucketName(bucket)
	if problem := bucketNameProblem(name); problem != "" {
		explicit, _, _ := unstructured.NestedString(bucket.Object, "spec", "bucketName")
		if explicit != "" {
			// The CRD checks spec.bucketName and schema validation runs before this
			// webhook, so an explicit name should never arrive broken. Reported
			// rather than assumed away: reaching this means the schema and the two
			// copies of this rule have come apart, and a wrong refusal here is
			// easier to understand than a bucket the backend rejects later.
			return reject(fmt.Sprintf("spec.bucketName %q cannot be used: %s", name, problem)), nil
		}
		return reject(fmt.Sprintf(
			"the bucket name this SystemBucket maps to, %q, cannot be used: %s. "+
				"With spec.bucketName unset the name is derived as <namespace>-<name>, "+
				"which here is %q; set spec.bucketName to something S3 accepts",
			name, problem, bucket.GetNamespace()+"-"+bucket.GetName())), nil
	}

	existing, err := v.dyn.Resource(systemBucketGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list SystemBuckets: %w", err)
	}
	for i := range existing.Items {
		other := &existing.Items[i]
		if other.GetNamespace() == bucket.GetNamespace() && other.GetName() == bucket.GetName() {
			continue // the object being updated
		}
		if effectiveBucketName(other) == name {
			return reject(fmt.Sprintf(
				"bucket name %q is already used by SystemBucket %s/%s",
				name, other.GetNamespace(), other.GetName())), nil
		}
	}

	return &kwhvalidating.ValidatorResult{Valid: true}, nil
}

// platformNamespace reports whether the namespace carries the heritage label
// helm_lib stamps on every Deckhouse module namespace. A namespace that does not
// exist is not one (the API server would reject the object anyway).
func (v *Validator) platformNamespace(ctx context.Context, name string) (bool, error) {
	ns, err := v.dyn.Resource(namespaceGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read namespace %q: %w", name, err)
	}
	return ns.GetLabels()[labelHeritage] == heritageDeckhouse, nil
}
