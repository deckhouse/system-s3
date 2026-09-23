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

package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/deckhouse/system-s3/api/v1alpha1"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// sizeCondition works out the SizeApplied condition: whether spec.size has
// taken effect, and if not, which half of it has not.
//
// Raising spec.size does two things, and they finish at different times. The
// quota is a call to Garage and is in place within one reconcile. The volumes
// are sized from the sum of every bucket's size, and growing them is the
// StorageClass's business — minutes on a class that expands online, never on one
// that does not expand at all. A consumer who raised the size because writes
// were failing needs to know which of the two it is waiting for, and a bucket
// that is Ready says neither.
//
// It returns nil when the pass knows nothing about the volumes (the store's
// convergence stopped before sizing them), which leaves the last answer in place
// instead of replacing it with a guess.
func sizeCondition(requested int64, usage backend.BucketUsage, capacity backend.StoreCapacity) *metav1.Condition {
	if capacity.WantBytes == 0 {
		return nil
	}
	cond := func(status metav1.ConditionStatus, reason, msg string) *metav1.Condition {
		return &metav1.Condition{
			Type:    v1alpha1.SystemBucketConditionSizeApplied,
			Status:  status,
			Reason:  reason,
			Message: msg,
		}
	}
	size := bytesString(requested)
	want, have := bytesString(capacity.WantBytes), bytesString(capacity.HaveBytes)

	switch {
	case usage.QuotaBytes != requested:
		return cond(metav1.ConditionFalse, v1alpha1.SizeReasonQuotaPending, fmt.Sprintf(
			"the backend enforces a quota of %s and spec.size asks for %s; the next reconcile applies it",
			bytesString(usage.QuotaBytes), size))
	case capacity.Problem != "":
		return cond(metav1.ConditionFalse, v1alpha1.SizeReasonCannotGrow, fmt.Sprintf(
			"the quota of %s is in place, but the store's volumes cannot grow from %s to the %s every bucket's size adds up to, "+
				"so writes can fail before the quota is reached: %s",
			size, have, want, capacity.Problem))
	case capacity.HaveBytes < capacity.WantBytes:
		msg := fmt.Sprintf("the quota of %s is in place; the store's volumes are growing from %s to %s", size, have, want)
		if capacity.Detail != "" {
			msg += " (" + capacity.Detail + ")"
		} else {
			msg += " (no claim reports a resize under way yet)"
		}
		return cond(metav1.ConditionFalse, v1alpha1.SizeReasonGrowing, msg)
	default:
		return cond(metav1.ConditionTrue, v1alpha1.SizeReasonApplied, fmt.Sprintf(
			"the quota of %s is in place and the store's volumes (%s each) fit every bucket's size",
			size, have))
	}
}

// bytesString prints a byte count the way a quantity in a spec is written.
func bytesString(n int64) string {
	return resource.NewQuantity(n, resource.BinarySI).String()
}
