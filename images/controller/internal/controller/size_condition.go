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

// sizeCondition works out the SizeUpToDate condition: whether spec.size has
// taken effect as the bucket's own quota. Shared PVC capacity and CSI failures
// belong exclusively to Module/system-s3 CapacityReady.
func sizeCondition(requested int64, usage backend.BucketUsage) *metav1.Condition {
	cond := func(status metav1.ConditionStatus, reason, msg string) *metav1.Condition {
		return &metav1.Condition{
			Type:    v1alpha1.SystemBucketConditionSizeUpToDate,
			Status:  status,
			Reason:  reason,
			Message: msg,
		}
	}
	size := bytesString(requested)

	switch {
	case usage.QuotaBytes != requested:
		return cond(metav1.ConditionFalse, v1alpha1.SizeReasonQuotaPending, fmt.Sprintf(
			"the backend enforces a quota of %s and spec.size asks for %s; the next reconcile applies it",
			bytesString(usage.QuotaBytes), size))
	default:
		return cond(metav1.ConditionTrue, v1alpha1.SizeReasonApplied, fmt.Sprintf(
			"the bucket quota of %s is in place", size))
	}
}

// bytesString prints a byte count the way a quantity in a spec is written.
func bytesString(n int64) string {
	return resource.NewQuantity(n, resource.BinarySI).String()
}
