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

package monitoring

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestStoreReady(t *testing.T) {
	StoreReady("system", true)
	if got := testutil.ToFloat64(storeReady.WithLabelValues("system")); got != 1 {
		t.Errorf("store_ready=%v, want 1", got)
	}
	StoreReady("system", false)
	if got := testutil.ToFloat64(storeReady.WithLabelValues("system")); got != 0 {
		t.Errorf("store_ready=%v, want 0", got)
	}
}

// A capacity that is not reported must publish 0, not the previous reading.
//
// Carrying the last known value forward would keep an alert quiet about a store
// that has stopped answering — the reading would look current and be a memory of
// a cluster that is no longer there.
func TestStoreCapacityBytesForgetsWhatItCannotSee(t *testing.T) {
	v := int64(32212254720)
	StoreCapacityBytes("system", &v)
	if got := testutil.ToFloat64(storeCapacityBytes.WithLabelValues("system")); got != float64(v) {
		t.Fatalf("store_capacity_bytes=%v, want %d", got, v)
	}

	StoreCapacityBytes("system", nil)
	if got := testutil.ToFloat64(storeCapacityBytes.WithLabelValues("system")); got != 0 {
		t.Errorf("store_capacity_bytes=%v after an absent reading, want 0", got)
	}
}

// A reason that is gone must not keep a series: the alert names the reason, and
// a QuotaPending series left at 0 after the quota landed would keep firing.
func TestBucketSizeAppliedKeepsOnlyTheCurrentReason(t *testing.T) {
	BucketSizeApplied("ns", "b", "QuotaPending", false)
	BucketSizeApplied("ns", "b", "Applied", true)
	if n := testutil.CollectAndCount(bucketSizeApplied); n != 1 {
		t.Fatalf("%d series, want only the current reason's", n)
	}
	if got := testutil.ToFloat64(bucketSizeApplied.WithLabelValues("ns", "b", "Applied")); got != 1 {
		t.Errorf("bucket_size_applied{reason=Applied}=%v, want 1", got)
	}

	ForgetBucket("ns", "b", "bucket")
	if n := testutil.CollectAndCount(bucketSizeApplied); n != 0 {
		t.Errorf("%d series after the bucket was forgotten, want none", n)
	}
}

// The object half of a bucket's usage is published next to the bytes and
// forgotten with them: the fullness alerts read both.
func TestBucketUsagePublishesTheObjectCount(t *testing.T) {
	BucketUsage("ns", "o", "ns-o", 100, 1000, 7, 16384)
	if got := testutil.ToFloat64(bucketObjects.WithLabelValues("ns", "o", "ns-o")); got != 7 {
		t.Errorf("bucket_objects=%v, want 7", got)
	}
	if got := testutil.ToFloat64(bucketQuotaObjects.WithLabelValues("ns", "o", "ns-o")); got != 16384 {
		t.Errorf("bucket_quota_objects=%v, want 16384", got)
	}
	ForgetBucket("ns", "o", "ns-o")
	if n := testutil.CollectAndCount(bucketQuotaObjects); n != 0 {
		t.Errorf("%d bucket_quota_objects series after the bucket was forgotten", n)
	}
}
