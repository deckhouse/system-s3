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

// A store with no data plane has no series: store_ready == 0 is what pages, and
// a store nobody asked for yet is not down.
func TestForgetStore(t *testing.T) {
	StoreReady("system", false)
	StoreCapacityBytes("system", nil)
	ForgetStore("system")
	if n := testutil.CollectAndCount(storeReady); n != 0 {
		t.Errorf("store_ready has %d series after ForgetStore", n)
	}
	if n := testutil.CollectAndCount(storeCapacityBytes); n != 0 {
		t.Errorf("store_capacity_bytes has %d series after ForgetStore", n)
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
func TestBucketSizeUpToDateKeepsOnlyTheCurrentReason(t *testing.T) {
	BucketSizeUpToDate("ns", "b", "QuotaPending", false)
	BucketSizeUpToDate("ns", "b", "Applied", true)
	if n := testutil.CollectAndCount(bucketSizeUpToDate); n != 1 {
		t.Fatalf("%d series, want only the current reason's", n)
	}
	if got := testutil.ToFloat64(bucketSizeUpToDate.WithLabelValues("ns", "b", "Applied")); got != 1 {
		t.Errorf("bucket_size_applied{reason=Applied}=%v, want 1", got)
	}

	ForgetBucket("ns", "b", "bucket")
	if n := testutil.CollectAndCount(bucketSizeUpToDate); n != 0 {
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

// A migration that finished must leave nothing of the old cluster behind: its
// pods drawn next to the new ones would read as a store with twice the replicas.
// And store_info is the serving plane's shape alone, replaced rather than added
// to when the shape changes, or the console would compare the settings with two
// answers.
func TestDataPlanesReplacesWhatWasPublished(t *testing.T) {
	DataPlanes([]DataPlane{
		{Incarnation: 1, Serving: true, StorageClass: "local", HighAvailability: true, Replicas: 3,
			Pods: []DataPlanePod{{Name: "system-garage-0", Node: "a", Ready: true}, {Name: "system-garage-1"}}},
		{Incarnation: 2, StorageClass: "replicated", Replicas: 1,
			Pods: []DataPlanePod{{Name: "system-garage-i2-0", Node: "b", Ready: true}}},
	})
	if n := testutil.CollectAndCount(replicaReady); n != 3 {
		t.Fatalf("%d replica_ready series during the migration, want 3", n)
	}
	if got := testutil.ToFloat64(replicaReady.WithLabelValues("1", "system-garage-1", "", "true")); got != 0 {
		t.Errorf("replica_ready of the unscheduled pod = %v, want 0", got)
	}
	if n := testutil.CollectAndCount(planeInfo); n != 2 {
		t.Errorf("%d plane_info series during the migration, want 2", n)
	}
	if got := testutil.ToFloat64(storeInfo.WithLabelValues("local", "true", "3")); got != 1 {
		t.Errorf("store_info of the serving plane = %v, want 1", got)
	}

	DataPlanes([]DataPlane{
		{Incarnation: 2, Serving: true, StorageClass: "replicated", Replicas: 1,
			Pods: []DataPlanePod{{Name: "system-garage-i2-0", Node: "b", Ready: true}}},
	})
	if n := testutil.CollectAndCount(replicaReady); n != 1 {
		t.Errorf("%d replica_ready series after the migration, want 1", n)
	}
	if got := testutil.ToFloat64(replicaReady.WithLabelValues("2", "system-garage-i2-0", "b", "true")); got != 1 {
		t.Errorf("replica_ready of the new serving pod = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(storeInfo); n != 1 {
		t.Fatalf("%d store_info series, want exactly one", n)
	}
	if got := testutil.ToFloat64(storeInfo.WithLabelValues("replicated", "false", "1")); got != 1 {
		t.Errorf("store_info of the new shape = %v, want 1", got)
	}

	DataPlanes(nil)
	if n := testutil.CollectAndCount(storeInfo) + testutil.CollectAndCount(planeInfo) + testutil.CollectAndCount(replicaReady); n != 0 {
		t.Errorf("%d series for a store with no data plane, want none", n)
	}
}

// A condition that changed keeps only its current status and reason: an alert
// on PlacementBlocked that kept matching a series left at zero, or at one, after
// the placement went through would never resolve.
func TestModuleConditionsKeepOnlyTheCurrentValue(t *testing.T) {
	ModuleConditions([]Condition{{Type: "PlacementReady", Status: "False", Reason: "PlacementBlocked"}})
	ModuleConditions([]Condition{{Type: "PlacementReady", Status: "True", Reason: "Placed"}})
	if n := testutil.CollectAndCount(moduleCondition); n != 1 {
		t.Fatalf("%d condition series, want only the current one", n)
	}
	if got := testutil.ToFloat64(moduleCondition.WithLabelValues("PlacementReady", "True", "Placed")); got != 1 {
		t.Errorf("condition{PlacementReady,True,Placed}=%v, want 1", got)
	}
}

// A bucket that is deleted takes its credential requests with it, or the
// counters of every bucket that ever existed would stay for good.
func TestForgetBucketDropsItsCredentialRequests(t *testing.T) {
	STSRequest("ns", "gone", "Granted")
	STSRequest("", "", "InvalidRequest")
	ForgetBucket("ns", "gone", "ns-gone")
	if got := testutil.ToFloat64(stsRequestsTotal.WithLabelValues("", "", "InvalidRequest")); got < 1 {
		t.Errorf("the requests of no bucket were dropped with a bucket")
	}
	if n := testutil.CollectAndCount(stsRequestsTotal); n != 1 {
		t.Errorf("%d sts_requests_total series after the bucket was forgotten, want 1", n)
	}
}
