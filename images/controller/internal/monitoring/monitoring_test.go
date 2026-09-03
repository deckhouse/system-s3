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
