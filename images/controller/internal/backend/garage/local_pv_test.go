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

package garage

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Which volumes the collector considers stale, and — more importantly — which it
// must leave alone.
//
// This decides whether a volume holding data gets deleted. A Bound volume is
// serving a replica right now; a volume on a node that is still a master and
// still in the wanted set is the pool doing its job. Getting either wrong takes
// data with it, and the review found this file untested.
func TestWhichLocalPVsCountAsStale(t *testing.T) {
	ns := "d8-system-s3"
	live := []string{"m0", "m1", "m2"}
	wanted := map[string]struct{}{"pool-m0-0": {}, "pool-m1-0": {}}

	pv := func(name, node string, phase corev1.PersistentVolumePhase) *corev1.PersistentVolume {
		return &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{labelSystemLocalNode: node}},
			Status:     corev1.PersistentVolumeStatus{Phase: phase},
		}
	}

	for _, tc := range []struct {
		name     string
		pv       *corev1.PersistentVolume
		wantGone bool
	}{
		{
			// Released means its claim is gone: whatever it held is not coming back
			// through this volume.
			name: "released", pv: pv("pool-m0-0", "m0", corev1.VolumeReleased), wantGone: true,
		},
		{
			// The pool doing its job: free, on a live master, still wanted.
			name: "available, live node, in the pool", pv: pv("pool-m0-0", "m0", corev1.VolumeAvailable), wantGone: false,
		},
		{
			// The node stopped being a master, so nothing will ever bind this.
			name: "available on a node that is gone", pv: pv("pool-m9-0", "m9", corev1.VolumeAvailable), wantGone: true,
		},
		{
			// Left over from a previous incarnation: the name is not in the wanted
			// set any more.
			name: "available but no longer wanted", pv: pv("pool-old-0", "m0", corev1.VolumeAvailable), wantGone: true,
		},
		{
			// The one that must never be touched: it is serving a replica.
			name: "bound", pv: pv("pool-m1-0", "m1", corev1.VolumeBound), wantGone: false,
		},
		{
			// Bound on a node that is no longer a master is still bound: deleting it
			// would pull the volume out from under a running pod.
			name: "bound on a departed node", pv: pv("pool-m9-1", "m9", corev1.VolumeBound), wantGone: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(rfScheme(t)).WithObjects(tc.pv).Build()
			d := &Driver{client: c, apiReader: c, namespace: ns}
			pool := &corev1.PersistentVolumeList{Items: []corev1.PersistentVolume{*tc.pv}}

			if err := d.gcLocalPVs(context.Background(), pool, live, wanted); err != nil {
				t.Fatalf("gcLocalPVs: %v", err)
			}

			err := c.Get(context.Background(), client.ObjectKey{Name: tc.pv.Name}, &corev1.PersistentVolume{})
			gone := apierrors.IsNotFound(err)
			if gone != tc.wantGone {
				t.Fatalf("deleted = %v, want %v", gone, tc.wantGone)
			}
		})
	}
}
