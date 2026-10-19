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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/sds-common-lib/logger"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// TestPendingClaimsAreNamedWithTheirReason: a plane whose claims nothing
// provisions says so — how many, on which class, for how long and what
// Kubernetes recorded — instead of only "0/3 pods ready"; claims that are
// young or bound are not called out.
func TestPendingClaimsAreNamedWithTheirReason(t *testing.T) {
	const ns = "d8-system-s3"
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	plane := &backend.Store{Name: backend.StoreName, Incarnation: 3, HighAvailability: true}
	broken := "e2e-broken"
	claim := func(ord string, phase corev1.PersistentVolumeClaimPhase, age time.Duration) client.Object {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data-" + resourceName(plane) + "-" + ord, Namespace: ns,
				CreationTimestamp: metav1.NewTime(now.Add(-age)),
			},
			Spec:   corev1.PersistentVolumeClaimSpec{StorageClassName: &broken},
			Status: corev1.PersistentVolumeClaimStatus{Phase: phase},
		}
	}
	event := func(name, claimName, msg string, at time.Duration) client.Object {
		return &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: ns},
			InvolvedObject: corev1.ObjectReference{Kind: "PersistentVolumeClaim", Name: claimName, Namespace: ns},
			Message:        msg,
			LastTimestamp:  metav1.NewTime(now.Add(-at)),
		}
	}
	first := "data-" + resourceName(plane) + "-0"
	newDriver := func(objs ...client.Object) *Driver {
		c := fake.NewClientBuilder().WithScheme(rfScheme(t)).WithObjects(objs...).Build()
		return &Driver{client: c, apiReader: c, namespace: ns, log: logger.NewNop()}
	}

	d := newDriver(
		claim("0", corev1.ClaimPending, 5*time.Minute),
		claim("1", corev1.ClaimPending, 5*time.Minute),
		claim("2", corev1.ClaimBound, 5*time.Minute),
		event("old", first, `storageclass.storage.k8s.io "e2e-broken" not found`, 4*time.Minute),
		event("new", first, `waiting for a volume to be created either by the external provisioner "e2e.invalid/none" or manually`, time.Minute),
		event("other", "something-else", "unrelated", 0),
	)
	want := `2 of 3 volume claims are Pending on StorageClass e2e-broken for 5m0s: ` +
		`waiting for a volume to be created either by the external provisioner "e2e.invalid/none" or manually`
	if got := d.pendingClaimsNote(context.Background(), plane, now); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	young := newDriver(claim("0", corev1.ClaimPending, 20*time.Second))
	if got := young.pendingClaimsNote(context.Background(), plane, now); got != "" {
		t.Errorf("a claim provisioning normally was called out: %q", got)
	}
	none := newDriver()
	if got := none.pendingClaimsNote(context.Background(), plane, now); got != "" {
		t.Errorf("no claims, yet: %q", got)
	}
}
