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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// pendingClaimGrace is how long a claim may be Pending before the conditions
// call it out. Provisioning a volume takes seconds on a working class; a claim
// Pending past this is waiting for something that is not coming by itself.
const pendingClaimGrace = time.Minute

// pendingClaimsNote says why a plane's pods cannot start when the reason is its
// volumes: how many of its claims are Pending, on which StorageClass, since
// when, and what Kubernetes last said about them. It is "" when no claim has
// been Pending past pendingClaimGrace.
//
// A plane whose class cannot provision — a provisioner that is not installed, a
// class that does not exist — used to be reported as "0/3 pods ready", which is
// true and says nothing about where to look. The reason is on the claims, in
// the events the provisioning controller records on them ("waiting for a volume
// to be created ... by external provisioner X", "storageclass ... not found"),
// and the controller reads those rather than the StorageClass itself: it has no
// business reading cluster-wide storage configuration to report on its own
// claims.
func (d *Driver) pendingClaimsNote(ctx context.Context, plane *backend.Store, now time.Time) string {
	replicas := desiredReplicas(plane)
	var pending []*corev1.PersistentVolumeClaim
	for ord := int32(0); ord < replicas; ord++ {
		pvc := &corev1.PersistentVolumeClaim{}
		name := fmt.Sprintf("data-%s-%d", resourceName(plane), ord)
		if err := d.apiReader.Get(ctx, client.ObjectKey{Namespace: d.namespace, Name: name}, pvc); err != nil {
			if !apierrors.IsNotFound(err) {
				d.log.Debug("unable to read a claim of the plane", "claim", name, "error", err.Error())
			}
			continue
		}
		if pvc.Status.Phase == corev1.ClaimPending && now.Sub(pvc.CreationTimestamp.Time) >= pendingClaimGrace {
			pending = append(pending, pvc)
		}
	}
	if len(pending) == 0 {
		return ""
	}

	first := pending[0]
	class := "the default StorageClass"
	if first.Spec.StorageClassName != nil && *first.Spec.StorageClassName != "" {
		class = "StorageClass " + *first.Spec.StorageClassName
	}
	note := fmt.Sprintf("%d of %d volume claims are Pending on %s for %s",
		len(pending), replicas, class, now.Sub(first.CreationTimestamp.Time).Round(time.Minute))
	if why := d.lastClaimEvent(ctx, first.Name); why != "" {
		note += ": " + why
	}
	return note
}

// lastClaimEvent is the message of the latest event recorded on a claim in the
// module namespace, or "" when there is none or it cannot be read.
func (d *Driver) lastClaimEvent(ctx context.Context, claim string) string {
	events := &corev1.EventList{}
	if err := d.apiReader.List(ctx, events, client.InNamespace(d.namespace)); err != nil {
		d.log.Debug("unable to read the events of a claim", "claim", claim, "error", err.Error())
		return ""
	}
	var latest *corev1.Event
	var latestAt time.Time
	for i := range events.Items {
		e := &events.Items[i]
		if e.InvolvedObject.Kind != "PersistentVolumeClaim" || e.InvolvedObject.Name != claim {
			continue
		}
		at := eventTime(e)
		if latest == nil || at.After(latestAt) {
			latest, latestAt = e, at
		}
	}
	if latest == nil {
		return ""
	}
	return latest.Message
}

// eventTime is when an event last happened, whichever of its timestamps the
// recorder filled in.
func eventTime(e *corev1.Event) time.Time {
	switch {
	case !e.LastTimestamp.IsZero():
		return e.LastTimestamp.Time
	case !e.EventTime.IsZero():
		return e.EventTime.Time
	default:
		return e.CreationTimestamp.Time
	}
}
