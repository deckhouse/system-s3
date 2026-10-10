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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/system-s3/images/controller/internal/migrator"
)

func TestCopyQuotaOutcomeRequiresAnOwnedCopierPod(t *testing.T) {
	for _, tc := range []struct {
		name, pass, container string
		uid                   types.UID
		exit                  int32
		want                  copyOutcome
	}{
		{"quota", copyPassBulk, "copier", "job", migrator.QuotaExitCode, copyQuotaBlocked},
		{"ordinary failure", copyPassBulk, "copier", "job", 1, copyFailed},
		{"old job pod", copyPassBulk, "copier", "old", migrator.QuotaExitCode, copyFailed},
		{"wrong container", copyPassBulk, "other", "job", migrator.QuotaExitCode, copyFailed},
		{"final pass must retry", copyPassDelta, "copier", "job", migrator.QuotaExitCode, copyFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			source := planeOf(systemStore(), 1, true, localSC)
			target := planeOf(systemStore(), 2, true, localSC)
			buckets := []string{"bucket"}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: copyJobName(source, 2, tc.pass), Namespace: "d8-system-s3", UID: "job", Annotations: map[string]string{annCopyBuckets: bucketsStamp(buckets)}},
				Spec:   batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "copier"}}}}},
				Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}
			configureCopyPass(job, tc.pass)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "copy-pod", Namespace: job.Namespace, Labels: map[string]string{"job-name": job.Name},
				OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: tc.uid, Controller: ptr.To(true)}}},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: tc.container, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: tc.exit}}}}}}
			c := fake.NewClientBuilder().WithScheme(coreScheme(t)).WithObjects(job, pod).Build()
			d := testDriver(t, c, job.Namespace)
			got, _, err := d.runCopyPass(ctx, source, target, tc.pass, buckets)
			if err != nil || got != tc.want {
				t.Fatalf("outcome=%v want=%v err=%v", got, tc.want, err)
			}
			err = c.Get(ctx, client.ObjectKeyFromObject(job), &batchv1.Job{})
			if tc.want == copyQuotaBlocked && err != nil {
				t.Fatalf("quota evidence deleted before phase persistence: %v", err)
			}
			if tc.want == copyFailed && !apierrors.IsNotFound(err) {
				t.Fatalf("failed job not removed: %v", err)
			}
		})
	}
}

func TestQuotaBlockedBulkPersistsBoundedCutover(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(coreScheme(t)).Build()
	d := testDriver(t, c, "d8-system-s3")
	store := planeOf(systemStore(), 1, true, localSC)
	state := migrationState{serving: 1, other: 2, phase: phaseCopying}
	before := time.Now()
	next, _, err := d.recordCopyOutcome(ctx, store, state, copyQuotaBlocked, "quota")
	if err != nil || next.phase != phaseCutover || next.frozenAt.Before(before) {
		t.Fatalf("quota did not enter bounded cutover: %+v %v", next, err)
	}
	loaded, err := d.loadMigrationState(ctx, store)
	if err != nil || loaded.phase != phaseCutover || loaded.frozenAt.IsZero() {
		t.Fatalf("cutover was not persisted: %+v %v", loaded, err)
	}
	stuck, _, err := d.recordCopyOutcome(ctx, store, state, copyFailed, "authentication failed")
	if err != nil || stuck.phase != phaseCopying {
		t.Fatalf("ordinary error triggered cutover: %+v %v", stuck, err)
	}
}
