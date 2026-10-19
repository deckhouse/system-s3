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

// Package stopdataplane stops the Garage data plane when the module is disabled,
// and leaves its data where it is.
//
// The module namespace outlives the release (helm.sh/resource-policy: keep in
// templates/namespace.yaml): disabling the module must not delete every
// platform component's system bucket, and deleting the data is an explicit step
// of its own. What the chart rendered goes with the release; the data plane is
// the controller's and stays — the StatefulSets, their claims, the Secrets that
// make the volumes a cluster again.
//
// It stays stopped. A Garage cluster nobody manages keeps serving with nobody to
// renew its certificate, expire its keys or say that a replica is gone, and the
// module reports nothing once it is disabled. So the StatefulSets are scaled to
// zero, which keeps their claims, and the copier Jobs of a migration that was
// under way are deleted, so that nothing writes into the volumes while the
// module is away. Enabling the module again scales the StatefulSets back — the
// controller applies them with their replica count on its first pass — and a
// migration resumes from the state it saved.
package stopdataplane

import (
	"context"
	"errors"
	"fmt"

	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/registry"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/kubeclient"

	"github.com/deckhouse/system-s3/hooks/go/consts"
)

const hookName = "stop-data-plane-on-module-delete"

// dataPlaneLabels select the Garage workloads the controller creates: every
// StatefulSet of every plane, and the copier Jobs of a migration. They are the
// controller's commonLabels; the controller lives in another Go module, so they
// are spelled out here.
var dataPlaneLabels = client.MatchingLabels{
	"app.kubernetes.io/managed-by": "system-s3",
	"app.kubernetes.io/name":       "garage",
}

var _ = registry.RegisterFunc(config, handle)

var config = &pkg.HookConfig{
	OnAfterDeleteHelm: &pkg.OrderedConfig{Order: 20},
}

func handle(ctx context.Context, input *pkg.HookInput) error {
	cl, err := kubeclient.New(appsv1.AddToScheme, batchv1.AddToScheme)
	if err != nil {
		return fmt.Errorf("failed to initialize kube client: %w", err)
	}
	return stopDataPlane(ctx, cl, input.Logger)
}

// stopDataPlane scales the data plane's StatefulSets to zero and deletes its
// copier Jobs, in the module namespace.
func stopDataPlane(ctx context.Context, cl client.Client, logger pkg.Logger) error {
	var resultErr error

	sets := &appsv1.StatefulSetList{}
	if err := cl.List(ctx, sets, client.InNamespace(consts.ModuleNamespace), dataPlaneLabels); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: list the Garage StatefulSets: %w", hookName, err))
	}
	for i := range sets.Items {
		sts := &sets.Items[i]
		if sts.Spec.Replicas != nil && *sts.Spec.Replicas == 0 {
			continue
		}
		logger.Info(fmt.Sprintf("[%s]: stopping StatefulSet %s; its claims and data are kept", hookName, sts.Name))
		patch := client.MergeFrom(sts.DeepCopy())
		zero := int32(0)
		sts.Spec.Replicas = &zero
		if err := cl.Patch(ctx, sts, patch); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: scale StatefulSet %s to zero: %w", hookName, sts.Name, err))
		}
	}

	jobs := &batchv1.JobList{}
	if err := cl.List(ctx, jobs, client.InNamespace(consts.ModuleNamespace), dataPlaneLabels); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: list the copier Jobs: %w", hookName, err))
	}
	background := metav1.DeletePropagationBackground
	for i := range jobs.Items {
		job := &jobs.Items[i]
		logger.Info(fmt.Sprintf("[%s]: deleting copier Job %s; the migration resumes when the module is enabled", hookName, job.Name))
		if err := cl.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &background}); client.IgnoreNotFound(err) != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: delete Job %s: %w", hookName, job.Name, err))
		}
	}
	return resultErr
}
