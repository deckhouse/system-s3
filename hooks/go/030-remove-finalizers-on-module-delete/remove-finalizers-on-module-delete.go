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

package hooks_common

import (
	"context"
	"errors"
	"fmt"

	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/registry"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/kubeclient"
	"github.com/deckhouse/system-s3/hooks/go/consts"
)

const hookName = "remove-finalizers-on-module-delete"

func removeFinalizersFromObject(ctx context.Context, cl client.Client, obj client.Object, logger pkg.Logger) error {
	logger.Info(fmt.Sprintf("[%s]: Removing finalizers from %s %s", hookName, obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName()))

	patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
	obj.SetFinalizers(nil)

	if err := cl.Patch(ctx, obj, patch); err != nil {
		return fmt.Errorf("failed to patch %s %s: %w", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), err)
	}
	return nil
}

var _ = registry.RegisterFunc(configRemoveFinalizersOnModuleDelete, handlerRemoveFinalizersOnModuleDelete)

var configRemoveFinalizersOnModuleDelete = &pkg.HookConfig{
	OnAfterDeleteHelm: &pkg.OrderedConfig{Order: 10},
}

func handlerRemoveFinalizersOnModuleDelete(ctx context.Context, input *pkg.HookInput) error {
	input.Logger.Info(fmt.Sprintf("[%s]: Started removing finalizers on module delete", hookName))
	var resultErr error

	cl, err := kubeclient.New(
		admissionregistrationv1.AddToScheme,
		clientgoscheme.AddToScheme,
		extv1.AddToScheme,
		corev1.AddToScheme,
		storagev1.AddToScheme,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize kube client: %w", err)
	}

	secretList := &corev1.SecretList{}
	if err := cl.List(ctx, secretList, client.InNamespace(consts.ModuleNamespace)); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: failed to list secrets: %w", hookName, err))
	} else {
		for i := range secretList.Items {
			if len(secretList.Items[i].Finalizers) == 0 {
				continue
			}
			if err := removeFinalizersFromObject(ctx, cl, &secretList.Items[i], input.Logger); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}

	configMapList := &corev1.ConfigMapList{}
	if err := cl.List(ctx, configMapList, client.InNamespace(consts.ModuleNamespace)); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: failed to list configmaps: %w", hookName, err))
	} else {
		for i := range configMapList.Items {
			if len(configMapList.Items[i].Finalizers) == 0 {
				continue
			}
			if err := removeFinalizersFromObject(ctx, cl, &configMapList.Items[i], input.Logger); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}

	for _, name := range consts.WebhookConfigurationsToDelete {
		vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, vwc); err == nil {
			input.Logger.Info(fmt.Sprintf("[%s]: Deleting ValidatingWebhookConfiguration %s", hookName, name))
			if err := cl.Delete(ctx, vwc); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: failed to delete ValidatingWebhookConfiguration %s: %w", hookName, name, err))
			}
		}
	}

	for _, provisioner := range consts.AllowedProvisioners {
		scList := &storagev1.StorageClassList{}
		if err := cl.List(ctx, scList); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: failed to list storage classes: %w", hookName, err))
			break
		}
		for i := range scList.Items {
			if scList.Items[i].Provisioner != provisioner {
				continue
			}
			if len(scList.Items[i].Finalizers) == 0 {
				continue
			}
			if err := removeFinalizersFromObject(ctx, cl, &scList.Items[i], input.Logger); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}

	for _, crgvk := range consts.CRGVKsForFinalizerRemoval {
		// List cluster-wide (no namespace scoping): the module's namespaced CRs
		// (e.g. BucketAccess) live in the consuming applications'
		// namespaces, not in the module namespace, so restricting to
		// ModuleNamespace would leave their finalizers set and the objects stuck
		// in Terminating forever after the module (and its controller) is gone.
		crList := &unstructured.UnstructuredList{}
		crList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   crgvk.Group,
			Version: crgvk.Version,
			Kind:    crgvk.Kind + "List",
		})
		if err := cl.List(ctx, crList); err != nil {
			input.Logger.Info(fmt.Sprintf("[%s]: skipping CR %s (may not exist): %v", hookName, crgvk.Kind, err))
			continue
		}
		for i := range crList.Items {
			cr := &crList.Items[i]
			if len(cr.GetFinalizers()) == 0 {
				continue
			}
			if err := removeFinalizersFromObject(ctx, cl, cr, input.Logger); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}

	// The local PV pool is the one thing the module leaves behind, and it is the
	// one thing that makes re-enabling it unsafe.
	//
	// These PersistentVolumes are cluster-scoped, so deleting the namespace does
	// not take them; Helm does not own them, because the controller creates them
	// rather than the chart; and the store has no custom resource for them to be
	// owned by. Their only collector lives in the controller, which is gone by the
	// time this runs. So without this step they stay in the cluster forever,
	// Available or Released, pointing at a StorageClass that no longer exists —
	// and re-enabling the module starts at incarnation 1, binds the same PV names,
	// and brings the new data plane up on the previous installation's data. That
	// is exactly what the incarnation mechanism exists to prevent.
	pvList := &corev1.PersistentVolumeList{}
	if err := cl.List(ctx, pvList,
		client.HasLabels{consts.SystemLocalNodeLabel},
		client.HasLabels{consts.SystemStoreLabel},
	); err != nil {
		input.Logger.Info(fmt.Sprintf("[%s]: skipping the local PV pool: %v", hookName, err))
	} else {
		for i := range pvList.Items {
			pv := &pvList.Items[i]
			if len(pv.Finalizers) > 0 {
				if err := removeFinalizersFromObject(ctx, cl, pv, input.Logger); err != nil {
					resultErr = errors.Join(resultErr, err)
					continue
				}
			}
			if err := cl.Delete(ctx, pv); err != nil && !apierrors.IsNotFound(err) {
				resultErr = errors.Join(resultErr, fmt.Errorf("delete local pv %s: %w", pv.Name, err))
				continue
			}
			input.Logger.Info(fmt.Sprintf("[%s]: deleted local pool PV %s", hookName, pv.Name))
		}
	}

	input.Logger.Info(fmt.Sprintf("[%s]: Finished removing finalizers on module delete", hookName))
	return resultErr
}
