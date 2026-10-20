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

// Package releaseheritage hands what a disabled module leaves behind to the
// cluster's administrators, and takes it back when the module is enabled again.
//
// The module namespace and the objects in it outlive the release (see
// templates/namespace.yaml and 050-stop-data-plane-on-module-delete), and the
// documented way to delete the data is to delete the namespace once the module
// is disabled. But the namespace and the module CA carry heritage: deckhouse
// from helm_lib_module_labels, and Deckhouse's admission policy
// label-objects.deckhouse.io lets nobody but Deckhouse itself and a short list of
// system users change or delete an object with that label. So the step the
// documentation describes was refused to the very administrator it was written
// for, and the only way through was to act as system:sudouser.
//
// Disabling the module therefore takes the label off: from the namespace and
// from every object in it that has it, each marked with releasedLabel so that
// enabling the module puts the label back on exactly those and on nothing a user
// created meanwhile. The marker is a label rather than an annotation so that the
// way back is a label selector and not a read of every object in the namespace
// on every Helm run.
//
// The namespace keeps every other label, the Prometheus watchers' among them:
// only heritage is touched. Without heritage the namespace is still not turned
// into a Project by multitenancy-manager, which leaves every d8-* namespace alone
// by its name.
package releaseheritage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/deckhouse/module-sdk/pkg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/system-s3/hooks/go/consts"
)

const (
	// heritageLabel is what label-objects.deckhouse.io selects the objects it
	// guards by, and heritageDeckhouse the value it guards.
	heritageLabel     = "heritage"
	heritageDeckhouse = "deckhouse"

	// releasedLabel marks an object the module took heritage off when it was
	// disabled. Its value is the heritage the object had, which is what is put
	// back.
	releasedLabel = "system-s3.deckhouse.io/released-heritage"
)

// namespacedKinds are the kinds of the objects a disabled module leaves in its
// namespace: the CA Secret the chart keeps, and the data plane the controller
// created — the StatefulSets, their claims, the Secrets and ConfigMaps that make
// the volumes a cluster again, its Services and its ServiceAccount.
var namespacedKinds = []schema.GroupVersionKind{
	{Version: "v1", Kind: "Secret"},
	{Version: "v1", Kind: "ConfigMap"},
	{Version: "v1", Kind: "PersistentVolumeClaim"},
	{Version: "v1", Kind: "Service"},
	{Version: "v1", Kind: "ServiceAccount"},
	{Group: "apps", Version: "v1", Kind: "StatefulSet"},
}

var namespaceKind = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}

// labelsPatch is a JSON merge patch of metadata.labels; a nil value removes the
// label.
func labelsPatch(labels map[string]interface{}) client.Patch {
	raw, _ := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"labels": labels}})
	return client.RawPatch(types.MergePatchType, raw)
}

// objectRef is one object to patch: its kind, namespace and name, and the
// labels it has.
type objectRef struct {
	gvk       schema.GroupVersionKind
	namespace string
	name      string
	labels    map[string]string
}

func (o objectRef) String() string {
	if o.namespace == "" {
		return fmt.Sprintf("%s %s", o.gvk.Kind, o.name)
	}
	return fmt.Sprintf("%s %s/%s", o.gvk.Kind, o.namespace, o.name)
}

func (o objectRef) patch(ctx context.Context, cl client.Client, p client.Patch) error {
	obj := &metav1.PartialObjectMetadata{}
	obj.SetGroupVersionKind(o.gvk)
	obj.SetNamespace(o.namespace)
	obj.SetName(o.name)
	return client.IgnoreNotFound(cl.Patch(ctx, obj, p))
}

// selectObjects lists the module namespace and the objects in it whose labels
// match the selector. Only the metadata is read: a Secret's data has no part in
// this, and a hook that reads none cannot leak any.
func selectObjects(ctx context.Context, cl client.Client, selector client.MatchingLabels) ([]objectRef, error) {
	var refs []objectRef

	ns := &metav1.PartialObjectMetadata{}
	ns.SetGroupVersionKind(namespaceKind)
	err := cl.Get(ctx, client.ObjectKey{Name: consts.ModuleNamespace}, ns)
	switch {
	case apierrors.IsNotFound(err):
		// Deleted by the administrator: nothing in it either.
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read the namespace %s: %w", consts.ModuleNamespace, err)
	}
	if matches(ns.Labels, selector) {
		refs = append(refs, objectRef{gvk: namespaceKind, name: ns.Name, labels: ns.Labels})
	}

	var resultErr error
	for _, gvk := range namespacedKinds {
		list := &metav1.PartialObjectMetadataList{}
		list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		if err := cl.List(ctx, list, client.InNamespace(consts.ModuleNamespace), selector); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("list %s in %s: %w", gvk.Kind, consts.ModuleNamespace, err))
			continue
		}
		for i := range list.Items {
			item := &list.Items[i]
			refs = append(refs, objectRef{gvk: gvk, namespace: item.Namespace, name: item.Name, labels: item.Labels})
		}
	}
	return refs, resultErr
}

func matches(labels map[string]string, selector client.MatchingLabels) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// releaseHeritage takes heritage: deckhouse off the module namespace and every
// object in it that has it, and marks each with releasedLabel. Running it again
// finds nothing left to release.
func releaseHeritage(ctx context.Context, cl client.Client, logger pkg.Logger, hook string) error {
	refs, resultErr := selectObjects(ctx, cl, client.MatchingLabels{heritageLabel: heritageDeckhouse})
	for _, ref := range refs {
		logger.Info(fmt.Sprintf("[%s]: removing %s=%s from %s, so that it can be deleted without the module",
			hook, heritageLabel, heritageDeckhouse, ref))
		p := labelsPatch(map[string]interface{}{heritageLabel: nil, releasedLabel: ref.labels[heritageLabel]})
		if err := ref.patch(ctx, cl, p); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: release %s: %w", hook, ref, err))
		}
	}
	return resultErr
}

// restoreHeritage puts heritage back on every object releaseHeritage marked, as
// it was, and removes the mark. Objects without the mark — created by somebody
// while the module was away, or released by nobody — are left alone. Running it
// again finds nothing marked.
func restoreHeritage(ctx context.Context, cl client.Client, logger pkg.Logger, hook string) error {
	refs, resultErr := selectObjects(ctx, cl, client.MatchingLabels{releasedLabel: heritageDeckhouse})
	for _, ref := range refs {
		logger.Info(fmt.Sprintf("[%s]: giving %s=%s back to %s", hook, heritageLabel, heritageDeckhouse, ref))
		p := labelsPatch(map[string]interface{}{heritageLabel: ref.labels[releasedLabel], releasedLabel: nil})
		if err := ref.patch(ctx, cl, p); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("[%s]: restore %s: %w", hook, ref, err))
		}
	}
	return resultErr
}
