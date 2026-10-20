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

package releaseheritage

import (
	"context"
	"fmt"

	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/registry"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/deckhouse/sds-common-lib/kubeclient"
)

const restoreHookName = "restore-heritage-on-module-enable"

var _ = registry.RegisterFunc(restoreConfig, handleRestore)

// Before every Helm run rather than on startup: it is the one point every
// enabling of the module is sure to pass, and on a module that was never
// disabled nothing carries the mark and this is one read by label per kind.
//
// Helm may well put heritage back on the namespace and the CA Secret on its own
// when it adopts them again, but whether it does depends on how it computes the
// patch for an object it did not create in this release; and anything else the
// mark names is not rendered by Helm at all. The mark is the record of which
// objects had the label, so it is given back from the mark.
var restoreConfig = &pkg.HookConfig{
	OnBeforeHelm: &pkg.OrderedConfig{Order: 2},
}

func handleRestore(ctx context.Context, input *pkg.HookInput) error {
	cl, err := kubeclient.New(corev1.AddToScheme, appsv1.AddToScheme)
	if err != nil {
		return fmt.Errorf("failed to initialize kube client: %w", err)
	}
	return restoreHeritage(ctx, cl, input.Logger, restoreHookName)
}
