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
	"github.com/deckhouse/system-s3/hooks/go/consts"
)

const releaseHookName = "release-heritage-on-module-delete"

var _ = registry.RegisterFunc(releaseConfig, handleRelease)

// After the data plane is stopped (050-stop-data-plane-on-module-delete, order
// 20): an administrator who deletes the namespace the moment the label is gone
// deletes a stopped store, not a running one.
var releaseConfig = &pkg.HookConfig{
	OnAfterDeleteHelm: &pkg.OrderedConfig{Order: 30},
}

func handleRelease(ctx context.Context, input *pkg.HookInput) error {
	cl, err := kubeclient.New(corev1.AddToScheme, appsv1.AddToScheme)
	if err != nil {
		return fmt.Errorf("failed to initialize kube client: %w", err)
	}
	if err := releaseHeritage(ctx, cl, input.Logger, releaseHookName); err != nil {
		return err
	}
	input.Logger.Info(fmt.Sprintf("[%s]: what the module left in %s can be deleted by a cluster administrator", releaseHookName, consts.ModuleNamespace))
	return nil
}
