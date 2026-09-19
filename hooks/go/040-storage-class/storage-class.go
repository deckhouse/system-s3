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

// Package hooks_storageclass resolves the StorageClass the data plane orders its
// volumes from, and writes it to systemS3.internal.effectiveStorageClass for the
// chart to hand to the controller.
//
// There is a common hook for this in module-sdk (common-hooks/storage-class-change),
// and the resolution order here is deliberately the same one it implements, so an
// operator who knows how upmeter or Prometheus picks a class knows how this module
// picks one. What is NOT taken from it is everything it does with the answer: on a
// change of the class it deletes the workload's PersistentVolumeClaims and the
// workload itself, which for a module whose volumes hold the platform's own
// objects is data loss on an edited setting. Here the hook only reports the
// desired class; the controller compares it with what the running store is
// actually bound to and says so when they disagree, exactly as it does for the
// high-availability mode.
package hooks_storageclass

import (
	"context"
	"fmt"

	"github.com/deckhouse/module-sdk/pkg"
	objectpatch "github.com/deckhouse/module-sdk/pkg/object-patch"
	"github.com/deckhouse/module-sdk/pkg/registry"

	"github.com/deckhouse/system-s3/hooks/go/consts"
)

const hookName = "storage-class"

// snapshotStorageClasses is the name of the StorageClass snapshot; only the
// default-class annotations are read out of it.
const snapshotStorageClasses = "storageClasses"

// valuesPathEffectiveStorageClass is where the answer lands for the chart.
const valuesPathEffectiveStorageClass = consts.ModuleName + ".internal.effectiveStorageClass"

// configPathStorageClass is the module's own setting, and
// configPathGlobalStorageClass the platform-wide one it falls back to.
const (
	configPathStorageClass       = consts.ModuleName + ".storageClass"
	configPathGlobalStorageClass = "global.modules.storageClass"
)

// storageClassFilter keeps the two things the decision needs: the name, and
// whether the class is the cluster default (in either spelling of the
// annotation — the beta one is still what some provisioners write).
var storageClassFilter = `{
	"name": .metadata.name,
	"isDefault": (
		(.metadata.annotations["storageclass.kubernetes.io/is-default-class"] == "true")
		or
		(.metadata.annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true")
	)
}`

type filteredStorageClass struct {
	Name      string `json:"name"`
	IsDefault bool   `json:"isDefault"`
}

var _ = registry.RegisterFunc(configStorageClass, handlerStorageClass)

var configStorageClass = &pkg.HookConfig{
	// Before Helm, because the chart reads the value this writes.
	OnBeforeHelm: &pkg.OrderedConfig{Order: 1},
	Kubernetes: []pkg.KubernetesConfig{
		{
			Name:       snapshotStorageClasses,
			APIVersion: "storage.k8s.io/v1",
			Kind:       "StorageClass",
			JqFilter:   storageClassFilter,
		},
	},
}

func handlerStorageClass(_ context.Context, input *pkg.HookInput) error {
	classes, err := objectpatch.UnmarshalToStruct[filteredStorageClass](input.Snapshots, snapshotStorageClasses)
	if err != nil {
		return fmt.Errorf("[%s]: unmarshal the StorageClass snapshot: %w", hookName, err)
	}

	effective := resolveStorageClass(classes, configValue(input, configPathGlobalStorageClass), configValue(input, configPathStorageClass))
	input.Values.Set(valuesPathEffectiveStorageClass, effective)

	if effective == "" {
		// Said once per convergence, and it is the only place it can be said before
		// the controller exists: a fresh installation with no StorageClass at all
		// has no SystemBucket yet to carry the condition either.
		input.Logger.Warn(fmt.Sprintf(
			"[%s]: no StorageClass resolved (neither %s, nor %s, nor a default class in the cluster): "+
				"the system object storage cannot be brought up until one is set",
			hookName, configPathStorageClass, configPathGlobalStorageClass))
	}

	return nil
}

// configValue reads a configuration value that may be absent. A pointer, not a
// string, because "set to an empty string" and "not set" are different answers
// here: the first is an operator saying something the schema does not allow and
// the second is the common case.
func configValue(input *pkg.HookInput, path string) *string {
	if !input.ConfigValues.Exists(path) {
		return nil
	}
	v := input.ConfigValues.Get(path).String()
	return &v
}

// resolveStorageClass resolves the class by the platform's usual order, most
// specific last: the cluster default, then the platform-wide setting, then the
// module's own.
//
// Note what is NOT in the chain: the class the running store's volumes are
// already bound to. The common hook puts it in — and above the platform-wide
// setting — which makes the answer "whatever is running" and quietly ignores an
// edit until something recreates the claims. Here the running class is not an
// input at all: this is the desired state, the controller knows the actual one,
// and a disagreement between them is a thing to report rather than to hide.
func resolveStorageClass(classes []filteredStorageClass, globalClass, moduleClass *string) string {
	effective := ""

	for _, sc := range classes {
		if sc.IsDefault {
			effective = sc.Name
			break
		}
	}

	if globalClass != nil {
		effective = *globalClass
	}

	if moduleClass != nil {
		effective = *moduleClass
	}

	return effective
}
