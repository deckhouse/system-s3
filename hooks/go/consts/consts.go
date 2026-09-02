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

package consts

const (
	// SystemStoreLabel and SystemLocalNodeLabel are on every local PersistentVolume
	// the controller provisions for the data plane.
	//
	// The module-delete hook needs them because that pool is the one thing the
	// module leaves behind: PersistentVolumes are cluster-scoped, so deleting the
	// namespace does not take them, Helm does not own them (the controller creates
	// them, not the chart), and the store has no custom resource for them to be
	// owned by. The only collector lives in the controller, which is gone by then.
	SystemStoreLabel     string = "storage.deckhouse.io/system-store"
	SystemLocalNodeLabel string = "storage.deckhouse.io/system-local-node"

	ModuleName       string = "systemS3"
	ModuleNamespace  string = "d8-system-s3"
	ModulePluralName string = "system-s3"
	// WebhookCertCn is the CN used for the self-signed TLS material that
	// the webhooks container consumes. It must match the Service name
	// (helm_lib_module_webhook_service defaults to "webhooks").
	WebhookCertCn string = "webhooks"
)

// ModuleFinalizerPrefix is what a finalizer this module sets looks like (the
// controller's is ModuleFinalizerPrefix + "-controller"). The module-delete hook
// removes only finalizers matching it, so a foreign finalizer on one of the same
// objects — set by a backup operator or a policy controller — is left where it is.
const ModuleFinalizerPrefix = "storage.deckhouse.io/system-s3"

// WebhookConfigurationsToDelete lists ValidatingWebhookConfigurations the
// module-delete hook removes on uninstall.
var WebhookConfigurationsToDelete = []string{
	"d8-system-s3-systembucket-validation",
}

// CRGVKsForFinalizerRemoval lists CRs the module creates and which may carry a
// controller-managed finalizer, stripped on module delete.
var CRGVKsForFinalizerRemoval = []CRGVK{
	{Group: "storage.deckhouse.io", Version: "v1alpha1", Kind: "SystemBucket", Namespaced: true},
}

type CRGVK struct {
	Group      string
	Version    string
	Kind       string
	Namespaced bool
}
