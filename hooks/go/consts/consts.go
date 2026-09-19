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
	ModuleName       string = "systemS3"
	ModuleNamespace  string = "d8-system-s3"
	ModulePluralName string = "system-s3"
	// WebhookCertCn is the CN used for the self-signed TLS material that
	// the webhooks container consumes. It must match the Service name
	// (helm_lib_module_webhook_service defaults to "webhooks").
	WebhookCertCn string = "webhooks"

	// STSServiceName is the Service in front of the credentials endpoint (the
	// controller's STS listener). The same certificate covers it — it is issued
	// for names in this namespace and the endpoint needs no PKI of its own — which
	// is why the name is here, next to the CN, rather than in the chart alone:
	// the certificate's SANs have to include it or every client fails TLS
	// verification against a name the certificate never claimed.
	STSServiceName string = "system-s3-sts"
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
