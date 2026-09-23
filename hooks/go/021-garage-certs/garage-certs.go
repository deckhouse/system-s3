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
	"fmt"

	tlscertificate "github.com/deckhouse/module-sdk/common-hooks/tls-certificate"

	consts "github.com/deckhouse/system-s3/hooks/go/consts"
)

// The certificate the data plane's TLS terminator serves.
//
// Garage has no TLS of its own; a sidecar in every Garage pod terminates it (see
// internal/tlsproxy in the controller). This is its certificate.
//
// Same CA as the webhook and credentials-endpoint certificate — the shared
// CommonCAValuesPath — so a consumer is handed one CA and it verifies both
// endpoints it talks to. A different certificate and key, though: the Garage
// pods hold this key, and a data-plane pod that is compromised must not be able
// to impersonate the endpoint that issues credentials.
//
// The names cover the endpoint Service by name, and every Service in the
// namespace by wildcard. The wildcard is not laziness: each Garage cluster has
// Services of its own named after its incarnation (system-garage-api,
// system-garage-i2-api, ...), which the controller and the migration Job dial,
// and incarnations are created at run time, long after this certificate.
// Clients that reach a node by its pod IP verify the endpoint Service's name
// explicitly, since no certificate can name addresses assigned at scheduling.
var _ = tlscertificate.RegisterInternalTLSHookEM(tlscertificate.GenSelfSignedTLSHookConf{
	CommonCAValuesPath:    fmt.Sprintf("%s.internal.customWebhookCA", consts.ModuleName),
	CommonCACanonicalName: fmt.Sprintf("%s-%s", consts.ModulePluralName, consts.WebhookCertCn),
	CN:                    consts.GarageServiceName,
	TLSSecretName:         consts.GarageTLSSecret,
	Namespace:             consts.ModuleNamespace,
	SANs: tlscertificate.DefaultSANs([]string{
		consts.GarageServiceName,
		fmt.Sprintf("%s.%s", consts.GarageServiceName, consts.ModuleNamespace),
		fmt.Sprintf("%s.%s.svc", consts.GarageServiceName, consts.ModuleNamespace),
		fmt.Sprintf("%%CLUSTER_DOMAIN%%://%s.%s.svc", consts.GarageServiceName, consts.ModuleNamespace),
		fmt.Sprintf("*.%s.svc", consts.ModuleNamespace),
		fmt.Sprintf("%%CLUSTER_DOMAIN%%://*.%s.svc", consts.ModuleNamespace),
	}),
	FullValuesPathPrefix: fmt.Sprintf("%s.internal.garageCert", consts.ModuleName),
})
