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
	"github.com/deckhouse/system-s3/hooks/go/persistedca"
)

// The CA is kept in values of its own, and that is not a detail.
//
// Without CommonCAValuesPath the SDK generates the CA with the CERTIFICATE's
// common name, so the CA and the certificate it signs both end up as
// "CN=webhooks". Go's verifier tolerates that — it matches an issuer by name and
// key id — and the API server's client for the admission webhook this
// certificate was first issued for is a Go verifier, so the webhook worked and
// nothing said otherwise. OpenSSL does not: it sees a certificate
// whose issuer name equals its own subject name, concludes it is self-issued,
// and stops at depth 0 with "self-signed certificate". Every consumer that
// verifies the credentials endpoint with curl, Python or Java — which is every
// consumer not written in Go — cannot reach it.
//
// With the path set, the CA is generated once under CommonCACanonicalName,
// distinct from the certificate's CN, and the chain verifies everywhere.
// Registered through persistedca so that the common CA survives a restart of
// Deckhouse (see the package).
var _ = persistedca.Register(tlscertificate.GenSelfSignedTLSHookConf{
	CommonCAValuesPath:    fmt.Sprintf("%s.internal.customWebhookCA", consts.ModuleName),
	CommonCACanonicalName: fmt.Sprintf("%s-%s", consts.ModulePluralName, consts.WebhookCertCn),
	CN:                    consts.WebhookCertCn,
	TLSSecretName:         fmt.Sprintf("%s-https-certs", consts.WebhookCertCn),
	Namespace:             consts.ModuleNamespace,
	SANs: tlscertificate.DefaultSANs([]string{
		consts.WebhookCertCn,
		fmt.Sprintf("%s.%s", consts.WebhookCertCn, consts.ModuleNamespace),
		fmt.Sprintf("%s.%s.svc", consts.WebhookCertCn, consts.ModuleNamespace),
		// %CLUSTER_DOMAIN%:// is a special value to generate SAN like 'svc_name.svc_namespace.svc.cluster.local'
		fmt.Sprintf("%%CLUSTER_DOMAIN%%://%s.%s.svc", consts.WebhookCertCn, consts.ModuleNamespace),

		// The credentials endpoint is served by the controller container, behind a
		// Service of its own, and it is what presents this certificate now.
		// Its names have to be in here: a consumer that cannot verify the endpoint
		// is a consumer that either refuses to fetch credentials or fetches them
		// from whatever answered.
		consts.STSServiceName,
		fmt.Sprintf("%s.%s", consts.STSServiceName, consts.ModuleNamespace),
		fmt.Sprintf("%s.%s.svc", consts.STSServiceName, consts.ModuleNamespace),
		fmt.Sprintf("%%CLUSTER_DOMAIN%%://%s.%s.svc", consts.STSServiceName, consts.ModuleNamespace),
	}),
	FullValuesPathPrefix: fmt.Sprintf("%s.internal.customWebhookCert", consts.ModuleName),
})
