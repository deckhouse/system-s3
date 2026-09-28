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

// Package persistedca keeps the module's common CA across Deckhouse restarts.
//
// module-sdk's self-signed TLS hook reads the common CA from the module's
// values and nowhere else. Values live in Deckhouse's memory: on every start of
// the operator they are empty, the hook finds no CA, generates a new one and
// re-issues every certificate signed by the old one. For this module that means
// a new CA behind the S3 endpoint and the credentials endpoint each time
// Deckhouse restarts — and a consumer that trusts the CA the way USAGE says to
// (SSL_CERT_DIR, read once per process by Go) stops trusting both endpoints
// until it is restarted itself. On a stand whose Deckhouse was settling after
// bootstrap it happened four times in an hour.
//
// The CA, key included, is therefore rendered into a Secret of its own
// (templates/module-ca.yaml), and the certificate hooks are registered through
// Register: before module-sdk's logic runs, a CA missing from the values is taken
// from that Secret. The leaf certificates are then still signed by the CA they
// carry, and nothing is re-issued.
package persistedca

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	tlscertificate "github.com/deckhouse/module-sdk/common-hooks/tls-certificate"
	"github.com/deckhouse/module-sdk/pkg"
	"github.com/deckhouse/module-sdk/pkg/certificate"
	objectpatch "github.com/deckhouse/module-sdk/pkg/object-patch"
	"github.com/deckhouse/module-sdk/pkg/registry"
	"github.com/tidwall/gjson"
)

// SecretName is the Secret the common CA is kept in, in the module namespace.
const SecretName = "system-s3-ca"

const snapshotKey = "persisted_ca"

// persisted is the CA as the snapshot filter reads it out of the Secret: the
// base64 of each PEM, as it is in the Secret's data.
type persisted struct {
	Crt string `json:"crt"`
	Key string `json:"key"`
}

// Register registers a self-signed TLS hook whose common CA survives a restart
// of Deckhouse: module-sdk's hook, with the CA Secret added to its snapshots and
// the CA restored from it into the values the hook reads.
func Register(conf tlscertificate.GenSelfSignedTLSHookConf) bool {
	cfg := tlscertificate.GenSelfSignedTLSConfig(conf)
	cfg.Kubernetes = append(cfg.Kubernetes, pkg.KubernetesConfig{
		Name:              snapshotKey,
		APIVersion:        "v1",
		Kind:              "Secret",
		NamespaceSelector: &pkg.NamespaceSelector{NameSelector: &pkg.NameSelector{MatchNames: []string{conf.Namespace}}},
		NameSelector:      &pkg.NameSelector{MatchNames: []string{SecretName}},
		JqFilter:          `{"crt": .data."tls.crt", "key": .data."tls.key"}`,
	})
	generate := tlscertificate.GenSelfSignedTLS(conf)
	caPath := conf.CommonCAPath()

	return registry.RegisterFunc(cfg, func(ctx context.Context, input *pkg.HookInput) error {
		if hasCA(input.Values.Get(caPath)) {
			return generate(ctx, input)
		}
		stored, err := objectpatch.UnmarshalToStruct[persisted](input.Snapshots, snapshotKey)
		if err != nil {
			return fmt.Errorf("read the persisted CA: %w", err)
		}
		if len(stored) == 0 {
			// First install: nothing to restore. module-sdk generates the CA and
			// the chart persists it from the values.
			return generate(ctx, input)
		}
		auth, err := authorityFrom(stored[0])
		if err != nil {
			// A Secret that cannot be read back is reported, not replaced: a new
			// CA is exactly what this package exists to prevent, and it should
			// not happen quietly because a Secret was edited by hand.
			return fmt.Errorf("the CA in Secret %s/%s is unusable: %w", conf.Namespace, SecretName, err)
		}
		raw, err := json.Marshal(auth)
		if err != nil {
			return fmt.Errorf("encode the persisted CA: %w", err)
		}
		input.Values.Set(caPath, auth)
		input.Logger.Info("restored the module CA from its Secret", "secret", SecretName)

		// module-sdk's Set records a patch and does not change what Get returns in
		// this same run, so the hook is handed values in which the CA is already
		// there.
		restored := *input
		restored.Values = overlay{PatchableValuesCollector: input.Values, path: caPath, raw: string(raw)}
		return generate(ctx, &restored)
	})
}

// hasCA reports whether the values hold a usable CA. The schema defaults the
// path to an empty object, which is present and still no CA.
func hasCA(v gjson.Result) bool {
	return v.Get("crt").String() != "" && v.Get("key").String() != ""
}

// authorityFrom turns the Secret's data back into the CA module-sdk works with,
// checking that it is a certificate and a key rather than trusting the bytes.
func authorityFrom(p persisted) (*certificate.Authority, error) {
	crt, err := base64.StdEncoding.DecodeString(p.Crt)
	if err != nil || !strings.Contains(string(crt), "BEGIN CERTIFICATE") {
		return nil, fmt.Errorf("tls.crt is not a PEM certificate")
	}
	key, err := base64.StdEncoding.DecodeString(p.Key)
	if err != nil || !strings.Contains(string(key), "PRIVATE KEY") {
		return nil, fmt.Errorf("tls.key is not a PEM private key")
	}
	return &certificate.Authority{Cert: crt, Key: key}, nil
}

// overlay answers reads of one path from a value of its own and everything else
// from the values underneath. Writes go to the values underneath.
type overlay struct {
	pkg.PatchableValuesCollector
	path string
	raw  string
}

func (o overlay) Get(path string) gjson.Result {
	if v, ok := o.lookup(path); ok {
		return v
	}
	return o.PatchableValuesCollector.Get(path)
}

func (o overlay) GetOk(path string) (gjson.Result, bool) {
	if v, ok := o.lookup(path); ok {
		return v, v.Exists()
	}
	return o.PatchableValuesCollector.GetOk(path)
}

func (o overlay) Exists(path string) bool {
	if v, ok := o.lookup(path); ok {
		return v.Exists()
	}
	return o.PatchableValuesCollector.Exists(path)
}

func (o overlay) GetRaw(path string) any {
	if v, ok := o.lookup(path); ok {
		return v.Value()
	}
	return o.PatchableValuesCollector.GetRaw(path)
}

func (o overlay) lookup(path string) (gjson.Result, bool) {
	switch {
	case path == o.path:
		return gjson.Parse(o.raw), true
	case strings.HasPrefix(path, o.path+"."):
		return gjson.Get(o.raw, strings.TrimPrefix(path, o.path+".")), true
	}
	return gjson.Result{}, false
}
