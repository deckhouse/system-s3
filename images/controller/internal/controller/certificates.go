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

package controller

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/monitoring"
)

// certificatesInterval is how often the module's certificates are read for
// certificate_expiration_seconds. The alert on them looks thirty days ahead, so
// a reading minutes old is as good as a fresh one, and each reading is a GET
// per Secret on the API server.
const certificatesInterval = 5 * time.Minute

// WithCertificates names the TLS Secrets in namespace whose expiry is published.
// Empty names are skipped: a chart that configures no credentials endpoint has
// no certificate for it.
func (m *StoreManager) WithCertificates(reader client.Reader, namespace string, secrets ...string) *StoreManager {
	m.certReader, m.certNamespace = reader, namespace
	m.certSecrets = m.certSecrets[:0]
	for _, s := range secrets {
		if s != "" {
			m.certSecrets = append(m.certSecrets, s)
		}
	}
	return m
}

// publishCertificates refreshes certificate_expiration_seconds, no more often
// than certificatesInterval.
//
// Read through the uncached reader: these Secrets are rendered by the chart,
// not written by the controller, so they do not carry the label the Secret
// cache is narrowed to (see cmd/main.go).
//
// A Secret that cannot be read or holds no parseable certificate loses its
// series instead of keeping the last one. The alert then cannot fire for it,
// which is the lesser evil: a stale expiry would say a certificate that is gone
// is fine until a date that no longer means anything. The log says which.
func (m *StoreManager) publishCertificates(ctx context.Context) {
	if m.certReader == nil || len(m.certSecrets) == 0 {
		return
	}
	now := m.clock()
	if !m.lastCerts.IsZero() && now.Sub(m.lastCerts) < certificatesInterval {
		return
	}
	m.lastCerts = now

	for _, name := range m.certSecrets {
		secret := &corev1.Secret{}
		if err := m.certReader.Get(ctx, client.ObjectKey{Namespace: m.certNamespace, Name: name}, secret); err != nil {
			m.log.Error("unable to read a certificate Secret", logger.Err(err), "secret", name)
			monitoring.ForgetCertificate(name)
			continue
		}
		notAfter, err := leafNotAfter(secret.Data[corev1.TLSCertKey])
		if err != nil {
			m.log.Error("unable to parse the certificate in a Secret", logger.Err(err), "secret", name)
			monitoring.ForgetCertificate(name)
			continue
		}
		monitoring.CertificateExpiration(name, notAfter)
	}
}

// leafNotAfter is the expiry of the first certificate in a PEM bundle, which is
// the leaf: tls.crt carries the serving certificate first and any intermediates
// after it.
func leafNotAfter(bundle []byte) (time.Time, error) {
	for len(bundle) > 0 {
		var block *pem.Block
		block, bundle = pem.Decode(bundle)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return time.Time{}, err
		}
		return cert.NotAfter, nil
	}
	return time.Time{}, errors.New("no PEM certificate in tls.crt")
}
