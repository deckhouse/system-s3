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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/deckhouse/sds-common-lib/logger"
)

// pemCertificate is a self-signed certificate valid until notAfter, PEM-encoded.
func pemCertificate(t *testing.T, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "system-garage"},
		NotBefore:    notAfter.Add(-365 * 24 * time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func tlsSecret(name string, crt []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "d8-system-s3", Name: name},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: crt},
	}
}

// certificateExpiry reads one series of certificate_expiration_seconds, and
// whether it exists.
func certificateExpiry(t *testing.T, secret string) (float64, bool) {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != "system_s3_certificate_expiration_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "secret" && l.GetValue() == secret {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// The expiry of each Secret's leaf certificate is published, and a Secret that
// is gone or holds nothing parseable loses its series: a stale date would say
// a certificate that is no longer there is fine until then.
func TestCertificatesArePublishedAndForgottenWhenUnreadable(t *testing.T) {
	notAfter := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	garageTLS := tlsSecret("garage-tls", pemCertificate(t, notAfter))
	c := fake.NewClientBuilder().WithScheme(reconcilerScheme(t)).
		WithObjects(garageTLS, tlsSecret("webhooks-https-certs", []byte("not a certificate"))).Build()

	now := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	m := (&StoreManager{log: logger.NewNop(), now: func() time.Time { return now }}).
		WithCertificates(c, "d8-system-s3", "garage-tls", "webhooks-https-certs", "")
	if len(m.certSecrets) != 2 {
		t.Fatalf("certSecrets=%v, want the empty name skipped", m.certSecrets)
	}
	m.publishCertificates(context.Background())

	if got, ok := certificateExpiry(t, "garage-tls"); !ok || got != float64(notAfter.Unix()) {
		t.Errorf("garage-tls expiry = %v (present %v), want %d", got, ok, notAfter.Unix())
	}
	if _, ok := certificateExpiry(t, "webhooks-https-certs"); ok {
		t.Error("an unparseable certificate published an expiry")
	}

	// Not read again before the interval is up.
	if err := c.Delete(context.Background(), garageTLS); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	m.publishCertificates(context.Background())
	if _, ok := certificateExpiry(t, "garage-tls"); !ok {
		t.Fatal("the Secret was read again before certificatesInterval")
	}

	now = now.Add(certificatesInterval)
	m.publishCertificates(context.Background())
	if _, ok := certificateExpiry(t, "garage-tls"); ok {
		t.Error("a Secret that is gone kept its expiry")
	}
}

// The leaf is the first certificate of the bundle; what follows it is the
// chain, which expires on its own schedule.
func TestLeafNotAfterReadsTheFirstCertificate(t *testing.T) {
	leaf := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	bundle := append(pemCertificate(t, leaf), pemCertificate(t, leaf.AddDate(5, 0, 0))...)
	got, err := leafNotAfter(bundle)
	if err != nil || !got.Equal(leaf) {
		t.Fatalf("leafNotAfter = %v, %v; want %v", got, err, leaf)
	}
	if _, err := leafNotAfter(nil); err == nil {
		t.Error("an empty tls.crt parsed")
	}
}
