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

package tlsclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCAIsReReadWhenTheFileChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	first := writeCA(t, path, "first")

	ca := NewCA(path)
	pool1, err := ca.Pool()
	if err != nil {
		t.Fatal(err)
	}
	if !trusts(pool1, first) {
		t.Fatal("the pool does not hold the CA from the file")
	}

	time.Sleep(20 * time.Millisecond)
	second := writeCA(t, path, "second-and-longer")
	pool2, err := ca.Pool()
	if err != nil {
		t.Fatal(err)
	}
	if !trusts(pool2, second) || trusts(pool2, first) {
		t.Fatal("after the file was replaced the pool still holds the old CA")
	}
}

func TestCARefusesWhatIsNotACertificate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCA(path).Pool(); err == nil {
		t.Fatal("a file with no PEM certificate was accepted as a CA")
	}
	if _, err := NewCA("").Pool(); err == nil {
		t.Fatal("an unconfigured CA was accepted")
	}
}

func TestConfigVerifiesTheGivenName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	writeCA(t, path, "ca")
	cfg, err := NewCA(path).Config("system-garage.d8-system-s3.svc")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "system-garage.d8-system-s3.svc" || cfg.InsecureSkipVerify || cfg.MinVersion < tls.VersionTLS12 {
		t.Fatalf("unexpected client config: name %q, skip %v, min %x", cfg.ServerName, cfg.InsecureSkipVerify, cfg.MinVersion)
	}
}

func writeCA(t *testing.T, path, cn string) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return cert
}

// trusts reports whether cert verifies as a root in pool.
func trusts(pool *x509.CertPool, cert *x509.Certificate) bool {
	_, err := cert.Verify(x509.VerifyOptions{Roots: pool})
	return err == nil
}
