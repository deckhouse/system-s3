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

package sts

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

const testServerName = "system-s3-sts.d8-system-s3.svc"

// TestEndpointPicksUpARenewedCertificate replaces the certificate files the way
// the kubelet does after a renewal and expects the next handshake to present the
// new pair. Nothing restarts the controller on a renewal, so an endpoint that
// read the files once would serve the old certificate until it expired.
func TestEndpointPicksUpARenewedCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	roots := x509.NewCertPool()
	roots.AddCert(issueSelfSigned(t, certFile, keyFile, 1))

	addr := freeAddr(t)
	r := &Runnable{
		Server:   testServer(t, &fakeAuthorizer{}, &fakeIssuer{}),
		Addr:     addr,
		CertFile: certFile,
		KeyFile:  keyFile,
		Log:      logger.NewNop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("the endpoint stopped with %v", err)
		}
	})

	if got := servedSerial(t, addr, roots); got != 1 {
		t.Fatalf("served serial %d, want 1", got)
	}

	// The loader watches size and mtime; make sure the clock has moved so a
	// coarse filesystem timestamp cannot hide the change.
	time.Sleep(20 * time.Millisecond)
	roots.AddCert(issueSelfSigned(t, certFile, keyFile, 2))

	if got := servedSerial(t, addr, roots); got != 2 {
		t.Fatalf("after renewal the endpoint still serves serial %d", got)
	}
}

// servedSerial completes a handshake with the endpoint, waiting for it to start
// listening, and returns the serial of the certificate it presented.
func servedSerial(t *testing.T, addr string, roots *x509.CertPool) int64 {
	t.Helper()
	cfg := &tls.Config{RootCAs: roots, ServerName: testServerName, MinVersion: tls.VersionTLS12}
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := tls.Dial("tcp", addr, cfg)
		if err == nil {
			defer conn.Close()
			return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
		}
		if time.Now().After(deadline) {
			t.Fatalf("no handshake with the endpoint at %s: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// issueSelfSigned writes a self-signed pair for testServerName and returns the
// certificate, which the client trusts directly.
func issueSelfSigned(t *testing.T, certFile, keyFile string, serial int64) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: testServerName},
		DNSNames:     []string{testServerName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
