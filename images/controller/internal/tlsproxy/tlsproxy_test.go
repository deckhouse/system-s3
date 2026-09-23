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

package tlsproxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

func TestParseRoutes(t *testing.T) {
	routes, err := ParseRoutes(" [::]:3900=127.0.0.1:3910, [::]:3903=127.0.0.1:3913 ,")
	if err != nil {
		t.Fatal(err)
	}
	want := []Route{{"[::]:3900", "127.0.0.1:3910"}, {"[::]:3903", "127.0.0.1:3913"}}
	if fmt.Sprint(routes) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", routes, want)
	}

	for _, bad := range []string{"", ",", "[::]:3900", "=127.0.0.1:1", "[::]:1="} {
		if _, err := ParseRoutes(bad); err == nil {
			t.Errorf("ParseRoutes(%q) accepted a malformed value", bad)
		}
	}
}

// TestProxyCarriesHTTPThroughTLS drives a real HTTP exchange through the
// terminator: the client verifies the certificate against the CA, and the
// backend sees the request — Host header included, because a SigV4 signature
// covers it and a terminator that rewrote it would break every signed request.
func TestProxyCarriesHTTPThroughTLS(t *testing.T) {
	dir := t.TempDir()
	ca := newCA(t)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	ca.issue(t, certFile, keyFile, "system-garage.d8-system-s3.svc", 1)

	var gotHost string
	backend := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHost = r.Host
			body, _ := io.ReadAll(r.Body)
			_, _ = w.Write([]byte("echo:" + string(body)))
		}),
	}
	bln := listenLoopback(t)
	go func() { _ = backend.Serve(bln) }()
	t.Cleanup(func() { _ = backend.Close() })

	addr := startProxy(t, bln.Addr().String(), certFile, keyFile)

	client := httpsClient(ca.pool, "system-garage.d8-system-s3.svc")
	req, _ := http.NewRequest(http.MethodPut, "https://"+addr+"/bucket/object", strings.NewReader("payload"))
	req.Host = "system-garage.d8-system-s3.svc.cluster.local:3900"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if string(body) != "echo:payload" {
		t.Fatalf("body through the terminator is %q", body)
	}
	if gotHost != req.Host {
		t.Fatalf("backend saw Host %q, the client sent %q", gotHost, req.Host)
	}
}

// TestProxyIsRefusedByAClientWithAnotherCA is the property the whole thing
// exists for: a client holding a different CA does not get a connection.
func TestProxyIsRefusedByAClientWithAnotherCA(t *testing.T) {
	dir := t.TempDir()
	ca := newCA(t)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	ca.issue(t, certFile, keyFile, "system-garage.d8-system-s3.svc", 1)

	bln := listenLoopback(t)
	t.Cleanup(func() { _ = bln.Close() })
	addr := startProxy(t, bln.Addr().String(), certFile, keyFile)

	other := newCA(t)
	_, err := httpsClient(other.pool, "system-garage.d8-system-s3.svc").Get("https://" + addr + "/")
	if err == nil {
		t.Fatal("a client that does not trust the module's CA got through")
	}
}

// TestProxyPicksUpARenewedCertificate replaces the certificate files the way a
// renewal does and expects the next handshake to present the new one, without
// the terminator being restarted.
func TestProxyPicksUpARenewedCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := newCA(t)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	ca.issue(t, certFile, keyFile, "system-garage.d8-system-s3.svc", 1)

	bln := listenLoopback(t)
	t.Cleanup(func() { _ = bln.Close() })
	addr := startProxy(t, bln.Addr().String(), certFile, keyFile)

	if got := servedSerial(t, addr, ca.pool); got != 1 {
		t.Fatalf("served serial %d, want 1", got)
	}

	// A different size or mtime is what the loader watches; make sure the clock
	// has moved so a coarse filesystem timestamp cannot hide the change.
	time.Sleep(20 * time.Millisecond)
	ca.issue(t, certFile, keyFile, "system-garage.d8-system-s3.svc", 2)

	if got := servedSerial(t, addr, ca.pool); got != 2 {
		t.Fatalf("after renewal the terminator still serves serial %d", got)
	}
}

// TestPipeHalfClose checks that the response still arrives after the client has
// closed its sending side — the case a naive "close both at the first EOF" gets
// wrong.
func TestPipeHalfClose(t *testing.T) {
	dir := t.TempDir()
	ca := newCA(t)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	ca.issue(t, certFile, keyFile, "garage", 1)

	bln := listenLoopback(t)
	t.Cleanup(func() { _ = bln.Close() })
	go func() {
		c, err := bln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		in, _ := io.ReadAll(c) // until the client's write side closes
		_, _ = c.Write([]byte("got " + string(in)))
	}()

	addr := startProxy(t, bln.Addr().String(), certFile, keyFile)
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: ca.pool, ServerName: "garage", MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("hello"))
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	out, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "got hello" {
		t.Fatalf("response after half-close is %q", out)
	}
}

// --- helpers ------------------------------------------------------------------

func startProxy(t *testing.T, backend, certFile, keyFile string) string {
	t.Helper()
	addr := freeAddr(t)
	log, err := logger.NewLogger(logger.InfoLevel)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	p := &Proxy{Routes: []Route{{Listen: addr, Backend: backend}}, CertFile: certFile, KeyFile: keyFile, Log: log}
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("proxy stopped with %v", err)
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("proxy did not start listening on %s", addr)
	return ""
}

func servedSerial(t *testing.T, addr string, pool *x509.CertPool) int64 {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs: pool, ServerName: "system-garage.d8-system-s3.svc", MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64()
}

func httpsClient(pool *x509.CertPool, serverName string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12,
		}},
	}
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln := listenLoopback(t)
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1000),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

func (ca *testCA) issue(t *testing.T, certFile, keyFile, dnsName string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
