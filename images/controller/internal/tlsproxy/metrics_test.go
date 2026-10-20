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
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/deckhouse/sds-common-lib/logger"
)

// TestProxyCountsTrafficAndServesItWithTheToken: the bytes a client sends and
// receives are counted on the listener's port, the connection once, and the
// counters reach only a scraper that presents the metrics token.
func TestProxyCountsTrafficAndServesItWithTheToken(t *testing.T) {
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
		in, _ := io.ReadAll(c)
		_, _ = c.Write([]byte("got " + string(in)))
	}()

	addr, metricsAddr := freeAddr(t), freeAddr(t)
	m := NewMetrics()
	log, err := logger.NewLogger(logger.InfoLevel)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	p := &Proxy{
		Routes:   []Route{{Listen: addr, Backend: bln.Addr().String()}},
		CertFile: certFile, KeyFile: keyFile, Log: log,
		Metrics: m, MetricsListen: metricsAddr, MetricsToken: "secret",
	}
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("proxy stopped with %v", err)
		}
	})

	clientTLS := &tls.Config{RootCAs: ca.pool, ServerName: "garage", MinVersion: tls.VersionTLS12}
	var conn *tls.Conn
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if conn, err = tls.Dial("tcp", addr, clientTLS); err == nil || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("hello"))
	_ = conn.CloseWrite()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if out, err := io.ReadAll(conn); err != nil || string(out) != "got hello" {
		t.Fatalf("response %q, %v", out, err)
	}

	_, port, _ := net.SplitHostPort(addr)
	for deadline := time.Now().Add(5 * time.Second); testutil.ToFloat64(m.sent.WithLabelValues(port)) < 9 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.received.WithLabelValues(port)); got != 5 {
		t.Errorf("received %v bytes, want 5", got)
	}
	if got := testutil.ToFloat64(m.sent.WithLabelValues(port)); got != 9 {
		t.Errorf("sent %v bytes, want 9", got)
	}
	if got := testutil.ToFloat64(m.connections.WithLabelValues(port)); got != 1 {
		t.Errorf("%v connections, want 1", got)
	}

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}, Timeout: 5 * time.Second}
	scrape := func(token string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, "https://"+metricsAddr+"/metrics", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	if code, _ := scrape(""); code != http.StatusForbidden {
		t.Errorf("scrape without the token: %d, want 403", code)
	}
	if code, _ := scrape("wrong"); code != http.StatusForbidden {
		t.Errorf("scrape with a wrong token: %d, want 403", code)
	}
	code, body := scrape("secret")
	want := fmt.Sprintf(`system_s3_proxy_received_bytes_total{port=%q} 5`, port)
	if code != http.StatusOK || !strings.Contains(body, want) {
		t.Errorf("scrape with the token: %d, body lacks %s:\n%s", code, want, body)
	}
}

// TestMetricsRefuseEveryoneWithoutAToken: a terminator started before the token
// exists serves its counters to no one, not to everyone.
func TestMetricsRefuseEveryoneWithoutAToken(t *testing.T) {
	h := NewMetrics().Handler("")
	for _, header := range []string{"", "Bearer ", "Bearer x"} {
		req, _ := http.NewRequest(http.MethodGet, "/metrics", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("Authorization %q: %d, want 403", header, rec.Code)
		}
	}
}

// The size of the metadata is the files of Garage's metadata_dir, of its LMDB
// environment and of its snapshots — and nothing under the data directory next
// to it, which on a full replica holds millions of files. A directory that
// cannot be read publishes no series rather than a zero.
func TestMetadataSizeCountsTheMetadataAndNothingElse(t *testing.T) {
	root := t.TempDir()
	meta := filepath.Join(root, "meta")
	write := func(path string, size int) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(meta, "node_key"), 64)
	write(filepath.Join(meta, "db.lmdb", "data.mdb"), 4096)
	write(filepath.Join(meta, "db.lmdb", "lock.mdb"), 1024)
	write(filepath.Join(meta, "snapshots", "2026-10-20T00:00:00Z", "data.mdb"), 2048)
	write(filepath.Join(meta, "deeper", "ignored"), 100000)
	write(filepath.Join(root, "data", "ab", "cd", "block"), 1<<20)

	m := NewMetrics()
	m.WatchMetadata(meta)
	want := `
# HELP system_s3_replica_metadata_bytes Size of Garage's metadata database on this replica (the LMDB files and any metadata snapshots), in bytes.
# TYPE system_s3_replica_metadata_bytes gauge
system_s3_replica_metadata_bytes 7232
`
	if err := testutil.GatherAndCompare(m.registry, strings.NewReader(want), "system_s3_replica_metadata_bytes"); err != nil {
		t.Error(err)
	}

	gone := NewMetrics()
	gone.WatchMetadata(filepath.Join(root, "absent"))
	if n, err := testutil.GatherAndCount(gone.registry, "system_s3_replica_metadata_bytes"); err != nil || n != 0 {
		t.Errorf("%d series (%v) for a directory that does not exist, want none", n, err)
	}
}
