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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/deckhouse/sds-common-lib/logger"
)

func TestBucketOf(t *testing.T) {
	for _, tc := range []struct {
		host, target, want string
	}{
		{"system-garage.d8-system-s3.svc.cluster.local:3900", "/loki/chunks/1", "loki"},
		{"system-garage.d8-system-s3.svc.cluster.local", "/loki", "loki"},
		{"system-garage.d8-system-s3.svc.cluster.local:3900", "/loki?list-type=2", "loki"},
		{"system-garage.d8-system-s3.svc.cluster.local:3900", "/loki/a%2Fb%20c//d", "loki"},
		{"system-garage.d8-system-s3.svc.cluster.local:3900", "/", ""},
		{"loki.s3.garage", "/chunks/1", "loki"},
		{"Loki.S3.Garage:3900", "/chunks/1", "loki"},
		{"s3.garage", "/loki/x", "loki"},
		{"[::1]:3900", "/loki/x", "loki"},
	} {
		r := httptest.NewRequest(http.MethodGet, tc.target, nil)
		r.Host = tc.host
		if got := bucketOf(r, ".s3.garage"); got != tc.want {
			t.Errorf("Host %q, %s: bucket %q, want %q", tc.host, tc.target, got, tc.want)
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/chunks", nil)
	r.Host = "loki.s3.garage"
	if got := bucketOf(r, ""); got != "chunks" {
		t.Errorf("without a root domain the Host names no bucket, got %q", got)
	}
}

// TestBucketsAllowlist reads a directory laid out the way the kubelet lays out a
// ConfigMap volume, and names only the buckets in it.
func TestBucketsAllowlist(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "..2026_10_20_00_00_00.000000001")
	if err := os.Mkdir(data, 0o755); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"d8-monitoring-loki": "d8-monitoring/loki\n",
		"registry":           "d8-system/registry",
		"malformed":          "no-slash",
	} {
		if err := os.WriteFile(filepath.Join(data, key), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..data", key), filepath.Join(dir, key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Base(data), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}

	b := NewBuckets(dir)
	if got, _, _ := b.Labels("registry"); got != OtherBucket {
		t.Errorf("before Load registry is %q, want %q", got, OtherBucket)
	}
	if err := b.Load(); err != nil {
		t.Fatal(err)
	}
	for bucket, want := range map[string][3]string{
		"d8-monitoring-loki": {"d8-monitoring-loki", "d8-monitoring", "loki"},
		"registry":           {"registry", "d8-system", "registry"},
		"malformed":          {OtherBucket, "", ""},
		"..data":             {OtherBucket, "", ""},
		"made-up":            {OtherBucket, "", ""},
		"":                   {NoBucket, "", ""},
	} {
		got1, got2, got3 := b.Labels(bucket)
		if got := [3]string{got1, got2, got3}; got != want {
			t.Errorf("Labels(%q) = %v, want %v", bucket, got, want)
		}
	}

	if got, _, _ := (*Buckets)(nil).Labels("registry"); got != OtherBucket {
		t.Errorf("a nil allowlist names %q", got)
	}
	// The optional volume of a ConfigMap that does not exist yet.
	empty := NewBuckets(filepath.Join(dir, "missing"))
	if err := empty.Load(); err != nil {
		t.Errorf("a missing directory is an error: %v", err)
	}
}

// TestS3ProxyKeepsTheRequestAsSigned writes request lines by hand — an HTTP
// client would normalise them — and expects Garage to receive each one exactly:
// the request target byte for byte, the client's Host, the client's headers and
// no header the proxy made up.
func TestS3ProxyKeepsTheRequestAsSigned(t *testing.T) {
	type seen struct {
		uri, host, xff, ua string
		hasXFF, hasUA      bool
	}
	got := make(chan seen, 1)
	backend := serveBackend(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		xff, hasXFF := r.Header["X-Forwarded-For"]
		ua, hasUA := r.Header["User-Agent"]
		got <- seen{uri: r.RequestURI, host: r.Host, xff: strings.Join(xff, ","), hasXFF: hasXFF, ua: strings.Join(ua, ","), hasUA: hasUA}
		w.WriteHeader(http.StatusNoContent)
	}))
	addr, _, pool := startS3Proxy(t, backend, nil)

	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, ServerName: "garage", MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	const host = "system-garage.d8-system-s3.svc.cluster.local:3900"

	for _, tc := range []struct {
		target, extra string
	}{
		{"/bkt/a%2Fb%20c//d?x-id=PutObject&z=%41", ""},
		{"/bkt/key%20with%20spaces", ""},
		{"/bkt//double//slashes/", ""},
		{"/bkt/a+b/~t%7Eilde/%C3%A9", ""},
		{"/bkt/semi?a=1;b=2&c", ""},
		{"/bkt/presigned?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=GK%2F20261020%2Fgarage%2Fs3%2Faws4_request&X-Amz-Signature=abc", ""},
		{"/bkt/{raw}", ""},
		{"//bkt/leading", ""},
		{"/bkt/xff", "X-Forwarded-For: 10.0.0.1\r\nUser-Agent: aws-sdk-go\r\n"},
	} {
		fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\n%s\r\n", tc.target, host, tc.extra)
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("%s: %v", tc.target, err)
		}
		_ = resp.Body.Close()
		s := <-got
		if s.uri != tc.target {
			t.Errorf("Garage got request target %q, the client sent %q", s.uri, tc.target)
		}
		if s.host != host {
			t.Errorf("%s: Garage got Host %q, the client sent %q", tc.target, s.host, host)
		}
		if tc.extra == "" && (s.hasXFF || s.ua != "") {
			t.Errorf("%s: the proxy added X-Forwarded-For %q or User-Agent %q", tc.target, s.xff, s.ua)
		}
		if tc.extra != "" && (s.xff != "10.0.0.1" || s.ua != "aws-sdk-go") {
			t.Errorf("%s: the client's X-Forwarded-For/User-Agent arrived as %q/%q", tc.target, s.xff, s.ua)
		}
	}
}

// TestS3ProxyPassesAwsChunkedUntouched: a streaming-signature upload's body is
// the client's framing with a signature per chunk, and Garage verifies those
// signatures against the bytes, so they have to arrive as sent.
func TestS3ProxyPassesAwsChunkedUntouched(t *testing.T) {
	body := []byte("5;chunk-signature=aaaa\r\nhello\r\n0;chunk-signature=bbbb\r\n\r\n")
	type seen struct {
		body                []byte
		length              int64
		encoding, decoded   string
		contentSHA, expects string
	}
	got := make(chan seen, 1)
	backend := serveBackend(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{b, r.ContentLength, r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Decoded-Content-Length"),
			r.Header.Get("X-Amz-Content-Sha256"), r.Header.Get("Expect")}
	}))
	addr, _, pool := startS3Proxy(t, backend, nil)

	// The client waits for 100 Continue for up to 5 s before it sends the body.
	// The proxy has to pass Garage's 100 on, or every upload stalls that long.
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{RootCAs: pool, ServerName: "garage", MinVersion: tls.VersionTLS12},
		ExpectContinueTimeout: 5 * time.Second,
	}
	req, _ := http.NewRequest(http.MethodPut, "https://"+addr+"/bkt/obj", bytes.NewReader(body))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", "5")
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("Expect", "100-continue")
	start := time.Now()
	resp, err := (&http.Client{Transport: tr, Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("the upload took %v: 100 Continue did not come through", elapsed)
	}
	s := <-got
	if !bytes.Equal(s.body, body) || s.length != int64(len(body)) {
		t.Errorf("Garage got %q (Content-Length %d), the client sent %q", s.body, s.length, body)
	}
	if s.encoding != "aws-chunked" || s.decoded != "5" || s.contentSHA != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" || s.expects != "100-continue" {
		t.Errorf("headers arrived as %+v", s)
	}
}

// TestS3ProxyStreamsAndCountsPerBucket moves a body each way that the backend
// and the client only finish after the other side has seen its first part: a
// proxy that buffered either one would never deliver it. The bytes are counted
// against the bucket's SystemBucket, the request by method and status class.
func TestS3ProxyStreamsAndCountsPerBucket(t *testing.T) {
	const upload, download, part = 3 << 20, 5 << 20, 64 << 10
	firstUp, firstDown := make(chan struct{}), make(chan struct{})
	backend := serveBackend(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			if _, err := io.ReadFull(r.Body, make([]byte, part)); err != nil {
				t.Errorf("first part of the upload: %v", err)
			}
			close(firstUp)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Length", strconv.Itoa(download))
			_, _ = w.Write(make([]byte, part))
			http.NewResponseController(w).Flush()
			<-firstDown
			_, _ = w.Write(make([]byte, download-part))
		}
	}))
	allow := allowlist(t, map[string]string{"bkt": "team/app"})
	addr, m, pool := startS3Proxy(t, backend, allow)
	client := s3Client(pool)

	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(make([]byte, part))
		select {
		case <-firstUp:
		case <-time.After(5 * time.Second):
			pw.CloseWithError(fmt.Errorf("the backend never saw the first part of the upload"))
			return
		}
		_, _ = pw.Write(make([]byte, upload-part))
		_ = pw.Close()
	}()
	req, _ := http.NewRequest(http.MethodPut, "https://"+addr+"/bkt/big", pr)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	resp, err = client.Get("https://" + addr + "/bkt/big")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp.Body, make([]byte, part)); err != nil {
		t.Fatalf("first part of the download: %v", err)
	}
	close(firstDown)
	if n, _ := io.Copy(io.Discard, resp.Body); n != download-part {
		t.Errorf("downloaded %d bytes after the first part, want %d", n, download-part)
	}
	_ = resp.Body.Close()

	waitFor(t, func() bool {
		return testutil.ToFloat64(m.bucketRequests.WithLabelValues("bkt", "team", "app", "GET", "2xx")) == 1
	})
	if got := testutil.ToFloat64(m.bucketReceived.WithLabelValues("bkt", "team", "app")); got != upload {
		t.Errorf("bucket received %v bytes, want %d", got, upload)
	}
	if got := testutil.ToFloat64(m.bucketSent.WithLabelValues("bkt", "team", "app")); got != download {
		t.Errorf("bucket sent %v bytes, want %d", got, download)
	}
	if got := testutil.ToFloat64(m.bucketRequests.WithLabelValues("bkt", "team", "app", "PUT", "2xx")); got != 1 {
		t.Errorf("%v PUTs counted, want 1", got)
	}
	if got := histogramCount(t, m, "bkt"); got != 2 {
		t.Errorf("%d durations observed, want 2", got)
	}

	// The connection-level counters still count the TLS connection, headers
	// included, on the S3 port.
	_, port, _ := net.SplitHostPort(addr)
	if got := testutil.ToFloat64(m.received.WithLabelValues(port)); got <= upload {
		t.Errorf("port %s received %v bytes, want more than the %d of the body", port, got, upload)
	}
	if got := testutil.ToFloat64(m.sent.WithLabelValues(port)); got <= download {
		t.Errorf("port %s sent %v bytes, want more than the %d of the body", port, got, download)
	}
	if got := testutil.ToFloat64(m.connections.WithLabelValues(port)); got < 1 {
		t.Errorf("port %s counted %v connections", port, got)
	}
}

// TestS3ProxyLabelsAreBounded: what the client controls — the bucket in the path,
// the method — reaches a label only as one of a few fixed values; the status is
// counted by class.
func TestS3ProxyLabelsAreBounded(t *testing.T) {
	backend := serveBackend(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code, err := strconv.Atoi(r.URL.Query().Get("code"))
		if err != nil {
			code = http.StatusOK
		}
		w.WriteHeader(code)
	}))
	allow := allowlist(t, map[string]string{"bkt": "team/app"})
	addr, m, pool := startS3Proxy(t, backend, allow)
	client := s3Client(pool)

	do := func(method, target string) {
		t.Helper()
		req, _ := http.NewRequest(method, "https://"+addr+target, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	do(http.MethodGet, "/bkt/a?code=200")
	do(http.MethodHead, "/bkt/a?code=304")
	do(http.MethodGet, "/bkt/a?code=403")
	do(http.MethodDelete, "/bkt/a?code=503")
	do("PROPFIND", "/bkt/a")
	do(http.MethodGet, "/someone-elses-bucket/a?code=404")
	do(http.MethodGet, "/")

	for _, l := range [][]string{
		{"bkt", "team", "app", "GET", "2xx"},
		{"bkt", "team", "app", "HEAD", "3xx"},
		{"bkt", "team", "app", "GET", "403"},
		{"bkt", "team", "app", "DELETE", "5xx"},
		{"bkt", "team", "app", "other", "2xx"},
		{OtherBucket, "", "", "GET", "4xx"},
		{NoBucket, "", "", "GET", "2xx"},
	} {
		waitFor(t, func() bool { return testutil.ToFloat64(m.bucketRequests.WithLabelValues(l...)) == 1 })
	}
	if n := testutil.CollectAndCount(m.bucketRequests); n != 7 {
		t.Errorf("%d request series, want 7", n)
	}
}

// TestS3ProxyGarageDown: a request Garage does not answer is a 502 to the
// client and a 5xx in the count.
func TestS3ProxyGarageDown(t *testing.T) {
	addr, m, pool := startS3Proxy(t, freeAddr(t), allowlist(t, map[string]string{"bkt": "team/app"}))
	resp, err := s3Client(pool).Get("https://" + addr + "/bkt/a")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d, want 502", resp.StatusCode)
	}
	waitFor(t, func() bool {
		return testutil.ToFloat64(m.bucketRequests.WithLabelValues("bkt", "team", "app", "GET", "5xx")) == 1
	})
}

// TestS3HTTPCanBeTurnedOff: TLS_S3_HTTP=false puts S3 back on the TCP copy, which
// carries anything — here bytes that are not HTTP at all — and counts nothing
// per bucket.
func TestS3HTTPCanBeTurnedOff(t *testing.T) {
	env := func(vals map[string]string) func(string) string {
		return func(k string) string { return vals[k] }
	}
	if got := S3ListenFromEnv(env(map[string]string{EnvS3Listen: "[::]:3900"})); got != "[::]:3900" {
		t.Errorf("by default S3 is served on %q as HTTP, want [::]:3900", got)
	}
	if got := S3ListenFromEnv(env(map[string]string{EnvS3Listen: "[::]:3900", EnvS3HTTP: "true"})); got != "[::]:3900" {
		t.Errorf("TLS_S3_HTTP=true: %q", got)
	}
	if got := S3ListenFromEnv(env(map[string]string{EnvS3Listen: "[::]:3900", EnvS3HTTP: "false"})); got != "" {
		t.Errorf("TLS_S3_HTTP=false still serves %q as HTTP", got)
	}

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
	addr := freeAddr(t)
	m := NewMetrics()
	runProxy(t, &Proxy{
		Routes:   []Route{{Listen: addr, Backend: bln.Addr().String()}},
		CertFile: certFile, KeyFile: keyFile,
		S3Listen: S3ListenFromEnv(env(map[string]string{EnvS3Listen: addr, EnvS3HTTP: "false"})),
		Buckets:  allowlist(t, map[string]string{"bkt": "team/app"}),
		Metrics:  m,
	}, addr)

	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: ca.pool, ServerName: "garage", MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("\x00not http"))
	_ = conn.CloseWrite()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if out, err := io.ReadAll(conn); err != nil || string(out) != "got \x00not http" {
		t.Fatalf("response %q, %v", out, err)
	}
	if n := testutil.CollectAndCount(m.bucketRequests); n != 0 {
		t.Errorf("the TCP copy counted %d request series", n)
	}
}

// --- helpers ------------------------------------------------------------------

// startS3Proxy runs a terminator whose one route is S3, served as HTTP.
func startS3Proxy(t *testing.T, backend string, buckets *Buckets) (string, *Metrics, *x509.CertPool) {
	t.Helper()
	dir := t.TempDir()
	ca := newCA(t)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	ca.issue(t, certFile, keyFile, "garage", 1)
	addr := freeAddr(t)
	m := NewMetrics()
	runProxy(t, &Proxy{
		Routes:   []Route{{Listen: addr, Backend: backend}},
		CertFile: certFile, KeyFile: keyFile,
		S3Listen: addr, S3RootDomain: ".s3.garage", Buckets: buckets,
		Metrics: m,
	}, addr)
	return addr, m, ca.pool
}

func runProxy(t *testing.T, p *Proxy, addr string) {
	t.Helper()
	log, err := logger.NewLogger(logger.InfoLevel)
	if err != nil {
		t.Fatal(err)
	}
	p.Log = log
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("proxy stopped with %v", err)
		}
	})
	waitFor(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	})
}

func serveBackend(t *testing.T, h http.Handler) string {
	t.Helper()
	ln := listenLoopback(t)
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func allowlist(t *testing.T, owners map[string]string) *Buckets {
	t.Helper()
	dir := t.TempDir()
	for bucket, owner := range owners {
		if err := os.WriteFile(filepath.Join(dir, bucket), []byte(owner), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	b := NewBuckets(dir)
	if err := b.Load(); err != nil {
		t.Fatal(err)
	}
	return b
}

func s3Client(pool *x509.CertPool) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: pool, ServerName: "garage", MinVersion: tls.VersionTLS12,
		}},
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
	}
}

// histogramCount is how many durations were observed for bucket.
func histogramCount(t *testing.T, m *Metrics, bucket string) uint64 {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, f := range families {
		if f.GetName() != "system_s3_proxy_bucket_request_duration_seconds" {
			continue
		}
		for _, s := range f.GetMetric() {
			for _, l := range s.GetLabel() {
				if l.GetName() == "bucket" && l.GetValue() == bucket {
					n += s.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return n
}
