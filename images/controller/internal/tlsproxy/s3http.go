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
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/deckhouse/sds-common-lib/logger"
)

// S3 as HTTP.
//
// The S3 route reads each request far enough to know its bucket and passes it to
// Garage through a reverse proxy, so that the bucket's requests and bytes can be
// counted (metrics.go). What a SigV4 signature covers is the method, the request
// target exactly as written on the request line, the Host header and the signed
// headers, and the proxy keeps every one of them:
//
//   - The request target is copied from the request line, not rebuilt from the
//     parsed URL. Go would re-escape a path on the way out wherever its idea of
//     the canonical escaping differs from the client's, and a key with %2F, a
//     space or a double slash is exactly where they differ.
//   - The Host header is the client's, not the backend address.
//   - No header is added. A reverse proxy normally adds X-Forwarded-For; Garage
//     would only log it, and any header the client sent is passed as it was.
//
// Bodies are streamed both ways and never held: a multi-gigabyte upload or
// download, or an aws-chunked body with its per-chunk signatures, goes through
// as it arrives. What the proxy does drop are the hop-by-hop headers HTTP says a
// proxy must (Connection and those it names, Keep-Alive, TE, Upgrade, …): no S3
// client signs them.
//
// The connection-level counters (bytes and connections per port) count the TLS
// connection as before, so they mean the same on both kinds of route.
const (
	// s3HeaderTimeout bounds how long a client may take to send a request's
	// headers, and s3IdleTimeout how long a keep-alive connection may wait for
	// its next request. Bodies have no bound: a large object over a slow link
	// takes as long as it takes.
	s3HeaderTimeout = 30 * time.Second
	s3IdleTimeout   = 90 * time.Second

	// backendIdleConns is how many idle connections to Garage are kept for reuse.
	// Every client request becomes a request on one of them, so with too few a
	// busy terminator opens and closes a loopback connection per request.
	backendIdleConns = 256

	// backendIdleTimeout is how long such a connection may stay idle. It is kept
	// under the time Garage's HTTP server waits for the next request on an idle
	// keep-alive connection before closing it: a request written onto a
	// connection Garage has just closed fails, and one with a body cannot be
	// retried on another.
	backendIdleTimeout = 15 * time.Second

	// statusClientClosedRequest is what a request is counted as when its client
	// went away before Garage answered: nginx's 499. It is never sent — there is
	// nobody to send it to — and it keeps a client's disconnect out of the 5xx
	// that mean Garage failed.
	statusClientClosedRequest = 499
)

// forwardingHeaders are the headers httputil.ReverseProxy strips from a request
// before Rewrite, and that Rewrite puts back: whatever the client sent is passed
// on, signed or not.
var forwardingHeaders = []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"}

// s3Server is the HTTP server of the S3 route. The route's accept loop does the
// handshake and hands the connection over; the server does the rest.
type s3Server struct {
	srv *http.Server
	ln  *connListener
	lm  *listenerMetrics
}

func (p *Proxy) startS3(backend string, addr net.Addr, lm *listenerMetrics) *s3Server {
	errLog := log.New(debugWriter{p.Log}, "", 0)
	h := &s3Handler{
		proxy: &httputil.ReverseProxy{
			Rewrite:       s3Rewrite(backend),
			Transport:     s3Transport(),
			FlushInterval: -1,
			ErrorHandler:  p.s3Error,
			ErrorLog:      errLog,
		},
		rootDomain: p.S3RootDomain,
		buckets:    p.Buckets,
		metrics:    p.Metrics,
	}
	s := &s3Server{
		srv: &http.Server{
			Handler:           h,
			ReadHeaderTimeout: s3HeaderTimeout,
			IdleTimeout:       s3IdleTimeout,
			ErrorLog:          errLog,
		},
		ln: newConnListener(addr),
		lm: lm,
	}
	go func() { _ = s.srv.Serve(s.ln) }()
	return s
}

// serve hands a client connection, its handshake done, to the HTTP server,
// counting its bytes as the TCP copy does.
func (s *s3Server) serve(client *tls.Conn) {
	var c net.Conn = client
	if s.lm != nil {
		c = &countedConn{Conn: client, received: s.lm.received, sent: s.lm.sent}
	}
	s.ln.deliver(c)
}

// Close stops the server and every connection it holds.
func (s *s3Server) Close() {
	_ = s.ln.Close()
	_ = s.srv.Close()
}

// s3Rewrite points a request at Garage and leaves everything SigV4 covers as the
// client sent it.
func s3Rewrite(backend string) func(*httputil.ProxyRequest) {
	return func(pr *httputil.ProxyRequest) {
		pr.Out.URL.Scheme = "http"
		pr.Out.URL.Host = backend
		// Out.Host stays the client's Host header: the request was cloned from In.
		//
		// The query too is the client's: ReverseProxy re-encodes one with a ';' in
		// it before Rewrite is called.
		pr.Out.URL.RawQuery = pr.In.URL.RawQuery
		// An opaque URL is written on the request line as it is. A path that
		// begins with "//" cannot be one — Go would read it as a scheme-relative
		// URL — and is left to the parsed form, which keeps it as long as its
		// escaping is one Go considers valid.
		if path, _, _ := strings.Cut(pr.In.RequestURI, "?"); strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "//") {
			pr.Out.URL.Opaque = path
		}
		for _, h := range forwardingHeaders {
			if v, ok := pr.In.Header[h]; ok {
				pr.Out.Header[h] = v
			}
		}
	}
}

// s3Transport is the connection pool to Garage on the loopback.
//
// It has no timeout on a response: a ListObjects over a large bucket or a
// CompleteMultipartUpload of many parts may take Garage a while, and a client
// that gives up cancels its request anyway. It does not compress (an
// Accept-Encoding it added would be a header the client did not sign, and it
// would decode the answer behind the client's back), and it ignores any proxy in
// the environment: Garage is on the loopback of this pod.
func s3Transport() *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          backendIdleConns,
		MaxIdleConnsPerHost:   backendIdleConns,
		IdleConnTimeout:       backendIdleTimeout,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
	}
}

// s3Error answers a request Garage did not.
func (p *Proxy) s3Error(w http.ResponseWriter, r *http.Request, err error) {
	if clientGone(r) {
		w.WriteHeader(statusClientClosedRequest)
		return
	}
	p.Log.Warn("Garage behind the terminator did not answer", logger.Err(err), "method", r.Method)
	w.WriteHeader(http.StatusBadGateway)
}

// clientGone reports whether a failed request failed because its client went
// away: the request was cancelled, or its body stopped arriving.
func clientGone(r *http.Request) bool {
	if r.Context().Err() != nil {
		return true
	}
	b, ok := r.Body.(*countingBody)
	return ok && b.failed.Load()
}

// s3Handler counts each request against its bucket and passes it to Garage.
type s3Handler struct {
	proxy      *httputil.ReverseProxy
	rootDomain string
	buckets    *Buckets
	metrics    *Metrics
}

func (h *s3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, namespace, name := h.buckets.Labels(bucketOf(r, h.rootDomain))
	bm := h.metrics.bucket(bucket, namespace, name)
	if r.Body != nil {
		r.Body = &countingBody{ReadCloser: r.Body, c: bm.received}
	}
	rec := &recorder{ResponseWriter: w, sent: bm.sent}
	start := time.Now()
	// Deferred, because ReverseProxy ends a response it cannot finish — the
	// client went away halfway through a download — with a panic.
	defer func() {
		if h.metrics == nil {
			return
		}
		bm.duration.Observe(time.Since(start).Seconds())
		h.metrics.bucketRequests.WithLabelValues(bucket, namespace, name, methodLabel(r.Method), statusClass(rec.status())).Inc()
	}()
	h.proxy.ServeHTTP(rec, r)
}

// bucketOf is the bucket a request is for, read the way Garage reads it: from
// the Host header when it is a name under rootDomain (virtual-host addressing),
// otherwise from the first segment of the path (path-style). It is "" for a
// request that names no bucket, such as ListBuckets on "/".
func bucketOf(r *http.Request, rootDomain string) string {
	if rootDomain != "" {
		host := strings.ToLower(r.Host)
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if b, ok := strings.CutSuffix(host, strings.ToLower(rootDomain)); ok && b != "" {
			return b
		}
	}
	bucket, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return bucket
}

// methodLabel bounds the method label to the methods S3 uses: the method is
// whatever the client wrote, and a label must not be.
func methodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodOptions:
		return method
	}
	return "other"
}

// statusClass is the code label: "2xx", "4xx" and so on, with 403 on its own.
//
// A 403 is a key refused: one that expired, was revoked by a migration's write
// freeze, or was issued for another bucket. Mixed into 4xx it would drown among
// the 404s every HEAD of a missing object answers, and it is the one an
// operator looks for when a consumer stops writing.
func statusClass(code int) string {
	if code < 100 || code > 599 {
		return "other"
	}
	if code == http.StatusForbidden {
		return "403"
	}
	return strconv.Itoa(code/100) + "xx"
}

// countingBody counts a request body as Garage reads it, and remembers whether
// reading it failed: a body that stops halfway is the client's doing.
type countingBody struct {
	io.ReadCloser
	c      prometheus.Counter
	failed atomic.Bool
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	add(b.c, n)
	if err != nil && !errors.Is(err, io.EOF) {
		b.failed.Store(true)
	}
	return n, err
}

// recorder counts a response body as it is written and remembers its status.
// Unwrap lets ReverseProxy reach the flusher and the hijacker underneath.
type recorder struct {
	http.ResponseWriter
	sent prometheus.Counter
	code int
}

func (w *recorder) WriteHeader(code int) {
	if w.code == 0 && code >= 200 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *recorder) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	add(w.sent, n)
	return n, err
}

func (w *recorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *recorder) status() int {
	if w.code == 0 {
		return http.StatusOK
	}
	return w.code
}

// countedConn counts a connection's plaintext both ways.
type countedConn struct {
	net.Conn
	received, sent prometheus.Counter
}

func (c *countedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	add(c.received, n)
	return n, err
}

func (c *countedConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	add(c.sent, n)
	return n, err
}

// connListener is a net.Listener whose connections are handed to it rather than
// accepted: the HTTP server's view of the route's own accept loop.
type connListener struct {
	addr  net.Addr
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newConnListener(addr net.Addr) *connListener {
	return &connListener{addr: addr, conns: make(chan net.Conn), done: make(chan struct{})}
}

func (l *connListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *connListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *connListener) Addr() net.Addr { return l.addr }

// deliver hands c to Accept, or closes it once the listener is closed.
func (l *connListener) deliver(c net.Conn) {
	select {
	case l.conns <- c:
	case <-l.done:
		_ = c.Close()
	}
}

// debugWriter is the HTTP server's and the reverse proxy's error log. What they
// log is a client's malformed request or a connection reset — the HTTP form of
// a failed handshake, and not logged above debug for the same reason.
type debugWriter struct{ log logger.Logger }

func (w debugWriter) Write(p []byte) (int, error) {
	w.log.Debug(strings.TrimSpace(string(p)))
	return len(p), nil
}

// add adds n to c; a nil counter counts nothing.
func add(c prometheus.Counter, n int) {
	if c != nil && n > 0 {
		c.Add(float64(n))
	}
}
