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

// Package tlsproxy is the data plane's TLS terminator: the controller binary,
// started in the Garage pod as a sidecar, in front of Garage's S3 and admin
// listeners.
//
// Garage has no TLS of its own — its listeners take an address and nothing else,
// and its documentation says to put a reverse proxy in front for TLS. The proxy
// is this code rather than an nginx or an envoy because it is one binary the
// module already builds, scans and ships, so the data plane gains no component
// to keep patched.
//
// The admin API is carried at the TCP level. A connection is accepted, the TLS
// handshake is done, and the plaintext is copied to Garage on the loopback
// interface and back. Nothing is parsed, so nothing can be broken by it.
//
// S3 is carried as HTTP (s3http.go), because the one thing an operator asks of
// the traffic that TCP cannot answer — which bucket it was for — is inside the
// request, and Garage counts nothing per bucket. Reading the request is all the
// proxy does with it: the Host header and the request target a SigV4 signature
// covers go to Garage as the client sent them, and bodies are streamed, not
// buffered. EnvS3HTTP puts S3 back on the TCP copy if that ever has to be ruled
// out. Garage itself listens on 127.0.0.1 only, so the terminator is the only
// way into it from outside the pod.
package tlsproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/servingcert"
)

// Command is the argument that starts the controller binary as the terminator.
const Command = "tls-terminate"

// The environment the terminator is configured with. The driver sets it on the
// sidecar; nothing else does.
const (
	// EnvRoutes is a comma-separated list of listen=backend pairs, for example
	// "[::]:3900=127.0.0.1:3910,[::]:3903=127.0.0.1:3913".
	EnvRoutes   = "TLS_ROUTES"
	EnvCertFile = "TLS_CERT_FILE"
	EnvKeyFile  = "TLS_KEY_FILE"

	// EnvS3Listen is the listen address, one of EnvRoutes, that carries S3 and
	// is served as HTTP; empty means every route is a TCP copy.
	EnvS3Listen = "TLS_S3_LISTEN"
	// EnvS3HTTP set to "false" serves the S3 route as a TCP copy, the way it was
	// served before the terminator read requests: no per-bucket metrics, and
	// nothing between the client and Garage that could be suspected of changing
	// a request. Anything else, including unset, is the HTTP proxy.
	EnvS3HTTP = "TLS_S3_HTTP"
	// EnvS3RootDomain is Garage's s3_api.root_domain: a Host under it names the
	// bucket (virtual-host addressing), the way Garage reads it.
	EnvS3RootDomain = "TLS_S3_ROOT_DOMAIN"
	// EnvBucketsDir is the directory of the bucket allowlist (see buckets.go).
	EnvBucketsDir = "TLS_BUCKETS_DIR"
)

// S3ListenFromEnv is the S3 route to serve as HTTP, as getenv configures it:
// EnvS3Listen, unless EnvS3HTTP turns the HTTP proxy off.
func S3ListenFromEnv(getenv func(string) string) string {
	if getenv(EnvS3HTTP) == "false" {
		return ""
	}
	return getenv(EnvS3Listen)
}

const (
	// handshakeTimeout bounds how long a client may take to finish the TLS
	// handshake. The listener is reachable by every pod that may reach the
	// Service, so a connection that opens and never speaks must not be able to
	// accumulate.
	handshakeTimeout = 10 * time.Second

	// dialTimeout bounds the connection to Garage on the loopback. Anything but
	// immediate means Garage is not listening yet (the pod is starting) or has
	// died; either way the client is better off failing than waiting.
	dialTimeout = 5 * time.Second
)

// Route is one listener and the Garage address it forwards to.
type Route struct {
	Listen  string
	Backend string
}

// ParseRoutes reads the EnvRoutes format.
func ParseRoutes(raw string) ([]Route, error) {
	var routes []Route
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		listen, backend, ok := strings.Cut(pair, "=")
		if !ok || listen == "" || backend == "" {
			return nil, fmt.Errorf("route %q is not listen=backend", pair)
		}
		routes = append(routes, Route{Listen: listen, Backend: backend})
	}
	if len(routes) == 0 {
		return nil, errors.New("no routes configured")
	}
	return routes, nil
}

// Proxy terminates TLS on every route.
//
// S3Listen is the route served as HTTP (see EnvS3Listen); S3RootDomain and
// Buckets are what it names buckets by. Metrics, when set, count the traffic of
// every route, and MetricsListen is where they are served (see EnvMetricsListen).
type Proxy struct {
	Routes   []Route
	CertFile string
	KeyFile  string
	Log      logger.Logger

	S3Listen     string
	S3RootDomain string
	Buckets      *Buckets

	Metrics       *Metrics
	MetricsListen string
	MetricsToken  string
}

// Run serves every route until ctx is cancelled or a listener fails.
//
// A listener that fails brings the whole process down rather than leaving the
// other one serving. The pod then restarts the sidecar, and Kubernetes sees it:
// half a terminator — S3 answering, the admin API not — would pass the readiness
// probe on one port while the controller could not manage the node on the other.
func (p *Proxy) Run(ctx context.Context) error {
	certs := servingcert.New(p.CertFile, p.KeyFile)
	if _, err := certs.Certificate(); err != nil {
		return err
	}
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: certs.GetCertificate,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var lc net.ListenConfig
	errs := make(chan error, len(p.Routes))
	var wg sync.WaitGroup
	for _, r := range p.Routes {
		ln, err := lc.Listen(ctx, "tcp", r.Listen)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", r.Listen, err)
		}
		p.Log.Info("terminating TLS", "listen", r.Listen, "backend", r.Backend)
		lm := p.Metrics.listener(listenPort(r.Listen))
		serve := func(client *tls.Conn) { p.pipeTo(client, r.Backend, lm) }
		if p.S3Listen != "" && r.Listen == p.S3Listen {
			s3 := p.startS3(r.Backend, ln.Addr(), lm)
			defer s3.Close()
			serve = s3.serve
		}
		wg.Add(1)
		go func(ln net.Listener) {
			defer wg.Done()
			errs <- p.serve(ctx, ln, cfg, lm, serve)
		}(ln)
		go func(ln net.Listener) {
			<-ctx.Done()
			_ = ln.Close()
		}(ln)
	}

	if p.Metrics != nil && p.MetricsListen != "" {
		p.serveMetrics(ctx, &lc, cfg)
	}

	var first error
	select {
	case <-ctx.Done():
	case first = <-errs:
		cancel()
	}
	wg.Wait()
	if ctx.Err() != nil && first == nil {
		return nil
	}
	return first
}

// serve accepts connections on ln and gives each one, once its handshake is
// done, to handle.
func (p *Proxy) serve(ctx context.Context, ln net.Listener, cfg *tls.Config, lm *listenerMetrics, handle func(*tls.Conn)) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return fmt.Errorf("accept on %s: %w", ln.Addr(), err)
		}
		go func() {
			if client := p.handshake(conn, cfg, lm); client != nil {
				handle(client)
			}
		}()
	}
}

// handshake does the TLS handshake on raw and returns the connection, or nil
// after closing raw when the handshake fails.
func (p *Proxy) handshake(raw net.Conn, cfg *tls.Config, lm *listenerMetrics) *tls.Conn {
	client := tls.Server(raw, cfg)
	_ = raw.SetDeadline(time.Now().Add(handshakeTimeout))
	if err := client.Handshake(); err != nil {
		// Not logged above debug: a port scanner, a kubelet TCP check or a client
		// that does not trust the CA each produce one of these, and none of them
		// is something an operator can act on from this side.
		p.Log.Debug("TLS handshake failed", logger.Err(err), slog.String("remote", raw.RemoteAddr().String()))
		_ = raw.Close()
		return nil
	}
	_ = raw.SetDeadline(time.Time{})
	if lm != nil {
		lm.connections.Inc()
	}
	return client
}

// pipeTo copies the client's connection to backend and back, byte for byte.
func (p *Proxy) pipeTo(client *tls.Conn, backend string, lm *listenerMetrics) {
	defer client.NetConn().Close()

	conn, err := net.DialTimeout("tcp", backend, dialTimeout)
	if err != nil {
		p.Log.Error("unable to reach Garage behind the terminator", logger.Err(err), "backend", backend)
		return
	}
	defer conn.Close()
	upstream, ok := conn.(*net.TCPConn)
	if !ok {
		p.Log.Error("the connection to Garage is not TCP", "backend", backend)
		return
	}

	var received, sent prometheus.Counter
	if lm != nil {
		received, sent = lm.received, lm.sent
	}
	pipe(client, upstream, sent, received)
}

// halfCloser is a connection whose write side can be closed on its own:
// *net.TCPConn does it with a FIN, *tls.Conn with a close_notify alert.
type halfCloser interface {
	net.Conn
	CloseWrite() error
}

// pipe copies both ways until both directions have finished, adding what goes
// from b to a to toA and what goes from a to b to toB; a nil counter counts
// nothing.
//
// Each direction closes the write side of its destination when its source is
// done, instead of tearing the whole connection down. A request whose client has
// finished sending may still be receiving its response, and closing both halves
// at the first EOF would cut that response off.
func pipe(a, b halfCloser, toA, toB prometheus.Counter) {
	var wg sync.WaitGroup
	wg.Add(2)
	cp := func(dst, src halfCloser, c prometheus.Counter) {
		defer wg.Done()
		var w io.Writer = dst
		if c != nil {
			w = counting{Writer: dst, c: c}
		}
		_, _ = io.Copy(w, src)
		_ = dst.CloseWrite()
	}
	go cp(a, b, toA)
	go cp(b, a, toB)
	wg.Wait()
}

// serveMetrics serves the counters on MetricsListen until ctx is done.
//
// Its failure is logged, not returned: a terminator that cannot report its
// traffic still carries it, and taking S3 down over a metrics port would be the
// wrong way round.
func (p *Proxy) serveMetrics(ctx context.Context, lc *net.ListenConfig, cfg *tls.Config) {
	ln, err := lc.Listen(ctx, "tcp", p.MetricsListen)
	if err != nil {
		p.Log.Error("unable to serve the terminator's metrics", logger.Err(err), "listen", p.MetricsListen)
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", p.Metrics.Handler(p.MetricsToken))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: handshakeTimeout}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		if err := srv.Serve(tls.NewListener(ln, cfg)); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.Log.Error("the terminator's metrics stopped", logger.Err(err), "listen", p.MetricsListen)
		}
	}()
	p.Log.Info("serving the terminator's metrics", "listen", p.MetricsListen)
}

// listenPort is the port of a listen address, the label the counters carry.
func listenPort(listen string) string {
	if _, port, err := net.SplitHostPort(listen); err == nil {
		return port
	}
	return listen
}
