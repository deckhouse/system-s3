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
// is this code rather than an nginx or an envoy for two reasons: it is one
// binary the module already builds, scans and ships, so the data plane gains no
// component to keep patched; and it does not need to understand HTTP at all.
//
// It works at the TCP level. A connection is accepted, the TLS handshake is
// done, and the plaintext is copied to Garage on the loopback interface and
// back. Nothing is parsed, so nothing about S3 can be broken by it: the Host
// header a SigV4 signature covers, chunked uploads, presigned URLs, range reads
// and long streams all pass through byte for byte. Garage itself listens on
// 127.0.0.1 only, so this is the only way into it from outside the pod.
package tlsproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
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
)

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
type Proxy struct {
	Routes   []Route
	CertFile string
	KeyFile  string
	Log      logger.Logger
}

// Run serves every route until ctx is cancelled or a listener fails.
//
// A listener that fails brings the whole process down rather than leaving the
// other one serving. The pod then restarts the sidecar, and Kubernetes sees it:
// half a terminator — S3 answering, the admin API not — would pass the readiness
// probe on one port while the controller could not manage the node on the other.
func (p *Proxy) Run(ctx context.Context) error {
	certs := &certLoader{certFile: p.CertFile, keyFile: p.KeyFile}
	if _, err := certs.get(); err != nil {
		return err
	}
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return certs.get() },
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
		wg.Add(1)
		go func(r Route, ln net.Listener) {
			defer wg.Done()
			errs <- p.serve(ctx, ln, r.Backend, cfg)
		}(r, ln)
		go func(ln net.Listener) {
			<-ctx.Done()
			_ = ln.Close()
		}(ln)
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

func (p *Proxy) serve(ctx context.Context, ln net.Listener, backend string, cfg *tls.Config) error {
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
		go p.handle(conn, backend, cfg)
	}
}

func (p *Proxy) handle(raw net.Conn, backend string, cfg *tls.Config) {
	defer raw.Close()

	client := tls.Server(raw, cfg)
	_ = raw.SetDeadline(time.Now().Add(handshakeTimeout))
	if err := client.Handshake(); err != nil {
		// Not logged above debug: a port scanner, a kubelet TCP check or a client
		// that does not trust the CA each produce one of these, and none of them
		// is something an operator can act on from this side.
		p.Log.Debug("TLS handshake failed", logger.Err(err), slog.String("remote", raw.RemoteAddr().String()))
		return
	}
	_ = raw.SetDeadline(time.Time{})

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

	pipe(client, upstream)
}

// halfCloser is a connection whose write side can be closed on its own:
// *net.TCPConn does it with a FIN, *tls.Conn with a close_notify alert.
type halfCloser interface {
	net.Conn
	CloseWrite() error
}

// pipe copies both ways until both directions have finished.
//
// Each direction closes the write side of its destination when its source is
// done, instead of tearing the whole connection down. A request whose client has
// finished sending may still be receiving its response, and closing both halves
// at the first EOF would cut that response off.
func pipe(a, b halfCloser) {
	var wg sync.WaitGroup
	wg.Add(2)
	cp := func(dst, src halfCloser) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		_ = dst.CloseWrite()
	}
	go cp(a, b)
	go cp(b, a)
	wg.Wait()
}

// certLoader serves the certificate from files that the kubelet replaces when
// the certificate hook renews them.
//
// The files are stat'ed on every handshake and re-read when either changed. A
// stat is a cheap syscall against a handshake that costs far more, and the
// alternative — reading them once — would keep serving a certificate the hook has
// already replaced, until it expired in the middle of the night.
type certLoader struct {
	certFile, keyFile string

	mu      sync.Mutex
	certSig string
	cert    *tls.Certificate
}

func (l *certLoader) get() (*tls.Certificate, error) {
	sig, err := fileSignature(l.certFile, l.keyFile)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cert != nil && sig == l.certSig {
		return l.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(l.certFile, l.keyFile)
	if err != nil {
		if l.cert != nil {
			// Mid-swap: the kubelet writes the pair as one atomic symlink flip, but
			// a handshake that lands between two files of different generations
			// would see a mismatch. Keep serving what worked; the next handshake
			// reads again.
			return l.cert, nil
		}
		return nil, fmt.Errorf("load the TLS certificate: %w", err)
	}
	l.cert, l.certSig = &cert, sig
	return l.cert, nil
}

func fileSignature(paths ...string) (string, error) {
	var b strings.Builder
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", p, err)
		}
		fmt.Fprintf(&b, "%s:%d:%d;", p, st.Size(), st.ModTime().UnixNano())
	}
	return b.String(), nil
}
