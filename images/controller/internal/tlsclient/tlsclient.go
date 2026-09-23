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

// Package tlsclient is how everything in this module that talks to Garage —
// the controller's admin and S3 clients, the migration Job's copier — verifies
// that it is talking to Garage.
//
// Garage serves no TLS of its own (its S3 and admin listeners take an address
// and nothing else), so the data plane terminates TLS in a sidecar with a
// certificate the module issues from its own CA. Nothing in the cluster trusts
// that CA by default, which is why every client here is handed it explicitly
// rather than falling back to the system roots, where it would fail closed —
// and why there is no "skip verification" switch: a client that does not verify
// is a client that hands an admin token or a secret key to whatever answered.
package tlsclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// CA is a CA bundle read from a file that may be replaced underneath us.
//
// The file is a mounted Secret. The certificate hook renews the CA long before
// it expires, the kubelet swaps the file, and a process that read it once at
// start would keep verifying against the old CA until it was restarted — which
// for the controller means until the next rollout, and for a rotation that is
// the wrong way round: the server has the new certificate, the client refuses
// it. So the file is re-read whenever it changes.
type CA struct {
	path string

	mu    sync.Mutex
	mtime time.Time
	size  int64
	pool  *x509.CertPool
}

// NewCA returns a CA backed by the PEM file at path. The file is read lazily.
func NewCA(path string) *CA {
	return &CA{path: path}
}

// Path is the file the CA is read from.
func (c *CA) Path() string { return c.path }

// Pool returns the certificates in the file, re-reading it when its size or
// modification time changed since the last read.
func (c *CA) Pool() (*x509.CertPool, error) {
	if c == nil || c.path == "" {
		return nil, errors.New("no CA file configured for the Garage endpoint")
	}
	st, err := os.Stat(c.path)
	if err != nil {
		return nil, fmt.Errorf("read the Garage CA: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pool != nil && st.ModTime().Equal(c.mtime) && st.Size() == c.size {
		return c.pool, nil
	}

	pem, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("read the Garage CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("the Garage CA file %s holds no PEM certificate", c.path)
	}
	c.pool, c.mtime, c.size = pool, st.ModTime(), st.Size()
	return pool, nil
}

// Config is a client TLS configuration that verifies the peer against this CA,
// for the given name.
//
// serverName is set explicitly rather than taken from the URL, because the
// controller reaches each Garage pod's admin API by the pod's IP — the only way
// to ask one particular node — and a certificate cannot name addresses that are
// assigned at scheduling time. It names the endpoint Service instead, and every
// connection is verified against that name whatever address it was dialled at.
func (c *CA) Config(serverName string) (*tls.Config, error) {
	pool, err := c.Pool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// Transport is an HTTP transport that verifies Garage with this CA.
//
// A fresh transport per call is deliberate: the callers build a client per
// operation, operations are seconds apart at the fastest, and a transport that
// outlived a CA rotation would go on refusing the new certificate.
func (c *CA) Transport(serverName string) (*http.Transport, error) {
	cfg, err := c.Config(serverName)
	if err != nil {
		return nil, err
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = cfg
	return t, nil
}
