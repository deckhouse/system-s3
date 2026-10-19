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

// Package servingcert is how every TLS listener in this module — the data
// plane's terminator and the controller's credentials endpoint — serves its
// certificate.
//
// Both certificates are mounted Secrets that a certificate hook renews and the
// kubelet swaps underneath the running process. Nothing restarts the process
// when that happens: the controller Deployment carries no checksum of the
// certificate, and the Garage pods must not be rolled for a renewal. A listener
// that read its pair once at start would go on serving the old certificate
// until it expired, so the pair is handed to crypto/tls through GetCertificate
// and re-read when the files change.
//
// It is the server-side counterpart of tlsclient.CA, which does the same for the
// CA bundle the clients verify against.
package servingcert

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"sync"
)

// KeyPair is a certificate and key served from files that may be replaced
// underneath us.
//
// The files are stat'ed on every handshake and re-read when either changed. A
// stat is a cheap syscall against a handshake that costs far more, and the
// alternative — reading them once — would keep serving a certificate the hook has
// already replaced, until it expired in the middle of the night.
type KeyPair struct {
	certFile, keyFile string

	mu      sync.Mutex
	certSig string
	cert    *tls.Certificate
}

// New returns a KeyPair backed by the PEM files at certFile and keyFile. The
// files are read lazily; call Certificate once to fail early on a missing or
// broken pair.
func New(certFile, keyFile string) *KeyPair {
	return &KeyPair{certFile: certFile, keyFile: keyFile}
}

// Certificate returns the pair from the files, re-reading them when the size or
// modification time of either changed since the last read.
func (k *KeyPair) Certificate() (*tls.Certificate, error) {
	sig, err := fileSignature(k.certFile, k.keyFile)
	if err != nil {
		return nil, err
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if k.cert != nil && sig == k.certSig {
		return k.cert, nil
	}
	cert, err := tls.LoadX509KeyPair(k.certFile, k.keyFile)
	if err != nil {
		if k.cert != nil {
			// Mid-swap: the kubelet writes the pair as one atomic symlink flip, but
			// a handshake that lands between two files of different generations
			// would see a mismatch. Keep serving what worked; the next handshake
			// reads again.
			return k.cert, nil
		}
		return nil, fmt.Errorf("load the TLS certificate: %w", err)
	}
	k.cert, k.certSig = &cert, sig
	return k.cert, nil
}

// GetCertificate is Certificate in the shape tls.Config.GetCertificate takes.
func (k *KeyPair) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return k.Certificate()
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
