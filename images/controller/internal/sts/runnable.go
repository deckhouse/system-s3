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
	"crypto/tls"
	"errors"
	"net/http"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

// readHeaderTimeout bounds how long a client may take to send its request
// headers: the endpoint is reachable by every Pod in the cluster, so a connection
// that opens and never speaks must not be able to accumulate.
const readHeaderTimeout = 10 * time.Second

// shutdownGrace is how long an in-flight credentials request has to finish when
// the process is going down.
const shutdownGrace = 5 * time.Second

// Runnable serves the endpoint for the controller-runtime manager.
type Runnable struct {
	Server   *Server
	Addr     string
	CertFile string
	KeyFile  string
	Log      logger.Logger
}

// NeedLeaderElection reports false: this is not reconciliation.
//
// The endpoint answers a caller's request in the moment; a replica that is not
// the leader can do that as well as the leader can, and a replica that waits for
// leadership before it starts listening is a replica whose readiness probe passes
// while it refuses connections. (The module runs one controller replica today,
// which is exactly why this must not be tied to an election it would always win
// late.)
func (r *Runnable) NeedLeaderElection() bool { return false }

// Start serves until the context is cancelled.
func (r *Runnable) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:              r.Addr,
		Handler:           r.Server.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		// Not the Go default, which is a property of the toolchain rather than a
		// decision of this module. What travels over this connection is a secret
		// key, so the floor is stated.
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}

	done := make(chan error, 1)
	go func() {
		r.Log.Info("serving the credentials endpoint", "address", r.Addr)
		err := srv.ListenAndServeTLS(r.CertFile, r.KeyFile)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			r.Log.Error("the credentials endpoint did not shut down cleanly", logger.Err(err))
		}
		return nil
	}
}
