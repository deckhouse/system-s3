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

package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// testLogger discards nothing but writes to the test's own output, so a failing
// test carries the manager's log with it.
func testLogger(t *testing.T) logger.Logger {
	t.Helper()
	l, err := logger.NewLoggerToWriter(&testWriter{t: t}, logger.ErrorLevel)
	if err != nil {
		t.Fatalf("build logger: %v", err)
	}
	return l
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// countingDriver converges slowly and counts how many convergences were started.
type countingDriver struct {
	started atomic.Int32

	// release blocks the convergence until the test lets it finish, so every
	// caller in the test arrives while the first one is still inside.
	release chan struct{}

	// entered is closed once the first convergence is inside EnsureStore.
	entered   chan struct{}
	enterOnce sync.Once
}

func (d *countingDriver) EnsureStore(_ context.Context, _ *backend.Store) (backend.StoreState, error) {
	d.started.Add(1)
	d.enterOnce.Do(func() { close(d.entered) })
	<-d.release
	return backend.StoreState{Ready: true, Message: "converged"}, nil
}

// TestConcurrentEnsureCollapsesIntoOneConvergence pins the shape of the
// serialization: callers that arrive while a convergence is running share its
// result instead of queueing for one of their own.
//
// Both properties matter. Two convergences at once would pin the replication
// factor, recycle PVCs and rewrite the layout in parallel; N convergences in
// sequence would make each SystemBucket reconcile wait for all the others, and
// raising MAX_CONCURRENT_RECONCILES would multiply the work rather than the
// throughput.
func TestConcurrentEnsureCollapsesIntoOneConvergence(t *testing.T) {
	drv := &countingDriver{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
	m := &StoreManager{
		driver: drv,
		store:  &backend.Store{Name: backend.StoreName},
		log:    testLogger(t),
	}

	const callers = 8
	var wg sync.WaitGroup
	states := make([]backend.StoreState, callers)
	errs := make([]error, callers)

	wg.Add(1)
	go func() {
		defer wg.Done()
		states[0], errs[0] = m.Ensure(context.Background())
	}()

	// Everyone else joins while the first convergence is inside the driver.
	select {
	case <-drv.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first convergence never reached the driver")
	}
	for i := 1; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			states[i], errs[i] = m.Ensure(context.Background())
		}(i)
	}

	// Give the late callers time to either join the flight or start their own.
	time.Sleep(200 * time.Millisecond)
	close(drv.release)
	wg.Wait()

	if got := drv.started.Load(); got != 1 {
		t.Errorf("the driver converged %d times, want 1: concurrent callers must share one convergence", got)
	}
	for i := range states {
		if errs[i] != nil {
			t.Errorf("caller %d: unexpected error: %v", i, errs[i])
		}
		if !states[i].Ready || states[i].Message != "converged" {
			t.Errorf("caller %d: state=%+v, want the shared converged state", i, states[i])
		}
	}
}
