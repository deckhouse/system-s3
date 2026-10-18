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
	"sync/atomic"
	"testing"
	"time"

	"github.com/deckhouse/system-s3/images/controller/internal/backend"
)

// slowUploadsDriver holds every sweep of the unfinished uploads until the test
// releases it, the way Garage holds a CleanupIncompleteUploads call on a bucket
// with millions of deleted objects.
type slowUploadsDriver struct {
	countingDriver
	uploads atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (d *slowUploadsDriver) SweepIncompleteUploads(context.Context, *backend.Store) (int, error) {
	d.uploads.Add(1)
	d.entered <- struct{}{}
	<-d.release
	return 0, nil
}

// TestUploadSweepNeitherBlocksNorOverlaps: a sweep that takes minutes does not
// hold the loop that converges the store, a tick that comes while it runs does
// not start a second one beside it, and the next one waits uploadSweepInterval.
func TestUploadSweepNeitherBlocksNorOverlaps(t *testing.T) {
	drv := &slowUploadsDriver{entered: make(chan struct{}, 4), release: make(chan struct{})}
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	m := &StoreManager{
		driver: drv,
		store:  &backend.Store{Name: backend.StoreName},
		log:    testLogger(t),
		now:    func() time.Time { return now },
	}

	returned := make(chan struct{})
	go func() {
		m.sweep(context.Background())
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep waited for the unfinished uploads")
	}
	<-drv.entered

	now = now.Add(uploadSweepInterval + time.Minute)
	m.sweep(context.Background())
	if got := drv.uploads.Load(); got != 1 {
		t.Fatalf("a second upload sweep started while the first ran: %d", got)
	}

	close(drv.release)
	waitIdle := func() {
		for m.uploadSweeping.Load() {
			time.Sleep(time.Millisecond)
		}
	}
	waitIdle()

	now = now.Add(sweepInterval)
	m.sweep(context.Background())
	<-drv.entered
	waitIdle()
	if got := drv.uploads.Load(); got != 2 {
		t.Fatalf("no upload sweep once the previous one finished and uploadSweepInterval passed: %d", got)
	}

	now = now.Add(time.Hour)
	m.sweep(context.Background())
	waitIdle()
	if got := drv.uploads.Load(); got != 2 {
		t.Fatalf("an upload sweep started an hour after the last: %d", got)
	}
}
