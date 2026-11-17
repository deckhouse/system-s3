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

package backend

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
)

func TestRetryOnConflictReportsAContextErrorOfTheAttempt(t *testing.T) {
	err := RetryOnConflict(retry.DefaultRetry, func() error { return context.DeadlineExceeded })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestRetryOnConflictRetriesAConflict(t *testing.T) {
	calls := 0
	err := RetryOnConflict(retry.DefaultRetry, func() error {
		calls++
		if calls == 1 {
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "x", errors.New("modified"))
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err = %v, calls = %d, want nil after 2 calls", err, calls)
	}
}
