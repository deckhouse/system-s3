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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
)

func RetryOnConflict(backoff wait.Backoff, fn func() error) error {
	var attemptErr error
	err := retry.RetryOnConflict(backoff, func() error {
		attemptErr = fn()
		return attemptErr
	})
	if err == nil {
		err = attemptErr
	}
	return err
}
