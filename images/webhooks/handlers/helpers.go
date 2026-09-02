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

package handlers

import (
	kwhvalidating "github.com/slok/kubewebhook/v2/pkg/webhook/validating"
)

// reject returns a deny ValidatorResult with the supplied message.
// Centralised so every webhook in this package surfaces consistent
// rejection text without per-validator boilerplate.
func reject(message string) *kwhvalidating.ValidatorResult {
	return &kwhvalidating.ValidatorResult{
		Valid:   false,
		Message: message,
	}
}
