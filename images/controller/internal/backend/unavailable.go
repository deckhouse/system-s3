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
	"errors"
	"time"
)

// UnavailableError is a refusal that will not last: the store cannot do what
// was asked now, and can be asked again after RetryAfter.
//
// It exists for the credentials endpoint, which answers it with 503 and a
// Retry-After instead of a 500. A consumer's SDK retries a 503 on its own, and
// the header tells it when; a 500 reads as a fault on the server that someone
// has to look at, when what happened is a switch to a new cluster, or a replica
// that went away and that the others have not given up on yet.
type UnavailableError struct {
	RetryAfter time.Duration
	Err        error
}

func (e *UnavailableError) Error() string { return e.Err.Error() }

func (e *UnavailableError) Unwrap() error { return e.Err }

// Unavailable marks err as a refusal that will pass in about retryAfter.
func Unavailable(retryAfter time.Duration, err error) error {
	return &UnavailableError{RetryAfter: retryAfter, Err: err}
}

// AsUnavailable reports whether err, or anything it wraps, is an
// UnavailableError, and returns it.
func AsUnavailable(err error) (*UnavailableError, bool) {
	var u *UnavailableError
	if errors.As(err, &u) {
		return u, true
	}
	return nil, false
}

// ErrLimitExceeded is a refusal the caller brought on itself: it holds as many
// unexpired temporary credentials for a bucket as the store issues, and asking
// again will not help until some of them expire.
//
// The credentials endpoint answers it with 400 and the code LimitExceeded, not
// a 500 — nothing is wrong with the store — and not a 503 or a 429, which an SDK
// would retry on its own into the same refusal.
var ErrLimitExceeded = errors.New("too many unexpired temporary credentials")
