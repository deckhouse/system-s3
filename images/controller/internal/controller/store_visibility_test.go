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
	"testing"
	"time"
)

// A store that never comes up has to keep saying so, and not every thirty
// seconds.
//
// It has no custom resource of its own, so on a cluster with no SystemBucket —
// the normal state of a fresh installation — the log is the only channel its
// message can reach. Logging only readiness transitions meant it said so at most
// once and then went quiet forever, and the first consumer to create a
// SystemBucket a week later was the first to learn why. Repeating every pass
// would trade one problem for another.
func TestANeverReadyStoreRepeatsItselfButNotEveryPass(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	for _, tc := range []struct {
		name    string
		lastMsg string
		lastAt  time.Time
		msg     string
		now     time.Time
		wantLog bool
	}{
		{
			// Nothing said yet: the first pass has to speak.
			name: "nothing logged yet", lastMsg: "", lastAt: time.Time{},
			msg: "waiting for PVs", now: base, wantLog: true,
		},
		{
			// New information, whatever the rate.
			name: "the reason changed", lastMsg: "waiting for PVs", lastAt: base,
			msg: "admin API not answering", now: base.Add(time.Second), wantLog: true,
		},
		{
			// A reminder, and too soon for one.
			name: "same reason, moments later", lastMsg: "waiting for PVs", lastAt: base,
			msg: "waiting for PVs", now: base.Add(30 * time.Second), wantLog: false,
		},
		{
			name: "same reason, just under the interval", lastMsg: "waiting for PVs", lastAt: base,
			msg: "waiting for PVs", now: base.Add(notReadyLogInterval - time.Second), wantLog: false,
		},
		{
			name: "same reason, the interval has passed", lastMsg: "waiting for PVs", lastAt: base,
			msg: "waiting for PVs", now: base.Add(notReadyLogInterval), wantLog: true,
		},
		{
			// An empty message is still a state worth repeating: it means the store
			// is not ready and said nothing about why, which is itself the news.
			name: "no message at all", lastMsg: "waiting for PVs", lastAt: base,
			msg: "", now: base.Add(time.Second), wantLog: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &StoreManager{
				lastNotReadyMsg: tc.lastMsg,
				lastNotReadyAt:  tc.lastAt,
				now:             func() time.Time { return tc.now },
			}
			if got := m.shouldRepeatNotReady(tc.msg); got != tc.wantLog {
				t.Fatalf("shouldRepeatNotReady(%q) = %v, want %v", tc.msg, got, tc.wantLog)
			}
		})
	}
}

// The clock falls back to time.Now when no test replaced it — the production
// path must not depend on a field only tests set.
func TestTheClockDefaultsToWallTime(t *testing.T) {
	m := &StoreManager{}
	before := time.Now()
	got := m.clock()
	if got.Before(before.Add(-time.Second)) || got.After(time.Now().Add(time.Second)) {
		t.Fatalf("clock() = %v, want something around now", got)
	}
}
