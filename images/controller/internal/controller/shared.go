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

// Package controller holds the sds-object-system reconcilers. Each reconciler
// drives its CR through a small condition-based FSM: a fixed list of stage
// conditions is advanced in order, downstream stages are gated when an upstream
// stage is not Ready, and the aggregate Ready condition plus the coarse Phase are
// derived from the stage conditions. The backend-specific work behind each stage
// lives in the garage driver, and the data plane itself — which has no CR — is
// converged by StoreManager.
//
// The FSM itself is not here: conditions.Stages is the state machine and
// conditions.Builder the scratch space a pass accumulates its verdicts in. Both
// were copied into this module and into sds-object before the shared library grew
// them, which is how the copies drifted. What stays here is this module's own
// vocabulary — the stage order, and the aggregate the FSM gates.
package controller

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/deckhouse/sds-common-lib/conditions"
)

// Finalizer is placed on every SystemBucket the controller reconciles, so the
// bucket and its access key are removed from the backend before the CR
// disappears.
const Finalizer = "storage.deckhouse.io/sds-object-system-controller"

// conditionReady is the aggregate readiness condition type
// (SystemBucketConditionReady), so the shared FSM gates it by this single
// invariant.
const conditionReady = "Ready"

// stageVocabulary declares what these reconcilers share: the aggregate
// condition type. Everything else is left at the shared library's defaults, so
// the stages are reported with the reasons every other storage module reports
// them with — Reconciled, ReconcileFailed, Pending and WaitingForDependency.
//
// Reasons a stage publishes for itself are a separate matter and stay:
// NamespaceNotPlatform names something this module knows and no shared
// vocabulary can say.
//
// SkipMissing is deliberately left off, so a stage carrying no condition
// aggregates to Unknown and the phase reads Pending. A stage that has never been
// evaluated is not evidence that the resource is healthy, and answering Ready on
// a set of verdicts that is not complete is how a resource gets called usable on
// nobody's word.
//
// It costs nothing today: every path that flushes the status has written the
// whole stage set, because the FSM follows every stage that does not pass and
// writes the ones below it. The reading only differs after a stage is added to a
// stage order — the resources already in the cluster then report Pending until
// the controller has reconciled them once, which is the honest answer while the
// new stage has no verdict.
var stageVocabulary = conditions.Stages{
	ReadyType: conditionReady,
}

// stagesFor pairs the shared vocabulary with one reconciler's stage order.
func stagesFor(stageOrder []string) conditions.Stages {
	s := stageVocabulary
	s.Types = stageOrder
	return s
}

// newStatusBuilder opens the scratch space for one reconcile pass over the given
// stage order.
func newStatusBuilder(stageOrder []string, generation int64) *conditions.Builder {
	return conditions.NewBuilder(stagesFor(stageOrder), generation)
}

// derivePhase converts stage conditions into the coarse Phase. Only the stages
// in stageOrder are consulted (the aggregate Ready is ignored here), matching the
// sds-elastic semantics.
//
// It is taken over the CR's merged conditions rather than the pass's own set:
// a pass that stopped at a failing stage wrote only the stages it reached.
func derivePhase(conds []metav1.Condition, stageOrder []string) string {
	return stagesFor(stageOrder).Phase(conds)
}

// withReconcileTimeout bounds a single reconcile. Reconcilers talk to backends
// over HTTP, exec into data-plane pods and wait on the API server, and a
// reconcile context carries no deadline of its own — so one call that never
// returns takes its worker with it. With MaxConcurrentReconciles at 1 that is the
// entire controller: every other SystemBucket silently stops being reconciled
// until the pod restarts, which is exactly what an e2e run of the user-facing
// module caught (its store worker entered a reconcile and nothing was reconciled
// for the next half hour). A deadline turns that into an error naming the object,
// and a retry.
//
// A zero timeout (unset config, as in tests) leaves the context untouched.
func withReconcileTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
