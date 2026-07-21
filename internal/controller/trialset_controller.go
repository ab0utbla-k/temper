/*
Copyright 2026.

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
	"fmt"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/safeguard"
)

// TrialSetReconciler reconciles a TrialSet object. It mirrors the CronTrial
// reconciler's phase machine (Idle/Running/Paused/Completed/Halted/Failed)
// but fans out to one owned Trial per discovered Deployment, throttled by
// maxConcurrent. Discovery, the generated Trials, and their targets all stay
// in the TrialSet's own namespace — the owner reference garbage-collects the
// Trials, and namespace-scoped RBAC on Trials bounds the blast radius.
type TrialSetReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Recorder          events.EventRecorder
	NewAlertChecker   func(string) (safeguard.AlertChecker, error)
	NewMetricsQuerier func(string) (safeguard.MetricsQuerier, error)
}

// +kubebuilder:rbac:groups=temper.io,resources=trialsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temper.io,resources=trialsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temper.io,resources=trialsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=temper.io,resources=trials,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives the TrialSet phase machine. Phases:
//
//   - Idle/"":   waits for the next fire (scheduled) or fires once (one-shot).
//   - Running:  discovers Deployments, creates owned Trials up to
//     maxConcurrent, watches them to terminal, then completes the batch.
//   - Paused:   user set suspend=true; an in-progress batch still runs.
//   - Completed/Halted/Failed: terminal for one-shot; for scheduled, re-arm
//     to Idle (unless suspended) for the next fire.
//
// Forbid concurrency is enforced structurally: a new batch only ever fires
// from the Idle phase, and a Running TrialSet never re-enters Idle until the
// batch finishes — so overlapping batches cannot start.
func (r *TrialSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var trialSet temperv1alpha1.TrialSet
	if err := r.Get(ctx, req.NamespacedName, &trialSet); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get TrialSet: %w", err)
	}

	switch trialSet.Status.Phase {
	case "", temperv1alpha1.TrialSetPhaseIdle:
		return r.reconcileIdle(ctx, &trialSet)
	case temperv1alpha1.TrialSetPhaseRunning:
		return r.reconcileRunning(ctx, &trialSet)
	case temperv1alpha1.TrialSetPhasePaused:
		if trialSet.Spec.Suspend {
			return ctrl.Result{}, nil
		}
		trialSet.Status.Phase = temperv1alpha1.TrialSetPhaseIdle
		if err := r.Status().Update(ctx, &trialSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Idle: %w", err)
		}
		r.Recorder.Eventf(&trialSet, nil, "Normal", "Resumed", "Resumed", "TrialSet resumed")
		return ctrl.Result{RequeueAfter: time.Second}, nil
	case temperv1alpha1.TrialSetPhaseCompleted, temperv1alpha1.TrialSetPhaseHalted, temperv1alpha1.TrialSetPhaseFailed:
		if trialSet.Spec.Suspend {
			trialSet.Status.Phase = temperv1alpha1.TrialSetPhasePaused
			if err := r.Status().Update(ctx, &trialSet); err != nil {
				return ctrl.Result{}, fmt.Errorf("update status to Paused: %w", err)
			}
			r.Recorder.Eventf(&trialSet, nil, "Normal", "Paused", "Paused", "TrialSet suspended after batch")
			return ctrl.Result{}, nil
		}
		// One-shot TrialSets are terminal once a batch finishes.
		if trialSet.Spec.Schedule == nil {
			return ctrl.Result{}, nil
		}
		// Scheduled: re-arm for the next fire.
		return r.reconcileIdle(ctx, &trialSet)
	default:
		log.Info("Unknown phase", "phase", trialSet.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// reconcileIdle handles the wait-for-fire and fire transitions. A suspended
// TrialSet goes to Paused. For one-shot TrialSets (no schedule), it fires
// exactly once on first reconcile. For scheduled TrialSets, it parses the cron
// expression, computes the next fire time from LastScheduleTime (or the
// creation timestamp), and requeues until due.
func (r *TrialSetReconciler) reconcileIdle(ctx context.Context, trialSet *temperv1alpha1.TrialSet) (ctrl.Result, error) {
	if trialSet.Spec.Suspend {
		trialSet.Status.Phase = temperv1alpha1.TrialSetPhasePaused
		if err := r.Status().Update(ctx, trialSet); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Paused: %w", err)
		}
		r.Recorder.Eventf(trialSet, nil, "Normal", "Paused", "Paused", "TrialSet suspended")
		return ctrl.Result{}, nil
	}

	// One-shot: fire once on first reconcile, then stay terminal.
	if trialSet.Spec.Schedule == nil {
		if trialSet.Status.LastScheduleTime != nil {
			// Already fired — one-shot is terminal after its batch.
			return ctrl.Result{}, nil
		}
		return r.fireBatch(ctx, trialSet)
	}

	// Scheduled: compute the next fire time.
	loc, err := time.LoadLocation(trialSet.Spec.Timezone)
	if err != nil {
		return r.failTrialSet(ctx, trialSet, fmt.Sprintf("Invalid timezone %q: %v", trialSet.Spec.Timezone, err))
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronSched, err := parser.Parse(*trialSet.Spec.Schedule)
	if err != nil {
		return r.failTrialSet(ctx, trialSet, fmt.Sprintf("Invalid cron expression %q: %v", *trialSet.Spec.Schedule, err))
	}

	now := time.Now().In(loc)
	var lastRun time.Time
	if trialSet.Status.LastScheduleTime != nil {
		lastRun = trialSet.Status.LastScheduleTime.Time
	} else {
		lastRun = trialSet.CreationTimestamp.Time
	}

	nextRun := cronSched.Next(lastRun)
	if now.Before(nextRun) {
		return ctrl.Result{RequeueAfter: nextRun.Sub(now)}, nil
	}
	return r.fireBatch(ctx, trialSet)
}

// reconcileRunning drives an in-progress batch: it lists owned Trials,
// categorizes them by terminal phase, creates new Trials for uncovered matches
// up to maxConcurrent, and completes the batch when every match is covered and
// no Trial is active. Safeguard-unsafe Deployments are skipped (covered
// without a Trial) — they do not halt the batch (per-Deployment semantics).
//
// Discovery re-lists Deployments on each reconcile (continuous discovery
// rather than a snapshot at fire time). This is idempotent: a Deployment that
// already has a Trial (any phase) is never given a second one, and active
// counts include every owned Trial, so completion is never reached while a
// Trial is still running.
func (r *TrialSetReconciler) reconcileRunning(ctx context.Context, trialSet *temperv1alpha1.TrialSet) (ctrl.Result, error) {
	// 1. List the current batch's Trials and categorize. Filtering by the
	// batch label matters: earlier batches' Trials remain in the cluster as
	// history and must not count as coverage for this batch.
	var trialList temperv1alpha1.TrialList
	if err := r.List(ctx, &trialList,
		client.InNamespace(trialSet.Namespace),
		client.MatchingLabels{
			temperv1alpha1.LabelTrialSet: trialSet.Name,
			temperv1alpha1.LabelBatch:    batchLabel(trialSet),
		},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("list owned trials: %w", err)
	}

	var active, completed, failed, halted int32
	var activeNames []string
	var lastHaltReason *string
	hasTrial := make(map[string]bool)
	for i := range trialList.Items {
		t := &trialList.Items[i]
		if t.Spec.Target.Name != nil {
			hasTrial[*t.Spec.Target.Name] = true
		}
		switch t.Status.Phase {
		case temperv1alpha1.TrialPhaseCompleted:
			completed++
		case temperv1alpha1.TrialPhaseFailed:
			failed++
		case temperv1alpha1.TrialPhaseHalted:
			halted++
			if t.Status.HaltReason != nil {
				lastHaltReason = t.Status.HaltReason
			}
		case "", temperv1alpha1.TrialPhasePending, temperv1alpha1.TrialPhaseRunning:
			// A freshly created Trial has Status.Phase == "" until the Trial
			// reconciler sets Pending. Treat it as active so maxConcurrent
			// throttling and the completion gate (active == 0) account for
			// in-flight Trials that have not yet been observed as Pending.
			active++
			activeNames = append(activeNames, t.Name)
		}
	}

	// 2. Discover matches.
	matches, err := r.discoverMatches(ctx, trialSet)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Create Trials for uncovered matches up to maxConcurrent.
	maxConcurrent := max(trialSet.Spec.MaxConcurrent, 1)
	slots := maxConcurrent - active
	created := 0
	for i := range matches {
		if int32(created) >= slots {
			break
		}
		dep := &matches[i]
		if hasTrial[dep.Name] {
			continue
		}

		safe, reason, err := r.checkSafeguards(ctx, trialSet, dep)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check safeguards for %s: %w", dep.Name, err)
		}
		if !safe {
			r.Recorder.Eventf(trialSet, nil, "Warning", "SafeguardSkipped", "SafeguardSkipped",
				"Skipping deployment %s: %s", dep.Name, reason)
			// Mark covered so we do not retry the safeguard each reconcile.
			hasTrial[dep.Name] = true
			continue
		}

		if err := r.createTrialFor(ctx, trialSet, dep); err != nil {
			return ctrl.Result{}, fmt.Errorf("create trial for %s: %w", dep.Name, err)
		}
		hasTrial[dep.Name] = true
		created++
	}

	// 4. Update status from observed state.
	now := metav1.Now()
	trialSet.Status.DiscoveredDeployments = int32(len(matches))
	trialSet.Status.TrialsCreated = int32(len(trialList.Items)) + int32(created)
	trialSet.Status.TrialsCompleted = completed
	trialSet.Status.TrialsFailed = failed
	trialSet.Status.TrialsHalted = halted
	trialSet.Status.ActiveTrialNames = activeNames
	trialSet.Status.LastDiscoveryTime = &now
	if halted > 0 {
		trialSet.Status.History.LastHaltReason = lastHaltReason
	}

	if err := r.Status().Update(ctx, trialSet); err != nil {
		return ctrl.Result{}, fmt.Errorf("update TrialSet status: %w", err)
	}

	// 5. If we just created Trials, their Pending phase is not yet observed —
	// requeue and let the Owns watch drive the next pass.
	if created > 0 {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// 6. Completion: every match covered and nothing active.
	allCovered := true
	for i := range matches {
		if !hasTrial[matches[i].Name] {
			allCovered = false
			break
		}
	}
	if allCovered && active == 0 {
		return r.completeBatch(ctx, trialSet, failed, halted, lastHaltReason)
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// fireBatch starts a new batch: discovers matches, resets the per-batch
// counters, sets phase Running, and bumps history.TotalBatches. Trial
// creation itself happens in reconcileRunning (so maxConcurrent throttling
// stays in one place).
func (r *TrialSetReconciler) fireBatch(ctx context.Context, trialSet *temperv1alpha1.TrialSet) (ctrl.Result, error) {
	matches, err := r.discoverMatches(ctx, trialSet)
	if err != nil {
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	trialSet.Status.Phase = temperv1alpha1.TrialSetPhaseRunning
	trialSet.Status.DiscoveredDeployments = int32(len(matches))
	trialSet.Status.LastScheduleTime = &now
	trialSet.Status.LastDiscoveryTime = &now
	trialSet.Status.TrialsCreated = 0
	trialSet.Status.TrialsCompleted = 0
	trialSet.Status.TrialsFailed = 0
	trialSet.Status.TrialsHalted = 0
	trialSet.Status.ActiveTrialNames = nil
	trialSet.Status.History.TotalBatches++

	if err := r.Status().Update(ctx, trialSet); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Running: %w", err)
	}
	r.Recorder.Eventf(trialSet, nil, "Normal", "BatchStarted", "BatchStarted",
		"Discovered %d deployments", len(matches))

	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// completeBatch finalizes a finished batch: sets phase Completed (one halt
// does not halt the whole batch — per-Deployment semantics), bumps history
// counters, and requeues scheduled TrialSets so reconcileIdle can re-arm the
// next fire. One-shot TrialSets return terminal.
func (r *TrialSetReconciler) completeBatch(
	ctx context.Context,
	trialSet *temperv1alpha1.TrialSet,
	failed, halted int32,
	lastHaltReason *string,
) (ctrl.Result, error) {
	trialSet.Status.Phase = temperv1alpha1.TrialSetPhaseCompleted
	trialSet.Status.ActiveTrialNames = nil

	if halted > 0 {
		trialSet.Status.History.HaltedBatches++
		trialSet.Status.History.LastHaltReason = lastHaltReason
	}
	// A successful batch is one whose Trials all Completed (no failures, no halts).
	if failed == 0 && halted == 0 {
		trialSet.Status.History.SuccessfulBatches++
	}

	if err := r.Status().Update(ctx, trialSet); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Completed: %w", err)
	}
	r.Recorder.Eventf(trialSet, nil, "Normal", "BatchCompleted", "BatchCompleted",
		"Batch finished: %d completed, %d failed, %d halted",
		trialSet.Status.TrialsCompleted, failed, halted)

	if trialSet.Spec.Schedule != nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *TrialSetReconciler) failTrialSet(ctx context.Context, trialSet *temperv1alpha1.TrialSet, reason string) (ctrl.Result, error) {
	trialSet.Status.Phase = temperv1alpha1.TrialSetPhaseFailed
	if err := r.Status().Update(ctx, trialSet); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Failed: %w", err)
	}
	r.Recorder.Eventf(trialSet, nil, "Warning", "Failed", "Failed", reason)
	return ctrl.Result{}, nil
}

// discoverMatches lists Deployments matching targetSelector in the TrialSet's
// own namespace, then filters out those below minReadyReplicas.
func (r *TrialSetReconciler) discoverMatches(ctx context.Context, trialSet *temperv1alpha1.TrialSet) ([]appsv1.Deployment, error) {
	selector, err := metav1.LabelSelectorAsSelector(&trialSet.Spec.TargetSelector)
	if err != nil {
		return nil, fmt.Errorf("parse targetSelector: %w", err)
	}

	var depList appsv1.DeploymentList
	if err := r.List(ctx, &depList,
		client.InNamespace(trialSet.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}

	var matches []appsv1.Deployment
	for i := range depList.Items {
		dep := &depList.Items[i]
		if trialSet.Spec.MinReadyReplicas != nil && dep.Status.ReadyReplicas < *trialSet.Spec.MinReadyReplicas {
			continue
		}
		matches = append(matches, *dep)
	}
	return matches, nil
}

// checkSafeguards delegates to the shared safeguard.CheckSafeguards helper.
func (r *TrialSetReconciler) checkSafeguards(ctx context.Context, trialSet *temperv1alpha1.TrialSet, dep *appsv1.Deployment) (bool, string, error) {
	return safeguard.CheckSafeguards(ctx, r.Client, trialSet.Namespace, dep.Name,
		trialSet.Spec.Safeguards, r.NewAlertChecker, r.NewMetricsQuerier)
}

// createTrialFor stamps a Trial from the inline template pointed at dep. The
// Trial lives in the TrialSet's namespace (owner ref + GC) and carries the
// temper.io/trial-set and temper.io/batch labels (metrics attribution,
// watcher scope, per-batch coverage). The name <trialset>-b<batch>-<deployment>
// is deterministic within a batch, so a re-create under a stale cache fails
// with AlreadyExists instead of injecting the same Deployment twice.
func (r *TrialSetReconciler) createTrialFor(ctx context.Context, trialSet *temperv1alpha1.TrialSet, dep *appsv1.Deployment) error {
	tmpl := trialSet.Spec.TrialTemplate.DeepCopy()

	depName := dep.Name
	trialName := fmt.Sprintf("%s-b%s-%s", trialSet.Name, batchLabel(trialSet), dep.Name)

	trial := &temperv1alpha1.Trial{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: trialSet.Namespace,
			Name:      trialName,
			Labels: map[string]string{
				temperv1alpha1.LabelTrialSet: trialSet.Name,
				temperv1alpha1.LabelBatch:    batchLabel(trialSet),
			},
		},
		Spec: temperv1alpha1.TrialSpec{
			Target: temperv1alpha1.Target{
				Kind: "Deployment",
				Name: &depName,
			},
			Scenarios: tmpl.Scenarios,
			Execution: tmpl.Execution,
		},
	}

	if err := controllerutil.SetControllerReference(trialSet, trial, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference: %w", err)
	}

	if err := r.Create(ctx, trial); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// The Trial exists but the cache has not caught up yet — the next
			// reconcile will observe it. Not an error, and crucially not a
			// second injection.
			return nil
		}
		return fmt.Errorf("create trial: %w", err)
	}
	r.Recorder.Eventf(trialSet, trial, "Normal", "TrialCreated", "TrialCreated",
		"Created trial %s for deployment %s", trialName, depName)
	return nil
}

// SetupWithManager sets up the controller with the Manager. It watches
// TrialSets and owns the Trials it generates (so Trial status changes drive
// reconcileRunning).
func (r *TrialSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&temperv1alpha1.TrialSet{}).
		Owns(&temperv1alpha1.Trial{}).
		Named("trialset").
		Complete(r)
}

// batchLabel is the temper.io/batch value for the current batch:
// history.totalBatches as a string. fireBatch increments the counter before
// the batch starts running, so the value is 1-based and stable for the whole
// batch.
func batchLabel(trialSet *temperv1alpha1.TrialSet) string {
	return strconv.FormatInt(int64(trialSet.Status.History.TotalBatches), 10)
}
