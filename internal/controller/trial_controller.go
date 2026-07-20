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
	"errors"
	"fmt"
	"net/http"
	"time"

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
	"github.com/ab0utbla-k/temper/internal/metrics"
	"github.com/ab0utbla-k/temper/internal/risk"
	"github.com/ab0utbla-k/temper/internal/scenario"
)

const (
	trialFinalizer      = "temper.io/trial-cleanup"
	recoveryGracePeriod = 5 * time.Second
	targetReadyGrace    = 60 * time.Second
)

// TrialReconciler reconciles a Trial object
type TrialReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// newScenario overrides scenario construction in tests.
	// Nil means buildScenario.
	newScenario func(c client.Client, spec temperv1alpha1.Scenario, owner string) (scenario.Scenario, error)
}

// +kubebuilder:rbac:groups=temper.io,resources=trials,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temper.io,resources=trials/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temper.io,resources=trials/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *TrialReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var trial temperv1alpha1.Trial
	if err := r.Get(ctx, req.NamespacedName, &trial); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get Trial: %w", err)
	}

	if !trial.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&trial, trialFinalizer) {
			if err := r.revertIfActive(ctx, &trial); err != nil {
				return ctrl.Result{}, fmt.Errorf("revert on deletion: %w", err)
			}
			controllerutil.RemoveFinalizer(&trial, trialFinalizer)
			if err := r.Update(ctx, &trial); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&trial, trialFinalizer) {
		controllerutil.AddFinalizer(&trial, trialFinalizer)
		if err := r.Update(ctx, &trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if reason, ok := trial.Annotations[temperv1alpha1.AnnotationHaltReason]; ok {
		code := trial.Annotations[temperv1alpha1.AnnotationHaltCode]
		if trial.Status.Phase == temperv1alpha1.TrialPhaseHalted {
			delete(trial.Annotations, temperv1alpha1.AnnotationHaltReason)
			delete(trial.Annotations, temperv1alpha1.AnnotationHaltCode)

			if err := r.Update(ctx, &trial); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove halt annotations: %w", err)
			}
			return ctrl.Result{}, nil
		}

		if err := r.revertIfActive(ctx, &trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("revert on halt: %w", err)
		}

		trial.Status.Phase = temperv1alpha1.TrialPhaseHalted
		trial.Status.Outcome = temperv1alpha1.OutcomeHalted
		trial.Status.HaltReason = &reason
		if err := r.Status().Update(ctx, &trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Halted: %w", err)
		}
		r.Recorder.Eventf(&trial, nil, "Warning", "Halted", "Halted",
			"Trial halted by safeguard: %s", reason)

		metrics.TrialsHaltedTotal.WithLabelValues(trial.Namespace, sourceLabel(&trial), code).Inc()

		delete(trial.Annotations, temperv1alpha1.AnnotationHaltReason)
		delete(trial.Annotations, temperv1alpha1.AnnotationHaltCode)

		if err := r.Update(ctx, &trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove halt annotations: %w", err)
		}

		return ctrl.Result{}, nil
	}

	switch trial.Status.Phase {
	case "", temperv1alpha1.TrialPhasePending:
		return r.reconcilePending(ctx, &trial)
	case temperv1alpha1.TrialPhaseRunning:
		return r.reconcileRunning(ctx, &trial)
	case temperv1alpha1.TrialPhaseCompleted, temperv1alpha1.TrialPhaseFailed, temperv1alpha1.TrialPhaseHalted:
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown phase", "phase", trial.Status.Phase)
		return ctrl.Result{}, nil
	}
}

func (r *TrialReconciler) revertIfActive(ctx context.Context, trial *temperv1alpha1.Trial) error {
	if trial.Status.InjectedAt == nil {
		return nil
	}

	idx := int(trial.Status.CurrentScenarioIndex)
	if idx >= len(trial.Spec.Scenarios) {
		return nil
	}

	s, err := r.scenarioFor(trial, trial.Spec.Scenarios[idx])
	if err != nil {
		return fmt.Errorf("build scenario for revert: %w", err)
	}

	return s.Revert(ctx, targetFromSpec(trial))
}

func (r *TrialReconciler) scenarioFor(trial *temperv1alpha1.Trial, spec temperv1alpha1.Scenario) (scenario.Scenario, error) {
	owner := client.ObjectKeyFromObject(trial).String()
	if r.newScenario != nil {
		return r.newScenario(r.Client, spec, owner)
	}

	return buildScenario(r.Client, spec, owner)
}

func (r *TrialReconciler) reconcilePending(ctx context.Context, trial *temperv1alpha1.Trial) (ctrl.Result, error) {
	trial.Status.Phase = temperv1alpha1.TrialPhaseRunning

	// Detect resilience risks on the target and record them before scenarios
	// inject. Detection is advisory and best-effort: a failure is logged but
	// never blocks or fails the Trial.
	r.detectRisks(ctx, trial)

	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Running: %w", err)
	}
	r.Recorder.Eventf(trial, nil, "Normal", "Started", "Started", "Trial started")
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// detectRisks refreshes trial.Status.Risks against the target workload's
// current state: newly detected risks are added and mitigated risks are
// removed. It reports whether the recorded set changed so callers know a
// status write is needed. It is advisory and never returns an error:
// detection problems are logged and leave Risks untouched (never wiped) so
// the Trial still runs.
func (r *TrialReconciler) detectRisks(ctx context.Context, trial *temperv1alpha1.Trial) bool {
	log := logf.FromContext(ctx)

	if trial.Spec.Target.Name == nil {
		log.Info("Skipping risk detection: target has no name",
			"targetKind", trial.Spec.Target.Kind)
		return false
	}

	// Carry the full target reference on every line so risk detection is easy
	// to trace and debug per Trial.
	log = log.WithValues(
		"targetKind", trial.Spec.Target.Kind,
		"targetNamespace", trial.Namespace,
		"targetName", *trial.Spec.Target.Name,
	)

	log.Info("Detecting target risks")

	risks, err := risk.Detect(ctx, r.Client,
		trial.Spec.Target.Kind, trial.Namespace, *trial.Spec.Target.Name)
	if err != nil {
		log.Error(err, "Failed to detect target risks")
		return false
	}

	if risksEqual(trial.Status.Risks, risks) {
		log.Info("Target risks unchanged", "count", len(risks))
		return false
	}

	if added := diffRules(risks, trial.Status.Risks); len(added) > 0 {
		log.Info("Detected new target risks", "rules", added)
	}
	if removed := diffRules(trial.Status.Risks, risks); len(removed) > 0 {
		log.Info("Removed mitigated target risks", "rules", removed)
	}

	if len(risks) == 0 {
		log.Info("Detected no target risks")
	} else {
		rules := make([]string, len(risks))
		for i, rk := range risks {
			rules[i] = string(rk.Rule)
		}
		log.Info("Updated target risks", "count", len(risks), "rules", rules)
	}

	trial.Status.Risks = risks
	return true
}

// risksEqual reports whether two risk sets carry the same rules and messages
// in the same order (detection output is deterministic).
func risksEqual(a, b []temperv1alpha1.Risk) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Rule != b[i].Rule || a[i].Message != b[i].Message {
			return false
		}
	}
	return true
}

// diffRules returns the rule tokens present in a but absent from b.
func diffRules(a, b []temperv1alpha1.Risk) []string {
	in := make(map[temperv1alpha1.RiskRule]bool, len(b))
	for _, rk := range b {
		in[rk.Rule] = true
	}
	var out []string
	for _, rk := range a {
		if !in[rk.Rule] {
			out = append(out, string(rk.Rule))
		}
	}
	return out
}

func (r *TrialReconciler) reconcileRunning(ctx context.Context, trial *temperv1alpha1.Trial) (ctrl.Result, error) {
	// Keep status.risks current on every pass, independent of scenario
	// execution: risks whose condition was mitigated since the last pass
	// disappear, newly introduced ones appear. Persist immediately because
	// several paths below return without another status write. Best-effort:
	// a failed write is retried naturally on the next pass.
	if r.detectRisks(ctx, trial) {
		if err := r.Status().Update(ctx, trial); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to persist updated risks")
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	idx := int(trial.Status.CurrentScenarioIndex)
	if idx >= len(trial.Spec.Scenarios) {
		return r.completeTrial(ctx, trial)
	}

	spec := trial.Spec.Scenarios[idx]
	target := targetFromSpec(trial)

	// State 1: Not yet injected — inject now.
	if trial.Status.InjectedAt == nil {
		s, err := r.scenarioFor(trial, spec)
		if err != nil {
			return r.failTrial(ctx, trial, fmt.Sprintf("Build scenario: %v", err))
		}

		// Persist intent before the side effect: if this write fails, nothing
		// was injected and the retry is safe. Writing after Inject risks a
		// second injection when the write is lost (crash, conflict).
		trial.Status.InjectedAt = new(metav1.Now())
		if err := r.Status().Update(ctx, trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status before inject: %w", err)
		}

		result, err := s.Inject(ctx, target)
		if err != nil {
			if errors.Is(err, scenario.ErrTargetNotInjectable) && time.Since(trial.CreationTimestamp.Time) < targetReadyGrace {
				// Target's pods aren't up yet and nothing was injected — clear the
				// intent marker and retry instead of failing permanently.
				trial.Status.InjectedAt = nil
				if err := r.Status().Update(ctx, trial); err != nil {
					return ctrl.Result{}, fmt.Errorf("clear InjectedAt: %w", err)
				}

				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			// InjectedAt stays set: clearing it would reopen the double-inject
			// window for a partially applied Inject.
			return r.failTrial(ctx, trial, fmt.Sprintf("Inject %s: %v", spec.Type, err))
		}

		if result.Incomplete {
			trial.Status.InjectionIncomplete = true
		}

		return r.recordInjectResult(ctx, trial, spec, result, idx)
	}

	if trial.Status.InjectionIncomplete {
		return r.continueInjection(ctx, trial, spec, target)
	}

	// State 2/3: Already injected — check duration.
	elapsed := time.Since(trial.Status.InjectedAt.Time)
	remaining := spec.Duration.Duration - elapsed

	if remaining > 0 {
		// Still within duration — poll for recovery.
		if trial.Status.RecoveredAt == nil && time.Since(trial.Status.InjectedAt.Time) >= recoveryGracePeriod {
			s, err := r.scenarioFor(trial, spec)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("build scenario for recovery probe: %w", err)
			}

			// The Trial may override the scenario's default probe.
			probe := s.RecoveryProbe()
			if rec := trial.Spec.Recovery; rec != nil && rec.HTTP != nil {
				probe = scenario.RecoveryProbe{HTTP: &scenario.HTTPProbe{URL: rec.HTTP.URL}}
			}

			if recovered, err := r.checkRecovery(ctx, trial, probe); err != nil {
				return ctrl.Result{}, fmt.Errorf("check recovery: %w", err)
			} else if recovered {
				trial.Status.RecoveredAt = new(metav1.Now())

				// The result row can be missing if the post-inject status write was
				// lost - then there is nothing to fill, and completion will treat the
				// scenario as unproven.
				if n := len(trial.Status.ScenarioResults); n > 0 {
					trial.Status.ScenarioResults[n-1].RecoveredAt = trial.Status.RecoveredAt
				}

				if err := r.Status().Update(ctx, trial); err != nil {
					return ctrl.Result{}, fmt.Errorf("update status after recovery: %w", err)
				}
			}
		}
		poll := min(5*time.Second, remaining)
		return ctrl.Result{RequeueAfter: poll}, nil
	}

	// Duration elapsed — revert and advance.
	s, err := r.scenarioFor(trial, spec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build scenario for revert: %w", err)
	}

	if err := s.Revert(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("revert %s: %w", spec.Type, err)
	}

	// Update MTTR if we recorded recovery.
	if trial.Status.RecoveredAt != nil {
		// Metrics can be nil here if the post-inject status write was lost.
		if trial.Status.Metrics == nil {
			trial.Status.Metrics = &temperv1alpha1.TrialMetrics{}
		}
		recoveryTime := trial.Status.RecoveredAt.Sub(trial.Status.InjectedAt.Time)
		metrics.RecoveryTimeSeconds.WithLabelValues(trial.Namespace, sourceLabel(trial), string(spec.Type)).Observe(recoveryTime.Seconds())
		if prev := trial.Status.Metrics.MeanRecoveryTime; prev != nil && idx > 0 {
			n := time.Duration(idx)
			avg := (prev.Duration*n + recoveryTime) / (n + 1)
			trial.Status.Metrics.MeanRecoveryTime = &metav1.Duration{Duration: avg}
		} else {
			trial.Status.Metrics.MeanRecoveryTime = &metav1.Duration{Duration: recoveryTime}
		}
	}

	r.Recorder.Eventf(trial, nil, "Normal", "Reverted", "Reverted",
		"Reverted scenario %s (%d/%d)", spec.Type, idx+1, len(trial.Spec.Scenarios))

	// Clear per-scenario tracking, advance index.
	trial.Status.RecoveredAt = nil
	trial.Status.InjectedAt = nil
	trial.Status.CurrentScenarioIndex = int32(idx + 1)

	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status after revert: %w", err)
	}

	// Pause between scenarios if configured.
	pause := time.Second
	if trial.Spec.Execution != nil && trial.Spec.Execution.PauseBetween != nil {
		pause = trial.Spec.Execution.PauseBetween.Duration
	}
	return ctrl.Result{RequeueAfter: pause}, nil
}

func (r *TrialReconciler) continueInjection(
	ctx context.Context,
	trial *temperv1alpha1.Trial,
	spec temperv1alpha1.Scenario,
	target scenario.Target,
) (ctrl.Result, error) {
	timeout := 30 * time.Second

	if spec.NodeDrain != nil && spec.NodeDrain.EvictionTimeout != nil {
		timeout = spec.NodeDrain.EvictionTimeout.Duration
	}
	if time.Since(trial.Status.InjectedAt.Time) > timeout {
		// The PDB never yielded — that is the verdict. Revert first: a
		// terminal Trial is never reconciled again, so going terminal with
		// the cordon still on would leak it.
		if err := r.revertIfActive(ctx, trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("revert before blocked: %w", err)
		}

		trial.Status.InjectionIncomplete = false
		trial.Status.Phase = temperv1alpha1.TrialPhaseCompleted
		trial.Status.Outcome = temperv1alpha1.OutcomeBlocked

		if err := r.Status().Update(ctx, trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Blocked: %w", err)
		}
		r.Recorder.Eventf(trial, nil, "Warning", "Blocked", "Blocked",
			"Evictions stayed blocked past the eviction timeout; see scenarioResults findings")
		metrics.TrialsTotal.WithLabelValues(trial.Namespace, sourceLabel(trial), "blocked").Inc()
		metrics.TrialDurationSeconds.WithLabelValues(trial.Namespace, sourceLabel(trial)).Observe(time.Since(trial.CreationTimestamp.Time).Seconds())
		return ctrl.Result{}, nil
	}

	s, err := r.scenarioFor(trial, spec)
	if err != nil {
		return r.failTrial(ctx, trial, fmt.Sprintf("Build scenario: %v", err))
	}

	result, err := s.Inject(ctx, target)
	if err != nil {
		return r.failTrial(ctx, trial, fmt.Sprintf("Inject %s: %v", spec.Type, err))
	}

	// The latest attempt is the truth: replace the row's findings, don't append.
	if n := len(trial.Status.ScenarioResults); n > 0 {
		trial.Status.ScenarioResults[n-1].Findings = apiFindings(result.Findings)
	}

	if !result.Incomplete {
		trial.Status.InjectionIncomplete = false
	}

	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status after injection retry: %w", err)
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// recordInjectResult persists the post-inject status and emits the scenario's
// events and metrics.
func (r *TrialReconciler) recordInjectResult(
	ctx context.Context,
	trial *temperv1alpha1.Trial,
	spec temperv1alpha1.Scenario,
	result scenario.Result,
	idx int,
) (ctrl.Result, error) {
	if trial.Status.Metrics == nil {
		trial.Status.Metrics = &temperv1alpha1.TrialMetrics{}
	}

	var podsKilled int32
	if spec.Type == temperv1alpha1.ScenarioTypePodKill {
		podsKilled = int32(result.PodsAffected)
		trial.Status.Metrics.TotalPodsKilled += podsKilled
	}

	trial.Status.ScenarioResults = append(trial.Status.ScenarioResults, temperv1alpha1.ScenarioResult{
		Type:       spec.Type,
		InjectedAt: *trial.Status.InjectedAt,
		Findings:   apiFindings(result.Findings),
	})

	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status after inject: %w", err)
	}
	r.Recorder.Eventf(trial, nil, "Normal", "Injected", "Injected",
		"Injected scenario %s (%d/%d)", spec.Type, idx+1, len(trial.Spec.Scenarios))

	metrics.ScenariosExecutedTotal.WithLabelValues(trial.Namespace, sourceLabel(trial), string(spec.Type)).Inc()
	if podsKilled > 0 {
		metrics.PodsKilledTotal.WithLabelValues(trial.Namespace, sourceLabel(trial)).Add(float64(podsKilled))
	}

	if spec.Type == temperv1alpha1.ScenarioTypeNodeDrain {
		if result.PodsAffected > 0 {
			metrics.PodsEvictedTotal.WithLabelValues(trial.Namespace, sourceLabel(trial)).Add(float64(result.PodsAffected))
		}

		for _, f := range result.Findings {
			r.Recorder.Eventf(trial, nil, "Warning", "EvictionBlocked", "EvictionBlocked",
				"Pod %s eviction blocked: %s", f.Pod, f.Reason)
			metrics.EvictionsBlockedTotal.WithLabelValues(trial.Namespace, sourceLabel(trial)).Inc()
		}
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// completeTrial concludes a Trial whose scenarios all ran: the outcome is
// Passed only with recovery proof for every scenario, otherwise Failed.
func (r *TrialReconciler) completeTrial(ctx context.Context, trial *temperv1alpha1.Trial) (ctrl.Result, error) {
	trial.Status.Phase = temperv1alpha1.TrialPhaseCompleted

	outcome := temperv1alpha1.OutcomePassed
	if len(trial.Status.ScenarioResults) != len(trial.Spec.Scenarios) {
		outcome = temperv1alpha1.OutcomeFailed
	}
	for _, res := range trial.Status.ScenarioResults {
		if res.RecoveredAt == nil {
			outcome = temperv1alpha1.OutcomeFailed
			break
		}
	}
	trial.Status.Outcome = outcome

	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Completed: %w", err)
	}
	r.Recorder.Eventf(trial, nil, "Normal", "Completed", "Completed",
		"All scenarios completed")
	metrics.TrialsTotal.WithLabelValues(trial.Namespace, sourceLabel(trial), "completed").Inc()
	metrics.TrialDurationSeconds.WithLabelValues(trial.Namespace, sourceLabel(trial)).Observe(time.Since(trial.CreationTimestamp.Time).Seconds())
	return ctrl.Result{}, nil
}

func (r *TrialReconciler) failTrial(ctx context.Context, trial *temperv1alpha1.Trial, reason string) (ctrl.Result, error) {
	// Record the reason before any step that can fail: on a revert or status
	// error the retry loses this context (the next reconcile resumes the
	// normal flow and may end the Trial Completed), and the event is then
	// the only trace of the failure. Fail-intent persistence arrives with
	// the Outcome field.
	r.Recorder.Eventf(trial, nil, "Warning", "Failed", "Failed", reason)

	// A terminal Trial is never reconciled again, so going terminal with an
	// active injection would leak it (e.g. a node stays cordoned). Revert
	// first; on error requeue instead.
	if err := r.revertIfActive(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("revert before fail: %w", err)
	}

	trial.Status.Phase = temperv1alpha1.TrialPhaseFailed
	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Failed: %w", err)
	}
	metrics.TrialsTotal.WithLabelValues(trial.Namespace, sourceLabel(trial), "failed").Inc()
	metrics.TrialDurationSeconds.WithLabelValues(trial.Namespace, sourceLabel(trial)).Observe(time.Since(trial.CreationTimestamp.Time).Seconds())
	return ctrl.Result{}, nil
}

func (r *TrialReconciler) checkRecovery(
	ctx context.Context,
	trial *temperv1alpha1.Trial,
	probe scenario.RecoveryProbe,
) (bool, error) {
	// The HTTP probe hits a URL directly; it needs no target lookup.
	if probe.HTTP != nil {
		return checkHTTPRecovery(ctx, probe.HTTP)
	}

	if trial.Spec.Target.Name == nil {
		return false, nil
	}

	var dep appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: trial.Namespace,
		Name:      *trial.Spec.Target.Name,
	}, &dep); err != nil {
		return false, fmt.Errorf("get deployment: %w", err)
	}

	switch {
	case probe.WorkloadReady != nil:
		if dep.Status.ObservedGeneration < dep.Generation {
			return false, nil
		}

		desired := int32(1)
		if dep.Spec.Replicas != nil {
			desired = *dep.Spec.Replicas
		}
		return dep.Status.ReadyReplicas == desired, nil
	case probe.Condition != nil:
		for _, cond := range dep.Status.Conditions {
			if string(cond.Type) == probe.Condition.Type && string(cond.Status) == string(probe.Condition.Status) {
				return true, nil
			}
		}
		return false, nil
	case probe.Query != nil:
		return false, fmt.Errorf("query recovery probes are not wired yet")
	default:
		return false, fmt.Errorf("recovery probe has no kind set")
	}
}

// checkHTTPRecovery reports recovery when an HTTP GET to the probe URL returns
// a 2xx status. A malformed URL is a configuration error and is returned. A
// transport error (connection refused, DNS failure, timeout) or a non-2xx
// status is the normal "not recovered yet" signal, not a controller error.
func checkHTTPRecovery(ctx context.Context, probe *scenario.HTTPProbe) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probe.URL, nil)
	if err != nil {
		return false, fmt.Errorf("build recovery request: %w", err)
	}

	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

// apiFindings converts scenario findings into their API status form.
func apiFindings(findings []scenario.Finding) []temperv1alpha1.Finding {
	out := make([]temperv1alpha1.Finding, 0, len(findings))
	for _, f := range findings {
		out = append(out, temperv1alpha1.Finding{
			Reason:  "EvictionBlocked",
			Message: fmt.Sprintf("Pod %s eviction blocked: %s", f.Pod, f.Reason),
		})
	}
	return out
}

func buildScenario(c client.Client, spec temperv1alpha1.Scenario, owner string) (scenario.Scenario, error) {
	switch spec.Type {
	case temperv1alpha1.ScenarioTypePodKill:
		count := int32(1)
		if spec.PodKill != nil {
			count = spec.PodKill.Count
		}

		return &scenario.PodKill{
			Client: c,
			Count:  count,
		}, nil
	case temperv1alpha1.ScenarioTypeNodeDrain:
		name := ""
		if spec.NodeDrain != nil {
			name = spec.NodeDrain.NodeName
		}
		return &scenario.NodeDrain{
			Client:   c,
			Owner:    owner,
			NodeName: name,
		}, nil
	case temperv1alpha1.ScenarioTypeSpotReclaim:
		name := ""
		if spec.SpotReclaim != nil {
			name = spec.SpotReclaim.NodeName
		}
		return &scenario.SpotReclaim{
			Client:   c,
			Owner:    owner,
			NodeName: name,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported scenario type: %s", spec.Type)
	}
}

func targetFromSpec(trial *temperv1alpha1.Trial) scenario.Target {
	t := scenario.Target{
		Namespace: trial.Namespace,
		Kind:      trial.Spec.Target.Kind,
	}
	if trial.Spec.Target.Name != nil {
		t.Name = *trial.Spec.Target.Name
	}
	return t
}

func sourceLabel(trial *temperv1alpha1.Trial) string {
	if val := trial.Labels[temperv1alpha1.LabelCronTrial]; val != "" {
		return val
	}
	return "adhoc"
}

// SetupWithManager sets up the controller with the Manager.
func (r *TrialReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&temperv1alpha1.Trial{}).
		Named("trial").
		Complete(r)
}
