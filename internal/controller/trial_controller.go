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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;update
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

	if err := r.Status().Update(ctx, trial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Running: %w", err)
	}
	r.Recorder.Eventf(trial, nil, "Normal", "Started", "Started", "Trial started")
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *TrialReconciler) reconcileRunning(ctx context.Context, trial *temperv1alpha1.Trial) (ctrl.Result, error) {
	idx := int(trial.Status.CurrentScenarioIndex)
	if idx >= len(trial.Spec.Scenarios) {
		// All scenarios done.
		trial.Status.Phase = temperv1alpha1.TrialPhaseCompleted
		if err := r.Status().Update(ctx, trial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Completed: %w", err)
		}
		r.Recorder.Eventf(trial, nil, "Normal", "Completed", "Completed",
			"All scenarios completed")
		metrics.TrialsTotal.WithLabelValues(trial.Namespace, sourceLabel(trial), "completed").Inc()
		metrics.TrialDurationSeconds.WithLabelValues(trial.Namespace, sourceLabel(trial)).Observe(time.Since(trial.CreationTimestamp.Time).Seconds())
		return ctrl.Result{}, nil
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

		return r.recordInjectResult(ctx, trial, spec, result, idx)
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

			if recovered, err := r.checkRecovery(ctx, trial, s.RecoveryProbe()); err != nil {
				return ctrl.Result{}, fmt.Errorf("check recovery: %w", err)
			} else if recovered {
				trial.Status.RecoveredAt = new(metav1.Now())

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
		return &scenario.NodeDrain{Client: c, Owner: owner}, nil
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
