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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	experimentFinalizer = "temper.io/experiment-cleanup"
	recoveryGracePeriod = 5 * time.Second
)

// ChaosExperimentReconciler reconciles a ChaosExperiment object
type ChaosExperimentReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// newScenario overrides scenario construction in tests.
	// Nil means buildScenario.
	newScenario func(c client.Client, spec temperv1alpha1.Scenario) (scenario.Scenario, error)
}

// +kubebuilder:rbac:groups=temper.io,resources=chaosexperiments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temper.io,resources=chaosexperiments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temper.io,resources=chaosexperiments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *ChaosExperimentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var exp temperv1alpha1.ChaosExperiment
	if err := r.Get(ctx, req.NamespacedName, &exp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get ChaosExperiment: %w", err)
	}

	if !exp.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&exp, experimentFinalizer) {
			if err := r.revertIfActive(ctx, &exp); err != nil {
				return ctrl.Result{}, fmt.Errorf("revert on deletion: %w", err)
			}
			controllerutil.RemoveFinalizer(&exp, experimentFinalizer)
			if err := r.Update(ctx, &exp); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&exp, experimentFinalizer) {
		controllerutil.AddFinalizer(&exp, experimentFinalizer)
		if err := r.Update(ctx, &exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if reason, ok := exp.Annotations[temperv1alpha1.AnnotationHaltReason]; ok {
		code := exp.Annotations[temperv1alpha1.AnnotationHaltCode]
		if exp.Status.Phase == temperv1alpha1.ExperimentPhaseHalted {
			delete(exp.Annotations, temperv1alpha1.AnnotationHaltReason)
			delete(exp.Annotations, temperv1alpha1.AnnotationHaltCode)

			if err := r.Update(ctx, &exp); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove halt annotations: %w", err)
			}
			return ctrl.Result{}, nil
		}

		if err := r.revertIfActive(ctx, &exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("revert on halt: %w", err)
		}

		exp.Status.Phase = temperv1alpha1.ExperimentPhaseHalted
		exp.Status.HaltReason = &reason
		if err := r.Status().Update(ctx, &exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Halted: %w", err)
		}
		r.Recorder.Eventf(&exp, nil, "Warning", "Halted", "Halted", "Experiment halted by safeguard: %s", reason)

		metrics.ExperimentsHaltedTotal.WithLabelValues(exp.Namespace, sourceLabel(&exp), code).Inc()

		delete(exp.Annotations, temperv1alpha1.AnnotationHaltReason)
		delete(exp.Annotations, temperv1alpha1.AnnotationHaltCode)

		if err := r.Update(ctx, &exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove halt annotations: %w", err)
		}

		return ctrl.Result{}, nil
	}

	switch exp.Status.Phase {
	case "", temperv1alpha1.ExperimentPhasePending:
		return r.reconcilePending(ctx, &exp)
	case temperv1alpha1.ExperimentPhaseRunning:
		return r.reconcileRunning(ctx, &exp)
	case temperv1alpha1.ExperimentPhaseCompleted, temperv1alpha1.ExperimentPhaseFailed, temperv1alpha1.ExperimentPhaseHalted:
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown phase", "phase", exp.Status.Phase)
		return ctrl.Result{}, nil
	}
}

func (r *ChaosExperimentReconciler) revertIfActive(ctx context.Context, exp *temperv1alpha1.ChaosExperiment) error {
	if exp.Status.InjectedAt == nil {
		return nil
	}

	idx := int(exp.Status.CurrentScenarioIndex)
	if idx >= len(exp.Spec.Scenarios) {
		return nil
	}

	s, err := r.scenarioFor(exp.Spec.Scenarios[idx])
	if err != nil {
		return fmt.Errorf("build scenario for revert: %w", err)
	}

	return s.Revert(ctx, targetFromSpec(exp))
}

func (r *ChaosExperimentReconciler) scenarioFor(spec temperv1alpha1.Scenario) (scenario.Scenario, error) {
	if r.newScenario != nil {
		return r.newScenario(r.Client, spec)
	}
	return buildScenario(r.Client, spec)
}

func (r *ChaosExperimentReconciler) reconcilePending(ctx context.Context, exp *temperv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	exp.Status.Phase = temperv1alpha1.ExperimentPhaseRunning

	if err := r.Status().Update(ctx, exp); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Running: %w", err)
	}
	r.Recorder.Eventf(exp, nil, "Normal", "Started", "Started", "Experiment started")
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *ChaosExperimentReconciler) reconcileRunning(ctx context.Context, exp *temperv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	idx := int(exp.Status.CurrentScenarioIndex)
	if idx >= len(exp.Spec.Scenarios) {
		// All scenarios done.
		exp.Status.Phase = temperv1alpha1.ExperimentPhaseCompleted
		if err := r.Status().Update(ctx, exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Completed: %w", err)
		}
		r.Recorder.Eventf(exp, nil, "Normal", "Completed", "Completed", "All scenarios completed")
		metrics.ExperimentsTotal.WithLabelValues(exp.Namespace, sourceLabel(exp), "completed").Inc()
		metrics.ExperimentDurationSeconds.WithLabelValues(exp.Namespace, sourceLabel(exp)).Observe(time.Since(exp.CreationTimestamp.Time).Seconds())
		return ctrl.Result{}, nil
	}

	spec := exp.Spec.Scenarios[idx]
	target := targetFromSpec(exp)

	// State 1: Not yet injected — inject now.
	if exp.Status.InjectedAt == nil {
		s, err := r.scenarioFor(spec)
		if err != nil {
			return r.failExperiment(ctx, exp, fmt.Sprintf("Build scenario: %v", err))
		}

		// Persist intent before the side effect: if this write fails, nothing
		// was injected and the retry is safe. Writing after Inject risks a
		// second injection when the write is lost (crash, conflict).
		exp.Status.InjectedAt = new(metav1.Now())
		if err := r.Status().Update(ctx, exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status before inject: %w", err)
		}

		if err := s.Inject(ctx, target); err != nil {
			// InjectedAt stays set: clearing it would reopen the double-inject
			// window for a partially applied Inject.
			return r.failExperiment(ctx, exp, fmt.Sprintf("Inject %s: %v", spec.Type, err))
		}

		if exp.Status.Metrics == nil {
			exp.Status.Metrics = &temperv1alpha1.ExperimentMetrics{}
		}

		var podsKilled int32
		if spec.Type == temperv1alpha1.ScenarioTypePodKill {
			podsKilled = 1
			if spec.PodKill != nil {
				podsKilled = spec.PodKill.Count
			}
			exp.Status.Metrics.TotalPodsKilled += podsKilled
		}

		if err := r.Status().Update(ctx, exp); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after inject: %w", err)
		}
		r.Recorder.Eventf(exp, nil, "Normal", "Injected", "Injected", "Injected scenario %s (%d/%d)", spec.Type, idx+1,
			len(exp.Spec.Scenarios))

		metrics.ScenariosExecutedTotal.WithLabelValues(exp.Namespace, sourceLabel(exp), string(spec.Type)).Inc()
		if podsKilled > 0 {
			metrics.PodsKilledTotal.WithLabelValues(exp.Namespace, sourceLabel(exp)).Add(float64(podsKilled))
		}

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// State 2/3: Already injected — check duration.
	elapsed := time.Since(exp.Status.InjectedAt.Time)
	remaining := spec.Duration.Duration - elapsed

	if remaining > 0 {
		// Still within duration — poll for recovery.
		if exp.Status.RecoveredAt == nil && time.Since(exp.Status.InjectedAt.Time) >= recoveryGracePeriod {
			if recovered, err := r.checkRecovery(ctx, exp); err != nil {
				return ctrl.Result{}, fmt.Errorf("check recovery: %w", err)
			} else if recovered {
				exp.Status.RecoveredAt = new(metav1.Now())

				if err := r.Status().Update(ctx, exp); err != nil {
					return ctrl.Result{}, fmt.Errorf("update status after recovery: %w", err)
				}
			}
		}
		poll := min(5*time.Second, remaining)
		return ctrl.Result{RequeueAfter: poll}, nil
	}

	// Duration elapsed — revert and advance.
	s, err := r.scenarioFor(spec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build scenario for revert: %w", err)
	}

	if err := s.Revert(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("revert %s: %w", spec.Type, err)
	}

	// Update MTTR if we recorded recovery.
	if exp.Status.RecoveredAt != nil {
		// Metrics can be nil here if the post-inject status write was lost.
		if exp.Status.Metrics == nil {
			exp.Status.Metrics = &temperv1alpha1.ExperimentMetrics{}
		}
		recoveryTime := exp.Status.RecoveredAt.Sub(exp.Status.InjectedAt.Time)
		metrics.RecoveryTimeSeconds.WithLabelValues(exp.Namespace, sourceLabel(exp), string(spec.Type)).Observe(recoveryTime.Seconds())
		if prev := exp.Status.Metrics.MeanRecoveryTime; prev != nil && idx > 0 {
			n := time.Duration(idx)
			avg := (prev.Duration*n + recoveryTime) / (n + 1)
			exp.Status.Metrics.MeanRecoveryTime = &metav1.Duration{Duration: avg}
		} else {
			exp.Status.Metrics.MeanRecoveryTime = &metav1.Duration{Duration: recoveryTime}
		}
	}

	r.Recorder.Eventf(exp, nil, "Normal", "Reverted", "Reverted", "Reverted scenario %s (%d/%d)", spec.Type, idx+1,
		len(exp.Spec.Scenarios))

	// Clear per-scenario tracking, advance index.
	exp.Status.RecoveredAt = nil
	exp.Status.InjectedAt = nil
	exp.Status.CurrentScenarioIndex = int32(idx + 1)

	if err := r.Status().Update(ctx, exp); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status after revert: %w", err)
	}

	// Pause between scenarios if configured.
	pause := time.Second
	if exp.Spec.Execution != nil && exp.Spec.Execution.PauseBetween != nil {
		pause = exp.Spec.Execution.PauseBetween.Duration
	}
	return ctrl.Result{RequeueAfter: pause}, nil
}

func (r *ChaosExperimentReconciler) failExperiment(ctx context.Context, exp *temperv1alpha1.ChaosExperiment, reason string) (ctrl.Result, error) {
	exp.Status.Phase = temperv1alpha1.ExperimentPhaseFailed
	if err := r.Status().Update(ctx, exp); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Failed: %w", err)
	}
	r.Recorder.Eventf(exp, nil, "Warning", "Failed", "Failed", reason)
	metrics.ExperimentsTotal.WithLabelValues(exp.Namespace, sourceLabel(exp), "failed").Inc()
	metrics.ExperimentDurationSeconds.WithLabelValues(exp.Namespace, sourceLabel(exp)).Observe(time.Since(exp.CreationTimestamp.Time).Seconds())
	return ctrl.Result{}, nil
}

func (r *ChaosExperimentReconciler) checkRecovery(ctx context.Context, exp *temperv1alpha1.ChaosExperiment) (bool, error) {
	if exp.Spec.Target.Name == nil {
		return false, nil
	}

	var dep appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: exp.Namespace,
		Name:      *exp.Spec.Target.Name,
	}, &dep); err != nil {
		return false, fmt.Errorf("get deployment: %w", err)
	}

	for _, cond := range dep.Status.Conditions {
		if cond.Type == appsv1.DeploymentAvailable && cond.Status == corev1.ConditionTrue {
			return true, nil
		}
	}
	return false, nil
}

func buildScenario(c client.Client, spec temperv1alpha1.Scenario) (scenario.Scenario, error) {
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
	default:
		return nil, fmt.Errorf("unsupported scenario type: %s", spec.Type)
	}
}

func targetFromSpec(exp *temperv1alpha1.ChaosExperiment) scenario.Target {
	t := scenario.Target{
		Namespace: exp.Namespace,
		Kind:      exp.Spec.Target.Kind,
	}
	if exp.Spec.Target.Name != nil {
		t.Name = *exp.Spec.Target.Name
	}
	return t
}

func sourceLabel(exp *temperv1alpha1.ChaosExperiment) string {
	if val := exp.Labels[temperv1alpha1.LabelSchedule]; val != "" {
		return val
	}
	return "adhoc"
}

// SetupWithManager sets up the controller with the Manager.
func (r *ChaosExperimentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&temperv1alpha1.ChaosExperiment{}).
		Named("chaosexperiment").
		Complete(r)
}
