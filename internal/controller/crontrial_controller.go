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

	"github.com/robfig/cron/v3"
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

// CronTrialReconciler reconciles a CronTrial object
type CronTrialReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Recorder          events.EventRecorder
	NewAlertChecker   func(string) (safeguard.AlertChecker, error)
	NewMetricsQuerier func(string) (safeguard.MetricsQuerier, error)
}

// +kubebuilder:rbac:groups=temper.io,resources=crontrials,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temper.io,resources=crontrials/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temper.io,resources=crontrials/finalizers,verbs=update
// +kubebuilder:rbac:groups=temper.io,resources=trials,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *CronTrialReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cronTrial temperv1alpha1.CronTrial
	if err := r.Get(ctx, req.NamespacedName, &cronTrial); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get CronTrial: %w", err)
	}
	// If suspended, just set phase and stop.
	if cronTrial.Spec.Suspend {
		if cronTrial.Status.Phase != temperv1alpha1.CronTrialPhasePaused {
			cronTrial.Status.Phase = temperv1alpha1.CronTrialPhasePaused
			if err := r.Status().Update(ctx, &cronTrial); err != nil {
				return ctrl.Result{}, fmt.Errorf("update status to Paused: %w", err)
			}
			r.Recorder.Eventf(&cronTrial, nil, "Normal", "Paused", "Paused", "CronTrial suspended")
		}
		return ctrl.Result{}, nil
	}

	// Phase dispatch.
	switch cronTrial.Status.Phase {
	case "", temperv1alpha1.CronTrialPhaseIdle:
		return r.reconcileIdle(ctx, &cronTrial)
	case temperv1alpha1.CronTrialPhaseRunning:
		return r.reconcileRunning(ctx, &cronTrial)
	case temperv1alpha1.CronTrialPhasePaused:
		// Was paused, now unsuspended — go back to Idle.
		cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseIdle
		if err := r.Status().Update(ctx, &cronTrial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Idle: %w", err)
		}
		r.Recorder.Eventf(&cronTrial, nil, "Normal", "Resumed", "Resumed", "CronTrial resumed")
		return ctrl.Result{RequeueAfter: time.Second}, nil
	case temperv1alpha1.CronTrialPhaseHalted, temperv1alpha1.CronTrialPhaseCompleted, temperv1alpha1.CronTrialPhaseFailed:
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown phase", "phase", cronTrial.Status.Phase)
		return ctrl.Result{}, nil
	}
}

func (r *CronTrialReconciler) reconcileIdle(ctx context.Context, cronTrial *temperv1alpha1.CronTrial) (ctrl.Result, error) {
	loc, err := time.LoadLocation(cronTrial.Spec.Timezone)
	if err != nil {
		return r.failCronTrial(ctx, cronTrial, fmt.Sprintf("Invalid timezone %q: %v", cronTrial.Spec.Timezone, err))
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronSched, err := parser.Parse(cronTrial.Spec.Schedule)
	if err != nil {
		return r.failCronTrial(ctx, cronTrial, fmt.Sprintf("Invalid cron expression %q: %v", cronTrial.Spec.Schedule, err))
	}

	now := time.Now().In(loc)
	var lastRun time.Time
	if cronTrial.Status.LastScheduleTime != nil {
		lastRun = cronTrial.Status.LastScheduleTime.Time
	} else {
		lastRun = cronTrial.CreationTimestamp.Time
	}

	nextRun := cronSched.Next(lastRun)
	if now.Before(nextRun) {
		// Not time yet — requeue at next fire time.
		return ctrl.Result{RequeueAfter: nextRun.Sub(now)}, nil
	}
	// Time to fire — create a Trial.
	return r.createTrial(ctx, cronTrial)
}

func (r *CronTrialReconciler) reconcileRunning(ctx context.Context, cronTrial *temperv1alpha1.CronTrial) (ctrl.Result, error) {
	if cronTrial.Status.ActiveTrialName == nil {
		// Shouldn't happen — recover by going back to Idle.
		log := logf.FromContext(ctx)
		log.Info("Running phase with no active trial, recovering to Idle")
		cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseIdle
		if err := r.Status().Update(ctx, cronTrial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status to Idle: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	var trial temperv1alpha1.Trial
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: cronTrial.Namespace,
		Name:      *cronTrial.Status.ActiveTrialName,
	}, &trial); err != nil {
		if apierrors.IsNotFound(err) {
			// Trial was deleted externally — go back to Idle.
			cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseIdle
			cronTrial.Status.ActiveTrialName = nil
			if err := r.Status().Update(ctx, cronTrial); err != nil {
				return ctrl.Result{}, fmt.Errorf("update status to Idle: %w", err)
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get active trial: %w", err)
	}

	switch trial.Status.Phase {
	case temperv1alpha1.TrialPhaseCompleted:
		cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseIdle
		cronTrial.Status.ActiveTrialName = nil
		cronTrial.Status.History.SuccessfulRuns++
		if err := r.Status().Update(ctx, cronTrial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after trial completed: %w", err)
		}
		r.Recorder.Eventf(cronTrial, &trial, "Normal", "TrialCompleted", "TrialCompleted",
			"Trial %s completed", trial.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	case temperv1alpha1.TrialPhaseFailed:
		cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseIdle
		cronTrial.Status.ActiveTrialName = nil
		if err := r.Status().Update(ctx, cronTrial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after trial failed: %w", err)
		}
		r.Recorder.Eventf(cronTrial, &trial, "Warning", "TrialFailed", "TrialFailed",
			"Trial %s failed", trial.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	case temperv1alpha1.TrialPhaseHalted:
		cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseHalted
		cronTrial.Status.ActiveTrialName = nil
		cronTrial.Status.History.HaltedRuns++
		cronTrial.Status.History.LastHaltReason = trial.Status.HaltReason
		if err := r.Status().Update(ctx, cronTrial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after trial halted: %w", err)
		}

		reason := "unknown"
		if trial.Status.HaltReason != nil {
			reason = *trial.Status.HaltReason
		}
		r.Recorder.Eventf(cronTrial, &trial, "Warning", "TrialHalted", "TrialHalted", "Trial %s halted: %s",
			trial.Name, reason)
		return ctrl.Result{}, nil
	default:
		// Still running — wait for Owns() watch to notify us.
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
}

func (r *CronTrialReconciler) failCronTrial(ctx context.Context, cronTrial *temperv1alpha1.CronTrial, reason string) (ctrl.Result, error) {
	cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseFailed
	if err := r.Status().Update(ctx, cronTrial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status to Failed: %w", err)
	}
	r.Recorder.Eventf(cronTrial, nil, "Warning", "Failed", "Failed", reason)
	return ctrl.Result{}, nil
}

func (r *CronTrialReconciler) createTrial(ctx context.Context, cronTrial *temperv1alpha1.CronTrial) (ctrl.Result, error) {
	var template temperv1alpha1.Trial

	if err := r.Get(ctx, client.ObjectKey{
		Namespace: cronTrial.Namespace,
		Name:      cronTrial.Spec.TrialRef,
	}, &template); err != nil {
		return r.failCronTrial(ctx, cronTrial, fmt.Sprintf("Get trial template %q: %v", cronTrial.Spec.TrialRef, err))
	}

	safe, reason, err := r.checkSafeguards(ctx, cronTrial, &template)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("check safeguards: %w", err)
	}
	if !safe {
		r.Recorder.Eventf(cronTrial, nil, "Warning", "SafeguardBlocked", "SafeguardBlocked",
			"Skipping run: %s", reason)
		// Update LastScheduleTime so we don't retry this fire time.
		cronTrial.Status.LastScheduleTime = new(metav1.Now())
		if err := r.Status().Update(ctx, cronTrial); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after safeguard block: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Create a new trial from the template.
	trialName := fmt.Sprintf("%s-%d", cronTrial.Name, time.Now().Unix())

	trial := &temperv1alpha1.Trial{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cronTrial.Namespace,
			Name:      trialName,
			Labels:    map[string]string{temperv1alpha1.LabelCronTrial: cronTrial.Name},
		},
		Spec: template.Spec,
	}
	// Owner reference — CronTrial owns the trial.
	if err := controllerutil.SetControllerReference(cronTrial, trial, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("set owner reference: %w", err)
	}

	if err := r.Create(ctx, trial); err != nil {
		return r.failCronTrial(ctx, cronTrial, fmt.Sprintf("create trial: %v", err))
	}

	// Update CronTrial status.
	cronTrial.Status.Phase = temperv1alpha1.CronTrialPhaseRunning
	cronTrial.Status.ActiveTrialName = new(trialName)
	cronTrial.Status.LastScheduleTime = new(metav1.Now())
	cronTrial.Status.History.TotalRuns++

	if err := r.Status().Update(ctx, cronTrial); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status after creating trial: %w", err)
	}
	r.Recorder.Eventf(cronTrial, trial, "Normal", "TrialCreated", "TrialCreated",
		"Created trial %s", trialName)

	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// checkSafeguards delegates to the shared safeguard.CheckSafeguards helper,
// resolving the CronTrial's pointer-bearing spec into the plain string
// arguments the helper expects. The CronTrial's own namespace is used for
// metric attribution (resource attribution per trialset-design.md); the target
// namespace is the Trial template's target.namespace if set, else the
// CronTrial's namespace (same-namespace default).
func (r *CronTrialReconciler) checkSafeguards(ctx context.Context, cronTrial *temperv1alpha1.CronTrial, template *temperv1alpha1.Trial) (bool, string, error) {
	targetNamespace := cronTrial.Namespace
	if template.Spec.Target.Namespace != nil {
		targetNamespace = *template.Spec.Target.Namespace
	}
	targetName := ""
	if template.Spec.Target.Name != nil {
		targetName = *template.Spec.Target.Name
	}
	return safeguard.CheckSafeguards(ctx, r.Client, cronTrial.Namespace, targetNamespace, targetName,
		cronTrial.Spec.Safeguards, r.NewAlertChecker, r.NewMetricsQuerier)
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronTrialReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&temperv1alpha1.CronTrial{}).
		Owns(&temperv1alpha1.Trial{}).
		Named("crontrial").
		Complete(r)
}
