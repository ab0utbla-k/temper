package safeguard

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/metrics"
)

type Watcher struct {
	client client.Client
	// recorder is held for future use; the watcher will emit events when
	// transient safeguard checks fail.
	recorder            events.EventRecorder
	consecutiveFailures map[string]int
	failureThreshold    int
	newAlertChecker     func(string) (AlertChecker, error)
	newMetricsQuerier   func(string) (MetricsQuerier, error)
}

func NewWatcher(c client.Client, rec events.EventRecorder) *Watcher {
	return &Watcher{
		client:              c,
		recorder:            rec,
		consecutiveFailures: make(map[string]int),
		failureThreshold:    3,
		newAlertChecker:     func(url string) (AlertChecker, error) { return NewAlertmanagerChecker(url) },
		newMetricsQuerier:   func(url string) (MetricsQuerier, error) { return NewPrometheusQuerier(url) },
	}
}

func (w *Watcher) Start(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.checkAll(ctx)
		}
	}
}

func (w *Watcher) NeedLeaderElection() bool {
	return true
}

func (w *Watcher) checkAll(ctx context.Context) {
	log := logf.FromContext(ctx)
	var cronTrialList temperv1alpha1.CronTrialList
	if err := w.client.List(ctx, &cronTrialList); err != nil {
		log.Error(err, "Failed to list CronTrials")
		return
	}

	for i := range cronTrialList.Items {
		cronTrial := &cronTrialList.Items[i]
		if cronTrial.Status.Phase != temperv1alpha1.CronTrialPhaseRunning || cronTrial.Status.ActiveTrialName == nil {
			continue
		}

		w.checkCronTrial(ctx, cronTrial)
	}
}

func (w *Watcher) checkCronTrial(ctx context.Context, cronTrial *temperv1alpha1.CronTrial) {
	log := logf.FromContext(ctx)
	sg := cronTrial.Spec.Safeguards

	if sg == nil {
		return
	}

	haltCode, haltDetail, checkErr := w.checkAlerts(ctx, cronTrial.Namespace, sg)
	if haltCode == "" && checkErr == nil {
		haltCode, haltDetail, checkErr = w.checkSLO(ctx, cronTrial.Namespace, sg)
	}

	key := fmt.Sprintf("%s/%s", cronTrial.Namespace, cronTrial.Name)
	needsReplicaCheck := sg.MinReplicasAvailable != nil || sg.MaxUnavailable != nil

	if haltCode == "" && checkErr == nil && !needsReplicaCheck {
		delete(w.consecutiveFailures, key)
		return
	}

	var trial temperv1alpha1.Trial
	if err := w.client.Get(ctx, client.ObjectKey{
		Namespace: cronTrial.Namespace,
		Name:      *cronTrial.Status.ActiveTrialName,
	}, &trial); err != nil {
		log.Error(err, "Failed to get active trial", "trial", *cronTrial.Status.ActiveTrialName)
		return
	}

	if haltCode == "" && needsReplicaCheck {
		if trial.Spec.Target.Name == nil {
			log.Info("Skipping replica check: no target name", "cronTrial", key)
		} else {
			metrics.SafeguardChecksTotal.WithLabelValues(cronTrial.Namespace, metrics.SafeguardTypeReplicas).Inc()

			var dep appsv1.Deployment
			if err := w.client.Get(ctx, client.ObjectKey{
				Namespace: cronTrial.Namespace,
				Name:      *trial.Spec.Target.Name,
			}, &dep); err != nil {
				checkErr = err
			} else {
				if sg.MinReplicasAvailable != nil && dep.Status.AvailableReplicas < *sg.MinReplicasAvailable {
					haltCode = temperv1alpha1.HaltCodeReplicaMin
					haltDetail = fmt.Sprintf("Available replicas %d < minimum %d", dep.Status.AvailableReplicas, *sg.MinReplicasAvailable)
				} else if sg.MaxUnavailable != nil && dep.Status.UnavailableReplicas > *sg.MaxUnavailable {
					haltCode = temperv1alpha1.HaltCodeReplicaMax
					haltDetail = fmt.Sprintf("Unavailable replicas %d > maximum %d", dep.Status.UnavailableReplicas, *sg.MaxUnavailable)
				}
			}
		}
	}

	switch {
	case haltCode != "":
		if err := w.haltTrial(ctx, &trial, haltCode, haltDetail); err != nil {
			log.Error(err, "Failed to annotate trial for halt",
				"trial", trial.Name, "code", haltCode, "reason", haltDetail)
			return
		}

		log.Info(
			"Halting trial",
			"trial", trial.Name, "cronTrial", key, "code", haltCode, "reason", haltDetail)

		delete(w.consecutiveFailures, key)
	case checkErr != nil:
		w.consecutiveFailures[key]++

		log.Info(
			"Safeguard check failed",
			"cronTrial", key,
			"consecutive", w.consecutiveFailures[key],
			"threshold", w.failureThreshold,
			"error", checkErr,
		)

		if w.consecutiveFailures[key] >= w.failureThreshold {
			haltCode = temperv1alpha1.HaltCodeUnreachable
			haltDetail = fmt.Sprintf("Safeguard checks unreachable for %ds: %v", w.failureThreshold*5, checkErr)

			if err := w.haltTrial(ctx, &trial, haltCode, haltDetail); err != nil {
				log.Error(err, "Failed to annotate trial for halt",
					"trial", trial.Name, "code", haltCode, "reason", haltDetail)
				return
			}
			log.Info("Halting trial",
				"trial", trial.Name, "cronTrial", key, "code", haltCode, "reason", haltDetail)
			delete(w.consecutiveFailures, key)
		}
	default:
		delete(w.consecutiveFailures, key)
	}
}

func (w *Watcher) checkAlerts(ctx context.Context, namespace string, sg *temperv1alpha1.Safeguards) (temperv1alpha1.HaltCode, string, error) {
	if sg.AlertSource == nil {
		return "", "", nil
	}

	checker, err := w.newAlertChecker(sg.AlertSource.URL)
	if err != nil {
		return temperv1alpha1.HaltCodeConfigError, fmt.Sprintf("Invalid alert source config: %v", err), nil
	}

	metrics.SafeguardChecksTotal.WithLabelValues(namespace, metrics.SafeguardTypeAlerts).Inc()

	return CheckAlertsFiring(ctx, sg.HaltOnAlertLabels, checker)
}

func (w *Watcher) checkSLO(ctx context.Context, namespace string, sg *temperv1alpha1.Safeguards) (temperv1alpha1.HaltCode, string, error) {
	if sg.MetricsSource == nil || sg.SLOProtection == nil {
		return "", "", nil
	}

	querier, err := w.newMetricsQuerier(sg.MetricsSource.URL)
	if err != nil {
		return temperv1alpha1.HaltCodeConfigError, fmt.Sprintf("Invalid metrics source config: %v", err), nil
	}

	metrics.SafeguardChecksTotal.WithLabelValues(namespace, metrics.SafeguardTypeSLO).Inc()

	return CheckSLOBreach(ctx, sg.SLOProtection, querier)
}

func (w *Watcher) haltTrial(ctx context.Context, trial *temperv1alpha1.Trial, code temperv1alpha1.HaltCode, detail string) error {
	if trial.Annotations == nil {
		trial.Annotations = make(map[string]string)
	}
	trial.Annotations[temperv1alpha1.AnnotationHaltReason] = detail
	trial.Annotations[temperv1alpha1.AnnotationHaltCode] = string(code)

	if err := w.client.Update(ctx, trial); err != nil {
		return err
	}

	metrics.SafeguardHaltsTotal.WithLabelValues(trial.Namespace, string(code)).Inc()
	return nil
}
