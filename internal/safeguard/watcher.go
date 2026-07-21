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

	// TrialSet-generated Trials carry the temper.io/trial-set label. We list ALL
	// Trials (no server-side label filter) and filter in-memory by label
	// presence. controller-runtime's client.MatchingLabels matches label
	// *equality* (key=value), and an empty value matches only labels whose
	// value is literally "", not "the key exists" — there is no built-in
	// label-exists selector in the client API. So a server-side
	// MatchingLabels{LabelTrialSet:""} would MISS real TrialSet Trials (whose
	// label value is the TrialSet name, e.g. "ts-halt"). The in-memory
	// `_, ok := trial.Labels[LabelTrialSet]` check is the correct expression
	// of "label exists". This is a full scan, acceptable because Trial count
	// is bounded by TrialSet activity and the watcher ticks every 5s.
	//
	// For each matched Running-phase Trial, safeguardsForTrial looks up the
	// owning TrialSet (by controller owner ref) and returns its Safeguards.
	// Empty safeguards means skip (no runtime checks). checkTrial is then the
	// shared halt path shared with the CronTrial branch above.
	var trialList temperv1alpha1.TrialList
	if err := w.client.List(ctx, &trialList); err != nil {
		log.Error(err, "Failed to list Trials for TrialSet safeguard checks")
		return
	}
	for i := range trialList.Items {
		trial := &trialList.Items[i]
		if _, ok := trial.Labels[temperv1alpha1.LabelTrialSet]; !ok {
			continue
		}
		if trial.Status.Phase != temperv1alpha1.TrialPhaseRunning {
			continue
		}
		sg := w.safeguardsForTrial(ctx, trial)
		if sg == nil {
			continue
		}
		w.checkTrial(ctx, trial, sg)
	}
}

// safeguardsForTrial looks up the owning TrialSet and returns its Safeguards.
// Returns nil if the Trial has no TrialSet controller owner, the TrialSet
// cannot be found, or the TrialSet has no safeguards configured (skip).
func (w *Watcher) safeguardsForTrial(ctx context.Context, trial *temperv1alpha1.Trial) *temperv1alpha1.Safeguards {
	log := logf.FromContext(ctx)
	for _, ref := range trial.OwnerReferences {
		if ref.Kind != "TrialSet" || ref.Controller == nil || !*ref.Controller {
			continue
		}
		var ts temperv1alpha1.TrialSet
		if err := w.client.Get(ctx, client.ObjectKey{Namespace: trial.Namespace, Name: ref.Name}, &ts); err != nil {
			log.Error(err, "Failed to get owning TrialSet for Trial",
				"trial", trial.Name, "trialSet", ref.Name)
			return nil
		}
		return ts.Spec.Safeguards
	}
	return nil
}

func (w *Watcher) checkCronTrial(ctx context.Context, cronTrial *temperv1alpha1.CronTrial) {
	log := logf.FromContext(ctx)
	sg := cronTrial.Spec.Safeguards
	if sg == nil {
		return
	}
	if cronTrial.Status.ActiveTrialName == nil {
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

	w.checkTrial(ctx, &trial, sg)
}

// checkTrial evaluates all configured safeguards for a single running Trial.
// It is the shared implementation behind both the CronTrial path (via
// checkCronTrial) and the TrialSet path (via checkAll listing
// LabelTrialSet-labeled Trials). If a safeguard trips (haltCode != "") or
// the checks are unreachable (checkErr != nil), the Trial is halted via
// annotations (haltTrial) or tracked in consecutiveFailures until the
// threshold is reached. The key is derived from the Trial's own namespace
// and name so each Trial's failure streak is tracked independently.
func (w *Watcher) checkTrial(ctx context.Context, trial *temperv1alpha1.Trial, sg *temperv1alpha1.Safeguards) {
	log := logf.FromContext(ctx)
	if sg == nil {
		return
	}

	haltCode, haltDetail, checkErr := w.checkAlerts(ctx, trial.Namespace, sg)
	if haltCode == "" && checkErr == nil {
		haltCode, haltDetail, checkErr = w.checkSLO(ctx, trial.Namespace, sg)
	}

	key := fmt.Sprintf("%s/%s", trial.Namespace, trial.Name)
	needsReplicaCheck := sg.MinReplicasAvailable != nil || sg.MaxUnavailable != nil

	if haltCode == "" && checkErr == nil && !needsReplicaCheck {
		delete(w.consecutiveFailures, key)
		return
	}

	if haltCode == "" && needsReplicaCheck {
		if trial.Spec.Target.Name == nil {
			log.Info("Skipping replica check: no target name", "trial", key)
		} else {
			// A Trial may target a Deployment in a different namespace than
			// the Trial/CronTrial that owns it (e.g. TrialSet-generated
			// Trials). Fall back to the Trial's own namespace for the legacy
			// same-namespace path.
			targetNamespace := trial.Namespace
			if trial.Spec.Target.Namespace != nil {
				targetNamespace = *trial.Spec.Target.Namespace
			}

			// Namespace label is the Trial's own namespace (resource
			// attribution), not the target's — intentional per trialset-design.md.
			metrics.SafeguardChecksTotal.WithLabelValues(trial.Namespace, metrics.SafeguardTypeReplicas).Inc()

			var dep appsv1.Deployment
			if err := w.client.Get(ctx, client.ObjectKey{
				Namespace: targetNamespace,
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
		if err := w.haltTrial(ctx, trial, haltCode, haltDetail); err != nil {
			log.Error(err, "Failed to annotate trial for halt",
				"trial", trial.Name, "code", haltCode, "reason", haltDetail)
			return
		}

		log.Info(
			"Halting trial",
			"trial", trial.Name, "key", key, "code", haltCode, "reason", haltDetail)

		delete(w.consecutiveFailures, key)
	case checkErr != nil:
		w.consecutiveFailures[key]++

		log.Info(
			"Safeguard check failed",
			"key", key,
			"consecutive", w.consecutiveFailures[key],
			"threshold", w.failureThreshold,
			"error", checkErr,
		)

		if w.consecutiveFailures[key] >= w.failureThreshold {
			haltCode = temperv1alpha1.HaltCodeUnreachable
			haltDetail = fmt.Sprintf("Safeguard checks unreachable for %ds: %v", w.failureThreshold*5, checkErr)

			if err := w.haltTrial(ctx, trial, haltCode, haltDetail); err != nil {
				log.Error(err, "Failed to annotate trial for halt",
					"trial", trial.Name, "code", haltCode, "reason", haltDetail)
				return
			}
			log.Info("Halting trial",
				"trial", trial.Name, "key", key, "code", haltCode, "reason", haltDetail)
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

	// Namespace label is the Trial's own namespace (resource attribution),
	// not the target's — intentional per trialset-design.md. For
	// CronTrial-generated Trials this equals the CronTrial's namespace.
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

	// Namespace label is the Trial's own namespace (resource attribution),
	// not the target's — intentional per trialset-design.md. For
	// CronTrial-generated Trials this equals the CronTrial's namespace.
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

	// Namespace label is the Trial's own namespace (resource attribution),
	// not the target's — intentional per trialset-design.md. Do not change
	// to *trial.Spec.Target.Namespace.
	metrics.SafeguardHaltsTotal.WithLabelValues(trial.Namespace, string(code)).Inc()
	return nil
}
