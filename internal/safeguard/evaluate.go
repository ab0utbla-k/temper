package safeguard

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/metrics"
)

// CheckSafeguards evaluates the per-target safeguards before a Trial is created.
//
// It is shared by the CronTrial and TrialSet controllers. The callers resolve
// their own pointer-bearing spec into the plain string arguments here.
//
// namespace is both where the target Deployment lives and the metric label:
// the owning CronTrial/TrialSet, its Trials, and their targets always share
// one namespace.
//
// targetName is the Deployment name. When empty, all checks are skipped
// (safeguards only make sense against a concrete target).
//
// Returns (safe, reason, err). When safe is false and err is nil, reason is a
// human-readable explanation suitable for an event; the caller should skip the
// run and record LastScheduleTime.
func CheckSafeguards(
	ctx context.Context,
	c client.Client,
	namespace string,
	targetName string,
	sg *temperv1alpha1.Safeguards,
	newAlertChecker func(string) (AlertChecker, error),
	newMetricsQuerier func(string) (MetricsQuerier, error),
) (bool, string, error) {
	if sg == nil || targetName == "" {
		return true, "", nil
	}

	if sg.MinReplicasAvailable != nil || sg.MaxUnavailable != nil {
		metrics.SafeguardChecksTotal.WithLabelValues(namespace, metrics.SafeguardTypeReplicas).Inc()

		var dep appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKey{
			Namespace: namespace,
			Name:      targetName,
		}, &dep); err != nil {
			return false, "", fmt.Errorf("get deployment: %w", err)
		}

		if sg.MinReplicasAvailable != nil && dep.Status.AvailableReplicas < *sg.MinReplicasAvailable {
			return false, fmt.Sprintf("available replicas %d < minimum %d",
				dep.Status.AvailableReplicas, *sg.MinReplicasAvailable), nil
		}
		if sg.MaxUnavailable != nil && dep.Status.UnavailableReplicas > *sg.MaxUnavailable {
			return false, fmt.Sprintf("unavailable replicas %d > maximum %d",
				dep.Status.UnavailableReplicas, *sg.MaxUnavailable), nil
		}
	}

	if sg.AlertSource != nil {
		checker, err := newAlertChecker(sg.AlertSource.URL)
		if err != nil {
			return false, fmt.Sprintf("create alert checker: %v", err), nil
		}

		metrics.SafeguardChecksTotal.WithLabelValues(namespace, metrics.SafeguardTypeAlerts).Inc()

		_, reason, err := CheckAlertsFiring(ctx, sg.HaltOnAlertLabels, checker)
		if err != nil {
			return false, fmt.Sprintf("check alerts: %v", err), nil
		}

		if reason != "" {
			return false, reason, nil
		}
	}

	if sg.MetricsSource != nil && sg.SLOProtection != nil {
		querier, err := newMetricsQuerier(sg.MetricsSource.URL)
		if err != nil {
			return false, fmt.Sprintf("create metrics querier: %v", err), nil
		}

		metrics.SafeguardChecksTotal.WithLabelValues(namespace, metrics.SafeguardTypeSLO).Inc()

		_, reason, err := CheckSLOBreach(ctx, sg.SLOProtection, querier)
		if err != nil {
			return false, fmt.Sprintf("check SLO: %v", err), nil
		}

		if reason != "" {
			return false, reason, nil
		}
	}

	return true, "", nil
}
