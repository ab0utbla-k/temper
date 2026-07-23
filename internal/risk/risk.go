// Package risk detects resilience weaknesses on a Trial's target workload
// and classifies them per scenario.
//
// Detection is strictly read-only: it reads the target workload, its running
// pods, and any PodDisruptionBudgets, and returns a list of Risks. It never
// mutates cluster state.
//
// Each rule also declares which scenario types its condition is a
// prerequisite for (Rule.AppliesTo), and Relevant filters a risk list down to
// the risks that matter for a given scenario set. What to do with the result
// is the caller's decision: the Trial controller records risks in status, a
// sweep skips workloads whose relevant list is not empty, and the passport
// refuses eligibility while relevant risks remain.
package risk

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// workload is the normalized view of a target we need for risk detection,
// extracted from either a Deployment or a StatefulSet.
type workload struct {
	replicas int32
	selector *metav1.LabelSelector
	template corev1.PodTemplateSpec
}

// Detect reads the target workload and returns the resilience risks found on
// it. Kind must be "Deployment" or "StatefulSet". The returned slice is nil
// when no risks are found (no false positives). It is read-only.
func Detect(ctx context.Context, c client.Client, kind, namespace, name string) ([]temperv1alpha1.Risk, error) {
	wl, err := loadWorkload(ctx, c, kind, namespace, name)
	if err != nil {
		return nil, err
	}

	selector, err := metav1.LabelSelectorAsSelector(wl.selector)
	if err != nil {
		return nil, fmt.Errorf("parse selector: %w", err)
	}

	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	var pdbList policyv1.PodDisruptionBudgetList
	if err := c.List(ctx, &pdbList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list poddisruptionbudgets: %w", err)
	}

	snapshot := Snapshot{
		Workload: wl,
		Pods:     podList.Items,
		PDBs:     pdbList.Items,
	}
	return evaluate(ctx, snapshot), nil
}

// loadWorkload reads the target and normalizes it into a workload view.
func loadWorkload(ctx context.Context, c client.Client, kind, namespace, name string) (workload, error) {
	key := client.ObjectKey{Namespace: namespace, Name: name}

	switch kind {
	case "Deployment":
		var dep appsv1.Deployment
		if err := c.Get(ctx, key, &dep); err != nil {
			return workload{}, fmt.Errorf("get deployment: %w", err)
		}
		return workload{
			replicas: replicasOrDefault(dep.Spec.Replicas),
			selector: dep.Spec.Selector,
			template: dep.Spec.Template,
		}, nil
	case "StatefulSet":
		var sts appsv1.StatefulSet
		if err := c.Get(ctx, key, &sts); err != nil {
			return workload{}, fmt.Errorf("get statefulset: %w", err)
		}
		return workload{
			replicas: replicasOrDefault(sts.Spec.Replicas),
			selector: sts.Spec.Selector,
			template: sts.Spec.Template,
		}, nil
	default:
		return workload{}, fmt.Errorf("unsupported target kind for risk detection: %q", kind)
	}
}

// replicasOrDefault mirrors the Kubernetes default of 1 replica when the field
// is unset.
func replicasOrDefault(r *int32) int32 {
	if r == nil {
		return 1
	}
	return *r
}

// evaluate iterates the ordered rule registry over the snapshot and collects
// the risks whose conditions hold. Pure aside from logging: no cluster
// access, so every rule is unit-testable in isolation. Registry order keeps
// output deterministic. Per-rule outcomes are logged at V(1) so the
// evaluation of every rule is traceable without flooding the default level
// on each reconcile pass.
func evaluate(ctx context.Context, s Snapshot) []temperv1alpha1.Risk {
	log := logf.FromContext(ctx)

	var risks []temperv1alpha1.Risk

	for _, rule := range rules {
		r := rule.Detect(s)
		if r == nil {
			log.V(1).Info("Evaluated risk rule", "rule", rule.ID(), "detected", false)
			continue
		}
		log.V(1).Info("Evaluated risk rule", "rule", rule.ID(), "detected", true,
			"message", r.Message)
		risks = append(risks, *r)
	}

	return risks
}

// checkSingleReplica flags a workload configured with a single replica: any
// disruption takes the whole workload down.
func checkSingleReplica(wl workload) *temperv1alpha1.Risk {
	if wl.replicas > 1 {
		return nil
	}
	return &temperv1alpha1.Risk{
		Rule:    temperv1alpha1.RiskSingleReplica,
		Message: "Target runs a single replica; any disruption takes the whole workload down. Increase spec.replicas to at least 2.",
	}
}

// checkNoPodAntiAffinity flags a pod template that defines neither pod
// anti-affinity nor topology spread constraints, leaving the scheduler free to
// pack all replicas onto one node.
func checkNoPodAntiAffinity(wl workload) *temperv1alpha1.Risk {
	spec := wl.template.Spec

	hasAntiAffinity := spec.Affinity != nil && spec.Affinity.PodAntiAffinity != nil
	hasTopologySpread := len(spec.TopologySpreadConstraints) > 0

	if hasAntiAffinity || hasTopologySpread {
		return nil
	}
	return &temperv1alpha1.Risk{
		Rule:    temperv1alpha1.RiskNoPodAntiAffinity,
		Message: "Pod template defines no pod anti-affinity or topology spread constraints, so nothing stops the scheduler from packing every replica onto one node. This is a latent risk: pods may be well spread right now purely by chance, and ConcentratedPlacement reports where they actually sit. Add podAntiAffinity or topologySpreadConstraints.",
	}
}

// checkMissingReadinessProbe flags a workload where at least one container has
// no readiness probe. Only readiness is checked: it is what gates traffic and
// therefore disruption safety. A liveness probe is deliberately not required —
// an unnecessary one mostly buys restart loops.
//
// Note this proves a probe exists, not that it is honest: a readiness probe
// that returns 200 before the app can actually serve still passes here.
// Catching that is the experiment's job, not the linter's.
func checkMissingReadinessProbe(wl workload) *temperv1alpha1.Risk {
	for _, ctr := range wl.template.Spec.Containers {
		if ctr.ReadinessProbe == nil {
			return &temperv1alpha1.Risk{
				Rule:    temperv1alpha1.RiskMissingReadinessProbe,
				Message: fmt.Sprintf("Container %q has no readiness probe; Kubernetes sends traffic to it as soon as it starts, before it can serve. Define a readinessProbe.", ctr.Name),
			}
		}
	}
	return nil
}

// checkNoPodDisruptionBudget flags a workload with no PodDisruptionBudget whose
// selector matches its pods, leaving voluntary disruptions unbounded.
func checkNoPodDisruptionBudget(wl workload, pdbs []policyv1.PodDisruptionBudget) *temperv1alpha1.Risk {
	podLabels := labels.Set(wl.template.Labels)

	for i := range pdbs {
		if pdbs[i].Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdbs[i].Spec.Selector)
		if err != nil {
			// A malformed PDB selector cannot be said to protect the target;
			// keep scanning the rest.
			continue
		}
		// An empty ({}) selector is not "no selector": in policy/v1 it selects
		// every pod in the namespace, so such a PDB does protect the target.
		// (A null selector matches nothing and is skipped above.)
		if selector.Empty() || selector.Matches(podLabels) {
			return nil
		}
	}
	return &temperv1alpha1.Risk{
		Rule:    temperv1alpha1.RiskNoPodDisruptionBudget,
		Message: "No PodDisruptionBudget protects the target; voluntary disruptions (e.g. node drains) are unbounded. Define a PodDisruptionBudget.",
	}
}

// checkConcentratedPlacement flags a workload whose running pods all share a
// single node, so a single node loss disrupts every replica. It only fires
// when more than one pod is running.
func checkConcentratedPlacement(pods []corev1.Pod) *temperv1alpha1.Risk {
	nodes := make(map[string]struct{})
	running := 0

	for i := range pods {
		if pods[i].Status.Phase != corev1.PodRunning {
			continue
		}
		running++
		if node := pods[i].Spec.NodeName; node != "" {
			nodes[node] = struct{}{}
		}
	}

	if running < 2 || len(nodes) != 1 {
		return nil
	}
	return &temperv1alpha1.Risk{
		Rule:    temperv1alpha1.RiskConcentratedPlacement,
		Message: "All running pods are scheduled on a single node; losing that node disrupts every replica. Spread pods across nodes.",
	}
}
