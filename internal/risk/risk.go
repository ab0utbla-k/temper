// Package risk detects resilience weaknesses on a Trial's target workload.
//
// Detection is strictly read-only: it reads the target workload, its running
// pods, and any PodDisruptionBudgets, and returns a list of advisory Risks.
// It never mutates cluster state and never fails a Trial — callers surface the
// result in status only.
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
	return evaluate(snapshot), nil
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
// the risks whose conditions hold. Pure: no cluster access, so every rule is
// unit-testable in isolation. Registry order keeps output deterministic.
func evaluate(s Snapshot) []temperv1alpha1.Risk {
	var risks []temperv1alpha1.Risk

	for _, rule := range rules {
		if r := rule.Detect(s); r != nil {
			risks = append(risks, *r)
		}
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
		Message: "Pod template defines no pod anti-affinity or topology spread constraints; replicas may be packed onto one node. Add podAntiAffinity or topologySpreadConstraints to spread pods.",
	}
}

// checkMissingHealthProbes flags a workload where at least one non-init
// container lacks a readiness or liveness probe.
func checkMissingHealthProbes(wl workload) *temperv1alpha1.Risk {
	for _, ctr := range wl.template.Spec.Containers {
		if ctr.ReadinessProbe == nil || ctr.LivenessProbe == nil {
			return &temperv1alpha1.Risk{
				Rule:    temperv1alpha1.RiskMissingHealthProbes,
				Message: fmt.Sprintf("Container %q is missing a readiness or liveness probe; Kubernetes cannot tell when it is ready or needs a restart. Define both probes.", ctr.Name),
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
		if !selector.Empty() && selector.Matches(podLabels) {
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
