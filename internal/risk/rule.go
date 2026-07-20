package risk

import (
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// Snapshot is the read-only view of the target's cluster state that rules
// evaluate against: the normalized workload plus the pods and
// PodDisruptionBudgets relevant to it. Building it is the only part of risk
// detection that touches the cluster; rules themselves are pure.
type Snapshot struct {
	Workload workload
	Pods     []corev1.Pod
	PDBs     []policyv1.PodDisruptionBudget
}

// Rule encapsulates one resilience risk condition. Each supported RiskRule is
// implemented by its own struct satisfying this interface:
//
//   - Detect creates the Risk when the condition currently holds on the
//     snapshot, or returns nil when it does not.
//   - Mitigated reports that the condition no longer holds, so a previously
//     recorded risk for this rule can be removed.
//
// Detect and Mitigated are two views of the same condition and must stay in
// agreement: Mitigated(s) == (Detect(s) == nil).
type Rule interface {
	// ID returns the bounded machine token identifying this rule.
	ID() temperv1alpha1.RiskRule
	// Detect returns the risk when the rule's condition holds, else nil.
	Detect(s Snapshot) *temperv1alpha1.Risk
	// Mitigated reports whether the rule's condition has cleared.
	Mitigated(s Snapshot) bool
}

// rules is the deterministic, ordered registry of every supported rule. The
// reconciler iterates it instead of switching over RiskRule tokens; adding a
// rule means adding a struct and registering it here.
var rules = []Rule{
	singleReplicaRule{},
	noPodAntiAffinityRule{},
	missingReadinessProbeRule{},
	noPodDisruptionBudgetRule{},
	concentratedPlacementRule{},
}

// singleReplicaRule flags a workload configured with a single replica.
type singleReplicaRule struct{}

func (singleReplicaRule) ID() temperv1alpha1.RiskRule { return temperv1alpha1.RiskSingleReplica }

func (r singleReplicaRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkSingleReplica(s.Workload)
}

func (r singleReplicaRule) Mitigated(s Snapshot) bool { return r.Detect(s) == nil }

// noPodAntiAffinityRule flags a pod template with neither pod anti-affinity
// nor topology spread constraints.
type noPodAntiAffinityRule struct{}

func (noPodAntiAffinityRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskNoPodAntiAffinity
}

func (r noPodAntiAffinityRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkNoPodAntiAffinity(s.Workload)
}

func (r noPodAntiAffinityRule) Mitigated(s Snapshot) bool { return r.Detect(s) == nil }

// missingReadinessProbeRule flags containers lacking a readiness probe.
type missingReadinessProbeRule struct{}

func (missingReadinessProbeRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskMissingReadinessProbe
}

func (r missingReadinessProbeRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkMissingReadinessProbe(s.Workload)
}

func (r missingReadinessProbeRule) Mitigated(s Snapshot) bool { return r.Detect(s) == nil }

// noPodDisruptionBudgetRule flags a workload no PodDisruptionBudget selects.
type noPodDisruptionBudgetRule struct{}

func (noPodDisruptionBudgetRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskNoPodDisruptionBudget
}

func (r noPodDisruptionBudgetRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkNoPodDisruptionBudget(s.Workload, s.PDBs)
}

func (r noPodDisruptionBudgetRule) Mitigated(s Snapshot) bool { return r.Detect(s) == nil }

// concentratedPlacementRule flags running pods all packed onto a single node.
type concentratedPlacementRule struct{}

func (concentratedPlacementRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskConcentratedPlacement
}

func (r concentratedPlacementRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkConcentratedPlacement(s.Pods)
}

func (r concentratedPlacementRule) Mitigated(s Snapshot) bool { return r.Detect(s) == nil }
