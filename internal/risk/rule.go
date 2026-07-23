package risk

import (
	"slices"

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
//   - AppliesTo declares which scenario types the condition is a
//     prerequisite for. A workload carrying a relevant risk has a known-bad
//     answer to that scenario, so a sweep skips it instead of injecting.
type Rule interface {
	// ID returns the bounded machine token identifying this rule.
	ID() temperv1alpha1.RiskRule
	// Detect returns the risk when the rule's condition holds, else nil.
	Detect(s Snapshot) *temperv1alpha1.Risk
	// AppliesTo reports whether this rule is a prerequisite for the given
	// scenario type.
	AppliesTo(t temperv1alpha1.ScenarioType) bool
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

// rulesByID indexes the registry for lookup by token. Derived from rules in
// init, never written by hand — the slice stays the single source of truth.
var rulesByID = make(map[temperv1alpha1.RiskRule]Rule, len(rules))

// Relevant filters risks down to those whose rule is a prerequisite for at
// least one of the given scenario types. A risk whose token is not in the
// registry is kept: it cannot be proven harmless, so it stays relevant —
// fail closed, the same choice safeguards make when a check errors. The
// returned slice is nil when nothing is relevant.
func Relevant(risks []temperv1alpha1.Risk, types []temperv1alpha1.ScenarioType) []temperv1alpha1.Risk {
	var relevant []temperv1alpha1.Risk

	for _, risk := range risks {
		rule, ok := rulesByID[risk.Rule]
		if !ok {
			// Unknown token: we cannot prove it is harmless, so we keep it.
			relevant = append(relevant, risk)
			continue
		}

		if slices.ContainsFunc(types, rule.AppliesTo) {
			relevant = append(relevant, risk)
		}
	}

	return relevant
}

// singleReplicaRule flags a workload configured with a single replica.
type singleReplicaRule struct{}

func (singleReplicaRule) ID() temperv1alpha1.RiskRule { return temperv1alpha1.RiskSingleReplica }

func (r singleReplicaRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkSingleReplica(s.Workload)
}

func (singleReplicaRule) AppliesTo(_ temperv1alpha1.ScenarioType) bool { return true }

// noPodAntiAffinityRule flags a pod template with neither pod anti-affinity
// nor topology spread constraints.
type noPodAntiAffinityRule struct{}

func (noPodAntiAffinityRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskNoPodAntiAffinity
}

func (r noPodAntiAffinityRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkNoPodAntiAffinity(s.Workload)
}

func (noPodAntiAffinityRule) AppliesTo(t temperv1alpha1.ScenarioType) bool { return nodeScoped(t) }

// missingReadinessProbeRule flags containers lacking a readiness probe.
type missingReadinessProbeRule struct{}

func (missingReadinessProbeRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskMissingReadinessProbe
}

func (r missingReadinessProbeRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkMissingReadinessProbe(s.Workload)
}

func (missingReadinessProbeRule) AppliesTo(_ temperv1alpha1.ScenarioType) bool { return true }

// noPodDisruptionBudgetRule flags a workload no PodDisruptionBudget selects.
type noPodDisruptionBudgetRule struct{}

func (noPodDisruptionBudgetRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskNoPodDisruptionBudget
}

func (r noPodDisruptionBudgetRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkNoPodDisruptionBudget(s.Workload, s.PDBs)
}

// Only node-drain evicts through the PDB; spot-reclaim and pod-kill delete
// pods directly and never consult it.
func (noPodDisruptionBudgetRule) AppliesTo(t temperv1alpha1.ScenarioType) bool {
	return t == temperv1alpha1.ScenarioTypeNodeDrain
}

// concentratedPlacementRule flags running pods all packed onto a single node.
type concentratedPlacementRule struct{}

func (concentratedPlacementRule) ID() temperv1alpha1.RiskRule {
	return temperv1alpha1.RiskConcentratedPlacement
}

func (r concentratedPlacementRule) Detect(s Snapshot) *temperv1alpha1.Risk {
	return checkConcentratedPlacement(s.Pods)
}

func (concentratedPlacementRule) AppliesTo(t temperv1alpha1.ScenarioType) bool { return nodeScoped(t) }

// nodeScoped reports whether the scenario disrupts at node granularity
// (node-drain, spot-reclaim). The placement rules are prerequisites only for
// these: replicas packed onto one node are harmless to a single pod kill but
// all go down together when that node goes away.
func nodeScoped(t temperv1alpha1.ScenarioType) bool {
	return t == temperv1alpha1.ScenarioTypeNodeDrain || t == temperv1alpha1.ScenarioTypeSpotReclaim
}

// init builds rulesByID from the registry. A duplicate ID is a programming
// error (a copy-pasted rule with an unchanged token), so it crashes at
// startup instead of silently dropping a rule from lookups.
func init() {
	for _, r := range rules {
		if _, dup := rulesByID[r.ID()]; dup {
			panic("duplicate risk rule ID: " + r.ID())
		}
		rulesByID[r.ID()] = r
	}
}
