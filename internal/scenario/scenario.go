package scenario

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Target identifies the workload to inject faults into.
type Target struct {
	Name      string
	Namespace string
	Kind      string
	Labels    map[string]string
}

// ConditionProbe watches a Kubernetes condition on the target resource.
type ConditionProbe struct {
	Type   string
	Status metav1.ConditionStatus
}

// QueryProbe checks a PromQL query. Recovery is when the result is non-zero.
type QueryProbe struct {
	Query string
}

// WorkloadReadyProbe reports recovery when the target is back to full
// strength: all desired replicas ready and the observed status current.
// Unlike the Available condition, it does not tolerate partial disruption.
type WorkloadReadyProbe struct{}

// RecoveryProbe defines how to detect that the system recovered.
// Exactly one of Condition, Query, or WorkloadReady must be set.
type RecoveryProbe struct {
	// Condition watches a Kubernetes resource condition (e.g., Deployment Available=True).
	Condition *ConditionProbe

	// Query checks a PromQL query result (e.g., redis_connected_clients > 0).
	Query *QueryProbe

	// WorkloadReady waits for readyReplicas == desired with current status.
	WorkloadReady *WorkloadReadyProbe
}

// Result reports what a scenario's Inject did. Ignored when Inject errors.
type Result struct {
	// PodsAffected is how many pods Inject acted on (killed, evicted, …).
	PodsAffected int

	// Findings are noteworthy conditions that are NOT failures (e.g. a
	// PDB-blocked eviction). The controller surfaces each as an event + status.
	Findings []Finding
}

// Finding is one noteworthy, non-fatal condition from an injection.
type Finding struct {
	Pod    string
	Reason string
}

// Scenario defines the contract for all fault injection types.
//
// Callers persist injection intent (Status.InjectedAt) before calling Inject,
// so a crash or failed status write after a successful Inject never causes a
// second injection. Implementations can assume at-most-once Inject per run.
type Scenario interface {
	// Inject applies the fault. It must be safe to retry on failure.
	Inject(ctx context.Context, target Target) (Result, error)

	// Revert undoes the fault. It must be idempotent and safe to call
	// even if Inject was never called or already reverted.
	Revert(ctx context.Context, target Target) error

	// RecoveryProbe returns what to watch to know the system recovered.
	RecoveryProbe() RecoveryProbe
}
