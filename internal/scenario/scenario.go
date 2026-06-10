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

// RecoveryProbe defines how to detect that the system recovered.
// Exactly one of Condition or Query must be set.
type RecoveryProbe struct {
	// Condition watches a Kubernetes resource condition (e.g., Deployment Available=True).
	Condition *ConditionProbe

	// Query checks a PromQL query result (e.g., redis_connected_clients > 0).
	Query *QueryProbe
}

// Scenario defines the contract for all fault injection types.
//
// Callers persist injection intent (Status.InjectedAt) before calling Inject,
// so a crash or failed status write after a successful Inject never causes a
// second injection. Implementations can assume at-most-once Inject per run.
type Scenario interface {
	// Inject applies the fault. It must be safe to retry on failure.
	Inject(ctx context.Context, target Target) error

	// Revert undoes the fault. It must be idempotent and safe to call
	// even if Inject was never called or already reverted.
	Revert(ctx context.Context, target Target) error

	// RecoveryProbe returns what to watch to know the system recovered.
	RecoveryProbe() RecoveryProbe
}
