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

// HTTPProbe reports recovery when an HTTP GET to URL returns a 2xx status.
// It checks that the service actually answers, not merely that the workload's
// readiness probe claims it is ready.
type HTTPProbe struct {
	URL string
}

// RecoveryProbe defines how to detect that the system recovered.
// Exactly one of Condition, Query, WorkloadReady, or HTTP must be set.
type RecoveryProbe struct {
	// Condition watches a Kubernetes resource condition (e.g., Deployment Available=True).
	Condition *ConditionProbe

	// Query checks a PromQL query result (e.g., redis_connected_clients > 0).
	Query *QueryProbe

	// WorkloadReady waits for readyReplicas == desired with current status.
	WorkloadReady *WorkloadReadyProbe

	// HTTP waits for an HTTP GET to return 2xx (the service actually answers).
	HTTP *HTTPProbe
}

// Result reports what a scenario's Inject did. Ignored when Inject errors.
type Result struct {
	// PodsAffected is how many pods Inject acted on (killed, evicted, …).
	PodsAffected int

	// Findings are noteworthy conditions that are NOT failures (e.g. a
	// PDB-blocked eviction). The controller surfaces each as an event + status.
	Findings []Finding

	// Incomplete reports that Inject has more work to do (e.g. evictions
	// still blocked by a PDB). The controller will call Inject again later;
	// a scenario that sets this must make repeated calls safe. A scenario
	// that never sets it is never called twice.
	Incomplete bool
}

// Finding is one noteworthy, non-fatal condition from an injection.
type Finding struct {
	Pod    string
	Reason string
}

// Scenario defines the contract for all fault injection types.
//
// Callers persist injection intent (Status.InjectedAt) before the first
// Inject call, so a crash or failed status write after a successful Inject
// never causes a second injection. After that first call, Inject is called
// again only while the scenario reports Result.Incomplete — such scenarios
// must make every repeated step safe (idempotent cordon, retryable eviction).
// A scenario that never reports Incomplete keeps the plain at-most-once
// guarantee.
type Scenario interface {
	// Inject applies the fault. It must be safe to retry on failure.
	Inject(ctx context.Context, target Target) (Result, error)

	// Revert undoes the fault. It must be idempotent and safe to call
	// even if Inject was never called or already reverted.
	Revert(ctx context.Context, target Target) error

	// RecoveryProbe returns what to watch to know the system recovered.
	RecoveryProbe() RecoveryProbe
}
