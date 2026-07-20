/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
// Important: Run "make" to regenerate code after modifying this file
// The following markers will use OpenAPI v3 schema to validate the value
// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html
// For Kubernetes API conventions, see:
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

// Target specifies which workload to inject faults into.
// Exactly one of name or selector must be set.
type Target struct {
	// kind is the target resource type (e.g., Deployment).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=Deployment
	Kind string `json:"kind"`

	// name targets a specific resource by name. Mutually exclusive with selector.
	// +optional
	Name *string `json:"name,omitempty"`

	// selector targets resources by label. Mutually exclusive with name.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

// +kubebuilder:validation:Enum=pod-kill;node-drain

// ScenarioType identifies the kind of fault to inject.
type ScenarioType string

const (
	ScenarioTypePodKill   ScenarioType = "pod-kill"
	ScenarioTypeNodeDrain ScenarioType = "node-drain"
)

// PodKillConfig configures the pod-kill scenario.
type PodKillConfig struct {
	// count is the number of pods to kill at a time.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Count int32 `json:"count"`

	// interval is the time between repeated kills. If unset, pods are killed once.
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`
}

// NodeDrainConfig configures the node-drain scenario.
type NodeDrainConfig struct {
	// nodeName pins the drain to this node. Empty means the scenario picks
	// the node running the most target pods.
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	// evictionTimeout is how long blocked evictions are retried before the
	// trial concludes Blocked. Unset means 30s.
	// +optional
	EvictionTimeout *metav1.Duration `json:"evictionTimeout,omitempty"`
}

// Scenario defines a single fault injection step.
type Scenario struct {
	// type selects which fault to inject.
	// +kubebuilder:validation:Required
	Type ScenarioType `json:"type"`

	// duration is how long this scenario runs before reverting.
	// +kubebuilder:validation:Required
	Duration metav1.Duration `json:"duration"`

	// podKill configures the pod-kill scenario. Required when type is "pod-kill".
	// +optional
	PodKill *PodKillConfig `json:"podKill,omitempty"`

	// nodeDrain configures the node-drain scenario. Only read when type is
	// "node-drain"; omitted means default behavior (drain the busiest node).
	// +optional
	NodeDrain *NodeDrainConfig `json:"nodeDrain,omitempty"`
}

// ExecutionMode controls how scenarios are run.
// +kubebuilder:validation:Enum=sequential;parallel
type ExecutionMode string

const (
	ExecutionModeSequential ExecutionMode = "sequential"
	ExecutionModeParallel   ExecutionMode = "parallel"
)

// Execution controls how scenarios in the list are run.
type Execution struct {
	// mode determines whether scenarios run one after another or simultaneously.
	// +kubebuilder:default=sequential
	Mode ExecutionMode `json:"mode,omitempty"`

	// pauseBetween is the wait time between scenarios in sequential mode.
	// +optional
	PauseBetween *metav1.Duration `json:"pauseBetween,omitempty"`
}

// RecoverySpec overrides how the controller decides the target recovered.
// When set, it replaces the scenario's default recovery probe. Exactly one
// probe kind may be set.
type RecoverySpec struct {
	// http probes recovery with an HTTP GET; a 2xx response means recovered.
	// It checks that the service actually answers, not merely that the
	// workload's readiness probe claims it is ready.
	// +optional
	HTTP *HTTPRecoveryProbe `json:"http,omitempty"`
}

// HTTPRecoveryProbe probes recovery by issuing an HTTP GET.
type HTTPRecoveryProbe struct {
	// url is the endpoint to probe. A 2xx response means recovered.
	// +kubebuilder:validation:Required
	URL string `json:"url"`
}

// TrialSpec defines the desired state of Trial
type TrialSpec struct {
	// target specifies which workload to inject faults into.
	// +kubebuilder:validation:Required
	Target Target `json:"target"`

	// scenarios is the ordered list of fault injection steps.
	// +kubebuilder:validation:MinItems=1
	Scenarios []Scenario `json:"scenarios"`

	// execution controls how scenarios are run. Defaults to sequential.
	// +optional
	Execution *Execution `json:"execution,omitempty"`

	// recovery overrides how recovery is detected. When unset, each scenario's
	// default probe is used.
	// +optional
	Recovery *RecoverySpec `json:"recovery,omitempty"`
}

// HaltCode is a bounded bucket for the cause of a safeguard halt. Written to
// the temper.io/halt-code annotation and used as a Prometheus metric label.
type HaltCode string

const (
	HaltCodeAlertMatch  HaltCode = "alert-match"
	HaltCodeSLOBreach   HaltCode = "slo-breach"
	HaltCodeReplicaMin  HaltCode = "replica-min"
	HaltCodeReplicaMax  HaltCode = "replica-max"
	HaltCodeUnreachable HaltCode = "unreachable"
	HaltCodeConfigError HaltCode = "config-error"
)

const (
	// AnnotationHaltReason is the annotation key set by the safeguard watcher to signal
	// that the trial should be halted. The trial controller reads and removes it.
	AnnotationHaltReason = "temper.io/halt-reason"

	// AnnotationCordonedBy is the annotation key the node-drain scenario sets on a
	// Node it cordons, recording the owning trial (namespace/name). Revert reads it
	// to un-cordon only the nodes this trial cordoned.
	AnnotationCordonedBy = "temper.io/cordoned-by"

	// LabelCronTrial is the label set by the CronTrial controller on every
	// Trial it creates. Metrics use its value as a bounded source
	// identifier (the CronTrial name, or "adhoc" when absent).
	LabelCronTrial = "temper.io/cron-trial"

	// AnnotationHaltCode is the bounded halt bucket set alongside AnnotationHaltReason.
	// Used for metric labels; the value is one of the HaltCode constants.
	AnnotationHaltCode = "temper.io/halt-code"
)

// TrialPhase describes the lifecycle stage of a Trial.
// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed;Halted
type TrialPhase string

const (
	TrialPhasePending   TrialPhase = "Pending"
	TrialPhaseRunning   TrialPhase = "Running"
	TrialPhaseCompleted TrialPhase = "Completed"
	TrialPhaseFailed    TrialPhase = "Failed"
	TrialPhaseHalted    TrialPhase = "Halted"
)

// TrialOutcome is what the experiment concluded — independent of TrialPhase,
// which says whether the controller ran it. A tool error has no outcome.
// +kubebuilder:validation:Enum=Passed;Blocked;Failed;Halted
type TrialOutcome string

const (
	// OutcomePassed means the disruption ran and recovery met the deadline.
	OutcomePassed TrialOutcome = "Passed"
	// OutcomeBlocked means a PodDisruptionBudget refused the disruption.
	OutcomeBlocked TrialOutcome = "Blocked"
	// OutcomeFailed means the workload did not recover in time.
	OutcomeFailed TrialOutcome = "Failed"
	// OutcomeHalted means a safeguard stopped the run.
	OutcomeHalted TrialOutcome = "Halted"
)

// TrialMetrics tracks aggregate results of the trial.
type TrialMetrics struct {
	// totalPodsKilled is the number of pods deleted across all runs.
	TotalPodsKilled int32 `json:"totalPodsKilled,omitempty"`

	// meanRecoveryTime is the average time for the target to recover after injection.
	// +optional
	MeanRecoveryTime *metav1.Duration `json:"meanRecoveryTime,omitempty"`
}

// ScenarioResult records what one scenario run concluded.
type ScenarioResult struct {
	// type is the scenario that ran.
	Type ScenarioType `json:"type"`

	// injectedAt is when the fault was injected.
	InjectedAt metav1.Time `json:"injectedAt"`

	// recoveredAt is when the recovery probe first succeeded. Unset means the
	// target never recovered within the scenario's duration.
	// +optional
	RecoveredAt *metav1.Time `json:"recoveredAt,omitempty"`

	// findings are noteworthy, non-fatal conditions observed during the run
	// (e.g. a PDB-blocked eviction).
	// +optional
	Findings []Finding `json:"findings,omitempty"`
}

// Finding is one noteworthy, non-fatal condition observed during a scenario run.
type Finding struct {
	// reason is a machine-readable token identifying the finding kind
	// (e.g. EvictionBlocked).
	Reason string `json:"reason"`

	// message is a human-readable explanation of the finding.
	// +optional
	Message string `json:"message,omitempty"`
}

// TrialStatus defines the observed state of Trial.
type TrialStatus struct {
	// conditions represent the current state of the Trial resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// phase is the current lifecycle stage of the trial.
	// +optional
	Phase TrialPhase `json:"phase,omitempty"`

	// outcome is what the experiment concluded. Empty until a verdict exists,
	// and never set when the controller itself failed (phase Failed).
	// +optional
	Outcome TrialOutcome `json:"outcome,omitempty"`

	// metrics tracks aggregate trial results.
	// +optional
	Metrics *TrialMetrics `json:"metrics,omitempty"`

	// currentScenarioIndex is the zero-based index of the currently executing scenario.
	CurrentScenarioIndex int32 `json:"currentScenarioIndex,omitempty"`

	// injectedAt is when the current scenario's fault was injected.
	// +optional
	InjectedAt *metav1.Time `json:"injectedAt,omitempty"`

	// injectionIncomplete reports that the current scenario's Inject has
	// work left (e.g. evictions blocked by a PDB) and will be called again.
	// +optional
	InjectionIncomplete bool `json:"injectionIncomplete,omitempty"`

	// recoveredAt is when the target recovered from the current scenario's fault.
	// +optional
	RecoveredAt *metav1.Time `json:"recoveredAt,omitempty"`

	// scenarioResults records the per-scenario timeline, in execution order.
	// +optional
	ScenarioResults []ScenarioResult `json:"scenarioResults,omitempty"`

	// haltReason explains why a safeguard stopped the trial. Only set when phase is Halted.
	// +optional
	HaltReason *string `json:"haltReason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Outcome",type=string,JSONPath=`.status.outcome`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Trial is the Schema for the trials API
type Trial struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Trial
	// +required
	Spec TrialSpec `json:"spec"`

	// status defines the observed state of Trial
	// +optional
	Status TrialStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TrialList contains a list of Trial
type TrialList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Trial `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &Trial{}, &TrialList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})
}
