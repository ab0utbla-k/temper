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

// +kubebuilder:validation:Enum=pod-kill

// ScenarioType identifies the kind of fault to inject.
type ScenarioType string

const (
	ScenarioTypePodKill ScenarioType = "pod-kill"
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

// ChaosExperimentSpec defines the desired state of ChaosExperiment
type ChaosExperimentSpec struct {
	// target specifies which workload to inject faults into.
	// +kubebuilder:validation:Required
	Target Target `json:"target"`

	// scenarios is the ordered list of fault injection steps.
	// +kubebuilder:validation:MinItems=1
	Scenarios []Scenario `json:"scenarios"`

	// execution controls how scenarios are run. Defaults to sequential.
	// +optional
	Execution *Execution `json:"execution,omitempty"`
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
	// that the experiment should be halted. The experiment controller reads and removes it.
	AnnotationHaltReason = "temper.io/halt-reason"

	// LabelSchedule is the label set by the schedule controller on every
	// ChaosExperiment it creates. Metrics use its value as a bounded source
	// identifier (the schedule name, or "adhoc" when absent).
	LabelSchedule = "temper.io/schedule"

	// AnnotationHaltCode is the bounded halt bucket set alongside AnnotationHaltReason.
	// Used for metric labels; the value is one of the HaltCode constants.
	AnnotationHaltCode = "temper.io/halt-code"
)

// ExperimentPhase describes the lifecycle stage of a ChaosExperiment.
// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed;Halted
type ExperimentPhase string

const (
	ExperimentPhasePending   ExperimentPhase = "Pending"
	ExperimentPhaseRunning   ExperimentPhase = "Running"
	ExperimentPhaseCompleted ExperimentPhase = "Completed"
	ExperimentPhaseFailed    ExperimentPhase = "Failed"
	ExperimentPhaseHalted    ExperimentPhase = "Halted"
)

// ExperimentMetrics tracks aggregate results of the experiment.
type ExperimentMetrics struct {
	// totalPodsKilled is the number of pods deleted across all runs.
	TotalPodsKilled int32 `json:"totalPodsKilled,omitempty"`

	// meanRecoveryTime is the average time for the target to recover after injection.
	// +optional
	MeanRecoveryTime *metav1.Duration `json:"meanRecoveryTime,omitempty"`
}

// ChaosExperimentStatus defines the observed state of ChaosExperiment.
type ChaosExperimentStatus struct {
	// conditions represent the current state of the ChaosExperiment resource.
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

	// phase is the current lifecycle stage of the experiment.
	// +optional
	Phase ExperimentPhase `json:"phase,omitempty"`

	// metrics tracks aggregate experiment results.
	// +optional
	Metrics *ExperimentMetrics `json:"metrics,omitempty"`

	// currentScenarioIndex is the zero-based index of the currently executing scenario.
	CurrentScenarioIndex int32 `json:"currentScenarioIndex,omitempty"`

	// injectedAt is when the current scenario's fault was injected.
	// +optional
	InjectedAt *metav1.Time `json:"injectedAt,omitempty"`

	// recoveredAt is when the target recovered from the current scenario's fault.
	// +optional
	RecoveredAt *metav1.Time `json:"recoveredAt,omitempty"`

	// haltReason explains why a safeguard stopped the experiment. Only set when phase is Halted.
	// +optional
	HaltReason *string `json:"haltReason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ChaosExperiment is the Schema for the chaosexperiments API
type ChaosExperiment struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ChaosExperiment
	// +required
	Spec ChaosExperimentSpec `json:"spec"`

	// status defines the observed state of ChaosExperiment
	// +optional
	Status ChaosExperimentStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ChaosExperimentList contains a list of ChaosExperiment
type ChaosExperimentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ChaosExperiment `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &ChaosExperiment{}, &ChaosExperimentList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})
}
