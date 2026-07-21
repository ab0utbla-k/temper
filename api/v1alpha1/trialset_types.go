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

// TrialTemplateSpec is a Trial spec template without target — the TrialSet
// controller sets target.name per discovered Deployment.
type TrialTemplateSpec struct {
	// scenarios is the ordered list of fault injection steps applied to each
	// discovered Deployment.
	// +kubebuilder:validation:MinItems=1
	Scenarios []Scenario `json:"scenarios"`

	// execution controls how scenarios run within each generated Trial.
	// Defaults to sequential when unset.
	// +optional
	Execution *Execution `json:"execution,omitempty"`
}

// TrialSetSpec defines the desired state of TrialSet
type TrialSetSpec struct {
	// targetSelector selects Deployments to generate Trials for. Matches via
	// labels, in the TrialSet's own namespace only — a Trial's target always
	// lives in the Trial's namespace, so RBAC on Trials bounds the blast
	// radius per namespace.
	// +kubebuilder:validation:Required
	TargetSelector metav1.LabelSelector `json:"targetSelector"`

	// trialTemplate is applied to each discovered Deployment. The controller
	// stamps target.name onto each generated Trial from the Deployment it
	// matched.
	// +kubebuilder:validation:Required
	TrialTemplate TrialTemplateSpec `json:"trialTemplate"`

	// schedule, if set, repeats the batch on a cron expression (e.g.
	// "0 2 * * 1-5"). If unset, the batch runs once on creation.
	// +optional
	Schedule *string `json:"schedule,omitempty"`

	// timezone for the schedule (e.g. "UTC", "America/New_York"). Defaults
	// to UTC. Ignored when schedule is unset.
	// +kubebuilder:default=UTC
	Timezone string `json:"timezone,omitempty"`

	// concurrencyPolicy controls what happens when a new batch is due while
	// the previous one is still active. Default Forbid (skip the new fire).
	// +kubebuilder:default=Forbid
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// maxConcurrent limits how many generated Trials run simultaneously within
	// a batch. Default 1 (most conservative — one pod-kill at a time).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	MaxConcurrent int32 `json:"maxConcurrent,omitempty"`

	// minReadyReplicas skips Deployments whose readyReplicas are below this
	// threshold at discovery time. Nil means generate for all matches
	// (including zero-ready Deployments, which will then fail their Trial
	// after the existing 60s target-ready grace). Recommended value in
	// practice: 1.
	// +optional
	MinReadyReplicas *int32 `json:"minReadyReplicas,omitempty"`

	// suspend stops future batch runs when true. An in-progress batch is not
	// interrupted — it runs to completion.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// safeguards defines safety checks (same semantics as CronTrial.safeguards),
	// evaluated per generated Trial against that Trial's target Deployment.
	// +optional
	Safeguards *Safeguards `json:"safeguards,omitempty"`
}

// TrialSetPhase describes the lifecycle stage of a TrialSet.
// +kubebuilder:validation:Enum=Idle;Running;Paused;Completed;Halted;Failed
type TrialSetPhase string

const (
	TrialSetPhaseIdle      TrialSetPhase = "Idle"
	TrialSetPhaseRunning   TrialSetPhase = "Running"   // a batch is in progress
	TrialSetPhasePaused    TrialSetPhase = "Paused"    // suspended by the user
	TrialSetPhaseCompleted TrialSetPhase = "Completed" // last batch finished
	TrialSetPhaseHalted    TrialSetPhase = "Halted"    // safeguard halted a run
	TrialSetPhaseFailed    TrialSetPhase = "Failed"
)

// TrialSetHistory tracks aggregate results across all batches.
type TrialSetHistory struct {
	// totalBatches is the number of batches fired (including the current one).
	TotalBatches int32 `json:"totalBatches,omitempty"`

	// successfulBatches is the number of batches whose Trials all Completed.
	SuccessfulBatches int32 `json:"successfulBatches,omitempty"`

	// haltedBatches is the number of batches in which at least one Trial was
	// halted by a safeguard.
	HaltedBatches int32 `json:"haltedBatches,omitempty"`

	// lastHaltReason describes why the most recent halt occurred.
	// +optional
	LastHaltReason *string `json:"lastHaltReason,omitempty"`
}

// TrialSetStatus defines the observed state of TrialSet.
type TrialSetStatus struct {
	// conditions represent the current state of the TrialSet resource.
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

	// phase is the current lifecycle stage of the TrialSet.
	// +optional
	Phase TrialSetPhase `json:"phase,omitempty"`

	// discoveredDeployments is the number of Deployments matched by
	// targetSelector in the last discovery.
	DiscoveredDeployments int32 `json:"discoveredDeployments,omitempty"`

	// Per-batch counts (reset at the start of each batch).

	// trialsCreated is the number of Trials created in the current batch.
	TrialsCreated int32 `json:"trialsCreated,omitempty"`

	// trialsCompleted is the number of Trials that reached Completed in the
	// current batch.
	TrialsCompleted int32 `json:"trialsCompleted,omitempty"`

	// trialsFailed is the number of Trials that reached Failed in the
	// current batch.
	TrialsFailed int32 `json:"trialsFailed,omitempty"`

	// trialsHalted is the number of Trials halted by safeguards in the
	// current batch.
	TrialsHalted int32 `json:"trialsHalted,omitempty"`

	// activeTrialNames are the in-progress Trials in the current batch.
	// +optional
	ActiveTrialNames []string `json:"activeTrialNames,omitempty"`

	// lastScheduleTime is when the last batch was fired.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// lastDiscoveryTime is when Deployments were last listed.
	// +optional
	LastDiscoveryTime *metav1.Time `json:"lastDiscoveryTime,omitempty"`

	// history tracks aggregate results across all batches.
	// +optional
	History TrialSetHistory `json:"history,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Discovered",type=integer,JSONPath=`.status.discoveredDeployments`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TrialSet is the Schema for the trialsets API
type TrialSet struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TrialSet
	// +required
	Spec TrialSetSpec `json:"spec"`

	// status defines the observed state of TrialSet
	// +optional
	Status TrialSetStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TrialSetList contains a list of TrialSet
type TrialSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TrialSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &TrialSet{}, &TrialSetList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})
}
