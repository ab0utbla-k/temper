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

// ConcurrencyPolicy determines how to handle overlapping trial runs. Only
// Forbid is implemented: both controllers prevent overlap structurally (a new
// run only ever fires from the Idle phase). The enum is restricted to what is
// implemented — Allow/Replace can be added back the day they do something,
// instead of being accepted and silently treated as Forbid.
// +kubebuilder:validation:Enum=Forbid
type ConcurrencyPolicy string

const (
	ConcurrencyPolicyForbid ConcurrencyPolicy = "Forbid"
)

// AlertSourceType identifies the alert backend.
// +kubebuilder:validation:Enum=alertmanager
type AlertSourceType string

const (
	AlertSourceTypeAlertmanager AlertSourceType = "alertmanager"
)

// SLOMode selects the SLO evaluation strategy.
// +kubebuilder:validation:Enum=static
type SLOMode string

const (
	SLOModeStatic SLOMode = "static"
)

// AlertSource configures the alert backend used by safeguard checks.
type AlertSource struct {
	// type selects the alert backend. Currently only "alertmanager" is supported.
	Type AlertSourceType `json:"type"`

	// url is the base URL of the alert backend (e.g., "http://alertmanager.monitoring.svc:9093").
	// +kubebuilder:validation:Required
	URL string `json:"url"`
}

// MetricsSource configures the PromQL-compatible metrics backend used by safeguard checks.
type MetricsSource struct {
	// url is the base URL of the metrics backend (e.g., "http://prometheus.monitoring.svc:9090").
	// Works with Prometheus, VictoriaMetrics, Thanos, Mimir, and Cortex.
	// +kubebuilder:validation:Required
	URL string `json:"url"`
}

// SLOQuery is a named PromQL expression evaluated during safeguard checks.
type SLOQuery struct {
	// name identifies this query in logs and halt reasons.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// query is a PromQL expression that must return a single scalar.
	// +kubebuilder:validation:Required
	Query string `json:"query"`
}

// SLOProtection configures SLO-based safeguard checks.
type SLOProtection struct {
	// mode selects the evaluation strategy. Currently only "static" is supported.
	// +kubebuilder:default=static
	Mode SLOMode `json:"mode"`

	// threshold is the value above which the trial is halted (static mode).
	// +optional
	Threshold *string `json:"threshold,omitempty"`

	// queries is the list of PromQL expressions to evaluate.
	// +optional
	Queries []SLOQuery `json:"queries,omitempty"`
}

// Safeguards defines safety checks performed before and during trials.
type Safeguards struct {
	// maxUnavailable is the maximum number of pods that can be unavailable during an trial.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`

	// minReplicasAvailable is the minimum number of pods that must remain running.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinReplicasAvailable *int32 `json:"minReplicasAvailable,omitempty"`

	// alertSource configures the alert backend for safeguard checks.
	// +optional
	AlertSource *AlertSource `json:"alertSource,omitempty"`

	// haltOnAlertLabels are label matchers that trigger a halt when a firing alert matches all of them.
	// +optional
	HaltOnAlertLabels map[string]string `json:"haltOnAlertLabels,omitempty"`

	// metricsSource configures the PromQL-compatible metrics backend for safeguard checks.
	// +optional
	MetricsSource *MetricsSource `json:"metricsSource,omitempty"`

	// sloProtection configures SLO-based safeguard checks.
	// +optional
	SLOProtection *SLOProtection `json:"sloProtection,omitempty"`
}

// CronTrialSpec defines the desired state of CronTrial
type CronTrialSpec struct {
	// trialRef references the Trial to run.
	// +kubebuilder:validation:Required
	TrialRef string `json:"trialRef"`

	// schedule is a cron expression defining when to run (e.g., "0 2 * * 1-5").
	// +kubebuilder:validation:Required
	Schedule string `json:"schedule"`

	// timezone for the cron schedule (e.g., "UTC", "America/New_York").
	// +kubebuilder:default=UTC
	Timezone string `json:"timezone,omitempty"`

	// concurrencyPolicy controls what happens when a new run is due while one is active.
	// +kubebuilder:default=Forbid
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// suspend stops future runs when true. Active runs are not affected.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// safeguards defines safety checks performed before and during trials.
	// +optional
	Safeguards *Safeguards `json:"safeguards,omitempty"`
}

// CronTrialPhase describes the lifecycle stage of a CronTrial.
// +kubebuilder:validation:Enum=Idle;Running;Paused;Halted;Completed;Failed
type CronTrialPhase string

const (
	CronTrialPhaseIdle      CronTrialPhase = "Idle"
	CronTrialPhaseRunning   CronTrialPhase = "Running"
	CronTrialPhasePaused    CronTrialPhase = "Paused"
	CronTrialPhaseHalted    CronTrialPhase = "Halted"
	CronTrialPhaseCompleted CronTrialPhase = "Completed"
	CronTrialPhaseFailed    CronTrialPhase = "Failed"
)

// CronTrialRun tracks the state of the current trial run.
type CronTrialRun struct {
	// startedAt is when the current run began.
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// scenariosCompleted is how many scenarios have finished in this run.
	ScenariosCompleted int32 `json:"scenariosCompleted,omitempty"`

	// scenariosTotal is the total number of scenarios in this run.
	ScenariosTotal int32 `json:"scenariosTotal,omitempty"`
}

// CronTrialHistory tracks aggregate results across all runs.
type CronTrialHistory struct {
	// totalRuns is the number of trial runs triggered.
	TotalRuns int32 `json:"totalRuns,omitempty"`

	// successfulRuns is the number of runs that completed without issues.
	SuccessfulRuns int32 `json:"successfulRuns,omitempty"`

	// haltedRuns is the number of runs stopped by safeguards.
	HaltedRuns int32 `json:"haltedRuns,omitempty"`

	// lastHaltReason describes why the most recent halt occurred.
	// +optional
	LastHaltReason *string `json:"lastHaltReason,omitempty"`
}

// CronTrialStatus defines the observed state of CronTrial.
type CronTrialStatus struct {
	// conditions represent the current state of the CronTrial resource.
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

	// phase is the current lifecycle stage of the CronTrial.
	// +optional
	Phase CronTrialPhase `json:"phase,omitempty"`

	// activeScenario is the name of the currently running scenario type.
	// +optional
	ActiveScenario *string `json:"activeScenario,omitempty"`

	// currentRun tracks the in-progress trial run.
	// +optional
	CurrentRun *CronTrialRun `json:"currentRun,omitempty"`

	// history tracks aggregate results across all runs.
	// +optional
	History CronTrialHistory `json:"history,omitempty"`

	// activeTrialName is the name of the Trial CR created for the current run.
	// +optional
	ActiveTrialName *string `json:"activeTrialName,omitempty"`

	// lastScheduleTime is when the last trial was triggered.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Trial",type=string,JSONPath=`.spec.trialRef`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CronTrial is the Schema for the crontrials API
type CronTrial struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of CronTrial
	// +required
	Spec CronTrialSpec `json:"spec"`

	// status defines the observed state of CronTrial
	// +optional
	Status CronTrialStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// CronTrialList contains a list of CronTrial
type CronTrialList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CronTrial `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &CronTrial{}, &CronTrialList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})
}
