package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	SafeguardTypeAlerts   = "alerts"
	SafeguardTypeSLO      = "slo"
	SafeguardTypeReplicas = "replicas"
)

var (
	TrialsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_trials_total",
		Help: "Total number of trials by status.",
	}, []string{"namespace", "source", "status"})

	ScenariosExecutedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_scenarios_executed_total",
		Help: "Total number of scenarios injected.",
	}, []string{"namespace", "source", "type"})

	PodsKilledTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_pods_killed_total",
		Help: "Total number of pods killed.",
	}, []string{"namespace", "source"})

	RecoveryTimeSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "temper_recovery_time_seconds",
		Help:    "Time for target to recover after fault injection.",
		Buckets: []float64{1, 5, 10, 15, 30, 60},
	}, []string{"namespace", "source", "type"})

	TrialDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "temper_trial_duration_seconds",
		Help:    "Total wall-clock duration of trials.",
		Buckets: []float64{10, 30, 60, 120, 300, 600},
	}, []string{"namespace", "source"})

	SafeguardChecksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_safeguard_checks_total",
		Help: "Total number of safeguard checks executed, by type.",
	}, []string{"namespace", "type"})

	SafeguardHaltsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_safeguard_halts_total",
		Help: "Total number of times the safeguard watcher halted a trial, by code.",
	}, []string{"namespace", "code"})

	TrialsHaltedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_trials_halted_total",
		Help: "Total number of trials transitioned to the Halted phase, by code.",
	}, []string{"namespace", "source", "code"})

	PodsEvictedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_pods_evicted_total",
		Help: "Total number of pods evicted by node-drain.",
	}, []string{"namespace", "source"})

	EvictionsBlockedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "temper_evictions_blocked_total",
		Help: "Total number of evictions blocked by a PodDisruptionBudget.",
	}, []string{"namespace", "source"})
)

func init() {
	metrics.Registry.MustRegister(
		TrialsTotal,
		ScenariosExecutedTotal,
		PodsKilledTotal,
		RecoveryTimeSeconds,
		TrialDurationSeconds,
		SafeguardChecksTotal,
		SafeguardHaltsTotal,
		TrialsHaltedTotal,
		PodsEvictedTotal,
		EvictionsBlockedTotal,
	)
}
