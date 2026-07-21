package safeguard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// fakeAlertChecker is a stand-in AlertChecker used by watcher tests. It
// returns whatever alerts/error it was configured with, regardless of the
// label matchers passed, so tests can simulate a firing Alertmanager alert
// without standing up a real alert backend.
type fakeAlertChecker struct {
	alerts []Alert
	err    error
}

func (f *fakeAlertChecker) CheckAlerts(_ context.Context, _ map[string]string) ([]Alert, error) {
	return f.alerts, f.err
}

// fakeMetricsQuerier is a stand-in MetricsQuerier used by watcher tests. It
// returns a configured scalar value (or error) for any InstantQuery, so tests
// can simulate an SLO breach without standing up a real Prometheus.
type fakeMetricsQuerier struct {
	val float64
	err error
}

func (f *fakeMetricsQuerier) InstantQuery(_ context.Context, _ string) (float64, error) {
	return f.val, f.err
}

func (f *fakeMetricsQuerier) RangeQuery(_ context.Context, _ string, _, _ time.Time, _ time.Duration) ([]DataPoint, error) {
	return nil, f.err
}

// newScheme registers temper's types alongside the core Kubernetes types the
// fake client needs to serialize objects (Pod, Deployment, etc.).
func newScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, temperv1alpha1.AddToScheme(s))
	return s
}

// newWatcherWithFake builds a Watcher backed by a fake client pre-loaded with
// the given objects, and injects a fakeAlertChecker that returns the given
// alerts (so alert-source safeguard checks deterministically fire). The
// returned watcher's failureThreshold defaults to 100 (so the alert/replica
// tests don't trip the unreachable path); callers that need to exercise the
// threshold state machine can override it after construction, or pass
// newWatcherWithFakeOpts for explicit control.
func newWatcherWithFake(t *testing.T, objs []client.Object, alerts []Alert) *Watcher {
	scheme := newScheme(t)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&temperv1alpha1.TrialSet{}, &temperv1alpha1.Trial{}).
		Build()
	w := NewWatcher(fakeClient, events.NewFakeRecorder(32))
	w.newAlertChecker = func(_ string) (AlertChecker, error) {
		return &fakeAlertChecker{alerts: alerts}, nil
	}
	w.newMetricsQuerier = func(_ string) (MetricsQuerier, error) {
		// Default: return a zero-value querier. SLO tests override this.
		return &fakeMetricsQuerier{}, nil
	}
	// Pin a high failure threshold so unreachable-path halts do not fire
	// unexpectedly in the alert tests.
	w.failureThreshold = 100
	return w
}

// makeTrialSetOwnedTrial builds a TrialSet-generated Trial: it carries the
// temper.io/trial-set label, a controller owner reference back to the
// TrialSet, and a Running phase (so checkAll will consider it active).
func makeTrialSetOwnedTrial(ts *temperv1alpha1.TrialSet, name string) *temperv1alpha1.Trial {
	return &temperv1alpha1.Trial{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ts.Namespace,
			Name:      name,
			Labels: map[string]string{
				temperv1alpha1.LabelTrialSet: ts.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "temper.io/v1alpha1",
					Kind:       "TrialSet",
					Name:       ts.Name,
					UID:        ts.UID,
					Controller: new(true),
				},
			},
		},
		Spec: temperv1alpha1.TrialSpec{
			Target: temperv1alpha1.Target{
				Kind: "Deployment",
				Name: new("pay-one"),
			},
		},
		Status: temperv1alpha1.TrialStatus{
			Phase: temperv1alpha1.TrialPhaseRunning,
		},
	}
}

// alertSafeguards builds a Safeguards spec wired to an alert source, so
// checkAlerts will call the injected fakeAlertChecker.
func alertSafeguards() *temperv1alpha1.Safeguards {
	return &temperv1alpha1.Safeguards{
		AlertSource: &temperv1alpha1.AlertSource{
			Type: temperv1alpha1.AlertSourceTypeAlertmanager,
			URL:  "http://alertmanager.test:9093",
		},
		HaltOnAlertLabels: map[string]string{"severity": "critical"},
	}
}

// TestWatcher_HaltsTrialSetTrialOnFiringAlert verifies the core Phase 3
// guarantee: a TrialSet-generated Trial with a firing Alertmanager alert gets
// the temper.io/halt-reason + temper.io/halt-code annotations written, so the
// TrialReconciler will transition it to Halted on the next reconcile and the
// TrialSet controller will then increment trialsHalted / set history.lastHaltReason.
func TestWatcher_HaltsTrialSetTrialOnFiringAlert(t *testing.T) {
	ctx := context.Background()

	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-halt",
			UID:       types.UID("ts-halt-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			Safeguards: alertSafeguards(),
		},
	}
	trial := makeTrialSetOwnedTrial(ts, "ts-halt-pay-one-1700000000")

	firing := []Alert{{Name: "HighErrorRate", Labels: map[string]string{"severity": "critical"}}}
	w := newWatcherWithFake(t, []client.Object{ts, trial}, firing)

	w.checkAll(ctx)

	var got temperv1alpha1.Trial
	require.NoError(t, w.client.Get(ctx, client.ObjectKey{Namespace: trial.Namespace, Name: trial.Name}, &got))

	assert.Equal(t, temperv1alpha1.TrialPhaseRunning, got.Status.Phase,
		"watcher must not mutate phase directly; the TrialReconciler does that on the next reconcile")
	require.Contains(t, got.Annotations, temperv1alpha1.AnnotationHaltReason,
		"halt-reason annotation must be written")
	require.Contains(t, got.Annotations, temperv1alpha1.AnnotationHaltCode,
		"halt-code annotation must be written")
	assert.Equal(t, string(temperv1alpha1.HaltCodeAlertMatch),
		got.Annotations[temperv1alpha1.AnnotationHaltCode],
		"halt-code must be alert-match for a firing alert")
	assert.Contains(t, got.Annotations[temperv1alpha1.AnnotationHaltReason], "HighErrorRate",
		"halt-reason should reference the firing alert name")
}

// TestWatcher_SkipsTrialSetTrialWithEmptySafeguards verifies that a
// TrialSet-generated Trial whose owning TrialSet has no safeguards configured
// is skipped entirely (no annotations written, no failure tracking).
func TestWatcher_SkipsTrialSetTrialWithEmptySafeguards(t *testing.T) {
	ctx := context.Background()

	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-nosafeguard",
			UID:       types.UID("ts-nosafeguard-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			// No Safeguards field set.
		},
	}
	trial := makeTrialSetOwnedTrial(ts, "ts-nosafeguard-pay-one-1700000001")

	// Even if the fake checker would fire, safeguardsForTrial returns nil
	// (empty safeguards) and checkAll skips the Trial before checkAlerts runs.
	w := newWatcherWithFake(t, []client.Object{ts, trial}, []Alert{{Name: "ShouldNotBeReached"}})

	w.checkAll(ctx)

	var got temperv1alpha1.Trial
	require.NoError(t, w.client.Get(ctx, client.ObjectKey{Namespace: trial.Namespace, Name: trial.Name}, &got))
	assert.Empty(t, got.Annotations, "no halt annotations should be written when safeguards are nil")
}

// TestWatcher_DoesNotHaltNonTrialSetTrial verifies the watcher's TrialSet path
// only touches Trials carrying the temper.io/trial-set label. A plain Trial
// (ad-hoc, no label, no TrialSet owner) must not be inspected by the TrialSet
// branch of checkAll.
func TestWatcher_DoesNotHaltNonTrialSetTrial(t *testing.T) {
	ctx := context.Background()

	// A TrialSet exists with safeguards, but the Trial has no
	// temper.io/trial-set label — it is not TrialSet-generated, so the
	// TrialSet branch of checkAll must skip it.
	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-irrelevant",
			UID:       types.UID("ts-irrelevant-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			Safeguards: alertSafeguards(),
		},
	}
	plainTrial := &temperv1alpha1.Trial{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "adhoc-trial",
			// No temper.io/trial-set label — not TrialSet-generated.
			Labels: map[string]string{},
		},
		Spec: temperv1alpha1.TrialSpec{
			Target: temperv1alpha1.Target{Kind: "Deployment", Name: new("pay-one")},
		},
		Status: temperv1alpha1.TrialStatus{Phase: temperv1alpha1.TrialPhaseRunning},
	}

	firing := []Alert{{Name: "HighErrorRate", Labels: map[string]string{"severity": "critical"}}}
	w := newWatcherWithFake(t, []client.Object{ts, plainTrial}, firing)

	w.checkAll(ctx)

	var got temperv1alpha1.Trial
	require.NoError(t, w.client.Get(ctx, client.ObjectKey{Namespace: plainTrial.Namespace, Name: plainTrial.Name}, &got))
	assert.Empty(t, got.Annotations, "non-TrialSet Trial must not be halted by the TrialSet watcher branch")
}

// TestWatcher_HaltsOnReplicaMinSafeguard verifies the replica-check path for
// TrialSet-generated Trials: when the owning TrialSet sets
// minReplicasAvailable and the target Deployment's AvailableReplicas is below
// it, the Trial is halted with the replica-min code.
func TestWatcher_HaltsOnReplicaMinSafeguard(t *testing.T) {
	ctx := context.Background()

	minReplicas := int32(3)
	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-replica",
			UID:       types.UID("ts-replica-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			Safeguards: &temperv1alpha1.Safeguards{
				MinReplicasAvailable: &minReplicas,
			},
		},
	}
	trial := makeTrialSetOwnedTrial(ts, "ts-replica-pay-one-1700000002")
	// Target Deployment with AvailableReplicas=1 < minReplicas=3.
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pay-one"},
		Status:     appsv1.DeploymentStatus{AvailableReplicas: 1},
	}

	w := newWatcherWithFake(t, []client.Object{ts, trial, dep}, nil)

	w.checkAll(ctx)

	var got temperv1alpha1.Trial
	require.NoError(t, w.client.Get(ctx, client.ObjectKey{Namespace: trial.Namespace, Name: trial.Name}, &got))
	require.Contains(t, got.Annotations, temperv1alpha1.AnnotationHaltCode)
	assert.Equal(t, string(temperv1alpha1.HaltCodeReplicaMin),
		got.Annotations[temperv1alpha1.AnnotationHaltCode])
	assert.Contains(t, got.Annotations[temperv1alpha1.AnnotationHaltReason], "Available replicas")
}

// erringAlertChecker returns the same error on every CheckAlerts call, so the
// watcher's consecutiveFailures state machine accumulates across checkAll
// ticks. Used to prove the threshold logic survived the checkTrial refactor.
type erringAlertChecker struct{ err error }

func (e *erringAlertChecker) CheckAlerts(_ context.Context, _ map[string]string) ([]Alert, error) {
	return nil, e.err
}

// TestWatcher_ConsecutiveFailuresThresholdHaltsOnThird proves the
// consecutiveFailures/threshold state machine is preserved end-to-end on the
// TrialSet path after the checkTrial refactor. With failureThreshold=3, two
// transient checker errors must NOT halt the Trial (no annotations), but a
// third error must trigger a HaltCodeUnreachable halt.
func TestWatcher_ConsecutiveFailuresThresholdHaltsOnThird(t *testing.T) {
	ctx := context.Background()

	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-threshold",
			UID:       types.UID("ts-threshold-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			Safeguards: alertSafeguards(),
		},
	}
	trial := makeTrialSetOwnedTrial(ts, "ts-threshold-pay-one-1700000003")

	scheme := newScheme(t)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ts, trial).
		WithStatusSubresource(&temperv1alpha1.TrialSet{}, &temperv1alpha1.Trial{}).
		Build()
	w := NewWatcher(fakeClient, events.NewFakeRecorder(32))
	w.newAlertChecker = func(_ string) (AlertChecker, error) {
		return &erringAlertChecker{err: assertError("alertmanager unreachable")}, nil
	}
	w.newMetricsQuerier = func(_ string) (MetricsQuerier, error) {
		return &fakeMetricsQuerier{}, nil
	}
	w.failureThreshold = 3

	trialKey := client.ObjectKey{Namespace: trial.Namespace, Name: trial.Name}

	// Tick 1: first transient error. consecutiveFailures[key]==1, below
	// threshold, no halt.
	w.checkAll(ctx)
	var got temperv1alpha1.Trial
	require.NoError(t, w.client.Get(ctx, trialKey, &got))
	assert.Empty(t, got.Annotations, "first transient error must not halt")

	// Tick 2: second transient error. consecutiveFailures[key]==2, still
	// below threshold, no halt.
	w.checkAll(ctx)
	require.NoError(t, w.client.Get(ctx, trialKey, &got))
	assert.Empty(t, got.Annotations, "second transient error must not halt")

	// Tick 3: third transient error. consecutiveFailures[key]==3 reaches the
	// threshold and the Trial is halted with HaltCodeUnreachable.
	w.checkAll(ctx)
	require.NoError(t, w.client.Get(ctx, trialKey, &got))
	require.Contains(t, got.Annotations, temperv1alpha1.AnnotationHaltCode,
		"third consecutive failure must halt")
	assert.Equal(t, string(temperv1alpha1.HaltCodeUnreachable),
		got.Annotations[temperv1alpha1.AnnotationHaltCode])
	assert.Contains(t, got.Annotations[temperv1alpha1.AnnotationHaltReason], "unreachable")
}

// TestWatcher_SweepsStaleFailureEntries verifies that a Trial which ends
// mid-failure-streak does not leave its consecutiveFailures entry behind.
// Trial names are unique per batch and never reused, so without the sweep the
// map would grow for the lifetime of the process.
func TestWatcher_SweepsStaleFailureEntries(t *testing.T) {
	ctx := context.Background()

	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-sweep",
			UID:       types.UID("ts-sweep-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			Safeguards: alertSafeguards(),
		},
	}
	trial := makeTrialSetOwnedTrial(ts, "ts-sweep-pay-one-1700000005")

	scheme := newScheme(t)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ts, trial).
		WithStatusSubresource(&temperv1alpha1.TrialSet{}, &temperv1alpha1.Trial{}).
		Build()
	w := NewWatcher(fakeClient, events.NewFakeRecorder(32))
	w.newAlertChecker = func(_ string) (AlertChecker, error) {
		return &erringAlertChecker{err: assertError("alertmanager unreachable")}, nil
	}
	w.newMetricsQuerier = func(_ string) (MetricsQuerier, error) {
		return &fakeMetricsQuerier{}, nil
	}
	w.failureThreshold = 100 // high, so the streak never halts during the test

	// Tick 1: the checker errors, so the Trial accumulates a failure entry.
	w.checkAll(ctx)
	key := trial.Namespace + "/" + trial.Name
	require.Contains(t, w.consecutiveFailures, key,
		"a transient error must be tracked while the Trial is Running")

	// The Trial finishes and is deleted (e.g. batch done, retention reaped it).
	require.NoError(t, w.client.Delete(ctx, trial))

	// Tick 2: the Trial is gone, so its streak entry must be swept.
	w.checkAll(ctx)
	assert.NotContains(t, w.consecutiveFailures, key,
		"finished Trials must not leave consecutiveFailures entries behind")
}

// assertError is a tiny error type for test fixtures.
type assertError string

func (e assertError) Error() string { return string(e) }

// sloSafeguards builds a Safeguards spec wired to a metrics source with a
// static SLO threshold, so checkSLO will call the injected fakeMetricsQuerier.
func sloSafeguards() *temperv1alpha1.Safeguards {
	threshold := "0.01"
	return &temperv1alpha1.Safeguards{
		MetricsSource: &temperv1alpha1.MetricsSource{URL: "http://prometheus.test:9090"},
		SLOProtection: &temperv1alpha1.SLOProtection{
			Mode:      temperv1alpha1.SLOModeStatic,
			Threshold: &threshold,
			Queries: []temperv1alpha1.SLOQuery{
				{Name: "error-rate", Query: "rate(errors_total[5m])"},
			},
		},
	}
}

// TestWatcher_HaltsTrialSetTrialOnSLOBreach closes the parity gap with the
// alerts branch: a TrialSet-generated Trial whose SLO query returns a value at
// or above the configured threshold is halted with the slo-breach code.
func TestWatcher_HaltsTrialSetTrialOnSLOBreach(t *testing.T) {
	ctx := context.Background()

	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "ts-slo",
			UID:       types.UID("ts-slo-uid"),
		},
		Spec: temperv1alpha1.TrialSetSpec{
			Safeguards: sloSafeguards(),
		},
	}
	trial := makeTrialSetOwnedTrial(ts, "ts-slo-pay-one-1700000004")

	// No firing alerts (alerts branch returns clean), so the SLO branch is
	// the one that trips the halt.
	w := newWatcherWithFake(t, []client.Object{ts, trial}, nil)
	// Inject a metrics querier returning 0.05, which is >= threshold 0.01.
	w.newMetricsQuerier = func(_ string) (MetricsQuerier, error) {
		return &fakeMetricsQuerier{val: 0.05}, nil
	}

	w.checkAll(ctx)

	var got temperv1alpha1.Trial
	require.NoError(t, w.client.Get(ctx, client.ObjectKey{Namespace: trial.Namespace, Name: trial.Name}, &got))
	require.Contains(t, got.Annotations, temperv1alpha1.AnnotationHaltCode,
		"SLO breach must halt the Trial")
	assert.Equal(t, string(temperv1alpha1.HaltCodeSLOBreach),
		got.Annotations[temperv1alpha1.AnnotationHaltCode])
	assert.Contains(t, got.Annotations[temperv1alpha1.AnnotationHaltReason], "error-rate",
		"halt reason should reference the breaching SLO query name")
}
