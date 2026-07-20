package risk

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

const (
	ns     = "demo"
	target = "payment"
)

var appLabels = map[string]string{"app": "payment"}

// hasRule reports whether risks contains the given rule token.
func hasRule(risks []temperv1alpha1.Risk, rule temperv1alpha1.RiskRule) bool {
	for _, r := range risks {
		if r.Rule == rule {
			return true
		}
	}
	return false
}

// resilientTemplate is a pod template with none of the risk conditions:
// two containers each with both probes, anti-affinity defined.
func resilientTemplate() corev1.PodTemplateSpec {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"},
	}}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				PodAntiAffinity: &corev1.PodAntiAffinity{},
			},
			Containers: []corev1.Container{
				{Name: "app", ReadinessProbe: probe, LivenessProbe: probe},
			},
		},
	}
}

func resilientWorkload() workload {
	return workload{
		replicas: 3,
		selector: &metav1.LabelSelector{MatchLabels: appLabels},
		template: resilientTemplate(),
	}
}

// --- Pure per-case checks -------------------------------------------------

func TestRiskCheckSingleReplica(t *testing.T) {
	tests := []struct {
		name     string
		replicas int32
		want     bool
	}{
		{"one replica flags", 1, true},
		{"zero replicas flags", 0, true},
		{"two replicas clean", 2, false},
		{"many replicas clean", 5, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wl := resilientWorkload()
			wl.replicas = tc.replicas
			got := checkSingleReplica(wl) != nil
			if got != tc.want {
				t.Fatalf("checkSingleReplica(replicas=%d) present=%v, want %v", tc.replicas, got, tc.want)
			}
		})
	}
}

func TestRiskCheckNoPodAntiAffinity(t *testing.T) {
	withSpread := resilientTemplate()
	withSpread.Spec.Affinity = nil
	withSpread.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{TopologyKey: "kubernetes.io/hostname"}}

	bare := resilientTemplate()
	bare.Spec.Affinity = nil

	tests := []struct {
		name     string
		template corev1.PodTemplateSpec
		want     bool
	}{
		{"no affinity or spread flags", bare, true},
		{"anti-affinity clean", resilientTemplate(), false},
		{"topology spread clean", withSpread, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wl := resilientWorkload()
			wl.template = tc.template
			got := checkNoPodAntiAffinity(wl) != nil
			if got != tc.want {
				t.Fatalf("checkNoPodAntiAffinity present=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestRiskCheckMissingHealthProbes(t *testing.T) {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}}

	both := corev1.Container{Name: "app", ReadinessProbe: probe, LivenessProbe: probe}
	onlyReadiness := corev1.Container{Name: "app", ReadinessProbe: probe}
	onlyLiveness := corev1.Container{Name: "app", LivenessProbe: probe}
	neither := corev1.Container{Name: "app"}

	tests := []struct {
		name       string
		containers []corev1.Container
		want       bool
	}{
		{"both probes clean", []corev1.Container{both}, false},
		{"missing liveness flags", []corev1.Container{onlyReadiness}, true},
		{"missing readiness flags", []corev1.Container{onlyLiveness}, true},
		{"no probes flags", []corev1.Container{neither}, true},
		{"one good one bad flags", []corev1.Container{both, neither}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wl := resilientWorkload()
			wl.template.Spec.Containers = tc.containers
			got := checkMissingHealthProbes(wl) != nil
			if got != tc.want {
				t.Fatalf("checkMissingHealthProbes present=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestRiskCheckNoPodDisruptionBudget(t *testing.T) {
	matching := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb", Namespace: ns},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: appLabels}},
	}
	nonMatching := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: ns},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}},
	}

	tests := []struct {
		name string
		pdbs []policyv1.PodDisruptionBudget
		want bool
	}{
		{"no pdbs flags", nil, true},
		{"non-matching pdb flags", []policyv1.PodDisruptionBudget{nonMatching}, true},
		{"matching pdb clean", []policyv1.PodDisruptionBudget{matching}, false},
		{"matching among others clean", []policyv1.PodDisruptionBudget{nonMatching, matching}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkNoPodDisruptionBudget(resilientWorkload(), tc.pdbs) != nil
			if got != tc.want {
				t.Fatalf("checkNoPodDisruptionBudget present=%v, want %v", got, tc.want)
			}
		})
	}
}

func runningPod(name, node string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: appLabels},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func TestRiskCheckConcentratedPlacement(t *testing.T) {
	pending := runningPod("p-pending", "node-a")
	pending.Status.Phase = corev1.PodPending

	tests := []struct {
		name string
		pods []corev1.Pod
		want bool
	}{
		{"all on one node flags", []corev1.Pod{runningPod("p1", "node-a"), runningPod("p2", "node-a")}, true},
		{"spread across nodes clean", []corev1.Pod{runningPod("p1", "node-a"), runningPod("p2", "node-b")}, false},
		{"single running pod clean", []corev1.Pod{runningPod("p1", "node-a")}, false},
		{"no pods clean", nil, false},
		{"non-running ignored", []corev1.Pod{runningPod("p1", "node-a"), pending}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := checkConcentratedPlacement(tc.pods) != nil
			if got != tc.want {
				t.Fatalf("checkConcentratedPlacement present=%v, want %v", got, tc.want)
			}
		})
	}
}

// --- Detect over a fake client ------------------------------------------

func newClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func riskyDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: target, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: appLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
			},
		},
	}
}

func TestDetectRiskyDeploymentAllFiveTokens(t *testing.T) {
	// One replica, no anti-affinity, no probes, no PDB, and its single running
	// pod shares a node with a second running pod.
	dep := riskyDeployment()
	pods := []client.Object{
		toObj(runningPod("payment-1", "node-a")),
		toObj(runningPod("payment-2", "node-a")),
	}
	c := newClient(append([]client.Object{dep}, pods...)...)

	risks, err := Detect(context.Background(), c, "Deployment", ns, target)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	for _, want := range []temperv1alpha1.RiskRule{
		temperv1alpha1.RiskSingleReplica,
		temperv1alpha1.RiskNoPodAntiAffinity,
		temperv1alpha1.RiskMissingHealthProbes,
		temperv1alpha1.RiskNoPodDisruptionBudget,
		temperv1alpha1.RiskConcentratedPlacement,
	} {
		if !hasRule(risks, want) {
			t.Errorf("expected risk %q, not found in %+v", want, risks)
		}
	}
	for _, r := range risks {
		if r.Message == "" {
			t.Errorf("risk %q has empty message", r.Rule)
		}
	}
}

func TestDetectCleanDeploymentNoRisks(t *testing.T) {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: target, Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(3)),
			Selector: &metav1.LabelSelector{MatchLabels: appLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
				Spec: corev1.PodSpec{
					Affinity:   &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}},
					Containers: []corev1.Container{{Name: "app", ReadinessProbe: probe, LivenessProbe: probe}},
				},
			},
		},
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb", Namespace: ns},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: appLabels}},
	}
	c := newClient(dep, pdb,
		toObj(runningPod("payment-1", "node-a")),
		toObj(runningPod("payment-2", "node-b")),
		toObj(runningPod("payment-3", "node-c")),
	)

	risks, err := Detect(context.Background(), c, "Deployment", ns, target)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}
	if len(risks) != 0 {
		t.Fatalf("expected no risks for clean deployment, got %+v", risks)
	}
}

func TestDetectRiskyStatefulSet(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: target, Namespace: ns},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: appLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
			},
		},
	}
	c := newClient(sts)

	risks, err := Detect(context.Background(), c, "StatefulSet", ns, target)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}
	// Single replica, no anti-affinity, no probes, no PDB should all fire.
	for _, want := range []temperv1alpha1.RiskRule{
		temperv1alpha1.RiskSingleReplica,
		temperv1alpha1.RiskNoPodAntiAffinity,
		temperv1alpha1.RiskMissingHealthProbes,
		temperv1alpha1.RiskNoPodDisruptionBudget,
	} {
		if !hasRule(risks, want) {
			t.Errorf("expected risk %q for statefulset, not found in %+v", want, risks)
		}
	}
}

func TestDetectUnsupportedKind(t *testing.T) {
	c := newClient()
	if _, err := Detect(context.Background(), c, "DaemonSet", ns, target); err == nil {
		t.Fatal("expected error for unsupported kind, got nil")
	}
}

// toObj is a helper to pass a value pod as a client.Object.
func toObj(p corev1.Pod) client.Object { return &p }

// --- Mitigation: detect, fix, re-detect ----------------------------------

// TestDetectMitigationPerRule proves removal symmetry for every rule: the
// fully risky deployment carries the rule, applying that rule's fix makes a
// subsequent Detect pass drop it while unrelated risks remain.
func TestDetectMitigationPerRule(t *testing.T) {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}}

	tests := []struct {
		name     string
		rule     temperv1alpha1.RiskRule
		mitigate func(t *testing.T, c client.Client)
	}{
		{
			name: "scaling up clears SingleReplica",
			rule: temperv1alpha1.RiskSingleReplica,
			mitigate: func(t *testing.T, c client.Client) {
				t.Helper()
				var dep appsv1.Deployment
				mustGet(t, c, &dep)
				dep.Spec.Replicas = new(int32(3))
				mustUpdate(t, c, &dep)
			},
		},
		{
			name: "adding anti-affinity clears NoPodAntiAffinity",
			rule: temperv1alpha1.RiskNoPodAntiAffinity,
			mitigate: func(t *testing.T, c client.Client) {
				t.Helper()
				var dep appsv1.Deployment
				mustGet(t, c, &dep)
				dep.Spec.Template.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
				mustUpdate(t, c, &dep)
			},
		},
		{
			name: "adding probes clears MissingHealthProbes",
			rule: temperv1alpha1.RiskMissingHealthProbes,
			mitigate: func(t *testing.T, c client.Client) {
				t.Helper()
				var dep appsv1.Deployment
				mustGet(t, c, &dep)
				for i := range dep.Spec.Template.Spec.Containers {
					dep.Spec.Template.Spec.Containers[i].ReadinessProbe = probe
					dep.Spec.Template.Spec.Containers[i].LivenessProbe = probe
				}
				mustUpdate(t, c, &dep)
			},
		},
		{
			name: "creating a PDB clears NoPodDisruptionBudget",
			rule: temperv1alpha1.RiskNoPodDisruptionBudget,
			mitigate: func(t *testing.T, c client.Client) {
				t.Helper()
				pdb := &policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{Name: "pdb", Namespace: ns},
					Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: appLabels}},
				}
				if err := c.Create(context.Background(), pdb); err != nil {
					t.Fatalf("create pdb: %v", err)
				}
			},
		},
		{
			name: "spreading pods clears ConcentratedPlacement",
			rule: temperv1alpha1.RiskConcentratedPlacement,
			mitigate: func(t *testing.T, c client.Client) {
				t.Helper()
				var pod corev1.Pod
				if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "payment-2"}, &pod); err != nil {
					t.Fatalf("get pod: %v", err)
				}
				pod.Spec.NodeName = "node-b"
				mustUpdate(t, c, &pod)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(riskyDeployment(),
				toObj(runningPod("payment-1", "node-a")),
				toObj(runningPod("payment-2", "node-a")),
			)

			before, err := Detect(context.Background(), c, "Deployment", ns, target)
			if err != nil {
				t.Fatalf("Detect (before): %v", err)
			}
			if !hasRule(before, tc.rule) {
				t.Fatalf("precondition failed: rule %q not detected before mitigation in %+v", tc.rule, before)
			}

			tc.mitigate(t, c)

			after, err := Detect(context.Background(), c, "Deployment", ns, target)
			if err != nil {
				t.Fatalf("Detect (after): %v", err)
			}
			if hasRule(after, tc.rule) {
				t.Fatalf("rule %q still present after mitigation: %+v", tc.rule, after)
			}
			// Exactly one condition was fixed: the other risks must remain.
			if len(after) != len(before)-1 {
				t.Fatalf("expected %d risks after mitigating %q, got %d: %+v",
					len(before)-1, tc.rule, len(after), after)
			}
		})
	}
}

// TestDetectLateAppearingRisk proves a condition introduced after a clean pass
// is reported by a later pass (PDB deleted -> NoPodDisruptionBudget appears).
func TestDetectLateAppearingRisk(t *testing.T) {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb", Namespace: ns},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: appLabels}},
	}
	c := newClient(riskyDeployment(), pdb)

	before, err := Detect(context.Background(), c, "Deployment", ns, target)
	if err != nil {
		t.Fatalf("Detect (before): %v", err)
	}
	if hasRule(before, temperv1alpha1.RiskNoPodDisruptionBudget) {
		t.Fatalf("precondition failed: NoPodDisruptionBudget present with PDB in place")
	}

	if err := c.Delete(context.Background(), pdb); err != nil {
		t.Fatalf("delete pdb: %v", err)
	}

	after, err := Detect(context.Background(), c, "Deployment", ns, target)
	if err != nil {
		t.Fatalf("Detect (after): %v", err)
	}
	if !hasRule(after, temperv1alpha1.RiskNoPodDisruptionBudget) {
		t.Fatalf("NoPodDisruptionBudget not detected after PDB deletion: %+v", after)
	}
}

func mustGet(t *testing.T, c client.Client, dep *appsv1.Deployment) {
	t.Helper()
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: target}, dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
}

func mustUpdate(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	if err := c.Update(context.Background(), obj); err != nil {
		t.Fatalf("update %T: %v", obj, err)
	}
}

// --- Rule interface and registry ------------------------------------------

// TestRuleRegistryCompleteAndUnique proves the registry covers every supported
// RiskRule token exactly once, in deterministic order.
func TestRuleRegistryCompleteAndUnique(t *testing.T) {
	want := []temperv1alpha1.RiskRule{
		temperv1alpha1.RiskSingleReplica,
		temperv1alpha1.RiskNoPodAntiAffinity,
		temperv1alpha1.RiskMissingHealthProbes,
		temperv1alpha1.RiskNoPodDisruptionBudget,
		temperv1alpha1.RiskConcentratedPlacement,
	}
	if len(rules) != len(want) {
		t.Fatalf("registry has %d rules, want %d", len(rules), len(want))
	}
	seen := make(map[temperv1alpha1.RiskRule]bool, len(rules))
	for i, rule := range rules {
		id := rule.ID()
		if seen[id] {
			t.Errorf("rule %q registered more than once", id)
		}
		seen[id] = true
		if id != want[i] {
			t.Errorf("registry[%d] = %q, want %q (order must be deterministic)", i, id, want[i])
		}
	}
}

// TestRuleDetectMitigatedAgreement proves the interface invariant for every
// rule on both a risky and a clean snapshot: Mitigated(s) == (Detect(s) == nil),
// and a detected risk carries the rule's own ID with a message.
func TestRuleDetectMitigatedAgreement(t *testing.T) {
	risky := Snapshot{
		Workload: workload{
			replicas: 1,
			selector: &metav1.LabelSelector{MatchLabels: appLabels},
			template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
			},
		},
		Pods: []corev1.Pod{runningPod("p1", "node-a"), runningPod("p2", "node-a")},
	}
	clean := Snapshot{
		Workload: resilientWorkload(),
		Pods:     []corev1.Pod{runningPod("p1", "node-a"), runningPod("p2", "node-b")},
		PDBs: []policyv1.PodDisruptionBudget{{
			ObjectMeta: metav1.ObjectMeta{Name: "pdb", Namespace: ns},
			Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: appLabels}},
		}},
	}

	for _, rule := range rules {
		for _, tc := range []struct {
			name string
			snap Snapshot
		}{{"risky", risky}, {"clean", clean}} {
			t.Run(string(rule.ID())+"/"+tc.name, func(t *testing.T) {
				detected := rule.Detect(tc.snap)
				if got, want := rule.Mitigated(tc.snap), detected == nil; got != want {
					t.Fatalf("Mitigated=%v disagrees with Detect==nil (%v)", got, want)
				}
				if detected != nil {
					if detected.Rule != rule.ID() {
						t.Errorf("Detect returned rule %q, want %q", detected.Rule, rule.ID())
					}
					if detected.Message == "" {
						t.Errorf("rule %q returned an empty message", rule.ID())
					}
				}
			})
		}
	}

	// Every rule must flag the fully risky snapshot and clear the clean one.
	for _, rule := range rules {
		if rule.Detect(risky) == nil {
			t.Errorf("rule %q did not fire on the fully risky snapshot", rule.ID())
		}
		if rule.Detect(clean) != nil {
			t.Errorf("rule %q fired on the clean snapshot", rule.ID())
		}
	}
}
