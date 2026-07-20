package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// hasRiskRule reports whether the trial status carries the given risk token.
func hasRiskRule(risks []temperv1alpha1.Risk, rule temperv1alpha1.RiskRule) bool {
	for _, r := range risks {
		if r.Rule == rule {
			return true
		}
	}
	return false
}

// createPDB creates a PodDisruptionBudget selecting the given app label.
func createPDB(ctx context.Context, name, namespace, app string) {
	minAvail := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvail,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
		},
	}
	Expect(k8sClient.Create(ctx, pdb)).To(Succeed())
}

// hardenDeployment adds readiness/liveness probes and pod anti-affinity to a
// deployment's template so the probe and anti-affinity risks do not fire.
func hardenDeployment(dep *appsv1.Deployment) {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)},
	}}
	for i := range dep.Spec.Template.Spec.Containers {
		dep.Spec.Template.Spec.Containers[i].ReadinessProbe = probe
		dep.Spec.Template.Spec.Containers[i].LivenessProbe = probe
	}
	dep.Spec.Template.Spec.Affinity = &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					TopologyKey:   "kubernetes.io/hostname",
					LabelSelector: &metav1.LabelSelector{MatchLabels: dep.Spec.Template.Labels},
				},
			}},
		},
	}
}

var _ = Describe("Trial risk detection", func() {
	It("records single-replica, no-anti-affinity, missing-probes and no-PDB risks", func() {
		dep := createDeployment(ctx, "dep-risky", "default", 1)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-risky", "default", dep.Name, 15*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskSingleReplica)).To(BeTrue())
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskNoPodAntiAffinity)).To(BeTrue())
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskMissingHealthProbes)).To(BeTrue())
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskNoPodDisruptionBudget)).To(BeTrue())
			// One replica cannot be "concentrated".
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskConcentratedPlacement)).To(BeFalse())
			for _, r := range got.Status.Risks {
				g.Expect(r.Message).NotTo(BeEmpty())
			}
		}, timeout, interval).Should(Succeed())
	})

	It("records the concentrated-placement risk when all pods share a node", func() {
		dep := createDeployment(ctx, "dep-concentrated", "default", 2)
		// Harden everything else so only concentrated placement is in question.
		hardenDeployment(dep)
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())
		createPDB(ctx, "pdb-concentrated", "default", dep.Name)
		createRunningPodsOnNode(ctx, dep, "node-shared", 2, 0)
		trial := createTrial(ctx, "exp-concentrated", "default", dep.Name, 15*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskConcentratedPlacement)).To(BeTrue())
			// The hardened, multi-replica, PDB-protected workload must not
			// trip the other risks.
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskSingleReplica)).To(BeFalse())
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskNoPodAntiAffinity)).To(BeFalse())
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskMissingHealthProbes)).To(BeFalse())
			g.Expect(hasRiskRule(got.Status.Risks, temperv1alpha1.RiskNoPodDisruptionBudget)).To(BeFalse())
		}, timeout, interval).Should(Succeed())
	})

	It("records no risks for a resilient workload", func() {
		dep := createDeployment(ctx, "dep-clean", "default", 3)
		hardenDeployment(dep)
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())
		createPDB(ctx, "pdb-clean", "default", dep.Name)
		createRunningPodsOnNode(ctx, dep, "node-a", 1, 0)
		createRunningPodsOnNode(ctx, dep, "node-b", 1, 1)
		createRunningPodsOnNode(ctx, dep, "node-c", 1, 2)
		trial := createTrial(ctx, "exp-clean", "default", dep.Name, 15*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
		}, timeout, interval).Should(Succeed())

		// Risks are written in the same status update that enters Running, so
		// once Running is observed the empty slice is authoritative.
		var got temperv1alpha1.Trial
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
		Expect(got.Status.Risks).To(BeEmpty())
	})
})

var _ = Describe("Trial risk detection for StatefulSet target", func() {
	It("detects risks on a StatefulSet target without running scenarios", func() {
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "sts-risky", Namespace: "default"},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    new(int32(1)),
				ServiceName: "sts-risky",
				Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": "sts-risky"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "sts-risky"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, sts)).To(Succeed())

		risks := detectStatefulSetRisks(ctx, "default", "sts-risky")
		Expect(hasRiskRule(risks, temperv1alpha1.RiskSingleReplica)).To(BeTrue())
		Expect(hasRiskRule(risks, temperv1alpha1.RiskNoPodAntiAffinity)).To(BeTrue())
		Expect(hasRiskRule(risks, temperv1alpha1.RiskMissingHealthProbes)).To(BeTrue())
		Expect(hasRiskRule(risks, temperv1alpha1.RiskNoPodDisruptionBudget)).To(BeTrue())
	})
})

// detectStatefulSetRisks is a thin wrapper so the StatefulSet spec is exercised
// through the same detector the controller uses.
func detectStatefulSetRisks(ctx context.Context, namespace, name string) []temperv1alpha1.Risk {
	r := &TrialReconciler{Client: k8sClient}
	trial := &temperv1alpha1.Trial{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace},
		Spec: temperv1alpha1.TrialSpec{
			Target: temperv1alpha1.Target{Kind: "StatefulSet", Name: &name},
		},
	}
	r.detectRisks(ctx, trial)
	return trial.Status.Risks
}
