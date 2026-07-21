package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

var _ = Describe("Trial Controller", func() {
	It("should add finalizer on creation", func() {
		dep := createDeployment(ctx, "dep-finalizer", "default", 1)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-finalizer", "default", dep.Name, 30*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(controllerutil.ContainsFinalizer(&got, trialFinalizer)).To(BeTrue())
		}, timeout, interval).Should(Succeed())
	})

	It("should run pod-kill and complete", func() {
		dep := createDeployment(ctx, "dep-happy", "default", 3)
		createRunningPods(ctx, dep)
		// The duration must comfortably exceed the 5s recovery grace period:
		// polling starts only after it, and a trial that ends without one
		// successful recovery poll gets Outcome=Failed.
		trial := createTrial(ctx, "exp-happy", "default", dep.Name, 15*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
		}, timeout, interval).Should(Succeed())

		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 3, false)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Outcome).To(Equal(temperv1alpha1.OutcomePassed))
			g.Expect(got.Status.Metrics).NotTo(BeNil())
			g.Expect(got.Status.Metrics.TotalPodsKilled).To(BeNumerically(">", 0))

			// Passports are for spot-reclaim only; a passed pod-kill gets none.
			g.Expect(got.Status.Passport).To(BeNil())
		}, 25*time.Second, interval).Should(Succeed())
	})

	It("should record recovery only at full strength with current status", func() {
		dep := createDeployment(ctx, "dep-recovery", "default", 3)
		createRunningPods(ctx, dep)
		// Duration long enough for the whole timeline below to fit in the run.
		trial := createTrial(ctx, "exp-recovery", "default", dep.Name, 60*time.Second)
		key := client.ObjectKeyFromObject(trial)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.InjectedAt).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())

		// Degraded: 2 of 3 ready. The old Available=True signal would already
		// call this recovered; the WorkloadReady probe must not.
		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 2, false)

		// Window covers the 5s recovery grace period plus a poll round.
		Consistently(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.RecoveredAt).To(BeNil())
		}, 8*time.Second, interval).Should(Succeed())

		// Full strength, but the status describes an older generation — the
		// numbers can't be trusted yet, so still no recovery.
		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 3, true)

		Consistently(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.RecoveredAt).To(BeNil())
		}, 6*time.Second, interval).Should(Succeed())

		// Full strength with a current status — this is recovery.
		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 3, false)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.RecoveredAt).NotTo(BeNil())

			// The per-scenario row survives with the same timeline.
			g.Expect(got.Status.ScenarioResults).To(HaveLen(1))
			res := got.Status.ScenarioResults[0]
			g.Expect(res.Type).To(Equal(temperv1alpha1.ScenarioTypePodKill))
			g.Expect(res.InjectedAt.IsZero()).To(BeFalse())
			g.Expect(res.ReadyAt).NotTo(BeNil())
			g.Expect(res.RecoveredAt).NotTo(BeNil())
			g.Expect(res.Findings).To(BeEmpty())
		}, 15*time.Second, interval).Should(Succeed())

		// Don't leave the 60s trial running under the other specs.
		Expect(k8sClient.Delete(ctx, trial)).To(Succeed())
	})

	It("should revert on deletion while running", func() {
		dep := createDeployment(ctx, "dep-delete", "default", 1)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-delete", "default", dep.Name, 30*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
		}, timeout, interval).Should(Succeed())

		Expect(k8sClient.Delete(ctx, trial)).To(Succeed())

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, timeout, interval).Should(Succeed())
	})

	It("should be idempotent on halt re-entry", func() {
		dep := createDeployment(ctx, "dep-halt-reentry", "default", 2)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-halt-reentry", "default", dep.Name, 30*time.Second)
		key := client.ObjectKeyFromObject(trial)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
		}, timeout, interval).Should(Succeed())

		// First halt — simulates safeguard watcher writing the annotations.
		setHaltAnnotation(ctx, key, "reason1", temperv1alpha1.HaltCodeAlertMatch)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseHalted))
			g.Expect(got.Status.Outcome).To(Equal(temperv1alpha1.OutcomeHalted))
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltReason))
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltCode))
			g.Expect(got.Status.HaltReason).NotTo(BeNil())
			g.Expect(*got.Status.HaltReason).To(Equal("reason1"))
		}, timeout, interval).Should(Succeed())

		// Simulate crash-recovery: annotations reappear after Halted was already written.
		// The re-entry guard must clean up both without re-running the halt logic.
		setHaltAnnotation(ctx, key, "reason2", temperv1alpha1.HaltCodeSLOBreach)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltReason))
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltCode))
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseHalted))
			// HaltReason stays "reason1". If the guard were missing, the main halt path
			// would run again and overwrite it with "reason2".
			g.Expect(*got.Status.HaltReason).To(Equal("reason1"))
		}, timeout, interval).Should(Succeed())
	})

	It("should leave InjectedAt set and not re-inject when Inject fails", func() {
		dep := createDeployment(ctx, failInjectTarget, "default", 1)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-fail-inject", "default", dep.Name, 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseFailed))
			// A tool error is not an experiment verdict: Outcome stays empty.
			g.Expect(got.Status.Outcome).To(BeEmpty())
			// Ghost state: a failed-but-marked attempt must stay marked, or the
			// next reconcile would re-enter the inject path.
			g.Expect(got.Status.InjectedAt).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())

		// At-most-once: requeues after the failure must not call Inject again.
		Consistently(failInjectCalls.Load, 2*time.Second, interval).Should(Equal(int32(1)))
	})

	It("should conclude Blocked when evictions stay blocked past the timeout", func() {
		createNode(ctx, blockedEvictionNode)
		dep := createDeployment(ctx, blockedEvictionTarget, "default", 1)
		createRunningPods(ctx, dep)
		trial := createNodeDrainTrial(ctx, "exp-blocked", "default", dep.Name, 30*time.Second, 3*time.Second)
		key := client.ObjectKeyFromObject(trial)

		// While evictions are blocked, the retry flag is up.
		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.InjectionIncomplete).To(BeTrue())
		}, timeout, interval).Should(Succeed())

		// Past the eviction timeout: Blocked verdict, flag down, no cordon
		// left, and the finding names the guilty PDB.
		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Outcome).To(Equal(temperv1alpha1.OutcomeBlocked))
			g.Expect(got.Status.InjectionIncomplete).To(BeFalse())

			g.Expect(got.Status.ScenarioResults).To(HaveLen(1))
			g.Expect(got.Status.ScenarioResults[0].Findings).NotTo(BeEmpty())
			g.Expect(got.Status.ScenarioResults[0].Findings[0].Message).To(ContainSubstring("test-pdb"))

			var node corev1.Node
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: blockedEvictionNode}, &node)).To(Succeed())
			g.Expect(node.Spec.Unschedulable).To(BeFalse())
		}, 20*time.Second, interval).Should(Succeed())
	})

	It("should resume the normal flow when blocked evictions later succeed", func() {
		dep := createDeployment(ctx, yieldingTarget, "default", 1)
		createRunningPods(ctx, dep)
		trial := createNodeDrainTrial(ctx, "exp-yielding", "default", dep.Name, 5*time.Second, 30*time.Second)
		key := client.ObjectKeyFromObject(trial)

		// Assert the retry happened via the stub's call counter, which only
		// grows. Polling status.injectionIncomplete instead would race the
		// controller: it is a mid-flight flag that clears as soon as the stub
		// yields, so a run that gets through the attempts between two polls
		// never observes it. The flag is still asserted below, where it has
		// settled.
		Eventually(func(g Gomega) {
			g.Expect(yieldingInjectCalls.Load()).To(BeNumerically(">=", 3))
		}, 20*time.Second, interval).Should(Succeed())

		// The stub yields on the third attempt: the flag drops and the trial
		// runs to a normal end — anything but Blocked.
		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Outcome).NotTo(Equal(temperv1alpha1.OutcomeBlocked))
			g.Expect(got.Status.InjectionIncomplete).To(BeFalse())
		}, 30*time.Second, interval).Should(Succeed())
	})

	It("should leave no cordon behind when Inject fails after mutating", func() {
		createNode(ctx, cordonLeakNode)
		dep := createDeployment(ctx, failAfterCordonTarget, "default", 1)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-cordon-leak", "default", dep.Name, 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseFailed))
		}, timeout, interval).Should(Succeed())

		// The item-1 regression: failTrial must revert before going terminal,
		// so the node cordoned by the failed Inject ends up uncordoned.
		Eventually(func(g Gomega) {
			var node corev1.Node
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cordonLeakNode}, &node)).To(Succeed())
			g.Expect(node.Spec.Unschedulable).To(BeFalse())
		}, timeout, interval).Should(Succeed())
	})

	It("should target a Deployment in a different namespace via spec.target.namespace", func() {
		// The Trial lives in "default" but targets a Deployment in "alt". The
		// pod-kill scenario must act on pods in "alt", the recovery probe must
		// watch the Deployment in "alt", and revert must clean up there too.
		// envtest only pre-creates "default"; "alt" must be created explicitly.
		createNamespace(ctx, "alt")

		dep := createDeployment(ctx, "dep-cross-ns", "alt", 3)
		createRunningPods(ctx, dep)
		trial := createCrossNamespaceTrial(ctx, "exp-cross-ns", "default", dep.Name, "alt", 15*time.Second)
		key := client.ObjectKeyFromObject(trial)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
		}, timeout, interval).Should(Succeed())

		// Recovery and completion are reported against the Deployment in "alt".
		patchDeploymentStatus(ctx, dep.Name, "alt", 3, false)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Outcome).To(Equal(temperv1alpha1.OutcomePassed))
			g.Expect(got.Status.Metrics).NotTo(BeNil())
			g.Expect(got.Status.Metrics.TotalPodsKilled).To(BeNumerically(">", 0))
		}, 25*time.Second, interval).Should(Succeed())
	})

	It("should fail when target deployment doesn't exist", func() {
		trial := createTrial(ctx, "exp-no-target", "default", "nonexistent", 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseFailed))
		}, timeout, interval).Should(Succeed())
	})

	It("should wait, not fail, when target pods aren't running yet", func() {
		// Deployment exists but has no running pods (envtest has no controllers to
		// create them) — the same state as a Trial applied alongside its workload.
		dep := createDeployment(ctx, "dep-wait", "default", 1)
		trial := createTrial(ctx, "exp-wait", "default", dep.Name, 2*time.Second)
		key := client.ObjectKeyFromObject(trial)

		// With no running pods, the Trial must keep waiting rather than Fail.
		Consistently(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).NotTo(Equal(temperv1alpha1.TrialPhaseFailed))
		}, 5*time.Second, interval).Should(Succeed())

		// Once pods are running it injects and runs to completion.
		createRunningPods(ctx, dep)
		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 1, false)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
		}, 20*time.Second, interval).Should(Succeed())
	})
})
