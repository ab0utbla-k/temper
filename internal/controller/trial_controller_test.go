package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		trial := createTrial(ctx, "exp-happy", "default", dep.Name, 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseRunning))
		}, timeout, interval).Should(Succeed())

		patchDeploymentAvailable(ctx, dep.Name, dep.Namespace)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Metrics).NotTo(BeNil())
			g.Expect(got.Status.Metrics.TotalPodsKilled).To(BeNumerically(">", 0))
		}, 20*time.Second, interval).Should(Succeed())
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
			// Ghost state: a failed-but-marked attempt must stay marked, or the
			// next reconcile would re-enter the inject path.
			g.Expect(got.Status.InjectedAt).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())

		// At-most-once: requeues after the failure must not call Inject again.
		Consistently(failInjectCalls.Load, 2*time.Second, interval).Should(Equal(int32(1)))
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
		patchDeploymentAvailable(ctx, dep.Name, dep.Namespace)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
		}, 20*time.Second, interval).Should(Succeed())
	})
})
