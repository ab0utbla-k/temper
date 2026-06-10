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

var _ = Describe("ChaosExperiment Controller", func() {
	It("should add finalizer on creation", func() {
		dep := createDeployment(ctx, "dep-finalizer", "default", 1)
		createRunningPods(ctx, dep)
		exp := createExperiment(ctx, "exp-finalizer", "default", dep.Name, 30*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)).To(Succeed())
			g.Expect(controllerutil.ContainsFinalizer(&got, experimentFinalizer)).To(BeTrue())
		}, timeout, interval).Should(Succeed())
	})

	It("should run pod-kill and complete", func() {
		dep := createDeployment(ctx, "dep-happy", "default", 3)
		createRunningPods(ctx, dep)
		exp := createExperiment(ctx, "exp-happy", "default", dep.Name, 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseRunning))
		}, timeout, interval).Should(Succeed())

		patchDeploymentAvailable(ctx, dep.Name, dep.Namespace)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseCompleted))
			g.Expect(got.Status.Metrics).NotTo(BeNil())
			g.Expect(got.Status.Metrics.TotalPodsKilled).To(BeNumerically(">", 0))
		}, 20*time.Second, interval).Should(Succeed())
	})

	It("should revert on deletion while running", func() {
		dep := createDeployment(ctx, "dep-delete", "default", 1)
		createRunningPods(ctx, dep)
		exp := createExperiment(ctx, "exp-delete", "default", dep.Name, 30*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseRunning))
		}, timeout, interval).Should(Succeed())

		Expect(k8sClient.Delete(ctx, exp)).To(Succeed())

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}, timeout, interval).Should(Succeed())
	})

	It("should be idempotent on halt re-entry", func() {
		dep := createDeployment(ctx, "dep-halt-reentry", "default", 2)
		createRunningPods(ctx, dep)
		exp := createExperiment(ctx, "exp-halt-reentry", "default", dep.Name, 30*time.Second)
		key := client.ObjectKeyFromObject(exp)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseRunning))
		}, timeout, interval).Should(Succeed())

		// First halt — simulates safeguard watcher writing the annotations.
		setHaltAnnotation(ctx, key, "reason1", temperv1alpha1.HaltCodeAlertMatch)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseHalted))
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltReason))
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltCode))
			g.Expect(got.Status.HaltReason).NotTo(BeNil())
			g.Expect(*got.Status.HaltReason).To(Equal("reason1"))
		}, timeout, interval).Should(Succeed())

		// Simulate crash-recovery: annotations reappear after Halted was already written.
		// The re-entry guard must clean up both without re-running the halt logic.
		setHaltAnnotation(ctx, key, "reason2", temperv1alpha1.HaltCodeSLOBreach)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltReason))
			g.Expect(got.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationHaltCode))
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseHalted))
			// HaltReason stays "reason1". If the guard were missing, the main halt path
			// would run again and overwrite it with "reason2".
			g.Expect(*got.Status.HaltReason).To(Equal("reason1"))
		}, timeout, interval).Should(Succeed())
	})

	It("should leave InjectedAt set and not re-inject when Inject fails", func() {
		dep := createDeployment(ctx, failInjectTarget, "default", 1)
		createRunningPods(ctx, dep)
		exp := createExperiment(ctx, "exp-fail-inject", "default", dep.Name, 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseFailed))
			// Ghost state: a failed-but-marked attempt must stay marked, or the
			// next reconcile would re-enter the inject path.
			g.Expect(got.Status.InjectedAt).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())

		// At-most-once: requeues after the failure must not call Inject again.
		Consistently(failInjectCalls.Load, 2*time.Second, interval).Should(Equal(int32(1)))
	})

	It("should fail when target deployment doesn't exist", func() {
		exp := createExperiment(ctx, "exp-no-target", "default", "nonexistent", 5*time.Second)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.ChaosExperiment
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(exp), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.ExperimentPhaseFailed))
		}, timeout, interval).Should(Succeed())
	})
})
