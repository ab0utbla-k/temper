package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

var _ = Describe("Trial passport", func() {
	It("should stamp a passport on a passed spot-reclaim trial", func() {
		createNode(ctx, "passport-node")
		dep := createDeployment(ctx, "dep-passport", "default", 2)
		createRunningPodsOnNode(ctx, dep, "passport-node", 2, 0)
		// Duration must comfortably exceed the 5s recovery grace period so at
		// least one recovery poll runs before the scenario ends.
		trial := createSpotReclaimTrial(ctx, "exp-passport", "default", dep.Name, 15*time.Second)
		key := client.ObjectKeyFromObject(trial)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.InjectedAt).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())

		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 2, false)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Outcome).To(Equal(temperv1alpha1.OutcomePassed))

			var gotDep appsv1.Deployment
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: dep.Namespace, Name: dep.Name}, &gotDep)).To(Succeed())
			g.Expect(got.Status.Passport).NotTo(BeNil())
			g.Expect(got.Status.Passport.Eligible).To(BeTrue())
			g.Expect(got.Status.Passport.TestedGeneration).To(Equal(gotDep.Generation))
			g.Expect(got.Status.Passport.ExpiresAt.Time).To(BeTemporally(">", metav1.Now().Time))
		}, 25*time.Second, interval).Should(Succeed())
	})

	It("should not stamp a passport on a failed spot-reclaim trial", func() {
		createNode(ctx, "passport-fail-node")
		dep := createDeployment(ctx, "dep-passport-fail", "default", 2)
		createRunningPodsOnNode(ctx, dep, "passport-fail-node", 2, 0)
		// Never patch the deployment ready: no recovery poll succeeds, so the
		// scenario ends unrecovered and the outcome is Failed.
		trial := createSpotReclaimTrial(ctx, "exp-passport-fail", "default", dep.Name, 8*time.Second)
		key := client.ObjectKeyFromObject(trial)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialPhaseCompleted))
			g.Expect(got.Status.Outcome).To(Equal(temperv1alpha1.OutcomeFailed))
			g.Expect(got.Status.Passport).To(BeNil())
		}, 25*time.Second, interval).Should(Succeed())
	})
})
