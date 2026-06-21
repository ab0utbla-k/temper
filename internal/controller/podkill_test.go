package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ab0utbla-k/temper/internal/scenario"
)

// Tests the PodKill scenario directly against the envtest apiserver.
var _ = Describe("PodKill scenario", func() {
	It("deletes count pods and reports them", func() {
		dep := createDeployment(ctx, "pk-dep", "default", 3)
		createRunningPods(ctx, dep)

		pk := &scenario.PodKill{Client: k8sClient, Count: 2}
		target := scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"}

		result, err := pk.Inject(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.PodsAffected).To(Equal(2))
		Expect(result.Findings).To(BeEmpty())

		// One pod survives; the other two are deleted (gone or Terminating).
		var pods corev1.PodList
		Expect(k8sClient.List(ctx, &pods,
			client.InNamespace("default"),
			client.MatchingLabels{"app": dep.Name})).To(Succeed())
		live := 0
		for _, p := range pods.Items {
			if p.DeletionTimestamp == nil {
				live++
			}
		}
		Expect(live).To(Equal(1))
	})

	It("caps at the number of running pods", func() {
		dep := createDeployment(ctx, "pk-cap", "default", 2)
		createRunningPods(ctx, dep)

		pk := &scenario.PodKill{Client: k8sClient, Count: 5}
		result, err := pk.Inject(ctx, scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.PodsAffected).To(Equal(2)) // capped at what's running
	})

	It("errors when there are no running pods", func() {
		dep := createDeployment(ctx, "pk-no-pods", "default", 0)
		pk := &scenario.PodKill{Client: k8sClient, Count: 1}
		_, err := pk.Inject(ctx, scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"})
		Expect(err).To(HaveOccurred())
	})
})
