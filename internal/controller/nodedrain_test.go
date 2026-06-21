package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/scenario"
)

// These exercise the NodeDrain scenario directly against the envtest apiserver,
// which handles the eviction subresource correctly. The PDB-blocked finding is
// not covered here — it needs the disruption controller (kind), not envtest.
var _ = Describe("NodeDrain scenario", func() {
	It("cordons the busiest node, evicts its target pods, and reverts", func() {
		createNode(ctx, "nd-node-a")
		createNode(ctx, "nd-node-b")
		dep := createDeployment(ctx, "nd-dep", "default", 3)
		createRunningPodsOnNode(ctx, dep, "nd-node-a", 2, 0) // busiest
		createRunningPodsOnNode(ctx, dep, "nd-node-b", 1, 2)

		nd := &scenario.NodeDrain{Client: k8sClient, Owner: "default/nd-trial"}
		target := scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"}

		result, err := nd.Inject(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.PodsAffected).To(Equal(2)) // only node-a's pods
		Expect(result.Findings).To(BeEmpty())

		// Busiest node cordoned + tagged.
		var nodeA corev1.Node
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "nd-node-a"}, &nodeA)).To(Succeed())
		Expect(nodeA.Spec.Unschedulable).To(BeTrue())
		Expect(nodeA.Annotations).To(HaveKeyWithValue(temperv1alpha1.AnnotationCordonedBy, "default/nd-trial"))

		// The other node is left alone.
		var nodeB corev1.Node
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "nd-node-b"}, &nodeB)).To(Succeed())
		Expect(nodeB.Spec.Unschedulable).To(BeFalse())

		// Revert uncordons and removes the tag.
		Expect(nd.Revert(ctx, target)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "nd-node-a"}, &nodeA)).To(Succeed())
		Expect(nodeA.Spec.Unschedulable).To(BeFalse())
		Expect(nodeA.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationCordonedBy))
	})

	It("Revert is a no-op when this trial cordoned nothing", func() {
		nd := &scenario.NodeDrain{Client: k8sClient, Owner: "default/nd-never-ran"}
		Expect(nd.Revert(ctx, scenario.Target{Namespace: "default"})).To(Succeed())
	})

	It("errors when the target has no running pods", func() {
		dep := createDeployment(ctx, "nd-no-pods", "default", 0)
		nd := &scenario.NodeDrain{Client: k8sClient, Owner: "default/nd-no-pods"}
		_, err := nd.Inject(ctx, scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"})
		Expect(err).To(HaveOccurred())
	})
})
