package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/scenario"
)

// These exercise the SpotReclaim scenario directly against the envtest
// apiserver. Node selection and cordon are covered via the shared helpers in
// the NodeDrain specs; here we focus on spot's real difference: it deletes
// target pods directly, bypassing any PodDisruptionBudget.
var _ = Describe("SpotReclaim scenario", func() {
	It("cordons the busiest node, deletes its target pods, and reverts", func() {
		createNode(ctx, "sr-node-a")
		createNode(ctx, "sr-node-b")
		dep := createDeployment(ctx, "sr-dep", "default", 3)
		createRunningPodsOnNode(ctx, dep, "sr-node-a", 2, 0) // busiest
		createRunningPodsOnNode(ctx, dep, "sr-node-b", 1, 2)

		sr := &scenario.SpotReclaim{Client: k8sClient, Owner: "default/sr-trial"}
		target := scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"}

		result, err := sr.Inject(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.PodsAffected).To(Equal(2)) // only node-a's pods
		Expect(result.Findings).To(BeEmpty())
		Expect(result.Incomplete).To(BeFalse())

		// node-a's pods are deleted; node-b's one pod survives. envtest has no
		// kubelet, so a deleted pod lingers with a DeletionTimestamp set.
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

		// Busiest node cordoned + tagged.
		var nodeA corev1.Node
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "sr-node-a"}, &nodeA)).To(Succeed())
		Expect(nodeA.Spec.Unschedulable).To(BeTrue())
		Expect(nodeA.Annotations).To(HaveKeyWithValue(temperv1alpha1.AnnotationCordonedBy, "default/sr-trial"))

		// The other node is left alone.
		var nodeB corev1.Node
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "sr-node-b"}, &nodeB)).To(Succeed())
		Expect(nodeB.Spec.Unschedulable).To(BeFalse())

		// Revert uncordons and removes the tag.
		Expect(sr.Revert(ctx, target)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "sr-node-a"}, &nodeA)).To(Succeed())
		Expect(nodeA.Spec.Unschedulable).To(BeFalse())
		Expect(nodeA.Annotations).NotTo(HaveKey(temperv1alpha1.AnnotationCordonedBy))
	})

	It("deletes target pods even behind a PodDisruptionBudget that allows no disruptions", func() {
		createNode(ctx, "sr-pdb-node")
		dep := createDeployment(ctx, "sr-pdb-dep", "default", 2)
		createRunningPodsOnNode(ctx, dep, "sr-pdb-node", 2, 0)

		// minAvailable == replicas allows zero disruptions: this would block a
		// voluntary eviction (drain), but not an involuntary spot reclaim.
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "sr-pdb", Namespace: "default"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: new(intstr.FromInt32(2)),
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": dep.Name}},
			},
		}
		Expect(k8sClient.Create(ctx, pdb)).To(Succeed())

		sr := &scenario.SpotReclaim{Client: k8sClient, Owner: "default/sr-pdb-trial"}
		target := scenario.Target{Name: dep.Name, Namespace: "default", Kind: "Deployment"}

		result, err := sr.Inject(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.PodsAffected).To(Equal(2)) // deleted despite the PDB
		Expect(result.Findings).To(BeEmpty())    // no Blocked finding — delete bypasses the PDB
		Expect(result.Incomplete).To(BeFalse())

		Expect(sr.Revert(ctx, target)).To(Succeed())
	})
})
