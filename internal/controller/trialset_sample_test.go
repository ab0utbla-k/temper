package controller

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// The sample manifest at config/samples/v1alpha1_trialset.yaml must apply
// cleanly against a real apiserver running the generated TrialSet CRD. This
// test loads the exact sample file and performs a server-side dry-run Create
// against the envtest apiserver (already running for the controller suite),
// so any CRD schema violation surfaces as a test failure. A real kind cluster
// is not required here because envtest runs the same kube-apiserver binary.
var _ = Describe("TrialSet sample manifest", Serial, func() {
	It("config/samples/v1alpha1_trialset.yaml applies cleanly (server-side dry run)", func() {
		path := filepath.Join("..", "..", "config", "samples", "v1alpha1_trialset.yaml")
		bytes, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred(), "read sample manifest")

		// Decode the sample YAML into a TrialSet via the scheme's universal
		// deserializer, so we exercise the same code path as kubectl apply.
		decoder := serializer.NewCodecFactory(k8sClient.Scheme()).UniversalDeserializer()
		obj, gvk, err := decoder.Decode(bytes, nil, nil)
		Expect(err).NotTo(HaveOccurred(), "decode sample manifest")
		Expect(gvk.Kind).To(Equal("TrialSet"))

		trialSet, ok := obj.(*temperv1alpha1.TrialSet)
		Expect(ok).To(BeTrue(), "decoded object is a *TrialSet")

		// The sample intentionally omits metadata.namespace so it applies to any
		// namespace. For a dry-run Create the object needs a namespace; use the
		// suite's test namespace. The namespaces field (payments/payments-canary)
		// is a discovery-scope config of plain strings, not an apiserver-validated
		// reference, so it does not need those namespaces to exist at apply time.
		trialSet.Namespace = "default"
		trialSet.Name = "sample-dryrun"

		// DryRunAll performs full server-side validation (CRD schema, required
		// fields, defaults) without persisting the object.
		err = k8sClient.Create(ctx, trialSet, client.DryRunAll)
		Expect(err).NotTo(HaveOccurred(), "server-side dry-run apply of sample manifest")
	})
})
