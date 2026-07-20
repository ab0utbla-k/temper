package controller

import (
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/scenario"
)

// Tests the HTTP recovery probe directly. It needs no apiserver: it only
// makes an HTTP GET, so httptest servers stand in for the target service.
var _ = Describe("HTTP recovery probe", func() {
	It("reports recovered on a 2xx response", func() {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		recovered, err := checkHTTPRecovery(ctx, &scenario.HTTPProbe{URL: srv.URL})
		Expect(err).NotTo(HaveOccurred())
		Expect(recovered).To(BeTrue())
	})

	It("reports not recovered on a non-2xx response", func() {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		recovered, err := checkHTTPRecovery(ctx, &scenario.HTTPProbe{URL: srv.URL})
		Expect(err).NotTo(HaveOccurred())
		Expect(recovered).To(BeFalse())
	})

	It("reports not recovered (no error) when the service is unreachable", func() {
		// Start then immediately close: the URL is valid but nothing listens,
		// so the GET is refused — the normal "still recovering" signal.
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := srv.URL
		srv.Close()

		recovered, err := checkHTTPRecovery(ctx, &scenario.HTTPProbe{URL: url})
		Expect(err).NotTo(HaveOccurred())
		Expect(recovered).To(BeFalse())
	})

	It("errors on a malformed URL", func() {
		recovered, err := checkHTTPRecovery(ctx, &scenario.HTTPProbe{URL: "http://%zz"})
		Expect(err).To(HaveOccurred())
		Expect(recovered).To(BeFalse())
	})
})

// Tests that a Trial's recovery.http override drives recovery detection
// instead of the scenario's default WorkloadReady probe.
var _ = Describe("Trial HTTP recovery override", func() {
	It("records recovery from the HTTP probe even when the workload is degraded", func() {
		// The service answers 200 regardless of replica counts.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		dep := createDeployment(ctx, "dep-http-recovery", "default", 3)
		createRunningPods(ctx, dep)

		trial := &temperv1alpha1.Trial{
			ObjectMeta: metav1.ObjectMeta{Name: "exp-http-recovery", Namespace: "default"},
			Spec: temperv1alpha1.TrialSpec{
				Target: temperv1alpha1.Target{Kind: "Deployment", Name: new(dep.Name)},
				Scenarios: []temperv1alpha1.Scenario{{
					Type:     temperv1alpha1.ScenarioTypePodKill,
					Duration: metav1.Duration{Duration: 60 * time.Second},
				}},
				Recovery: &temperv1alpha1.RecoverySpec{
					HTTP: &temperv1alpha1.HTTPRecoveryProbe{URL: srv.URL},
				},
			},
		}
		Expect(k8sClient.Create(ctx, trial)).To(Succeed())
		key := client.ObjectKeyFromObject(trial)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.InjectedAt).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())

		// Keep the workload degraded: the default WorkloadReady probe would
		// report "not recovered". The HTTP probe answers 200, so recovery is
		// recorded anyway — proof the override replaces the default.
		patchDeploymentStatus(ctx, dep.Name, dep.Namespace, 1, false)

		Eventually(func(g Gomega) {
			var got temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
			g.Expect(got.Status.RecoveredAt).NotTo(BeNil())
		}, 15*time.Second, interval).Should(Succeed())

		Expect(k8sClient.Delete(ctx, trial)).To(Succeed())
	})
})
