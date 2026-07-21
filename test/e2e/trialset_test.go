//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ab0utbla-k/temper/test/utils"
)

// trialSetTestNamespace is the namespace used for the TrialSet e2e test. It is
// separate from the controller's temper-system namespace so the test does not
// collide with controller pods. The controller is cluster-scoped, so it can
// watch and reconcile objects here.
const trialSetTestNamespace = "temper-trialset-e2e"

// deploymentManifests is the YAML for two labeled Deployments the TrialSet will
// discover. The pause image is cheap and keeps the e2e run fast; the Trial's
// pod-kill needs real pods to kill, so replicas=3 and a real pod template.
const deploymentManifests = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: payment-a
  namespace: ` + trialSetTestNamespace + `
  labels:
    temper.io/e2e-trialset: payments
spec:
  replicas: 3
  selector:
    matchLabels:
      app: payment-a
  template:
    metadata:
      labels:
        app: payment-a
        temper.io/e2e-trialset: payments
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.9
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payment-b
  namespace: ` + trialSetTestNamespace + `
  labels:
    temper.io/e2e-trialset: payments
spec:
  replicas: 3
  selector:
    matchLabels:
      app: payment-b
  template:
    metadata:
      labels:
        app: payment-b
        temper.io/e2e-trialset: payments
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.9
`

// trialSetManifest is the TrialSet that matches the two Deployments above and
// runs a pod-kill scenario against each. maxConcurrent=2 so both Trials are
// created without waiting for the first to complete. minReadyReplicas=1 so the
// Deployments (3 replicas each) pass the discovery filter once they roll out.
const trialSetManifest = `apiVersion: temper.io/v1alpha1
kind: TrialSet
metadata:
  name: payments-trialset
  namespace: ` + trialSetTestNamespace + `
spec:
  targetSelector:
    matchLabels:
      temper.io/e2e-trialset: payments
  trialTemplate:
    scenarios:
      - type: pod-kill
        duration: 5s
        podKill:
          count: 1
  maxConcurrent: 2
  minReadyReplicas: 1
`

var _ = Describe("TrialSet controller", Ordered, func() {
	BeforeAll(func() {
		By("creating the TrialSet test namespace")
		cmd := exec.Command("kubectl", "create", "ns", trialSetTestNamespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create TrialSet test namespace")
	})

	AfterAll(func() {
		// Best-effort cleanup. The TrialSet's generated Trials are owned by it,
		// so deleting the namespace reaps both the TrialSet and its Trials.
		By("deleting the TrialSet test namespace")
		cmd := exec.Command("kubectl", "delete", "ns", trialSetTestNamespace,
			"--ignore-not-found=true", "--timeout=60s")
		_, _ = utils.Run(cmd)
	})

	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)

	It("generates one Trial per matching Deployment and reaches Completed", func() {
		By("applying two labeled Deployments")
		Expect(applyManifest(deploymentManifests)).To(Succeed(),
			"Failed to apply target Deployments")

		By("waiting for both Deployments to become ready")
		Eventually(func(g Gomega) {
			for _, name := range []string{"payment-a", "payment-b"} {
				ready := kubectlOrEmpty("get", "deployment", name, "-n", trialSetTestNamespace,
					"-o", "jsonpath={.status.readyReplicas}")
				g.Expect(ready).To(Equal("3"),
					fmt.Sprintf("Deployment %s not ready: readyReplicas=%s", name, ready))
			}
		}).Should(Succeed())

		By("applying the TrialSet")
		Expect(applyManifest(trialSetManifest)).To(Succeed(), "Failed to apply TrialSet")

		By("waiting for the TrialSet to discover 2 Deployments and create 2 Trials")
		Eventually(func(g Gomega) {
			// Two Trials should be created, labeled with the TrialSet name.
			out := kubectlOrEmpty("get", "trials", "-n", trialSetTestNamespace,
				"-l", "temper.io/trial-set=payments-trialset",
				"-o", "jsonpath={.items[*].metadata.name}")
			names := nonEmpty(out)
			g.Expect(names).To(HaveLen(2),
				fmt.Sprintf("expected 2 generated Trials, got %d: %v", len(names), names))
		}).Should(Succeed())

		By("waiting for both Trials to reach Completed")
		Eventually(func(g Gomega) {
			out := kubectlOrEmpty("get", "trials", "-n", trialSetTestNamespace,
				"-l", "temper.io/trial-set=payments-trialset",
				"-o", "jsonpath={.items[*].status.phase}")
			phases := nonEmpty(out)
			g.Expect(phases).To(HaveLen(2), "expected 2 Trials")
			for _, p := range phases {
				g.Expect(p).To(Equal("Completed"),
					fmt.Sprintf("Trial not Completed: phases=%v", phases))
			}
		}).Should(Succeed())

		By("verifying TrialSet status reflects the batch outcome")
		Eventually(func(g Gomega) {
			discovered := kubectlOrEmpty("get", "trialset", "payments-trialset",
				"-n", trialSetTestNamespace,
				"-o", "jsonpath={.status.discoveredDeployments}")
			completed := kubectlOrEmpty("get", "trialset", "payments-trialset",
				"-n", trialSetTestNamespace,
				"-o", "jsonpath={.status.trialsCompleted}")
			phase := kubectlOrEmpty("get", "trialset", "payments-trialset",
				"-n", trialSetTestNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(discovered).To(Equal("2"),
				fmt.Sprintf("discoveredDeployments=%s, want 2", discovered))
			g.Expect(completed).To(Equal("2"),
				fmt.Sprintf("trialsCompleted=%s, want 2", completed))
			g.Expect(phase).To(Equal("Completed"),
				fmt.Sprintf("TrialSet phase=%s, want Completed", phase))
		}).Should(Succeed())
	})
})

// applyManifest applies the given YAML via `kubectl apply -f -` on stdin.
func applyManifest(yaml string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	_, err := utils.Run(cmd)
	return err
}

// kubectlOrEmpty runs kubectl with the given args and returns stdout (or "" on
// error). Used in Eventually loops where transient "not found" errors are
// acceptable while resources propagate.
func kubectlOrEmpty(args ...string) string {
	cmd := exec.Command("kubectl", args...)
	out, err := utils.Run(cmd)
	if err != nil {
		return ""
	}
	return out
}

// nonEmpty splits a space-separated string into a slice of non-empty tokens.
func nonEmpty(s string) []string {
	var res []string
	for _, t := range strings.Fields(s) {
		if t != "" {
			res = append(res, t)
		}
	}
	return res
}
