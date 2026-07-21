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

package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// makeTrialSet builds a one-shot TrialSet (no schedule) whose targetSelector
// matches Deployments carrying app=<appLabel>, via a 5-minute pod-kill
// scenario (long enough that generated Trials stay Running through the test
// window). maxConcurrent, minReadyReplicas, suspend and safeguards are
// caller-tunable via mutate.
func makeTrialSet(name, namespace, appLabel string, mutate ...func(*temperv1alpha1.TrialSet)) *temperv1alpha1.TrialSet { //nolint:unparam // namespace is always "default" in the test suite
	one := int32(1)
	ts := &temperv1alpha1.TrialSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: temperv1alpha1.TrialSetSpec{
			TargetSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"app": appLabel},
			},
			TrialTemplate: temperv1alpha1.TrialTemplateSpec{
				Scenarios: []temperv1alpha1.Scenario{
					{
						Type:     temperv1alpha1.ScenarioTypePodKill,
						Duration: metav1.Duration{Duration: 5 * time.Minute},
					},
				},
			},
			MaxConcurrent:     1,
			MinReadyReplicas:  &one,
			ConcurrencyPolicy: temperv1alpha1.ConcurrencyPolicyForbid,
		},
	}
	for _, m := range mutate {
		m(ts)
	}
	return ts
}

// listOwnedTrials returns the Trials the TrialSet has generated (by label).
func listOwnedTrials(trialSetName, namespace string) []temperv1alpha1.Trial {
	var list temperv1alpha1.TrialList
	Expect(k8sClient.List(ctx, &list,
		client.InNamespace(namespace),
		client.MatchingLabels{temperv1alpha1.LabelTrialSet: trialSetName},
	)).To(Succeed())
	return list.Items
}

// backdateLastScheduleTime sets LastScheduleTime to 2 minutes ago so a
// scheduled TrialSet's next fire is immediately due. Retries on conflict
// because the controller may write status concurrently.
func backdateLastScheduleTime(key types.NamespacedName) {
	Eventually(func(g Gomega) {
		var got temperv1alpha1.TrialSet
		g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		got.Status.LastScheduleTime = &metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
		g.Expect(k8sClient.Status().Update(ctx, &got)).To(Succeed())
	}, timeout, interval).Should(Succeed())
}

// patchTrialPhase forces a Trial into a terminal phase so the TrialSet
// controller's reconcileRunning observes it as done. envtest has no real
// pod-kill semantics, so tests that need batches to complete drive the Trial
// to Completed/Failed/Halted directly. A Completed-phase Trial is a no-op for
// the Trial reconciler (trial_controller.go:146), so this is safe.
func patchTrialPhase(key types.NamespacedName, phase temperv1alpha1.TrialPhase) {
	patchTrialPhaseWithHalt(key, phase, nil)
}

// patchTrialPhaseWithHalt forces a Trial into a terminal phase, optionally
// setting Status.HaltReason (used to drive the TrialSet controller's
// trialsHalted/history.lastHaltReason wiring on the Halted path). envtest has
// no real safeguard backend, so tests that need a halted Trial drive it
// directly. A Halted-phase Trial is a no-op for the Trial reconciler
// (trial_controller.go:146), so this is safe.
func patchTrialPhaseWithHalt(key types.NamespacedName, phase temperv1alpha1.TrialPhase, haltReason *string) {
	Eventually(func(g Gomega) {
		var got temperv1alpha1.Trial
		g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		got.Status.Phase = phase
		got.Status.HaltReason = haltReason
		g.Expect(k8sClient.Status().Update(ctx, &got)).To(Succeed())
	}, timeout, interval).Should(Succeed())
}

var _ = Describe("TrialSet Controller", func() {
	Context("one-shot batch discovery and Trial generation", func() {
		It("should create one Trial per matching Deployment", func() {
			dep1 := createLabeledDeployment(ctx, "pay-one", "default", 3, "ts1")
			patchDeploymentStatus(ctx, dep1.Name, dep1.Namespace, 3, false)
			dep2 := createLabeledDeployment(ctx, "pay-two", "default", 3, "ts1")
			patchDeploymentStatus(ctx, dep2.Name, dep2.Namespace, 3, false)

			// maxConcurrent=2 so both Trials are created without waiting for
			// the first to complete (the 5-minute scenario duration would
			// otherwise serialize them under the default maxConcurrent=1).
			two := int32(2)
			trialSet := makeTrialSet("ts-one-per-match", "default", "ts1", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.MaxConcurrent = two
			})
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// The batch fires, discovers 2, and creates 2 owned Trials.
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(2))

				var ts temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trialSet), &ts)).To(Succeed())
				g.Expect(ts.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseRunning))
				g.Expect(ts.Status.DiscoveredDeployments).To(Equal(int32(2)))
				g.Expect(ts.Status.TrialsCreated).To(Equal(int32(2)))
			}, timeout, interval).Should(Succeed())

			// Each Trial carries the temper.io/trial-set label and targets
			// one of the discovered Deployments by name.
			trials := listOwnedTrials(trialSet.Name, trialSet.Namespace)
			Expect(trials).To(HaveLen(2))
			names := map[string]bool{}
			for _, t := range trials {
				Expect(t.Labels).To(HaveKeyWithValue(temperv1alpha1.LabelTrialSet, trialSet.Name))
				Expect(t.Spec.Target.Name).NotTo(BeNil())
				names[*t.Spec.Target.Name] = true
			}
			Expect(names).To(HaveKey("pay-one"))
			Expect(names).To(HaveKey("pay-two"))
		})

		It("should reach Completed with discoveredDeployments=0 when nothing matches", func() {
			trialSet := makeTrialSet("ts-no-matches", "default", "ts2")
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trialSet), &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseCompleted))
				g.Expect(got.Status.DiscoveredDeployments).To(Equal(int32(0)))
				g.Expect(got.Status.TrialsCreated).To(Equal(int32(0)))
				g.Expect(got.Status.History.SuccessfulBatches).To(Equal(int32(1)))
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(1)))
			}, timeout, interval).Should(Succeed())

			// One-shot with zero matches produces no Trials at all.
			Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).To(BeEmpty())
		})

		It("should copy the template recovery probe onto generated Trials", func() {
			ready := createLabeledDeployment(ctx, "pay-recovery", "default", 3, "ts13")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			probeURL := "http://pay-recovery.default.svc/healthz"
			trialSet := makeTrialSet("ts-recovery", "default", "ts13", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.TrialTemplate.Recovery = &temperv1alpha1.RecoverySpec{
					HTTP: &temperv1alpha1.HTTPRecoveryProbe{URL: probeURL},
				}
			})
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// The generated Trial carries spec.recovery from the template, so
			// the Trial controller probes real recovery over HTTP instead of
			// trusting the workload's readiness numbers.
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				g.Expect(got[0].Spec.Recovery).NotTo(BeNil())
				g.Expect(got[0].Spec.Recovery.HTTP).NotTo(BeNil())
				g.Expect(got[0].Spec.Recovery.HTTP.URL).To(Equal(probeURL))
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("maxConcurrent throttling", func() {
		It("should never exceed maxConcurrent running Trials", func() {
			// Three matching Deployments, maxConcurrent=2. readyReplicas
			// patched to 3 so they pass the default minReadyReplicas=1 filter;
			// the 5-minute scenario duration keeps the generated Trials
			// Running through the assertion window.
			d1 := createLabeledDeployment(ctx, "pay-mc-1", "default", 3, "ts3")
			patchDeploymentStatus(ctx, d1.Name, d1.Namespace, 3, false)
			d2 := createLabeledDeployment(ctx, "pay-mc-2", "default", 3, "ts3")
			patchDeploymentStatus(ctx, d2.Name, d2.Namespace, 3, false)
			d3 := createLabeledDeployment(ctx, "pay-mc-3", "default", 3, "ts3")
			patchDeploymentStatus(ctx, d3.Name, d3.Namespace, 3, false)
			_ = d3

			two := int32(2)
			trialSet := makeTrialSet("ts-max-concurrent", "default", "ts3", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.MaxConcurrent = two
			})
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Wait until two Trials exist (Pending/Running — active).
			Eventually(func(g Gomega) {
				g.Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).To(HaveLen(2))
			}, timeout, interval).Should(Succeed())

			// A third Trial must not appear while the first two are still
			// active (maxConcurrent=2). The trials never reach Completed
			// within this window because the 5-minute scenario duration has
			// not elapsed.
			Consistently(func(g Gomega) {
				g.Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).To(HaveLen(2))
			}, 4*time.Second, interval).Should(Succeed())
		})
	})

	Context("minReadyReplicas filter", func() {
		It("should skip Deployments below minReadyReplicas", func() {
			// Both carry app=ts4 so both are discovered; minReadyReplicas=1
			// then filters out the zero-ready one.
			ready := createLabeledDeployment(ctx, "pay-ready", "default", 3, "ts4")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			notReady := createLabeledDeployment(ctx, "pay-notready", "default", 3, "ts4")
			patchDeploymentStatus(ctx, notReady.Name, notReady.Namespace, 0, false)

			one := int32(1)
			trialSet := makeTrialSet("ts-min-ready", "default", "ts4", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.MinReadyReplicas = &one
			})
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Exactly one Trial — for the ready Deployment only.
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				g.Expect(*got[0].Spec.Target.Name).To(Equal("pay-ready"))
			}, timeout, interval).Should(Succeed())

			// And no Trial for the not-ready Deployment ever appears.
			Consistently(func(g Gomega) {
				for _, t := range listOwnedTrials(trialSet.Name, trialSet.Namespace) {
					if t.Spec.Target.Name != nil {
						g.Expect(*t.Spec.Target.Name).NotTo(Equal("pay-notready"))
					}
				}
			}, 3*time.Second, interval).Should(Succeed())
		})
	})

	Context("suspend", func() {
		It("should not fire a batch while suspended", func() {
			ready := createLabeledDeployment(ctx, "pay-suspend", "default", 3, "ts5")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			trialSet := makeTrialSet("ts-suspend", "default", "ts5", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.Suspend = true
			})
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Goes to Paused and creates no Trials.
			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trialSet), &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhasePaused))
			}, timeout, interval).Should(Succeed())

			Consistently(func(g Gomega) {
				g.Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).To(BeEmpty())
			}, 3*time.Second, interval).Should(Succeed())
		})
	})

	Context("concurrencyPolicy=Forbid", func() {
		It("should not start a second batch while one is running", func() {
			ready := createLabeledDeployment(ctx, "pay-forbid", "default", 3, "ts6")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			// A scheduled TrialSet (so a second fire could become due) with
			// a 1-minute cron. Forbid means: while Running, skip the next fire.
			oneMin := "* * * * *"
			trialSet := makeTrialSet("ts-forbid", "default", "ts6", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.Schedule = &oneMin
			})
			key := types.NamespacedName{Namespace: trialSet.Namespace, Name: trialSet.Name}
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Backdate LastScheduleTime so the first fire is immediately due
			// (avoids waiting up to a minute for the cron tick).
			backdateLastScheduleTime(key)

			// First batch fires: phase Running, one Trial, TotalBatches=1.
			Eventually(func(g Gomega) {
				g.Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).NotTo(BeEmpty())
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseRunning))
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(1)))
			}, timeout, interval).Should(Succeed())

			// Make the next fire "due" again. Because phase is Running, the
			// controller dispatches to reconcileRunning (not reconcileIdle),
			// so no new batch fires — Forbid is enforced structurally.
			backdateLastScheduleTime(key)

			Consistently(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseRunning))
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(1)))
			}, 3*time.Second, interval).Should(Succeed())
		})
	})

	Context("owner-reference garbage collection", func() {
		It("should stamp an owner reference enabling GC of generated Trials", func() {
			ready := createLabeledDeployment(ctx, "pay-gc", "default", 3, "ts7")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			trialSet := makeTrialSet("ts-gc", "default", "ts7")
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// envtest runs only kube-apiserver + etcd (no
			// kube-controller-manager), so the garbage collector that
			// performs cascading deletes is absent. We therefore verify the
			// precondition for GC: the generated Trial carries a controller
			// owner reference back to the TrialSet. In a real cluster this
			// owner reference is what reaps the Trial when the TrialSet is
			// deleted.
			var trialKey types.NamespacedName
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				trialKey = types.NamespacedName{Namespace: got[0].Namespace, Name: got[0].Name}
				g.Expect(got[0].OwnerReferences).NotTo(BeEmpty())
				owner := got[0].OwnerReferences[0]
				g.Expect(owner.Name).To(Equal(trialSet.Name))
				g.Expect(owner.Kind).To(Equal("TrialSet"))
				g.Expect(owner.Controller).NotTo(BeNil())
				g.Expect(*owner.Controller).To(BeTrue())
			}, timeout, interval).Should(Succeed())

			// Sanity: the Trial is owned and lives in the TrialSet's namespace.
			var trial temperv1alpha1.Trial
			Expect(k8sClient.Get(ctx, trialKey, &trial)).To(Succeed())
			Expect(trial.Namespace).To(Equal(trialSet.Namespace))
		})
	})

	Context("scheduled (cron) batch", func() {
		It("should fire a batch on schedule and generate a Trial", func() {
			ready := createLabeledDeployment(ctx, "pay-cron", "default", 3, "ts8")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			// A scheduled TrialSet (1-minute cron) with LastScheduleTime
			// backdated so the first fire is immediately due.
			oneMin := "* * * * *"
			trialSet := makeTrialSet("ts-cron", "default", "ts8", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.Schedule = &oneMin
			})
			key := types.NamespacedName{Namespace: trialSet.Namespace, Name: trialSet.Name}
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())
			backdateLastScheduleTime(key)

			// The scheduled path fires: phase Running, one Trial created.
			// This exercises the reconcileIdle schedule-parsing branch
			// (cron.NewParser + time.LoadLocation + Next), distinct from
			// the one-shot branch covered elsewhere.
			Eventually(func(g Gomega) {
				g.Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).To(HaveLen(1))
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseRunning))
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(1)))
				g.Expect(got.Status.LastScheduleTime).NotTo(BeNil())
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("batch identity across scheduled fires", func() {
		It("should generate a new Trial in the second batch instead of reusing the first batch's", func() {
			ready := createLabeledDeployment(ctx, "pay-batch2", "default", 3, "ts12")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			oneMin := "* * * * *"
			trialSet := makeTrialSet("ts-batch-identity", "default", "ts12", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.Schedule = &oneMin
			})
			key := types.NamespacedName{Namespace: trialSet.Namespace, Name: trialSet.Name}
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())
			backdateLastScheduleTime(key)

			// Batch 1 fires and creates one Trial.
			var firstTrialKey types.NamespacedName
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				firstTrialKey = types.NamespacedName{Namespace: got[0].Namespace, Name: got[0].Name}
			}, timeout, interval).Should(Succeed())

			// Finish batch 1: drive its Trial to Completed and wait for the
			// TrialSet to close the batch.
			patchTrialPhase(firstTrialKey, temperv1alpha1.TrialPhaseCompleted)
			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseCompleted))
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(1)))
			}, timeout, interval).Should(Succeed())

			// Make the second fire due. Batch 1's completed Trial still exists
			// in the cluster — batch 2 must create a NEW Trial for the same
			// Deployment rather than treating the old one as covering it.
			backdateLastScheduleTime(key)

			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(2)))
				g.Expect(listOwnedTrials(trialSet.Name, trialSet.Namespace)).To(HaveLen(2),
					"second batch must generate its own Trial; the first batch's Trial must not satisfy it")
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("per-match safeguard skip", func() {
		It("should skip an unsafe Deployment without halting the batch", func() {
			// Two matching Deployments. The second has availableReplicas
			// below minReplicasAvailable, so its per-match safeguard check
			// fails; the first passes and gets a Trial. The batch must NOT
			// transition to Halted — per-Deployment semantics mean one unsafe
			// match does not halt the whole batch.
			safe := createLabeledDeployment(ctx, "pay-safe", "default", 3, "ts9")
			patchDeploymentStatus(ctx, safe.Name, safe.Namespace, 3, false)
			// Safeguard checks read AvailableReplicas (not ReadyReplicas), so set
			// it explicitly on the safe Deployment to pass minReplicasAvailable=2.
			patchDeploymentAvailable(ctx, safe.Name, safe.Namespace, 3)

			unsafe := createLabeledDeployment(ctx, "pay-unsafe", "default", 3, "ts9")
			patchDeploymentStatus(ctx, unsafe.Name, unsafe.Namespace, 0, false)
			// Leave AvailableReplicas=0 so the safeguard trips for this match.

			two := int32(2)
			minAvail := int32(2)
			// minReadyReplicas=0 so both Deployments are discovered; the
			// per-match safeguard (minReplicasAvailable=2) then decides.
			zero := int32(0)
			trialSet := makeTrialSet("ts-safeguard-skip", "default", "ts9", func(ts *temperv1alpha1.TrialSet) {
				ts.Spec.MaxConcurrent = two
				ts.Spec.MinReadyReplicas = &zero
				ts.Spec.Safeguards = &temperv1alpha1.Safeguards{
					MinReplicasAvailable: &minAvail,
				}
			})
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Exactly one Trial — for the safe Deployment only.
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				g.Expect(*got[0].Spec.Target.Name).To(Equal("pay-safe"))
			}, timeout, interval).Should(Succeed())

			// No Trial for the unsafe Deployment ever appears.
			Consistently(func(g Gomega) {
				for _, t := range listOwnedTrials(trialSet.Name, trialSet.Namespace) {
					if t.Spec.Target.Name != nil {
						g.Expect(*t.Spec.Target.Name).NotTo(Equal("pay-unsafe"))
					}
				}
			}, 3*time.Second, interval).Should(Succeed())

			// The batch is Running (not Halted) — one unsafe match does not
			// halt the whole batch.
			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trialSet), &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseRunning))
				g.Expect(got.Status.History.HaltedBatches).To(Equal(int32(0)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("batch completion", func() {
		It("should transition to Completed when all generated Trials complete", func() {
			ready := createLabeledDeployment(ctx, "pay-complete", "default", 3, "ts10")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			trialSet := makeTrialSet("ts-complete", "default", "ts10")
			key := types.NamespacedName{Namespace: trialSet.Namespace, Name: trialSet.Name}
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Wait for the one Trial to be created and reach Running (the
			// Trial reconciler drives Pending -> Running on its own).
			var trialKey types.NamespacedName
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				trialKey = types.NamespacedName{Namespace: got[0].Namespace, Name: got[0].Name}
			}, timeout, interval).Should(Succeed())

			// Drive the Trial to Completed directly. envtest has no real
			// pod-kill, and the 5-minute scenario duration would force a long
			// wait; patching the phase exercises the TrialSet's completion gate
			// (allCovered && active == 0 -> completeBatch) deterministically.
			patchTrialPhase(trialKey, temperv1alpha1.TrialPhaseCompleted)

			// The TrialSet observes the Completed Trial and transitions to
			// Completed with trialsCompleted=1 and a successful batch in history.
			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.TrialSetPhaseCompleted))
				g.Expect(got.Status.TrialsCompleted).To(Equal(int32(1)))
				g.Expect(got.Status.TrialsFailed).To(Equal(int32(0)))
				g.Expect(got.Status.TrialsHalted).To(Equal(int32(0)))
				g.Expect(got.Status.History.SuccessfulBatches).To(Equal(int32(1)))
				g.Expect(got.Status.History.TotalBatches).To(Equal(int32(1)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("halt wiring", func() {
		It("should increment trialsHalted and set history.lastHaltReason when a generated Trial is halted", func() {
			ready := createLabeledDeployment(ctx, "pay-halt", "default", 3, "ts11")
			patchDeploymentStatus(ctx, ready.Name, ready.Namespace, 3, false)

			trialSet := makeTrialSet("ts-halt-wiring", "default", "ts11")
			key := types.NamespacedName{Namespace: trialSet.Namespace, Name: trialSet.Name}
			Expect(k8sClient.Create(ctx, trialSet)).To(Succeed())

			// Wait for the one Trial to be created.
			var trialKey types.NamespacedName
			Eventually(func(g Gomega) {
				got := listOwnedTrials(trialSet.Name, trialSet.Namespace)
				g.Expect(got).To(HaveLen(1))
				trialKey = types.NamespacedName{Namespace: got[0].Namespace, Name: got[0].Name}
			}, timeout, interval).Should(Succeed())

			// Simulate the safeguard watcher halting the Trial: patch phase to
			// Halted with a HaltReason (the watcher writes halt annotations, the
			// TrialReconciler transitions Pending -> Halted on the next reconcile,
			// then the TrialSet controller observes the Halted phase here). This
			// drives the cross-controller halt chain end-to-end without a real
			// Alertmanager backend.
			haltReason := "Critical alert: HighErrorRate"
			patchTrialPhaseWithHalt(trialKey, temperv1alpha1.TrialPhaseHalted, &haltReason)

			// The TrialSet controller observes the Halted Trial and increments
			// trialsHalted, sets history.lastHaltReason, and (because halted > 0)
			// records a HaltedBatch rather than a SuccessfulBatch.
			Eventually(func(g Gomega) {
				var got temperv1alpha1.TrialSet
				g.Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
				g.Expect(got.Status.TrialsHalted).To(Equal(int32(1)),
					"trialsHalted must reflect the halted generated Trial")
				g.Expect(got.Status.History.LastHaltReason).NotTo(BeNil())
				g.Expect(*got.Status.History.LastHaltReason).To(Equal(haltReason),
					"history.lastHaltReason must mirror the Trial's HaltReason")
				g.Expect(got.Status.History.HaltedBatches).To(Equal(int32(1)),
					"a batch with halted Trials counts as a HaltedBatch")
				g.Expect(got.Status.History.SuccessfulBatches).To(Equal(int32(0)),
					"a halted batch is not a successful batch")
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("metrics attribution", func() {
		It("sourceLabel returns the TrialSet name for a labeled Trial", func() {
			trial := &temperv1alpha1.Trial{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "trial-source",
					Namespace: "default",
					Labels:    map[string]string{temperv1alpha1.LabelTrialSet: "my-ts"},
				},
			}
			Expect(sourceLabel(trial)).To(Equal("my-ts"))
		})

		It("sourceLabel falls back to CronTrial then adhoc", func() {
			byCron := &temperv1alpha1.Trial{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					temperv1alpha1.LabelCronTrial: "cron-1",
					temperv1alpha1.LabelTrialSet:  "ts-1",
				}},
			}
			Expect(sourceLabel(byCron)).To(Equal("cron-1"))

			noLabels := &temperv1alpha1.Trial{
				ObjectMeta: metav1.ObjectMeta{Labels: nil},
			}
			Expect(sourceLabel(noLabels)).To(Equal("adhoc"))
		})
	})
})

// patchDeploymentAvailable sets AvailableReplicas on a Deployment's status.
// patchDeploymentStatus only sets Replicas and ReadyReplicas (the values
// the TrialReconciler's WorkloadReady probe reads); safeguard checks read
// AvailableReplicas, so tests exercising the per-match safeguard path must
// set it explicitly.
func patchDeploymentAvailable(ctx context.Context, name, namespace string, available int32) {
	Eventually(func(g Gomega) {
		var dep appsv1.Deployment
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{
			Namespace: namespace,
			Name:      name,
		}, &dep)).To(Succeed())
		dep.Status.AvailableReplicas = available
		g.Expect(k8sClient.Status().Update(ctx, &dep)).To(Succeed())
	}, timeout, interval).Should(Succeed())
}

// template labels all carry app=<appValue>, so it matches a targetSelector
// keyed on app. Used instead of createDeployment when the Deployment must be
// discoverable by a TrialSet's targetSelector.
func createLabeledDeployment(ctx context.Context, name, namespace string, replicas int, appValue string) *appsv1.Deployment { //nolint:unparam // namespace is always "default" in the test suite
	labels := map[string]string{"app": appValue}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(replicas)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "test", Image: "busybox"},
				}},
			},
		},
	}
	Expect(k8sClient.Create(ctx, dep)).To(Succeed())
	return dep
}
