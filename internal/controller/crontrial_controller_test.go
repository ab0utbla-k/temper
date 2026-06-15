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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

var _ = Describe("CronTrial Controller", func() {
	It("should create trial on schedule", func() {
		dep := createDeployment(ctx, "dep-crontrial", "default", 3)
		createRunningPods(ctx, dep)
		trial := createTrial(ctx, "exp-template-crontrial", "default", dep.Name, 30*time.Second)
		cronTrial := &temperv1alpha1.CronTrial{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sched-create",
				Namespace: "default",
			},
			Spec: temperv1alpha1.CronTrialSpec{
				TrialRef: trial.Name,
				Schedule: "* * * * *",
			},
		}
		Expect(k8sClient.Create(ctx, cronTrial)).To(Succeed())
		cronTrial.Status.LastScheduleTime = &metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
		Expect(k8sClient.Status().Update(ctx, cronTrial)).To(Succeed())

		Eventually(func(g Gomega) {
			var got temperv1alpha1.CronTrial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cronTrial), &got)).To(Succeed())
			g.Expect(got.Status.Phase).To(Equal(temperv1alpha1.CronTrialPhaseRunning))
			g.Expect(got.Status.ActiveTrialName).NotTo(BeNil())
		}, timeout, interval).Should(Succeed())
	})

	It("should label created trial with schedule name", func() {
		dep := createDeployment(ctx, "dep-label", "default", 3)
		createRunningPods(ctx, dep)
		template := createTrial(ctx, "exp-template-label", "default", dep.Name, 30*time.Second)
		cronTrial := &temperv1alpha1.CronTrial{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sched-label",
				Namespace: "default",
			},
			Spec: temperv1alpha1.CronTrialSpec{
				TrialRef: template.Name,
				Schedule: "* * * * *",
			},
		}
		Expect(k8sClient.Create(ctx, cronTrial)).To(Succeed())
		cronTrial.Status.LastScheduleTime = &metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
		Expect(k8sClient.Status().Update(ctx, cronTrial)).To(Succeed())

		Eventually(func(g Gomega) {
			var got temperv1alpha1.CronTrial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cronTrial), &got)).To(Succeed())
			g.Expect(got.Status.ActiveTrialName).NotTo(BeNil())

			var created temperv1alpha1.Trial
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: cronTrial.Namespace,
				Name:      *got.Status.ActiveTrialName,
			}, &created)).To(Succeed())
			g.Expect(created.Labels).To(HaveKeyWithValue(temperv1alpha1.LabelCronTrial, cronTrial.Name))
		}, timeout, interval).Should(Succeed())
	})

	It("should block when safeguards fail", func() {
		dep := createDeployment(ctx, "dep-safeguard", "default", 3)
		trial := createTrial(ctx, "exp-template-safeguard", "default", dep.Name, 30*time.Second)
		cronTrial := &temperv1alpha1.CronTrial{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sched-safeguard",
				Namespace: "default",
			},
			Spec: temperv1alpha1.CronTrialSpec{
				TrialRef: trial.Name,
				Schedule: "* * * * *",
				Safeguards: &temperv1alpha1.Safeguards{
					MinReplicasAvailable: new(int32(2)),
				},
			},
		}
		Expect(k8sClient.Create(ctx, cronTrial)).To(Succeed())
		cronTrial.Status.LastScheduleTime = &metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
		Expect(k8sClient.Status().Update(ctx, cronTrial)).To(Succeed())

		Consistently(func(g Gomega) {
			var got temperv1alpha1.CronTrial
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cronTrial), &got)).To(Succeed())
			g.Expect(got.Status.ActiveTrialName).To(BeNil())
		}, timeout, interval).Should(Succeed())
	})
})
