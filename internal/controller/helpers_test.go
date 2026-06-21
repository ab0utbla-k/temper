package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
	"github.com/ab0utbla-k/temper/internal/scenario"
)

const (
	timeout  = 10 * time.Second
	interval = 250 * time.Millisecond

	// failInjectTarget marks a deployment whose scenario Inject always fails
	// (see newTestScenario, installed suite-wide in BeforeSuite).
	failInjectTarget = "dep-fail-inject"
)

var failInjectCalls atomic.Int32

// newTestScenario builds the real scenario, wrapped so Inject fails (and is
// counted) for failInjectTarget. All other targets behave normally.
func newTestScenario(c client.Client, spec temperv1alpha1.Scenario, owner string) (scenario.Scenario, error) {
	s, err := buildScenario(c, spec, owner)
	if err != nil {
		return nil, err
	}
	return &failInjectScenario{inner: s}, nil
}

type failInjectScenario struct {
	inner scenario.Scenario
}

func (f *failInjectScenario) Inject(ctx context.Context, target scenario.Target) (scenario.Result, error) {
	if target.Name == failInjectTarget {
		failInjectCalls.Add(1)
		return scenario.Result{}, errors.New("inject failed by test")
	}
	return f.inner.Inject(ctx, target)
}

func (f *failInjectScenario) Revert(ctx context.Context, target scenario.Target) error {
	return f.inner.Revert(ctx, target)
}

func (f *failInjectScenario) RecoveryProbe() scenario.RecoveryProbe {
	return f.inner.RecoveryProbe()
}

func createDeployment(ctx context.Context, name, namespace string, replicas int) *appsv1.Deployment { //nolint:unparam // may vary
	labels := map[string]string{"app": name}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(replicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "busybox"},
					},
				},
			},
		},
	}

	Expect(k8sClient.Create(ctx, dep)).To(Succeed())

	return dep
}

func createRunningPods(ctx context.Context, dep *appsv1.Deployment) {
	for i := range *dep.Spec.Replicas {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", dep.Name, i),
				Namespace: dep.Namespace,
				Labels:    dep.Spec.Template.Labels,
			},
			Spec: dep.Spec.Template.Spec,
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		pod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
}

// createRunningPodsOnNode creates count running pods for dep, all assigned to
// nodeName. startIdx offsets the pod names so repeated calls don't collide.
func createRunningPodsOnNode(ctx context.Context, dep *appsv1.Deployment, nodeName string, count, startIdx int) {
	for i := range count {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", dep.Name, startIdx+i),
				Namespace: dep.Namespace,
				Labels:    dep.Spec.Template.Labels,
			},
			Spec: dep.Spec.Template.Spec,
		}
		pod.Spec.NodeName = nodeName
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		pod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
}

// createNode creates a bare Node object (envtest has no kubelet to register one).
func createNode(ctx context.Context, name string) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	Expect(k8sClient.Create(ctx, node)).To(Succeed())
}

func patchDeploymentAvailable(ctx context.Context, name, namespace string) {
	var dep appsv1.Deployment
	Expect(k8sClient.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, &dep)).To(Succeed())

	dep.Status.Conditions = append(dep.Status.Conditions, appsv1.DeploymentCondition{
		Type:   appsv1.DeploymentAvailable,
		Status: corev1.ConditionTrue,
	})

	Expect(k8sClient.Status().Update(ctx, &dep)).To(Succeed())
}

func setHaltAnnotation(ctx context.Context, key client.ObjectKey, reason string, code temperv1alpha1.HaltCode) {
	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var got temperv1alpha1.Trial
		if err := k8sClient.Get(ctx, key, &got); err != nil {
			return err
		}
		if got.Annotations == nil {
			got.Annotations = map[string]string{}
		}
		got.Annotations[temperv1alpha1.AnnotationHaltReason] = reason
		got.Annotations[temperv1alpha1.AnnotationHaltCode] = string(code)
		return k8sClient.Update(ctx, &got)
	})).To(Succeed())
}

func createTrial(
	ctx context.Context,
	name, namespace, targetDeployment string, //nolint:unparam // may vary
	duration time.Duration,
) *temperv1alpha1.Trial {
	trial := &temperv1alpha1.Trial{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: temperv1alpha1.TrialSpec{
			Target: temperv1alpha1.Target{
				Kind: "Deployment",
				Name: new(targetDeployment),
			},
			Scenarios: []temperv1alpha1.Scenario{
				{
					Type:     temperv1alpha1.ScenarioTypePodKill,
					Duration: metav1.Duration{Duration: duration},
				},
			},
		},
	}

	Expect(k8sClient.Create(ctx, trial)).To(Succeed())

	return trial
}
