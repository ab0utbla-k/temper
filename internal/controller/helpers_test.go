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

	// failAfterCordonTarget marks a deployment whose scenario Inject cordons
	// cordonLeakNode and THEN fails — the post-mutation failure mode that
	// failTrial must clean up (the item-1 cordon-leak regression).
	failAfterCordonTarget = "dep-fail-after-cordon"
	cordonLeakNode        = "node-cordon-leak"

	// blockedEvictionTarget marks a deployment whose scenario Inject cordons
	// blockedEvictionNode and reports Incomplete forever — a PDB that never
	// yields. The controller must end it in Outcome=Blocked and uncordon.
	blockedEvictionTarget = "dep-blocked-eviction"
	blockedEvictionNode   = "node-blocked-eviction"

	// yieldingTarget marks a deployment whose scenario Inject reports
	// Incomplete twice and then succeeds — a PDB that yields mid-trial.
	yieldingTarget = "dep-yielding-eviction"
)

var (
	failInjectCalls     atomic.Int32
	yieldingInjectCalls atomic.Int32
)

// newTestScenario builds the real scenario, wrapped so Inject fails (and is
// counted) for failInjectTarget. All other targets behave normally.
func newTestScenario(c client.Client, spec temperv1alpha1.Scenario, owner string) (scenario.Scenario, error) {
	s, err := buildScenario(c, spec, owner)
	if err != nil {
		return nil, err
	}
	return &failInjectScenario{inner: s, c: c}, nil
}

type failInjectScenario struct {
	inner scenario.Scenario
	c     client.Client
}

func (f *failInjectScenario) Inject(ctx context.Context, target scenario.Target) (scenario.Result, error) {
	switch target.Name {
	case failInjectTarget:
		failInjectCalls.Add(1)
		return scenario.Result{}, errors.New("inject failed by test")
	case failAfterCordonTarget:
		// Mutate first, then fail — models an eviction error after the cordon.
		if err := f.cordonNode(ctx, cordonLeakNode); err != nil {
			return scenario.Result{}, err
		}
		return scenario.Result{}, errors.New("inject failed after cordon by test")
	case blockedEvictionTarget:
		if err := f.cordonNode(ctx, blockedEvictionNode); err != nil {
			return scenario.Result{}, err
		}
		return scenario.Result{
			Incomplete: true,
			Findings: []scenario.Finding{
				{Pod: "pod-blocked", Reason: "PodDisruptionBudget test-pdb allows 0 disruptions"},
			},
		}, nil
	case yieldingTarget:
		if yieldingInjectCalls.Add(1) <= 2 {
			return scenario.Result{Incomplete: true}, nil
		}
		return scenario.Result{PodsAffected: 1}, nil
	}
	return f.inner.Inject(ctx, target)
}

func (f *failInjectScenario) Revert(ctx context.Context, target scenario.Target) error {
	switch target.Name {
	case failAfterCordonTarget:
		return f.uncordonNode(ctx, cordonLeakNode)
	case blockedEvictionTarget:
		return f.uncordonNode(ctx, blockedEvictionNode)
	}
	return f.inner.Revert(ctx, target)
}

func (f *failInjectScenario) cordonNode(ctx context.Context, name string) error {
	var node corev1.Node
	if err := f.c.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
		return err
	}
	node.Spec.Unschedulable = true
	return f.c.Update(ctx, &node)
}

func (f *failInjectScenario) uncordonNode(ctx context.Context, name string) error {
	var node corev1.Node
	if err := f.c.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !node.Spec.Unschedulable {
		return nil
	}
	node.Spec.Unschedulable = false
	return f.c.Update(ctx, &node)
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

// patchDeploymentStatus reports readyReplicas in the Deployment status (envtest
// has no deployment controller to do it). stale=true marks the status as
// describing an older spec generation — the WorkloadReady probe must ignore
// numbers from a stale status.
func patchDeploymentStatus(ctx context.Context, name, namespace string, readyReplicas int32, stale bool) {
	var dep appsv1.Deployment
	Expect(k8sClient.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, &dep)).To(Succeed())

	dep.Status.ObservedGeneration = dep.Generation
	if stale {
		dep.Status.ObservedGeneration = dep.Generation - 1
	}
	// The API server rejects readyReplicas > replicas, so report all desired
	// pods as created and readyReplicas of them as ready.
	desired := int32(1)
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	dep.Status.Replicas = desired
	dep.Status.ReadyReplicas = readyReplicas

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

func createNodeDrainTrial(
	ctx context.Context,
	name, namespace, targetDeployment string,
	duration, evictionTimeout time.Duration,
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
					Type:     temperv1alpha1.ScenarioTypeNodeDrain,
					Duration: metav1.Duration{Duration: duration},
					NodeDrain: &temperv1alpha1.NodeDrainConfig{
						EvictionTimeout: &metav1.Duration{Duration: evictionTimeout},
					},
				},
			},
		},
	}

	Expect(k8sClient.Create(ctx, trial)).To(Succeed())

	return trial
}
