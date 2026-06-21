package scenario

import (
	"context"
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrTargetNotInjectable = errors.New("target not injectable")

func runningTargetPods(ctx context.Context, c client.Client, target Target) ([]corev1.Pod, error) {
	var dep appsv1.Deployment
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: target.Namespace,
		Name:      target.Name,
	}, &dep); err != nil {
		return nil, fmt.Errorf("get deployment: %w", err)
	}

	selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parse selector: %w", err)
	}

	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace(target.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	var running []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			running = append(running, pod)
		}
	}

	return running, nil
}

func requireRunningPods(ctx context.Context, c client.Client, target Target) ([]corev1.Pod, error) {
	running, err := runningTargetPods(ctx, c, target)
	if err != nil {
		return nil, err
	}
	if len(running) == 0 {
		return nil, fmt.Errorf("%w: no running pods for deployment %s/%s", ErrTargetNotInjectable, target.Namespace, target.Name)
	}

	return running, nil
}
