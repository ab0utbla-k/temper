package scenario

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// candidateNode picks the node to disrupt. A non-empty pinned name wins;
// otherwise it returns the node running the most of the given pods.
func candidateNode(pods []corev1.Pod, pinned string) string {
	if pinned != "" {
		return pinned
	}

	byNode := make(map[string]int)
	for _, pod := range pods {
		byNode[pod.Spec.NodeName]++
	}

	var busiest string
	most := 0
	for node, count := range byNode {
		if count > most {
			busiest, most = node, count
		}
	}
	return busiest
}

// cordonNode marks the node unschedulable and records the owning trial in the
// temper.io/cordoned-by annotation, so a later Revert un-cordons only what it
// cordoned. It retries on write conflict.
func cordonNode(ctx context.Context, c client.Client, nodeName, owner string) error {
	var node corev1.Node
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
			return fmt.Errorf("get node: %w", err)
		}

		node.Spec.Unschedulable = true
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		node.Annotations[temperv1alpha1.AnnotationCordonedBy] = owner

		return c.Update(ctx, &node)
	}); err != nil {
		return fmt.Errorf("cordon node %s: %w", nodeName, err)
	}
	return nil
}

// uncordonOwnedNodes un-cordons every node this owner cordoned, clearing the
// temper.io/cordoned-by annotation. It is idempotent: nodes not tagged by this
// owner are left untouched.
func uncordonOwnedNodes(ctx context.Context, c client.Client, owner string) error {
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	for _, node := range nodes.Items {
		if node.Annotations[temperv1alpha1.AnnotationCordonedBy] != owner {
			continue
		}
		node.Spec.Unschedulable = false
		delete(node.Annotations, temperv1alpha1.AnnotationCordonedBy)
		if err := c.Update(ctx, &node); err != nil {
			return fmt.Errorf("uncordon node %s: %w", node.Name, err)
		}
	}
	return nil
}
