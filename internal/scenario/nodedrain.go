package scenario

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	temperv1alpha1 "github.com/ab0utbla-k/temper/api/v1alpha1"
)

// NodeDrain evicts a node's target pods via the Eviction API, simulating node loss.
type NodeDrain struct {
	Client client.Client
	Owner  string
	// NodeName pins the drain to this node. Empty means "pick the node
	// running the most target pods."
	NodeName string
}

func (n *NodeDrain) Inject(ctx context.Context, target Target) (Result, error) {
	if target.Name == "" {
		return Result{}, fmt.Errorf("node-drain scenario requires a named target (label selector not yet supported)")
	}

	running, err := requireRunningPods(ctx, n.Client, target)
	if err != nil {
		return Result{}, err
	}

	drainNode := n.NodeName
	if drainNode == "" {
		byNode := make(map[string]int)
		for _, pod := range running {
			byNode[pod.Spec.NodeName]++
		}

		var busiest string
		mostPods := 0
		for node, count := range byNode {
			if count > mostPods {
				busiest, mostPods = node, count
			}
		}
		drainNode = busiest
	}

	var node corev1.Node
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := n.Client.Get(ctx, client.ObjectKey{Name: drainNode}, &node); err != nil {
			return fmt.Errorf("get node: %w", err)
		}

		node.Spec.Unschedulable = true
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		node.Annotations[temperv1alpha1.AnnotationCordonedBy] = n.Owner

		return n.Client.Update(ctx, &node)
	}); err != nil {
		return Result{}, fmt.Errorf("cordon node %s: %w", drainNode, err)
	}

	running, err = runningTargetPods(ctx, n.Client, target)
	if err != nil {
		return Result{}, err
	}

	evicted := 0
	var findings []Finding

	for _, pod := range running {
		if pod.Spec.NodeName != drainNode {
			continue
		}

		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: pod.Namespace,
				Name:      pod.Name,
			},
		}

		err := n.Client.SubResource("eviction").Create(ctx, &pod, eviction)

		switch {
		case err == nil:
			evicted++
		case apierrors.IsTooManyRequests(err):
			findings = append(findings, Finding{Pod: pod.Name, Reason: "blocked by PodDisruptionBudget"})
		case apierrors.IsNotFound(err):
			// already gone — nothing to do
		default:
			return Result{}, fmt.Errorf("evict pod %s: %w", pod.Name, err)
		}
	}

	return Result{PodsAffected: evicted, Findings: findings}, nil
}

func (n *NodeDrain) Revert(ctx context.Context, _ Target) error {
	var nodes corev1.NodeList
	if err := n.Client.List(ctx, &nodes); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	for _, node := range nodes.Items {
		if node.Annotations[temperv1alpha1.AnnotationCordonedBy] != n.Owner {
			continue
		}
		node.Spec.Unschedulable = false
		delete(node.Annotations, temperv1alpha1.AnnotationCordonedBy)
		if err := n.Client.Update(ctx, &node); err != nil {
			return fmt.Errorf("uncordon node %s: %w", node.Name, err)
		}
	}
	return nil
}

func (n *NodeDrain) RecoveryProbe() RecoveryProbe {
	// Same rationale as pod-kill: evicted pods must be rescheduled and ready
	// again everywhere, not merely "still above the availability floor".
	return RecoveryProbe{
		WorkloadReady: &WorkloadReadyProbe{},
	}
}
