package scenario

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SpotReclaim simulates a cloud spot (preemptible) node being reclaimed.
// Unlike node-drain (a voluntary, PDB-gated eviction), a spot reclaim is
// involuntary: it deletes the target pods directly, so a PodDisruptionBudget
// does not protect them.
type SpotReclaim struct {
	Client client.Client
	Owner  string
	// NodeName pins the reclaim to this node. Empty means "pick the node
	// running the most target pods."
	NodeName string
}

func (s *SpotReclaim) Inject(ctx context.Context, target Target) (Result, error) {
	if target.Name == "" {
		return Result{}, fmt.Errorf("spot-reclaim scenario requires a named target (label selector not yet supported)")
	}

	running, err := requireRunningPods(ctx, s.Client, target)
	if err != nil {
		return Result{}, err
	}

	reclaimNode := candidateNode(running, s.NodeName)
	if err := cordonNode(ctx, s.Client, reclaimNode, s.Owner); err != nil {
		return Result{}, err
	}

	running, err = runningTargetPods(ctx, s.Client, target)
	if err != nil {
		return Result{}, err
	}

	reclaimed := 0
	for _, pod := range running {
		if pod.Spec.NodeName != reclaimNode {
			continue
		}

		// Delete, not evict: a spot reclaim is involuntary and bypasses any
		// PodDisruptionBudget.
		if err := s.Client.Delete(ctx, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue // already gone
			}
			return Result{}, fmt.Errorf("delete pod %s: %w", pod.Name, err)
		}
		reclaimed++
	}

	return Result{PodsAffected: reclaimed}, nil
}

func (s *SpotReclaim) Revert(ctx context.Context, _ Target) error {
	return uncordonOwnedNodes(ctx, s.Client, s.Owner)
}

func (s *SpotReclaim) RecoveryProbe() RecoveryProbe {
	// Default to full-strength readiness; a Trial can override with an HTTP
	// probe to prove the service actually serves traffic again.
	return RecoveryProbe{
		WorkloadReady: &WorkloadReadyProbe{},
	}
}
