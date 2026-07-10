package scenario

import (
	"context"
	"fmt"
	"math/rand/v2"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodKill deletes random pods from the target workload.
type PodKill struct {
	Client client.Client
	Count  int32
}

func (p *PodKill) Inject(ctx context.Context, target Target) (Result, error) {
	if target.Name == "" {
		return Result{}, fmt.Errorf("pod-kill scenario requires a named target (label selector not yet supported)")
	}

	running, err := requireRunningPods(ctx, p.Client, target)
	if err != nil {
		return Result{}, err
	}

	count := int(p.Count)
	if count <= 0 {
		return Result{}, fmt.Errorf("count must be at least 1, got %d", p.Count)
	}
	if count > len(running) {
		count = len(running)
	}

	rand.Shuffle(len(running), func(i, j int) {
		running[i], running[j] = running[j], running[i]
	})

	for _, pod := range running[:count] {
		if err := p.Client.Delete(ctx, &pod); err != nil {
			return Result{}, fmt.Errorf("delete pod %s: %w", pod.Name, err)
		}
	}

	return Result{PodsAffected: count}, nil
}

func (p *PodKill) Revert(_ context.Context, _ Target) error {
	// Pod-kill is self-reverting — Kubernetes replaces deleted pods automatically.
	return nil
}

func (p *PodKill) RecoveryProbe() RecoveryProbe {
	// Available=True tolerates partial disruption (it follows maxUnavailable),
	// so it would report instant recovery. Full strength is the honest signal.
	return RecoveryProbe{
		WorkloadReady: &WorkloadReadyProbe{},
	}
}
