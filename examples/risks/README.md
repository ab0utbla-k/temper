# Risk detection examples

Each manifest deploys a target workload plus a Trial so the controller
populates `status.risks` with a specific risk token. Workloads are hardened
against every other rule, so each example isolates one risk (except the
worst-case and StatefulSet ones, which fire several at once).

| File | Risk demonstrated |
| --- | --- |
| `single_replica.yaml` | `SingleReplica` |
| `no_pod_anti_affinity.yaml` | `NoPodAntiAffinity` |
| `missing_health_probes.yaml` | `MissingHealthProbes` |
| `no_pod_disruption_budget.yaml` | `NoPodDisruptionBudget` |
| `concentrated_placement.yaml` | `ConcentratedPlacement` (needs 2+ nodes for mitigation) |
| `worst_case.yaml` | Four risks at once on a bare deployment |
| `statefulset_target.yaml` | Detection on a StatefulSet target (detect-only) |

## Usage

```bash
# Apply an example
kubectl apply -f examples/risks/single_replica.yaml

# Watch the detected risks
kubectl get trial risk-single-replica -o jsonpath='{.status.risks}' | jq

# Mitigate the condition mid-run and watch the risk disappear, e.g.:
kubectl scale deployment risk-worst-case --replicas=3   # clears SingleReplica
```

Risks are re-evaluated on every reconcile while the Trial is Pending or
Running: mitigated risks are removed from `status.risks` and newly introduced
conditions are added. Once the Trial completes, the recorded risks freeze.
