# temper

temper is a Kubernetes operator for small, controlled failure tests.

It runs a `Trial` against a Deployment, watches how the workload behaves, and writes the result back to Kubernetes status and metrics. The project is meant to stay simple: CRDs, a controller, events, and Prometheus metrics. No UI is required.

## Status

Work in progress. Do not use temper in production yet.

Current state:

- `Trial` CRD for manual test runs
- `CronTrial` CRD for scheduled tests
- `TrialSet` CRD for discovering Deployments by label selector and generating one owned Trial per match
- `pod-kill` scenario
- basic `node-drain` scenario
- safeguard checks for replica availability, Alertmanager alerts, and static PromQL thresholds
- Prometheus metrics from the controller

## Why this exists

Many teams want to test resilience, but they do not want a large chaos platform with many CRDs, a UI, and a privileged node agent.

temper takes a smaller path:

- define the test in Kubernetes YAML
- run only a few useful scenarios
- stop tests when safeguards say the system is unhealthy
- record what happened in status, events, and metrics
- keep the system easy to read and review

## API overview

### Trial

A `Trial` runs one or more scenarios against a target Deployment.

```yaml
apiVersion: temper.io/v1alpha1
kind: Trial
metadata:
  name: payment-pod-kill
  namespace: demo
spec:
  target:
    kind: Deployment
    name: payment
  scenarios:
    - type: pod-kill
      duration: 30s
      podKill:
        count: 1
```

### CronTrial

A `CronTrial` creates Trial runs from a schedule. It can also define safeguards.

```yaml
apiVersion: temper.io/v1alpha1
kind: CronTrial
metadata:
  name: payment-nightly
  namespace: demo
spec:
  trialRef: payment-pod-kill
  schedule: "0 2 * * 1-5"
  timezone: UTC
  concurrencyPolicy: Forbid
  safeguards:
    minReplicasAvailable: 2
    maxUnavailable: 1
```

### TrialSet

A `TrialSet` discovers Deployments by label selector (optionally across
namespaces) and generates one owned Trial per match from a shared inline
template. Each generated Trial carries a `temper.io/trial-set` label (for
metrics attribution) and a `spec.target.namespace` pointing at the Deployment's
namespace, so a TrialSet in one namespace can target workloads in others.

`maxConcurrent` throttles how many Trials run at once within a batch;
`minReadyReplicas` filters out Deployments that are not ready at discovery
time; `suspend` pauses future batches; `concurrencyPolicy: Forbid` skips a
fire when a batch is already running. With no `schedule`, the batch is a
one-shot that fires once on creation; with a cron expression it repeats.

```yaml
apiVersion: temper.io/v1alpha1
kind: TrialSet
metadata:
  name: pod-kill-all-payments
spec:
  # targetSelector picks Deployments to generate Trials for by label.
  targetSelector:
    matchLabels:
      app.kubernetes.io/part-of: payments
  # namespaces scopes discovery across namespaces. Unset means the TrialSet's
  # own namespace only. Generated Trials carry spec.target.namespace pointing
  # at each matching Deployment's namespace (cross-namespace targeting).
  namespaces:
    names:
      - payments
      - payments-canary
  # trialTemplate is applied to each discovered Deployment; the controller
  # stamps target.name and target.namespace onto each generated Trial.
  trialTemplate:
    scenarios:
      - type: pod-kill
        duration: 30s
        podKill:
          count: 1
  # schedule omitted -> one-shot batch that fires once on creation.
  maxConcurrent: 1
  minReadyReplicas: 1
  safeguards:
    minReplicasAvailable: 2
    maxUnavailable: 1
```

The TrialSet controller, the safeguard watcher (which halts TrialSet-generated
Trials that breach a safeguard during a run), and the cross-namespace trust
boundary are documented in the design doc at
[.kimchi/docs/trialset-design.md](.kimchi/docs/trialset-design.md).

## Scenarios

### pod-kill

Deletes one or more pods owned by the target Deployment. Kubernetes creates replacement pods. temper measures recovery after the injection.

### node-drain

Cordons one node that hosts target pods and evicts those pods through the Kubernetes Eviction API. This respects PodDisruptionBudgets.

This scenario is experimental.

## Safeguards

Safeguards can stop a scheduled Trial before or during a run.

Supported checks:

- minimum available replicas
- maximum unavailable replicas
- firing Alertmanager alerts that match labels
- static PromQL threshold checks

When a safeguard halts a Trial, temper records a halt reason and emits metrics.

## Metrics

The controller exports Prometheus metrics such as:

- `temper_trials_total`
- `temper_scenarios_executed_total`
- `temper_pods_killed_total`
- `temper_recovery_time_seconds`
- `temper_trials_halted_total`
- `temper_pods_evicted_total`
- `temper_evictions_blocked_total`

Metrics are served by the controller manager. The default Kubebuilder metrics setup uses HTTPS and Kubernetes auth.

## Development

Requirements:

- Go
- kubectl
- kind, for end-to-end tests and node-drain testing
- Docker or another container tool, for building the manager image

Common commands:

```bash
make test
make lint
make run
```

Regenerate Kubernetes manifests and generated Go code after changing API types or kubebuilder markers:

```bash
make manifests generate
```

Run e2e tests against an isolated kind cluster:

```bash
make test-e2e
```

Build and deploy the controller image:

```bash
export IMG=<registry>/temper:<tag>
make docker-build docker-push IMG=$IMG
make deploy IMG=$IMG
```

Build one install YAML:

```bash
make build-installer IMG=<registry>/temper:<tag>
```

## Repository layout

```text
api/v1alpha1/              CRD types (Trial, CronTrial, TrialSet)
internal/controller/       Trial, CronTrial, and TrialSet controllers
internal/scenario/         scenario implementations
internal/safeguard/        safeguard checks and watcher
internal/metrics/          Prometheus metrics
config/                    CRDs, RBAC, manager manifests, samples
config/samples/v1alpha1_trialset.yaml   TrialSet sample CR
test/e2e/                  kind-based e2e tests
```

Generated files are not edited by hand:

- `api/v1alpha1/zz_generated.deepcopy.go`
- `config/crd/bases/*.yaml`
- `config/rbac/role.yaml`

Use `make generate` and `make manifests` instead.

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0. See the license text in this repository or at <https://www.apache.org/licenses/LICENSE-2.0>.
