# temper

[![test](https://github.com/ab0utbla-k/temper/actions/workflows/test.yml/badge.svg)](https://github.com/ab0utbla-k/temper/actions/workflows/test.yml)
[![lint](https://github.com/ab0utbla-k/temper/actions/workflows/lint.yml/badge.svg)](https://github.com/ab0utbla-k/temper/actions/workflows/lint.yml)

temper is a Kubernetes operator that proves whether a workload survives real disruption. You describe a failure test in YAML. temper injects the fault, watches the recovery, and writes a verdict into the resource status.

> **Status: work in progress.** Do not use in production yet.

Many teams want resilience tests but do not want a large chaos platform with many CRDs, a UI, and a privileged node agent. temper stays small on purpose:

- three CRDs: `Trial` (one test), `CronTrial` (scheduled test), `TrialSet` (test many Deployments at once)
- three scenarios that answer real operational questions
- safeguards that stop a test when the system is unhealthy
- results in status, events, and Prometheus metrics
- no node agent, no webhook, no UI

## How it works

A `Trial` names a target Deployment and one or more scenarios. The controller:

1. scans the target for resilience risks and records them in `status.risks`
2. injects the fault
3. polls for recovery and records a timeline per scenario
4. reverts the injection and writes the verdict

The verdict lives in two separate fields, on purpose:

- `status.phase` says whether the controller ran the trial: `Pending`, `Running`, `Completed`, `Failed`, `Halted`.
- `status.outcome` says what the test concluded: `Passed`, `Blocked`, `Failed`, `Halted`.

A tool error is `phase: Failed` with no outcome. A test that ran and found a problem is `phase: Completed`, `outcome: Failed`. The full field reference is the comments in [`api/v1alpha1/trial_types.go`](api/v1alpha1/trial_types.go).

## Quick start

You need kubectl, Docker, and [kind](https://kind.sigs.k8s.io/).

1. Create a local cluster, build the manager image, and deploy temper into it:

   ```bash
   make kind
   ```

2. Run a first trial against one of your Deployments:

   ```bash
   cat <<EOF | kubectl apply -f -
   apiVersion: temper.io/v1alpha1
   kind: Trial
   metadata:
     name: first-trial
   spec:
     target:
       kind: Deployment
       name: <deployment-name>
     scenarios:
       - type: pod-kill
         duration: 30s
         podKill:
           count: 1
   EOF
   ```

3. Watch the verdict:

   ```bash
   kubectl get trials -w
   ```

   The trial ends with phase `Completed` and an outcome. `Passed` means the Deployment recovered within the scenario's duration.

4. Read the recovery timeline and any detected risks:

   ```bash
   kubectl get trial first-trial -o jsonpath='{.status.scenarioResults[0]}'
   kubectl get trial first-trial -o jsonpath='{.status.risks}'
   ```

More examples are in [`config/samples/`](config/samples/).

## Scenarios

| Scenario | What it does | PodDisruptionBudget |
| --- | --- | --- |
| `pod-kill` | Deletes pods of the target. Kubernetes replaces them. | Not consulted. A crash is involuntary. |
| `node-drain` | Cordons the node running the most target pods, then evicts them through the Eviction API. | Respected. A blocked eviction ends the trial with outcome `Blocked`. |
| `spot-reclaim` | Cordons the node running the most target pods, then deletes them directly, like a cloud provider reclaiming a spot node. | Bypassed. A real reclaim does not ask. |

`node-drain` answers "does my PDB let cluster maintenance proceed, and does the app survive it". `spot-reclaim` answers "can I run this workload on spot capacity". Node scenarios uncordon the node when the trial ends. Per-scenario options (`podKill.count`, `nodeDrain.evictionTimeout`, `nodeName` pinning) are documented in [`api/v1alpha1/trial_types.go`](api/v1alpha1/trial_types.go).

## Recovery probes

By default a scenario counts as recovered when all replicas of the target report Ready. Readiness can lie: a probe that returns 200 before the app can serve makes recovery look instant. Override the probe with an HTTP check to require real responses:

```yaml
spec:
  recovery:
    http:
      url: http://<service>.<namespace>.svc:<port>/
```

A 2xx response means recovered. Each scenario result records both moments:

```yaml
status:
  scenarioResults:
    - type: spot-reclaim
      injectedAt: "2026-07-23T10:00:00Z"
      readyAt: "2026-07-23T10:00:05Z"      # replicas reported Ready
      recoveredAt: "2026-07-23T10:00:45Z"  # the probe first succeeded
```

The gap between `readyAt` and `recoveredAt` is how long the workload claimed readiness without actually serving. A missing `recoveredAt` means the workload never recovered within the scenario's duration, and the trial ends `Failed`.

## Spot eligibility passport

A `Passed` trial that ran `spot-reclaim` stamps a passport into status:

```yaml
status:
  passport:
    eligible: true
    testedGeneration: 7
    expiresAt: "2026-08-22T10:00:45Z"
```

`testedGeneration` is the Deployment generation that was tested. A later generation means the workload changed, and the passport no longer describes it. The passport expires after 30 days: a passed run is evidence with a shelf life, not a permanent property. It is a machine-readable signal for platforms that decide which workloads can move to spot capacity.

## Risks

Before injecting, the controller scans the target and records weaknesses in `status.risks`:

- `SingleReplica` - any disruption takes the whole workload down
- `NoPodAntiAffinity` - nothing stops the scheduler from packing all replicas onto one node
- `ConcentratedPlacement` - all running pods sit on a single node right now
- `MissingReadinessProbe` - Kubernetes routes traffic to a container as soon as it starts
- `NoPodDisruptionBudget` - voluntary disruptions are unbounded

Risks are advisory: they never fail a trial. The same rules also run standalone, with no Trial and no disruption:

```bash
make riskscan RISKSCAN_NAMESPACE=<namespace>
```

For CI, `go run ./cmd/riskscan -namespace <namespace> -fail-on-risk` exits 1 when any risk is found.

## Scheduled runs: CronTrial

A `CronTrial` re-runs an existing Trial on a cron schedule:

```yaml
apiVersion: temper.io/v1alpha1
kind: CronTrial
metadata:
  name: payment-nightly
spec:
  trialRef: first-trial
  schedule: "0 2 * * 1-5"
  timezone: UTC
  safeguards:
    minReplicasAvailable: 2
```

## Sweeps: TrialSet

A `TrialSet` discovers Deployments by label selector in its own namespace and generates one owned Trial per match from a shared template. Use it to test a whole group of workloads with one object:

```yaml
apiVersion: temper.io/v1alpha1
kind: TrialSet
metadata:
  name: payments-sweep
spec:
  targetSelector:
    matchLabels:
      app.kubernetes.io/part-of: payments
  trialTemplate:
    scenarios:
      - type: pod-kill
        duration: 30s
        podKill:
          count: 1
  maxConcurrent: 1
  minReadyReplicas: 1
  safeguards:
    minReplicasAvailable: 2
```

Without `spec.schedule`, the batch runs once on creation; with a cron expression, it repeats. `maxConcurrent` limits how many Trials run at the same time. Deployments that fail the safeguard pre-flight are skipped for the batch and recorded in `status.skippedDeployments`. Discovery and targets stay in the TrialSet's own namespace, so RBAC on Trials bounds the blast radius; to sweep several namespaces, create one TrialSet per namespace. Design reasoning: [`docs/trialset-design.md`](docs/trialset-design.md).

## Safeguards

Safeguards protect the cluster from the test. They run at two points:

- **Pre-flight**, before a CronTrial or TrialSet starts a run. An unhealthy target skips this run; the next fire retries.
- **Runtime**, every 5 seconds while a trial runs. A breach halts the trial: the injection is reverted, the phase becomes `Halted`, and `status.haltReason` records why.

```yaml
safeguards:
  minReplicasAvailable: 2
  maxUnavailable: 1
  alertSource:
    type: alertmanager
    url: http://alertmanager.monitoring.svc:9093
  haltOnAlertLabels:
    severity: critical
  metricsSource:
    url: http://prometheus.monitoring.svc:9090
  sloProtection:
    mode: static
    threshold: "0.01"
    queries:
      - name: error-rate
        query: sum(rate(http_requests_total{code=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))
```

The checks are replica availability, firing Alertmanager alerts that match all `haltOnAlertLabels`, and PromQL queries compared against a static threshold. The metrics backend can be Prometheus, VictoriaMetrics, Thanos, Mimir, or Cortex. An error from a safeguard backend counts as unsafe: safeguards fail closed.

## Metrics

The controller manager serves Prometheus metrics (HTTPS with Kubernetes auth by default):

| Metric | Meaning |
| --- | --- |
| `temper_trials_total` | Trials by final status |
| `temper_trial_duration_seconds` | Wall-clock trial duration |
| `temper_scenarios_executed_total` | Scenarios injected, by type |
| `temper_recovery_time_seconds` | Time from injection to recovery |
| `temper_pods_killed_total` | Pods deleted by pod-kill |
| `temper_pods_evicted_total` | Pods evicted by node-drain |
| `temper_evictions_blocked_total` | Evictions a PDB refused |
| `temper_risks_detected_total` | Risks detected on targets, by rule |
| `temper_safeguard_checks_total` | Safeguard checks executed, by type |
| `temper_safeguard_halts_total` | Halts issued by the watcher, by code |
| `temper_trials_halted_total` | Trials that reached the Halted phase, by code |

## Development

```bash
make test    # unit and envtest suites
make lint    # golangci-lint, pinned version
make run     # run the controller from your host
```

After changing API types or kubebuilder markers:

```bash
make manifests generate
```

envtest has no kubelet and no disruption controller. Verify anything involving PDBs, evictions, or real scheduling in kind:

```bash
make kind        # build, load, and deploy into the local kind cluster
make test-e2e    # e2e suite against an isolated kind cluster
make kind-clean  # delete the local cluster
```

To deploy to another cluster, build and push an image, then:

```bash
make docker-build docker-push IMG=<registry>/temper:<tag>
make install deploy IMG=<registry>/temper:<tag>
```

`make build-installer IMG=<registry>/temper:<tag>` generates a single install YAML under `dist/`.

## Repository layout

```text
api/v1alpha1/         CRD types (Trial, CronTrial, TrialSet)
cmd/riskscan/         read-only risk scanner CLI
internal/controller/  Trial, CronTrial, and TrialSet controllers
internal/scenario/    fault injections (pod-kill, node-drain, spot-reclaim)
internal/safeguard/   safeguard checks and the runtime watcher
internal/risk/        static resilience analysis rules
internal/metrics/     Prometheus metrics
config/               CRDs, RBAC, manager manifests, samples
docs/                 design docs
test/e2e/             kind-based e2e tests
```

`api/v1alpha1/zz_generated.deepcopy.go`, `config/crd/bases/`, and `config/rbac/role.yaml` are generated. Run `make manifests generate` instead of editing them.

## License

[Apache License 2.0](LICENSE)
