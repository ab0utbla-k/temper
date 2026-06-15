# Pod-Kill Scenario — What We Learn

## The Flow

1. A Deployment is running (e.g., `payment-service` with 3 replicas)
2. A `Trial` CR says "kill 1 pod every 5 minutes for 30 minutes"
3. Controller picks a random pod from the Deployment and deletes it
4. Kubernetes notices the Deployment is below desired replicas and schedules a replacement
5. Controller measures **recovery time** — how long until the Deployment is fully Available again
6. After 30 minutes, trial completes: "killed 6 pods, mean recovery time 8s"

## What We Can Learn (by phase)

### Phase 1 — Pod kill + recovery measurement

We measure:
- **Recovery time** — time until Deployment condition `Available=True` again
- **Pod kill count** — how many pods were deleted
- **Trial outcome** — completed or failed

This answers: "How fast does K8s replace a dead pod?" but NOT "did users notice?"

### Phase 2 — Add metrics and alert correlation

We add PromQL queries running during the trial. Now we can answer:
- **Does your app handle graceful shutdown?** — watch `rate(http_requests_total{status=~"5.."}[1m])` spike after a kill. If it spikes, in-flight requests are being dropped.
- **Do readiness probes work?** — if error rate spikes AFTER the new pod starts, traffic is routing to an unready pod
- **Does retry/circuit-breaker logic work?** — if client error rates stay flat during a kill, your resilience patterns are working
- **Alert correlation** — did a pod kill trigger a PagerDuty alert? If yes, your app didn't handle it gracefully

### Phase 5 — Adaptive baselines and regression detection

We add historical comparison:
- **Recovery regression** — "recovery used to take 4s, now it takes 12s" flags automatically
- **Anomaly detection** — compares SLO metrics during chaos against learned baselines, halts on real deviations

## Key Insight

The trial is just the stimulus. The metrics are where you learn. Most chaos tools stop at "inject fault, hope for the best." We build the measurement framework around it.
