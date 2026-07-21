# TrialSet design

A `TrialSet` is temper's sweep primitive: it discovers Deployments by label
selector and generates one owned `Trial` per match from a shared inline
template. This doc records the design decisions and their reasoning; the API
reference is the field comments in `api/v1alpha1/trialset_types.go`.

## Shape: one Trial per target

Everything temper produces is per-Deployment — verdict, MTTR, passport — so
the unit of execution must also be per-Deployment. A single Trial matching N
Deployments would produce one verdict for N workloads and no per-target
passport. The `TrialSet → Trial` relationship mirrors `CronJob → Job`:
the parent holds schedule and policy, each child holds one run's record.

## Same-namespace only (scope = blast radius)

A Trial's target always lives in the Trial's own namespace, and a TrialSet
discovers only in its own namespace.

The rule behind this: **an object's RBAC scope must match its blast radius.**
"May create Trials in namespace X" must stay equivalent to "may inject chaos
in namespace X". A namespaced object that can point at another namespace
breaks that boundary — any user who can create a Trial anywhere could then
fault-inject anywhere, because the manager is cluster-scoped.

Consequences:

- Multi-namespace sweeps = one TrialSet per namespace. A UI backend can fan
  out creates per namespace (impersonation + `SelfSubjectAccessReview` for
  the workload list) and aggregate status client-side.
- Fleet-wide sweeps come later as a cluster-scoped `ClusterTrialSet` whose
  creation requires cluster-level RBAC (the `Role`/`ClusterRole` pattern),
  plus a `temper.io/allow-trials` opt-in label on target namespaces
  (precedent: Chaos Mesh's `chaos-mesh.org/inject=enabled`).

## Batch identity

Generated Trials carry two labels: `temper.io/trial-set: <name>` (owner,
metrics attribution, watcher scope) and `temper.io/batch: <n>` (which run).
The batch number is `status.history.totalBatches`, incremented when a batch
fires.

Finished Trials stay in the cluster as records — nothing deletes them until
the TrialSet is deleted (owner-reference GC). The batch label is what makes
that safe: `reconcileRunning` lists Trials by both labels, so an earlier
batch's Trials can never satisfy the current batch's coverage. Without it, a
scheduled TrialSet would run real chaos only on its first fire.

Trial names are `<trialset>-b<batch>-<deployment>` — deterministic within a
batch, so a re-create under a stale informer cache collides with
AlreadyExists (treated as success) instead of injecting the same Deployment
twice.

## Phase machine and concurrency

The TrialSet mirrors the CronTrial phase machine:
Idle → Running → Completed/Halted/Failed, plus Paused (suspend). A batch only
ever fires from Idle, and a Running TrialSet never re-enters Idle until its
batch finishes — so overlapping batches cannot start. That makes
`concurrencyPolicy: Forbid` structural; the enum is restricted to `Forbid`
until Allow/Replace actually do something.

## Safeguards

- **Pre-flight, per Deployment:** before creating each Trial,
  `safeguard.CheckSafeguards` (shared with the CronTrial controller) checks
  replicas / alerts / SLO. An unsafe Deployment is skipped for the rest of
  the batch — recorded in `status.skippedDeployments`, `SafeguardSkipped`
  event fired once — and retried on the next batch. One unsafe Deployment
  never halts the whole batch (per-Deployment semantics).
- **Runtime:** the safeguard watcher lists Running Trials carrying the
  trial-set label (`client.HasLabels`), resolves the owning TrialSet's
  safeguards via the controller owner reference, and halts breaching Trials
  through the same annotation path used for CronTrial-generated Trials.

## Status-write discipline

`reconcileRunning` executes every few seconds while a batch is active, and a
status write triggers another reconcile of the same object. Status is
therefore written only when its content actually changed (slices sorted so
comparison cannot flap on ordering); `lastDiscoveryTime` records the last
discovery pass that changed the status.

## Follow-ups (tracked on PR #35)

- Trial history retention (`successfulTrialsHistoryLimit` /
  `failedTrialsHistoryLimit`); must fold MTTR into a rolling aggregate on the
  TrialSet before deleting Trials, so MTTR-regression tracking never depends
  on deleted objects or on Prometheus being installed.
- `ttlSecondsAfterFinished` for one-shot TrialSets.
- Recovery URL templating (the template's `recovery.http.url` is copied
  verbatim to every Trial — only sensible for single-match selectors today).
- `ClusterTrialSet` + namespace opt-in label.
