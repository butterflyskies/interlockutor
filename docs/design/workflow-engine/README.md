# Receipt-bound workflow engine

Status: decision-ready draft

This design makes implementation and independent review explicit workflow states. A model may perform work or review; it does not supervise whether the required process happened.

## Decision requested

Approve this boundary and first implementation atom:

```text
Interlockutor  neutral append, delivery, claim, lease, fence
workflow-core durable aggregate state, attempts, artifacts, receipts, transitions
house policy   elbow-grease lenses, thresholds, reviewer rules, verification commands
Dione          thin submission and result adapter
```

The first atom is a separate `workflow-core` crate with a pure reducer and an in-memory expected-version repository. It proves that stale attempts and stale review receipts cannot advance a run. It does not launch constructs, persist to SQLite, schedule DAGs, or integrate with Dione.

## Problem

Behavioral instructions such as “review your work before presenting it” are not reliably applied by every model. Review must be an externally enforced state transition with durable evidence.

Interlockutor supplies useful event and lease mechanics. It does not supply aggregate state, artifact or instruction identity, review receipts, policy evaluation, or merge eligibility. Those belong in a workflow layer above it.

## Core invariant

A run may enter `Approved` only with at least one valid receipt and when its review policy accepts receipts bound to the exact current run, step, revision, artifact bytes, producer instructions, review instructions, policy, review attempt, and workflow-owned attempt epoch.

Changing any bound input invalidates the receipt. Queue acknowledgement is not evidence that a workflow transition is valid.

## Requirements

The workflow engine must:

1. Give run, step, revision, attempt, artifact, instruction, policy, and receipt distinct types.
2. Seal the exact instruction bytes delivered to executors and reviewers.
3. Store artifacts by digest and verify bytes on read.
4. Accept worker transitions only from the authenticated assigned principal for the current attempt, workflow epoch, unexpired workflow deadline, revision, and instruction digest.
5. Require atomic expected-version transitions on a workflow aggregate.
6. Record immutable execution and review receipts.
7. Evaluate review policy independently of queue delivery state.
8. Invalidate approvals when any bound input changes.
9. Make duplicate commands and receipts idempotent.
10. Bound retries and expose terminal failure, cancellation, and operator escalation.
11. Preserve correlation, causation, actor, and schema-version metadata.
12. Bind code artifacts to target ref, base commit/tree, result tree, and patch, then require merge execution to reproduce the approved result tree and atomically compare-and-swap the ref from the approved base commit.
13. Accept mutating commands only through a trusted adapter that supplies an authenticated principal; workers never append aggregate events directly.

## First-atom acceptance

```text
create run
-> seal instructions
-> dispatch attempt
-> submit artifact
-> request review
-> record receipt
-> approve or request revision
```

The reducer must reject each stale or mismatched binding independently. The first atom explicitly excludes construct launching, Dione integration, persistent storage, DAG scheduling, merge execution, model selection, and house-specific severity vocabulary.

See [architecture.md](architecture.md) and [threat-and-test-plan.md](threat-and-test-plan.md).
