# Architecture

## Boundary

Interlockutor remains a neutral delivery substrate. It may carry workflow commands and events, but it does not decide whether workflow state advances.

```text
caller / Dione
      |
      v
workflow coordinator ---> aggregate store (source of truth)
      |                         |
      v                         v
Interlockutor             artifact store
 claim / lease / fence     content-addressed bytes
      |
      v
executor or reviewer
```

The coordinator commits an aggregate transition and outbox record before acknowledging delivered work. Redelivery reconciles against durable aggregate state.

## State model

```text
Created
  -> Dispatchable(revision, instructions)
  -> Running(revision, attempt, epoch, deadline, instructions)
  -> ReviewRequired(revision, artifact, instructions, policy, receipts)
  -> Reviewing(revision, artifact, review attempts, policy, receipts)
  -> Approved(revision, artifact, receipts)
  -> Completed(output)

Running
  -> Dispatchable     retryable failure or expired attempt
  -> Failed           permanent or exhausted failure

ReviewRequired
  -> Reviewing        dispatch one or more fenced review attempts

Reviewing
  -> ReviewRequired   valid receipt, quorum incomplete
  -> RevisionRequired findings require changes
  -> Approved         policy satisfied
  -> ReviewRequired   expired or retryable review attempt

Approved
  -> Completed        verified publication succeeds
  -> RevisionRequired merge compare-and-swap loses; mint new revision

RevisionRequired
  -> Dispatchable     new revision and newly sealed instructions
```

`Failed`, `Cancelled`, and `Completed` are terminal. `Reviewing` retains every active review attempt ID, reviewer principal, workflow epoch, and deadline that may submit a receipt. A new revision monotonically supersedes every implementation attempt, review attempt, and receipt from the preceding revision.

## Bound data

Use newtypes for `WorkflowId`, `RunId`, `StepId`, `Revision`, `AttemptId`, `ReceiptId`, and authenticated `PrincipalId`.

```rust
struct ArtifactRef {
    digest: Digest,
    media_type: String,
    byte_len: u64,
    storage_key: String,
    canonicalization: CanonicalizationId,
}

struct CodeArtifactRef {
    target_ref: String,
    base_commit_oid: String,
    base_tree_digest: Digest,
    result_tree_digest: Digest,
    patch: ArtifactRef,
    subject_digest: Digest,
}

struct InstructionRef {
    digest: Digest, // exact rendered bytes delivered
    template_digest: Digest,
    referenced_artifacts_digest: Digest,
    byte_len: u64,
}

struct ReviewAttemptRef {
    attempt_id: AttemptId,
    reviewer_id: PrincipalId,
    epoch: AttemptEpoch,
    deadline: MonotonicDeadline,
    review_instruction_digest: Digest,
    status: ReviewAttemptStatus,
}

struct ReviewReceipt {
    receipt_id: ReceiptId,
    run_id: RunId,
    step_id: StepId,
    revision: Revision,
    reviewer_id: PrincipalId,
    reviewer_role: ReviewerRole,
    subject_artifact_digest: Digest,
    producer_instruction_digest: Digest,
    review_instruction_digest: Digest,
    policy_digest: Digest,
    review_attempt_id: AttemptId,
    review_epoch: AttemptEpoch,
    verdict: ReviewVerdict,
    findings_digest: Digest,
    receipt_digest: Digest,
}
```

Artifacts hash exact stored bytes. Structured envelopes use a named canonical format and domain-separated SHA-256. Incidental serializer field order is not a contract. Revision and attempt binding prevent identical bytes from creating an ABA approval path.

A code artifact's `subject_digest` is the domain-separated canonical digest over `target_ref`, `base_commit_oid`, `base_tree_digest`, `result_tree_digest`, and `patch.digest`. `ReviewReceipt.subject_artifact_digest` must equal that enclosing digest; binding only the patch or any proper subset is non-conforming. A rebase, amend, or target change that alters any component creates a new artifact and invalidates prior review, even when the visible diff happens to be identical.

## Transition protocol

Every command supplies an expected aggregate version, command idempotency key, authenticated actor, correlation and causation IDs, and schema version. Worker results also supply current attempt ID, workflow epoch, revision, and instruction digest. The reducer checks its server-owned clock against the attempt deadline; expiry rejects a result even when no redispatch has occurred. Review receipts are accepted only for an active review attempt recorded in `Reviewing`, and only when the authenticated command actor equals the attempt's recorded reviewer principal.

The aggregate store atomically compares the expected version and appends resulting events. Exactly one of two concurrent writers may succeed.

`DispatchReview` mints the next workflow-owned `AttemptEpoch` and records a `ReviewAttemptRef` before its outbox entry becomes deliverable. `RecordReviewReceipt` rejects any receipt whose authenticated actor, attempt, reviewer, epoch, unexpired deadline, or review-instruction digest does not match a recorded active attempt, then atomically changes that attempt to `ReceiptRecorded(receipt_id)`.

`Approve` first enforces a reducer-level floor of at least one receipt with a matching `ReceiptRecorded` attempt. Policy may add stricter quorum or independence requirements but cannot remove that floor. It then re-evaluates the selected receipt set against the current aggregate version, revision, artifact digest, producer-instruction digest, review-instruction digests, policy digest, and matching attempt records in the same atomic transition that emits `StepApproved`. A receipt being valid when recorded does not make it permanently valid. No separate projection or earlier policy decision may authorize this transition.

Workflow epochs and deadlines are distinct from Interlockutor delivery leases. The workflow aggregate mints epochs and owns attempt validity. Interlockutor mints `Fence` values when work is claimed and uses them only for delivery renewal, ACK, and NACK. An adapter may record delivery metadata for diagnosis, but an Interlockutor fence never authorizes a workflow result.

The aggregate checks its server-owned clock on every heartbeat, result, and receipt command. A sweeper may materialize `Expired` status for operations, but correctness never depends on the sweeper running: a deadline in the past is invalid even when stored status still says active.

## Authority and sealing

Workers and reviewers cannot append aggregate events. They submit signed or session-authenticated command envelopes to a trusted adapter; the adapter verifies identity and constructs an `AuthenticatedPrincipal`, and only the coordinator has expected-version write authority. The pure first atom uses a fake verifier in tests. Choosing the production authentication mechanism is deferred, but an adapter that accepts caller-supplied principal IDs is non-conforming. Projections are read-only and carry no transition authority.

The review policy selects an eligible reviewer set; the coordinator assigns `reviewer_id` from that set when it records `DispatchReview`. House policy owns independence constraints, while the generic reducer requires the assignment to exist and binds receipt admission to it.

Producer instructions, review instructions, and policy bytes are sealed for a review round. Changing any of them cancels active review attempts, invalidates recorded receipts, increments the review round, and returns the aggregate to `ReviewRequired`. Reusing an earlier digest does not revive an earlier attempt or receipt.

## Policy boundary

```rust
trait ReviewPolicy {
    fn evaluate(
        &self,
        context: &ReviewContext,
        receipts: &[ReviewReceipt],
    ) -> PolicyDecision;
}
```

Generic decisions are `NeedMoreReviews`, `Advance`, `Repeat`, and `Halt`.

House-specific elbow-grease policy owns reviewer independence, review lenses, severity thresholds, verification commands, maximum rounds, prompts, and model selection. Those do not belong in Interlockutor or the generic reducer.

## Why not Interlockutor?

Interlockutor's reusable primitives are event IDs, topics, idempotent append, ordered broadcast cursors, at-least-once shared work, leases, renewal, negative acknowledgement, and monotonic fences.

Its fence protects `ack_work`; it does not prevent a stale worker from appending a result. It also lacks conditional aggregate append, arbitrary stream reads, transactions, outbox, persistence, snapshots, retry budgets, and dead-letter state. Encoding elbow-grease there would couple neutral transport to one domain while leaving workflow truth under-specified.

## Delivery sequence

1. Coordinator creates a dispatch event transactionally with an outbox entry.
2. Interlockutor delivers work under a lease and fence.
3. Worker performs attempt-scoped idempotent work and submits a result envelope.
4. Coordinator validates authenticated actor, aggregate version, workflow epoch, workflow deadline, and all content bindings.
5. Coordinator commits the transition and outbox record.
6. Only then does it acknowledge the Interlockutor lease.
7. If acknowledgement is lost, redelivery finds the existing transition and safely acknowledges it.

For code, the merge executor must start from the approved base commit/tree and verify that the candidate merge produces the approved result tree. It updates the approved target ref with an atomic compare-and-swap whose expected old OID is `base_commit_oid` (for Git, a locked `update-ref` transaction with the old OID). CAS loss performs no retry or force update: it invalidates the candidate and requires a new artifact and review against the new base. A changed base, merge conflict resolution, rebase, amend, target ref, or result tree cannot reuse the old approval.

`Complete` rechecks that the output artifact is the artifact carried by `Approved`. For code, completion additionally requires the merge executor's base/result verification receipt. `Approved` is not permission to substitute a different output on the last hop.

## Later production increments

After the first atom: SQLite expected-version aggregate storage and outbox; Interlockutor adapter; artifact storage; Dione pending/resume adapter; house elbow-grease policy; then DAG scheduling and operational recovery.
