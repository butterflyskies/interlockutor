# Threat model and test plan

## Trust boundary

Executors, reviewers, queue consumers, adapters, and coordinators may crash, retry, race, or submit stale data. Queue consumer identity is not authenticated principal identity. Interlockutor delivery state is not workflow truth.

## Threats and controls

| Threat | Control |
| --- | --- |
| Worker finishes after workflow deadline | Reject by server-owned clock even without redispatch. |
| Crash after external effect, before result commit | Attempt-scoped idempotency key and result lookup. |
| Crash after result commit, before queue ACK | Reapply idempotently on redelivery, then ACK. |
| ACK before durable state commit | Forbidden: commit aggregate and outbox first. |
| Lost ACK response | Reconcile from aggregate state. |
| Concurrent coordinators | Atomic expected-version append. |
| Artifact changes during review | Bind exact artifact digest and revision. |
| Instructions or policy change | Bind producer instructions, review instructions, and policy digest. |
| State changes after receipt admission, before approval | Revalidate every binding inside the atomic `Approve` transition. |
| Receipt replay across runs or steps | Bind all coordinates and enforce receipt uniqueness. |
| Identical bytes reused later | Revision and attempt binding prevent ABA approval. |
| Reviewer aliases evade independence | Use authenticated principal identity, not consumer ID. |
| Worker submits a forged reviewer receipt | Require authenticated command actor to equal the recorded reviewer principal. |
| Policy returns `Advance` with zero receipts | Reducer requires a non-empty, terminally linked receipt set before policy evaluation can advance. |
| Noncanonical hashing | Named canonical encoding and domain-separated hashing. |
| Artifact tampering | Verify digest and length on every read. |
| Renewal fails during long work | Workflow heartbeat must extend the aggregate deadline; otherwise reject the result. |
| Poison work retries forever | Bounded retry and dead-letter/operator escalation. |
| Process restart loses state | Durable backend plus kill/restart tests. |
| Logs treated as audit ledger | Store transitions and receipts durably. |
| Dione blocks on multi-agent work | Deadline plus pending/resume protocol. |
| Reviewed code is rebased or applied to a different base | Code subject digest binds target ref, base commit/tree, result tree, and patch digest. |
| Target ref moves between merge verification and update | Atomically compare-and-swap from the approved base OID; CAS loss returns to review. |
| Merge executor produces unreviewed bytes | Verify approved base and resulting tree before the atomic ref update. |
| Caller forges a principal ID | Only a trusted authentication adapter may construct `AuthenticatedPrincipal`; caller-supplied IDs are rejected. |
| Policy or instructions change during review | Cancel attempts, invalidate receipts, and begin a new sealed review round. |
| Projection or worker writes aggregate state | Repository write capability is coordinator-only; projections are read-only. |
| Expiry sweeper is delayed or down | Result and receipt admission check server-owned deadline directly. |
| Unapproved output substituted at completion | `Complete` rechecks the approved artifact and merge verification receipt. |

## Invariants

1. Queue ACK success cannot authorize a workflow transition.
2. A stale workflow epoch or expired workflow deadline can never mutate current aggregate state.
3. Approval implies a policy-valid current receipt set.
4. Mutating any receipt-bound input makes the receipt ineligible.
5. Terminal states never reopen.
6. Revision increases monotonically.
7. Artifact bytes are verified before use.
8. Approval policy is evaluated against current aggregate state in the approval transition, never trusted from a projection or prior evaluation.
9. Policy cannot advance an empty receipt set.
10. Code merge updates the approved ref only when base/result checks pass and atomic compare-and-swap from the approved base OID succeeds.
11. Completion output equals the approved artifact.

## Reducer tests

Use table-driven tests for every allowed transition and illegal source/command pair. Independently reject mismatched run, step, revision, artifact digest, producer instruction digest, review instruction digest, policy digest, attempt, workflow epoch, expired deadline, and authenticated reviewer identity.

Also record a valid receipt, then mutate each bound aggregate input before `Approve`; every case must fail. Receipt admission requires an active matching attempt and atomically marks it `ReceiptRecorded(receipt_id)`. Approval requires that exact terminal linkage. Unrecorded, expired, revoked, still-active-without-receipt, or differently assigned attempts must fail.

Supply correct public receipt fields from the wrong authenticated actor; admission must fail. Make policy return `Advance` with no receipts; the reducer must fail before honoring policy. For code artifacts, vary target ref, base commit, base tree, result tree, and patch independently; each variation must change the enclosing subject digest and invalidate prior approval. A receipt bound only to `patch.digest` must fail. The merge executor must refuse a changed base or result tree, and a concurrent ref move must make its atomic compare-and-swap fail without retry or force update.

On merge compare-and-swap loss, assert `Approved -> RevisionRequired`, revision increments, and the new base requires a new artifact and receipt set. The run must not stall in `Approved` or reuse the old approval.

Test that caller-supplied principal IDs cannot bypass the authentication adapter; projections and worker capabilities cannot call aggregate append; delayed expiry sweeping does not permit a past-deadline result; policy/instruction resealing invalidates all earlier attempts and receipts; and `Complete` rejects an output other than the approved artifact.

Assert exact emitted events and resulting state. Verify terminal-state closure and monotonic revision.

## Idempotency and concurrency

- Identical duplicate commands return the existing outcome.
- Reused idempotency keys with different content conflict.
- Duplicate receipts do not count twice toward quorum.
- Two writers at one expected version race; exactly one succeeds.

## Crash-point tests

Simulate result commit before ACK, redelivery after commit, ACK response loss, result after expiry and redispatch, revision while review is outstanding, and coordinator restart between aggregate commit and outbox dispatch.

The accepted result remains singular, stale results fail, and pending outbox work resumes.

## Property and backend tests

Generate event sequences and assert terminal closure, non-empty approval receipt validity, monotonic revision, current attempt epoch plus unexpired deadline for every accepted result, authenticated actor binding, and invalidation after changing any bound digest.

The in-memory and later SQLite repositories share an aggregate conformance suite. SQLite adds kill/restart, transaction interruption, concurrent writer, migration, and corruption tests. Artifact tests replace stored bytes beneath a valid key and require reads to fail closed.

Later adapter tests distinguish workflow epoch/deadline from Interlockutor delivery fence and cover expiry without redispatch, renewal loss in either system, negative ACK, duplicate delivery, commit-before-ACK, and ACK reconciliation. Dione tests prove a bounded pending response can resume without holding an MCP call open for the workflow duration.
