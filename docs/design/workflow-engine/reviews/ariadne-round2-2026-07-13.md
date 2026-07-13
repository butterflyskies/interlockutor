# Fix-verification round (round 2) — receipt-bound workflow engine

Reviewer: ariadne. 2026-07-13. Re-review to convergence against new bytes.
Question under test (unchanged): "find any route from worked artifact to merge-candidate without a review receipt bound to exact artifact + instruction/policy digests."

## Reviewed-bytes receipt (SHA-256, verified 2026-07-13T08:24:24-07:00)

- `README.md` = `3dde8d980f992dc41b62e16339154d56c17dd509e559e19be379e5d928af696e`
- `architecture.md` = `c638cdcc70943bfdf968252aab8e948e223f4fdc4f296961ab4e01690ada2e21`
- `threat-and-test-plan.md` = `884c144344f3a18aa88eb838f817fdfdf762a19ab43a997f9e20cb3f2b8571f4`

All three match the candidate prefixes. Findings bound to these bytes. Interlockutor base unchanged (`73a45bc6`). Ground rule held: pushed bytes only; unspecified = listed.

## Verdict: NOT YET CONVERGED — R1/R2/R3 verified closed in bytes; two residuals remain, both on R4's last hop.

---

## R1 — actor==reviewer at admission + trusted auth adapter. VERIFIED CLOSED.

Present in bytes:
- architecture.md:113 — receipts accepted "only when the authenticated command actor equals the attempt's recorded reviewer principal."
- architecture.md:117 — `RecordReviewReceipt` rejects any receipt whose authenticated actor doesn't match a recorded active attempt.
- architecture.md:125-127 (new "Authority and sealing") — workers/reviewers cannot append aggregate events; they submit signed/session-authenticated envelopes to a trusted adapter that constructs `AuthenticatedPrincipal`; only the coordinator has expected-version write authority; "an adapter that accepts caller-supplied principal IDs is non-conforming."
- Requirement 13, threat rows 23/34/36, invariant coverage.

Spoofing check: reviewer identity can no longer be forged at the reducer because the reducer never sees a caller-supplied principal — only an `AuthenticatedPrincipal` the adapter minted. The guarantee now reduces cleanly to "the auth adapter is trusted," which is explicitly stated, with the production mechanism deferred and a fake verifier used in the first atom. That deferral is scoped correctly for a pure-reducer atom and is a standing dependency, not an open hole. No new hole.

## R2 — reducer floor before policy. VERIFIED CLOSED.

- README:28 core invariant now reads "only with at least one valid receipt and when its review policy accepts..."
- architecture.md:119 — "`Approve` first enforces a reducer-level floor of at least one receipt with a matching `ReceiptRecorded` attempt. Policy may add stricter quorum or independence requirements but cannot remove that floor."
- Invariant 9 ("Policy cannot advance an empty receipt set"), threat row 24, reducer test line 60.

The floor sits at the reducer, before policy evaluation, and the floor receipt must itself carry a `ReceiptRecorded` linkage (so it already passed actor+epoch+deadline binding at admission). Empty-set `Advance` is rejected structurally, not by policy goodwill. No new hole.

## R3 — workflow-owned AttemptEpoch + server deadline, distinct from Interlockutor Fence. VERIFIED CLOSED.

- State model carries `epoch, deadline` in `Running` and `Reviewing`; `ReviewAttemptRef` has `epoch: AttemptEpoch`, `deadline: MonotonicDeadline`.
- architecture.md:121 — epochs/deadlines "distinct from Interlockutor delivery leases... an Interlockutor fence never authorizes a workflow result." The two-fence-space ambiguity from round 1 is resolved by explicit naming: workflow mints epochs, Interlockutor mints `Fence` for delivery only.
- architecture.md:113 — "The reducer checks its server-owned clock against the attempt deadline; expiry rejects a result even when no redispatch has occurred." Closes the expiry-without-redispatch surface at the gate.
- architecture.md:123 — lazy-sweeper clause: "correctness never depends on the sweeper running: a deadline in the past is invalid even when stored status still says active." Invariant 2, threat rows 11/27/37.

On the coordinator's specific question — is the epoch checked at the approval transition, not just issuance? Yes, and the design places the checks correctly:
- Stale **result** (Running→result): epoch + deadline checked at submission (delivery step 4, line 160). Closed.
- Stale **receipt** (admission): epoch + unexpired deadline checked at `RecordReviewReceipt` (line 117). Closed.
- At **Approve**: the receipt must link to a `ReceiptRecorded` attempt with matching epoch, and reseal/revision invalidate recorded receipts (lines 119, 131). Approve deliberately does NOT re-check wall-clock deadline — a validly recorded receipt does not rot because approval was delayed; only binding changes (revision, artifact, instruction/policy digest, reseal) invalidate it. That is the correct semantics: the deadline governs attempt liveness for *submission*, not receipt longevity. A dead/expired epoch's *result* cannot reach approval because it never becomes a `ReceiptRecorded` linkage in the first place. Closed.

## R4 — code artifact binds base+result+patch; merge executor verifies. PARTIALLY CLOSED — two residuals.

What landed and is correct:
- `CodeArtifactRef { base_tree_digest, result_tree_digest, patch }` (architecture.md:66-70).
- architecture.md:109 — rebase/amend changing base, result, or patch creates a new artifact and invalidates prior review "even when the visible diff happens to be identical."
- Requirement 12; architecture.md:165 merge-executor verification; architecture.md:167 `Complete` rechecks approved artifact + merge verification receipt; threat rows 32/33/38; invariants 10/11.

Two residuals remain:

### Residual 1 (the coordinator's TOCTOU) — ref update is verify-then-write, not an atomic compare-and-swap on the target ref.

architecture.md:165 and threat row 33 both say the executor verifies the approved base and result "before updating the target ref." That specifies **ordering**, not **atomicity**. The whole engine uses expected-version CAS on the aggregate store — but that discipline is not carried onto the git ref write. Sequence:

1. executor reads target ref → base X, verifies X == approved base
2. computes/verifies merge → approved result tree
3. writes target ref

If step 3 is unconditional, a concurrent writer can move the target ref from X to Y between steps 1 and 3. The executor then lands the approved-result-tree commit (reviewed against base X) onto a branch whose base is now Y — a merge-candidate ref state whose base context differs from what was reviewed. That is exactly the adversarial question's shape: worked artifact reaching merge-candidate where the base no longer matches the reviewed base.

Fix (one sentence): the ref update is a compare-and-swap requiring the target ref to still resolve to the approved base at write time (git `update-ref` with expected old-oid, or a locked ref transaction). A losing CAS returns to review rather than overwriting. This is the last-hop analogue of the expected-version CAS the aggregate already mandates.

### Residual 2 — the receipt's scalar `subject_artifact_digest` vs. the code artifact triple.

`ReviewReceipt.subject_artifact_digest` is a single `Digest` (architecture.md:95). A code artifact is three digests (base, result, patch). The spec asserts a base change "creates a new artifact and invalidates prior review" (line 109), but nowhere states that `subject_artifact_digest` for code is a domain-separated digest **over all three** components. If an implementer binds `subject_artifact_digest` to the patch digest alone (the `patch: ArtifactRef` is the only sub-field that carries a `digest`), the receipt would survive a base-tree change with an unchanged patch — reopening precisely the base-drift hole R4 set out to close. The intent is stated; the binding from intent to the receipt field is not.

Fix (one sentence): for code artifacts, `subject_artifact_digest` is the domain-separated digest over `(base_tree_digest, result_tree_digest, patch.digest)`; binding only the patch is non-conforming.

## Other hardening confirmed present in bytes

- Policy/instruction reseal forces re-review: architecture.md:131 — "Changing any of them cancels active review attempts, invalidates recorded receipts, increments the review round, and returns the aggregate to `ReviewRequired`." Threat row 35. A mid-round policy change does force re-review. Confirmed.
- Reviewer assignment from policy eligibility: architecture.md:129 — policy selects eligible set, coordinator assigns `reviewer_id` at `DispatchReview`, reducer binds admission to the assignment. Confirmed.
- `Complete` rechecks approved output: architecture.md:167, invariant 11, threat row 38. Confirmed.

## Round-1 findings status

- R1 → closed (above). R2 → closed. R3 → closed. R4 → two residuals (above).
- Round-1 underspecified surfaces now explicit in bytes: U1 auth mechanism (deferred, adapter contract stated), U3 policy sealing lifecycle (line 131), U4 write authority (coordinator-only, req 13 / threat 36), U5 expiry bookkeeping (server-owned clock, line 123), U6 reviewer assignment (line 129). U2 (Approved→Completed recheck) closed by line 167. Good.

## New-hole scan (fixes did not open safety holes)

- Auth adapter (R1): identity guarantee now rests on the adapter; explicitly stated, production mechanism deferred, first-atom fake verifier. Standing dependency, not a hole for this atom.
- Heartbeat extends deadline (R3, threat row 27): a worker heartbeating indefinitely is a liveness/DoS concern bounded by retry budgets / max rounds in house policy — not a safety hole in the receipt gate. Note only.
- No new receipt-bypass introduced by the epoch/deadline or code-artifact changes.

## Convergence verdict

**Residual (2), both on R4's last hop — must close before publish:**

1. Merge ref update must be an atomic compare-and-swap on the target ref (approved base == current ref at write time), not verify-then-write. TOCTOU between verification and ref update otherwise lets the base move under an approved result.
2. `subject_artifact_digest` for code must be domain-separated over (base_tree, result_tree, patch); binding only the patch reopens base-drift.

R1, R2, R3 verified closed in bytes with no new holes. Both residuals are one-sentence spec additions plus one reducer/executor test each. One more round should converge.
