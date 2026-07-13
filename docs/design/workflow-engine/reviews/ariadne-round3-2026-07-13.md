# Convergence round (round 3) — receipt-bound workflow engine

Reviewer: ariadne. 2026-07-13.
Question under test (unchanged): "find any route from worked artifact to merge-candidate without a review receipt bound to exact artifact + instruction/policy digests."

## Reviewed-bytes receipt (SHA-256, verified 2026-07-13T08:30:08-07:00)

- `README.md` = `e151b0d9ac0a8444c8acf578cce8155501a0a0387b239b1ea8bfff0d8d97e425`
- `architecture.md` = `522b2faf37db7a0b56f956bcbc56bd93fe05b49bbbe19f5cc6f470b86daefe89`
- `threat-and-test-plan.md` = `c746682415a2401c6d9ce0d2d85c0e9e830bb9f09e3abe076bc474b95380fc46`

All three match the round-3 candidate prefixes. Interlockutor base unchanged (`73a45bc6`). Findings bound to these bytes.

## Verdict: CONVERGED on the adversarial question — all four findings (R1–R4) closed, no unreceipted route to merge, no new security hole. One non-blocking state-model completeness note (fail-safe, does not gate publish).

---

## Residual-2 (code-triple digest) — VERIFIED CLOSED.

`CodeArtifactRef` now carries `target_ref`, `base_commit_oid`, `base_tree_digest`, `result_tree_digest`, `patch: ArtifactRef`, and an enclosing `subject_digest: Digest` (architecture.md:66-73).

architecture.md:112: `subject_digest` is "the domain-separated canonical digest over `target_ref`, `base_commit_oid`, `base_tree_digest`, `result_tree_digest`, and `patch.digest`. `ReviewReceipt.subject_artifact_digest` must equal that enclosing digest; binding only the patch or any proper subset is non-conforming."

Coordinator's questions:

- **Domain separation actually specified?** Yes. Line 110 fixes the construction ("named canonical format and domain-separated SHA-256"); threat row 25 restates it ("Named canonical encoding and domain-separated hashing"); line 112 applies it to a fixed, enumerated 5-tuple. Named canonical encoding removes the concatenation-ambiguity class (a component-tuple boundary is unambiguous, so `(a, bc)` and `(ab, c)` cannot collide), and the domain tag prevents a component digest (e.g. `patch.digest`, itself an `ArtifactRef` digest under its own domain) from being reinterpreted as the enclosing `subject_digest`. Adequately specified for a design doc; the exact domain-tag string scheme is implementation detail, appropriately deferred.

- **"Non-conforming" enforced, not just described?** Yes, at the correct gate. A subset-bound receipt (`subject_artifact_digest = patch.digest`) can be admitted by `RecordReviewReceipt` (admission checks actor/attempt/reviewer/epoch/deadline/review-instruction digest — deliberately not content digests), but it CANNOT advance: `Approve` re-evaluates `subject_artifact_digest` against the aggregate's current artifact digest, which for code is the enclosing `subject_digest` (architecture.md:122). `patch.digest ≠ subject_digest` → rejected. Test plan line 61 asserts it directly: "A receipt bound only to `patch.digest` must fail." Placing the content-digest check at `Approve` (not admission) is consistent with the whole design — content digests are re-verified against current aggregate state atomically at the transition, which is what closes the TOCTOU class. Enforcement is reducer-level and tested. Closed.

## Residual-1 (atomic CAS merge) — VERIFIED CLOSED.

architecture.md:168: the merge executor "updates the approved target ref with an atomic compare-and-swap whose expected old OID is `base_commit_oid` (for Git, a locked `update-ref` transaction with the old OID). CAS loss performs no retry or force update: it invalidates the candidate and requires a new artifact and review against the new base." Threat row 33, invariant 10, requirement 12, test line 61 ("a concurrent ref move must make its atomic compare-and-swap fail without retry or force update").

- **TOCTOU genuinely closed?** Yes. The atomic primitive IS the check: expected-old-OID = approved `base_commit_oid`. If the target ref still resolves to the approved base at write time, the update lands atomically; if it moved between verification and write, the CAS fails. There is no verify-then-write window left — the compare and the set are one operation. This is the last-hop analogue of the aggregate's expected-version CAS.

- **New-path scan (coordinator's specific ask): can the candidate be re-approved on the moved base WITHOUT a fresh receipt?** No — and the two R4 fixes interlock to guarantee it. `base_commit_oid` is a component of `subject_digest` (Residual-2 fix). A moved base means a new `base_commit_oid` → a new `subject_digest` → the old receipt's `subject_artifact_digest` no longer equals the aggregate's artifact digest → the old approval fails `Approve` for the new artifact. So CAS loss cannot "ride the retry": there is no retry (line 168 forbids it), and even if a fresh review is opened on the new base, it requires a fresh `CodeArtifactRef` with a fresh `subject_digest` and therefore a fresh receipt bound to the new base. A stale approval physically cannot satisfy the new artifact's digest. Closed.

## New-hole scan on the round-3 fix mechanisms

- **`target_ref` bound into `subject_digest`:** tightens — a patch approved for branch X cannot merge to branch Y without new review. No hole.
- **CAS expected-old = `base_commit_oid`:** correct git semantics (ref CAS compares current tip to expected-old). A concurrent push moves the tip, CAS fails, fail-safe. No hole.
- **Completion path:** `Complete` requires the merge executor's base/result verification receipt (architecture.md:170). If the base moved, the CAS fails → no merge receipt can be produced → `Complete` cannot fire → the old approved artifact CANNOT be published onto the moved base. The old approval is bound to the old `base_commit_oid`; substitution on the last hop is blocked. No hole.
- Rounds 1–2 closures (R1 auth adapter §128-134, R2 reducer floor §122, R3 epoch/deadline §124-126) are byte-identical to the round-2 bytes I verified. Still closed.

## Non-blocking completeness note (liveness, fail-safe — NOT a security residual, does NOT gate publish)

The state model (architecture.md:25-49) shows only `Approved -> Completed`, and `Completed`/`Failed`/`Cancelled` are terminal. But architecture.md:168 says a merge CAS loss "invalidates the candidate and requires a new artifact and review against the new base" — a return into the review cycle for which the state diagram has no edge from `Approved` (there is no `Approved -> RevisionRequired` or `Approved -> Dispatchable`). 

This is a completeness gap, not a security hole: the safety property holds regardless. On CAS loss the ref is not updated, `Complete` cannot fire without the merge receipt, and the moved base cannot reuse the old receipt (per the interlock above). The run fails safe — it stalls rather than merging unreviewed bytes. Nothing rides through. The fix is one state edge (e.g. `Approved -> RevisionRequired` on merge-CAS-loss, minting a new revision and thus a new artifact) so the prose in §168 has a corresponding transition. Syne's call whether to add it before or after publish; it changes no security property and creates no unreceipted route.

## Convergence verdict

**Converged.** All four findings closed in bytes:
- R1 (actor==reviewer + trusted auth adapter) — closed round 2, unchanged.
- R2 (reducer receipt floor) — closed round 2, unchanged.
- R3 (workflow epoch/deadline distinct from Fence, checked at every gate) — closed round 2, unchanged.
- R4 (code artifact base/result/patch binding + atomic CAS merge) — both residuals closed this round; the two fixes interlock so a moved base cannot reuse a stale approval.

No unreceipted route from worked artifact to merge-candidate exists in these bytes. No new security hole introduced by the fix mechanisms. One fail-safe state-model completeness note (Approved→review edge for CAS-loss), non-blocking. Clear to publish.
