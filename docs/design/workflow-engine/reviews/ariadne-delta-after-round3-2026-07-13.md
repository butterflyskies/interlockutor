# Delta receipt after round 3

Reviewer: Ariadne. 2026-07-13.

Bound to:

- `README.md`: `e151b0d9ac0a8444c8acf578cce8155501a0a0387b239b1ea8bfff0d8d97e425` (unchanged)
- `architecture.md`: `3b9a78d8638165227381b27cec0473ac10ac1f601304f74e214e2737829f290c`
- `threat-and-test-plan.md`: `575e9a0b811be74e7e2ce1a77efa807459abb4528d91047852c5df47616d4e1d`

The reviewer reconstructed the round-3 bytes (`522b2faf` / `c746682`), verified their hashes, and confirmed the delta contains exactly two additive hunks:

1. The state model adds `Approved -> RevisionRequired` when merge compare-and-swap loses, minting a new revision, plus the explicit successful `Approved -> Completed` edge. A moved base therefore requires a fresh `CodeArtifactRef`, subject digest, and receipt; old approval cannot be reused.
2. The threat/test plan adds the corresponding no-stall and no-reuse assertion.

R1-R4 sections are otherwise unchanged. `Approved` was never terminal, monotonic revision still holds, and the liveness edge does not weaken the converged safety contract.

Verdict: clean.
