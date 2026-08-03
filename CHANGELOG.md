# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

## [Unreleased]

### Added

- Added `ClaimOutcome` and `EventStore::claim_detailed`, so a losing claimant
  can distinguish live contention from an empty topic and can read the current
  holder and its lease expiry instead of retrying blindly. A contended outcome
  reports only work that is *currently* leased and unexpired: acknowledged and
  fence-exhausted items are never reported as having a current holder.
  Available work still wins over earlier contention, and because
  `claim_detailed` takes no event id, the disclosed holder is the one on the
  lowest-sequence contended event. That holder may be the calling consumer
  itself.

  Disclosing the holder is a deliberate exposure, documented on `ClaimOutcome`
  and in the README: it is an enumeration surface for any future backend serving
  mutually-distrusting claimants, which must gate or redact it under its own
  policy. The holder's active **fencing token is not disclosed**.

- Added a domain-neutral urgent-message dogfood trace covering one live courier
  lease, at-least-once redelivery after a lost queue acknowledgement, stale
  fence rejection, and the losing claimant's view of the holder.

  Its recipient adapter records one file per `EventId` via stage-fsync-link, so
  deduplication and the effect record are a single atomic step. Tests establish
  once-only acceptance under a barrier-released race between a stale and a
  superseding courier, non-wedging recovery from torn staging writes, path
  confinement for hostile `EventId`s, refusal to treat a corrupt or substituted
  record as a prior acceptance, durability repair after a failed post-link
  directory sync, and a live-stage-versus-scavenger race. Two negative controls
  are included and establish different things: an unkeyed sequential control
  isolating `EventId` keying, and a check-then-append control whose losing
  interleaving is forced rather than sampled. Per-step failure semantics for the
  commit protocol are tabulated in the module docs, including the cells that are
  argued rather than injected. Crash durability is implemented to the standard
  commit protocol but is not demonstrated by any test, and directory-sync
  durability is a unix-only guarantee.

### Changed

- **Breaking (implementors):** `EventStore::claim_detailed` is now the required
  claim method. Backends implementing `EventStore` must implement it.

- **Breaking (callers):** `claim` moved off `EventStore` onto a new sealed
  `EventStoreExt`, blanket-implemented for every `EventStore`. Callers must add
  `use interlockutor::EventStoreExt;`. The signature is unchanged and the
  behaviour is unchanged: `claim` is still exactly
  `claim_detailed(..)?.granted()`.

  As a provided trait method, `claim` was overridable, so the documented
  single-authority equivalence held only by an implementor's goodwill — two
  computations, two lock acquisitions, two chances to observe different state.
  A blanket impl behind a private sealed supertrait means there is exactly one
  body and no backend can substitute another, so the equivalence now holds by
  construction rather than by documentation.

- Replaced the leased-work transition internals with a small semantic kernel
  shared verbatim with an unpublished Kani proof crate. Fence exhaustion now
  leaves an item permanently unclaimable instead of panicking, and lease
  timestamp overflow is rejected as an invalid duration. The product MSRV
  remains 1.95; the proof adapter declares Rust 1.93 for Kani 0.67.

### Fixed

- **Breaking (callers), security:** `Lease` fields are private and the type has
  no public constructor. Read-only accessors `event`, `owner`, `fence`, and
  `expires_at` replace direct field access; mutating a granted lease is no
  longer possible from outside the crate.

  Combined with the removal of `fence` from `ClaimOutcome::Contended`, this
  closes a lease-forgery path. `Contended` disclosed the holder *and* the active
  fence; every `Lease` field was public; and lease mutations validate the
  supplied owner, fence, and canonical event without authenticating the caller.
  A topic-authorized losing claimant could therefore read the canonical event,
  construct the winner's `Lease` from published data, and terminally `ack_work`
  another worker's live item before that worker performed its effect.

  The two halves are coupled and neither may be relaxed alone: possession-as-
  capability requires the token to be unconstructable, or else disclosure leaks
  the capability. The design position — that mutation authenticates the token
  rather than the caller — is now stated explicitly on `Lease` and in the
  README. Caller authentication is deliberately not implemented.

  **Known break:** because `claim_detailed` and `renew` return `Lease` values
  that nothing outside the crate can construct, out-of-crate backends cannot
  implement `EventStore` as of this change. The minting seam is unresolved and
  needs a design decision; the alternatives and why each is weaker than it looks
  are documented on `EventStore` and in the README.

- Recipient acceptance no longer treats any record filed under the right name as
  a prior acceptance. The `AlreadyExists` path requires the parsed record to
  equal the receipt the event deterministically produces; a record with the
  correct `EventId` but a substituted `receipt_id` is `UnusableRecord`, not
  `Existing`.

- Recipient acceptance repairs the post-link durability gap. If `hard_link`
  succeeded and the `effects/` directory fsync failed, the entry was left
  visible while the attempt reported failure, and every later retry answered
  `Existing` without ever re-syncing — so a courier could acknowledge over an
  entry that was never made crash-durable. The `AlreadyExists` path now fsyncs
  the target directory before validating, and propagates a repeated failure.

- Corrected the recipient scavenger's ownership claim. Age does not establish
  abandonment, so a live staging file can be reaped when an attempt outlives the
  reap window; the previous "never removed" wording was false. Age is now
  documented as a heuristic with its assumption stated, `EffectStore::open_with`
  refuses a window below a floor, and zero-age reaping is confined to an
  explicitly exclusive recovery entry point.

- `AcceptError::UnusableRecord` retains the underlying parse or I/O failure as
  an `Error::source` instead of flattening it into a message string.

- **Security (CI enforcement):** the three compile-fail guards protecting the
  lease-forgery fix are now enforced. They were `compile_fail` doctests, and
  were unenforced in two independent ways:

  1. **CI never ran them.** `build.yml` runs `cargo nextest run`, and nextest
     does not execute doctests; there was no `cargo test --doc` step anywhere.
     A change restoring public `Lease` construction or re-disclosing
     `ClaimOutcome::Contended::fence` would have merged green.
  2. **The error was unpinned.** `compile_fail,CODE` does not self-enforce on
     stable — error-code checking is nightly-gated, so the code is parsed and
     silently ignored. Negative control on rustc 1.95.0: pinning a deliberately
     wrong code (`E0308`) on all three doctests still reported `3 passed;
     0 failed`, indistinguishable from a correct pin.

  The guards are now trybuild UI tests under `crates/interlockutor/tests/ui`
  with committed `.stderr` files, so the exact diagnostic is pinned and a guard
  that starts failing for an unrelated reason goes red. They are ordinary
  `#[test]`s, so nextest runs them with no extra CI step. CI additionally gained
  `cargo clippy --all-targets` and a `cargo test --workspace --doc` step as
  belt-and-braces for the remaining doctests.

  **The `EventStoreExt` sealing guard was vacuous and has been rebuilt.** Its
  `struct Backend;` never implemented the required `EventStore` supertrait, so
  it failed on the missing bound rather than on the seal — verified by removing
  the seal *and* the blanket impl entirely and watching the old snippet still
  fail to compile. The replacement uses a backend that fully implements
  `EventStore`, so the only remaining error is the substitution itself.

  That rebuild also corrected a doc claim: what rejects a substituted `claim`
  body is **coherence (`E0119`) against the blanket impl**, not the private
  sealed supertrait. `sealed::Sealed` is blanket-implemented for
  `T: EventStore + ?Sized`, the same bound as `EventStoreExt`'s blanket impl, so
  it is satisfied exactly when `EventStore` is and excludes no type on its own.
  The guarantee is unchanged and real; the attribution was wrong.

## [0.3.0] - 2026-07-24

### Changed

- Removed `#[non_exhaustive]` from `PayloadError` and `Error` per ADR-0040
  (public enums are exhaustive by default). Both enums now support exhaustive
  matching without a wildcard arm. Migrating callers who match every variant
  of `PayloadError` or `Error` plus a wildcard arm should remove the
  wildcard: it is now unreachable and will fail the build under
  `#![deny(warnings)]`. Callers with an intentionally partial match — a
  wildcard that still covers variants they don't name — need no change.
  Adding a new variant to either enum is now a breaking change requiring its
  own major-version bump.

## [0.2.0] - 2026-07-12

### Added

- Added the opaque `Payload` type with explicit raw-byte construction and
  access, JSON serialization, and a typed serialization error.
- Added `MemoryStore::with_clock` for deterministic tests and custom clock
  injection.

### Changed

- Changed `NewEvent::payload` and `Event::payload` from raw `Vec<u8>` values to
  `Payload`.
- Changed `MemoryStore::new` to require only an explicit authorizer and create
  its own process-monotonic clock. Migrating 0.1 callers should wrap raw bytes
  in `Payload` and use `MemoryStore::with_clock` when clock injection is needed.

### Fixed

- Sampled lease time while holding the store state lock so claim, renewal,
  acknowledgement, and negative acknowledgement use time authoritative at the
  state transition.

## [0.1.0] - 2026-07-11

### Added

- Initial public in-memory core for idempotent event append, independent
  broadcast cursors, and fenced leased-work delivery.

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html
[0.3.0]: https://github.com/butterflyskies/interlockutor/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/butterflyskies/interlockutor/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/butterflyskies/interlockutor/releases/tag/v0.1.0
