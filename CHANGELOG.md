# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

## [Unreleased]

### Added

- Added `ClaimOutcome` and `EventStore::claim_detailed`, so a losing claimant
  can distinguish live contention from an empty topic and can read the current
  holder, its fencing token, and its lease expiry instead of retrying blindly.
  A contended outcome reports only work that is *currently* leased and
  unexpired: acknowledged and fence-exhausted items are never reported as
  having a current holder. Available work still wins over earlier contention,
  and because `claim` names no event, the disclosed holder is the one on the
  lowest-sequence contended event.

  Disclosing the holder is a deliberate exposure, documented on `ClaimOutcome`
  and in the README. It grants no authority — fence and owner checks still gate
  every mutation, so a `ConsumerId` is a coordination identifier, not a
  credential — but it is an enumeration surface for any future backend serving
  mutually-distrusting claimants, which must gate or redact it under its own
  policy.

- Added a domain-neutral urgent-message dogfood trace covering one live courier
  lease, at-least-once redelivery after a lost queue acknowledgement, stale
  fence rejection, and the losing claimant's view of the holder.

  Its recipient adapter records one file per `EventId` via stage-fsync-link, so
  deduplication and the effect record are a single atomic step. Tests establish
  once-only acceptance under a barrier-released race between a stale and a
  superseding courier, non-wedging recovery from torn staging writes, path
  confinement for hostile `EventId`s, refusal to treat a corrupt record as a
  prior acceptance, and a negative control that genuinely duplicates without
  `EventId` keying. Crash durability is implemented to the standard commit
  protocol but is not demonstrated by any test, and directory-sync durability
  is a unix-only guarantee.

### Changed

- **Breaking (implementors):** `EventStore::claim_detailed` is now the required
  claim method and `EventStore::claim` is a provided method that projects its
  result. Backends implementing `EventStore` must implement `claim_detailed`;
  they should not override `claim`, because the two must not compute the
  outcome separately or take separate locks. Callers of `claim` need no change:
  `claim` remains `Result<Option<Lease>, Error>` and is exactly
  `claim_detailed(..)?.granted()`, an equivalence asserted in the test suite.

- Replaced the leased-work transition internals with a small semantic kernel
  shared verbatim with an unpublished Kani proof crate. Fence exhaustion now
  leaves an item permanently unclaimable instead of panicking, and lease
  timestamp overflow is rejected as an invalid duration. The product MSRV
  remains 1.95; the proof adapter declares Rust 1.93 for Kani 0.67.

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
