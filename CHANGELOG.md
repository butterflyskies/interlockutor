# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

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
