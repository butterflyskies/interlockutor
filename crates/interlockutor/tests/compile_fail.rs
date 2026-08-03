//! Compile-fail guards for the crate's security boundary, as trybuild UI tests.
//!
//! # Why these are not `compile_fail` doctests
//!
//! They were, and that was weaker than it read.
//!
//! 1. **CI never ran them.** `build.yml` runs `cargo nextest run`, and nextest
//!    does not execute doctests. There was no `cargo test --doc` step anywhere,
//!    so all three guards were unenforced: a change restoring public `Lease`
//!    construction or re-disclosing `Contended::fence` merged green.
//! 2. **The error was unpinned.** `compile_fail,E0451` does not self-enforce on
//!    stable; error-code checking is nightly-gated, so the code is parsed and
//!    silently ignored. Verified by negative control on rustc 1.95.0: pinning a
//!    deliberately wrong code (`E0308`) on all three doctests still reported
//!    `3 passed; 0 failed`, indistinguishable from a correct pin. A guard that
//!    started failing for an unrelated reason would have stayed green.
//!
//! Trybuild closes both. These are ordinary `#[test]`s, so nextest already runs
//! them with no extra CI step, and the committed `.stderr` pins the exact
//! diagnostic rather than merely "something went wrong".
//!
//! # The `.stderr` files are toolchain-sensitive
//!
//! Trybuild compares rendered diagnostics verbatim, so a rustc release that
//! rewords or re-spans one of these errors turns this test red on a build that
//! is otherwise fine. That is the cost of pinning the exact error, and it is
//! preferable to the previous state, where nothing was pinned and nothing ran.
//! Refresh with `TRYBUILD=overwrite cargo test -p interlockutor --test compile_fail`
//! and **read the diff** — confirm the error is still the intended one before
//! committing the new expectation.

#[test]
fn security_boundary_guards_do_not_compile() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
