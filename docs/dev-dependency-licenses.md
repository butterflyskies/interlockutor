# License receipt: the dev-only `trybuild` subtree

The compile-fail security guards are enforced by [trybuild], which arrived with a
dependency subtree that nothing in the published crate links. This file records
what that subtree is and what each crate is licensed under.

Regenerate or verify it with:

```console
scripts/check-dev-dependency-licenses.py          # verify, non-zero on drift
scripts/check-dev-dependency-licenses.py --write  # regenerate after a lock change
```

## Three separate claims, kept separate

They are easy to run together, and running them together is how a passing gate
gets credited for work it did not do.

1. **`cargo deny check` passes.** True, and it is the CI gate. `advisories ok,
   bans ok, licenses ok, sources ok`.

2. **`cargo-deny`'s license traversal does not reach this subtree.** Also true,
   and it is why claim 1 does not imply claim 3. `cargo deny list` enumerates
   exactly 13 crates — `interlockutor`, `interlockutor-kani`, `itoa`, `memchr`,
   `proc-macro2`, `quote`, `serde`, `serde_core`, `serde_derive`, `serde_json`,
   `syn`, `unicode-ident`, `zmij` — and not one of the 16 below. The check exits
   zero **without having looked at them**. A passing `cargo deny check` is
   therefore not evidence about these crates' licenses in either direction.

3. **The licenses below were checked, by hand, against the packaged metadata.**
   That is this file. Each entry's `license` field was read from the `Cargo.toml`
   inside the vendored `.crate` under `$CARGO_HOME/registry/src`, not from
   crates.io metadata or from a summary, and the corresponding licence texts were
   confirmed present in the same directory.

Claim 3 is what licence-clears the subtree. Claim 1 is a different check that
happens to be green. Nothing here proposes changing the `cargo-deny` policy or
its traversal — that is tracked separately — so the gap is recorded rather than
closed, and this receipt is the thing that closes it in the meantime.

## What was verified, and how

- **Membership** is derived from the gate itself: `Cargo.lock` minus everything
  `cargo deny list` enumerates. That is exactly the set the licence check exits
  zero without evaluating, so the receipt cannot disagree with `cargo-deny` about
  where the gap is.

  "Dev-only" and "not covered by `cargo-deny`" are **not** the same set, and
  conflating them overstates the gap by five crates. `serde_derive`, `syn`,
  `quote`, `proc-macro2`, and `unicode-ident` are absent from
  `cargo tree -e no-dev` — they reach the build only through the `derive` feature
  that the dev-dependency on `serde` enables — yet `cargo-deny` does evaluate
  them under `all-features = true`. They are covered, so they are not listed
  here.
- **Nothing below ships.** Every crate in the table is separately checked to be
  absent from `cargo tree -e no-dev --workspace`, whose whole output for this
  workspace is `serde`, `serde_core`, `serde_json`, `itoa`, `memchr`, and `zmij`.
  The check script fails loudly if a crate is ever both outside `cargo-deny`'s
  traversal and inside the shipping graph, which would be a materially worse
  finding than a stale table.
- **Declared licence** is the `license` field of the packaged `Cargo.toml`.
- **Licence texts** are the licence files shipped in the same package. All are
  full texts, not stubs; the smallest is `termcolor`'s and `winapi-util`'s
  126-byte `COPYING`, which is a pointer to the `UNLICENSE` and `LICENSE-MIT`
  files beside it, both of which are present and complete.

## Acceptability

Every entry is a permissive licence already on the `deny.toml` allow list, or a
disjunction with at least one allowed member:

- Fourteen are `MIT OR Apache-2.0` (in one order or the other). Both disjuncts
  are allowed.
- `winnow` is `MIT` alone. Allowed.
- `termcolor` and `winapi-util` are `Unlicense OR MIT`. `Unlicense` is not on the
  allow list; `MIT` is, and satisfies the disjunction. This is the same shape as
  `memchr`, which the existing `cargo-deny` run already accepts on its `MIT`
  disjunct.

No entry uses an `AND` conjunction, so no entry requires an unlisted licence.
**No code, manifest, or policy change follows from this receipt.**

## The subtree

<!-- BEGIN GENERATED: dev-dependency-licenses -->
| crate | version | declared license | licence files in package |
| --- | --- | --- | --- |
| `equivalent` | 1.0.2 | Apache-2.0 OR MIT | LICENSE-APACHE, LICENSE-MIT |
| `glob` | 0.3.4 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `hashbrown` | 0.17.1 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `indexmap` | 2.14.0 | Apache-2.0 OR MIT | LICENSE-APACHE, LICENSE-MIT |
| `serde_spanned` | 1.1.1 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `target-triple` | 1.0.1 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `termcolor` | 1.4.1 | Unlicense OR MIT | COPYING, LICENSE-MIT, UNLICENSE |
| `toml` | 1.1.4+spec-1.1.0 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `toml_datetime` | 1.1.1+spec-1.1.0 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `toml_parser` | 1.1.3+spec-1.1.0 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `toml_writer` | 1.1.2+spec-1.1.0 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `trybuild` | 1.0.120 | MIT OR Apache-2.0 | LICENSE-APACHE, LICENSE-MIT |
| `winapi-util` | 0.1.11 | Unlicense OR MIT | COPYING, LICENSE-MIT, UNLICENSE |
| `windows-link` | 0.2.1 | MIT OR Apache-2.0 | license-apache-2.0, license-mit |
| `windows-sys` | 0.61.2 | MIT OR Apache-2.0 | license-apache-2.0, license-mit |
| `winnow` | 1.0.4 | MIT | LICENSE-MIT |
<!-- END GENERATED: dev-dependency-licenses -->

`winapi-util`, `windows-link`, and `windows-sys` are `termcolor`'s
Windows-target dependencies. They are in `Cargo.lock` and so are enumerated here,
though they are not built on the Linux CI target.

## Scope of this receipt

It is a statement about the licences of a specific locked set at a specific
time. It is **not** a security review of those crates, an audit of their
transitive build scripts, or a claim that `cargo-deny` covers them — see claim 2.
`scripts/check-dev-dependency-licenses.py` fails when the locked set drifts from
the table, so a new or bumped dev dependency makes this file go stale loudly
rather than quietly. Wiring that script into CI is a separate decision and has
deliberately not been made here.

[trybuild]: https://docs.rs/trybuild
