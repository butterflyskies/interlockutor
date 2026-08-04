#!/usr/bin/env python3
"""Verify the dev-only dependency licence receipt against the current lockfile.

`cargo deny check` passes without examining the dev-only subtree at all, so its
green result is not evidence about these crates. `docs/dev-dependency-licenses.md`
records what was actually checked; this script is what catches that record going
stale when the lockfile moves.

Nothing runs it for you. No CI workflow invokes it and no gate depends on its
exit code, so a lockfile change can land with the receipt already stale. The
check happens when a human types one of the lines below and not otherwise;
wiring it into CI is a separate decision, tracked separately.

    check-dev-dependency-licenses.py           verify, non-zero exit on drift
    check-dev-dependency-licenses.py --write   regenerate the table

Membership is derived rather than listed, and derived from the gate itself:
everything in `Cargo.lock` that `cargo deny list` does not enumerate. That is
precisely the set the licence check exits zero without looking at, so the script
cannot disagree with the gate about where the gap is.

The set is separately asserted to be dev-only, by checking that none of it
appears in `cargo tree -e no-dev --target all`. The target is pinned to `all`
because `Cargo.lock` and `cargo deny` are both target-universal and `cargo
tree` is not: host-scoped, the assertion cannot see a crate version reachable
only behind `cfg(windows)`. Note that "dev-only" and "not covered by
cargo-deny" are *not* the same set: `serde_derive`, `syn`, `quote`,
`proc-macro2`, and `unicode-ident` reach the build only through a dev-enabled
feature, yet cargo-deny does evaluate them under `all-features = true`. They are
covered, so they are not part of this gap.

Licences are read from the `Cargo.toml` inside the vendored package under
`$CARGO_HOME/registry/src`, so this reports what was packaged, not what an index
or a summary says.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
import textwrap
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

ROOT = Path(__file__).resolve().parent.parent
RECEIPT = ROOT / "docs" / "dev-dependency-licenses.md"
BEGIN = "<!-- BEGIN GENERATED: dev-dependency-licenses -->"
END = "<!-- END GENERATED: dev-dependency-licenses -->"
LICENCE_FILE = re.compile(r"(?i)^(licen[cs]e|copying|unlicense|notice)")


def die(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


def name_version(crate: str) -> tuple[str, str]:
    """Splits a `name@version` key. Crate names cannot contain `@`; versions can
    contain `+` build metadata, so partition from the right."""
    name, _, version = crate.rpartition("@")
    return name, version


def crate_list(crates: Iterable[str]) -> str:
    return ", ".join(f"`{crate}`" for crate in sorted(crates, key=name_version))


def wrap(text: str) -> str:
    return textwrap.fill(
        text, width=80, break_long_words=False, break_on_hyphens=False
    )


@dataclass(frozen=True)
class LockedPackage:
    name: str
    version: str
    source: str | None
    checksum: str | None

    @property
    def key(self) -> str:
        return f"{self.name}@{self.version}"


def locked_packages() -> dict[str, LockedPackage]:
    """Every crate in the lockfile, keyed by `name@version`.

    Keyed by name *and* version throughout. A crate can appear at two versions,
    and the two versions are two independent licence questions: one may be inside
    cargo-deny's traversal while the other is not.

    Retains `source` and `checksum` so that `registry_dir` can resolve the exact
    vendored artifact the lockfile selected rather than whichever same-named
    directory `glob` finds first.
    """
    lock = (ROOT / "Cargo.lock").read_text()
    packages: dict[str, LockedPackage] = {}
    for block in lock.split("[[package]]")[1:]:
        name = re.search(r'^name = "(.*)"$', block, re.M)
        version = re.search(r'^version = "(.*)"$', block, re.M)
        if not (name and version):
            continue
        source = re.search(r'^source = "(.*)"$', block, re.M)
        checksum = re.search(r'^checksum = "(.*)"$', block, re.M)
        pkg = LockedPackage(
            name=name.group(1),
            version=version.group(1),
            source=source.group(1) if source else None,
            checksum=checksum.group(1) if checksum else None,
        )
        packages[pkg.key] = pkg
    if not packages:
        die("Cargo.lock parsed to nothing")
    return packages


def non_dev_reachable() -> set[str]:
    """Everything reachable without dev edges, plus the workspace members.

    Returned as `name@version`, because the question this answers is whether a
    *specific* locked version ships, not whether some version of that crate does.

    `--target all` because the two sets this is compared against are both
    target-universal. `Cargo.lock` records every target's dependencies and
    `cargo deny` traverses them all, but `cargo tree` defaults to the host
    triple. Left host-scoped, a crate version reachable only behind
    `cfg(windows)` is absent from this set, so the does-not-ship assertion below
    answers "no" for a crate version that genuinely ships to Windows consumers
    and the receipt prints "None of them ships" anyway. That is a fail-open, and
    the host running the check decides which crates it hides.

    The flag widens the set beyond target gating: `--target all` also drops
    resolver-v2's per-target feature de-unification, so a feature enabled only
    by a dev-dependency (here `serde/derive`) unifies into the normal graph and
    pulls its subtree in. That over-approximates. It over-approximates in the
    safe direction — a larger reachable set can only make the assertion below
    harder to satisfy, never easier — so the generated sentence reports what the
    command reaches rather than claiming all of it ships.
    """
    out = subprocess.run(
        ["cargo", "tree", "-e", "no-dev", "--workspace", "--prefix", "none",
         "--format", "{p}", "--target", "all"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        die(f"cargo tree failed:\n{out.stderr.strip()}")
    reachable = set()
    for line in out.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 2:
            reachable.add(f"{fields[0]}@{fields[1].removeprefix('v')}")
    if not reachable:
        # An empty parse here is not an empty graph: the workspace members alone
        # are always reachable. Left ungated it would silently satisfy the
        # does-not-ship assertion below for every crate at once, which is the one
        # failure this receipt exists to make loud.
        die(
            "cargo tree exited zero but parsed to nothing; the parse is wrong, "
            "not the graph. Treating it as empty would vacuously pass the "
            "assertion that none of the uncovered crates ships."
        )
    return reachable


def deny_covered() -> set[str]:
    """Crate versions cargo-deny's licence traversal actually enumerates.

    `cargo deny list` prints `LICENSE (n): name@version, name@version, ...` for
    every licence it encountered. Anything absent from that output is a crate
    version the licence check exited zero without evaluating.

    The `name@version` is kept whole. Reducing it to a bare name would let a
    covered version vouch for an uncovered one of the same crate, which is
    exactly the case this gap is supposed to surface.
    """
    out = subprocess.run(
        ["cargo", "deny", "list"], cwd=ROOT, capture_output=True, text=True,
        check=False,
    )
    if out.returncode != 0:
        die(
            "cargo deny list failed; cargo-deny is required to derive what the "
            f"gate covers:\n{out.stderr.strip()}"
        )
    covered = set()
    for line in out.stdout.splitlines():
        _, _, listed = line.partition("):")
        for entry in listed.split(","):
            entry = entry.strip()
            if entry:
                covered.add(entry)
    if not covered:
        die("cargo deny list produced no crates; the parse is wrong, not the gate")
    return covered


def _registry_slug(source: str | None) -> str | None:
    """Extracts the registry slug from a lockfile source URL.

    A source like `registry+https://github.com/rust-lang/crates.io-index`
    corresponds to a subdirectory under `$CARGO_HOME/registry/src/` whose name
    is derived from the URL. Cargo hashes the URL into a short slug
    (`index.crates.io-<hash>`), but the slug is opaque and not reproduced here.
    Instead this returns enough of the URL to identify the registry directory
    via glob matching.
    """
    if source is None:
        return None
    # `registry+https://github.com/rust-lang/crates.io-index` → crates.io-index
    # Custom registries may differ, but the domain/path suffix is still unique
    # enough to disambiguate.
    m = re.search(r'registry\+https?://[^/]+/(.+)', source)
    if not m:
        return None
    # Take the last path component: `rust-lang/crates.io-index` → `crates.io-index`
    return m.group(1).rstrip("/").rsplit("/", 1)[-1]


def registry_dir(name: str, version: str, pkg: LockedPackage | None = None) -> Path:
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    all_matches = sorted(
        (cargo_home / "registry" / "src").glob(f"*/{name}-{version}")
    )
    if not all_matches:
        die(
            f"{name} {version} is not vendored under {cargo_home}/registry/src. "
            "Run `cargo fetch` first; this script reads packaged metadata, not "
            "the crates.io index."
        )

    # When the lockfile records a source, narrow to the registry directory whose
    # name contains the registry slug. Without this, two registries caching the
    # same name/version yield ambiguous results and the first glob match wins —
    # which may be the wrong artifact.
    matches = all_matches
    if pkg and pkg.source:
        slug = _registry_slug(pkg.source)
        if slug:
            narrowed = [m for m in all_matches if slug in m.parent.name]
            if narrowed:
                matches = narrowed

    result = matches[0]

    # Verify the checksum when the lockfile provides one. The checksum in
    # Cargo.lock is the SHA-256 of the `.crate` tarball, not of the unpacked
    # directory, so a full verification would need the tarball. Instead, verify
    # that the `.cargo-checksum.json` file in the unpacked directory agrees with
    # the lockfile. Cargo writes this file on extraction and it contains the
    # tarball checksum.
    if pkg and pkg.checksum:
        checksum_file = result / ".cargo-checksum.json"
        if checksum_file.is_file():
            content = checksum_file.read_text()
            if pkg.checksum not in content:
                die(
                    f"{name} {version}: vendored artifact at {result} has a "
                    f"checksum that does not match the lockfile "
                    f"(expected {pkg.checksum[:16]}...). The cached package may "
                    "be from a different registry or a stale download."
                )

    return result


def describe(name: str, version: str, pkg: LockedPackage | None = None) -> tuple[str, str]:
    package = registry_dir(name, version, pkg)
    manifest = (package / "Cargo.toml").read_text()
    declared = re.search(r'^license\s*=\s*"(.*)"$', manifest, re.M)
    if declared:
        licence = declared.group(1)
    else:
        by_file = re.search(r'^license-file\s*=\s*"(.*)"$', manifest, re.M)
        licence = f"license-file: {by_file.group(1)}" if by_file else "NONE DECLARED"
    files = sorted(f.name for f in package.iterdir() if LICENCE_FILE.match(f.name))
    return licence, ", ".join(files) or "none"


def build_section() -> str:
    """The whole generated body: the derived claims as well as the table.

    Counts and crate lists live in here rather than in the surrounding prose.
    Outside the markers they are unreachable by this check by construction — the
    table would be regenerated and the sentences describing it would not, leaving
    a document that contradicts itself and still verifies clean.
    """
    covered = deny_covered()
    locked = locked_packages()
    uncovered = sorted(set(locked) - covered, key=name_version)
    if not uncovered:
        die(
            "cargo deny list covers every locked crate. If that is genuinely "
            "true the gap has closed and this receipt should be retired, not "
            "emptied — say so deliberately rather than committing a blank table."
        )
    # The receipt claims none of this ships to a consumer of the published
    # crate. Assert it here rather than in prose.
    #
    # Know what this assertion is worth. cargo-deny traverses with
    # `targets = []`, `all-features = true`, and no dev edges; so does the
    # command below. While those two agree, `reachable` equals `covered`,
    # `uncovered` is `locked - covered`, and the intersection is empty by
    # construction — the check cannot fire for any lockfile. That was true
    # before the target was pinned too, the host set being a strict subset of
    # the covered one. It is a detector for the two traversals *diverging*,
    # which a narrowed `targets` in deny.toml or a dependency kind only one of
    # them walks would cause. It is not what licence-clears the table.
    reachable = non_dev_reachable()
    shipped = [crate for crate in uncovered if crate in reachable]
    if shipped:
        die(
            "these crate versions are outside cargo-deny's licence traversal "
            f"*and* in the non-dev dependency graph: {', '.join(shipped)}. That "
            "is a worse finding than a stale receipt — they ship to consumers "
            "and nothing checks their licences."
        )
    rows = [
        "| crate | version | declared license | licence files in package |",
        "| --- | --- | --- | --- |",
    ]
    declared = Counter()
    for crate in uncovered:
        name, version = name_version(crate)
        licence, files = describe(name, version, locked.get(crate))
        declared[licence] += 1
        rows.append(f"| `{name}` | {version} | {licence} | {files} |")
    tally = ", ".join(
        f"{count} × `{licence}`"
        for licence, count in sorted(declared.items(), key=lambda kv: (-kv[1], kv[0]))
    )
    return "\n\n".join(
        [
            wrap(
                f"`cargo deny list` enumerates {len(covered)} crate versions and "
                f"no others: {crate_list(covered)}. Those, and only those, are "
                "what a green `cargo deny check` is evidence about."
            ),
            wrap(
                f"The {len(uncovered)} crate versions below are everything in "
                "`Cargo.lock` that it does not enumerate — the set the licence "
                "check exits zero without having evaluated."
            ),
            "\n".join(rows),
            wrap(f"Declared licences across those {len(uncovered)}: {tally}."),
            wrap(
                "None of them ships. `cargo tree -e no-dev --workspace --target "
                f"all` reaches {len(reachable)} crate versions — "
                f"{crate_list(reachable)} — and not one row of the table above. "
                "That set is an over-approximation of what ships, not a list of "
                "it: `--target all` switches off the resolver's feature "
                "de-unification wholesale, so a subtree reached only through a "
                "dev-enabled feature appears here too. It is the right side to "
                "err on, because every crate it adds is one more the table is "
                "checked against — but see the caveat above, which is that this "
                "set and the covered set are currently identical and so the "
                "check between them cannot fire."
            ),
        ]
    )


def main() -> int:
    write = "--write" in sys.argv[1:]
    text = RECEIPT.read_text()
    if BEGIN not in text or END not in text:
        die(f"{RECEIPT} is missing its generated-section markers")

    head, rest = text.split(BEGIN, 1)
    current, tail = rest.split(END, 1)
    section = build_section()
    updated = f"{head}{BEGIN}\n{section}\n{END}{tail}"

    if updated == text:
        print(f"ok: {RECEIPT.relative_to(ROOT)} matches the lockfile")
        return 0
    if write:
        RECEIPT.write_text(updated)
        print(f"rewrote {RECEIPT.relative_to(ROOT)}")
        return 0

    print(
        f"{RECEIPT.relative_to(ROOT)} is stale: the dev-only dependency set has "
        "changed.\n\nRecorded:\n"
        f"{current.strip()}\n\nActual:\n{section}\n\n"
        "Re-verify the new licences against the allow list in deny.toml, then "
        "run with --write. `cargo deny check` does not cover these crates, so a "
        "green deny run is not a substitute for looking.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
