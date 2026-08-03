#!/usr/bin/env python3
"""Verify the dev-only dependency licence receipt against the current lockfile.

`cargo deny check` passes without examining the dev-only subtree at all, so its
green result is not evidence about these crates. `docs/dev-dependency-licenses.md`
records what was actually checked; this script is what stops that record from
going quietly stale when the lockfile moves.

    check-dev-dependency-licenses.py           verify, non-zero exit on drift
    check-dev-dependency-licenses.py --write   regenerate the table

Membership is derived rather than listed, and derived from the gate itself:
everything in `Cargo.lock` that `cargo deny list` does not enumerate. That is
precisely the set the licence check exits zero without looking at, so the script
cannot disagree with the gate about where the gap is.

The set is separately asserted to be dev-only, by checking that none of it
appears in `cargo tree -e no-dev`. Note that "dev-only" and "not covered by
cargo-deny" are *not* the same set: `serde_derive`, `syn`, `quote`,
`proc-macro2`, and `unicode-ident` reach the build only through a dev-enabled
feature, yet cargo-deny does evaluate them under `all-features = true`. They are
covered, so they are not part of this gap.

Licences are read from the `Cargo.toml` inside the vendored package under
`$CARGO_HOME/registry/src`, so this reports what was packaged, not what an index
or a summary says.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RECEIPT = ROOT / "docs" / "dev-dependency-licenses.md"
BEGIN = "<!-- BEGIN GENERATED: dev-dependency-licenses -->"
END = "<!-- END GENERATED: dev-dependency-licenses -->"
LICENCE_FILE = re.compile(r"(?i)^(licen[cs]e|copying|unlicense|notice)")


def die(message: str) -> "typing.NoReturn":  # noqa: F821
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


def locked_packages() -> dict[str, str]:
    lock = (ROOT / "Cargo.lock").read_text()
    packages = {}
    for block in lock.split("[[package]]")[1:]:
        name = re.search(r'^name = "(.*)"$', block, re.M)
        version = re.search(r'^version = "(.*)"$', block, re.M)
        if name and version:
            packages[name.group(1)] = version.group(1)
    if not packages:
        die("Cargo.lock parsed to nothing")
    return packages


def non_dev_reachable() -> set[str]:
    """Everything reachable without dev edges, plus the workspace members."""
    out = subprocess.run(
        ["cargo", "tree", "-e", "no-dev", "--workspace", "--prefix", "none",
         "--format", "{p}"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        die(f"cargo tree failed:\n{out.stderr.strip()}")
    reachable = set()
    for line in out.stdout.splitlines():
        line = line.strip()
        if line:
            reachable.add(line.split()[0])
    return reachable


def deny_covered() -> set[str]:
    """Crates cargo-deny's licence traversal actually enumerates.

    `cargo deny list` prints `LICENSE (n): name@version, name@version, ...` for
    every licence it encountered. Anything absent from that output is a crate the
    licence check exited zero without evaluating.
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
                covered.add(entry.rsplit("@", 1)[0])
    if not covered:
        die("cargo deny list produced no crates; the parse is wrong, not the gate")
    return covered


def registry_dir(name: str, version: str) -> Path:
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    matches = sorted((cargo_home / "registry" / "src").glob(f"*/{name}-{version}"))
    if not matches:
        die(
            f"{name} {version} is not vendored under {cargo_home}/registry/src. "
            "Run `cargo fetch` first; this script reads packaged metadata, not "
            "the crates.io index."
        )
    return matches[0]


def describe(name: str, version: str) -> tuple[str, str]:
    package = registry_dir(name, version)
    manifest = (package / "Cargo.toml").read_text()
    declared = re.search(r'^license\s*=\s*"(.*)"$', manifest, re.M)
    if declared:
        licence = declared.group(1)
    else:
        by_file = re.search(r'^license-file\s*=\s*"(.*)"$', manifest, re.M)
        licence = f"license-file: {by_file.group(1)}" if by_file else "NONE DECLARED"
    files = sorted(f.name for f in package.iterdir() if LICENCE_FILE.match(f.name))
    return licence, ", ".join(files) or "none"


def build_table() -> str:
    packages = locked_packages()
    uncovered = sorted(set(packages) - deny_covered())
    if not uncovered:
        die(
            "cargo deny list covers every locked crate. If that is genuinely "
            "true the gap has closed and this receipt should be retired, not "
            "emptied — say so deliberately rather than committing a blank table."
        )
    # The receipt claims none of this ships to a consumer of the published
    # crate. Assert it here rather than in prose.
    reachable = non_dev_reachable()
    shipped = [name for name in uncovered if name in reachable]
    if shipped:
        die(
            "these crates are outside cargo-deny's licence traversal *and* in "
            f"the non-dev dependency graph: {', '.join(shipped)}. That is a "
            "worse finding than a stale receipt — they ship to consumers and "
            "nothing checks their licences."
        )
    dev_only = uncovered
    rows = [
        "| crate | version | declared license | licence files in package |",
        "| --- | --- | --- | --- |",
    ]
    for name in dev_only:
        version = packages[name]
        licence, files = describe(name, version)
        rows.append(f"| `{name}` | {version} | {licence} | {files} |")
    return "\n".join(rows)


def main() -> int:
    write = "--write" in sys.argv[1:]
    text = RECEIPT.read_text()
    if BEGIN not in text or END not in text:
        die(f"{RECEIPT} is missing its generated-section markers")

    head, rest = text.split(BEGIN, 1)
    current, tail = rest.split(END, 1)
    table = build_table()
    updated = f"{head}{BEGIN}\n{table}\n{END}{tail}"

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
        f"{current.strip()}\n\nActual:\n{table}\n\n"
        "Re-verify the new licences against the allow list in deny.toml, then "
        "run with --write. `cargo deny check` does not cover these crates, so a "
        "green deny run is not a substitute for looking.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
