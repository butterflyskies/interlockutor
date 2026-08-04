#!/usr/bin/env python3
"""Fixture tests for `check-dev-dependency-licenses.py`.

The script under test is a compensating control: it is the only thing that
examines the licences of the crate versions `cargo deny check` exits zero
without looking at. A control that has never been shown to fail is a claim, so
each case here drives the script against a stubbed `cargo` and a synthetic
lockfile and pins the verdict it must reach.

Nothing runs this for you either. No CI workflow invokes it and no gate consumes
its exit code — the same disclosure the script under test carries, and for the
same reason: wiring either into CI is a separate decision, tracked separately.

    test-check-dev-dependency-licenses.py            run every case
    test-check-dev-dependency-licenses.py --script P run them against script P

`--script` exists for negative controls. Revert one guard in a copy of the
script, point this at the copy, and the case that guard exists for must stop
passing. If it still passes, the case was not testing the guard.

The two axes that matter are coverage (is this crate version inside
`cargo deny list`?) and reachability (is it inside `cargo tree -e no-dev`?), and
the interesting cases are the ones where a crate is locked at two versions and
the axes disagree between them. The stub `cargo` answers `tree` differently for
`--target all` than for the host triple, which is what lets a case model a
dependency that ships only to Windows consumers.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_SCRIPT = HERE / "check-dev-dependency-licenses.py"
BEGIN = "<!-- BEGIN GENERATED: dev-dependency-licenses -->"
END = "<!-- END GENERATED: dev-dependency-licenses -->"

# Answers `cargo deny list` and `cargo tree`, and nothing else. `tree` reads a
# different file when the target is `all`, so a case can hold a crate version
# reachable on one target and invisible on the host — the shape the real
# `--target all` fix exists to stop hiding.
#
# It also asserts the flags it is handed. A stub that accepts whatever it is
# given is not a fixture, it is a rubber stamp: every case here passes against a
# script that has dropped `-e no-dev` or `--prefix none`, because the stub would
# answer identically. Those two flags are what make the command mean "the
# shipping graph" and "one crate per line" rather than something else entirely,
# so the stub refuses an invocation missing them. `--target` is deliberately not
# required — dropping it is the negative control for the fix.
STUB_CARGO = """#!/usr/bin/env bash
set -u
sub="${1:-}"
shift || true
joined=" $* "

require() {
  case "$joined" in
    *"$1"*) ;;
    *) echo "stub cargo: '$sub' invocation lacks '$1'; this fixture models only \
the documented command, and answering a different one would be a rubber stamp" >&2
       exit 127 ;;
  esac
}

target=host
while [ $# -gt 0 ]; do
  case "$1" in
    --target) target="${2:-}"; shift; shift || true ;;
    --target=*) target="${1#--target=}"; shift ;;
    *) shift ;;
  esac
done

case "$sub" in
  deny)
    require " list "
    cat "$FIXTURE/deny.out" ;;
  tree)
    require " -e no-dev "
    require " --workspace "
    require " --prefix none "
    require " --format {p} "
    if [ "$target" = all ]; then cat "$FIXTURE/tree.all.out"
    else cat "$FIXTURE/tree.host.out"; fi ;;
  *) echo "stub cargo: unexpected subcommand '$sub'" >&2; exit 127 ;;
esac
exit 0
"""


@dataclass
class Case:
    """One fixture run.

    `covered` is what `cargo deny list` reports. `tree_host` and `tree_all` are
    what `cargo tree` prints for the host triple and for `--target all`; a case
    that differs between them is modelling target-gated reachability.

    The fixed script only ever asks for `--target all`, so `tree_host` is read
    only on a `--script` run against a copy that has had the flag reverted. That
    is the point of carrying both: the host answer is the wrong answer, and a
    case proves the fix by showing what happens when the script gets it.

    `rows` pins the generated table and `reaches` pins the generated reachable
    set. Both are needed. The table is derived from the lockfile and the deny
    output; the reachable set is derived from `cargo tree`, and without an
    assertion on it a miscount of the tree output is invisible — the round trip
    compares two runs of the same parser, so a systematic error is
    self-consistent and passes.
    """

    name: str
    why: str
    locked: list[str]
    covered: list[str]
    tree_host: list[str]
    tree_all: list[str]
    exit_code: int
    stderr_has: list[str] = field(default_factory=list)
    stdout_has: list[str] = field(default_factory=list)
    rows: list[str] | None = None
    reaches: list[str] | None = None
    corrupt_receipt: bool = False


def deny_output(covered: list[str]) -> str:
    """`cargo deny list`'s shape: `LICENCE (n): name@version, name@version`."""
    return f"MIT ({len(covered)}): {', '.join(covered)}\n" if covered else "\n"


def tree_output(crates: list[str]) -> str:
    """`cargo tree --prefix none --format '{p}'`'s shape, including the parts a
    naive parser trips on.

    Real output is not one bare `name vX.Y.Z` per line. A workspace member
    carries its manifest path, a proc-macro carries a `(proc-macro)` marker, an
    already-printed subtree is elided to `(*)` rather than repeated, and roots
    are separated by a blank line. Which fixture crate carries which decoration
    is arbitrary — the point is that the parser must take the first two
    whitespace fields and ignore the rest, and must fold the `(*)` echo back
    onto the entry it repeats rather than counting it twice.
    """
    if not crates:
        return ""
    lines = []
    for index, crate in enumerate(crates):
        name, _, version = crate.rpartition("@")
        if index == 0:
            lines.append(f"{name} v{version} (/fixture/crates/{name})")
        elif index == 1:
            lines.append(f"{name} v{version} (proc-macro)")
        else:
            lines.append(f"{name} v{version}")
    name, _, version = crates[-1].rpartition("@")
    lines += ["", f"{name} v{version} (*)"]
    return "".join(f"{line}\n" for line in lines)


def build_fixture(root: Path, case: Case, script: Path) -> None:
    (root / "scripts").mkdir(parents=True)
    shutil.copy(script, root / "scripts" / "check-dev-dependency-licenses.py")

    packages = "\n".join(
        f'[[package]]\nname = "{crate.rpartition("@")[0]}"\n'
        f'version = "{crate.rpartition("@")[2]}"\n'
        for crate in case.locked
    )
    (root / "Cargo.lock").write_text(f"version = 4\n\n{packages}")

    (root / "docs").mkdir()
    (root / "docs" / "dev-dependency-licenses.md").write_text(
        f"# fixture\n{BEGIN}\n{END}\n"
    )

    registry = root / "cargo-home" / "registry" / "src" / "idx"
    for crate in case.locked:
        name, _, version = crate.rpartition("@")
        package = registry / f"{name}-{version}"
        package.mkdir(parents=True, exist_ok=True)
        (package / "Cargo.toml").write_text(
            f'[package]\nname = "{name}"\nversion = "{version}"\nlicense = "MIT"\n'
        )
        (package / "LICENSE-MIT").write_text("MIT fixture text\n")

    (root / "deny.out").write_text(deny_output(case.covered))
    (root / "tree.host.out").write_text(tree_output(case.tree_host))
    (root / "tree.all.out").write_text(tree_output(case.tree_all))

    (root / "bin").mkdir()
    cargo = root / "bin" / "cargo"
    cargo.write_text(STUB_CARGO)
    cargo.chmod(0o755)


def run(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{root / 'bin'}{os.pathsep}{env['PATH']}"
    env["CARGO_HOME"] = str(root / "cargo-home")
    env["FIXTURE"] = str(root)
    # Bytecode in the fixture is harmless but the repo tracked a .pyc once
    # already; keep the habit of not producing any.
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, str(root / "scripts" / "check-dev-dependency-licenses.py"),
         *args],
        capture_output=True, text=True, check=False, env=env,
    )


CASES = [
    Case(
        name="receipt-round-trips",
        why="--write then verify is clean; the baseline the other cases move off",
        locked=["foo@1.0.0", "bar@1.0.0"],
        covered=["bar@1.0.0"],
        tree_host=["bar@1.0.0"],
        tree_all=["bar@1.0.0"],
        exit_code=0,
        stdout_has=["ok:"],
        rows=["foo@1.0.0"],
        reaches=["bar@1.0.0"],
    ),
    Case(
        name="receipt-drift-is-loud",
        why="a receipt edited away from the lockfile exits non-zero, not silently",
        locked=["foo@1.0.0", "bar@1.0.0"],
        covered=["bar@1.0.0"],
        tree_host=["bar@1.0.0"],
        tree_all=["bar@1.0.0"],
        exit_code=1,
        stderr_has=["is stale"],
        corrupt_receipt=True,
    ),
    Case(
        name="split-coverage-uncovered-version-still-listed",
        why="a covered version must not vouch for an uncovered one of the same crate",
        locked=["foo@1.0.0", "foo@2.0.0", "bar@1.0.0"],
        covered=["foo@2.0.0", "bar@1.0.0"],
        tree_host=["bar@1.0.0"],
        tree_all=["bar@1.0.0"],
        exit_code=0,
        rows=["foo@1.0.0"],
        reaches=["bar@1.0.0"],
    ),
    Case(
        name="empty-tree-parse-does-not-vacuously-clear",
        why="an empty reachable set satisfies does-not-ship for every row at once",
        locked=["foo@1.0.0", "bar@1.0.0"],
        covered=["bar@1.0.0"],
        tree_host=[],
        tree_all=[],
        exit_code=2,
        stderr_has=["parsed to nothing"],
    ),
    Case(
        name="uncovered-and-exactly-reachable-must-fail",
        why=(
            "foo@1.0.0 is outside cargo-deny's traversal and reachable without dev "
            "edges on a non-host target. Host-scoped, cargo tree cannot see it and "
            "the receipt clears it — the fail-open --target all closes."
        ),
        locked=["foo@1.0.0", "foo@2.0.0", "bar@1.0.0"],
        covered=["foo@2.0.0", "bar@1.0.0"],
        tree_host=["bar@1.0.0", "foo@2.0.0"],
        tree_all=["bar@1.0.0", "foo@2.0.0", "foo@1.0.0"],
        exit_code=2,
        stderr_has=["non-dev dependency graph", "foo@1.0.0"],
    ),
    Case(
        name="uncovered-with-other-version-reachable-must-pass",
        why=(
            "the same lockfile with only the *covered* version reachable. The "
            "does-not-ship assertion is keyed by name@version, so foo@2.0.0 "
            "shipping says nothing about foo@1.0.0, which gets a row instead of "
            "a failure."
        ),
        locked=["foo@1.0.0", "foo@2.0.0", "bar@1.0.0"],
        covered=["foo@2.0.0", "bar@1.0.0"],
        tree_host=["bar@1.0.0", "foo@2.0.0"],
        tree_all=["bar@1.0.0", "foo@2.0.0"],
        exit_code=0,
        rows=["foo@1.0.0"],
        reaches=["bar@1.0.0", "foo@2.0.0"],
    ),
    Case(
        name="empty-deny-list-does-not-read-as-nothing-covered",
        why=(
            "an unparsed `cargo deny list` makes every locked crate look "
            "uncovered, which inflates the table instead of emptying it and so "
            "does not announce itself the way an empty table would"
        ),
        locked=["foo@1.0.0"],
        covered=[],
        tree_host=["foo@1.0.0"],
        tree_all=["foo@1.0.0"],
        exit_code=2,
        stderr_has=["produced no crates"],
    ),
]


def check(case: Case, script: Path) -> list[str]:
    failures = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "repo"
        build_fixture(root, case, script)
        receipt = root / "docs" / "dev-dependency-licenses.md"

        first = run(root, "--write")
        if case.corrupt_receipt:
            if first.returncode != 0:
                return [
                    f"{case.name}: setup --write exited {first.returncode}, "
                    f"expected 0\n{first.stderr}"
                ]
            before = receipt.read_text()
            after = before.replace("| `foo` |", "| `nope` |")
            if after == before:
                return [
                    f"{case.name}: no `foo` row to edit, so nothing was made "
                    "stale and the case would pass without testing anything"
                ]
            receipt.write_text(after)
            result = run(root)
        elif case.exit_code == 0:
            # A clean case has to survive the round trip, not just the write.
            if first.returncode != 0:
                return [
                    f"{case.name}: --write exited {first.returncode}, expected 0\n"
                    f"{first.stderr}"
                ]
            result = run(root)
        else:
            result = first

        if result.returncode != case.exit_code:
            failures.append(
                f"{case.name}: exit {result.returncode}, expected {case.exit_code}\n"
                f"  stdout: {result.stdout.strip()[:400]}\n"
                f"  stderr: {result.stderr.strip()[:400]}"
            )
        for fragment in case.stderr_has:
            if fragment not in result.stderr:
                failures.append(
                    f"{case.name}: stderr lacks {fragment!r}\n"
                    f"  stderr: {result.stderr.strip()[:400]}"
                )
        for fragment in case.stdout_has:
            if fragment not in result.stdout:
                failures.append(
                    f"{case.name}: stdout lacks {fragment!r}\n"
                    f"  stdout: {result.stdout.strip()[:400]}"
                )
        if case.rows is not None:
            body = receipt.read_text()
            generated = body.split(BEGIN, 1)[1].split(END, 1)[0]
            # Name *and* version. A row naming the right crate at the wrong
            # version is the exact defect the version keying exists to stop, so
            # comparing names alone would let it through.
            got = [
                f"{line.split('|')[1].strip().strip('`')}@"
                f"{line.split('|')[2].strip()}"
                for line in generated.splitlines()
                if line.startswith("| `")
            ]
            if got != case.rows:
                failures.append(f"{case.name}: table rows {got}, expected {case.rows}")
        if case.reaches is not None:
            # The generated sentence is wrapped, so compare on collapsed
            # whitespace. Both the count and the rendered list are pinned: the
            # count catches a `(*)` echo counted twice, the list catches the
            # wrong crates being counted the right number of times.
            body = " ".join(receipt.read_text().split())
            want_count = f"reaches {len(case.reaches)} crate versions"
            want_list = ", ".join(
                f"`{crate}`"
                for crate in sorted(case.reaches, key=lambda c: c.rpartition("@"))
            )
            if want_count not in body:
                failures.append(
                    f"{case.name}: receipt does not say {want_count!r}; the "
                    "reachable set was miscounted"
                )
            if want_list not in body:
                failures.append(
                    f"{case.name}: receipt does not name the reachable set as "
                    f"{want_list}"
                )
    return failures


def parse_args(argv: list[str]) -> Path:
    """Resolves `--script`, and rejects anything it does not understand.

    Silently ignoring an unrecognised argument is the one failure this tool
    cannot afford. `--script=PATH` skipped by a parser that only matches the
    space-separated form runs the whole suite against the *unmutated* script and
    prints `6/6 cases passed` — a green result from a run that never touched the
    mutant, in the tool built to stop exactly that kind of green.
    """
    rest = list(argv)
    script = DEFAULT_SCRIPT
    while rest:
        arg = rest.pop(0)
        if arg == "--script":
            if not rest:
                raise SystemExit("error: --script needs a path")
            script = Path(rest.pop(0))
        elif arg.startswith("--script="):
            script = Path(arg.partition("=")[2])
        else:
            raise SystemExit(
                f"error: unrecognised argument {arg!r}. Refusing to run rather "
                "than report a pass for a script you did not name."
            )
    return script.resolve()


def main() -> int:
    script = parse_args(sys.argv[1:])
    if not script.is_file():
        print(f"error: no script at {script}", file=sys.stderr)
        return 2

    print(f"testing {script}")
    failed = 0
    reports = []
    for case in CASES:
        found = check(case, script)
        failed += bool(found)
        print(f"  {'FAIL' if found else 'ok  '}  {case.name}")
        if found:
            reports.append(f"{case.name}\n  exists because: {case.why}\n"
                           + "\n".join(f"  {line}" for line in found))
    for report in reports:
        print(f"\n{report}", file=sys.stderr)
    print(f"\n{len(CASES) - failed}/{len(CASES)} cases passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
