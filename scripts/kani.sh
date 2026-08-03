#!/usr/bin/env bash
#
# Run the Kani proof harnesses for the leased-work transition kernel.
#
# The invocation is a script rather than a line of prose because the obvious
# thing to type does not work:
#
#     cargo kani                      # FAILS at the workspace root
#     cargo kani -p interlockutor-kani  # what this script runs
#
# Kani 0.67 pins rustc 1.93, and the product crate's MSRV is 1.95. A bare
# `cargo kani` at the root tries to build the whole workspace under Kani's
# pinned toolchain and fails on the version floor. `crates/interlockutor-kani`
# exists precisely to avoid that: it declares rust-version 1.93, supplies the
# two primitive domain types the kernel imports, and then compiles
# `crates/interlockutor/src/kernel.rs` unchanged via `#[path]`. There is one
# transition implementation, not a model that can drift from it.
#
# The proofs are not part of CI — adding them is tracked separately — so this is
# the only place the working invocation is enforced rather than described.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cargo-kani >/dev/null 2>&1; then
    cat >&2 <<'EOF'
error: cargo-kani is not installed.

    cargo install --locked kani-verifier && cargo kani setup

The proofs are optional and are not run in CI; the crate builds and tests
without them.
EOF
    exit 127
fi

echo "kani: $(cargo kani --version)"
exec cargo kani --package=interlockutor-kani "$@"
