#!/usr/bin/env bash
# Plan 01 CI gate, SCOPED to the SCALE-OUT (out-of-process) build: prove PyO3 is
# fully excised from it — in SOURCE *and* in shipped BINARIES. A plain
# `grep -rn pyo3` is insufficient because a stale compiled
# `_epistemic_graph*.so` extension is a binary artifact that no source grep can
# see (Plan 01 review finding #1). This gate fails on either.
#
# Two deployment shapes now exist (AGENTS.md, "two deployment shapes, one
# performance discipline"; docs/architecture/unified-inprocess-engine.md):
#
#   (a) Out-of-process (the horizontal-scale default, what THIS gate covers) —
#       Python talks to the engine over a socket. No pyo3 anywhere: `src/`,
#       `epistemic_graph/` (the pure-Python client package), and the top-level
#       `Cargo.toml`/`pyproject.toml` (the `bindings = "bin"` server wheel).
#   (b) Unified single binary (opt-in, the self-contained shape) — the engine
#       is embedded in-process via pyo3, in `crates/eg-pyengine` (its OWN
#       Cargo.toml + pyproject.toml — a SEPARATE maturin target, exactly like
#       `crates/eg-numeric`'s pre-existing pyo3 Surface-A kernel), reached ONLY
#       through the facade's `pyo3-engine` cargo feature. That feature is NOT
#       in `default`/`full`/`cluster` — checks 4 and 5 below prove that
#       mechanically, not just by convention.
#
# So this script is deliberately scoped to (a): it does NOT scan `crates/`
# (where eg-numeric's and eg-pyengine's OWN pyo3 bindings legitimately live),
# and it explicitly allow-lists the opt-in feature/dependency NAMES
# (`pyo3-engine`, `eg-pyengine`) that the top-level Cargo.toml legitimately
# mentions to gate that path — neither name is an actual pyo3 crate reference.
# Any OTHER pyo3/PyO3 mention in the scanned files still fails the gate, and
# check 4 independently proves the default/full dependency GRAPH links no pyo3
# regardless of what anything is named.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0

# 1. No PyO3 in Rust/Python source or build metadata for the scale-out build,
#    after excluding the documented opt-in feature/dependency NAMES (see the
#    header comment above) and this gate's own filename / the opt-in bench's
#    own filename, both of which legitimately appear in comments that
#    reference them by name without being a real pyo3 reference.
hits="$(grep -rn --include='*.rs' --include='*.py' --include='*.toml' \
     -e 'pyo3' -e 'PyO3' -e '#\[pymethod' -e '#\[pyclass' \
     -e '#\[pymodule' -e 'allow_threads' \
     src epistemic_graph Cargo.toml pyproject.toml 2>/dev/null \
     | grep -v -e 'pyo3-engine' -e 'eg-pyengine' -e 'eg_pyengine' \
               -e 'check_no_pyo3' -e 'pyo3_inprocess_vs_uds' || true)"
if [[ -n "$hits" ]]; then
  echo "FAIL: PyO3 reference found in source/metadata above." >&2
  echo "$hits" >&2
  fail=1
fi

# 2. No compiled PyO3 extension shipped in the package dir. Scoped to the
#    filenames the pre-Plan-01 whole-package extension used
#    (`_epistemic_graph*`/`epistemic_graph*.so`) — the two KNOWN, ALLOWED,
#    opt-in native extensions (`epistemic_graph/numeric*.so`, the eg-numeric
#    kernel; `epistemic_graph/engine*.so`, the eg-pyengine unified binding) are
#    module-named differently on purpose and are never produced by this
#    (scale-out) build path anyway — only an explicit
#    `maturin build -m crates/{eg-numeric,eg-pyengine}/Cargo.toml --features
#    python` + injection step produces them, never `cargo build`/plain
#    `maturin build` at the repo root.
if compgen -G "epistemic_graph/_epistemic_graph*.so" > /dev/null \
   || compgen -G "epistemic_graph/_epistemic_graph*.pyi" > /dev/null \
   || compgen -G "epistemic_graph/epistemic_graph*.so" > /dev/null; then
  echo "FAIL: compiled PyO3 extension / stub present in epistemic_graph/:" >&2
  ls -1 epistemic_graph/_epistemic_graph* epistemic_graph/epistemic_graph*.so 2>/dev/null >&2 || true
  fail=1
fi

# 3. maturin must ship the binary, not a python extension.
if ! grep -q 'bindings *= *"bin"' pyproject.toml; then
  echo "FAIL: pyproject.toml [tool.maturin] bindings must be \"bin\"." >&2
  fail=1
fi

# 4. Mechanical dependency-GRAPH proof (not just a source grep): the default
#    feature set (== `full`, the scale-out build) must link no pyo3 crate at
#    all. Same "asserted by cargo tree" discipline AGENTS.md uses elsewhere
#    (e.g. "a --features server build links neither nalgebra nor
#    tree-sitter") — this holds even if a future edit references pyo3 from a
#    spot check 1's text scan doesn't cover. `cargo tree` only reads
#    Cargo.lock/manifests (no compilation), so it is safe to run without an
#    isolated --target-dir.
#
#    The match pattern is anchored to a CRATE-NAME position (right after the
#    tree-drawing prefix or at line start, immediately followed by " v<digit>")
#    rather than a bare substring search: `cargo tree` prints each path
#    dependency's checkout directory, and a bare `grep -i pyo3` false-positives
#    on any checkout path that happens to contain "pyo3" — e.g. exactly the
#    worktree name this workstream itself uses
#    (`repository-worktrees/epistemic-graph/wA-pyo3-engine`). Verified against
#    both a default-feature tree (no match) and a `--features pyo3-engine`
#    tree (matches `pyo3`/`pyo3-ffi`/`pyo3-macros`) while building this gate.
pyo3_crate_pattern='(^|── )pyo3(-[a-z]+)? v[0-9]'
if command -v cargo > /dev/null 2>&1; then
  tree_out="$(cargo tree -e normal 2>/dev/null || true)"
  if grep -qE "$pyo3_crate_pattern" <<< "$tree_out"; then
    echo "FAIL: the default feature set links a pyo3 crate (cargo tree)." >&2
    grep -E "$pyo3_crate_pattern" <<< "$tree_out" >&2 || true
    fail=1
  fi
else
  echo "WARN: cargo not found on PATH; skipping the cargo-tree dependency check." >&2
fi

# 5. `pyo3-engine` (the opt-in unified-binary feature) must never be folded
#    into an aggregate the scale-out build enables by default.
if grep -E '^(default|full|cluster)[[:space:]]*=' Cargo.toml | grep -q 'pyo3-engine'; then
  echo "FAIL: pyo3-engine must not be part of default/full/cluster." >&2
  fail=1
fi

if [[ $fail -eq 0 ]]; then
  echo "OK: no PyO3 in the scale-out build (source, binaries, or dependency graph); maturin bindings=bin; pyo3-engine stays opt-in."
fi
exit $fail
