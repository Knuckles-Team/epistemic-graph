#!/usr/bin/env bash
# Plan 01 CI gate: prove PyO3 is fully excised — in SOURCE *and* in shipped
# BINARIES. A plain `grep -rn pyo3` is insufficient because a stale compiled
# `_epistemic_graph*.so` extension is a binary artifact that no source grep can
# see (Plan 01 review finding #1). This gate fails on either.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0

# 1. No PyO3 in Rust/Python source or build metadata.
if grep -rn --include='*.rs' --include='*.py' --include='*.toml' \
     -e 'pyo3' -e 'PyO3' -e '#\[pymethod' -e '#\[pyclass' \
     -e '#\[pymodule' -e 'allow_threads' \
     src epistemic_graph Cargo.toml pyproject.toml 2>/dev/null; then
  echo "FAIL: PyO3 reference found in source/metadata above." >&2
  fail=1
fi

# 2. No compiled PyO3 extension shipped in the package dir.
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

if [[ $fail -eq 0 ]]; then
  echo "OK: no PyO3 in source or binaries; maturin bindings=bin."
fi
exit $fail
