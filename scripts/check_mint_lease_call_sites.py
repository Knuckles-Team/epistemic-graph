#!/usr/bin/env python3
"""Fail CI when `mint_policy_decision_lease` grows a second production call site.

GRAPH-POLICY-LEASE-CONTRACT.md §3 hardening (`MintAuthorization`,
`crates/eg-core/src/isolation.rs`) closes the caller-supplied-claims gap by
requiring a cryptographic capability token instead of a raw
`RequestContextClaims` reference. That control is only as good as the claim
that exactly one production code path ever calls
`IsolationLayer::mint_policy_decision_lease` — `authorize_and_route_knowledge_stream`
in `src/server/dispatch.rs`, which already holds the server auth secret at the
point it has verified claims in hand. A second production call site (a future
internal tool, an admin surface, a copy-pasted handler) would be a NEW place
that must independently get the `MintAuthorization` construction right, and
nothing else in the build would notice if it didn't. This gate is the notice.

Test-only call sites are expected and permitted (see `_is_test_only` below) —
this is a call-site-count gate, not a ban on exercising the mint path in
tests.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# The one and only permitted production call site.
ALLOWED_PRODUCTION_CALL_SITE = "src/server/dispatch.rs"

# A real method call: `<receiver>.mint_policy_decision_lease(`. This deliberately
# does NOT match the method's own `pub fn mint_policy_decision_lease(` definition
# (no leading `.`) nor a doc/prose reference like
# "[`IsolationLayer::mint_policy_decision_lease`]" (`::`, not `.`, and no `(`
# immediately after the identifier in that form).
_CALL_SITE = re.compile(r"\.mint_policy_decision_lease\s*\(")

# Directories this repo's own Rust sources live under (mirrors what `cargo
# check -p epistemic-graph` / `-p eg-core` actually compile — see
# `scripts/check_current_only_architecture.py` for the same read-the-shipped-
# tree convention this gate follows).
_SCAN_DIRS = ("src", "crates")

# Path-shape conventions this repo uses for test-only Rust source (mirrors
# what `#[cfg(test)]` actually gates in practice): a `tests.rs` leaf module
# (e.g. `src/server/handlers/knowledge_stream/tests.rs`, wired behind
# `#[cfg(test)] mod tests;` in its parent `mod.rs`), anything under a `tests/`
# directory (unit-test submodules or the top-level integration-test crate),
# or a `..._test(s).rs` / `test_...rs` leaf filename.
_TEST_ONLY_PATH = re.compile(
    r"(^|/)(tests?)/.*\.rs$"  # under a tests/ or test/ directory
    r"|(^|/)tests\.rs$"  # a `tests.rs` leaf module
    r"|(^|/)test_[^/]*\.rs$"  # test_*.rs
    r"|(^|/)[^/]*_tests?\.rs$"  # *_test.rs / *_tests.rs
)


def _is_test_only(relative_posix: str) -> bool:
    return bool(_TEST_ONLY_PATH.search(relative_posix))


def _is_commented_out(line: str) -> bool:
    stripped = line.strip()
    return stripped.startswith("//") or stripped.startswith("*")


def find_call_sites(root: Path) -> list[tuple[str, int]]:
    """Every real `.mint_policy_decision_lease(` call under `root`'s Rust sources.

    Returns `(path-relative-to-root-as-posix, 1-based line number)` pairs,
    sorted for deterministic output.
    """

    sites: list[tuple[str, int]] = []
    for scan_dir in _SCAN_DIRS:
        base = root / scan_dir
        if not base.is_dir():
            continue
        for rust_file in sorted(base.rglob("*.rs")):
            relative = rust_file.relative_to(root).as_posix()
            text = rust_file.read_text(encoding="utf-8")
            for line_no, line in enumerate(text.splitlines(), start=1):
                if _CALL_SITE.search(line) and not _is_commented_out(line):
                    sites.append((relative, line_no))
    return sorted(sites)


def check(root: Path) -> None:
    sites = find_call_sites(root)
    if not sites:
        raise SystemExit(
            "mint-lease call-site gate failed: found ZERO calls to "
            "mint_policy_decision_lease — either the scan paths drifted or the "
            "feature was deleted; update this gate deliberately if so, don't "
            "let it go quietly blind"
        )

    production = [(path, line) for path, line in sites if not _is_test_only(path)]
    test_only = [(path, line) for path, line in sites if _is_test_only(path)]

    if len(production) != 1:
        rendered = ", ".join(f"{path}:{line}" for path, line in production)
        raise SystemExit(
            "mint-lease call-site gate failed: expected exactly ONE production "
            f"call site ({ALLOWED_PRODUCTION_CALL_SITE}), found "
            f"{len(production)}: [{rendered or 'none'}]. A second production "
            "caller of mint_policy_decision_lease means a second place that must "
            "independently get MintAuthorization construction right — audit it "
            "before letting this pass."
        )

    (only_path, only_line) = production[0]
    if only_path != ALLOWED_PRODUCTION_CALL_SITE:
        raise SystemExit(
            "mint-lease call-site gate failed: the sole production call site "
            f"moved to {only_path}:{only_line}, expected "
            f"{ALLOWED_PRODUCTION_CALL_SITE}. Update ALLOWED_PRODUCTION_CALL_SITE "
            "only after confirming the new site legitimately holds the server "
            "auth secret and verified claims at the call — never to silently "
            "wave a relocation through."
        )

    print(
        "mint-lease call-site gate passed: "
        f"1 production call site ({only_path}:{only_line}), "
        f"{len(test_only)} test-only call site(s)"
    )


def main() -> None:
    check(ROOT)


if __name__ == "__main__":
    main()
