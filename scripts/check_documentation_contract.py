#!/usr/bin/env python3
"""Fail when the documented main-artifact contract drifts from Cargo/source.

This gate is intentionally dependency-free.  It checks only release-defining facts
that have repeatedly drifted across README, AGENTS, MkDocs, and the generated method
ledger; prose that is not a machine-owned contract remains outside its scope.
"""

from __future__ import annotations

import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FULL_FEATURES = frozenset(
    {
        "epistemic",
        "epistemic-redaction",
        "epistemic-tms",
        "epistemic-causal",
        "evidence-graph",
        "jobs",
        "program-optimization",
        "modality-serving",
        "knowledge-batch",
        "numeric",
        "otel",
        "security",
        "server-tls",
    }
)

AUTHORITATIVE_PAGES = (
    "capabilities.generated.md",
    "architecture/request_authority.md",
    "architecture/mutation_batch.md",
    "architecture/change_envelope.md",
    "architecture/canonical-property-schema.md",
    "architecture/modality_serving.md",
    "architecture/native-program-optimization.md",
    "architecture/distributed-analytics-reasoning.md",
    "architecture/analytics_program.md",
    "architecture/numeric_kernel.md",
    "architecture/hot-path-complexity.md",
)

GENERATED_METHODS = (
    "ClaimWorkItem",
    "ApplyChangeEnvelope",
    "ServedModality",
    "KnowledgeStream",
)

STALE_CLAIMS: dict[str, tuple[str, ...]] = {
    "AGENTS.md": (
        "not folded into any serving tier",
        "default `full` build carries none",
        "Opt-in, not folded into `full`",
        "jobs`, feature `jobs`, off by default",
        "Spatial/R-tree index pushdown is NOT wired",
    ),
    "docs/capabilities.md": (
        "epistemic-tms` (opt-in, HEAVY — not in `full`)",
        "epistemic-causal` (opt-in, HEAVY — not in `full`)",
        "`epistemic-tms`/`epistemic-causal` remain opt-in",
        "dataset-handle",
    ),
    "docs/architecture/epistemic-os-hardening.md": (
        "dataset-handle",
    ),
}


def fail(message: str) -> None:
    raise AssertionError(message)


def text(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        fail(f"required documentation file is missing: {relative}")
    return path.read_text(encoding="utf-8")


def check_features() -> None:
    with (ROOT / "Cargo.toml").open("rb") as handle:
        cargo = tomllib.load(handle)
    features = cargo.get("features")
    if not isinstance(features, dict):
        fail("Cargo.toml has no [features] table")
    default = set(features.get("default", ()))
    full = set(features.get("full", ()))
    if "full" not in default:
        fail("the default artifact must include the full feature bundle")
    missing = sorted(REQUIRED_FULL_FEATURES - full)
    if missing:
        fail("full is missing release-defining features: " + ", ".join(missing))


def check_mkdocs() -> None:
    config = text("mkdocs.yml")
    for page in AUTHORITATIVE_PAGES:
        if page not in config:
            fail(f"mkdocs navigation omits authoritative page: {page}")
        if not (ROOT / "docs" / page).is_file():
            fail(f"mkdocs authoritative page does not exist: docs/{page}")


def check_generated_ledger() -> None:
    ledger = text("docs/capabilities.generated.md")
    for method in GENERATED_METHODS:
        if f"| `{method}` |" not in ledger:
            fail(
                "generated capability ledger is stale; missing method "
                f"{method!r}. Regenerate with `cargo run -p eg-capabilities --features jobs,knowledge-batch,modality-serving --bin gen_ledger`."
            )


def check_current_claims() -> None:
    for relative, stale_claims in STALE_CLAIMS.items():
        content = text(relative)
        for claim in stale_claims:
            if claim in content:
                fail(f"{relative} contains stale capability claim: {claim}")


def main() -> int:
    try:
        check_features()
        check_mkdocs()
        check_generated_ledger()
        check_current_claims()
    except (AssertionError, OSError, tomllib.TOMLDecodeError) as exc:
        print(f"documentation contract: FAIL: {exc}", file=sys.stderr)
        return 1
    print(
        "documentation contract: PASS "
        f"({len(REQUIRED_FULL_FEATURES)} full features, "
        f"{len(AUTHORITATIVE_PAGES)} authoritative pages, "
        f"{len(GENERATED_METHODS)} generated methods)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
