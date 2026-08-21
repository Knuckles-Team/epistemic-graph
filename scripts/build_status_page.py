#!/usr/bin/env python3
"""Regenerate the honesty-first status page (the "Codex") at docs/status.md.

Extends this repo's existing "Honesty first" framing (docs/index.md) into a
single, generated page that other pages can link to instead of restating a
number or a legend. Mirrors agent-utilities' `scripts/build_status_page.py`
(same page shape, same vocabulary) but is adapted to what this repo's own
sources actually carry — see the per-table notes below for exactly which
schema drove which number.

Sources (never hand-typed):

* ``docs/capabilities.generated.md`` — the machine-checked, compiler-enforced
  method ledger (CONCEPT:EG-P0-1). It is authoritative for per-method
  mutates/durability/authz facts, but it has NO status column (every listed
  method already exists in the exhaustive `MethodPolicy` match by
  construction) -- so it contributes only the total method count, as a
  cross-check figure, not a status breakdown.
* ``docs/capabilities.md`` -- the hand-curated, but machine-parseable,
  operation-by-operation truth table: every row's ``| Status |`` cell is one
  of the three emoji this page's vocabulary already uses (✅/🔶/🗺). This is
  therefore the actual source of the LIVE/BUILDING/ROADMAP capability counts
  -- capabilities.generated.md cannot supply them because it carries no
  status field.
* ``docs/concept_reservations.yaml`` -- the concept-ID reservation ledger.
  Entries with ``status: reserved`` count as RESERVED.
* Every ``CONCEPT:<ID>`` marker in tracked ``*.rs``/``*.py`` source -- swept
  and bucketed by pillar prefix (the part before the first ``.``, e.g.
  ``EG-KG``, ``AU-KG``) for the "concepts by pillar" table, since this repo
  (unlike agent-utilities) has no generated `concepts.yaml` registry.

Dependency-free by design, matching this repo's other advisory-gate scripts
(see ``scripts/check_documentation_contract.py``'s own docstring): the
reservations ledger is flow-style YAML (one ``- {k: v, ...}`` dict per line)
and is parsed with a small dedicated parser rather than a ``pyyaml`` import,
so this script needs no environment setup before it runs in CI.

Usage::

    python scripts/build_status_page.py --write
    python scripts/build_status_page.py --check
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CAPABILITIES_MD = ROOT / "docs" / "capabilities.md"
CAPABILITIES_GENERATED_MD = ROOT / "docs" / "capabilities.generated.md"
RESERVATIONS_PATH = ROOT / "docs" / "concept_reservations.yaml"
STATUS_PATH = ROOT / "docs" / "status.md"

CONCEPT_MARKER_RE = re.compile(r"CONCEPT:([A-Za-z]{2,5}-[A-Za-z0-9]+)\.[A-Za-z0-9.-]+")
STATUS_CELL_RE = re.compile(r"\|\s*(✅|🔶|🗺)")

# Structural pillar -> owning doc subtree -> primary enforcing CI/pre-commit
# gate. Not named humans -- see docs/status.md's "Domain ownership" section.
PILLAR_LABEL = {
    "EG-AHE": "harness-facing engine concepts",
    "EG-KG": "knowledge-graph engine core",
    "EG-ORCH": "routing / orchestration",
    "EG-OS": "deployment",
    "AU-KG": "agent-utilities KG concepts exercised against this engine",
    "AU-ECO": "agent-utilities ecosystem concepts exercised against this engine",
    "AU-AHE": "agent-utilities harness concepts exercised against this engine",
    "AU-ORCH": "agent-utilities orchestration concepts exercised against this engine",
    "AU-OS": "agent-utilities OS concepts exercised against this engine",
}
PILLAR_SUBTREE = {
    "EG-AHE": "docs/architecture/",
    "EG-KG": "docs/interfaces/ + docs/architecture/engine.md",
    "EG-ORCH": "docs/architecture/",
    "EG-OS": "docs/deploy/ + docs/deployment.md",
    "AU-KG": "agent-utilities: docs/pillars/2_epistemic_knowledge_graph/",
    "AU-ECO": "agent-utilities: docs/pillars/4_ecosystem_peripherals.md",
    "AU-AHE": "agent-utilities: docs/pillars/3_agentic_harness_engineering.md",
    "AU-ORCH": "agent-utilities: docs/pillars/1_graph_orchestration.md",
    "AU-OS": "agent-utilities: docs/pillars/5_agent_os_infrastructure.md",
}
PILLAR_GATE = {
    "EG-AHE": "`scripts/check_documentation_contract.py` (generated-ledger regen check)",
    "EG-KG": "`scripts/check_documentation_contract.py` (generated-ledger regen check)",
    "EG-ORCH": "`scripts/check_documentation_contract.py` (generated-ledger regen check)",
    "EG-OS": "`scripts/check_documentation_contract.py` (generated-ledger regen check)",
    "AU-KG": "agent-utilities' `scripts/check_concepts.py`",
    "AU-ECO": "agent-utilities' `scripts/check_concepts.py`",
    "AU-AHE": "agent-utilities' `scripts/check_concepts.py`",
    "AU-ORCH": "agent-utilities' `scripts/check_concepts.py`",
    "AU-OS": "agent-utilities' `scripts/check_concepts.py`",
}

HONESTY_FRAMING = (
    "**Honesty first.** Every capability on this page is tracked "
    "operation-by-operation, verified against the source, not against "
    "intent — the numbers below are computed from `docs/capabilities.md`, "
    "`docs/capabilities.generated.md`, and `docs/concept_reservations.yaml` "
    "at generation time, never hand-typed."
)

VOCAB_ROWS = [
    ("RESERVED", "", "A concept ID is allocated in the reservations ledger; no code implements it yet."),
    ("BUILDING", "🔶", "Partially built or actively being added; the unsupported part errors honestly."),
    ("LIVE", "✅", "Implemented and covered by tests."),
    ("ROADMAP", "🗺", "Designed, not yet built."),
    ("RETIRED", "", "Formerly live, intentionally removed."),
]


def _tracked_source_files() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "--", "*.rs", "*.py"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        return [ROOT / line for line in out.splitlines() if line]
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return [p for p in ROOT.rglob("*.rs") if "/target/" not in p.as_posix()] + list(
            ROOT.rglob("*.py")
        )


def _concept_pillar_counts() -> dict[str, int]:
    """Unique CONCEPT:<ID> markers per pillar prefix (never raw occurrences).

    The same concept ID is legitimately cited many times across a large file
    (a comment on every match arm/call site that implements it) -- counting
    occurrences would inflate the total by an order of magnitude and measure
    "how chatty the comments are" rather than "how many concepts exist".
    Dedupe by full ID first, then bucket by pillar prefix.
    """
    ids_by_pillar: dict[str, set[str]] = {}
    for path in _tracked_source_files():
        if "/target/" in path.as_posix() or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for match in CONCEPT_MARKER_RE.finditer(text):
            pillar = match.group(1)
            full_id = match.group(0).removeprefix("CONCEPT:")
            ids_by_pillar.setdefault(pillar, set()).add(full_id)
    return {pillar: len(ids) for pillar, ids in ids_by_pillar.items()}


def _capability_status_counts() -> dict[str, int]:
    text = CAPABILITIES_MD.read_text(encoding="utf-8")
    # Skip the legend block at the top of the file (its bullets carry the
    # same emoji but are not table rows) -- start counting from the first
    # Markdown table header.
    body = text.split("## Native governed external ingestion", 1)
    scan_text = body[1] if len(body) == 2 else text
    counts = {"✅": 0, "🔶": 0, "🗺": 0}
    for match in STATUS_CELL_RE.finditer(scan_text):
        counts[match.group(1)] += 1
    return counts


def _generated_method_count() -> int:
    text = CAPABILITIES_GENERATED_MD.read_text(encoding="utf-8")
    return len(re.findall(r"^\| `[A-Za-z]", text, flags=re.MULTILINE))


_FLOW_FIELD_RE = re.compile(r"(\w+):\s*(?:'([^']*)'|([^,}]*))")


def _parse_flow_dict_line(line: str) -> dict[str, str]:
    """Parse one ``- {k: v, k2: 'v2', ...}`` reservation-ledger line.

    Dependency-free stand-in for ``yaml.safe_load`` on this repo's flow-style
    ledger: every entry is one line, values are either bare (no comma/brace)
    or single-quoted (ISO timestamps). Good enough for the fields this page
    reads (``id``/``slug``/``pillar``/``status``) without a ``pyyaml`` import.
    """
    body = line.strip()
    if body.startswith("- {"):
        body = body[3:]
    if body.endswith("}"):
        body = body[:-1]
    fields: dict[str, str] = {}
    for match in _FLOW_FIELD_RE.finditer(body):
        key = match.group(1)
        value = match.group(2) if match.group(2) is not None else match.group(3)
        fields[key] = value.strip()
    return fields


def _reserved_pillar_counts() -> tuple[dict[str, int], int]:
    if not RESERVATIONS_PATH.is_file():
        return {}, 0
    counts: dict[str, int] = {}
    total = 0
    for line in RESERVATIONS_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip().startswith("- {"):
            continue
        entry = _parse_flow_dict_line(line)
        if entry.get("status") != "reserved":
            continue
        if "slug" not in entry or "pillar" not in entry:
            continue
        total += 1
        pillar = f"{entry['slug']}-{entry['pillar']}"
        counts[pillar] = counts.get(pillar, 0) + 1
    return counts, total


def render() -> str:
    raw_concept_counts = _concept_pillar_counts()
    # Restrict the table to the canonical 9-pillar taxonomy this page's
    # "Domain ownership" section documents (AU-AHE/ECO/KG/ORCH/OS,
    # EG-AHE/KG/ORCH/OS); a real, older generation of `CONCEPT:` markers
    # predates that taxonomy (e.g. `KG-2.55`, `ORCH-1.45`, `OS-5.14`,
    # `AHE-3.x`) and still exists in source. Report their combined count
    # honestly rather than expanding the table with prefixes this page does
    # not otherwise define an owning subtree/gate for.
    concept_counts = {p: n for p, n in raw_concept_counts.items() if p in PILLAR_LABEL}
    legacy_marker_total = sum(n for p, n in raw_concept_counts.items() if p not in PILLAR_LABEL)
    reserved_counts, total_reserved = _reserved_pillar_counts()
    cap_counts = _capability_status_counts()
    method_total = _generated_method_count()
    total_concepts = sum(concept_counts.values())
    pillars = sorted(set(concept_counts) | set(reserved_counts) | set(PILLAR_LABEL))

    lines: list[str] = []
    lines.append("# Status — the Codex")
    lines.append("")
    lines.append(
        "> **Generated — do not edit by hand.** Produced by "
        "`scripts/build_status_page.py` from `docs/capabilities.md`, "
        "`docs/capabilities.generated.md`, and "
        "`docs/concept_reservations.yaml`. See "
        '"How this page stays honest" at the bottom.'
    )
    lines.append("")
    lines.append(HONESTY_FRAMING)
    lines.append("")

    lines.append("## Capabilities by status")
    lines.append("")
    lines.append(
        f"**{cap_counts['✅']} LIVE**, **{cap_counts['🔶']} BUILDING**, and "
        f"**{cap_counts['🗺']} ROADMAP** operations tracked in "
        "`docs/capabilities.md`'s operation-by-operation truth table "
        f"(cross-check: the machine-checked `docs/capabilities.generated.md` "
        f"ledger currently enumerates **{method_total} wire methods** — a "
        "different, finer granularity, since several generated-ledger "
        "methods can compose into one `capabilities.md` row; the two are "
        "not expected to be equal, but a `capabilities.generated.md` that "
        "shrinks while `capabilities.md` does not is a drift signal)."
    )
    lines.append("")
    lines.append("| Status | Count |")
    lines.append("|:------|---:|")
    lines.append(f"| LIVE ✅ | {cap_counts['✅']} |")
    lines.append(f"| BUILDING 🔶 | {cap_counts['🔶']} |")
    lines.append(f"| ROADMAP 🗺 | {cap_counts['🗺']} |")
    lines.append(f"| RESERVED | {total_reserved} |")
    lines.append("")

    lines.append("## Concepts by pillar")
    lines.append("")
    lines.append(
        f"**{total_concepts} LIVE** unique `CONCEPT:<ID>` markers (swept "
        "from every tracked `*.rs`/`*.py` file, deduplicated by full ID, "
        "then bucketed by pillar prefix — the same concept is legitimately "
        "cited at every call site that implements it, so this counts "
        "distinct IDs, never raw occurrences) and "
        f"**{total_reserved} RESERVED** concept IDs (open, unexpired entries "
        "in `docs/concept_reservations.yaml`) across "
        f"**{len(pillars)} pillars**. Unlike agent-utilities, this repo has "
        "no generated `concepts.yaml` registry — the marker sweep below is "
        "this page's own generated source, not a restatement of one."
    )
    lines.append("")
    lines.append("| Pillar | LIVE ✅ | RESERVED |")
    lines.append("|:------|---:|---:|")
    for pillar in pillars:
        lines.append(
            f"| **{pillar}** — {PILLAR_LABEL.get(pillar, pillar)} "
            f"| {concept_counts.get(pillar, 0)} | {reserved_counts.get(pillar, 0)} |"
        )
    lines.append(
        f"| **Total** | {total_concepts} | {total_reserved} |"
    )
    lines.append("")
    if legacy_marker_total:
        lines.append(
            f"> {legacy_marker_total} additional unique `CONCEPT:<ID>` "
            "markers use a pre-migration prefix outside the current "
            "9-pillar taxonomy (e.g. `KG-2.*`, `ORCH-1.*`, `OS-5.*`, "
            "`AHE-3.*`) and are not broken out above — this page defines an "
            "owning subtree/gate only for the pillars in "
            "\"Domain ownership\" below. See `docs/concepts.md` for those "
            "markers in prose form."
        )
        lines.append("")

    lines.append("## Status vocabulary")
    lines.append("")
    lines.append(
        "Defined once, here — every other table in this repo's docs "
        "(README capability tables, `docs/capabilities.md`) should link to "
        "this section instead of restating or omitting it. Existing emoji "
        "are the rendering of this vocabulary, not a separate scheme."
    )
    lines.append("")
    lines.append("| Status | Emoji | Meaning |")
    lines.append("|:------|:---:|:------|")
    for name, emoji, meaning in VOCAB_ROWS:
        lines.append(f"| `{name}` | {emoji} | {meaning} |")
    lines.append("")
    lines.append(
        "Lifecycle: `RESERVED` → `BUILDING` → `LIVE` → (optionally) "
        "`RETIRED`, with `ROADMAP` as the not-yet-reserved intent stage."
    )
    lines.append("")

    lines.append("## Domain ownership")
    lines.append("")
    lines.append(
        "Structural ownership — which doc subtree and which CI/pre-commit "
        "gate is authoritative for each pillar. Not named humans. `AU-*` "
        "pillar concepts appear in this repo's own source because "
        "agent-utilities code that exercises this engine tags itself with "
        "the concept it is exercising (cross-repo concept federation) — the "
        "owning subtree for those is in the agent-utilities repo, not here."
    )
    lines.append("")
    lines.append("| Pillar | Owning doc subtree | Primary gate |")
    lines.append("|:------|:------|:------|")
    for pillar in pillars:
        subtree = PILLAR_SUBTREE.get(pillar, "—")
        gate = PILLAR_GATE.get(pillar, "`scripts/check_documentation_contract.py`")
        lines.append(f"| **{pillar}** | `{subtree}` | {gate} |")
    lines.append("")

    lines.append("## How this page stays honest")
    lines.append("")
    lines.append(
        "This page is produced by `scripts/build_status_page.py` from "
        "`docs/capabilities.md`, `docs/capabilities.generated.md`, and "
        "`docs/concept_reservations.yaml` — never hand-typed. Regenerate it "
        "with:"
    )
    lines.append("")
    lines.append("```bash")
    lines.append("python scripts/build_status_page.py --write")
    lines.append("```")
    lines.append("")
    lines.append(
        "`scripts/check_status_page.py` is wired into `.github/workflows/"
        "advisory.yml` (report-only, `continue-on-error: true`, matching "
        "this repo's existing approved-vs-enforced convention) and fails "
        "loudly — without blocking a release — the moment this file drifts "
        "from its sources. Run it locally with:"
    )
    lines.append("")
    lines.append("```bash")
    lines.append("python scripts/check_status_page.py")
    lines.append("```")
    lines.append("")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true", help="write docs/status.md in place")
    group.add_argument("--check", action="store_true", help="exit non-zero if docs/status.md is stale")
    args = parser.parse_args()

    rendered = render()

    if args.write:
        STATUS_PATH.write_text(rendered, encoding="utf-8")
        print(f"wrote {STATUS_PATH.relative_to(ROOT)}")
        return 0

    if not STATUS_PATH.is_file():
        print("docs/status.md is missing — run --write first.", file=sys.stderr)
        return 1
    current = STATUS_PATH.read_text(encoding="utf-8")
    if current != rendered:
        print(
            "docs/status.md is stale relative to docs/capabilities.md / "
            "docs/capabilities.generated.md / docs/concept_reservations.yaml. "
            "Run: python scripts/build_status_page.py --write",
            file=sys.stderr,
        )
        return 1
    print("docs/status.md is up to date.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
