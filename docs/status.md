# Status — the Codex

> **Generated — do not edit by hand.** Produced by `scripts/build_status_page.py` from `docs/capabilities.md`, `docs/capabilities.generated.md`, and `docs/concept_reservations.yaml`. See "How this page stays honest" at the bottom.

**Honesty first.** Every capability on this page is tracked operation-by-operation, verified against the source, not against intent — the numbers below are computed from `docs/capabilities.md`, `docs/capabilities.generated.md`, and `docs/concept_reservations.yaml` at generation time, never hand-typed.

## Capabilities by status

**263 LIVE**, **0 BUILDING**, and **0 ROADMAP** operations tracked in `docs/capabilities.md`'s operation-by-operation truth table (cross-check: the machine-checked `docs/capabilities.generated.md` ledger currently enumerates **407 wire methods** — a different, finer granularity, since several generated-ledger methods can compose into one `capabilities.md` row; the two are not expected to be equal, but a `capabilities.generated.md` that shrinks while `capabilities.md` does not is a drift signal).

| Status | Count |
|:------|---:|
| LIVE ✅ | 263 |
| BUILDING 🔶 | 0 |
| ROADMAP 🗺 | 0 |
| RESERVED | 1 |

## Concepts by pillar

**655 LIVE** unique `CONCEPT:<ID>` markers (swept from every tracked `*.rs`/`*.py` file, deduplicated by full ID, then bucketed by pillar prefix — the same concept is legitimately cited at every call site that implements it, so this counts distinct IDs, never raw occurrences) and **1 RESERVED** concept IDs (open, unexpired entries in `docs/concept_reservations.yaml`) across **9 pillars**. Unlike agent-utilities, this repo has no generated `concepts.yaml` registry — the marker sweep below is this page's own generated source, not a restatement of one.

| Pillar | LIVE ✅ | RESERVED |
|:------|---:|---:|
| **AU-AHE** — agent-utilities harness concepts exercised against this engine | 0 | 0 |
| **AU-ECO** — agent-utilities ecosystem concepts exercised against this engine | 0 | 0 |
| **AU-KG** — agent-utilities KG concepts exercised against this engine | 34 | 0 |
| **AU-ORCH** — agent-utilities orchestration concepts exercised against this engine | 3 | 0 |
| **AU-OS** — agent-utilities OS concepts exercised against this engine | 2 | 0 |
| **EG-AHE** — harness-facing engine concepts | 1 | 0 |
| **EG-KG** — knowledge-graph engine core | 604 | 1 |
| **EG-ORCH** — routing / orchestration | 2 | 0 |
| **EG-OS** — deployment | 9 | 0 |
| **Total** | 655 | 1 |

> 2 additional unique `CONCEPT:<ID>` markers use a pre-migration prefix outside the current 9-pillar taxonomy (e.g. `KG-2.*`, `ORCH-1.*`, `OS-5.*`, `AHE-3.*`) and are not broken out above — this page defines an owning subtree/gate only for the pillars in "Domain ownership" below. See `docs/concepts.md` for those markers in prose form.

## Status vocabulary

Defined once, here — every other table in this repo's docs (README capability tables, `docs/capabilities.md`) should link to this section instead of restating or omitting it. Existing emoji are the rendering of this vocabulary, not a separate scheme.

| Status | Emoji | Meaning |
|:------|:---:|:------|
| `RESERVED` |  | A concept ID is allocated in the reservations ledger; no code implements it yet. |
| `BUILDING` | 🔶 | Partially built or actively being added; the unsupported part errors honestly. |
| `LIVE` | ✅ | Implemented and covered by tests. |
| `ROADMAP` | 🗺 | Designed, not yet built. |
| `RETIRED` |  | Formerly live, intentionally removed. |

Lifecycle: `RESERVED` → `BUILDING` → `LIVE` → (optionally) `RETIRED`, with `ROADMAP` as the not-yet-reserved intent stage.

## Domain ownership

Structural ownership — which doc subtree and which CI/pre-commit gate is authoritative for each pillar. Not named humans. `AU-*` pillar concepts appear in this repo's own source because agent-utilities code that exercises this engine tags itself with the concept it is exercising (cross-repo concept federation) — the owning subtree for those is in the agent-utilities repo, not here.

| Pillar | Owning doc subtree | Primary gate |
|:------|:------|:------|
| **AU-AHE** | `agent-utilities: docs/pillars/3_agentic_harness_engineering.md` | agent-utilities' `scripts/check_concepts.py` |
| **AU-ECO** | `agent-utilities: docs/pillars/4_ecosystem_peripherals.md` | agent-utilities' `scripts/check_concepts.py` |
| **AU-KG** | `agent-utilities: docs/pillars/2_epistemic_knowledge_graph/` | agent-utilities' `scripts/check_concepts.py` |
| **AU-ORCH** | `agent-utilities: docs/pillars/1_graph_orchestration.md` | agent-utilities' `scripts/check_concepts.py` |
| **AU-OS** | `agent-utilities: docs/pillars/5_agent_os_infrastructure.md` | agent-utilities' `scripts/check_concepts.py` |
| **EG-AHE** | `docs/architecture/` | `scripts/check_documentation_contract.py` (generated-ledger regen check) |
| **EG-KG** | `docs/interfaces/ + docs/architecture/engine.md` | `scripts/check_documentation_contract.py` (generated-ledger regen check) |
| **EG-ORCH** | `docs/architecture/` | `scripts/check_documentation_contract.py` (generated-ledger regen check) |
| **EG-OS** | `docs/deploy/ + docs/deployment.md` | `scripts/check_documentation_contract.py` (generated-ledger regen check) |

## How this page stays honest

This page is produced by `scripts/build_status_page.py` from `docs/capabilities.md`, `docs/capabilities.generated.md`, and `docs/concept_reservations.yaml` — never hand-typed. Regenerate it with:

```bash
python scripts/build_status_page.py --write
```

`scripts/check_status_page.py` is wired into `.github/workflows/release.yml`'s `lint-and-architecture` job (release-blocking, no `continue-on-error`) and fails the release the moment this file drifts from its sources. Run it locally with:

```bash
python scripts/check_status_page.py
```
