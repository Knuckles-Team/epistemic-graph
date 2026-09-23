"""Parse the domain-owned current method-policy registry.

The eleven domain ROWS declarations are the only policy data. Their order is the
registry order in domains/mod.rs followed by declaration order inside each domain.
There is no separately ordered projection or second policy authority.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


class MethodPolicyInventoryError(ValueError):
    """The current method-policy registry is absent or malformed."""


@dataclass(frozen=True)
class MethodPolicyRow:
    name: str
    mutates: bool
    durability_domain: str
    authz_action: str
    idempotent: bool
    audited: bool
    emits_cdc: bool
    txn_participation: str
    note: str = ""
    cfg_feature: str | None = None
    domain: str = ""


# 408 -> 410: RF-020 admitted two method-policy rows, `AgentLibrary` (storage)
# and `KgDelegate` (coordination). This constant is a tripwire against a parser
# that silently reads nothing, not a ratchet -- it is raised deliberately when
# the method set legitimately changes, and `contract/methods.json` is the
# authority it must agree with (410 unique ids at the RF-020 closure).
#
# 410 -> 411: RF-ADR-008 admitted `AgentGraph` (storage), the composition
# surface over Agent Library entries. Registered at `Stability::Internal` with
# no generated consumer until its durable revision store lands -- the wire
# contract is frozen so AU can be written against it, but nothing may assume
# it serves yet.
# 411 -> 412: RF-ADR-008 layer 1 admitted `AgentComponent` (storage), the parts
# agents and graphs are assembled from.
# 412 -> 413: RF-ADR-008 item C admitted `AgentTemplate` (storage), the
# parameterized agent family. It takes its OWN `agent:template-write` /
# `agent:template-read` actions rather than the library's: publishing a
# generator of agents is a distinct privilege from publishing one agent.
# 413 -> 414: RF-019 admitted `SemanticIndex` (ingestion), the S1-S6 tiered
# semantic ingestion queue. It is the first row whose authz action is chosen
# per-operation rather than by a write/read pair: `SemanticIndexOp::authz_action`
# spans six actions (semantic:binding-write/-read, semantic:source-admit,
# semantic:stage-claim, semantic:stage-complete, semantic:stage-read), because
# curating what is indexed, feeding rows into an approved binding, taking work
# off the queue and declaring a stage durable are four different grants. The row
# below records the binding-write leg as its conservative upper bound, exactly
# as the four agent layers record theirs.
# 414 -> 415: `SqlSourceBatch` (storage) admits typed SQL source rows through
# the native SQL-catalog owner: rows, provider cursor, source epoch, terminal
# result, replay receipt and outbox commit in one MutationBatch.
# 415 -> 425: the 2.27.x contract wave's ten declared-but-not-yet-served
# methods, every one `Internal` with no consumer until its handler lands --
# `AgentAssemble`, `DecisionCommit`, `Decide`, `DecisionFit`, `DecisionEval`,
# `Solve`, `ConnectorPack`, `GraphSchema`, `GraphSchemaList` and
# `MutationOutbox`.
# 425 -> 426: RF-ADR-009's served `SourceIngest` raw-record authority.
# 426 -> 427: governed connector `WriteBack` authority.
EXPECTED_METHOD_POLICY_ROWS = 430
EXPECTED_DOMAIN_MODULES = (
    "cluster",
    "compute",
    "coordination",
    "graph",
    "ingestion",
    "messaging",
    "query",
    "reasoning",
    "security",
    "storage",
    "transactions",
)
EXPECTED_CFG_ROWS = (
    ("AnalyticsJob", "jobs"),
    ("Statechart", "statechart"),
    ("ServedModality", "modality-serving"),
    ("Quantum", "quantum"),
    ("Asr", "asr-native"),
    ("Viz", "viz"),
    ("KnowledgeStream", "knowledge-batch"),
)

_SOURCE_MARKER = re.compile(r"(?m)^// method-policy source: (?P<path>\S+)\s*$")
_CFG_ATTRIBUTE = re.compile(r'^\s*#\[cfg\(feature\s*=\s*"(?P<feature>[^"]+)"\)\]\s*$')
_REGISTRY_ROW = re.compile(
    r'^\s*\("(?P<name>[a-z][a-z0-9_]*)",\s*'
    r"(?P=name)::ROWS\),\s*$"
)
_QUOTED = r'"(?:\\.|[^"\\])*"'
# The authored half of a contract row (RF-RULING-003): `spec(..)` wraps
# `make_policy(..)` with the consumer profiles the row is generated for and its
# stability. Those two are the CONTRACT's facts, not the POLICY's; the parser still
# reads them, because a row shape it cannot read is a scanner that reports nothing
# while claiming to have checked every row. A row authors no result: that is the
# `eg_types::result_contract` marker the handler encodes through.
_POLICY_ROW = re.compile(
    rf"^\s*\(\s*(?P<name>{_QUOTED})\s*,\s*"
    r"spec\(\s*"
    r"make_policy\(\s*(?P<mutates>true|false)\s*,\s*"
    r"DurabilityDomain::(?P<durability>[A-Za-z][A-Za-z0-9_]*)\s*,\s*"
    rf"(?P<authz>{_QUOTED})\s*,\s*PolicyFlags\s*\{{\s*"
    r"idempotent:\s*(?P<idempotent>true|false)\s*,\s*"
    r"audited:\s*(?P<audited>true|false)\s*,\s*"
    r"emits_cdc:\s*(?P<cdc>true|false)\s*\}\s*,\s*"
    r"TxnParticipation::(?P<txn>[A-Za-z][A-Za-z0-9_]*)\s*\)\s*,\s*"
    r"(?P<consumers>[A-Z][A-Z0-9_]*)\s*,\s*"
    r"Stability::(?P<stability>[A-Za-z][A-Za-z0-9_]*)\s*\)\s*,\s*"
    rf"(?P<note>{_QUOTED})\s*\)\s*,?\s*$"
)


def render_capability_sources(sources: Mapping[str, str]) -> str:
    """Render named Rust files into a deterministic scanner bundle."""

    return "\n".join(
        f"// method-policy source: {path}\n{sources[path]}" for path in sorted(sources)
    )


def _split_sources(source: str) -> dict[str, str]:
    markers = list(_SOURCE_MARKER.finditer(source))
    if not markers:
        raise MethodPolicyInventoryError("capability source bundle has no file markers")
    result: dict[str, str] = {}
    for index, marker in enumerate(markers):
        path = marker.group("path")
        if path in result:
            raise MethodPolicyInventoryError(f"duplicate capability source: {path}")
        start = marker.end()
        end = markers[index + 1].start() if index + 1 < len(markers) else len(source)
        result[path] = source[start:end].lstrip("\n")
    return result


def _source(sources: Mapping[str, str], suffix: str) -> str:
    matches = [text for path, text in sources.items() if path.endswith(suffix)]
    if len(matches) != 1:
        raise MethodPolicyInventoryError(
            f"expected one capability source ending in {suffix}: found={len(matches)}"
        )
    return matches[0]


def _decode_string(value: str) -> str:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as error:
        raise MethodPolicyInventoryError("malformed policy string literal") from error
    if not isinstance(decoded, str):
        raise MethodPolicyInventoryError("policy literal is not a string")
    return decoded


def _registry_modules(source: str) -> tuple[str, ...]:
    if "ALL_METHODS" in source or "dispatch::" in source:
        raise MethodPolicyInventoryError(
            "retired flat method-policy projection remains"
        )
    marker = "const REGISTRY: &[(&str, &[PolicyRow])] = &["
    start = source.find(marker)
    if start < 0:
        raise MethodPolicyInventoryError("missing domain-owned REGISTRY")
    body = source[start + len(marker) :].split("];", 1)[0]
    modules: list[str] = []
    for line in body.splitlines():
        match = _REGISTRY_ROW.fullmatch(line)
        if match:
            modules.append(match.group("name"))
            continue
        stripped = line.strip()
        if stripped and not stripped.startswith("//"):
            raise MethodPolicyInventoryError(
                f"unrecognized domain registry line: {stripped}"
            )
    if tuple(modules) != EXPECTED_DOMAIN_MODULES:
        raise MethodPolicyInventoryError(
            f"domain registry differs: expected={EXPECTED_DOMAIN_MODULES}, "
            f"actual={tuple(modules)}"
        )
    return tuple(modules)


def _domain_sources(sources: Mapping[str, str]) -> dict[str, str]:
    result = {
        Path(path).stem: text
        for path, text in sources.items()
        if Path(path).parent.name == "domains" and Path(path).name != "mod.rs"
    }
    if set(result) != set(EXPECTED_DOMAIN_MODULES):
        raise MethodPolicyInventoryError(
            f"capability domains differ: expected={EXPECTED_DOMAIN_MODULES}, "
            f"actual={tuple(sorted(result))}"
        )
    return result


def _bool(value: str) -> bool:
    return value == "true"


def _start_cfg(
    match: re.Match[str],
    pending_feature: str | None,
    domain: str,
) -> str:
    if pending_feature is not None:
        raise MethodPolicyInventoryError(
            f"{domain} has consecutive method cfg attributes"
        )
    return match.group("feature")


def _policy_row(
    match: re.Match[str],
    domain: str,
    cfg_feature: str | None,
) -> MethodPolicyRow:
    row = MethodPolicyRow(
        name=_decode_string(match.group("name")),
        mutates=_bool(match.group("mutates")),
        durability_domain=match.group("durability"),
        authz_action=_decode_string(match.group("authz")),
        idempotent=_bool(match.group("idempotent")),
        audited=_bool(match.group("audited")),
        emits_cdc=_bool(match.group("cdc")),
        txn_participation=match.group("txn"),
        note=_decode_string(match.group("note")),
        cfg_feature=cfg_feature,
        domain=domain,
    )
    if ":" not in row.authz_action:
        raise MethodPolicyInventoryError(
            f"{row.name} authz action is not primitive:verb"
        )
    if row.mutates and row.durability_domain == "None":
        raise MethodPolicyInventoryError(
            f"{row.name} mutates without a durability domain"
        )
    return row


def _validate_non_row(
    line: str,
    domain: str,
    pending_feature: str | None,
) -> None:
    stripped = line.strip()
    if stripped.startswith('("') or "make_policy(" in line:
        raise MethodPolicyInventoryError(f"unparsed {domain} policy row: {stripped}")
    if pending_feature is not None and stripped and not stripped.startswith("//"):
        raise MethodPolicyInventoryError(
            f"{domain} cfg attribute does not guard a policy row"
        )


def _parse_domain(source: str, domain: str) -> tuple[MethodPolicyRow, ...]:
    rows: list[MethodPolicyRow] = []
    pending_feature: str | None = None
    for line in source.splitlines():
        cfg = _CFG_ATTRIBUTE.fullmatch(line)
        if cfg:
            pending_feature = _start_cfg(cfg, pending_feature, domain)
            continue
        match = _POLICY_ROW.fullmatch(line)
        if match:
            rows.append(_policy_row(match, domain, pending_feature))
            pending_feature = None
            continue
        _validate_non_row(line, domain, pending_feature)
    if pending_feature is not None:
        raise MethodPolicyInventoryError(
            f"{domain} cfg attribute does not guard a policy row"
        )
    if not rows:
        raise MethodPolicyInventoryError(f"{domain} policy rows are empty")
    return tuple(rows)


def _validate_unique(names: tuple[str, ...]) -> None:
    if len(names) != len(set(names)):
        duplicate = next(name for name in names if names.count(name) > 1)
        raise MethodPolicyInventoryError(
            f"duplicate method-policy declaration: {duplicate}"
        )


def _validate_cfg_rows(rows: tuple[MethodPolicyRow, ...]) -> None:
    cfg_rows = tuple(
        (row.name, row.cfg_feature) for row in rows if row.cfg_feature is not None
    )
    if cfg_rows != EXPECTED_CFG_ROWS:
        raise MethodPolicyInventoryError(
            f"cfg-gated method policies differ: expected={EXPECTED_CFG_ROWS}, "
            f"actual={cfg_rows}"
        )


def _validate_rows(
    rows: tuple[MethodPolicyRow, ...],
    expected_count: int,
    expected_order: tuple[str, ...] | None,
) -> tuple[MethodPolicyRow, ...]:
    if len(rows) != expected_count:
        raise MethodPolicyInventoryError(
            f"method-policy registry has {len(rows)} rows instead of {expected_count}"
        )
    names = tuple(row.name for row in rows)
    _validate_unique(names)
    if expected_order is not None and names != expected_order:
        raise MethodPolicyInventoryError(
            "method-policy registry order differs from expected order"
        )
    if expected_count == EXPECTED_METHOD_POLICY_ROWS:
        _validate_cfg_rows(rows)
    return rows


def parse_method_policy_table(
    source: str,
    *,
    expected_count: int = EXPECTED_METHOD_POLICY_ROWS,
    expected_order: tuple[str, ...] | None = None,
) -> tuple[MethodPolicyRow, ...]:
    """Return policies in canonical domain and declaration order."""

    sources = _split_sources(source)
    lib = _source(sources, "crates/eg-capabilities/src/lib.rs")
    if "mod domains;" not in lib or "method_policy_entries" not in lib:
        raise MethodPolicyInventoryError("capability library is not registry-backed")
    if "ALL_METHODS" in lib or "mod dispatch;" in lib:
        raise MethodPolicyInventoryError(
            "capability library retains flat policy authority"
        )
    registry = _registry_modules(
        _source(sources, "crates/eg-capabilities/src/domains/mod.rs")
    )
    domain_sources = _domain_sources(sources)
    rows = tuple(
        row
        for domain in registry
        for row in _parse_domain(domain_sources[domain], domain)
    )
    return _validate_rows(rows, expected_count, expected_order)


def load_capability_sources(root: Path) -> str:
    """Read and validate the complete production capability registry."""

    source_root = root / "crates/eg-capabilities/src"
    domain_root = source_root / "domains"
    paths = [source_root / "lib.rs", domain_root / "mod.rs"]
    paths.extend(domain_root / f"{module}.rs" for module in EXPECTED_DOMAIN_MODULES)
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise MethodPolicyInventoryError(
            f"missing capability source file(s): {', '.join(missing)}"
        )
    unexpected = sorted(
        path.name
        for path in domain_root.glob("*.rs")
        if path.stem not in {*EXPECTED_DOMAIN_MODULES, "mod"}
    )
    if unexpected:
        raise MethodPolicyInventoryError(
            f"unexpected capability domain source file(s): {', '.join(unexpected)}"
        )
    bundle = render_capability_sources(
        {
            str(path.relative_to(root)): path.read_text(encoding="utf-8")
            for path in paths
        }
    )
    parse_method_policy_table(bundle)
    return bundle
