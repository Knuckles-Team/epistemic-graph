#!/usr/bin/env python3
"""Reject reintroduction of obsolete structural-property fallbacks.

Property blobs remain open JSON/MessagePack objects, so an application is free to
store an ordinary field named ``type``.  Engine code must not reinterpret that
payload field as either an edge relationship or a KnowledgeSet row kind, and
must not restore the former ``rel_type`` integration key.  This
gate intentionally checks the structural readers where that ambiguity used to
exist instead of banning legitimate user data named ``type`` repository-wide.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

STRUCTURAL_READERS = (
    "crates/eg-core/src/graph.rs",
    "crates/eg-graphql/src/schema.rs",
    "crates/eg-graphql/src/resolver.rs",
    "crates/eg-query/src/cypher/exec.rs",
    "crates/eg-query/src/cypher/proc.rs",
    "crates/eg-plan/src/exec.rs",
    "crates/eg-rdf/src/mapping.rs",
    "crates/eg-rdf/src/update.rs",
    "crates/eg-rdf/src/sparql.rs",
    "crates/eg-rdf/src/owl.rs",
    "crates/eg-compute/src/reasoning.rs",
    "crates/eg-epistemic/src/adapter.rs",
    "crates/eg-epistemic/src/incremental.rs",
    "crates/eg-epistemic/src/recompute.rs",
    "crates/eg-jobs/src/claim.rs",
    "src/server/handlers/mining.rs",
    "src/server/handlers/graphlearn.rs",
    "src/server/handlers/graph_ops.rs",
    "src/server/handlers/txn.rs",
    "src/server/reasoning_projection.rs",
)
KNOWLEDGE_READER = "crates/eg-plan/src/knowledge.rs"
CYPHER_READERS = (
    "crates/eg-query/src/cypher/exec.rs",
    "crates/eg-query/src/cypher/proc.rs",
)

EDGE_TYPE_FALLBACKS = (
    re.compile(
        r'get\("relationship"\)\s*\.or_else\(\|\|\s*[A-Za-z_][A-Za-z0-9_]*\.get\("type"\)\)',
        re.MULTILINE,
    ),
    re.compile(r"`relationship`\s*/\s*`type`"),
    re.compile(r"`type`\s*/\s*`relationship`"),
    re.compile(r'get\("rel_type"\)'),
    re.compile(r'get\("relationship_type"\)'),
    re.compile(r'get\("relation"\)'),
    re.compile(r'"rel_type"\s*:'),
    re.compile(r'"relationship_type"\s*:'),
    re.compile(r'"relation"\s*:'),
    re.compile(r'"type"\s*:\s*(?:predicate|new_prop|prop\b|p\b)'),
)
KNOWLEDGE_TYPE_FALLBACK = re.compile(
    r'get\("node_type"\)\s*\.or_else\(\|\|\s*[A-Za-z_][A-Za-z0-9_]*\.get\("type"\)\)',
    re.MULTILINE,
)


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def main() -> int:
    failures: list[str] = []
    for path in STRUCTURAL_READERS:
        source = text(path)
        for pattern in EDGE_TYPE_FALLBACKS:
            if pattern.search(source):
                failures.append(
                    f"{path}: edge relationship must not fall back to ordinary `type`"
                )

    knowledge = text(KNOWLEDGE_READER)
    if KNOWLEDGE_TYPE_FALLBACK.search(knowledge):
        failures.append(
            f"{KNOWLEDGE_READER}: KnowledgeSet kind must read only canonical `node_type`"
        )

    # Cypher labels are a strict current-only seam. An application may still
    # carry ordinary payload fields named `type` or `label`, but MATCH/CREATE and
    # db.labels must never reinterpret them as structural labels.
    for path in CYPHER_READERS:
        source = text(path).split("#[cfg(test)]", 1)[0]
        if re.search(r'\.get\("(?:type|label)"\)', source):
            failures.append(
                f"{path}: Cypher structural label readers must use only `node_type`/`labels`"
            )

    cypher_exec = text("crates/eg-query/src/cypher/exec.rs")
    for required in (
        'materialize_node(view, id)',
        'obj.insert("id".to_string(), Value::String(node_id.to_string()));',
        'obj.insert("node_type".to_string(), Value::Null);',
        'props.insert("node_type".to_string(), Value::String(label.clone()));',
    ):
        if required not in cypher_exec:
            failures.append(
                "crates/eg-query/src/cypher/exec.rs: missing canonical Cypher node "
                f"projection/write marker `{required}`"
            )

    graphql_writer = text("crates/eg-graphql/src/mutation.rs")
    if 'obj.insert("relationship".to_string(), Value::String(rel.clone()));' not in graphql_writer:
        failures.append(
            "crates/eg-graphql/src/mutation.rs: GraphQL edge writer must stamp `relationship`"
        )

    typed_edge = text("crates/eg-types/src/types.rs")
    if (
        "pub relationship: String," not in typed_edge
        or "pub relationship_type:" in typed_edge
        or "#[serde(deny_unknown_fields)]\npub struct EdgeData" not in typed_edge
    ):
        failures.append(
            "crates/eg-types/src/types.rs: EdgeData must expose only canonical `relationship`"
        )

    docs = text("docs/architecture/canonical-property-schema.md")
    nav = text("mkdocs.yml")
    if "architecture/canonical-property-schema.md" not in nav:
        failures.append("mkdocs.yml: canonical property schema page is not in navigation")
    for required in ("`relationship`", "`node_type`", "`rel_type`"):
        if required not in docs:
            failures.append(
                f"docs/architecture/canonical-property-schema.md: missing contract term {required}"
            )

    if failures:
        print("canonical property schema gate: FAIL")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("canonical property schema gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
