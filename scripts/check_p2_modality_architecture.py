#!/usr/bin/env python3
"""Fail CI if the P2 modality/KnowledgeBatch architecture is bypassed.

Kept here are only properties the compiler and the Rust suites cannot see:

* bans on retired or forbidden shapes (a legacy handler module, no-op
  runtimes, TCK exemptions, no-op codec features, the retired compatibility
  projection, a source-bearing receipt digest, a source-bearing Raft field);
* the graph-dispatch ordering contract (graph ACL and placement resolve
  before KnowledgeStream can be routed), expressed over the call graph;
* the cross-language Python client / server ingest-stream item bound.

Everything is matched on identifiers over comment/string-masked Rust, so a
rename or reformat of unrelated code never needs this file edited.
"""

from __future__ import annotations

import re
from pathlib import Path

import tomllib
from rust_callgraph import reachable_source, top_level_fns
from rust_lexer import _balanced_span_from, _rust_code_mask, _rust_comments_mask
from rust_module_tree import read_compiler_family, read_module_tree

ROOT = Path(__file__).resolve().parents[1]

MODALITIES = ("document", "image", "audio", "video")
RETIRED_MODALITY_FEATURES = ("codec", "extract")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"P2 architecture gate failed: {message}")


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def compiler_source(relative: str) -> str:
    """Return a Rust module's compiler-reachable source tree."""

    return read_module_tree(relative, root_dir=ROOT, include_tests=True)


def has_identifier(source: str, pattern: str) -> bool:
    """Whether an identifier regex matches whole words of Rust *code*."""

    code = _rust_code_mask(source)
    return re.search(rf"(?<![\w])(?:{pattern})(?![\w])", code) is not None


def require_single_knowledge_stream_handler() -> None:
    """The KnowledgeStream handler is one module tree, not a file + directory."""

    require(
        not (ROOT / "src/server/handlers/knowledge_stream.rs").exists(),
        "KnowledgeStream retains an ambiguous legacy handler module",
    )


def require_native_runtime_contract(modality: str, runtime: str) -> None:
    """A served modality runtime must not fall back to a no-op."""

    require(
        not has_identifier(runtime, r"\w*Noop\w*"),
        f"{modality} runtime still contains a no-op",
    )


def require_no_tck_exemption(modality: str, contract: str) -> None:
    require(
        not has_identifier(contract, "tck_not_applicable"),
        f"{modality} still exempts a production TCK dimension",
    )


def require_no_noop_features(modality: str, cargo_toml: str) -> None:
    features = tomllib.loads(cargo_toml).get("features", {})
    retired = sorted(set(RETIRED_MODALITY_FEATURES) & set(features))
    require(not retired, f"{modality} retains a no-op codec/extractor feature")


def require_modality_crates() -> None:
    for modality in MODALITIES:
        crate = f"crates/eg-{modality}"
        require_native_runtime_contract(
            modality, compiler_source(f"{crate}/src/runtime.rs")
        )
        require_no_tck_exemption(modality, compiler_source(f"{crate}/src/contract.rs"))
        require_no_noop_features(modality, read(f"{crate}/Cargo.toml"))


def require_no_compatibility_projection(wire: str) -> None:
    require(
        not has_identifier(wire, "CompatibilityMsgpackV1"),
        "retired KnowledgeStream compatibility projection is still present",
    )


def call_offset(body: str, name: str) -> int:
    """Offset of a call to `name` in `body`, or -1 (word-boundary matched)."""

    hit = re.search(rf"\b{name}\s*\(", body)
    return hit.start() if hit else -1


def require_graph_dispatch_ordering(dispatch: str) -> None:
    """Graph ACL, then placement, then routing -- as an order of CALLS.

    `dispatch_graph_op_inner` must call the ACL capture, then placement
    resolution, then the post-lock method router; nothing it calls before the
    router may reach KnowledgeStream handling, and the ACL capture must still
    reach the graph ACL check. Resolved over the call graph (see
    scripts/rust_callgraph.py), so decomposing a helper does not matter.
    """

    code = _rust_code_mask(dispatch)
    fns = top_level_fns(code)
    inner = fns.get("dispatch_graph_op_inner", "")
    require(inner != "", "dispatch_graph_op_inner is absent from dispatch.rs")
    acl = call_offset(inner, "capture_graph_dispatch")
    placement = call_offset(inner, "resolve_routed_raft")
    routing = call_offset(inner, "route_graph_op_method")
    require(
        all((acl >= 0, placement > acl, routing > placement)),
        "KnowledgeStream is routed before graph ACL/placement semantics",
    )
    require(
        call_offset(
            reachable_source(code, "capture_graph_dispatch"), "check_graph_access"
        )
        >= 0,
        "the graph ACL gate no longer performs the graph ACL check",
    )
    knowledge_stream = re.compile(r"\bKnowledgeStream\b")
    body_start = inner.find("{")
    pre_route_calls = set(re.findall(r"(\w+)\s*\(", inner[body_start:routing]))
    require(
        not knowledge_stream.search(inner[body_start:routing])
        and not any(
            knowledge_stream.search(reachable_source(code, name))
            for name in pre_route_calls & set(fns)
        )
        and knowledge_stream.search(reachable_source(code, "route_graph_op_method"))
        is not None,
        "KnowledgeStream is routed outside the post-lock router, so it no "
        "longer sits behind graph ACL and placement resolution",
    )


def require_source_free_receipt(mutation: str) -> None:
    """The durable receipt must not digest the source-bearing wire body."""

    code = _rust_code_mask(mutation)
    receipt = reachable_source(code, "durable_receipt_method")
    require(receipt != "", "durable_receipt_method is absent")
    require(
        call_offset(receipt, "canonical_body_bytes") < 0,
        "modality receipt retains a direct digest of the source-bearing wire body",
    )


def sanitized_command(raft: str) -> tuple[str, str]:
    """(attributes, body) of the sole `SanitizedModalityRaftCommand` struct."""

    code = _rust_code_mask(raft)
    text = _rust_comments_mask(raft)
    matches = list(
        re.finditer(
            r"((?:#\[(?:[^\[\]]|\[[^\]]*\])*\]\s*)*)"
            r"(?:pub(?:\([^)]*\))?\s+)?struct\s+SanitizedModalityRaftCommand\b",
            code,
        )
    )
    require(len(matches) == 1, "the current sanitized modality Raft command is absent")
    opener = code.find("{", matches[0].end())
    require(opener >= 0, "the current sanitized modality Raft command is absent")
    closer = _balanced_span_from(code, opener, "{", "}")
    return text[matches[0].start(1) : matches[0].end(1)], code[opener + 1 : closer]


def require_modality_raft_replication() -> None:
    """The replicated modality command is closed and carries no source field."""

    raft = read_compiler_family("src/raft/mod.rs", ROOT).production
    attrs, body = sanitized_command(raft)
    fields = re.findall(r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:", body)
    require(
        "deny_unknown_fields" in attrs
        and not any(re.search(r"(?:^|_)source(?:_|$)", name) for name in fields),
        "Raft modality command is not closed and source-free",
    )


_CLIENT_INGEST_STREAM_BOUND = re.compile(r"\blen\(\s*items\s*\)\s*<=\s*(\d+)")
_SERVER_INGEST_STREAM_BOUND = re.compile(
    r"\bMAX_INGEST_STREAM_ITEMS\s*:\s*usize\s*=\s*(\d+)"
)


def require_ingest_stream_item_bound(python_client: str) -> None:
    """The client's `ingest_stream` item ceiling equals the server's.

    Cross-language, so no compiler sees it: the server bound moved 61 -> 49
    once while the client kept validating against 64.
    """

    client_bounds = set(_CLIENT_INGEST_STREAM_BOUND.findall(python_client))
    require(
        len(client_bounds) == 1,
        "Python client ingest_stream item bound is absent or inconsistent",
    )
    server = _rust_comments_mask(compiler_source("src/server/handlers/modality.rs"))
    server_bound = _SERVER_INGEST_STREAM_BOUND.search(server)
    require(server_bound is not None, "server MAX_INGEST_STREAM_ITEMS not found")
    assert server_bound is not None
    (client_bound,) = client_bounds
    require(
        client_bound == server_bound.group(1),
        f"Python client ingest_stream bound ({client_bound}) has drifted from the "
        f"server's MAX_INGEST_STREAM_ITEMS ({server_bound.group(1)})",
    )


def main() -> None:
    require_single_knowledge_stream_handler()
    require_modality_crates()
    require_no_compatibility_projection(
        compiler_source("crates/eg-types/src/knowledge_stream.rs")
    )
    require_graph_dispatch_ordering(
        read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    )
    require_source_free_receipt(
        read_module_tree("src/server/mutation.rs", root_dir=ROOT)
    )
    require_modality_raft_replication()
    require_ingest_stream_item_bound(read("epistemic_graph/client.py"))
    print("P2 modality architecture gate passed")


if __name__ == "__main__":
    main()
