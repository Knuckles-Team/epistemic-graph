#!/usr/bin/env python3
"""Fail CI when current-only mutation/projection persistence regresses."""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(Path(__file__).resolve().parent))

from method_policy_inventory import (
    EXPECTED_METHOD_POLICY_ROWS,
    MethodPolicyInventoryError,
    load_capability_sources,
    parse_method_policy_table,
)


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _resolve_module_child(path: Path, declaration: re.Match) -> Path:
    attrs = declaration.group("attrs")
    explicit = re.search(r'#\[path\s*=\s*"([^"]+)"\]', attrs)
    if explicit:
        return path.parent / explicit.group(1)
    container = (
        path.parent
        if path.name in {"mod.rs", "lib.rs", "main.rs"}
        else path.with_suffix("")
    )
    candidates = (
        container / f"{declaration.group('name')}.rs",
        container / declaration.group("name") / "mod.rs",
    )
    existing = tuple(candidate for candidate in candidates if candidate.is_file())
    require(
        len(existing) == 1,
        "declared Rust module must resolve to exactly one file: "
        f"{path}::{declaration.group('name')}",
    )
    return existing[0]


def _visit_module_tree(
    path: Path,
    include_tests: bool,
    loaded: set[Path],
    visiting: set[Path],
    sources: list[str],
) -> None:
    require(path.is_file(), f"missing declared Rust module: {path}")
    require(path not in visiting, f"cyclic Rust module declaration: {path}")
    if path in loaded:
        return
    visiting.add(path)
    loaded.add(path)
    source = path.read_text(encoding="utf-8")
    sources.append(source)
    for declaration in _MODULE_TREE_DECL.finditer(source):
        attrs = declaration.group("attrs")
        if not include_tests and re.search(r"#\[cfg\([^]]*\btest\b", attrs):
            continue
        _visit_module_tree(
            _resolve_module_child(path, declaration),
            include_tests,
            loaded,
            visiting,
            sources,
        )
    visiting.remove(path)


def read_module_tree(relative: str, *, include_tests: bool = False) -> str:
    """Read a Rust facade and every declared production child recursively.

    Rust supports both conventional ``foo/bar.rs`` children and ``#[path]``
    files beside a facade (the latter is how the flat query and mutation-store
    APIs retain their historical module names). Walking declarations instead of
    globbing a directory makes the scanner follow the compiler's module tree and
    fail closed when a declared implementation disappears.
    """

    root = ROOT / relative
    require(root.is_file(), f"missing Rust facade or module tree: {relative}")
    loaded: set[Path] = set()
    visiting: set[Path] = set()
    sources: list[str] = []
    _visit_module_tree(root, include_tests, loaded, visiting, sources)
    return "\n".join(sources)


def read_module_set(directory: str, names: tuple[str, ...]) -> str:
    """Read an explicitly named production module family in declaration order."""

    module_dir = ROOT / directory
    require(module_dir.is_dir(), f"missing Rust module directory: {directory}")
    paths = [module_dir / name for name in names]
    require(
        all(path.is_file() for path in paths),
        f"missing Rust module in {directory}: {names}",
    )
    return "\n".join(path.read_text(encoding="utf-8") for path in paths)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"persisted mutation contract gate failed: {message}")


_METHOD_VARIANT = re.compile(r"\bMethod::([A-Z][A-Za-z0-9_]*)")
_STRING_LITERAL = re.compile(r'"([A-Z][A-Za-z0-9_]*)"')


def _balanced_code_step(
    source: str,
    index: int,
    char: str,
    following: str,
    opener: str,
    closer: str,
    depth: int,
) -> tuple[str, int, int, int | None]:
    """Advance one code-state character in the balanced-span scanner."""

    token = char + following
    if token == "//":
        return "line-comment", depth, 1, None
    if token == "/*":
        return "block-comment", depth, 1, None
    if char == '"':
        return "string", depth, 0, None
    if char == "'":
        # Rust lifetimes are not character literals. Only enter the char state
        # when a closing quote is nearby.
        if source.find("'", index + 1, min(index + 8, len(source))) >= 0:
            return "char", depth, 0, None
        return "code", depth, 0, None
    if char == opener:
        return "code", depth + 1, 0, None
    if char == closer:
        depth -= 1
        if depth == 0:
            return "code", depth, 0, index
    return "code", depth, 0, None


def _balanced_non_code_step(
    state: str,
    char: str,
    following: str,
    block_comment_depth: int,
) -> tuple[str, int, int]:
    """Advance one comment/string/character-literal scanner state."""

    if state == "line-comment":
        return ("code" if char == "\n" else state), block_comment_depth, 0
    if state == "block-comment":
        token = char + following
        if token == "/*":
            return state, block_comment_depth + 1, 1
        if token == "*/":
            block_comment_depth -= 1
            return (
                "code" if block_comment_depth == 0 else state,
                block_comment_depth,
                1,
            )
        return state, block_comment_depth, 0
    quote = '"' if state == "string" else "'"
    if char == "\\":
        return state, block_comment_depth, 1
    return ("code" if char == quote else state), block_comment_depth, 0


def _balanced_span_from(source: str, start: int, opener: str, closer: str) -> int:
    """Index of the `closer` that balances the `opener` at `start`, comment/string-aware.

    The position-based core `_balanced_block` (and the call-graph resolution in
    `_routing_call_offset`/`_function_with_callees`) share, factored out so the
    latter can locate a function's body directly from a known start index instead
    of re-searching the whole source with `_function`'s marker-based lookup for
    every candidate — that repeated whole-source re-search is O(candidates ×
    file size) and was measured costing ~5s on dispatch.rs's ~600-function scale.
    """
    require(source[start] == opener, f"expected {opener!r} at position {start}")
    depth = 0
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(source):
        char = source[index]
        following = source[index + 1] if index + 1 < len(source) else ""
        if state == "code":
            state, depth, skip, closing_index = _balanced_code_step(
                source, index, char, following, opener, closer, depth
            )
            block_comment_depth = int(state == "block-comment")
        else:
            state, block_comment_depth, skip = _balanced_non_code_step(
                state, char, following, block_comment_depth
            )
            closing_index = None
        if closing_index is not None:
            return closing_index
        index += skip + 1
    require(False, f"unterminated balanced block starting at position {start}")
    return -1  # unreachable; keeps static type checkers total


def _balanced_block(source: str, marker: str, opener: str, closer: str) -> str:
    """Return one Rust block while ignoring delimiters in comments and strings."""

    marker_at = source.find(marker)
    require(marker_at >= 0, f"missing Rust inventory marker: {marker}")
    start = source.find(opener, marker_at + len(marker))
    require(start >= 0, f"missing {opener!r} after Rust inventory marker: {marker}")
    end = _balanced_span_from(source, start, opener, closer)
    return source[start + 1 : end]


def _const_slice(source: str, name: str) -> str:
    match = re.search(
        rf"(?:pub(?:\([^)]*\))?\s+)?const\s+{re.escape(name)}\b[^=]*=\s*&",
        source,
    )
    require(match is not None, f"missing Rust const inventory: {name}")
    return _balanced_block(source, match.group(0), "[", "]")


def _function(source: str, name: str) -> str:
    match = re.search(rf"\bfn\s+{re.escape(name)}\s*\(", source)
    require(match is not None, f"missing Rust function inventory: {name}")
    return _balanced_block(source, match.group(0), "{", "}")


_CALL_TARGET = re.compile(r"\b([a-z_][a-z0-9_]*)\s*\(")


def _function_if_present(source: str, name: str) -> str | None:
    if not re.search(rf"\bfn\s+{re.escape(name)}\s*\(", source):
        return None
    return _function(source, name)


def _collect_function_frontier(
    source: str, frontier: list[str], seen: set[str]
) -> tuple[list[str], list[str]]:
    collected: list[str] = []
    next_frontier: list[str] = []
    for fn_name in frontier:
        if fn_name in seen:
            continue
        seen.add(fn_name)
        body = _function_if_present(source, fn_name)
        if body is None:
            continue
        collected.append(body)
        next_frontier.extend(
            call
            for call in _CALL_TARGET.findall(body)
            if call not in seen and call != fn_name
        )
    return collected, next_frontier


def _function_with_callees(source: str, name: str, max_depth: int = 2) -> str:
    """`_function`'s body, plus the bodies of same-source functions it calls,
    followed up to `max_depth` hops.

    A legitimate extract-method split can move logic this gate looks for (a
    literal substring, a Method:: reference) out of the named function into a
    helper it now calls — the content is still there, just one hop away. A
    content check that inspects `_function(...)` alone then reports a false
    "missing" on every such split. This walks the local call graph (by function
    NAME, textually — no type resolution, so it can both under- and over-collect
    same-named functions elsewhere in the file; acceptable for this gate's existing
    level of rigor) so a content check keeps seeing the real, current implementation
    rather than a stale single-function snapshot.
    """
    collected: list[str] = []
    seen: set[str] = set()
    frontier = [name]
    depth = 0
    while frontier and depth <= max_depth:
        bodies, next_frontier = _collect_function_frontier(source, frontier, seen)
        collected.extend(bodies)
        frontier = next_frontier
        depth += 1
    require(collected, f"missing Rust function inventory: {name}")
    return "\n".join(collected)


def _enum(source: str, name: str) -> str:
    match = re.search(rf"\benum\s+{re.escape(name)}\b", source)
    require(match is not None, f"missing Rust enum inventory: {name}")
    return _balanced_block(source, match.group(0), "{", "}")


def _string_set(block: str, inventory: str) -> set[str]:
    values = _STRING_LITERAL.findall(block)
    require(values, f"empty Rust string inventory: {inventory}")
    require(
        len(values) == len(set(values)),
        f"duplicate entry in Rust inventory: {inventory}",
    )
    return set(values)


def _method_set(block: str, inventory: str) -> set[str]:
    values = _METHOD_VARIANT.findall(block)
    require(values, f"empty Rust Method inventory: {inventory}")
    return set(values)


def _policy_inventory(
    source: str,
    *,
    expected_count: int = EXPECTED_METHOD_POLICY_ROWS,
    expected_order: tuple[str, ...] | None = None,
) -> dict[str, tuple[bool, str]]:
    try:
        return {
            row.name: (row.mutates, row.durability_domain)
            for row in parse_method_policy_table(
                source,
                expected_count=expected_count,
                expected_order=expected_order,
            )
        }
    except MethodPolicyInventoryError as error:
        require(False, str(error))
        return {}  # unreachable; keeps static type checkers total


_FN_DEF = re.compile(
    r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)

_MODULE_DECL = re.compile(
    r"(?m)^[ \t]*(?:#\[path\s*=\s*\"(?P<path>[^\"]+)\"\][ \t]*\n[ \t]*)?"
    r"(?:#\[cfg\(test\)\][ \t]*\n[ \t]*)?mod\s+(?P<name>[a-z_][A-Za-z0-9_]*)\s*;"
)
_MODULE_TREE_DECL = re.compile(
    r"(?m)^[ \t]*(?P<attrs>(?:#\[[^\n]*\][ \t]*\n[ \t]*)*)"
    r"(?:pub(?:\([^)]*\))?[ \t]+)?mod\s+(?P<name>[a-z_][A-Za-z0-9_]*)\s*;"
)

_MUTATION_BATCH_MODULES = (
    "src/server/mutation_batch/canonical.rs",
    "src/server/mutation_batch/commit.rs",
    "src/server/mutation_batch/compile.rs",
    "src/server/mutation_batch/digest.rs",
    "src/server/mutation_batch/tests.rs",
)
_MUTATION_BATCH_PRODUCTION_MODULES = _MUTATION_BATCH_MODULES[:-1]


def _module_manifest(root_path: str, root_source: str) -> tuple[str, ...]:
    """Resolve one facade's external modules in declared source order.

    The manifest is deliberately exact rather than discovering arbitrary files
    beside a facade. A missing declaration, duplicate declaration, or reorder
    must fail closed; otherwise a new child can silently fall out of the static
    proof while the facade still parses and the gate reports a false green.
    """
    root = Path(root_path)
    declared: list[str] = []
    for match in _MODULE_DECL.finditer(root_source):
        if match.group("path"):
            module_path = root.parent / match.group("path")
        else:
            module_path = root.with_suffix("") / f"{match.group('name')}.rs"
        declared.append(module_path.as_posix())
    require(
        len(declared) == len(set(declared)),
        f"duplicate module declaration in {root_path}: {declared}",
    )
    expected = _MUTATION_BATCH_MODULES
    require(
        tuple(declared) == expected,
        f"mutation-batch module manifest drift: expected {expected}, observed {tuple(declared)}",
    )
    return tuple(declared)


def _read_module_union(root_path: str, root_source: str) -> str:
    """Read production facade modules after validating the complete manifest.

    The test module is read as part of manifest validation, but remains out of
    the production source bundle so a test-only string cannot satisfy a
    durability invariant that belongs to the live implementation.
    """
    modules = _module_manifest(root_path, root_source)
    loaded = {path: read(path) for path in modules}
    return "\n".join(
        (root_source, *(loaded[path] for path in _MUTATION_BATCH_PRODUCTION_MODULES))
    )


def _first_present(source: str, candidates: tuple[str, ...], start: int = 0) -> int:
    """Position of the first `candidates` marker found at/after `start`; -1 if none.

    An ordering check anchored on one literal string goes stale the moment a
    legitimate rename or inline-to-call-site extraction changes that exact text
    (e.g. `let response = match req.method` becoming `let response =
    dispatch_request_method(` once the 59-arm match was extracted into a named
    function) even though the position it marks is unchanged in spirit. Accepting
    several known-equivalent forms keeps the check meaningful across that kind of
    refactor instead of reporting a false "missing" on it.
    """
    for candidate in candidates:
        pos = source.find(candidate, start)
        if pos != -1:
            return pos
    return -1


def _routing_call_offset(source: str, literal_marker: str) -> int:
    """Offset of the effective CALL SITE that performs `literal_marker`'s check.

    Historically each routing predicate (e.g. `is_query_gateway_method(&method)`)
    was called inline in the dispatch pipeline, so its own text offset WAS the call
    site and a straight `source.find` sufficed to order the pipeline stages. A
    legitimate extract-method split (dispatch decomposition) can move that literal
    call inside a small named wrapper function (e.g. `route_query_gateway`) that the
    real pipeline calls elsewhere -- the literal's OWN offset then reflects where
    it is *defined*, not where the pipeline actually *runs* it, and a raw
    `source.find`-based ordering check goes stale without any real regression.
    Resolve one hop: if `literal_marker` sits inside a named `fn`, and that fn is
    itself invoked elsewhere in the same source, use the invocation's offset;
    otherwise the literal's own offset is already the call site.
    """
    literal_at = source.find(literal_marker)
    require(literal_at >= 0, f"missing dispatch routing marker: {literal_marker}")
    # Nearest-preceding-`fn` first: Rust top-level/impl fns don't overlap, so the
    # closest `fn` starting before `literal_at` is virtually always its enclosing
    # function. Trying candidates nearest-first and stopping at the first whose
    # (position-based, no whole-source re-search) body actually contains
    # `literal_at` keeps this O(1) fn bodies scanned in the common case, instead
    # of the O(every earlier fn) a forward scan of a ~14k-line file like
    # dispatch.rs would cost.
    candidates = [m for m in _FN_DEF.finditer(source) if m.start() <= literal_at]
    for match in reversed(candidates):
        name = match.group(1)
        body_open = source.find("{", match.end())
        if body_open < 0:
            continue
        body_end = _balanced_span_from(source, body_open, "{", "}")
        if body_open <= literal_at < body_end:
            call_match = re.search(rf"\b{re.escape(name)}\s*\(", source[body_end:])
            if call_match is not None:
                return body_end + call_match.start()
            return literal_at
    return literal_at


def _enum_variants(source: str, name: str) -> set[str]:
    block = _enum(source, name)
    values = re.findall(r"^\s*([A-Z][A-Za-z0-9_]*)\s*(?:\{|\(|,)", block, re.M)
    require(values, f"empty Rust enum inventory: {name}")
    require(len(values) == len(set(values)), f"duplicate Rust enum variant: {name}")
    return set(values)


def _check_mutation_authority_inventory(
    sources: Mapping[str, str],
) -> tuple[set[str], set[str], set[str]]:
    policy = _policy_inventory(sources["capabilities"])
    mutating = {name for name, (does_mutate, _) in policy.items() if does_mutate}
    require(mutating, "capability ledger has no mutating methods")
    require(
        all(policy[name][1] != "None" for name in mutating),
        "a mutating capability has DurabilityDomain::None",
    )

    consistency = sources["consistency"]
    graph_applier_mirror = _string_set(
        _const_slice(consistency, "MUTATION_APPLY_DURABLE_GRAPHREDB"),
        "MUTATION_APPLY_DURABLE_GRAPHREDB",
    )
    native_graph = _string_set(
        _const_slice(consistency, "NATIVE_GRAPHREDB_DURABLE"),
        "NATIVE_GRAPHREDB_DURABLE",
    )
    outbox_applier_mirror = _string_set(
        _const_slice(consistency, "MUTATION_APPLY_DURABLE_OUTBOX"),
        "MUTATION_APPLY_DURABLE_OUTBOX",
    )
    mirrored_applier = graph_applier_mirror | outbox_applier_mirror

    mutation_apply = sources["mutation_apply"]
    durable_apply = sources["durable_apply"]
    # The facade's is_durable_mutation handles four DAG-forced families explicitly
    # and delegates the base+broker set to eg_core::durable_apply::is_durable_mutation
    # (see the "durable_apply" source comment above) — union both bodies' Method
    # sets to see the classifier's real, post-hoist coverage.
    live_classifier = _method_set(
        _function_with_callees(mutation_apply, "is_durable_mutation", max_depth=4),
        "is_durable_mutation",
    ) | _method_set(
        _function(durable_apply, "is_durable_mutation"),
        "eg_core::durable_apply::is_durable_mutation",
    )
    require(
        live_classifier == mirrored_applier,
        "live durable classifier and authoritative consistency inventory differ: "
        f"missing={sorted(mirrored_applier - live_classifier)}, "
        f"stale={sorted(live_classifier - mirrored_applier)}",
    )

    graph_policy = {
        name for name, (_, domain) in policy.items() if domain == "GraphRedb"
    }
    outbox_policy = {name for name, (_, domain) in policy.items() if domain == "Outbox"}
    require(
        graph_policy == graph_applier_mirror | native_graph,
        "GraphRedb policy and graph/native applier inventories differ: "
        f"missing={sorted(graph_policy - graph_applier_mirror - native_graph)}, "
        f"stale={sorted((graph_applier_mirror | native_graph) - graph_policy)}",
    )
    require(
        outbox_policy == outbox_applier_mirror,
        "Outbox policy and applier inventory differ: "
        f"missing={sorted(outbox_policy - outbox_applier_mirror)}, "
        f"stale={sorted(outbox_applier_mirror - outbox_policy)}",
    )
    return mutating, live_classifier, native_graph


def _check_mutation_applier_inventory(
    sources: Mapping[str, str], live_classifier: set[str], native_graph: set[str]
) -> None:
    mutation_apply = sources["mutation_apply"]
    durable_apply = sources["durable_apply"]

    live_applier = _method_set(
        _function(mutation_apply, "apply"), "mutation_apply::apply"
    ) | _method_set(
        _function_with_callees(durable_apply, "apply"),
        "eg_core::durable_apply::apply",
    )
    work_item_classifier = _function_with_callees(
        sources["mutation_batch"], "is_work_item_method"
    )
    require(
        "=> MethodFamily::WorkItem" in work_item_classifier,
        "is_work_item_method no longer exposes its WorkItem family arm",
    )
    # `is_work_item_method` delegates to the shared `method_family` match. The
    # latter also contains Capacity/ResourceReservation/DevelopmentLane arms;
    # keep this inventory limited to the WorkItem arm instead of treating every
    # family as an applier-owned WorkItem method.
    work_item_classifier = work_item_classifier.split("=> MethodFamily::WorkItem", 1)[0]
    work_items = _method_set(work_item_classifier, "is_work_item_method")
    # SubmitWorkItem/SubmitWorkItems are `is_work_item_method` but admitted through
    # a dedicated engine-native atomic WorkItem command-log path (mutation_batch.rs/
    # redb_store.rs; see `src/server/mutation.rs`'s "dedicated engine-native atomic
    # WorkItem command-log admission" NON_GATEWAY_COORDINATED entry and `src/raft/
    # store.rs::require_durable_graph_method`, which explicitly EXCLUDES
    # `is_work_item_method` methods from the generic classify/apply "graph command"
    # contract). They never reach `is_durable_mutation`/`apply`, so they cannot be
    # required to appear in `live_classifier` the way the other 6 work-item methods
    # (ClaimWorkItem, RenewWorkItemLease, CommitWorkItemResult, CancelWorkItem,
    # DeferWorkItem, CasWorkItemMetadata — all generically applied/replayed) are.
    # `NATIVE_GRAPHREDB_DURABLE` is the authoritative Rust inventory of exactly this
    # natively-admitted set (mirrors the same exemption granted by the authoritative
    # `native_graph` inventory), so subtract it rather than hand-listing names here.
    natively_admitted_work_items = work_items & native_graph
    native_commands = _enum_variants(sources["raft"], "NativeMutationCommand")
    implemented_classifier = (
        live_applier
        | (work_items - natively_admitted_work_items)
        | (native_commands & live_classifier)
    )
    require(
        implemented_classifier == live_classifier,
        "durable classifier and deterministic applier ownership differ: "
        f"missing={sorted(live_classifier - implemented_classifier)}, "
        f"stale={sorted(implemented_classifier - live_classifier)}",
    )


def _check_mutation_runtime_inventory(
    sources: Mapping[str, str], mutating: set[str]
) -> tuple[str, set[str]]:
    mutation_runtime = sources["mutation_runtime"]
    routed = _string_set(
        _const_slice(mutation_runtime, "GATEWAY_ROUTED"), "GATEWAY_ROUTED"
    )
    coordinated_block = _const_slice(mutation_runtime, "NON_GATEWAY_COORDINATED")
    coordinated = set(re.findall(r'\(\s*"([A-Z][A-Za-z0-9_]*)"\s*,', coordinated_block))
    require(coordinated, "NON_GATEWAY_COORDINATED is empty")
    open_block = _const_slice(mutation_runtime, "OPEN_NOT_JUSTIFIED")
    open_entries = set(re.findall(r'\(\s*"([A-Z][A-Za-z0-9_]*)"\s*,', open_block))
    require(
        not open_entries, f"OPEN_NOT_JUSTIFIED is not empty: {sorted(open_entries)}"
    )
    require(
        not (routed & coordinated), "gateway and native coordinator inventories overlap"
    )
    require(
        routed | coordinated == mutating,
        "gateway/native ownership does not exactly cover mutating policy: "
        f"missing={sorted(mutating - routed - coordinated)}, "
        f"stale={sorted((routed | coordinated) - mutating)}",
    )
    return mutation_runtime, routed


def _check_mutation_gateway_inventory(
    sources: Mapping[str, str], mutation_runtime: str, routed: set[str]
) -> str:
    gateway_methods = _method_set(sources["graph_gateway"], "try_handle_gateway")
    gateway_body = sources["graph_gateway"]
    query_routes = _string_set(
        _function(mutation_runtime, "is_query_gateway_method"),
        "is_query_gateway_method",
    )
    rdf_routes = _string_set(
        _function(mutation_runtime, "is_rdf_gateway_method"), "is_rdf_gateway_method"
    )
    dispatch = sources["modality_dispatch"]
    dispatch_owned = set()
    if (
        "Method::ServedModality" in dispatch
        and "commit_conditional_mutation" in dispatch
    ):
        dispatch_owned.add("ServedModality")
    require(
        gateway_methods | query_routes | rdf_routes | dispatch_owned == routed,
        "served gateway ownership differs from GATEWAY_ROUTED: "
        f"missing={sorted(routed - gateway_methods - query_routes - rdf_routes - dispatch_owned)}, "
        f"stale={sorted((gateway_methods | query_routes | rdf_routes | dispatch_owned) - routed)}",
    )
    require(
        "MutationPlan::for_method" in gateway_body and "commit_gateway" in gateway_body,
        "graph gateway does not consume policy planning and the commit kernel",
    )
    return dispatch


def _check_mutation_cluster_inventory(
    sources: Mapping[str, str],
    mutation_runtime: str,
    mutating: set[str],
    routed: set[str],
) -> None:
    native_consensus = _string_set(
        _const_slice(sources["raft"], "NATIVE_CONSENSUS_METHODS"),
        "NATIVE_CONSENSUS_METHODS",
    )
    fanout = _string_set(
        _const_slice(mutation_runtime, "CONSENSUS_FANOUT_METHODS"),
        "CONSENSUS_FANOUT_METHODS",
    )
    # `clustered_mutation_inventory_is_complete` (the Rust test this gate mirrors)
    # ALSO extends its `covered` set from `SELF_ROUTED_ADMIN_METHODS` -- methods
    # with their own self-routing handler (resolves MultiRaft, does its own leader
    # check) rather than a bounded `NativeMutationCommand`. Missing this union
    # made the gate flag `RaftAddLearner`/`RaftChangeMembership` as "missing" even
    # though the real Rust test already covers them via that const.
    self_routed_admin = _string_set(
        _const_slice(mutation_runtime, "SELF_ROUTED_ADMIN_METHODS"),
        "SELF_ROUTED_ADMIN_METHODS",
    )
    cluster_test = _function(
        mutation_runtime, "clustered_mutation_inventory_is_complete"
    )
    explicit_cluster = set(
        re.findall(r'covered\.insert\("([A-Z][A-Za-z0-9_]*)"\)', cluster_test)
    )
    cluster_owned = (
        routed | native_consensus | fanout | self_routed_admin | explicit_cluster
    )
    require(
        cluster_owned == mutating,
        "cluster mutation ownership does not exactly cover mutating policy: "
        f"missing={sorted(mutating - cluster_owned)}, "
        f"stale={sorted(cluster_owned - mutating)}",
    )

    coordinated_events = _const_slice(
        mutation_runtime, "COORDINATED_APPLY_MUTATION_EVENTS"
    )
    event_constants = set(
        re.findall(
            r"crate::server::sparql_http::([A-Z][A-Z0-9_]*)",
            coordinated_events,
        )
    )
    require(
        event_constants == {"SPARQL_HTTP_UPDATE_EVENT"},
        "coordinated ApplyMutation event inventory differs from the current served carriers: "
        f"observed={sorted(event_constants)}",
    )
    # cluster_mutation_route delegates its consensus/fanout arms to
    # cluster_mutation_route_consensus (and its admin arms to
    # cluster_mutation_route_admin) -- follow both hops so a legitimate split
    # doesn't read as the SPARQL routing having disappeared.
    cluster_route = _function_with_callees(mutation_runtime, "cluster_mutation_route")
    require(
        "is_sparql_http_update(method)" in cluster_route
        and "ClusterMutationRoute::ConsensusFanout" in cluster_route,
        "SPARQL ApplyMutation event is not routed through consensus fanout",
    )


def _check_mutation_dispatch_order(sources: Mapping[str, str]) -> None:
    pipeline = sources["graph_pipeline"]
    # The mutation gateway has a small lease-binding wrapper of its own so the
    # replicated-apply actor can be substituted without duplicating the route.
    # Follow that local call when checking stage order; the effective call still
    # must precede the query/RDF stages and terminal handler.
    gateway = _function_with_callees(
        pipeline, "route_gateway_and_stateless_domains", max_depth=2
    )
    surfaces = _function(pipeline, "route_query_and_rdf_surfaces")
    terminal = _function(pipeline, "run_dispatch_pipeline")
    gateway_at = gateway.find("handlers::graph_ops::try_handle_gateway(")
    query_at = surfaces.find("route_query_gateway(")
    rdf_at = surfaces.find("route_rdf_gateway(")
    terminal_at = terminal.find("handlers::graph_ops::try_handle(")
    require(
        gateway_at >= 0,
        "dispatch no longer routes graph/query/RDF gateways before the terminal handler",
    )
    require(
        query_at >= 0,
        "dispatch no longer routes graph/query/RDF gateways before the terminal handler",
    )
    require(
        query_at < rdf_at,
        "dispatch no longer routes graph/query/RDF gateways before the terminal handler",
    )
    require(
        terminal_at >= 0,
        "dispatch no longer routes graph/query/RDF gateways before the terminal handler",
    )


def check_mutation_inventory(sources: Mapping[str, str]) -> None:
    """Translate the authoritative Rust inventory tests into a source-only proof.

    Unlike the leaf-crate mirror test, this reads the live classifier, applier,
    gateway, dispatch, and Raft sources in the same immutable tree and therefore
    fails on a stale transcribed list as well as on a missing implementation arm.
    """

    mutating, live_classifier, native_graph = _check_mutation_authority_inventory(
        sources
    )
    _check_mutation_applier_inventory(sources, live_classifier, native_graph)
    mutation_runtime, routed = _check_mutation_runtime_inventory(sources, mutating)
    _check_mutation_gateway_inventory(sources, mutation_runtime, routed)
    _check_mutation_dispatch_order(sources)
    _check_mutation_cluster_inventory(sources, mutation_runtime, mutating, routed)


_TEST_MODULE = re.compile(r"#\[cfg\(test\)\]\s*\nmod\s+\w+\s*\{", re.M)


def _production_source(source: str) -> str:
    """Everything before the file's `#[cfg(test)] mod ... { ... }` test module.

    A bare `source.split("#[cfg(test)]", 1)` truncates at the FIRST occurrence of
    that attribute anywhere in the file — but `#[cfg(test)]` also legitimately
    gates individual test-only items embedded among production code (e.g. a
    `#[cfg(test)] pub(crate) fn with_allow(...)` test-only constructor beside the
    real `from_env` one), which appears earlier than the real test module in
    files that have one. Splitting there silently discards every genuinely
    PRODUCTION line after it — both weakening the "forbidden pattern" checks
    below (they stop scanning real code) and starving positive-presence checks
    of code that is still there. Anchor on the actual module boundary instead.
    """
    match = _TEST_MODULE.search(source)
    if match is None:
        return source
    return source[: match.start()]


def _check_retired_dataset_surface(sources: Mapping[str, str]) -> None:
    for retired_path in (
        "src/server/dataset_handle.rs",
        "tests/dataset_handle_e2e.rs",
    ):
        require(
            not (ROOT / retired_path).exists(),
            f"retired duplicate dataset surface returned: {retired_path}",
        )

    cargo = sources["cargo"]
    require(
        re.search(
            r'^sparql-http\s*=\s*\[[^\]]*"shacl"[^\]]*"security"[^\]]*\]$',
            cargo,
            re.M,
        )
        is not None,
        "SPARQL HTTP must compose mandatory integrity and recovery encryption",
    )
    require(
        "dataset-handle" not in cargo,
        "retired duplicate dataset feature returned to Cargo",
    )


def _check_current_server_surface(sources: Mapping[str, str]) -> None:
    current_server = "\n".join(
        (sources["main"], sources["state"], sources["server"], sources["dispatch"])
    )
    for retired in (
        "EPISTEMIC_GRAPH_DATASET_ADDR",
        "dataset_addr",
        "DatasetHandleRegistry",
        "/dataset/export",
        "DATASET_RESULT_EVENT",
        "coordinated_dataset_result_commit",
    ):
        require(
            retired not in current_server,
            f"retired duplicate dataset surface returned: {retired}",
        )


def _check_external_compute_contract(external_compute: str) -> None:
    require(
        external_compute.count("Method::KnowledgeStream") >= 1,
        "signed KnowledgeStream/native AnalyticsJob external-compute proof is missing",
    )
    for required in (
        "KnowledgeStreamQuery::Graph",
        "Method::AnalyticsJob",
        "JobOp::Submit",
        "KnowledgeStreamQuery::Job",
        "signed_knowledge_stream_and_native_analytics_publication_round_trip",
    ):
        require(
            required in external_compute,
            "signed KnowledgeStream/native AnalyticsJob external-compute proof is missing",
        )


def _check_sparql_carrier(source: str) -> None:
    sparql = _production_source(source)
    for forbidden in (
        "core.mark_dirty()",
        "core.add_node(",
        "core.remove_node(",
        "crate::mutation_apply::apply",
    ):
        require(
            forbidden not in sparql, f"SPARQL HTTP retains direct mutation: {forbidden}"
        )
    for required in (
        "SPARQL_HTTP_UPDATE_EVENT",
        "signed_request(",
        "crate::server::dispatch::dispatch",
        "pub(crate) async fn plan_update",
        "existed_before",
    ):
        require(
            required in sparql,
            "SPARQL HTTP writes must bind an exact signed request and complete detached preimages",
        )


def _check_ros2_carrier(source: str) -> None:
    ros2 = _production_source(source)
    for forbidden in (
        "crate::mutation_apply::apply",
        "core.mark_dirty()",
        "registry.create_graph(",
    ):
        require(
            forbidden not in ros2, f"ROS2 carrier retains direct mutation: {forbidden}"
        )
    for required in (
        "publish_to_request",
        'auth_token.starts_with("eg2.")',
        "crate::server::dispatch::dispatch",
    ):
        require(
            required in ros2,
            "ROS2 inbound writes must reconstruct and dispatch the exact signed request",
        )


def _check_dispatch_order(sources: Mapping[str, str]) -> None:
    mutation_runtime = sources["mutation_runtime"]
    request_router = sources["request_router"]
    authenticated = sources["authenticated_dispatch"]
    preflight = sources["request_preflight"]
    require(
        "ClusterMutationRoute::ConsensusFanout" in mutation_runtime,
        "served coordinator routing or canonical graph-gateway termination is missing",
    )
    require(
        "coordinated_sparql_http_update(" in request_router,
        "served coordinator routing or canonical graph-gateway termination is missing",
    )
    require(
        "let response = dispatch_request_method(" in authenticated,
        "served coordinator routing or canonical graph-gateway termination is missing",
    )
    for required in ("Method::FromMsgpack", "Method::AddNode"):
        require(
            required in preflight,
            "served coordinator routing or canonical graph-gateway termination is missing",
        )


def _check_dispatch_recovery_proof(sources: Mapping[str, str]) -> None:
    recovery = sources["sparql_plan"] + sources["sparql_execution"]
    for required in (
        "seal_private_coordinator_plan",
        "open_private_coordinator_plan",
        "begin_named_admin_saga_with_private_payload",
        "SPARQL_RECOVERY_EVENT",
        "SPARQL_COMPENSATION_EVENT",
        "commit_coordinated_graph_methods",
        "clear_coordinated_graph_decision",
        "encrypted_sparql_preimages_survive_process_restart_and_tamper_fails",
        "durable_compensation_marker_fixes_restart_direction_and_erases_its_plan",
    ):
        require(
            required in recovery,
            f"recoverable served coordinator proof is missing: {required}",
        )


def _check_coordinator_limits(sources: Mapping[str, str]) -> None:
    mutation_runtime = sources["mutation_runtime"]
    raft = sources["raft"]
    require(
        "MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES: usize = 128 * 1024 * 1024"
        in mutation_runtime
        and "crate::server::mutation::MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES" in raft,
        "served preflight and Raft native-envelope ceilings differ",
    )


def _check_blob_result_contract(blob_store: str) -> None:
    implementation_at = blob_store.rfind("fn put_chunk_ref_batch(")
    require(implementation_at >= 0, "atomic blob chunk/reference kernel is missing")
    implementation = blob_store[implementation_at : implementation_at + 4_000]
    require(
        "self.commit_native_batch" in implementation
        and "CAS_CHUNKS" in implementation
        and "CAS_REFCOUNT" in implementation
        and "checked_add(1)" in implementation,
        "blob result kernel must atomically bind CAS, refcount, overflow, and MutationBatch",
    )
    require(
        "direct_ref_acquire_compensation_and_gc_are_restart_replay_safe" in blob_store
        and "adjust_ref_batch(&digest, -1" in blob_store,
        "direct CAS compensation restart/replay/GC proof is missing",
    )


def _check_dispatch_carrier(sources: Mapping[str, str]) -> None:
    _check_dispatch_order(sources)
    _check_dispatch_recovery_proof(sources)
    _check_coordinator_limits(sources)
    _check_blob_result_contract(sources["blob_store"])


def check_served_carrier_mutations(sources: Mapping[str, str]) -> None:
    """Reject served adapters that mutate live cores or native stores directly."""

    _check_retired_dataset_surface(sources)
    _check_current_server_surface(sources)
    _check_external_compute_contract(sources["external_compute_e2e"])
    _check_sparql_carrier(sources["sparql_http"])
    _check_ros2_carrier(sources["ros2_bridge"])
    _check_dispatch_carrier(sources)


def mutation_inventory_sources() -> dict[str, str]:
    """Load the complete live source set consumed by the inventory proof."""

    return {
        "cargo": read("Cargo.toml"),
        "capabilities": load_capability_sources(ROOT),
        "consistency": read_module_tree(
            "crates/eg-capabilities/tests/consistency.rs", include_tests=True
        ),
        "mutation_apply": read_module_tree("src/mutation_apply.rs"),
        # Hoisted 2026-08-25 (3810eb00, "Hoist durable-mutation classify/apply +
        # single-writer guard into eg-core"): the base graph-mutation set and the
        # `broker` family moved out of src/mutation_apply.rs into
        # eg_core::durable_apply. src/mutation_apply.rs now keeps only the four
        # DAG-forced families explicit and delegates everything else to this
        # module's `is_durable_mutation`/`apply` via its `_` arm — so the
        # classifier/applier inventory below must read BOTH sources and union
        # them, or it silently measures only the facade remainder (BUG-CX-112).
        "durable_apply": read_module_tree("crates/eg-core/src/durable_apply.rs"),
        "mutation_runtime": read_module_tree("src/server/mutation.rs"),
        "mutation_batch": _read_module_union(
            "src/server/mutation_batch.rs", read("src/server/mutation_batch.rs")
        ),
        "graph_ops": read_module_tree("src/server/handlers/graph_ops.rs"),
        "graph_gateway": read_module_set(
            "src/server/handlers/graph_ops",
            (
                "gateway.rs",
                "gateway_graph.rs",
                "gateway_broker.rs",
                "gateway_mining.rs",
                "gateway_mining_derived.rs",
                "gateway_mining_ml.rs",
            ),
        ),
        "dispatch": read("src/server/dispatch.rs"),
        "modality_dispatch": read("src/server/dispatch/modality_replication.rs"),
        "graph_pipeline": read("src/server/dispatch/graph_pipeline.rs"),
        "request_router": read("src/server/dispatch/request_router_data.rs"),
        "authenticated_dispatch": read("src/server/dispatch/authenticated_dispatch.rs"),
        "request_preflight": read("src/server/dispatch/request_preflight.rs"),
        "sparql_plan": read("src/server/dispatch/sparql_plan.rs"),
        "sparql_execution": read("src/server/dispatch/sparql_execution.rs"),
        # NativeMutationCommand lives below the raft facade after the command
        # extraction. Keep the facade (where consensus inventories remain) and
        # the exact native-command subtree in one scanner input.
        "raft": read("src/raft/mod.rs")
        + "\n"
        + read_module_tree("src/raft/command/native/mod.rs"),
        "sparql_http": read_module_tree("src/server/sparql_http.rs"),
        "ros2_bridge": read("src/server/ros2_bridge.rs"),
        "blob_store": read("src/server/blob/store.rs"),
        "main": read("src/main.rs"),
        "state": read("src/server/state.rs"),
        "server": read("src/server/mod.rs"),
        "external_compute_e2e": read("tests/external_compute_e2e.rs"),
    }


def check_work_item_projection_contract(graph_store: str, compiler: str) -> None:
    """A durably committed WorkItem must always be publishable to the projection.

    ``commit_work_item`` reads ``changed_work_item_ids`` from the terminal result
    AFTER the redb commit has already advanced the authoritative graph version. A
    result shape that omits the field therefore leaves the serving projection one
    version behind for good, and ``authoritative_graph_version`` then fails closed on
    every subsequent write — the whole graph goes read-only. Two mechanical rules keep
    that from recurring: every WorkItem result shape carries the field, and the
    post-commit publication repairs the projection from authority when it cannot.
    """

    body = _balanced_block(graph_store, "fn apply_work_item_rows(", "{", "}")
    for match in re.finditer(r"ResultPayload::Json\(", body):
        shape = _balanced_block(body[match.start() :], "ResultPayload::Json", "(", ")")
        require(
            "changed_work_item_ids" in shape,
            "every WorkItem durable result shape must carry changed_work_item_ids; "
            f"this one does not: {' '.join(shape.split())[:160]}",
        )

    require(
        "reconcile_projection_from_authority" in compiler
        and "read_authoritative_graph_snapshot" in compiler
        and "install_committed_snapshot" in compiler,
        "commit_work_item must repair the serving projection from the authoritative "
        "image when post-commit publication fails",
    )
    publish = _balanced_block(compiler, "fn publish_committed_work_item(", "{", "}")
    require(
        "mark_dirty()" in publish,
        "WorkItem projection publication must advance the serving version exactly once",
    )


def _check_version_fallbacks(
    persisted_sources: str, authoritative_writers: str
) -> None:
    """Reject permissive version fallbacks in every persistence writer surface."""

    for forbidden in (
        "source_graph_version.unwrap_or",
        "source_graph_version.is_some",
        "source_graph_version.or(",
        "source_graph_version: Some(",
        ".or(batch.expected_graph_version)",
        "Upgrade bridge:",
        "older version-1",
    ):
        require(
            forbidden not in persisted_sources,
            f"forbidden fallback remains: {forbidden}",
        )

    for forbidden in (
        "unwrap_or_else(|| core.version())",
        "unwrap_or_else(|| ctx.core.version())",
        "source_version.saturating_add(1)",
    ):
        require(
            forbidden not in authoritative_writers,
            f"authoritative mutation writer retains a permissive version fallback: {forbidden}",
        )


def _check_m1_identity_contract(contract: str) -> None:
    """Check the typed product-v1 mutation identity and batch shape."""

    require(
        "pub const MUTATION_BATCH_VERSION: u16 = 1;" in contract,
        "MutationBatch must use the first product typed-identity schema",
    )
    identity = _balanced_block(
        contract, "pub struct MutationScopeIdentity", "{", "}"
    )
    for field in ("tenant: TenantId", "scope: MutationScope", "incarnation_id: IncarnationId"):
        require(field in identity, f"typed mutation identity is missing {field}")
    require(
        "identity_digest: MutationScopeDigest" in identity,
        "typed mutation identity must persist its recomputable digest",
    )
    require(
        all(
            marker in contract
            for marker in (
                'b"eg/mutation-scope-identity/v1\\0"',
                'b"eg/mutation-scope-binding/v1\\0"',
                "update_lp32",
                "u32::try_from",
            )
        ),
        "mutation identity digest must use the versioned LP32 SHA-256 contract",
    )
    require(
        all(
            check
            for check in (
                '"sha256-row-delta-v1"' in contract,
                "sha256-row-delta-v2" not in contract,
                "sha256-row-delta-v3" not in contract,
            )
        ),
        "authoritative row-delta identity must expose only the product-v1 algorithm",
    )
    scope = _enum(contract, "MutationScope")
    require(
        all(
            check
            for check in (
                re.search(r"Graph\s*\{\s*graph:\s*LogicalName,?\s*\}", scope)
                is not None,
                "Native" in scope,
                "domain: MutationDomain" in scope,
                "resource: LogicalName" in scope,
            )
        ),
        "mutation scope must be tagged Graph or typed Native(domain, resource)",
    )
    require(
        all(
            marker in contract
            for marker in (
                "pub enum VersionExpectation",
                "pub enum CommittedVersion",
                "Unversioned",
                "UnversionedSystemMutation",
                "RESERVED_SYSTEM_TENANT",
            )
        ),
        "version semantics must be typed and Unversioned capability-gated",
    )
    batch = _balanced_block(contract, "pub struct MutationBatch", "{", "}")
    require("pub identity: MutationScopeIdentity" in batch, "batch identity is missing")
    require(
        "pub version_expectation: VersionExpectation" in batch,
        "batch version expectation must be typed",
    )
    for retired in (
        "pub tenant:",
        "pub graph:",
        "pub graph_incarnation_id:",
        "pub expected_graph_version:",
    ):
        require(retired not in batch, f"product batch retains duplicate flat field {retired}")
    require(
        all(
            check
            for check in (
                "#[serde(default" not in identity,
                "#[serde(default" not in batch,
                "#[serde(alias" not in contract,
            )
        ),
        "product identity must quarantine incompatible shapes without serde defaults or aliases",
    )
    require(
        all(
            marker not in contract
            for marker in ("MutationVersionScope", "NON_GRAPH_SOURCE_VERSION")
        ),
        "product contract retains implicit graph/non-graph sentinel semantics",
    )


def _check_m1_store_contract(native_store: str) -> None:
    """Check physical-root, table, binding, and quarantine invariants."""

    require(
        all(
            marker in native_store
            for marker in (
                "pub const MUTATION_STORE_SCHEMA_VERSION: u16 = 1;",
                'b"eg/mutation-store-root/v1\\0"',
                "pub struct StoreIncarnation",
                "pub struct MutationStore",
                "pub struct MutationWrite",
            )
        ),
        "native mutation store must separate physical root identity from logical bindings",
    )
    table_stems = (
        "mutation_store_root",
        "mutation_scope_bindings",
        "mutation_batches",
        "mutation_idempotency",
        "mutation_versions",
        "mutation_fences",
        "mutation_outbox",
        "mutation_private_payloads",
    )
    for stem in table_stems:
        require(
            f'TableDefinition::new("{stem}_v1")' in native_store,
            f"product mutation table is missing or not schema-one: {stem}",
        )
        require(
            f'"{stem}_v3"' in native_store,
            f"candidate prototype table is not quarantined: {stem}",
        )
    require(
        all(
            marker in native_store
            for marker in (
                "pub fn initialize<F>(",
                "pub fn bind_scope<F>(",
                "mutation scope rebinding mismatch",
                "binding_for_write(write",
            )
        ),
        "store initialization/binding and mutation entrypoints must fail closed through an owner-minted write",
    )
    require(
        all(
            check
            for check in (
                "RETIRED_PROTOTYPE_TABLES" in native_store,
                '"mutation_batches"' in native_store,
                "reject_prototype_names" in native_store,
                native_store.count(".list_tables()") >= 3,
                "quarantine before serving" in native_store,
            )
        ),
        "incompatible prototype tables must fail closed before initialization or serving",
    )


_M1_UNMIGRATED_SEMANTIC_MARKERS = (
    (
        "crates/eg-query/src/tables/semantic_authority/build.rs",
        "into_activation_parts(",
    ),
    (
        "crates/eg-query/src/tables/semantic_vectors.rs",
        "SemanticArtifactPurgeIdentity",
    ),
    (
        "crates/eg-query/src/tables/store_embedding_tables.rs",
        "pub struct EmbeddingBinding",
    ),
    (
        "crates/eg-query/src/tables/store_semantic_records.rs",
        "pub struct SemanticWorkRecord",
    ),
    ("src/server/semantic_index/coordinator.rs", "activate_generation_pair("),
    ("src/server/semantic_index/coordinator.rs", "purge_generation_pair("),
    ("src/server/semantic_index/cas_purge.rs", "purge_ann_cas_binding("),
)


def _check_m1_unmigrated_semantic_inventory() -> None:
    """Freeze the six-path W-semantic ship blocker without claiming migration."""

    for path, marker in _M1_UNMIGRATED_SEMANTIC_MARKERS:
        source = read(path)
        require(marker in source, f"unmigrated semantic bypass inventory drifted: {path}")
        require(
            "eg_mutation_store" not in source,
            f"partial semantic mutation-ledger migration is forbidden: {path}",
        )


def _check_m1_write_safety(contract: str, native_store: str) -> None:
    """Check physical derivation, authenticity, budgets, and unserved seams."""

    require(
        all(
            check
            for check in (
                "std::fs::canonicalize(path)" in native_store,
                "metadata.dev()" in native_store,
                "metadata.ino()" in native_store,
                "StoreIncarnation::new" not in native_store,
            )
        ),
        "physical store incarnation must be derived from the canonical database root",
    )
    require(
        all(
            check
            for check in (
                "pub trait PrivatePayloadIntegrity" in native_store,
                ".authenticate(sealed, digest)" in native_store,
                "private recovery payload failed canonical authentication" in native_store,
                "bytes[0]" not in native_store,
                "0xE6" not in native_store,
            )
        ),
        "private recovery authenticity must use the injected canonical integrity authority",
    )
    require(
        all(
            check
            for check in (
                "pub fn validate_write_budget" in contract,
                "fn encode_bounded" in native_store,
                "rmp_serde::encode::write_named(&mut counter" in native_store,
                native_store.count("rmp_serde::to_vec_named") == 1,
            )
        ),
        "every mutation write must preflight size/count budgets before allocating serialization",
    )
    require(
        all(
            check
            for check in (
                len(read("crates/eg-mutation-store/src/store/apply.rs").splitlines())
                < 427,
                len(read("crates/eg-mutation-store/src/store/persist.rs").splitlines())
                < 427,
                '#[path = "store/ledger.rs"]' in native_store,
                '#[path = "store/recovery.rs"]' in native_store,
            )
        ),
        "mutation apply/persistence must remain below the configured KISS limit via direct peer modules",
    )
    require(
        all(
            marker in contract
            for marker in (
                "semantic index mutations remain unserved",
                "semantic_index_remains_unserved_until_consumer_migration",
            )
        ),
        "semantic/vector/text/ANN mutation serving must remain gated until Native(SemanticIndex) consumer migration",
    )
    _check_m1_unmigrated_semantic_inventory()


def _check_m1_negative_tests(contract: str, native_store: str) -> None:
    """Require executable regressions for each fail-closed boundary."""

    for negative_test in (
        "malformed_names_are_rejected_without_normalization",
        "digest_separates_tenant_scope_kind_and_native_domain",
        "persisted_digest_tampering_is_rejected",
        "graph_domains_cannot_be_smuggled_into_native_scope",
        "semantic_domain_serde_rejects_component_aliases",
        "unauthorized_unversioned_is_rejected",
        "prototype_wire_shape_is_quarantined_without_defaults_or_aliases",
        "mismatched_rebinding_is_fail_closed",
        "partial_initialization_rolls_back_as_one_transaction",
        "incompatible_prototype_tables_are_quarantined_without_translation",
        "one_physical_root_serves_multiple_scopes_without_cross_tenant_aliasing",
        "persisted_root_digest_tampering_is_rejected_on_read",
        "backup_derives_a_distinct_physical_root_and_rebinds_scopes",
        "private_recovery_authenticity_uses_injected_canonical_authority",
        "forged_private_recovery_payload_fails_closed",
        "missing_private_integrity_authority_fails_closed",
    ):
        require(
            negative_test in contract or negative_test in native_store,
            f"missing M1 negative test: {negative_test}",
        )


def check_m1_mutation_identity(contract: str, native_store: str) -> None:
    """Freeze the universal product-v1 identity/store-root foundation."""

    _check_m1_identity_contract(contract)
    _check_m1_store_contract(native_store)
    _check_m1_write_safety(contract, native_store)
    _check_m1_negative_tests(contract, native_store)


def main() -> None:
    inventory_sources = mutation_inventory_sources()
    # `mutation_batch.rs` is a facade; the persisted structs live in its
    # declared production children.  Reading only the facade makes the gate
    # crash before it can report a contract violation whenever the module is
    # decomposed (the compiler follows the same tree).  Keep this structural
    # source union in lock-step with the live Rust module tree.
    contract = read_module_tree("crates/eg-types/src/mutation_batch.rs")
    graph_store = read_module_tree("src/redb_store.rs")
    native_store = read_module_tree("crates/eg-mutation-store/src/lib.rs")
    native_store_with_tests = read_module_tree(
        "crates/eg-mutation-store/src/lib.rs", include_tests=True
    )
    sql_store = read_module_tree("crates/eg-query/src/tables/store.rs")
    reasoning = read("src/server/reasoning_projection.rs")
    reasoning_index = read_module_tree("crates/eg-epistemic/src/incremental.rs")
    compiler = inventory_sources["mutation_batch"]
    mutation_runtime = read("src/server/mutation.rs")
    protocol = read("crates/eg-types/src/protocol.rs")
    check_m1_mutation_identity(contract, native_store_with_tests)
    require(
        "impl Default for MutationDomain" not in contract,
        "MutationDomain must not acquire an implicit default",
    )
    require(
        "SemanticIndex = 8" in contract
        and '"analytics_job",\n    "semantic_index",\n    "broker"' in contract,
        "semantic activation and purge require one closed native mutation domain",
    )
    operation = contract.split("pub struct MutationOperation", 1)[1].split("}", 1)[0]
    require("#[serde(default)]" not in operation, "operation domain must be required")
    require(
        "pub domain: MutationDomain" in operation, "operation domain field is missing"
    )
    require(
        "authoritative state target version must be exactly source version plus one"
        in contract
        and ".checked_add(1)" in contract,
        "state descriptors must advance exactly one checked graph-version step",
    )

    persisted_sources = "\n".join(
        (contract, graph_store, native_store, sql_store, reasoning, reasoning_index)
    )
    authoritative_writers = "\n".join((compiler, mutation_runtime))
    _check_version_fallbacks(persisted_sources, authoritative_writers)
    require(
        "authoritative_graph_version" in compiler
        and "projected_version == 0" in compiler
        and "does not match the serving projection" in compiler,
        "authoritative mutation writers must allow only explicit zero bootstrap and exact durable/RAM agreement",
    )
    require(
        "checked_add(1)" in graph_store
        and "checked_add(1)" in native_store
        and "checked_add(1)" in sql_store,
        "authoritative version advancement must reject overflow",
    )
    require(
        "STALE_PROJECTION_POSITION" in reasoning_index
        and "STALE_PROJECTION_CURSOR" in graph_store,
        "reasoning and durable projection watermarks must reject regression",
    )
    check_work_item_projection_contract(graph_store, compiler)
    require(
        "lower_atomic_alias" not in compiler,
        "mutation compiler must not retain retired alias lowering",
    )
    require(
        "#[serde(alias" not in protocol,
        "wire enums must accept only their canonical current names",
    )
    check_mutation_inventory(inventory_sources)
    check_served_carrier_mutations(inventory_sources)

    print("persisted mutation contract gate passed")


if __name__ == "__main__":
    main()
