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
from rust_module_tree import (
    _balanced_span_from,
    _delimiter_depths,
    _rust_code_mask,
    _rust_comments_mask,
)
from rust_module_tree import (
    read_compiler_family as _read_compiler_family,
)
from rust_module_tree import read_module_paths as _read_module_paths
from rust_module_tree import (
    read_module_tree as _read_module_tree,
)


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def read_module_tree(relative: str, *, include_tests: bool = False) -> str:
    return _read_module_tree(relative, root_dir=ROOT, include_tests=include_tests)


def read_compiler_family(relative: str):
    return _read_compiler_family(relative, root_dir=ROOT)


def read_module_paths(relative: str, *, include_tests: bool = True):
    return _read_module_paths(relative, root_dir=ROOT, include_tests=include_tests)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"persisted mutation contract gate failed: {message}")


_METHOD_VARIANT = re.compile(r"\bMethod::([A-Z][A-Za-z0-9_]*)")
_STRING_LITERAL = re.compile(r'"([A-Z][A-Za-z0-9_]*)"')
_NATIVE_CATALOG_ENTRY = re.compile(
    r"(?m)^[ \t]*(?:#\[cfg\([^\n]+\)\][ \t]*\n[ \t]*)?"
    r"(?:record|unit|write)[ \t]+([A-Z][A-Za-z0-9_]*)[ \t]+"
    r"=>[ \t]+([A-Z][A-Za-z0-9_]*)[ \t]*,$"
)
_NATIVE_CATALOG_MACRO = re.compile(r"\bmacro_rules\s*!\s*native_method_catalog\b")


def _native_method_catalog(source: str) -> dict[str, str]:
    """Parse the single Raft native-method/domain catalog fail-closed."""

    mask = _rust_code_mask(source)
    definitions = list(_NATIVE_CATALOG_MACRO.finditer(mask))
    require(len(definitions) == 1, "native method catalog must have one definition")
    body_start = mask.find("{", definitions[0].end())
    require(body_start >= 0, "native method catalog body is missing")
    body_end = _balanced_span_from(mask, body_start, "{", "}")
    macro_body = mask[body_start + 1 : body_end]
    depths = _delimiter_depths(macro_body)
    arrows = [
        position
        for position in range(len(macro_body) - 1)
        if macro_body.startswith("=>", position) and depths[position] == (0, 0, 0)
    ]
    require(
        len(arrows) == 1,
        "native method catalog must have exactly one $consumer:ident arm",
    )
    arrow = arrows[0]
    require(
        re.fullmatch(r"\s*\(\s*\$consumer\s*:\s*ident\s*\)\s*", macro_body[:arrow])
        is not None,
        "native method catalog matcher must be exactly $consumer:ident",
    )
    arm = macro_body[arrow + 2 :].strip()
    require(arm.startswith("{"), "native method catalog transcriber must be a block")
    arm_end = _balanced_span_from(arm, 0, "{", "}")
    require(
        arm[arm_end + 1 :].strip() == ";",
        "native method catalog must have exactly one transcriber",
    )
    transcriber = arm[1:arm_end].strip()
    consumer = re.match(r"\$consumer\s*!\s*\{", transcriber)
    require(
        consumer is not None,
        "native method catalog transcriber must invoke only $consumer",
    )
    catalog_start = consumer.end() - 1
    catalog_end = _balanced_span_from(transcriber, catalog_start, "{", "}")
    require(
        not transcriber[catalog_end + 1 :].strip(),
        "native method catalog transcriber contains extra tokens",
    )
    catalog = transcriber[catalog_start + 1 : catalog_end]
    entries = _NATIVE_CATALOG_ENTRY.findall(catalog)
    names = [name for name, _ in entries]
    require(
        len(names) == len(set(names)),
        "native method catalog contains a duplicate entry",
    )
    require(
        len(entries) == 99,
        f"native method catalog must contain 99 entries, observed {len(entries)}",
    )
    require("RegisterServer" not in names, "RegisterServer must remain gateway-routed")
    require(
        all(
            read_only not in names
            for read_only in (
                "CatalogList",
                "RebalancePlan",
                "PlacementRoute",
                "ClusterMembers",
                "GetMatView",
                "PlanMatViewGet",
            )
        ),
        "read-only cluster/catalog methods must remain outside the native mutation catalog",
    )
    domain_counts: dict[str, int] = {}
    for _, domain in entries:
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
    require(
        domain_counts
        == {
            "GraphState": 22,
            "Transaction": 15,
            "WorkItem": 18,
            "Blob": 6,
            "KeyValue": 3,
            "TimeSeries": 3,
            "AnalyticsJob": 1,
            "Statechart": 1,
            "SqliteCatalog": 1,
            "SessionControl": 13,
            "Identity": 2,
            "ClusterAdmin": 11,
            "GraphLifecycle": 2,
            "Multisig": 1,
        },
        f"native method catalog/domain partition drifted: {domain_counts}",
    )
    require(
        mask.count("native_method_catalog!(declare_native_consensus_methods);") == 1
        and mask.count("native_method_catalog!(declare_native_domain_classifier);") == 1
        and mask.count("native_domains!(declare_native_domains);") == 1
        and "NATIVE_DOMAIN_CONSTRUCTORS[domain as usize]" in mask,
        "native method names and domain classifier must share one catalog",
    )
    return dict(entries)


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
    mask = _rust_code_mask(source)
    match = re.search(rf"\bfn\s+{re.escape(name)}(?:\s*<[^>{{}}]*>)?\s*\(", mask)
    require(match is not None, f"missing Rust function inventory: {name}")
    start = mask.find("{", match.end())
    require(start >= 0, f"missing function body: {name}")
    end = _balanced_span_from(mask, start, "{", "}")
    return source[start + 1 : end]


_CALL_TARGET = re.compile(r"\b([a-z_][a-z0-9_]*)\s*\(")


def _function_if_present(source: str, name: str) -> str | None:
    if not re.search(
        rf"\bfn\s+{re.escape(name)}(?:\s*<[^>{{}}]*>)?\s*\(",
        _rust_code_mask(source),
    ):
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


def _direct_method_matches_set(block: str, inventory: str) -> set[str]:
    """Parse an exact ``matches!(method, Method::... | ...)`` return body.

    This is intentionally narrower than a Rust parser.  The WorkItem classifier
    is a security/durability inventory: comments, literals, helper calls,
    conditionals, multiple expressions, and a different selector must never be
    able to supply its variants.  If the implementation stops being this direct
    shape, the scanner fails closed until its semantic proof is updated.
    """

    mask = _rust_code_mask(block).strip()
    prefix = re.match(r"matches\s*!\s*\(", mask)
    require(
        prefix is not None,
        f"{inventory} must directly return matches!(method, exact variants)",
    )
    opener = prefix.end() - 1
    closer = _balanced_span_from(mask, opener, "(", ")")
    require(
        not mask[closer + 1 :].strip(),
        f"{inventory} must contain exactly one direct matches! expression",
    )
    arguments = mask[opener + 1 : closer]
    depths = _delimiter_depths(arguments)
    commas = [
        position
        for position, char in enumerate(arguments)
        if char == "," and depths[position] == (0, 0, 0)
    ]
    require(
        len(commas) == 1,
        f"{inventory} matches! must have one selector and one exact pattern",
    )
    selector = arguments[: commas[0]].strip()
    require(selector == "method", f"{inventory} must match the direct method argument")
    pattern = arguments[commas[0] + 1 :].strip()
    variant = r"Method\s*::\s*[A-Z][A-Za-z0-9_]*\s*\{\s*\.\.\s*\}"
    full_pattern = re.compile(rf"{variant}(?:\s*\|\s*{variant})*")
    require(
        full_pattern.fullmatch(pattern) is not None,
        f"{inventory} must be an exact union of Method variants",
    )
    values = _METHOD_VARIANT.findall(pattern)
    require(values, f"empty Rust Method inventory: {inventory}")
    require(
        len(values) == len(set(values)),
        f"duplicate entry in Rust inventory: {inventory}",
    )
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
    # This classifier is currently a direct `matches!` over Method variants.
    # Read that exact function rather than expecting a fictitious MethodFamily
    # indirection: changing any WorkItem variant must make this proof fail until
    # all durability inventories are reviewed together.
    work_item_classifier = _function(sources["mutation_batch"], "is_work_item_method")
    work_items = _direct_method_matches_set(work_item_classifier, "is_work_item_method")
    expected_work_items = {
        # RF-020: `KgDelegate` is an Agent Library pinned delegation that its
        # handler lowers into `SubmitWorkItem`, so it must take the same
        # natively-admitted WorkItem command-log path and must NOT be reachable
        # by the generic graph-command classify/apply contract.
        "KgDelegate",
        "SubmitWorkItem",
        "SubmitWorkItems",
        "ClaimWorkItem",
        "RenewWorkItemLease",
        "CommitWorkItemResult",
        "CancelWorkItem",
        "DeferWorkItem",
        "CasWorkItemMetadata",
    }
    require(
        work_items == expected_work_items,
        "is_work_item_method differs from the current WorkItem lifecycle: "
        f"missing={sorted(expected_work_items - work_items)}, "
        f"stale={sorted(work_items - expected_work_items)}",
    )
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
    mutation_runtime_tests = sources["mutation_runtime_tests"]
    routed = _string_set(
        _const_slice(mutation_runtime, "GATEWAY_ROUTED"), "GATEWAY_ROUTED"
    )
    coordinated_block = _const_slice(mutation_runtime_tests, "NON_GATEWAY_COORDINATED")
    coordinated = set(re.findall(r'\(\s*"([A-Z][A-Za-z0-9_]*)"\s*,', coordinated_block))
    require(coordinated, "NON_GATEWAY_COORDINATED is empty")
    open_block = _const_slice(mutation_runtime_tests, "OPEN_NOT_JUSTIFIED")
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
    gateway_methods = _method_set(
        _rust_code_mask(sources["graph_gateway_routes"]), "graph gateway routers"
    )
    gateway_body = _rust_comments_mask(sources["graph_gateway"])
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
    observed = gateway_methods | query_routes | rdf_routes | dispatch_owned
    require(
        routed <= observed,
        "served gateway ownership differs from GATEWAY_ROUTED: "
        f"missing={sorted(routed - observed)}",
    )
    # The current monolith's gateway function also contains read-only and
    # runtime-conditional arms. They are not mutation ownership, but they must
    # still be real capability rows and must not silently become mutating.
    policy = _policy_inventory(sources["capabilities"])
    extras = observed - routed
    require(
        extras <= set(policy) and all(not policy[name][0] for name in extras),
        "non-routed gateway arms must remain declared non-mutating capabilities: "
        f"invalid={sorted(name for name in extras if name not in policy or policy[name][0])}",
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
    native_consensus = set(_native_method_catalog(sources["raft"]))
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
        sources["mutation_runtime_tests"],
        "clustered_mutation_inventory_is_complete",
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


def _check_internal_graph_commit_lock(mutation_batch: str) -> None:
    """Keep internal graph commits in the same per-graph serialization lane.

    The internal commit family is a compiler-declared child of the mutation-batch
    facade. Its callers span jobs, query explanation, transactions, and program
    promotion, so the lock belongs at this one shared seam. Checking the lock's
    position relative to version discovery makes a split child fail closed when a
    future extraction drops the guard or moves it below the first authoritative read.
    """

    body = _function(mutation_batch, "commit_internal_graph_methods_with_nonce_mode")
    lock_at = body.find("lock_graph(request.graph).await")
    read_at = body.find("read_mutation_batch(")
    require(
        lock_at >= 0 and read_at >= 0 and lock_at < read_at,
        "internal graph commits must acquire lock_graph(request.graph) before the "
        "first authoritative read",
    )


def _check_internal_graph_state_payload(mutation_batch: str) -> None:
    """Keep the prepared graph delta wired into the authoritative state write.

    The staged internal-graph extraction carries the serialized row delta as a
    field on ``PreparedInternalGraphCommit``.  Check both sides of that seam so
    a future destructure split cannot silently leave the authoritative commit
    with an unbound or alternate payload.
    """

    body = _rust_code_mask(
        _function(mutation_batch, "commit_prepared_internal_graph")
    )
    prepared_at = body.find("let PreparedInternalGraphCommit {")
    input_at = body.find("} = input;", prepared_at)
    commit_at = body.find("commit_mutation_batch_state(", input_at)
    require(
        prepared_at >= 0 and input_at >= 0 and commit_at >= 0,
        "prepared internal graph commit must expose its destructure and "
        "authoritative state write",
    )
    prepared_fields = body[prepared_at:input_at]
    commit_call = body[commit_at:]
    require(
        re.search(r"\bstate_msgpack\s*,", prepared_fields) is not None
        and re.search(r"\bstate_msgpack\s*,", commit_call) is not None,
        "prepared internal graph commit must carry state_msgpack through the authoritative "
        "commit_mutation_batch_state call",
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
    _check_internal_graph_commit_lock(sources["mutation_batch"])
    _check_internal_graph_state_payload(sources["mutation_batch"])


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
    ):
        require(
            required in recovery,
            f"recoverable served coordinator proof is missing: {required}",
        )
    executable_proofs = sources["dispatch_tests"]
    for required in (
        "encrypted_sparql_preimages_survive_process_restart_and_tamper_fails",
        "durable_compensation_marker_fixes_restart_direction_and_erases_its_plan",
    ):
        require(
            required in executable_proofs,
            f"recoverable served coordinator proof is missing: {required}",
        )


def _check_coordinator_limits(sources: Mapping[str, str]) -> None:
    mutation_runtime = _rust_code_mask(sources["mutation_runtime"])
    raft = _rust_code_mask(sources["raft"])
    require(
        "MAX_NATIVE_COORDINATOR_PAYLOAD_BYTES" not in mutation_runtime,
        "mutation runtime must not own an unused native-command payload limit",
    )
    require(
        raft.count(
            "const MAX_REPLICATED_COMMAND_PAYLOAD_BYTES: usize = 128 * 1024 * 1024"
        )
        == 1,
        "native command plaintext limit must have one owner",
    )
    require(
        raft.count("MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES: usize = 64") == 1,
        "sealed native command overhead must have one owner",
    )
    require(
        re.search(
            r"MAX_REPLICATED_COMMAND_PAYLOAD_BYTES\s*"
            r"\+\s*MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES",
            raft,
        )
        is not None,
        "native command envelope must apply its named overhead",
    )


def _check_blob_result_contract(
    blob_store: str, blob_shared: str, blob_store_tests: str
) -> None:
    # This used to look for `CAS_CHUNKS`/`CAS_REFCOUNT`/`checked_add(1)` inside
    # the blob carrier's OWN local call graph, because the carrier once owned
    # those two table writes through local `insert_chunk_row`/`update_refcount`
    # helpers. They are no longer local, and that is the CORRECT direction under
    # RF-RULING-004: the CAS tables and the refcount arithmetic moved into the
    # storage kernel's shared blob handle (`eg-storage`'s `owner/blob_shared.rs`),
    # so there is one physical authority instead of a second one in the server.
    # The gate therefore asserts the same property across the seam it now spans:
    # the carrier binds the MutationBatch and does BOTH writes inside that one
    # transaction, and the kernel owns the tables and the overflow guard.
    implementation_at = blob_store.rfind("fn put_chunk_ref_batch(")
    require(implementation_at >= 0, "atomic blob chunk/reference kernel is missing")
    implementation = _function_with_callees(
        blob_store[implementation_at:], "put_chunk_ref_batch", max_depth=1
    )
    require(
        "self.commit_native_batch" in implementation
        and "blob_shared_write" in implementation
        and "insert_chunk_if_absent" in implementation
        and "adjust_refcount" in implementation,
        "blob result kernel must atomically bind CAS, refcount, overflow, and MutationBatch",
    )
    require(
        "CAS_CHUNKS" in blob_shared
        and "CAS_REFCOUNT" in blob_shared
        and "checked_add" in blob_shared
        and "shared blob reference count overflow" in blob_shared
        and "checked_sub" in blob_shared
        and "shared blob reference count underflow" in blob_shared,
        "blob result kernel must atomically bind CAS, refcount, overflow, and MutationBatch",
    )
    require(
        "direct_ref_acquire_compensation_and_gc_are_restart_replay_safe"
        in blob_store_tests
        and "adjust_ref_batch(&digest, -1" in blob_store_tests
        and "fn adjust_ref_batch(" in blob_store,
        "direct CAS compensation restart/replay/GC proof is missing",
    )


def _check_dispatch_carrier(sources: Mapping[str, str]) -> None:
    _check_dispatch_order(sources)
    _check_dispatch_recovery_proof(sources)
    _check_coordinator_limits(sources)
    _check_blob_result_contract(
        sources["blob_store"], sources["blob_shared"], sources["blob_store_tests"]
    )


def check_served_carrier_mutations(sources: Mapping[str, str]) -> None:
    """Reject served adapters that mutate live cores or native stores directly."""

    _check_retired_dataset_surface(sources)
    _check_current_server_surface(sources)
    _check_external_compute_contract(sources["external_compute_e2e"])
    _check_sparql_carrier(sources["sparql_http"])
    _check_ros2_carrier(sources["ros2_bridge"])
    _check_dispatch_carrier(sources)


_GRAPH_GATEWAY_FILES = (
    "gateway.rs",
    "gateway_broker.rs",
    "gateway_graph.rs",
    "gateway_graph_routes.rs",
    "gateway_mining.rs",
    "gateway_mining_derived.rs",
    "gateway_mining_ml.rs",
)
_GRAPH_GATEWAY_ROUTER_FILES = (
    "gateway_graph.rs",
    "gateway_graph_routes.rs",
    "gateway_broker.rs",
    "gateway_mining_ml.rs",
    "gateway_mining.rs",
)


def _graph_gateway_sources(declared_paths: set[Path]) -> tuple[str, str]:
    """Return the exact gateway family and only its live router bodies."""

    module_dir = (ROOT / "src/server/handlers/graph_ops").resolve()
    expected = {module_dir / filename for filename in _GRAPH_GATEWAY_FILES}
    discovered = {
        path
        for path in declared_paths
        if path.parent == module_dir and path.name.startswith("gateway")
    }
    require(
        discovered == expected,
        "graph gateway compiler family differs from the reviewed files: "
        f"missing={sorted(str(path) for path in expected - discovered)}, "
        f"stale={sorted(str(path) for path in discovered - expected)}",
    )
    sources = {
        filename: (module_dir / filename).read_text(encoding="utf-8")
        for filename in _GRAPH_GATEWAY_FILES
    }
    gateway_source = "\n".join(sources[filename] for filename in _GRAPH_GATEWAY_FILES)
    # Route classification and application may live in private helpers beside
    # `try_handle`; inspect each test-free router module in full so a structural
    # split cannot make owned methods disappear from this proof.
    router_source = "\n".join(
        sources[filename] for filename in _GRAPH_GATEWAY_ROUTER_FILES
    )
    return gateway_source, router_source


def mutation_inventory_sources() -> dict[str, str]:
    """Load the complete live source set consumed by the inventory proof."""

    mutation_runtime = read_compiler_family("src/server/mutation.rs")
    mutation_batch = read_compiler_family("src/server/mutation_batch.rs")
    graph_ops = read_compiler_family("src/server/handlers/graph_ops.rs")
    graph_gateway, graph_gateway_routes = _graph_gateway_sources(
        read_module_paths("src/server/handlers/graph_ops.rs", include_tests=False)
    )
    dispatch = read_compiler_family("src/server/dispatch.rs")
    raft = read_compiler_family("src/raft/mod.rs")
    blob_store = read_compiler_family("src/server/blob/store.rs")
    blob_shared = read_compiler_family("crates/eg-storage/src/owner/blob_shared.rs")

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
        "mutation_runtime": mutation_runtime.production,
        "mutation_runtime_tests": mutation_runtime.with_tests,
        "mutation_batch": mutation_batch.production,
        "mutation_batch_tests": mutation_batch.with_tests,
        # These three families may be monoliths or compiler-declared module
        # trees.  Every semantic check consumes the production view; named Rust
        # test proofs consume only the separately loaded test-inclusive view.
        "graph_ops": graph_ops.production,
        "graph_ops_tests": graph_ops.with_tests,
        "graph_gateway": graph_gateway,
        "graph_gateway_routes": graph_gateway_routes,
        "dispatch": dispatch.production,
        "dispatch_tests": dispatch.with_tests,
        "modality_dispatch": dispatch.production,
        "graph_pipeline": dispatch.production,
        "request_router": dispatch.production,
        "authenticated_dispatch": dispatch.production,
        "request_preflight": dispatch.production,
        "sparql_plan": dispatch.production,
        "sparql_execution": dispatch.production,
        "raft": raft.production,
        "raft_tests": raft.with_tests,
        "sparql_http": read_module_tree("src/server/sparql_http.rs"),
        "ros2_bridge": read("src/server/ros2_bridge.rs"),
        "blob_store": blob_store.production,
        "blob_store_tests": blob_store.with_tests,
        "blob_shared": blob_shared.production,
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


def _check_m1_identity_contract(contract: str, row_delta_producer: str) -> None:
    """Check the typed product-v1 mutation identity and batch shape."""

    require(
        "pub const MUTATION_BATCH_VERSION: u16 = 1;" in contract,
        "MutationBatch must use the first product typed-identity schema",
    )
    identity = _balanced_block(contract, "pub struct MutationScopeIdentity", "{", "}")
    for field in (
        "tenant: ScopeTenantId",
        "scope: MutationScope",
        "incarnation_id: IncarnationId",
    ):
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
    validator = _function(contract, "validate_authoritative_state")
    accepted_algorithms = set(re.findall(r'"(sha256(?:-row-delta-[^"]+)?)"', validator))
    require(
        accepted_algorithms == {"sha256", "sha256-row-delta-v2"},
        "authoritative state validator must accept exactly sha256 and sha256-row-delta-v2",
    )
    require(
        'const ROW_DELTA_ALGORITHM: &str = "sha256-row-delta-v2";' in row_delta_producer
        and "const ROW_DELTA_VERSION: u16 = 2;" in row_delta_producer,
        "row-delta producer constant/version must identify shipped v2",
    )
    for stale in (
        "sha256-row-delta-v1",
        "sha256-row-delta-v3",
        "sha256-row-delta-prototype",
    ):
        require(
            stale not in contract and stale not in row_delta_producer,
            f"retired row-delta identity remains in production source: {stale}",
        )
    scope = _enum(contract, "MutationScope")
    require(
        all(
            check
            for check in (
                re.search(r"Graph\s*\{\s*graph:\s*LogicalName,?\s*\}", scope)
                is not None,
                "Native" in scope,
                "domain: DurabilityDomain" in scope,
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
        require(
            retired not in batch,
            f"product batch retains duplicate flat field {retired}",
        )
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


# `crates/eg-mutation-store` is deleted. Its physical-authority half became
# `eg-storage`'s `StorageKernel` (kernel.rs, capability.rs, codec.rs,
# payload.rs, tables.rs, owner/*, physical/*, recovery/*); its ledger half
# became `eg-transaction`'s `MutationKernel` (admission.rs, admitted.rs,
# commit.rs, kernel.rs, ledger.rs, read.rs, replay.rs, saga.rs, tables.rs).
# `eg-storage::direct_state` and `eg-transaction::participant` are
# deliberately excluded below: neither existed in eg-mutation-store
# (`direct_state` is an unrelated new subsystem; `participant` was
# transplanted whole from tree 92a64a06's separate consensus-transaction
# intent codec, per commit b90e42a7's message), so folding either in would
# double-count marker text this gate asserts appears exactly once (e.g.
# `rmp_serde::to_vec_named`, which `eg-transaction/src/participant/
# record_codec.rs` also happens to call).
_STORAGE_KERNEL_MODULE_ROOTS: tuple[str, ...] = (
    "crates/eg-storage/src/kernel.rs",
    "crates/eg-storage/src/capability.rs",
    "crates/eg-storage/src/codec.rs",
    "crates/eg-storage/src/payload.rs",
    "crates/eg-storage/src/tables.rs",
    "crates/eg-storage/src/owner/mod.rs",
    "crates/eg-storage/src/physical/mod.rs",
    "crates/eg-storage/src/recovery/mod.rs",
)
_TRANSACTION_KERNEL_MODULE_ROOTS: tuple[str, ...] = (
    "crates/eg-transaction/src/admission.rs",
    "crates/eg-transaction/src/admitted.rs",
    "crates/eg-transaction/src/commit.rs",
    "crates/eg-transaction/src/kernel.rs",
    "crates/eg-transaction/src/ledger.rs",
    "crates/eg-transaction/src/outbox/mod.rs",
    "crates/eg-transaction/src/read.rs",
    "crates/eg-transaction/src/replay.rs",
    "crates/eg-transaction/src/saga.rs",
    "crates/eg-transaction/src/tables.rs",
)
_TRANSACTION_TESTS_ROOT = "crates/eg-transaction/src/tests/mod.rs"


def mutation_kernel_source(*, include_tests: bool = False) -> str:
    """Current-only successor to `eg-mutation-store`'s single lib.rs-rooted
    module tree: the union of `StorageKernel`'s and `MutationKernel`'s
    module trees, in that order."""

    roots = list(_STORAGE_KERNEL_MODULE_ROOTS) + list(_TRANSACTION_KERNEL_MODULE_ROOTS)
    if include_tests:
        roots.append(_TRANSACTION_TESTS_ROOT)
    return "\n".join(
        read_module_tree(root, include_tests=include_tests) for root in roots
    )


def _check_m1_store_contract(native_store: str) -> None:
    """Check physical-root, table, binding, and quarantine invariants."""

    require(
        all(
            marker in native_store
            for marker in (
                # `MUTATION_STORE_SCHEMA_VERSION` (u16 = 1) was renamed to
                # `STORAGE_KERNEL_SCHEMA_VERSION` and bumped to 2 by the same
                # commit (b90e42a7) that landed the storage/ledger key split
                # ("ledger format v2"); re-baselined to the real current
                # name/value, not merely moved.
                "pub const STORAGE_KERNEL_SCHEMA_VERSION: u16 = 2;",
                'b"eg/mutation-store-root/v1\\0"',
                "pub struct StoreIncarnation",
                # MutationStore -> MutationKernel; MutationWrite ->
                # AdmittedMutation (the capability handed back by admit()).
                "pub struct MutationKernel",
                "pub struct AdmittedMutation<'a, D: OwnerDomain>",
            )
        ),
        "native mutation store must separate physical root identity from logical bindings",
    )
    # The current kernel split owns a single live table namespace: physical
    # identity/owner tables in eg-storage and ledger/replay tables in
    # eg-transaction.  The pre-split ``*_v1`` names were retired; looking for
    # them made this gate report a missing product table even though the
    # compiler-reachable definitions in tables.rs are the active contract.
    live_tables = (
        "mutation_store_root",
        "mutation_scope_bindings",
        "mutation_owner_manifest",
        "ledger_batches",
        "ledger_maintenance",
        "ledger_versions",
        "ledger_fences",
        "ledger_outbox",
        "mutation_outbox_topic_index",
        "ledger_private_payloads",
        "mutation_outbox_consumers",
        "mutation_outbox_deliveries",
        "mutation_outbox_cursors",
        "mutation_outbox_claim_cursors",
        "mutation_outbox_fairness",
        "mutation_replay_nonces",
        "mutation_replay_operations",
        "mutation_classes",
    )
    for name in live_tables:
        require(
            f'TableDefinition::new("{name}")' in native_store,
            f"product mutation table is missing from the live kernel: {name}",
        )
    for retired in (
        "mutation_store_root_v1",
        "mutation_scope_bindings_v1",
        "mutation_batches_v1",
        "mutation_idempotency_v1",
        "mutation_versions_v1",
        "mutation_fences_v1",
        "mutation_outbox_v1",
        "mutation_private_payloads_v1",
    ):
        require(
            f'TableDefinition::new("{retired}")' not in native_store,
            f"retired product table remains in the live kernel: {retired}",
        )
    for prototype in (
        "mutation_store_root_v3",
        "mutation_scope_bindings_v3",
        "mutation_batches_v3",
        "mutation_idempotency_v3",
        "mutation_versions_v3",
        "mutation_fences_v3",
        "mutation_outbox_v3",
        "mutation_private_payloads_v3",
    ):
        require(
            f'"{prototype}"' in native_store,
            f"candidate prototype table is not quarantined: {prototype}",
        )
    require(
        all(
            marker in native_store
            for marker in (
                # The old caller-supplied closure constructors were deleted by
                # the storage-kernel split. Their current authority-bearing
                # successors are explicit owner creation/opening plus grant
                # authentication and one-time serving-scope binding.
                "pub fn create_owner<D: OwnerDomain>(",
                "pub fn open_owner<D: OwnerDomain>(",
                "pub fn authenticate_scope<D: OwnerDomain>(",
                "pub fn bind_serving_scope<D: OwnerDomain>(",
                "mutation scope rebinding mismatch",
                "pub(crate) fn binding_for_write(",
                "binding_for_write(store, transaction.get(), owner.identity())",
            )
        ),
        "store initialization/binding and mutation entrypoints must fail closed through an owner-minted write",
    )
    require(
        all(
            check
            for check in (
                "RETIRED_PROTOTYPE_TABLES" in native_store,
                "reject_prototype_names" in native_store,
                native_store.count(".list_tables()") >= 3,
                "quarantine before serving" in native_store,
            )
        ),
        "incompatible prototype tables must fail closed before initialization or serving",
    )
    retired = set(
        re.findall(
            r'"([a-z][a-z0-9_]*)"',
            _const_slice(native_store, "RETIRED_PROTOTYPE_TABLES"),
        )
    )
    require(retired, "RETIRED_PROTOTYPE_TABLES is empty")
    require(
        "mutation_batches" not in retired and "mutation_batches_v3" in retired,
        "prototype quarantine must not reject the live mutation_batches table",
    )


_M1_SEMANTIC_MARKERS = (
    (
        "query.catalog-binding",
        ("pub struct BindingRequest", "pub struct EmbeddingBinding"),
    ),
    (
        "query.binding-validation",
        ("pub fn bind(", "pub fn validate_binding("),
    ),
    (
        "server.generation-coordinator",
        ("pub fn activate_one(", "maybe_activate_after_write("),
    ),
)


def _semantic_authority_sources() -> dict[str, str]:
    # Semantic bindings and activation are separate from the SQL table-store
    # authority. Reading the whole tables module tree would pull in SQL's
    # intentional eg-storage/eg-transaction imports and misclassify them as a
    # partial semantic migration.
    query = "\n".join(
        read(path)
        for path in (
            "crates/eg-query/src/tables/embedding_binding.rs",
            "crates/eg-query/src/tables/index.rs",
            "crates/eg-query/src/tables/migration.rs",
        )
    )
    server = read("src/server/semantic_activation.rs")
    return {"query": query, "server": server}


def _check_m1_semantic_inventory(
    sources: Mapping[str, str] | None = None,
) -> None:
    """Keep semantic binding and activation authorities explicitly covered."""

    authority_sources = dict(sources or _semantic_authority_sources())
    require(
        set(authority_sources) == {"query", "server"},
        "semantic authority source groups must be exactly query and server",
    )
    missing: list[str] = []
    for authority, markers in _M1_SEMANTIC_MARKERS:
        source = authority_sources[authority.split(".", 1)[0]]
        if not all(marker in source for marker in markers):
            missing.append(authority)
    require(
        not missing,
        f"semantic authority contract is incomplete: missing={missing}",
    )
    for owner, source in authority_sources.items():
        # Semantic bindings and activation must not reach around the mutation
        # owner through a second storage or transaction authority.
        require(
            "eg_storage" not in source and "eg_transaction" not in source,
            f"semantic authority bypasses the mutation owner: {owner}",
        )


def _check_m1_write_safety(
    contract: str, native_store: str, contract_with_tests: str
) -> None:
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
                "private recovery payload failed canonical authentication"
                in native_store,
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
    # The old single-file KISS line caps were tied to the deleted
    # eg-mutation-store layout.  Preserve the architectural invariant directly:
    # admission, capability wrapping, commit, recovery validation, and reads
    # remain separate compiler-owned peers with explicit entry points.  This
    # catches a collapsed or orphaned module without making a line-count
    # threshold the contract.
    peer_modules = {
        "crates/eg-transaction/src/kernel.rs": (
            "pub struct MutationKernel",
            "pub fn admit<",
            "pub fn commit<",
        ),
        "crates/eg-transaction/src/admission.rs": (
            "pub(crate) enum AdmissionState",
            "pub(crate) fn admit_apply_batch",
            "pub(crate) fn validate_commit_admission",
        ),
        "crates/eg-transaction/src/admitted.rs": (
            "pub struct AdmittedMutation",
            "PhysicalWriteCapability",
            "pub(crate) fn commit(self)",
        ),
        "crates/eg-transaction/src/commit.rs": (
            "pub(crate) fn begin<",
            "pub(crate) fn commit<",
            "pub(crate) fn open_ledger_tables<",
        ),
        "crates/eg-storage/src/recovery/validate.rs": (
            "pub fn validate_recovery_store(",
            "pub(crate) fn validate_recovery_content(",
            "fn validate_scoped_table<",
        ),
        "crates/eg-transaction/src/read.rs": (
            "pub fn version<",
            "pub fn read_ledger<",
            "pub fn read_outbox<",
        ),
    }
    require(
        all(
            all(marker in read(path) for marker in markers)
            for path, markers in peer_modules.items()
        )
        and "mod ledger;" in read("crates/eg-transaction/src/lib.rs")
        and "mod recovery;" in read("crates/eg-storage/src/lib.rs"),
        "mutation apply/persistence lost direct peer-module ownership",
    )
    require(
        "SemanticIndex" in contract
        and "a_semantic_index_batch_is_served_on_its_own_native_scope"
        in contract_with_tests
        and "a_semantic_operation_is_refused_outside_a_semantic_scope"
        in contract_with_tests,
        "semantic index writes must use the dedicated native scope and reject graph-scope smuggling",
    )


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


def check_m1_mutation_identity(
    contract: str,
    native_store: str,
    contract_with_tests: str,
    native_store_with_tests: str,
    row_delta_producer: str,
) -> None:
    """Freeze the universal product-v1 identity/store-root foundation."""

    _check_m1_identity_contract(contract, row_delta_producer)
    _check_m1_store_contract(native_store)
    _check_m1_write_safety(contract, native_store, contract_with_tests)
    _check_m1_negative_tests(contract_with_tests, native_store_with_tests)


def main() -> None:
    inventory_sources = mutation_inventory_sources()
    # `mutation_batch.rs` is a facade; the persisted structs live in its
    # declared production children.  Reading only the facade makes the gate
    # crash before it can report a contract violation whenever the module is
    # decomposed (the compiler follows the same tree).  Keep this structural
    # source union in lock-step with the live Rust module tree.
    contract = read_module_tree("crates/eg-types/src/mutation_batch.rs")
    contract_with_tests = read_module_tree(
        "crates/eg-types/src/mutation_batch.rs", include_tests=True
    )
    graph_store = read_module_tree("src/redb_store.rs")
    native_store = mutation_kernel_source()
    native_store_with_tests = mutation_kernel_source(include_tests=True)
    sql_store = read_module_tree("crates/eg-query/src/tables/store.rs")
    reasoning = read("src/server/reasoning_projection.rs")
    reasoning_index = read_module_tree("crates/eg-epistemic/src/incremental.rs")
    row_delta_producer = read_module_tree("src/graph_delta.rs")
    compiler = inventory_sources["mutation_batch"]
    mutation_runtime = read("src/server/mutation.rs")
    protocol = read("crates/eg-types/src/protocol.rs")
    check_m1_mutation_identity(
        contract,
        native_store,
        contract_with_tests,
        native_store_with_tests,
        row_delta_producer,
    )
    _check_m1_semantic_inventory()
    require(
        "impl Default for DurabilityDomain" not in contract,
        "DurabilityDomain must not acquire an implicit default",
    )
    require(
        "SemanticIndex = 8" in contract
        and '"analytics_job",\n    "semantic_index",\n    "broker"' in contract,
        "semantic activation and purge require one closed native mutation domain",
    )
    operation = contract.split("pub struct MutationOperation", 1)[1].split("}", 1)[0]
    require("#[serde(default)]" not in operation, "operation domain must be required")
    require(
        "pub domain: DurabilityDomain" in operation, "operation domain field is missing"
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
        and "STALE_PROJECTION_CURSOR" in native_store,
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
