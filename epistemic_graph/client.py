# CONCEPT:EG-KG.query.wire-protocol — Epistemic Graph Service Client
#
# Async Python client for the Tokio-based epistemic-graph service.
# Communicates over UDS or TCP using Length-prefixed MessagePack framing
# with a signed, replay-protected request-context envelope.

from __future__ import annotations

import asyncio
import builtins
import concurrent.futures
import contextlib
import contextvars
import copy
import hashlib
import hmac
import inspect
import json
import logging
import math
import os
import secrets
import ssl
import struct
import threading
import time
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal, NamedTuple, TypedDict, cast

import msgpack

logger = logging.getLogger(__name__)


class SyncCallDeadlineExceeded(TimeoutError):
    """A synchronous client call exceeded its caller-provided deadline."""


class NativeResourceReservationUnavailable(RuntimeError):
    """The connected engine predates the dark native reservation protocol.

    Resource admission must fail closed when an older engine does not expose the
    additive methods; callers must not fall back to a local/JSON reservation.
    """

    code = "native_resource_reservation_unavailable"


class NativeCapacityLeaseUnavailable(RuntimeError):
    """The connected engine predates the native capacity lease authority."""

    code = "native_capacity_lease_unavailable"


class NativeWorkItemSubmissionUnavailable(RuntimeError):
    """The connected engine does not expose native SubmitWorkItem admission."""

    code = "native_work_item_submission_unavailable"


_SYNC_CALL_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "epistemic_graph_sync_call_deadline", default=None
)


@contextlib.contextmanager
def sync_call_deadline(timeout_s: float):
    """Bound synchronous graph calls made in this context.

    The deadline is carried by ``ContextVar``, so callers using
    :func:`asyncio.to_thread` propagate it into the synchronous worker.  On expiry
    the submitted graph coroutine is cancelled before the worker returns, avoiding
    a stranded executor worker that would otherwise delay ``asyncio.run()``
    shutdown.
    """
    if timeout_s <= 0:
        raise ValueError("sync call deadline must be positive")
    inherited = _SYNC_CALL_DEADLINE.get()
    deadline = time.monotonic() + timeout_s
    if inherited is not None:
        deadline = min(deadline, inherited)
    token = _SYNC_CALL_DEADLINE.set(deadline)
    try:
        yield
    finally:
        _SYNC_CALL_DEADLINE.reset(token)


def _sync_result_before_deadline(
    future: concurrent.futures.Future[Any],
) -> Any:
    """Return a submitted coroutine's result or cancel it at the ambient deadline."""
    deadline = _SYNC_CALL_DEADLINE.get()
    if deadline is None:
        return future.result()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        future.cancel()
        raise SyncCallDeadlineExceeded("synchronous graph call deadline expired")
    try:
        return future.result(timeout=remaining)
    except concurrent.futures.TimeoutError as exc:
        # A completed future can race the timeout; preserve its real result/error.
        if future.done():
            return future.result()
        future.cancel()
        raise SyncCallDeadlineExceeded(
            "synchronous graph call deadline expired"
        ) from exc


class _RequiredRequestContextClaims(TypedDict):
    """Authority claims bound to every request by the current wire signer."""

    principal: str
    tenant: str
    audience: str
    agent_id: str
    roles: list[str]
    scopes: list[str]
    policy_version: str
    delegation: list[str]


class RequestContextClaims(_RequiredRequestContextClaims, total=False):
    """``_RequiredRequestContextClaims`` plus the optional node-binding and
    QoS-priority claims.

    ``node`` (ADR-3 / W1.9 node-bound envelopes) is the target node id this
    envelope is minted for. It is normally supplied by the connection layer
    (``EpistemicGraphClient``/``ConnectionPool``'s ``node_id``), not by the
    caller's identity claims, so it stays optional here rather than joining
    the required base class -- most callers never set it directly.

    ``priority`` (W2.4 engine-native QoS lanes) is the advisory admission class
    this request declares -- one of the agent-utilities ``PriorityClass`` wire
    values ``"interactive"``/``"orchestration"``/``"hydration"``/
    ``"background_ingestion"``. It is MAC-covered exactly like every other claim
    (appended to the canonical envelope bytes as a distinct tag-``2`` trailer),
    so a principal cannot forge a higher class than it signed. Normally the
    agent-utilities carrier layer sets it from the ambient ``PriorityClass``
    contextvar; absent, the engine treats the request as the orchestration
    default.

    ``oidc_token`` (ADR-4 decision 5) is an optional RFC 8693 exchanged OIDC
    bearer/assertion binding ``principal``/``tenant``/``roles``/``scopes`` to a
    verified external identity. Unlike ``node``/``priority`` it does NOT ride
    the canonical MAC-covered claim set -- it is carried as a SIBLING top-level
    field on the wire envelope (``{"context": {...}, "oidc_token": "...",
    ...}``), matching the Rust decode shape (``EnvelopeV2.oidc_token`` in
    ``src/server/auth.rs``, deliberately kept out of ``build_envelope_v2_bytes``
    since the token's own RSA/JWKS signature is the trust anchor, not MAC
    coverage -- a holder of the HMAC secret gains nothing by swapping it, since
    the engine's ``bind_verified_identity`` independently verifies the token
    and rejects any subject/tenant mismatch against ``context``). Normally the
    agent-utilities delegation layer sets it from the ambient
    ``SpawnDelegation.oidc_token`` (``GraphSession._apply_spawn_delegation``);
    absent, the envelope is unchanged from before this claim existed.
    """

    node: str
    priority: str
    oidc_token: str


_REQUIRED_REQUEST_CONTEXT_FIELDS = frozenset(
    _RequiredRequestContextClaims.__required_keys__
)
_ALLOWED_REQUEST_CONTEXT_FIELDS = frozenset(RequestContextClaims.__annotations__)


def validate_request_context(
    context: RequestContextClaims | dict[str, Any],
) -> RequestContextClaims:
    """Validate and detach request authority before it reaches the signer.

    The validation mirrors the engine gate: every field is explicit, scalar
    claims and list entries are non-empty, list entries are unique, and a
    delegation chain must connect the authenticated principal to the effective
    agent. Unknown fields are rejected so deployment-specific identity data is
    not accidentally copied into the wire envelope. ``node`` is the one
    optional claim (ADR-3 / W1.9) -- present only when the caller (or the
    connection layer) knows the target node.
    """

    if not isinstance(context, dict):
        raise TypeError("verified_context must be a mapping")
    present = set(context)
    missing = sorted(_REQUIRED_REQUEST_CONTEXT_FIELDS - present)
    if missing:
        raise ValueError(
            "verified_context is missing required claims: " + ", ".join(missing)
        )
    unexpected = sorted(present - _ALLOWED_REQUEST_CONTEXT_FIELDS)
    if unexpected:
        raise ValueError(
            "verified_context contains unsupported claims: " + ", ".join(unexpected)
        )
    if "node" in context:
        node = context["node"]
        if not isinstance(node, str) or not node.strip():
            raise ValueError(
                "verified_context.node must be a non-empty string when present"
            )
    if "priority" in context:
        priority = context["priority"]
        if not isinstance(priority, str) or not priority.strip():
            raise ValueError(
                "verified_context.priority must be a non-empty string when present"
            )
    if "oidc_token" in context:
        oidc_token = context["oidc_token"]
        if not isinstance(oidc_token, str) or not oidc_token.strip():
            raise ValueError(
                "verified_context.oidc_token must be a non-empty string when present"
            )

    value: dict[str, Any] = copy.deepcopy(dict(context))
    for name in ("principal", "tenant", "audience", "agent_id", "policy_version"):
        claim = value[name]
        if not isinstance(claim, str) or not claim.strip():
            raise ValueError(f"verified_context.{name} must be a non-empty string")

    for name in ("roles", "scopes", "delegation"):
        claims = value[name]
        if not isinstance(claims, list):
            raise TypeError(f"verified_context.{name} must be a list of strings")
        seen: set[str] = set()
        for claim in claims:
            if not isinstance(claim, str) or not claim.strip():
                raise ValueError(
                    f"verified_context.{name} entries must be non-empty strings"
                )
            if claim in seen:
                raise ValueError(
                    f"verified_context.{name} contains duplicate entry {claim!r}"
                )
            seen.add(claim)

    principal = value["principal"]
    agent_id = value["agent_id"]
    delegation = value["delegation"]
    if principal == agent_id:
        if delegation:
            raise ValueError(
                "verified_context.delegation must be empty when principal is the agent"
            )
    elif (
        len(delegation) < 2 or delegation[0] != principal or delegation[-1] != agent_id
    ):
        raise ValueError(
            "verified_context.delegation must run from principal to effective agent"
        )
    return cast(RequestContextClaims, value)


_DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
_HARD_MAX_RESPONSE_BYTES = 384 * 1024 * 1024


def _bounded_env_int(name: str, default: int, hard_max: int) -> int:
    try:
        value = int(os.environ.get(name, "") or default)
    except (TypeError, ValueError):
        value = default
    return min(max(1, value), hard_max)


_MAX_RESPONSE_BYTES = _bounded_env_int(
    "EPISTEMIC_GRAPH_MAX_RESPONSE_BYTES",
    _DEFAULT_MAX_RESPONSE_BYTES,
    _HARD_MAX_RESPONSE_BYTES,
)

# ResourceStats is deliberately bounded independently of the generic transport
# frame limit.  The Rust server enforces the same finite page/tenant ceilings;
# the client repeats them before sending and validates the returned shape so an
# older or misconfigured peer cannot turn a telemetry call into an unbounded
# local object graph.
_DEFAULT_RESOURCE_STATS_LIMIT = 128
_MAX_RESOURCE_STATS_LIMIT = 1024
_MAX_RESOURCE_STATS_CURSOR_BYTES = 1024
_MAX_RESOURCE_STATS_TENANTS = 4096


_CANONICAL_BINARY_FIELDS = frozenset(
    {
        "properties_msgpack",
        "conditions_msgpack",
        "updates_msgpack",
        # Statechart::Define's payload (see `Statechart.define` below, which packs
        # the definition and sends {"Define": {"def_msgpack": <binary>}}). Omitting it
        # here left the field outside canonicalization, so the client's MAC did not
        # match the server's recomputation and every Statechart::Define was rejected
        # with "Authentication failed".
        "def_msgpack",
        "props_msgpack",
        "semantic_props_msgpack",
        "pose_msgpack",
        "action_msgpack",
        "operations_msgpack",
        "batches_msgpack",
        "msgpack",
        "files_msgpack",
        "obs_msgpack",
        "value_msgpack",
        "locus_msgpack",
        "points_msgpack",
        "left_ts_msgpack",
        "spec_msgpack",
        "pattern_msgpack",
        "wasm",
        "input",
        "data",
        "value",
    }
)
_BINARY_PAYLOAD_METHODS = frozenset(
    {
        "Publish",
        "PublishEx",
        "PublishConfirmed",
        "PublishIdempotent",
        "StreamPublish",
    }
)

_DIRECT_F32_VECTOR_FIELDS = {
    "CloseChannel": frozenset({"summary_embedding"}),
    "AddEmbedding": frozenset({"embedding"}),
    "SemanticSearch": frozenset({"query_embedding"}),
    "Discover": frozenset({"query_embedding"}),
    "TxnAddEmbedding": frozenset({"embedding"}),
}
_PLAN_F32_METHODS = frozenset(
    {
        "UnifiedQuery",
        "ExplainPlan",
        "ExplainProvenance",
        "ExplainPolicy",
        "PlanMatViewDefine",
        "TxnPlanWriteback",
        "TxnUnifiedQuery",
        "MineCluster",
        "MineAnomaly",
        "MineClassifyFit",
        "MineClassifyPredict",
        "MineReduce",
    }
)

# Current Rust `Method` fields typed `BTreeMap<String, _>` (CausalEstimate/
# CausalCounterfactual's `do_values`/`actual`) always serialize in SORTED key
# order -- a `BTreeMap` iterates that way by construction, unlike an ordinary
# Rust map. The `eg2.` MAC's canonical body hash is recomputed server-side from
# `rmp_serde::to_vec_named` of the DESERIALIZED, then re-serialized typed
# `Method` (`Method::canonical_body_bytes`), so it always sees these fields
# key-sorted -- regardless of what order the wire bytes carried them in. A
# caller-supplied Python `dict` preserves INSERTION order, so a caller passing
# e.g. ``{"z": 1.0, "x": 1.5, "y": 1.95}`` (an unsorted unit) would otherwise
# hash a different byte sequence than the server recomputes, failing with
# "Authentication failed" whenever the dict has more than one key in
# non-alphabetical insertion order. Sorting only these known fields (mirroring
# `_DIRECT_F32_VECTOR_FIELDS`'s per-method-per-field precedent) reproduces the
# server's `BTreeMap` order without guessing at the whole schema.
_BTREEMAP_SORTED_FIELDS = {
    "CausalEstimate": frozenset({"do_values"}),
    "CausalCounterfactual": frozenset({"actual", "do_values"}),
}

# These nested request fields are intentionally plain Rust ``Vec<u8>`` rather
# than ``#[serde(with = "serde_bytes")]`` in the current server protocol.  The
# transport may carry them as MessagePack ``bin`` (rmp-serde accepts bin while
# deserializing a byte sequence), but the server's typed re-serialization for
# the signed body remains an integer array.  Keep that canonical form without
# giving up the compact binary wire representation.
_CANONICAL_ARRAY_BYTE_FIELDS = frozenset(
    {"expected_metadata_msgpack", "set_metadata_msgpack"}
)


class _CanonicalF32:
    """One schema-declared Rust ``f32`` in the signed method body.

    Python's msgpack encoder represents every ordinary ``float`` as MessagePack
    float64. Rust deserializes the wire value into the method DTO first and then
    hashes ``rmp_serde::to_vec_named(Method)``, which represents ``f32`` fields as
    MessagePack float32. Retaining this marker only in the canonical signing copy
    lets both sides hash the same typed DTO without changing the transport value.
    """

    __slots__ = ("encoded",)

    def __init__(self, value: Any, *, field: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{field} must contain finite f32 values")
        try:
            encoded = struct.pack(">f", float(value))
        except (OverflowError, struct.error, ValueError) as error:
            raise ValueError(f"{field} must contain finite f32 values") from error
        if not math.isfinite(struct.unpack(">f", encoded)[0]):
            raise ValueError(f"{field} must contain finite f32 values")
        self.encoded = encoded


def _mark_f32_vector(container: dict[str, Any], field: str, *, path: str) -> None:
    value = container.get(field)
    if value is None:
        return
    if not isinstance(value, list | tuple):
        raise TypeError(f"{path} must be a list of finite f32 values")
    container[field] = [
        item
        if isinstance(item, _CanonicalF32)
        else _CanonicalF32(item, field=f"{path}[{index}]")
        for index, item in enumerate(value)
    ]


def _mark_f32_scalar(container: dict[str, Any], field: str, *, path: str) -> None:
    if field in container and not isinstance(container[field], _CanonicalF32):
        container[field] = _CanonicalF32(container[field], field=path)


def _mark_plan_f32(plan: Any, *, path: str) -> None:
    """Apply the current ``wire::Op`` f32 schema recursively to one Plan."""

    if not isinstance(plan, dict) or not isinstance(plan.get("ops"), list):
        return

    def mark_ops(ops: list[Any], *, ops_path: str) -> None:
        for index, op in enumerate(ops):
            if not isinstance(op, dict) or len(op) != 1:
                continue
            tag, payload = next(iter(op.items()))
            if not isinstance(payload, dict):
                continue
            current = f"{ops_path}[{index}].{tag}"
            if tag == "Rank":
                _mark_f32_vector(payload, "query", path=f"{current}.query")
            elif tag == "RankMmr":
                _mark_f32_scalar(payload, "lambda", path=f"{current}.lambda")
            elif tag == "FuseRrf":
                _mark_f32_scalar(payload, "k", path=f"{current}.k")
                branches = payload.get("branches")
                if isinstance(branches, list):
                    for branch_index, branch in enumerate(branches):
                        if isinstance(branch, list):
                            mark_ops(
                                branch,
                                ops_path=f"{current}.branches[{branch_index}]",
                            )

    mark_ops(plan["ops"], ops_path=f"{path}.ops")


def _sort_btreemap_field(container: dict[str, Any], field: str) -> None:
    """Reorder one ``BTreeMap``-typed field's keys to match Rust's sorted
    iteration order (see ``_BTREEMAP_SORTED_FIELDS``). No-op if absent/not a
    dict -- an omitted key is a separate (and separately handled) concern."""

    value = container.get(field)
    if isinstance(value, dict):
        container[field] = {key: value[key] for key in sorted(value)}


def _sorted_json_value(value: Any) -> Any:
    """Recursively sort every object's keys in an opaque JSON blob to match
    Rust ``serde_json::Value``'s ``Map`` -- this workspace does not enable
    serde_json's ``preserve_order`` feature (no ``indexmap`` in its Cargo.lock
    dependency list), so ``serde_json::Map`` is BTreeMap-backed and ALWAYS
    iterates/serializes an object's keys in sorted order, at every nesting
    depth, regardless of what order they were inserted in.

    A field typed plain ``serde_json::Value`` on the Rust side (e.g.
    ``VizRenderRequest::spec_json``, carrying an opaque caller-provided
    ``eg_viz_core::ViewSpec`` the wire protocol never types) is exactly such a
    blob: the server's ``eg2.`` MAC canonical-body recomputation re-serializes
    the DESERIALIZED value, which is always key-sorted -- but a caller-built
    Python dict (e.g. ``{"version": 1, "marks": [...]}``, whose insertion
    order is neither alphabetical nor Rust's declared field order, because
    there IS no declared field order for an opaque blob) hashes its own
    insertion order. The two diverge on any object with more than one key
    that isn't already alphabetically ordered, failing with "Authentication
    failed" before the request ever reaches its handler -- the same MAC class
    documented on ``_BTREEMAP_SORTED_FIELDS``, generalized to unbounded
    nesting depth since a JSON blob (unlike a flat ``BTreeMap<String, V>``
    field) has no fixed schema to enumerate known field names for."""

    if isinstance(value, dict):
        return {key: _sorted_json_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_sorted_json_value(item) for item in value]
    return value


def _reorder_dict_keys(value: Any, order: tuple[str, ...]) -> Any:
    """Return a copy of dict ``value`` with the keys in ``order`` moved first
    (in that order); any other keys keep their existing relative order after
    them. No-op for a non-dict. See ``_canonical_fitted_model`` for why this
    matters to the ``eg2.`` MAC, not just aesthetics."""

    if not isinstance(value, dict):
        return value
    ordered = {key: value[key] for key in order if key in value}
    for key, item in value.items():
        if key not in ordered:
            ordered[key] = item
    return ordered


def _canonical_decision_tree(tree: Any) -> Any:
    """Reorder one ``crate::wire::DecisionTree``/``TreeNode`` blob's keys to
    Rust's declared field order (``feature, threshold, left, right, value``
    per node) -- see ``_canonical_fitted_model``."""

    if not isinstance(tree, dict):
        return tree
    nodes = tree.get("nodes")
    if isinstance(nodes, list):
        tree = {
            **tree,
            "nodes": [
                _reorder_dict_keys(
                    node, ("feature", "threshold", "left", "right", "value")
                )
                for node in nodes
            ],
        }
    return _reorder_dict_keys(tree, ("nodes",))


def _canonical_fitted_model(model: Any) -> Any:
    """Reorder a ``crate::wire::FittedModel`` blob's dict keys to match Rust's
    struct declaration order, for THIS client's own canonical signing copy only
    (the actual wire payload -- what ``predict_estimator`` sends -- is untouched).

    ``fit_estimator``'s response and ``predict_estimator``'s ``model`` request
    argument are the identical blob shape, but a *response* payload is built
    off a ``serde_json``-shaped path (alphabetically-keyed) while the `eg2.`
    MAC's canonical body hash is recomputed from `Method::canonical_body_bytes`
    (`rmp_serde::to_vec_named`, Rust DECLARATION order) once the server
    deserializes `predict_estimator`'s request into a typed `Method`. A caller
    that -- like every real caller -- just forwards `fit_estimator`'s own
    response dict back into `predict_estimator` therefore hashes it in the
    WRONG order (e.g. a `TreeNode`'s alphabetical `feature, left, right,
    threshold, value` instead of the declared `feature, threshold, left,
    right, value`), failing with "Authentication failed" before
    `predict_estimator` ever runs -- for every non-`Linear` estimator kind
    (`Linear`'s own two fields happen to already coincide in both orders).
    """

    if not isinstance(model, dict):
        return model
    kind = model.get("kind")
    inner = model.get("model")
    if isinstance(inner, dict):
        if kind == "Linear":
            inner = _reorder_dict_keys(inner, ("coefficients", "intercept"))
        elif kind == "Tree":
            inner = _canonical_decision_tree(inner)
        elif kind == "Forest":
            trees = inner.get("trees")
            if isinstance(trees, list):
                inner = {**inner, "trees": [_canonical_decision_tree(t) for t in trees]}
            inner = _reorder_dict_keys(inner, ("trees",))
        elif kind == "GradientBoosting":
            trees = inner.get("trees")
            if isinstance(trees, list):
                inner = {**inner, "trees": [_canonical_decision_tree(t) for t in trees]}
            inner = _reorder_dict_keys(inner, ("init", "learning_rate", "trees"))
        elif kind == "AdaBoost":
            trees = inner.get("trees")
            if isinstance(trees, list):
                inner = {**inner, "trees": [_canonical_decision_tree(t) for t in trees]}
            inner = _reorder_dict_keys(inner, ("trees", "weights"))
        elif kind == "Svr":
            inner = _reorder_dict_keys(
                inner, ("support_vectors", "dual_coef", "intercept", "kernel", "gamma")
            )
    reordered = dict(model)
    if isinstance(inner, dict):
        reordered["model"] = inner
    return _reorder_dict_keys(reordered, ("kind", "model"))


def _mark_method_f32(method_wire: dict[str, Any], *, path: str = "method") -> None:
    """Mark current Rust ``f32`` fields only at typed ``Method`` schema paths."""

    method = method_wire.get("method")
    params = method_wire.get("params")
    if not isinstance(method, str) or not isinstance(params, dict):
        return

    for field in _BTREEMAP_SORTED_FIELDS.get(method, ()):
        _sort_btreemap_field(params, field)

    if method == "DsPredictEstimator" and isinstance(params.get("model"), dict):
        params["model"] = _canonical_fitted_model(params["model"])

    for field in _DIRECT_F32_VECTOR_FIELDS.get(method, ()):
        _mark_f32_vector(params, field, path=f"{path}.{method}.{field}")

    if method in _PLAN_F32_METHODS:
        _mark_plan_f32(
            params.get("plan"),
            path=f"{path}.{method}.params.plan",
        )

    if method == "Viz":
        op = params.get("op")
        render = op.get("Render") if isinstance(op, dict) else None
        if isinstance(render, dict):
            if "spec_json" in render:
                render["spec_json"] = _sorted_json_value(render["spec_json"])
            dataset = render.get("dataset")
            inline = dataset.get("InlineColumns") if isinstance(dataset, dict) else None
            if isinstance(inline, dict):
                _sort_btreemap_field(inline, "columns")

    if method == "KnowledgeStream":
        request = params.get("request")
        query = request.get("query") if isinstance(request, dict) else None
        if isinstance(query, dict) and query.get("family") == "vector":
            _mark_f32_vector(
                query,
                "query_embedding",
                path=f"{path}.KnowledgeStream.request.query.query_embedding",
            )
    elif method == "ServedModality":
        operation = params.get("op")
        predicate = operation.get("predicate") if isinstance(operation, dict) else None
        if isinstance(predicate, dict) and predicate.get("predicate") == "audio_window":
            _mark_f32_scalar(
                predicate,
                "minimum_rms",
                path=f"{path}.ServedModality.op.predicate.minimum_rms",
            )
    elif method == "ApplyChangeEnvelope":
        # The sole typed nested-Method carrier is
        # ChangeEnvelope.mutation.operations[].method. Do not recursively inspect
        # arbitrary maps: GraphQl.variables and GraphLearnPredict.model are
        # serde_json::Value and keys named ``method``/``plan`` remain ordinary JSON.
        envelope = params.get("envelope")
        mutation = envelope.get("mutation") if isinstance(envelope, dict) else None
        operations = mutation.get("operations") if isinstance(mutation, dict) else None
        if isinstance(operations, list):
            for index, operation in enumerate(operations):
                nested_method = (
                    operation.get("method") if isinstance(operation, dict) else None
                )
                if isinstance(nested_method, dict):
                    _mark_method_f32(
                        nested_method,
                        path=(
                            f"{path}.ApplyChangeEnvelope.params.envelope.mutation"
                            f".operations[{index}].method"
                        ),
                    )
    elif method == "ApplyChangeEnvelopes":
        # Plural of the above: mark f32 in each batched envelope's typed nested
        # operation methods so the batch's signed body byte-matches the server.
        envelopes = params.get("envelopes")
        if isinstance(envelopes, list):
            for env_index, envelope in enumerate(envelopes):
                mutation = (
                    envelope.get("mutation") if isinstance(envelope, dict) else None
                )
                operations = (
                    mutation.get("operations") if isinstance(mutation, dict) else None
                )
                if not isinstance(operations, list):
                    continue
                for index, operation in enumerate(operations):
                    nested_method = (
                        operation.get("method") if isinstance(operation, dict) else None
                    )
                    if isinstance(nested_method, dict):
                        _mark_method_f32(
                            nested_method,
                            path=(
                                f"{path}.ApplyChangeEnvelopes.params.envelopes"
                                f"[{env_index}].mutation.operations[{index}].method"
                            ),
                        )


def _pack_canonical_msgpack(value: Any) -> bytes:
    """Pack canonical method data with per-field f32/f64 width preserved."""

    packer = msgpack.Packer(use_bin_type=True)
    output = bytearray()

    def pack(item: Any) -> None:
        if isinstance(item, _CanonicalF32):
            output.append(0xCA)
            output.extend(item.encoded)
        elif isinstance(item, dict):
            output.extend(packer.pack_map_header(len(item)))
            for key, child in item.items():
                pack(key)
                pack(child)
        elif isinstance(item, list | tuple):
            output.extend(packer.pack_array_header(len(item)))
            for child in item:
                pack(child)
        else:
            output.extend(packer.pack(item))

    pack(value)
    return bytes(output)


def _pack_binary_msgpack(value: Any) -> bytes:
    """Encode an opaque MessagePack payload as a transport-native byte string.

    RPC fields declared as ``Vec<u8>``/``serde_bytes`` are binary payloads, not
    MessagePack arrays of integer octets.  Keeping this one helper at the client
    boundary makes every batch/lifecycle blob use the same ``bin`` encoding and
    avoids the substantial wire and signing overhead of ``list(packb(...))``.
    The payload itself remains ordinary named MessagePack, so this does not alter
    any scalar field compatibility.
    """

    return msgpack.packb(value, use_bin_type=True)


def _canonical_method_body(method: str, params: dict[str, Any] | None = None) -> bytes:
    method_wire: dict[str, Any] = {"method": method}
    if params is not None:
        method_wire["params"] = _canonicalize_method_value(params, method=method)
    _mark_method_f32(method_wire)
    return _pack_canonical_msgpack(method_wire)


def _canonicalize_method_value(value: Any, *, method: str, field: str = "") -> Any:
    """Mirror serde's binary representation for v2 method-body signatures."""
    if isinstance(value, bytes | bytearray):
        binary = bytes(value)
        if field in _CANONICAL_ARRAY_BYTE_FIELDS:
            return list(binary)
        return binary
    if isinstance(value, dict):
        return {
            key: _canonicalize_method_value(item, method=method, field=key)
            for key, item in value.items()
        }
    if isinstance(value, list):
        if field in _CANONICAL_BINARY_FIELDS or (
            field == "payload" and method in _BINARY_PAYLOAD_METHODS
        ):
            if all(isinstance(item, int) and 0 <= item <= 255 for item in value):
                return bytes(value)
        return [
            _canonicalize_method_value(item, method=method, field=field)
            for item in value
        ]
    return value


def _put_v2_text(buffer: bytearray, value: str) -> None:
    encoded = value.encode("utf-8")
    buffer.extend(len(encoded).to_bytes(4, "big"))
    buffer.extend(encoded)


def _put_v2_list(buffer: bytearray, values: list[str]) -> None:
    buffer.extend(len(values).to_bytes(4, "big"))
    for value in values:
        _put_v2_text(buffer, value)


def _put_operation_bytes(buffer: bytearray, value: bytes) -> None:
    buffer.extend(len(value).to_bytes(8, "big"))
    buffer.extend(value)


def _put_operation_list(buffer: bytearray, values: list[str]) -> None:
    _put_operation_bytes(buffer, len(values).to_bytes(8, "big"))
    for value in values:
        _put_operation_bytes(buffer, value.encode("utf-8"))


def _validate_explicit_string_list(name: str, values: Any) -> list[str]:
    if not isinstance(values, list):
        raise TypeError(f"{name} must be an explicit list of strings")
    seen: set[str] = set()
    detached: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} entries must be non-empty strings")
        if value in seen:
            raise ValueError(f"{name} contains a duplicate entry")
        seen.add(value)
        detached.append(value)
    return detached


def _logical_source_name(value: Any) -> str:
    """Return a portable source identifier that cannot expose a host path."""

    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError("source name must be a non-empty logical identifier")
    if "\x00" in value or value.startswith("~") or value.casefold().startswith("file:"):
        raise ValueError("source name must not identify a host filesystem location")
    windows_path = PureWindowsPath(value)
    if (
        PurePosixPath(value).is_absolute()
        or windows_path.is_absolute()
        or bool(windows_path.drive)
    ):
        raise ValueError("source name must not identify a host filesystem location")
    normalized = value.replace("\\", "/")
    if any(part in {".", ".."} for part in normalized.split("/")):
        raise ValueError("source name must not contain filesystem traversal segments")
    return normalized


class KnowledgeGraphQuery(TypedDict):
    family: Literal["graph"]
    label: str
    limit: int


class KnowledgeSqlQuery(TypedDict):
    family: Literal["sql"]
    query: str
    params_msgpack: bytes


class KnowledgeRdfQuery(TypedDict):
    family: Literal["rdf"]
    query: str
    base_iri: str
    type_convention: str


class KnowledgeVectorQuery(TypedDict):
    family: Literal["vector"]
    keywords: list[str]
    query_embedding: list[float]
    k: int


KnowledgeTimeSeriesQuery = TypedDict(
    "KnowledgeTimeSeriesQuery",
    {
        "family": Literal["time_series"],
        "series_id": str,
        "from": int,
        "to": int,
    },
)


class KnowledgeJobQuery(TypedDict):
    family: Literal["job"]
    job_id: str


class KnowledgeCrossModalQuery(TypedDict):
    family: Literal["cross_modal"]
    text: str


KnowledgeStreamQuery = (
    KnowledgeGraphQuery
    | KnowledgeSqlQuery
    | KnowledgeRdfQuery
    | KnowledgeVectorQuery
    | KnowledgeTimeSeriesQuery
    | KnowledgeJobQuery
    | KnowledgeCrossModalQuery
)


class KnowledgeStreamCursor(TypedDict):
    schema_version: Literal[1]
    family: Literal[
        "graph", "sql", "rdf", "vector", "time_series", "job", "cross_modal"
    ]
    integrity_ref: str
    tenant_ref: str
    access_policy_ref: str
    placement_ref: str
    snapshot_ref: str
    query_ref: str
    derivation_ref: str
    evidence_set_ref: str
    batch_size: int
    row_offset: int
    batch_index: int
    exhausted: bool


class KnowledgeStreamBatch(TypedDict):
    schema_version: Literal[1]
    family: Literal[
        "graph", "sql", "rdf", "vector", "time_series", "job", "cross_modal"
    ]
    projection: Literal["arrow_ipc_v1"]
    cursor: KnowledgeStreamCursor
    payload: bytes


class ModalityAuthority(TypedDict):
    tenant_ref: str
    access_policy_ref: str
    purpose_ref: str
    maximum_classification: Literal["public", "internal", "confidential", "restricted"]


class ModalityApplyOutcome(TypedDict):
    disposition: Literal["Applied", "IdempotentReplay"]
    observation_version: int
    event_sequence: int


class ServedModalityRecord(TypedDict):
    occurrence_id: str
    observation_version: int
    lifecycle: Literal["active", "cold"]
    bundle: dict[str, Any]
    value: dict[str, Any]


class ServedModalityPage(TypedDict):
    records: list[ServedModalityRecord]
    next: str | None


class ServedModalityEvent(TypedDict):
    sequence: int
    occurrence_id: str
    observation_version: int
    kind: Literal[
        "ingested", "updated", "deleted", "moved_to_cold", "restored", "reindexed"
    ]
    tenant_ref: str
    access_policy_ref: str


class ServedModalityStats(TypedDict):
    active_records: int
    total_records: int
    tombstoned_records: int
    modality_index_postings: int
    segment_index_postings: int
    native_index_keys: int
    native_index_postings: int
    events: int
    snapshot_bytes: int


class ServedModalityCapabilities(TypedDict):
    component_ready: Literal[True]
    component_pass: int
    component_not_applicable: int
    component_total: int


_KNOWLEDGE_FAMILIES = frozenset(
    {"graph", "sql", "rdf", "vector", "time_series", "job", "cross_modal"}
)
_SERVED_MODALITIES = frozenset({"document", "image", "audio", "video"})
_SERVED_SEGMENTS = frozenset(
    {
        "page",
        "paragraph",
        "table",
        "row",
        "region",
        "audio_range",
        "video_shot",
        "frame_range",
        "time_window",
        "code_symbol",
        "trace_span",
    }
)
_MAX_NATIVE_TEMPORAL_BUCKETS = 4_096
_ARTIFACT_BUNDLE_FIELDS = frozenset(
    {
        "protocol_version",
        "privacy",
        "artifacts",
        "occurrences",
        "renditions",
        "segments",
        "features",
        "evidence_loci",
    }
)


def _exact_mapping(name: str, value: Any, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    present = set(value)
    missing = sorted(fields - present)
    if missing:
        raise ValueError(f"{name} is missing required fields: {', '.join(missing)}")
    unexpected = sorted(present - fields)
    if unexpected:
        raise ValueError(f"{name} contains unsupported fields: {', '.join(unexpected)}")
    return dict(value)


def _closed_mapping(
    name: str,
    value: Any,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Validate a closed current schema whose optional fields are still explicit."""

    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    present = set(value)
    missing = sorted(required - present)
    if missing:
        raise ValueError(f"{name} is missing required fields: {', '.join(missing)}")
    unexpected = sorted(present - required - optional)
    if unexpected:
        raise ValueError(f"{name} contains unsupported fields: {', '.join(unexpected)}")
    return dict(value)


def _string(name: str, value: Any, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not allow_empty and (not value or value != value.strip()):
        raise ValueError(f"{name} must be a non-empty trimmed string")
    return value


def _integer(
    name: str,
    value: Any,
    *,
    minimum: int = 0,
    maximum: int = (1 << 64) - 1,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _canonical_profile_version(name: str, value: Any) -> str:
    value = _string(name, value)
    try:
        parsed = int(value, 10)
    except ValueError as exc:
        raise ValueError(f"{name} must be a canonical integer") from exc
    if parsed < 0 or str(parsed) != value:
        raise ValueError(f"{name} must be a canonical integer")
    return value


def _boolean(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


_WORK_ITEM_CAPABILITY_DECISIONS = frozenset(
    {
        "minted",
        "replayed",
        "verified",
        "input_conflict",
        "not_found",
        "unauthorized",
        "expired",
        "stale",
        "malformed",
        "retention_exhausted",
    }
)


def _work_item_capability_result(
    value: Any,
    *,
    verify: bool,
) -> dict[str, Any]:
    """Validate the generated capability result without modeling authority.

    This is intentionally an internal wire-shape validator. It has no fields
    for tenant, owner, lease, fence, attempt, or graph incarnation, and thus
    cannot become a caller-constructible authority object.
    """
    result = _exact_mapping(
        "WorkItemClaimCapability result",
        value,
        frozenset({"schema_version", "decision", "valid", "capability"}),
    )
    if result["schema_version"] != "1":
        raise ValueError("WorkItemClaimCapability result schema_version must be 1")
    decision = _string("WorkItemClaimCapability result.decision", result["decision"])
    if decision not in _WORK_ITEM_CAPABILITY_DECISIONS:
        raise ValueError("WorkItemClaimCapability result decision is invalid")
    valid = _boolean("WorkItemClaimCapability result.valid", result["valid"])
    capability = result["capability"]
    if capability is not None:
        if isinstance(capability, bytearray):
            capability = bytes(capability)
            result["capability"] = capability
        if not isinstance(capability, bytes) or len(capability) > 128:
            raise ValueError("WorkItemClaimCapability result.capability is invalid")
    if verify:
        if decision not in {"verified", "unauthorized"}:
            raise ValueError(
                "WorkItemClaimCapability verify result decision is invalid"
            )
        if decision == "verified" and not valid:
            raise ValueError("verified capability result must be valid")
        if decision == "unauthorized" and valid:
            raise ValueError("unauthorized capability result must be invalid")
        if capability is not None:
            raise ValueError("verify result must not return capability bytes")
    elif decision in {"minted", "replayed"}:
        if not valid or not isinstance(capability, bytes) or len(capability) != 36:
            raise ValueError("minted capability result is incomplete")
    elif valid or capability is not None:
        raise ValueError("refused capability result carries authority state")
    return result


_RESOURCE_RESERVATION_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "fence",
        "lease_epoch",
        "fencing_token",
        "attempt",
        "reservation_id",
        "input_fingerprint",
        "profile_name",
        "profile_version",
        "host_ref",
        "requirement",
        "target_kind",
        "target_alias",
        "repository_id",
        "branch",
        "concurrency_key",
        "concurrency_limit",
        "repository_exclusive",
        "branch_exclusive",
        "required_labels",
        "anti_affinity",
        "fairness_group",
        "fairness_cost",
        "disk_low_watermark_mib",
        "disk_high_watermark_mib",
        "disk_policy_key",
        "reserved_at_ms",
        "expires_at_ms",
        "idempotency_key",
        "now_ms",
        "expected_host_revision",
        "expected_lifecycle_revision",
    }
)


def _resource_reservation_request(value: Any) -> dict[str, Any]:
    request = _exact_mapping(
        "ResourceReservation request", value, _RESOURCE_RESERVATION_REQUEST_FIELDS
    )
    if request["schema_version"] != "1":
        raise ValueError("ResourceReservation schema_version must be 1")
    for field in (
        "tenant_ref",
        "work_item_id",
        "owner_id",
        "fence",
        "reservation_id",
        "profile_name",
        "profile_version",
        "host_ref",
        "target_kind",
        "repository_id",
        "branch",
        "concurrency_key",
        "fairness_group",
        "disk_policy_key",
        "idempotency_key",
    ):
        _string(f"ResourceReservation.{field}", request[field])
        if len(request[field]) > 256:
            raise ValueError(
                f"ResourceReservation.{field} exceeds the 256-character bound"
            )
    _canonical_profile_version(
        "ResourceReservation.profile_version", request["profile_version"]
    )
    fingerprint = _string(
        "ResourceReservation.input_fingerprint", request["input_fingerprint"]
    )
    if len(fingerprint) != 67 or not fingerprint.startswith("v1:"):
        raise ValueError("ResourceReservation.input_fingerprint must use v1 namespace")
    if any(char not in "0123456789abcdef" for char in fingerprint[3:]):
        raise ValueError("ResourceReservation.input_fingerprint must be lowercase hex")
    for field in ("lease_epoch", "fencing_token"):
        _integer(f"ResourceReservation.{field}", request[field])
    _integer("ResourceReservation.attempt", request["attempt"], minimum=1)
    requirement = _exact_mapping(
        "ResourceReservation.requirement",
        request["requirement"],
        frozenset({"cpu_weight", "memory_mib", "disk_mib", "process_slots"}),
    )
    for field in ("cpu_weight", "memory_mib", "disk_mib", "process_slots"):
        _integer(
            f"ResourceReservation.requirement.{field}", requirement[field], minimum=1
        )
    request["requirement"] = requirement
    if request["target_alias"] is not None:
        _string("ResourceReservation.target_alias", request["target_alias"])
    if request["target_kind"] not in {"local", "inventory_alias"}:
        raise ValueError("ResourceReservation.target_kind is invalid")
    if (request["target_kind"] == "local") != (request["target_alias"] is None):
        raise ValueError("ResourceReservation target_alias does not match target_kind")
    if request["concurrency_limit"] is not None:
        _integer(
            "ResourceReservation.concurrency_limit",
            request["concurrency_limit"],
            minimum=1,
        )
    for field in ("repository_exclusive", "branch_exclusive"):
        _boolean(f"ResourceReservation.{field}", request[field])
    for field in ("required_labels", "anti_affinity"):
        labels = request[field]
        if (
            not isinstance(labels, list)
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in labels
            )
            or len(labels) != len(set(labels))
            or len(labels) > 128
            or any(len(item) > 256 for item in labels)
        ):
            raise ValueError(f"ResourceReservation.{field} must be unique strings")
    _integer("ResourceReservation.fairness_cost", request["fairness_cost"], minimum=1)
    for field in ("disk_low_watermark_mib", "disk_high_watermark_mib"):
        if request[field] is not None:
            _integer(f"ResourceReservation.{field}", request[field])
    if (
        request["disk_low_watermark_mib"] is not None
        and request["disk_high_watermark_mib"] is not None
        and request["disk_low_watermark_mib"] > request["disk_high_watermark_mib"]
    ):
        raise ValueError(
            "ResourceReservation disk low watermark exceeds high watermark"
        )
    _integer("ResourceReservation.reserved_at_ms", request["reserved_at_ms"])
    _integer("ResourceReservation.expires_at_ms", request["expires_at_ms"], minimum=1)
    _integer("ResourceReservation.now_ms", request["now_ms"])
    if request["expires_at_ms"] <= request["reserved_at_ms"]:
        raise ValueError("ResourceReservation expiry must be after reservation time")
    for field in ("expected_host_revision", "expected_lifecycle_revision"):
        if request[field] is not None:
            _integer(f"ResourceReservation.{field}", request[field])
    return request


_RESOURCE_RESERVATION_DECISIONS = frozenset(
    {
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "capacity",
        "policy",
        "drained",
        "quarantined",
        "stale_host",
        "labels",
        "anti_affinity",
        "disk",
        "concurrency",
        "exclusivity",
        "not_found",
    }
)


def _resource_reservation_record(value: Any) -> dict[str, Any]:
    record = _exact_mapping(
        "ResourceReservation record",
        value,
        frozenset(
            {
                "reservation_id",
                "tenant_ref",
                "owner_id",
                "work_item_id",
                "fence",
                "attempt",
                "lease_epoch",
                "fencing_token",
                "input_fingerprint",
                "host_ref",
                "profile_name",
                "profile_version",
                "requirement",
                "capacity_snapshot",
                "selected_target",
                "target_kind",
                "target_alias",
                "repository_id",
                "branch",
                "concurrency_key",
                "concurrency_limit",
                "repository_exclusive",
                "branch_exclusive",
                "required_labels",
                "anti_affinity",
                "fairness_group",
                "fairness_cost",
                "disk_low_watermark_mib",
                "disk_high_watermark_mib",
                "disk_policy_key",
                "reserved_at_ms",
                "expires_at_ms",
                "state",
                "revision",
                "lifecycle_revision",
                "tombstone",
            }
        ),
    )
    for field in (
        "reservation_id",
        "tenant_ref",
        "owner_id",
        "work_item_id",
        "fence",
        "host_ref",
        "profile_name",
        "profile_version",
        "target_kind",
        "repository_id",
        "branch",
        "concurrency_key",
        "fairness_group",
        "disk_policy_key",
    ):
        _string(f"ResourceReservation record.{field}", record[field])
        if len(record[field]) > 256:
            raise ValueError(
                f"ResourceReservation record.{field} exceeds the 256-character bound"
            )
    _canonical_profile_version(
        "ResourceReservation record.profile_version", record["profile_version"]
    )
    fingerprint = _string(
        "ResourceReservation record.input_fingerprint", record["input_fingerprint"]
    )
    if (
        len(fingerprint) != 67
        or not fingerprint.startswith("v1:")
        or any(char not in "0123456789abcdef" for char in fingerprint[3:])
    ):
        raise ValueError("ResourceReservation record input_fingerprint is invalid")
    for field in ("attempt", "revision"):
        _integer(f"ResourceReservation record.{field}", record[field], minimum=1)
    for field in (
        "lease_epoch",
        "fencing_token",
        "lifecycle_revision",
        "reserved_at_ms",
        "expires_at_ms",
        "fairness_cost",
    ):
        _integer(
            f"ResourceReservation record.{field}",
            record[field],
            minimum=1 if field == "fairness_cost" else 0,
        )
    requirement = _exact_mapping(
        "ResourceReservation record.requirement",
        record["requirement"],
        frozenset({"cpu_weight", "memory_mib", "disk_mib", "process_slots"}),
    )
    for field in requirement:
        _integer(
            f"ResourceReservation record.requirement.{field}",
            requirement[field],
            minimum=1,
        )
    snapshot = _exact_mapping(
        "ResourceReservation record.capacity_snapshot",
        record["capacity_snapshot"],
        frozenset(
            {"cpu_weight", "memory_mib", "disk_mib", "process_slots", "host_revision"}
        ),
    )
    for field in snapshot:
        _integer(
            f"ResourceReservation record.capacity_snapshot.{field}", snapshot[field]
        )
    selected = _exact_mapping(
        "ResourceReservation record.selected_target",
        record["selected_target"],
        frozenset({"kind", "alias", "capability_labels"}),
    )
    if selected["kind"] not in {"local", "inventory_alias"}:
        raise ValueError("ResourceReservation record.selected_target.kind is invalid")
    if (selected["kind"] == "local") != (selected["alias"] is None):
        raise ValueError("ResourceReservation record.selected_target alias is invalid")
    if selected["alias"] is not None:
        _string("ResourceReservation record.selected_target.alias", selected["alias"])
    labels = selected["capability_labels"]
    if (
        not isinstance(labels, list)
        or len(labels) > 128
        or len(labels) != len(set(labels))
    ):
        raise ValueError(
            "ResourceReservation record.selected_target.capability_labels is invalid"
        )
    for label in labels:
        _string("ResourceReservation selected target label", label)
    if record["target_alias"] is not None:
        _string("ResourceReservation record.target_alias", record["target_alias"])
    if record["target_kind"] not in {"local", "inventory_alias"}:
        raise ValueError("ResourceReservation record.target_kind is invalid")
    if (record["target_kind"] == "local") != (record["target_alias"] is None):
        raise ValueError(
            "ResourceReservation record target_alias does not match target_kind"
        )
    if record["concurrency_limit"] is not None:
        _integer(
            "ResourceReservation record.concurrency_limit",
            record["concurrency_limit"],
            minimum=1,
        )
    for field in ("repository_exclusive", "branch_exclusive", "tombstone"):
        _boolean(f"ResourceReservation record.{field}", record[field])
    for field in ("required_labels", "anti_affinity"):
        labels = record[field]
        if (
            not isinstance(labels, list)
            or len(labels) > 128
            or len(labels) != len(set(labels))
            or any(
                not isinstance(item, str)
                or not item
                or item != item.strip()
                or len(item) > 256
                for item in labels
            )
        ):
            raise ValueError(f"ResourceReservation record.{field} is invalid")
    for field in ("disk_low_watermark_mib", "disk_high_watermark_mib"):
        if record[field] is not None:
            _integer(f"ResourceReservation record.{field}", record[field])
    if record["state"] not in {
        "reserved",
        "released",
        "reclaimed",
        "expired",
        "superseded",
        "absent",
    }:
        raise ValueError("ResourceReservation record state is invalid")
    return record


def _resource_reservation_result(value: Any) -> dict[str, Any]:
    result = _exact_mapping(
        "ResourceReservation result",
        value,
        frozenset(
            {
                "schema_version",
                "decision",
                "reservation_id",
                "work_item_id",
                "attempt",
                "lease_epoch",
                "fencing_token",
                "lifecycle_revision",
                "host_ref",
                "host_revision",
                "record",
                "state",
                "held_cpu_weight",
                "held_memory_mib",
                "held_disk_mib",
                "held_process_slots",
                "fairness_debt",
                "tombstone",
                "changed_work_item_ids",
            }
        ),
    )
    if result["schema_version"] != "1":
        raise ValueError("ResourceReservation result schema_version must be 1")
    if result["decision"] not in _RESOURCE_RESERVATION_DECISIONS:
        raise ValueError("ResourceReservation result decision is invalid")
    _string("ResourceReservation result.work_item_id", result["work_item_id"])
    if len(result["work_item_id"]) > 256:
        raise ValueError(
            "ResourceReservation result.work_item_id exceeds the 256-character bound"
        )
    if result["reservation_id"] is not None:
        _string("ResourceReservation result.reservation_id", result["reservation_id"])
        if len(result["reservation_id"]) > 256:
            raise ValueError(
                "ResourceReservation result.reservation_id exceeds the 256-character bound"
            )
    for field in (
        "attempt",
        "lease_epoch",
        "fencing_token",
        "lifecycle_revision",
        "host_revision",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "fairness_debt",
    ):
        _integer(f"ResourceReservation result.{field}", result[field])
    if result["host_ref"] is not None:
        _string("ResourceReservation result.host_ref", result["host_ref"])
        if len(result["host_ref"]) > 256:
            raise ValueError(
                "ResourceReservation result.host_ref exceeds the 256-character bound"
            )
    if result["record"] is not None:
        _resource_reservation_record(result["record"])
    if result["state"] not in {
        "reserved",
        "released",
        "reclaimed",
        "expired",
        "superseded",
        "absent",
    }:
        raise ValueError("ResourceReservation result state is invalid")
    _boolean("ResourceReservation result.tombstone", result["tombstone"])
    changed = result["changed_work_item_ids"]
    if (
        not isinstance(changed, list)
        or len(changed) > 128
        or any(
            not isinstance(item, str)
            or not item
            or item != item.strip()
            or len(item) > 256
            for item in changed
        )
        or len(changed) != len(set(changed))
    ):
        raise ValueError("ResourceReservation result changed ids are invalid")
    return result


def _resource_status_request(value: Any) -> dict[str, Any]:
    request = _exact_mapping(
        "ResourceReservationStatus request",
        value,
        frozenset(
            {
                "schema_version",
                "tenant_ref",
                "work_item_id",
                "reservation_id",
                "host_ref",
                "owner_id",
                "fence",
                "attempt",
                "lease_epoch",
                "fencing_token",
                "input_fingerprint",
                "fairness_group",
                "limit",
                "cursor",
                "now_ms",
            }
        ),
    )
    if request["schema_version"] != "1":
        raise ValueError("ResourceReservationStatus schema_version must be 1")
    _string("ResourceReservationStatus.tenant_ref", request["tenant_ref"])
    if len(request["tenant_ref"]) > 256:
        raise ValueError(
            "ResourceReservationStatus.tenant_ref exceeds the 256-character bound"
        )
    for field in (
        "work_item_id",
        "reservation_id",
        "host_ref",
        "owner_id",
        "fence",
        "cursor",
        "fairness_group",
    ):
        if request[field] is not None:
            _string(f"ResourceReservationStatus.{field}", request[field])
            if len(request[field]) > 256:
                raise ValueError(
                    f"ResourceReservationStatus.{field} exceeds the 256-character bound"
                )
    if request["input_fingerprint"] is not None:
        fingerprint = _string(
            "ResourceReservationStatus.input_fingerprint",
            request["input_fingerprint"],
        )
        if (
            len(fingerprint) != 67
            or not fingerprint.startswith("v1:")
            or any(char not in "0123456789abcdef" for char in fingerprint[3:])
        ):
            raise ValueError("ResourceReservationStatus input_fingerprint is invalid")
    if request["fairness_group"] is not None:
        _string("ResourceReservationStatus.fairness_group", request["fairness_group"])
        if len(request["fairness_group"]) > 256:
            raise ValueError(
                "ResourceReservationStatus.fairness_group exceeds the 256-character bound"
            )
    for field in ("attempt", "lease_epoch", "fencing_token"):
        if request[field] is not None:
            _integer(
                f"ResourceReservationStatus.{field}",
                request[field],
                minimum=1 if field == "attempt" else 0,
            )
    _integer(
        "ResourceReservationStatus.limit", request["limit"], minimum=1, maximum=1000
    )
    _integer("ResourceReservationStatus.now_ms", request["now_ms"])
    if (
        request["work_item_id"] is None
        and request["reservation_id"] is None
        and request["host_ref"] is None
    ):
        raise ValueError(
            "ResourceReservationStatus requires a reservation, WorkItem, or host filter"
        )
    return request


def _resource_reservation_status_result(value: Any) -> dict[str, Any]:
    result = _exact_mapping(
        "ResourceReservationStatus result",
        value,
        frozenset(
            {
                "schema_version",
                "complete",
                "next_cursor",
                "host_snapshot",
                "host_ref",
                "host_revision",
                "held_cpu_weight",
                "held_memory_mib",
                "held_disk_mib",
                "held_process_slots",
                "fairness_debt",
                "reservations",
                "orphan_count",
                "superseded_count",
            }
        ),
    )
    if result["schema_version"] != "1":
        raise ValueError("ResourceReservationStatus result schema_version must be 1")
    _boolean("ResourceReservationStatus result.complete", result["complete"])
    if result["next_cursor"] is not None:
        _string("ResourceReservationStatus result.next_cursor", result["next_cursor"])
        if len(result["next_cursor"]) > 256:
            raise ValueError(
                "ResourceReservationStatus result.next_cursor exceeds the 256-character bound"
            )
    if result["host_ref"] is not None:
        _string("ResourceReservationStatus result.host_ref", result["host_ref"])
        if len(result["host_ref"]) > 256:
            raise ValueError(
                "ResourceReservationStatus result.host_ref exceeds the 256-character bound"
            )
    _resource_host_snapshot(
        result["host_snapshot"],
        "ResourceReservationStatus result.host_snapshot",
    )
    for field in (
        "host_revision",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
        "fairness_debt",
        "orphan_count",
        "superseded_count",
    ):
        _integer(f"ResourceReservationStatus result.{field}", result[field])
    reservations = result["reservations"]
    if not isinstance(reservations, list) or len(reservations) > 1000:
        raise TypeError("ResourceReservationStatus result.reservations must be a list")
    summary_fields = frozenset(
        {
            "reservation_id",
            "work_item_id",
            "attempt",
            "host_ref",
            "profile_name",
            "fairness_group",
            "state",
            "revision",
            "expires_at_ms",
            "held_cpu_weight",
            "held_memory_mib",
            "held_disk_mib",
            "held_process_slots",
            "tombstone",
        }
    )
    for index, summary in enumerate(reservations):
        summary = _exact_mapping(
            f"ResourceReservationStatus result.reservations[{index}]",
            summary,
            summary_fields,
        )
        _string(
            f"reservation summary {index}.reservation_id", summary["reservation_id"]
        )
        _string(f"reservation summary {index}.work_item_id", summary["work_item_id"])
        _string(f"reservation summary {index}.host_ref", summary["host_ref"])
        _string(f"reservation summary {index}.profile_name", summary["profile_name"])
        _string(
            f"reservation summary {index}.fairness_group", summary["fairness_group"]
        )
        if any(
            len(summary[field]) > 256
            for field in (
                "reservation_id",
                "work_item_id",
                "host_ref",
                "profile_name",
                "fairness_group",
            )
        ):
            raise ValueError(
                f"reservation summary {index} contains an overlong identifier"
            )
        _integer(f"reservation summary {index}.attempt", summary["attempt"], minimum=1)
        _integer(
            f"reservation summary {index}.revision", summary["revision"], minimum=1
        )
        _integer(f"reservation summary {index}.expires_at_ms", summary["expires_at_ms"])
        for field in (
            "held_cpu_weight",
            "held_memory_mib",
            "held_disk_mib",
            "held_process_slots",
        ):
            _integer(f"reservation summary {index}.{field}", summary[field])
        if summary["state"] not in {
            "reserved",
            "released",
            "reclaimed",
            "expired",
            "superseded",
            "absent",
        }:
            raise ValueError(f"reservation summary {index}.state is invalid")
        _boolean(f"reservation summary {index}.tombstone", summary["tombstone"])
    return result


def _resource_host_snapshot(value: Any, name: str) -> dict[str, Any] | None:
    """Validate the bounded, non-authoritative host reconciliation projection."""
    if value is None:
        return None
    snapshot = _exact_mapping(
        name,
        value,
        frozenset(
            {
                "host_ref",
                "revision",
                "capacity",
                "observed",
                "heartbeat_at_ms",
                "heartbeat_ttl_ms",
                "draining",
                "quarantined",
                "labels",
                "target_kind",
                "target_alias",
                "disk_used_mib",
                "disk_capacity_mib",
                "held_cpu_weight",
                "held_memory_mib",
                "held_disk_mib",
                "held_process_slots",
                "disk_policies",
            }
        ),
    )
    _string(f"{name}.host_ref", snapshot["host_ref"])
    if len(snapshot["host_ref"]) > 256:
        raise ValueError(f"{name}.host_ref exceeds the 256-character bound")
    _integer(f"{name}.revision", snapshot["revision"])
    for capacity_name in ("capacity", "observed"):
        capacity = _exact_mapping(
            f"{name}.{capacity_name}",
            snapshot[capacity_name],
            frozenset({"cpu_weight", "memory_mib", "disk_mib", "process_slots"}),
        )
        for field in capacity:
            _integer(f"{name}.{capacity_name}.{field}", capacity[field])
        snapshot[capacity_name] = capacity
    for field in (
        "heartbeat_at_ms",
        "heartbeat_ttl_ms",
        "disk_used_mib",
        "disk_capacity_mib",
    ):
        _integer(f"{name}.{field}", snapshot[field])
    _integer(
        f"{name}.heartbeat_ttl_ms",
        snapshot["heartbeat_ttl_ms"],
        minimum=1000,
        maximum=86_400_000,
    )
    if snapshot["disk_used_mib"] > snapshot["disk_capacity_mib"]:
        raise ValueError(f"{name}.disk_used_mib exceeds disk_capacity_mib")
    for field in (
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
    ):
        _integer(f"{name}.{field}", snapshot[field])
    for field in ("draining", "quarantined"):
        _boolean(f"{name}.{field}", snapshot[field])
    labels = snapshot["labels"]
    if (
        not isinstance(labels, list)
        or len(labels) > 128
        or any(
            not isinstance(item, str)
            or not item
            or item != item.strip()
            or len(item) > 256
            for item in labels
        )
        or len(labels) != len(set(labels))
    ):
        raise ValueError(f"{name}.labels must be unique bounded strings")
    if snapshot["target_kind"] not in {"local", "inventory_alias"}:
        raise ValueError(f"{name}.target_kind is invalid")
    if snapshot["target_alias"] is not None:
        _string(f"{name}.target_alias", snapshot["target_alias"])
        if len(snapshot["target_alias"]) > 256:
            raise ValueError(f"{name}.target_alias exceeds the 256-character bound")
    if (snapshot["target_kind"] == "local") != (snapshot["target_alias"] is None):
        raise ValueError(f"{name}.target_alias does not match target_kind")
    policies = snapshot["disk_policies"]
    if not isinstance(policies, list) or len(policies) > 128:
        raise ValueError(f"{name}.disk_policies must be a bounded list")
    seen: set[str] = set()
    for index, policy_value in enumerate(policies):
        policy = _exact_mapping(
            f"{name}.disk_policies[{index}]",
            policy_value,
            frozenset(
                {
                    "policy_key",
                    "blocked",
                    "low_watermark_mib",
                    "high_watermark_mib",
                    "revision",
                }
            ),
        )
        _string(f"{name}.disk_policies[{index}].policy_key", policy["policy_key"])
        if len(policy["policy_key"]) > 256 or policy["policy_key"] in seen:
            raise ValueError(
                f"{name}.disk_policies contains duplicate/overlong policy keys"
            )
        seen.add(policy["policy_key"])
        _boolean(f"{name}.disk_policies[{index}].blocked", policy["blocked"])
        _integer(f"{name}.disk_policies[{index}].revision", policy["revision"])
        for field in ("low_watermark_mib", "high_watermark_mib"):
            if policy[field] is not None:
                _integer(f"{name}.disk_policies[{index}].{field}", policy[field])
        if (
            policy["low_watermark_mib"] is not None
            and policy["high_watermark_mib"] is not None
            and policy["low_watermark_mib"] > policy["high_watermark_mib"]
        ):
            raise ValueError(f"{name}.disk_policies[{index}] has inverted watermarks")
    return snapshot


def _resource_host_update_request(value: Any) -> dict[str, Any]:
    request = _exact_mapping(
        "ResourceHostUpdate request",
        value,
        frozenset(
            {
                "schema_version",
                "tenant_ref",
                "host_ref",
                "revision",
                "capacity",
                "observed",
                "heartbeat_at_ms",
                "heartbeat_ttl_ms",
                "now_ms",
                "draining",
                "quarantined",
                "labels",
                "target_kind",
                "target_alias",
                "disk_used_mib",
                "disk_capacity_mib",
            }
        ),
    )
    if request["schema_version"] != "1":
        raise ValueError("ResourceHostUpdate schema_version must be 1")
    for field in ("tenant_ref", "host_ref"):
        _string(f"ResourceHostUpdate.{field}", request[field])
    _integer("ResourceHostUpdate.revision", request["revision"], minimum=1)
    for capacity_name in ("capacity", "observed"):
        capacity = _exact_mapping(
            f"ResourceHostUpdate.{capacity_name}",
            request[capacity_name],
            frozenset({"cpu_weight", "memory_mib", "disk_mib", "process_slots"}),
        )
        for field in capacity:
            _integer(f"ResourceHostUpdate.{capacity_name}.{field}", capacity[field])
        request[capacity_name] = capacity
    for field in (
        "heartbeat_at_ms",
        "heartbeat_ttl_ms",
        "now_ms",
        "disk_used_mib",
        "disk_capacity_mib",
    ):
        _integer(f"ResourceHostUpdate.{field}", request[field])
    _integer(
        "ResourceHostUpdate.heartbeat_ttl_ms",
        request["heartbeat_ttl_ms"],
        minimum=1000,
        maximum=86_400_000,
    )
    if request["heartbeat_at_ms"] > request["now_ms"]:
        raise ValueError("ResourceHostUpdate heartbeat_at_ms must not exceed now_ms")
    if request["now_ms"] - request["heartbeat_at_ms"] > request["heartbeat_ttl_ms"]:
        raise ValueError("ResourceHostUpdate heartbeat is stale")
    if request["disk_used_mib"] > request["disk_capacity_mib"]:
        raise ValueError("ResourceHostUpdate disk_used_mib exceeds disk_capacity_mib")
    for field in ("draining", "quarantined"):
        _boolean(f"ResourceHostUpdate.{field}", request[field])
    labels = request["labels"]
    if (
        not isinstance(labels, list)
        or any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in labels
        )
        or len(labels) != len(set(labels))
    ):
        raise ValueError("ResourceHostUpdate.labels must be unique strings")
    if request["target_kind"] not in {"local", "inventory_alias"}:
        raise ValueError("ResourceHostUpdate.target_kind is invalid")
    if request["target_alias"] is not None:
        _string("ResourceHostUpdate.target_alias", request["target_alias"])
    if (request["target_kind"] == "local") != (request["target_alias"] is None):
        raise ValueError("ResourceHostUpdate target_alias does not match target_kind")
    return request


def _resource_host_update_result(value: Any) -> dict[str, Any]:
    result = _exact_mapping(
        "ResourceHostUpdate result",
        value,
        frozenset(
            {
                "schema_version",
                "accepted",
                "reason",
                "host_ref",
                "host_snapshot",
                "revision",
                "held_cpu_weight",
                "held_memory_mib",
                "held_disk_mib",
                "held_process_slots",
                "draining",
                "quarantined",
            }
        ),
    )
    if result["schema_version"] != "1":
        raise ValueError("ResourceHostUpdate result schema_version must be 1")
    _boolean("ResourceHostUpdate result.accepted", result["accepted"])
    if result["reason"] not in {"accepted", "stale_host", "conflict", "not_found"}:
        raise ValueError("ResourceHostUpdate result reason is invalid")
    _string("ResourceHostUpdate result.host_ref", result["host_ref"])
    _resource_host_snapshot(
        result["host_snapshot"], "ResourceHostUpdate result.host_snapshot"
    )
    for field in (
        "revision",
        "held_cpu_weight",
        "held_memory_mib",
        "held_disk_mib",
        "held_process_slots",
    ):
        _integer(f"ResourceHostUpdate result.{field}", result[field])
    for field in ("draining", "quarantined"):
        _boolean(f"ResourceHostUpdate result.{field}", result[field])
    if result["accepted"] != (result["reason"] == "accepted"):
        raise ValueError("ResourceHostUpdate result acceptance is inconsistent")
    return result


def _evidence_bundle(value: Any) -> dict[str, Any]:
    """Validate the current schema-generated evidence response projection."""
    bundle = _exact_mapping(
        "EvidenceBundle",
        value,
        frozenset(
            {
                "schema_version",
                "bundle_id",
                "resolved",
                "answer_ref",
                "claims",
                "policy_exclusions",
                "next_action_refs",
            }
        ),
    )
    if bundle["schema_version"] != "1":
        raise ValueError("EvidenceBundle schema_version must be 1")
    _string("EvidenceBundle.bundle_id", bundle["bundle_id"])
    _boolean("EvidenceBundle.resolved", bundle["resolved"])
    claims = bundle["claims"]
    if not isinstance(claims, list):
        raise TypeError("EvidenceBundle.claims must be a list")
    for index, claim in enumerate(claims):
        item = _exact_mapping(
            f"EvidenceBundle.claims[{index}]",
            claim,
            frozenset(
                {
                    "claim_ref",
                    "kind",
                    "score",
                    "confidence",
                    "valid_time",
                    "transaction_time",
                    "source_refs",
                    "evidence_locus_refs",
                    "contradiction_refs",
                    "proof_refs",
                    "policy_labels",
                }
            ),
        )
        _string(f"EvidenceBundle.claims[{index}].claim_ref", item["claim_ref"])
        _string(
            f"EvidenceBundle.claims[{index}].kind",
            item["kind"],
            allow_empty=True,
        )
        for field in ("valid_time", "transaction_time"):
            _exact_mapping(
                f"EvidenceBundle.claims[{index}].{field}",
                item[field],
                frozenset({"start_ms", "end_ms"}),
            )
    return bundle


def _bytes(name: str, value: Any, *, allow_empty: bool = True) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError(f"{name} must be bytes")
    if not allow_empty and not value:
        raise ValueError(f"{name} must not be empty")
    return value


def _opaque_ref(name: str, value: Any, *, namespace: str | None = None) -> str:
    reference = _string(name, value)
    parts = reference.split(":")
    valid = (
        3 <= len(parts) <= 6
        and parts[0] == "eg"
        and all(
            part
            and len(part) <= 32
            and all(
                char.isascii() and (char.islower() or char.isdigit() or char in "_-")
                for char in part
            )
            for part in parts[1:-1]
        )
        and 16 <= len(parts[-1]) <= 128
        and all(char in "0123456789abcdef" for char in parts[-1])
    )
    if not valid or (namespace is not None and parts[1] != namespace):
        expected = f" in the {namespace!r} namespace" if namespace else ""
        raise ValueError(f"{name} must be a valid opaque reference{expected}")
    return reference


_KNOWLEDGE_CURSOR_FIELDS = frozenset(
    {
        "schema_version",
        "family",
        "integrity_ref",
        "tenant_ref",
        "access_policy_ref",
        "placement_ref",
        "snapshot_ref",
        "query_ref",
        "derivation_ref",
        "evidence_set_ref",
        "batch_size",
        "row_offset",
        "batch_index",
        "exhausted",
    }
)


def _knowledge_cursor(value: Any) -> KnowledgeStreamCursor:
    cursor = _exact_mapping("KnowledgeStream cursor", value, _KNOWLEDGE_CURSOR_FIELDS)
    if _integer("cursor.schema_version", cursor["schema_version"], maximum=1) != 1:
        raise ValueError("cursor.schema_version must be 1")
    family = _string("cursor.family", cursor["family"])
    if family not in _KNOWLEDGE_FAMILIES:
        raise ValueError("cursor.family is not a current KnowledgeStream family")
    for field in (
        "integrity_ref",
        "tenant_ref",
        "access_policy_ref",
        "placement_ref",
        "snapshot_ref",
        "query_ref",
        "derivation_ref",
        "evidence_set_ref",
    ):
        cursor[field] = _opaque_ref(f"cursor.{field}", cursor[field])
    cursor["batch_size"] = _integer(
        "cursor.batch_size", cursor["batch_size"], minimum=1, maximum=65_536
    )
    cursor["row_offset"] = _integer("cursor.row_offset", cursor["row_offset"])
    cursor["batch_index"] = _integer("cursor.batch_index", cursor["batch_index"])
    cursor["exhausted"] = _boolean("cursor.exhausted", cursor["exhausted"])
    return cast(KnowledgeStreamCursor, cursor)


def _knowledge_query(value: Any) -> tuple[KnowledgeStreamQuery, str]:
    if not isinstance(value, dict):
        raise TypeError("KnowledgeStream query must be a mapping")
    family = _string("query.family", value.get("family"))
    if family not in _KNOWLEDGE_FAMILIES:
        raise ValueError("query.family is not a current KnowledgeStream family")

    if family == "graph":
        query = _exact_mapping(
            "graph KnowledgeStream query",
            value,
            frozenset({"family", "label", "limit"}),
        )
        query["label"] = _string("query.label", query["label"], allow_empty=True)
        query["limit"] = _integer("query.limit", query["limit"])
    elif family == "sql":
        query = _exact_mapping(
            "SQL KnowledgeStream query",
            value,
            frozenset({"family", "query", "params_msgpack"}),
        )
        query["query"] = _string("query.query", query["query"])
        query["params_msgpack"] = _bytes(
            "query.params_msgpack", query["params_msgpack"]
        )
    elif family == "rdf":
        query = _exact_mapping(
            "RDF KnowledgeStream query",
            value,
            frozenset({"family", "query", "base_iri", "type_convention"}),
        )
        query["query"] = _string("query.query", query["query"])
        query["base_iri"] = _string(
            "query.base_iri", query["base_iri"], allow_empty=True
        )
        query["type_convention"] = _string(
            "query.type_convention", query["type_convention"], allow_empty=True
        )
    elif family == "vector":
        query = _exact_mapping(
            "vector KnowledgeStream query",
            value,
            frozenset({"family", "keywords", "query_embedding", "k"}),
        )
        keywords = query["keywords"]
        if not isinstance(keywords, list):
            raise TypeError("query.keywords must be a list of strings")
        query["keywords"] = [
            _string("query.keywords entry", keyword) for keyword in keywords
        ]
        embedding = query["query_embedding"]
        if not isinstance(embedding, list):
            raise TypeError("query.query_embedding must be a list of finite numbers")
        normalized: list[float] = []
        for component in embedding:
            if isinstance(component, bool) or not isinstance(component, (int, float)):
                raise TypeError(
                    "query.query_embedding must be a list of finite numbers"
                )
            component = float(component)
            if not math.isfinite(component) or abs(component) > 3.4028235e38:
                raise ValueError("query.query_embedding contains an invalid f32 value")
            normalized.append(component)
        query["query_embedding"] = normalized
        query["k"] = _integer("query.k", query["k"], minimum=1)
    elif family == "time_series":
        query = _exact_mapping(
            "time-series KnowledgeStream query",
            value,
            frozenset({"family", "series_id", "from", "to"}),
        )
        query["series_id"] = _string("query.series_id", query["series_id"])
        query["from"] = _integer(
            "query.from", query["from"], minimum=-(1 << 63), maximum=(1 << 63) - 1
        )
        query["to"] = _integer(
            "query.to", query["to"], minimum=-(1 << 63), maximum=(1 << 63) - 1
        )
        if query["from"] > query["to"]:
            raise ValueError("query.from must not be greater than query.to")
    elif family == "job":
        query = _exact_mapping(
            "job KnowledgeStream query", value, frozenset({"family", "job_id"})
        )
        query["job_id"] = _string("query.job_id", query["job_id"])
    else:
        query = _exact_mapping(
            "cross-modal KnowledgeStream query", value, frozenset({"family", "text"})
        )
        query["text"] = _string("query.text", query["text"])
    return cast(KnowledgeStreamQuery, query), family


def _knowledge_batch(
    value: Any, *, family: str, batch_size: int
) -> KnowledgeStreamBatch:
    batch = _exact_mapping(
        "KnowledgeStream batch",
        value,
        frozenset({"schema_version", "family", "projection", "cursor", "payload"}),
    )
    if _integer("batch.schema_version", batch["schema_version"], maximum=1) != 1:
        raise ValueError("batch.schema_version must be 1")
    if batch["family"] != family:
        raise ValueError("KnowledgeStream response family does not match its query")
    if batch["projection"] != "arrow_ipc_v1":
        raise ValueError("KnowledgeStream response must use arrow_ipc_v1")
    cursor = _knowledge_cursor(batch["cursor"])
    if cursor["family"] != family or cursor["batch_size"] != batch_size:
        raise ValueError("KnowledgeStream response cursor does not match its request")
    batch["cursor"] = cursor
    batch["payload"] = _bytes("batch.payload", batch["payload"], allow_empty=False)
    return cast(KnowledgeStreamBatch, batch)


def _served_modality(name: str, value: Any) -> str:
    modality = _string(name, value)
    if modality not in _SERVED_MODALITIES:
        raise ValueError(f"{name} must be document, image, audio, or video")
    return modality


def _finite_f32(name: str, value: Any, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite number")
    normalized = float(value)
    if not math.isfinite(normalized) or not minimum <= normalized <= maximum:
        raise ValueError(f"{name} is outside the current bounded range")
    return normalized


def _native_temporal_window(start_ms: Any, end_ms: Any) -> tuple[int, int]:
    start = _integer("start_ms", start_ms)
    end = _integer("end_ms", end_ms)
    if end <= start:
        raise ValueError("end_ms must be greater than start_ms")
    first_bucket = start // 1_000
    last_bucket = (end - 1) // 1_000
    if last_bucket - first_bucket + 1 > _MAX_NATIVE_TEMPORAL_BUCKETS:
        raise ValueError("native temporal query exceeds 4096 index buckets")
    return start, end


def _modality_authority(value: Any) -> ModalityAuthority:
    authority = _exact_mapping(
        "served modality authority",
        value,
        frozenset(
            {
                "tenant_ref",
                "access_policy_ref",
                "purpose_ref",
                "maximum_classification",
            }
        ),
    )
    for field in ("tenant_ref", "access_policy_ref", "purpose_ref"):
        authority[field] = _opaque_ref(f"authority.{field}", authority[field])
    if authority["maximum_classification"] not in {
        "public",
        "internal",
        "confidential",
        "restricted",
    }:
        raise ValueError("authority.maximum_classification is invalid")
    return cast(ModalityAuthority, authority)


def _modality_outcome(value: Any) -> ModalityApplyOutcome:
    outcome = _exact_mapping(
        "served modality outcome",
        value,
        frozenset({"disposition", "observation_version", "event_sequence"}),
    )
    if outcome["disposition"] not in {"Applied", "IdempotentReplay"}:
        raise ValueError("served modality outcome disposition is invalid")
    outcome["observation_version"] = _integer(
        "outcome.observation_version", outcome["observation_version"], minimum=1
    )
    outcome["event_sequence"] = _integer(
        "outcome.event_sequence", outcome["event_sequence"], minimum=1
    )
    return cast(ModalityApplyOutcome, outcome)


def _modality_outcomes(value: Any, expected: int) -> list[ModalityApplyOutcome]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError("served modality stream outcome cardinality mismatch")
    return [_modality_outcome(item) for item in value]


def _artifact_bundle(value: Any) -> dict[str, Any]:
    bundle = _exact_mapping("served modality bundle", value, _ARTIFACT_BUNDLE_FIELDS)
    if _integer("bundle.protocol_version", bundle["protocol_version"], maximum=1) != 1:
        raise ValueError("bundle.protocol_version must be 1")
    if not isinstance(bundle["privacy"], dict):
        raise TypeError("bundle.privacy must be a mapping")
    for field in (
        "artifacts",
        "occurrences",
        "renditions",
        "segments",
        "features",
        "evidence_loci",
    ):
        if not isinstance(bundle[field], list):
            raise TypeError(f"bundle.{field} must be a list")
    return bundle


def _modality_page(value: Any) -> ServedModalityPage:
    page = _exact_mapping("served modality page", value, frozenset({"records", "next"}))
    if not isinstance(page["records"], list):
        raise TypeError("served modality page.records must be a list")
    records: list[ServedModalityRecord] = []
    for raw_record in page["records"]:
        record = _exact_mapping(
            "served modality record",
            raw_record,
            frozenset(
                {
                    "occurrence_id",
                    "observation_version",
                    "lifecycle",
                    "bundle",
                    "value",
                }
            ),
        )
        record["occurrence_id"] = _opaque_ref(
            "record.occurrence_id", record["occurrence_id"], namespace="occurrence"
        )
        record["observation_version"] = _integer(
            "record.observation_version", record["observation_version"], minimum=1
        )
        if record["lifecycle"] not in {"active", "cold"}:
            raise ValueError("query returned a non-visible modality lifecycle")
        record["bundle"] = _artifact_bundle(record["bundle"])
        if not isinstance(record["value"], dict):
            raise TypeError("visible modality record.value must be a mapping")
        records.append(cast(ServedModalityRecord, record))
    next_occurrence = page["next"]
    if next_occurrence is not None:
        next_occurrence = _opaque_ref(
            "page.next", next_occurrence, namespace="occurrence"
        )
    expected_next = records[-1]["occurrence_id"] if records else None
    if next_occurrence != expected_next:
        raise ValueError("served modality page cursor does not match its final record")
    return {"records": records, "next": next_occurrence}


def _modality_events(value: Any) -> list[ServedModalityEvent]:
    if not isinstance(value, list):
        raise TypeError("served modality events result must be a list")
    events: list[ServedModalityEvent] = []
    previous_sequence = 0
    for raw_event in value:
        event = _exact_mapping(
            "served modality event",
            raw_event,
            frozenset(
                {
                    "sequence",
                    "occurrence_id",
                    "observation_version",
                    "kind",
                    "tenant_ref",
                    "access_policy_ref",
                }
            ),
        )
        event["sequence"] = _integer("event.sequence", event["sequence"], minimum=1)
        if event["sequence"] <= previous_sequence:
            raise ValueError("served modality events must be strictly ordered")
        previous_sequence = event["sequence"]
        event["occurrence_id"] = _opaque_ref(
            "event.occurrence_id", event["occurrence_id"], namespace="occurrence"
        )
        event["observation_version"] = _integer(
            "event.observation_version", event["observation_version"], minimum=1
        )
        if event["kind"] not in {
            "ingested",
            "updated",
            "deleted",
            "moved_to_cold",
            "restored",
            "reindexed",
        }:
            raise ValueError("served modality event kind is invalid")
        event["tenant_ref"] = _opaque_ref("event.tenant_ref", event["tenant_ref"])
        event["access_policy_ref"] = _opaque_ref(
            "event.access_policy_ref", event["access_policy_ref"]
        )
        events.append(cast(ServedModalityEvent, event))
    return events


def _modality_capabilities(value: Any) -> ServedModalityCapabilities:
    capabilities = _exact_mapping(
        "served modality capabilities",
        value,
        frozenset(
            {
                "component_ready",
                "component_pass",
                "component_not_applicable",
                "component_total",
            }
        ),
    )
    certified = (
        capabilities["component_ready"] is True
        and _integer("capabilities.component_pass", capabilities["component_pass"])
        == 12
        and _integer(
            "capabilities.component_not_applicable",
            capabilities["component_not_applicable"],
        )
        == 0
        and _integer("capabilities.component_total", capabilities["component_total"])
        == 12
    )
    if not certified:
        raise ValueError(
            "served modality component is not certified at 12 PASS / 0 N/A"
        )
    return cast(ServedModalityCapabilities, capabilities)


def _modality_stats(value: Any) -> ServedModalityStats:
    fields = frozenset(
        {
            "active_records",
            "total_records",
            "tombstoned_records",
            "modality_index_postings",
            "segment_index_postings",
            "native_index_keys",
            "native_index_postings",
            "events",
            "snapshot_bytes",
        }
    )
    stats = _exact_mapping("served modality stats", value, fields)
    for field in fields:
        stats[field] = _integer(
            f"stats.{field}",
            stats[field],
            minimum=1 if field == "snapshot_bytes" else 0,
        )
    if stats["active_records"] + stats["tombstoned_records"] != stats["total_records"]:
        raise ValueError("served modality stats record totals are inconsistent")
    return cast(ServedModalityStats, stats)


class ResultTooLargeError(RuntimeError):
    """Raised when an unbounded read (e.g. ``nodes.list()`` / ``GetNodes``) would
    return more than the engine's configured node cap
    (``EPISTEMIC_GRAPH_MAX_RESPONSE_NODES``, CONCEPT:EG-KG.ingest.resets-socket-so-assimilation).

    The engine refuses to serialize a pathological full-graph dump (which would
    overrun/reset the connection) and instead returns a typed ``RESULT_TOO_LARGE``
    error. Catch this to fall back to a bounded query — ``nodes.list_by_label(
    label, limit)`` or pagination. Subclasses :class:`RuntimeError`, so existing
    ``except RuntimeError`` handlers keep working.
    """


class StaleRouteError(RuntimeError):
    """The contacted engine cannot serve the graph's current placement route.

    ``route`` is the engine-authored structured redirect containing the target,
    Raft group, catalog epoch, fencing token, and (when known) leader node.  It
    is deliberately kept as data so a topology-aware caller can refresh its
    placement catalog and retry without parsing an error string.
    """

    def __init__(self, message: str, route: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.route = dict(route or {})
        self.target_ref = str(self.route.get("target_ref") or "")
        self.group = self.route.get("group")
        self.epoch = int(self.route.get("epoch") or 0)
        self.fencing_token = self.route.get("fencing_token")
        self.leader_ref = self.route.get("leader_ref")


class LedgerNotPopulatedError(RuntimeError):
    """``GetLedger`` reported it could not read the ledger for this request's
    scope (BUG A1, 2026-08-12) — distinct from a genuinely empty ledger, which
    ``LedgerClient.get()`` returns as ``[]`` without raising.

    Before the fix, the engine's ``GetLedger`` RPC returned a bare ``[]`` for
    BOTH "nothing has mutated" and "the ledger could not be read", so a caller
    like ``agent_utilities.workflows.epistemic_sync.flush_ledger_to_backend``
    (a real production sync path) could not tell the two apart and silently
    treated the second as a no-op, flushing nothing. The engine now returns a
    typed ``{"populated": bool, "entries": [...]}`` result; this client raises
    on ``populated: false`` so a caller that has just committed mutations and
    expects to see them can fail loudly instead of reading a false "nothing
    to sync".
    """


class CdcGapError(RuntimeError):
    """``CdcRead``/``Watch``/``FiredTriggers`` reported that a cursor could not
    be served contiguously (B-8, 2026-08-13) — the same defect class as
    ``GetLedger``'s :class:`LedgerNotPopulatedError` above, applied to the
    engine's CDC feed (``CdcHub``, ``src/server/cdc.rs``).

    ``CdcHub`` is a bounded, PURELY IN-MEMORY ring per graph — DELIBERATELY
    EPHEMERAL, not durable (see that module's own doc for the reasoning;
    making it durable is a separate storage-format/migration decision, out
    of scope for this fix). Before this fix, a cursor that fell off the back
    of that ring — because the ring trimmed past it, or because the engine
    process restarted and the feed's seq numbering restarted from 0 — was
    served as a silently empty result, indistinguishable from "caught up".
    This exception makes that condition explicit instead: distinct from a
    genuinely caught-up read (an empty event list with NO error), it means
    the caller's cursor names history this epoch of the feed can no longer
    (or never did) vouch for, and the caller must re-seed rather than assume
    nothing happened.
    """


class NodeClient:
    """CONCEPT:AU-KG.query.object-graph-mapper — Topology Node Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add(self, node_id: str, properties: dict[str, Any] | None = None) -> None:
        await self._client._send(
            "AddNode",
            {
                "node_id": node_id,
                "properties_msgpack": _pack_binary_msgpack(properties or {}),
            },
        )

    async def create_if_absent(
        self, node_id: str, properties: dict[str, Any] | None = None
    ) -> bool:
        """Atomically create ``node_id`` without overwriting an existing node.

        Returns ``True`` only to the writer that inserted the node. Concurrent
        callers execute one durable server-side membership-test-and-insert operation;
        losers return ``False`` and leave the winning properties untouched.
        """
        return await self._client._send(
            "CreateNodeIfAbsent",
            {
                "node_id": node_id,
                "properties_msgpack": _pack_binary_msgpack(properties or {}),
            },
        )

    async def remove(self, node_id: str) -> None:
        await self._client._send("RemoveNode", {"node_id": node_id})

    async def has(self, node_id: str) -> bool:
        return await self._client._send("HasNode", {"node_id": node_id})

    async def compare_and_set(
        self, node_id: str, conditions: dict[str, Any], updates: dict[str, Any]
    ) -> bool:
        """Atomic compare-and-set on a node's property blob (CONCEPT:EG-KG.compute.backend backend-
        agnostic atomic claim). If every ``(field, expected)`` in ``conditions``
        matches the node's current value (a MISSING field reads as ``None``), the
        ``updates`` are merged in and ``True`` is returned; otherwise (node absent,
        any condition fails, or decode fails) the node is untouched and ``False``
        is returned. The read-modify-write runs atomically in the engine, so this
        is a backend-agnostic atomic claim for ``:Task``/``:Loop`` nodes."""
        return await self._client._send(
            "CompareAndSetNodeFields",
            {
                "node_id": node_id,
                "conditions_msgpack": _pack_binary_msgpack(conditions),
                "updates_msgpack": _pack_binary_msgpack(updates),
            },
        )

    async def claim_next(
        self, label: str, updates: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        """Atomically claim the oldest pending node of ``label`` (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending).

        Among ``label``'s nodes whose ``status == "pending"``, the engine picks the
        smallest ``seq`` and merges ``updates`` (the claim marker) in ONE round-trip
        under the write guard — the single-round-trip form of scan+``compare_and_set``.
        Returns ``(node_id, updated_properties)`` or ``None`` if nothing is claimable.
        ``updates`` MUST carry no wall-clock read (pass the lease/marker in) so WAL
        and Raft replay stay deterministic."""
        raw_val = await self._client._send(
            "ClaimNext",
            {"label": label, "updates_msgpack": _pack_binary_msgpack(updates)},
        )
        if isinstance(raw_val, bytes):
            raw_val = msgpack.unpackb(raw_val, raw=False)
        if not raw_val:
            return None
        return raw_val[0], raw_val[1]

    async def list(self) -> builtins.list[tuple[str, str]]:
        """Dump EVERY node in the graph (unbounded full-graph read).

        On a large graph this is refused by the engine's overload backstop
        (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation): if the graph has more than
        ``EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`` nodes (default 50_000), this raises
        :class:`ResultTooLargeError` instead of materializing a gigabyte-scale
        frame that would reset the connection. Use :meth:`list_by_label` (which is
        bounded by ``limit``) or paginate for large graphs.
        """
        return await self._client._send("GetNodes")

    async def list_by_label(
        self, label: str, limit: int = 0, *, after: str | None = None
    ) -> builtins.list[tuple[str, Any]]:
        """Return one deterministic keyset page of matching nodes.

        ``after`` is an exclusive node-id cursor. Advance it to the last id in
        each non-empty page. ``limit=0`` is uncapped and intended only for small
        graphs.
        """
        return await self._client._send(
            "GetNodesByLabel",
            {"label": label, "after": after, "limit": int(limit)},
        )

    async def properties(self, node_id: str) -> dict[str, Any] | None:
        raw_val = await self._client._send("GetNodeProperties", {"node_id": node_id})
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

    async def properties_batch(
        self, node_ids: builtins.list[str]
    ) -> dict[str, dict[str, Any] | None]:
        """Fetch properties for many nodes in ONE round-trip (CONCEPT:EG-KG.memory.forgetting-curve-decay).

        Returns a mapping ``node_id -> properties`` (``None`` for ids absent from
        the graph). Collapses what would be N ``properties()`` calls — and N
        network round-trips — into a single request.
        """
        rows = await self._client._send(
            "GetNodePropertiesBatch", {"node_ids": list(node_ids)}
        )
        out: dict[str, dict[str, Any] | None] = {}
        for entry in rows or []:
            nid, blob = entry[0], entry[1]
            out[nid] = msgpack.unpackb(blob, raw=False) if blob is not None else None
        return out

    async def has_batch(self, node_ids: builtins.list[str]) -> dict[str, bool]:
        """Existence check for many nodes in one round-trip."""
        ids = list(node_ids)
        flags = await self._client._send("HasNodesBatch", {"node_ids": ids})
        return dict(zip(ids, flags or [], strict=False))

    async def count(self) -> int:
        return await self._client._send("NodeCount")

    async def ids(self) -> builtins.list[str]:
        return await self._client._send("NodeIds")

    async def in_degree(self, node_id: str) -> int:
        return await self._client._send("InDegree", {"node_id": node_id})

    async def out_degree(self, node_id: str) -> int:
        return await self._client._send("OutDegree", {"node_id": node_id})

    async def predecessors(self, node_id: str) -> builtins.list[str]:
        return await self._client._send("GetPredecessors", {"node_id": node_id})

    async def successors(self, node_id: str) -> builtins.list[str]:
        return await self._client._send("GetSuccessors", {"node_id": node_id})

    async def neighbors(self, node_id: str) -> builtins.list[str]:
        return await self._client._send("GetNeighbors", {"node_id": node_id})

    async def neighbors_batch(
        self, node_ids: builtins.list[str]
    ) -> dict[str, builtins.list[str]]:
        """Neighbor ids for many nodes in ONE round-trip (D-DPF-1).

        Returns a mapping ``node_id -> neighbor_ids`` in input order (an id
        absent from the graph maps to ``[]``, matching the engine's
        fail-open-per-id batch shape — see :meth:`properties_batch` for the
        equivalent absent-id contract on node properties). Collapses what
        would otherwise be N ``neighbors()`` calls — and N network round-trips
        — into a single request, the same pattern as :meth:`properties_batch`
        / :meth:`has_batch`.
        """
        ids = list(node_ids)
        rows = await self._client._send("GetNeighborsBatch", {"node_ids": ids})
        return {nid: list(neighbor_ids) for nid, neighbor_ids in (rows or [])}

    # ── Cross-graph union reads (CONCEPT:EG-KG.query.cross-graph-union) ───────────────────────
    # Read across a SET of content graphs as if one, so writes can be partitioned
    # across per-graph write locks (each lane its own graph) while reads see the
    # union. Missing lane graphs in the set are skipped engine-side.

    async def properties_union(
        self, node_id: str, graphs: builtins.list[str]
    ) -> dict[str, Any] | None:
        """First-found node properties across ``graphs`` (in order)."""
        raw_val = await self._client._send(
            "UnionGetNodeProperties", {"graphs": list(graphs), "node_id": node_id}
        )
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

    async def list_by_label_union(
        self, label: str, graphs: builtins.list[str], limit: int = 0
    ) -> builtins.list[tuple[str, Any]]:
        """Label scan unioned + deduped by id across ``graphs`` (``limit=0`` ⇒ no cap)."""
        return await self._client._send(
            "UnionGetNodesByLabel",
            {"graphs": list(graphs), "label": label, "limit": int(limit)},
        )

    async def neighbors_union(
        self, node_id: str, graphs: builtins.list[str]
    ) -> builtins.list[str]:
        """Neighbour ids unioned + deduped across every graph that holds the anchor."""
        return await self._client._send(
            "UnionGetNeighbors", {"graphs": list(graphs), "node_id": node_id}
        )


class StatechartClient:
    """CONCEPT:INT-P2-2 — the native finite-state-machine / statechart engine namespace:
    define/instantiate/send_event/get_state/list over ``Method::Statechart { op }``
    (``eg_statechart::StatechartDef`` + a durable, rehydratable ``MachineInstance``).

    Definitions are content-addressed data (a dict shaped like
    ``{"name", "schema_version", "states", "alphabet", "transitions", "initial",
    "finals", "meta"}`` — see the Rust ``eg_statechart::model::StatechartDef`` this
    mirrors 1:1) — ``define()`` MessagePack-encodes it (named/map form, matching
    ``rmp_serde::to_vec_named``) and returns the server-computed, deterministic
    ``def_id``; re-defining a byte-identical chart is idempotent (same id, no new row).
    Instances are NOT graph-scoped (their own ``statecharts.redb``, like
    :class:`JobsClient`'s jobs), owner-scoped to the caller's ``(tenant, actor)``::

        def_id = await client.statechart.define(LOOP_STATECHART_DEF)
        instance = await client.statechart.instantiate(def_id, context={})
        out = await client.statechart.send_event(
            instance["instance_id"], "claim"
        )
        state = await client.statechart.get_state(instance["instance_id"])

    ``send_event``'s response includes ``fired`` (``False`` on a well-defined no-op —
    an undefined edge or every guard false — never an error) and the resulting
    ``instance["configuration"]["active"]`` (the new active state(s)).
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def define(self, definition: dict[str, Any]) -> str:
        """Register a ``StatechartDef`` (a plain dict — see the class docstring for
        its shape). Returns the content-addressed ``def_id``."""
        if not isinstance(definition, dict):
            raise TypeError("definition must be a dict shaped like StatechartDef")
        blob = _pack_binary_msgpack(definition)
        resp = await self._client._send(
            "Statechart", {"op": {"Define": {"def_msgpack": blob}}}
        )
        return str(resp["def_id"])

    async def instantiate(
        self, def_id: str, context: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Create a running instance of ``def_id`` in its initial state, seeded with
        ``context`` (a JSON object; empty/``None`` for no initial extended state).
        Returns the freshly durable ``MachineInstance`` record (including its
        server-issued ``instance_id``)."""
        return await self._client._send(
            "Statechart",
            {"op": {"Instantiate": {"def_id": def_id, "context": context or {}}}},
        )

    async def send_event(
        self,
        instance_id: str,
        event: str,
        payload: dict[str, Any] | None = None,
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """Deliver ``event`` (with optional structured ``payload`` guards/actions may
        read) to ``instance_id``. ``expected_version``, when given, is an OCC token —
        the send is rejected if the stored instance has moved on. Returns
        ``{"instance", "fired", "no_op_reason", "fired_label", "actions", "effects"}``."""
        return await self._client._send(
            "Statechart",
            {
                "op": {
                    "SendEvent": {
                        "instance_id": instance_id,
                        "event": event,
                        "payload": payload if payload is not None else {},
                        "expected_version": expected_version,
                    }
                }
            },
        )

    async def get_state(self, instance_id: str) -> dict[str, Any]:
        """Fetch (rehydrate) ``instance_id``'s current durable ``(state, context)`` +
        version. Read-only, owner-scoped."""
        return await self._client._send(
            "Statechart", {"op": {"GetState": {"instance_id": instance_id}}}
        )

    async def list(self, def_id: str | None = None) -> dict[str, Any]:
        """List instance ids owned by the caller, optionally filtered to one
        definition. Returns ``{"instance_ids", "count"}``."""
        return await self._client._send(
            "Statechart", {"op": {"List": {"def_id": def_id}}}
        )


class VizClient:
    """D-VZ-1 lanes V4 ("engine integration") / V6 ("graph-native marks") — the
    native visualization render surface: ``Method::Viz { op }`` resolves a
    caller-provided ``eg_viz_core::ViewSpec`` against a dataset (caller-supplied
    inline columns, or deterministic engine-side synthetic data — including a
    ``MarkKind::Graph`` node/edge dataset for the V6-lite graph-native marks
    demo) and returns REAL PNG/SVG/PDF bytes rendered server-side through the
    LOD ColumnStore/export pipeline (``eg-viz-core``/``eg-viz-columnstore``/
    ``eg-viz-export``) — never a stub or placeholder image.

    V4-LITE, not full V4: each :meth:`render` call resolves against a FRESH,
    ephemeral, per-request ``ColumnStore`` built fresh for that one request — no
    tile cache keyed by ``(query_hash, viewport, theme, tier)``, no provenance
    inherited from a durable ``eg-jobs`` job, no view over a live query against
    a resident graph. See the Rust handler's module doc
    (``src/server/handlers/viz.rs``) for the exact scope boundary this class
    is a thin wire wrapper over. NOT graph-scoped: this surface never reads or
    writes graph state.

    Example::

        matrix = await client.viz.capability_matrix()
        result = await client.viz.render(
            spec={
                "version": 1,
                "marks": [{
                    "kind": "scatter",
                    "data_ref": "ds:1",
                    "encodings": {"x": {"field": "x"}, "y": {"field": "y"}},
                }],
            },
            dataset={"InlineColumns": {"columns": {
                "x": {"F64": [1.0, 2.0, 3.0]},
                "y": {"F64": [3.0, 1.0, 2.0]},
            }}},
        )
        png_bytes = result["bytes"]
        assert result["view_result"]["exact"] is True
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def capability_matrix(self) -> dict[str, Any]:
        """Fetch the mark x surface support matrix
        (``eg_viz_core::CapabilityMatrix::default_matrix``) — which (mark,
        surface) pairs are real today, and which lane will land the rest.
        Returns ``{"entries": [{"mark", "surface", "level", "status",
        "target_wave", "notes"}, ...]}``.

        ``_send`` already decodes the wire's compact MessagePack `Raw` payload
        (the same "second unpackb over a top-level bytes result" every other
        ``Raw``/``PropertiesMsgpack`` result goes through) — no special-cased
        decoding needed here.
        """
        return await self._client._send("Viz", {"op": "CapabilityMatrix"})

    async def render(
        self,
        spec: dict[str, Any],
        dataset: dict[str, Any],
        *,
        width_px: int = 800,
        height_px: int = 600,
        format: str = "png",
        max_primitives: int = 200_000,
        max_bytes: int = 50_000_000,
        dataset_ref: str = "viz-render",
    ) -> dict[str, Any]:
        """Resolve ``spec`` against ``dataset`` and render it server-side.

        ``spec`` is a plain dict matching ``eg_viz_core::ViewSpec``'s JSON
        shape 1:1 (field names are exactly what that type's derived
        ``Serialize`` produces — snake_case throughout, e.g. ``{"version": 1,
        "marks": [{"kind": "scatter", "data_ref": "ds:1", "encodings": {"x":
        {"field": "x"}, "y": {"field": "y"}}}], "scales": [...], "theme":
        {...}}``); the server parses and ``ViewSpec::validate()``s it, so a
        malformed spec is a typed error, never a silent misrender.

        ``dataset`` is a plain dict matching one of
        ``eg_types::viz::VizDatasetSource``'s three variants, externally
        tagged (the SAME convention :class:`StatechartClient` already shows —
        e.g. its ``{"op": {"Define": {...}}}``):

        - ``{"InlineColumns": {"columns": {name: {"F64": [...]} | {"Utf8":
          [...]}, ...}}}`` — caller-supplied columns.
        - ``{"SyntheticScatterClusters": {"row_count": int, "clusters": int,
          "seed": int}}`` — a deterministic, seeded, ENGINE-SIDE-generated
          clustered scatter (never shipped over the wire), proving
          high-density rendering without a huge request payload.
        - ``{"SyntheticGraph": {"node_count": int, "edge_count": int, "seed":
          int}}`` — a deterministic, seeded, ENGINE-SIDE-generated random
          graph for a ``MarkKind: "graph"`` mark (V6-lite).

        ``format`` is one of ``"png"``/``"svg"``/``"pdf"`` (matching
        ``eg_types::viz::VizFormat``'s `snake_case` wire casing — the SAME
        casing ``eg_viz_core::StaticExportFormat`` uses).

        Returns ``{"view_result": {...}, "format": ..., "content_type": ...,
        "bytes": b"..."}`` — ``bytes`` is real, non-empty, format-appropriate
        image bytes rendered server-side (a PNG starts with the standard PNG
        signature); ``view_result`` carries the LOD tier actually used and the
        ``exact`` trust bit (``True`` only for an unreduced ``LodTier.Direct``
        render — see ``eg_viz_core::ViewResult``'s own doc for why a caller
        must check this rather than infer it from the tier name).
        """
        return await self._client._send(
            "Viz",
            {
                "op": {
                    "Render": {
                        "spec_json": spec,
                        "dataset": dataset,
                        "width_px": width_px,
                        "height_px": height_px,
                        "format": format,
                        "max_primitives": max_primitives,
                        "max_bytes": max_bytes,
                        "dataset_ref": dataset_ref,
                    }
                }
            },
        )


class AsrClient:
    """GOC-33 (`OWNER-VOICE-ASR`) — the native ASR provider surface:
    ``Method::Asr { op }`` over the whisper-rs/whisper.cpp provider in
    ``eg-asr-whisper``. A direct, non-durable batch-file transcription call —
    this is the wire surface ``audio-transcriber``'s pluggable
    ``TranscriptionProvider`` seam reaches over this SAME transport (no second
    transport). Reads no persisted graph state and commits no durable
    ``asr.result.v1`` (that governed commit is future worker/AU-orchestration
    work — see ``crates/eg-audio/src/asr.rs``'s module doc).

    Model acquisition/verification governance is explicitly NOT this surface's
    job (GOC-36): ``model_path``/``model_sha256`` must already name a
    caller-resolved, digest-declared model file — this call never downloads
    anything and fails closed (a distinct ``model_unavailable`` error) if the
    file is absent or the digest does not match.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def transcribe_file(
        self,
        audio_wav: bytes,
        *,
        model_path: str,
        model_sha256: str,
        language: str | None = None,
        translate: bool = False,
        word_timing: bool = False,
        window_ms: int = 0,
    ) -> dict[str, Any]:
        """Transcribe a 16kHz mono 16-bit PCM WAV byte buffer.

        Returns ``{"text": str, "language": str, "segments": [{"start",
        "end", "text", "avg_logprob", "no_speech_prob"}], "timing_available":
        bool}`` — the same shape ``audio_transcriber.asr_providers``'s
        ``TranscriptionProvider.transcribe`` Protocol expects.
        """
        return await self._client._send(
            "Asr",
            {
                "op": {
                    "TranscribeFile": {
                        "model_path": model_path,
                        "model_sha256": model_sha256,
                        "audio_wav": audio_wav,
                        "language": language,
                        "translate": translate,
                        "word_timing": word_timing,
                        "window_ms": window_ms,
                    }
                }
            },
        )


class QuantumClient:
    """Q8 (CONCEPT:EG-KG.compute.quantum-agent-api) — the agent-facing quantum
    control plane: ``Method::Quantum { op }`` over a registered
    ``eg_quantum_core::backend::QuantumBackend`` (today ``eg-quantum-sim``'s
    ``sv-cpu``/``stabilizer``).

    The agent never sees qubits. Each method takes a plain problem statement
    (candidates and weights, a Max-Cut instance, or a ``QuantumProgram`` in the
    native IR) and returns a result plus the planner's full audit trail.

    EVERY result is a PROPOSAL. This surface reads no persisted graph state and
    writes nothing durable — nothing here is committed to the graph, and only an
    ``exact: true`` result may later become a hard constraint through the
    epistemic commit path. Because the handler self-routes before
    ``dispatch_graph_op``, results never enter the per-graph tamper-evident audit
    chain; the ``PlannerDecision.audit`` trail returned in every response is the
    Q9 observability record, which the agent-utilities caller persists into the
    same ``:ToolCall``/``RunTrace`` provenance model as everything else.

    ``backend_id`` is the R5 escape hatch on every op: ``None`` (the default)
    lets the planner choose via rules R0-R4 and is the only path needing no
    hardware quota check; naming a backend is always honoured, if registered,
    and always audited.

    Example::

        result = await client.quantum.rank(
            candidates=[{"id": "a", "weight": 0.9}, {"id": "b", "weight": 0.2}],
            shots=1024,
            seed=7,
        )
        ordering = result["ranked"]
        assert result["audit"]  # planner decision trail (Q9)
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def rank(
        self,
        candidates: list[dict[str, Any]],
        *,
        shots: int | None = None,
        seed: int | None = None,
        backend_id: str | None = None,
    ) -> dict[str, Any]:
        """Rank ``candidates`` through amplitude encoding + interference.

        ``candidates`` is a list of ``eg_types::quantum::QuantumRankCandidate``
        dicts (``{"id": str, "weight": float}``). One qubit is used per
        candidate, so the count is bounded server-side by the facade's
        ``MAX_RANK_CANDIDATES``; an oversized request is rejected outright,
        never silently truncated.
        """
        return await self._client._send(
            "Quantum",
            {
                "op": {
                    "Rank": {
                        "candidates": candidates,
                        "shots": shots,
                        "seed": seed,
                        "backend_id": backend_id,
                    }
                }
            },
        )

    async def optimize_qaoa(
        self,
        nodes: list[str],
        edges: list[dict[str, Any]],
        *,
        p_layers: int | None = None,
        shots: int | None = None,
        seed: int | None = None,
        backend_id: str | None = None,
    ) -> dict[str, Any]:
        """Run ONE fixed-parameter QAOA layer set over a Max-Cut instance.

        ``edges`` is a list of ``eg_types::quantum::QuantumQaoaEdge`` dicts.
        This is NOT a variational optimizer loop: there is no classical outer
        loop, just one evaluation at a canonical fixed angle schedule, and the
        sampled cut assignment comes back as a proposal. ``p_layers`` defaults
        server-side when omitted.
        """
        op: dict[str, Any] = {
            "nodes": nodes,
            "edges": edges,
            "shots": shots,
            "seed": seed,
            "backend_id": backend_id,
        }
        if p_layers is not None:
            op["p_layers"] = p_layers
        return await self._client._send("Quantum", {"op": {"OptimizeQaoa": op}})

    async def expectation(
        self,
        program: dict[str, Any],
        observable_qubits: list[int],
        *,
        shots: int | None = None,
        seed: int | None = None,
        backend_id: str | None = None,
    ) -> dict[str, Any]:
        """Sampled Pauli-Z-string expectation value over ``program``.

        ``program`` is a plain dict matching ``eg_quantum_core::ir::
        QuantumProgram``'s own JSON round-trip. Every qubit named in
        ``observable_qubits`` MUST already be measured into a classical bit by
        ``program``: the facade validates this and rejects otherwise rather than
        silently appending measurements to a caller's circuit. Q8 v0 is
        restricted to Pauli-Z strings.
        """
        return await self._client._send(
            "Quantum",
            {
                "op": {
                    "Expectation": {
                        "program": program,
                        "observable_qubits": observable_qubits,
                        "shots": shots,
                        "seed": seed,
                        "backend_id": backend_id,
                    }
                }
            },
        )


_CAPACITY_DECISIONS = frozenset(
    {
        "accepted",
        "replayed",
        "released",
        "renewed",
        "reclaimed",
        "exhausted",
        "stale_epoch",
        "stale_fence",
        "expired",
        "not_found",
        "idempotency_conflict",
        "invalid",
        "backpressure",
    }
)


def _submit_work_item_request_shape(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError("SubmitWorkItems child must be a mapping")
    allowed = frozenset(
        {
            "schema_version",
            "context",
            "work_item_id",
            "idempotency_key",
            "command_digest",
            "kind",
            "priority",
            "depends_on",
            "input_ref",
            "policy_digest",
            "catalog_digest",
            "model_digest",
            "max_attempts",
            "deadline_unix",
            "metadata",
            "provenance_refs",
            "max_tenant_in_flight",
        }
    )
    return _exact_mapping("SubmitWorkItems child", value, allowed)


def _submit_work_item_result(value: Any) -> dict[str, Any]:
    result = _exact_mapping(
        "SubmitWorkItem result",
        value,
        frozenset(
            {
                "schema_version",
                "work_item_id",
                "status",
                "created",
                "replayed",
                "command_sequence",
                "idempotency_key",
                "dependency_count",
                "admitted_count",
                "max_tenant_in_flight",
                "outbox_id",
                "command_digest",
                "provenance_refs",
                "changed_work_item_ids",
            }
        ),
    )
    if result["schema_version"] != "1":
        raise ValueError("SubmitWorkItem result schema_version must be 1")
    for field in (
        "work_item_id",
        "status",
        "idempotency_key",
        "outbox_id",
        "command_digest",
    ):
        _string(f"SubmitWorkItem result.{field}", result[field])
    for field in (
        "command_sequence",
        "dependency_count",
        "admitted_count",
        "max_tenant_in_flight",
    ):
        _integer(f"SubmitWorkItem result.{field}", result[field])
    _boolean("SubmitWorkItem result.created", result["created"])
    _boolean("SubmitWorkItem result.replayed", result["replayed"])
    if result["created"] == result["replayed"]:
        raise ValueError("SubmitWorkItem result must be created xor replayed")
    if (
        not isinstance(result["provenance_refs"], list)
        or len(result["provenance_refs"]) > 64
    ):
        raise ValueError("SubmitWorkItem result provenance refs are invalid")
    if (
        not isinstance(result["changed_work_item_ids"], list)
        or not 1 <= len(result["changed_work_item_ids"]) <= 1025
    ):
        raise ValueError("SubmitWorkItem result list fields are invalid")
    for reference in result["provenance_refs"]:
        _string("SubmitWorkItem result.provenance_refs[]", reference)
        if len(reference.encode("utf-8")) > 1_048_576:
            raise ValueError("SubmitWorkItem result.provenance_refs[] exceeds 1 MiB")
    for reference in result["changed_work_item_ids"]:
        _string("SubmitWorkItem result.changed_work_item_ids[]", reference)
        if len(reference.encode("utf-8")) > 512:
            raise ValueError(
                "SubmitWorkItem result.changed_work_item_ids[] exceeds 512 bytes"
            )
    if result["work_item_id"] not in result["changed_work_item_ids"]:
        raise ValueError("SubmitWorkItem result does not identify its changed row")
    return result


def _submit_work_items_result(value: Any) -> dict[str, Any]:
    result = _exact_mapping(
        "SubmitWorkItems result",
        value,
        frozenset(
            {
                "schema_version",
                "results",
                "replayed",
                "outbox_id",
                "changed_work_item_ids",
            }
        ),
    )
    if result["schema_version"] != "1":
        raise ValueError("SubmitWorkItems result schema_version must be 1")
    if (
        not isinstance(result["results"], list)
        or not 1 <= len(result["results"]) <= 128
    ):
        raise ValueError("SubmitWorkItems result count is invalid")
    for child in result["results"]:
        _submit_work_item_result(child)
    _boolean("SubmitWorkItems result.replayed", result["replayed"])
    _string("SubmitWorkItems result.outbox_id", result["outbox_id"])
    if (
        not isinstance(result["changed_work_item_ids"], list)
        or len(result["changed_work_item_ids"]) > 4096
    ):
        raise ValueError("SubmitWorkItems result changed ids are invalid")
    for reference in result["changed_work_item_ids"]:
        _string("SubmitWorkItems result.changed_work_item_ids[]", reference)
        if len(reference.encode("utf-8")) > 512:
            raise ValueError(
                "SubmitWorkItems result.changed_work_item_ids[] exceeds 512 bytes"
            )
    return result


class WorkItemClient:
    """Engine-native durable WorkItem claim, lease, and result namespace."""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def _require_submit_method(self, method: str) -> None:
        supports = getattr(self._client, "supports", None)
        if supports is None or await supports(method) is not True:
            raise NativeWorkItemSubmissionUnavailable(
                NativeWorkItemSubmissionUnavailable.code
            )

    async def submit(self, request: dict[str, Any]) -> dict[str, Any]:
        """Admit one WorkItem through the engine-native command log.

        The engine owns tenant-scoped dedupe, dependency checks, admission
        quota, command sequencing, graph-row creation, and the transactional
        outbox. A replay returns the original explicit result; reusing a key
        with a different command digest is an error.
        """
        value = _exact_mapping(
            "SubmitWorkItem request",
            request,
            frozenset(
                {
                    "schema_version",
                    "context",
                    "work_item_id",
                    "idempotency_key",
                    "command_digest",
                    "kind",
                    "priority",
                    "depends_on",
                    "input_ref",
                    "policy_digest",
                    "catalog_digest",
                    "model_digest",
                    "max_attempts",
                    "deadline_unix",
                    "metadata",
                    "provenance_refs",
                    "max_tenant_in_flight",
                },
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("SubmitWorkItem schema_version must be 1")
        if not isinstance(value["context"], dict):
            raise TypeError("SubmitWorkItem.context must be a mapping")
        for field in (
            "idempotency_key",
            "kind",
            "input_ref",
            "policy_digest",
            "catalog_digest",
            "model_digest",
        ):
            _string(f"SubmitWorkItem.{field}", value[field])
        digest = _string("SubmitWorkItem.command_digest", value["command_digest"])
        if len(digest) != 64 or any(
            char not in "0123456789abcdefABCDEF" for char in digest
        ):
            raise ValueError(
                "SubmitWorkItem.command_digest must be a SHA-256 hex value"
            )
        if value["work_item_id"] is not None:
            _string("SubmitWorkItem.work_item_id", value["work_item_id"])
        _integer(
            "SubmitWorkItem.priority", value["priority"], minimum=-1024, maximum=1024
        )
        dependencies = value["depends_on"]
        if not isinstance(dependencies, list) or len(dependencies) > 1024:
            raise ValueError("SubmitWorkItem.depends_on must contain at most 1024 ids")
        for dependency in dependencies:
            _string("SubmitWorkItem.depends_on[]", dependency)
        _integer("SubmitWorkItem.max_attempts", value["max_attempts"], minimum=1)
        if value["deadline_unix"] is not None and (
            not isinstance(value["deadline_unix"], (int, float))
            or not math.isfinite(float(value["deadline_unix"]))
            or float(value["deadline_unix"]) < 0
        ):
            raise ValueError("SubmitWorkItem.deadline_unix is invalid")
        if not isinstance(value["metadata"], dict):
            raise TypeError("SubmitWorkItem.metadata must be a mapping")
        if len(msgpack.packb(value["metadata"], use_bin_type=True)) > 64 * 1024:
            raise ValueError("SubmitWorkItem.metadata exceeds 64 KiB")
        provenance = value["provenance_refs"]
        if not isinstance(provenance, list) or len(provenance) > 64:
            raise ValueError("SubmitWorkItem.provenance_refs exceeds 64 references")
        for reference in provenance:
            _string("SubmitWorkItem.provenance_refs[]", reference)
        quota = _integer(
            "SubmitWorkItem.max_tenant_in_flight", value["max_tenant_in_flight"]
        )
        if quota < 0 or quota > 4096:
            raise ValueError("SubmitWorkItem.max_tenant_in_flight must be 0..4096")
        await self._require_submit_method("SubmitWorkItem")
        result = await self._client._send("SubmitWorkItem", {"request": value})
        return _submit_work_item_result(result)

    async def submit_batch(self, request: dict[str, Any]) -> dict[str, Any]:
        """Admit a bounded all-or-nothing SubmitWorkItems request."""
        value = _exact_mapping(
            "SubmitWorkItems request",
            request,
            frozenset({"schema_version", "context", "idempotency_key", "requests"}),
        )
        if value["schema_version"] != "1":
            raise ValueError("SubmitWorkItems schema_version must be 1")
        if not isinstance(value["context"], dict):
            raise TypeError("SubmitWorkItems.context must be a mapping")
        _string("SubmitWorkItems.idempotency_key", value["idempotency_key"])
        requests = value["requests"]
        if not isinstance(requests, list) or not 1 <= len(requests) <= 128:
            raise ValueError("SubmitWorkItems.requests must contain 1..128 items")
        # Reuse the single-item validator before sending the parent envelope.
        for child in requests:
            _submit_work_item_request_shape(child)
        await self._require_submit_method("SubmitWorkItems")
        result = await self._client._send("SubmitWorkItems", {"request": value})
        return _submit_work_items_result(result)

    async def claim(self, request: dict[str, Any]) -> dict[str, Any]:
        """Return the authoritative claim result.

        ``{"claimed": false, "reason": "empty"|"tenant_quota"}`` is final;
        callers must not fall back to a second claim implementation. The tenant
        limit is required and bounded by the current wire contract (1..=4096).
        """
        value = _exact_mapping(
            "ClaimWorkItem request",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "queue_ref",
                    "resource_class",
                    "fairness_group",
                    "worker_ref",
                    "now_ms",
                    "lease_ms",
                    "max_tenant_in_flight",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("ClaimWorkItem schema_version must be 1")
        _string("ClaimWorkItem.tenant_ref", value["tenant_ref"])
        _string("ClaimWorkItem.worker_ref", value["worker_ref"])
        for field in (
            "work_item_id",
            "queue_ref",
            "resource_class",
            "fairness_group",
        ):
            if value[field] is not None:
                _string(f"ClaimWorkItem.{field}", value[field])
        _integer("ClaimWorkItem.now_ms", value["now_ms"])
        _integer("ClaimWorkItem.lease_ms", value["lease_ms"], minimum=1)
        tenant_limit = _integer("max_tenant_in_flight", value["max_tenant_in_flight"])
        if not 1 <= tenant_limit <= 4096:
            raise ValueError("max_tenant_in_flight must be between 1 and 4096")
        result = await self._client._send("ClaimWorkItem", {"request": value})
        answer = _exact_mapping(
            "ClaimWorkItem result",
            result,
            frozenset(
                {
                    "schema_version",
                    "claimed",
                    "reason",
                    "work_item_id",
                    "kind",
                    "payload_ref",
                    "lease_holder_ref",
                    "lease_epoch",
                    "fencing_token",
                    "lease_expires_at_ms",
                    "attempt",
                    "max_attempts",
                    "tenant_in_flight",
                    "changed_work_item_ids",
                }
            ),
        )
        if answer["schema_version"] != "1":
            raise ValueError("ClaimWorkItem result schema_version must be 1")
        claimed = _boolean("ClaimWorkItem result.claimed", answer["claimed"])
        reason = answer["reason"]
        if reason not in {"claimed", "empty", "tenant_quota"}:
            raise ValueError("ClaimWorkItem result reason is invalid")
        if claimed != (reason == "claimed"):
            raise ValueError("ClaimWorkItem result claim state is inconsistent")
        if claimed:
            for field in ("work_item_id", "lease_holder_ref"):
                _string(f"ClaimWorkItem result.{field}", answer[field])
            for field in (
                "lease_epoch",
                "fencing_token",
                "lease_expires_at_ms",
                "attempt",
                "max_attempts",
            ):
                _integer(
                    f"ClaimWorkItem result.{field}",
                    answer[field],
                    minimum=1 if field == "max_attempts" else 0,
                )
            changed = answer["changed_work_item_ids"]
            if not isinstance(changed, list) or answer["work_item_id"] not in changed:
                raise ValueError("ClaimWorkItem result changed ids are invalid")
        elif (
            any(
                answer[field] is not None
                for field in (
                    "work_item_id",
                    "kind",
                    "payload_ref",
                    "lease_holder_ref",
                    "lease_epoch",
                    "fencing_token",
                    "lease_expires_at_ms",
                    "attempt",
                    "max_attempts",
                )
            )
            or answer["changed_work_item_ids"] != []
        ):
            raise ValueError("negative ClaimWorkItem result carries lease state")
        return answer

    async def mint_capability(self, request: dict[str, Any]) -> dict[str, Any]:
        """Mint/replay an opaque capability for the caller's live WorkItem lease.

        The request deliberately contains only ``schema_version`` and
        ``work_item_id``. Tenant, worker, principal, session, lease tuple, graph
        incarnation, and expiry are derived by the authenticated engine; this
        client exposes no authority DTO or tuple reconstruction helper.
        """
        value = _exact_mapping(
            "WorkItemClaimCapability mint request",
            request,
            frozenset({"schema_version", "work_item_id"}),
        )
        if value["schema_version"] != "1":
            raise ValueError("WorkItemClaimCapability schema_version must be 1")
        _string("WorkItemClaimCapability.work_item_id", value["work_item_id"])
        if len(value["work_item_id"]) > 512:
            raise ValueError("WorkItemClaimCapability.work_item_id exceeds 512 bytes")
        return _work_item_capability_result(
            await self._client._send("MintWorkItemClaimCapability", {"request": value}),
            verify=False,
        )

    async def verify_capability(self, request: dict[str, Any]) -> dict[str, Any]:
        """Verify opaque capability bytes against the live native lease.

        Verification returns one privacy-safe denial shape for all invalid or
        foreign capabilities. The engine performs the live control-row check
        before any private payload lookup; callers cannot provide owner/lease
        fields to alter that order.
        """
        value = _exact_mapping(
            "WorkItemClaimCapability verify request",
            request,
            frozenset({"schema_version", "work_item_id", "capability"}),
        )
        if value["schema_version"] != "1":
            raise ValueError("WorkItemClaimCapability schema_version must be 1")
        _string("WorkItemClaimCapability.work_item_id", value["work_item_id"])
        if len(value["work_item_id"]) > 512:
            raise ValueError("WorkItemClaimCapability.work_item_id exceeds 512 bytes")
        capability = value["capability"]
        if isinstance(capability, bytearray):
            capability = bytes(capability)
            value["capability"] = capability
        if not isinstance(capability, bytes) or len(capability) > 128:
            raise ValueError(
                "WorkItemClaimCapability.capability must be at most 128 bytes"
            )
        return _work_item_capability_result(
            await self._client._send(
                "VerifyWorkItemClaimCapability", {"request": value}
            ),
            verify=True,
        )

    async def renew(
        self,
        *,
        tenant: str,
        work_item_id: str,
        worker_id: str,
        lease_epoch: int,
        fencing_token: int,
        now_ms: int,
        lease_ms: int,
    ) -> dict[str, Any]:
        return await self._client._send(
            "RenewWorkItemLease",
            {
                "tenant": tenant,
                "work_item_id": work_item_id,
                "worker_id": worker_id,
                "lease_epoch": int(lease_epoch),
                "fencing_token": int(fencing_token),
                "now_ms": int(now_ms),
                "lease_ms": int(lease_ms),
            },
        )

    async def commit_result(
        self,
        *,
        tenant: str,
        work_item_id: str,
        worker_id: str,
        lease_epoch: int,
        fencing_token: int,
        idempotency_key: str,
        outcome: str,
        now_ms: int,
        result_ref: str | None = None,
        error_ref: str | None = None,
        retryable: bool = False,
    ) -> dict[str, Any]:
        return await self._client._send(
            "CommitWorkItemResult",
            {
                "tenant": tenant,
                "work_item_id": work_item_id,
                "worker_id": worker_id,
                "lease_epoch": int(lease_epoch),
                "fencing_token": int(fencing_token),
                "idempotency_key": idempotency_key,
                "outcome": outcome,
                "result_ref": result_ref,
                "error_ref": error_ref,
                "retryable": bool(retryable),
                "now_ms": int(now_ms),
            },
        )

    async def cancel(
        self,
        *,
        tenant: str,
        work_item_id: str,
        idempotency_key: str,
        now_ms: int,
        reason_ref: str | None = None,
    ) -> dict[str, Any]:
        """Cancel a submitted/ready item without acquiring a synthetic lease.

        An active lease is preserved and returns ``status="in_flight"``. Only
        an opaque ``reason_ref`` is transmitted; free-form reason bodies are not
        retained by the engine.
        """
        return await self._client._send(
            "CancelWorkItem",
            {
                "tenant": tenant,
                "work_item_id": work_item_id,
                "idempotency_key": idempotency_key,
                "reason_ref": reason_ref,
                "now_ms": int(now_ms),
            },
        )

    async def defer(
        self,
        *,
        tenant: str,
        work_item_id: str,
        worker_id: str,
        lease_epoch: int,
        fencing_token: int,
        idempotency_key: str,
        next_retry_at_ms: int,
        now_ms: int,
        reason_ref: str | None = None,
    ) -> dict[str, Any]:
        """Release a fenced lease for later polling without using an attempt."""
        return await self._client._send(
            "DeferWorkItem",
            {
                "tenant": tenant,
                "work_item_id": work_item_id,
                "worker_id": worker_id,
                "lease_epoch": int(lease_epoch),
                "fencing_token": int(fencing_token),
                "idempotency_key": idempotency_key,
                "next_retry_at_ms": int(next_retry_at_ms),
                "reason_ref": reason_ref,
                "now_ms": int(now_ms),
            },
        )

    async def cas_metadata(
        self,
        *,
        tenant: str,
        work_item_id: str,
        expected_status: list[str],
        now_ms: int,
        expected_lease: dict[str, Any] | None = None,
        expected_checkpoint_id: str | None = None,
        set_checkpoint_id: str | None = None,
        expected_metadata: dict[str, Any] | None = None,
        set_metadata: dict[str, Any] | None = None,
        expected_prio_bucket: int | None = None,
        set_prio_bucket: int | None = None,
    ) -> dict[str, Any]:
        """Atomic compare-and-set on one WorkItem's non-authority SCHEDULING
        METADATA (BUG-111): ``checkpoint_id`` / ``metadata`` / ``prio_bucket``.

        The native replacement for a generic
        :meth:`NodeClient.compare_and_set` against a WorkItem row, which the
        engine's native-WorkItem-authority guard unconditionally refuses once
        the row is claimed. Exactly one of ``set_checkpoint_id`` /
        ``set_metadata`` / ``set_prio_bucket`` must be given.

        Pass ``expected_lease`` (``{"worker_ref", "lease_epoch",
        "fencing_token"}``) when the caller holds the live lease (checkpoint /
        request-input); omit it for a lease-less external/scheduler caller
        (submit-input / set-priority), which fences on ``expected_status``
        (and, for metadata, ``expected_metadata``) alone.

        Returns a mapping whose ``outcome`` is one of three DISTINCT strings
        — ``"applied"``, ``"conflict"``, ``"not_found"`` — never collapsed to
        a bool: a caller that cannot tell "lost a race" from "no such item"
        from "succeeded" cannot safely decide whether to retry, re-read, or
        abandon its lease (AU-P0-3 fail-closed doctrine).
        """
        set_fields = (
            set_checkpoint_id is not None,
            set_metadata is not None,
            set_prio_bucket is not None,
        )
        if sum(set_fields) != 1:
            raise ValueError(
                "cas_metadata requires exactly one of set_checkpoint_id / "
                "set_metadata / set_prio_bucket"
            )
        if not expected_status:
            raise ValueError("cas_metadata requires a non-empty expected_status")
        _string("CasWorkItemMetadata.tenant_ref", tenant)
        _string("CasWorkItemMetadata.work_item_id", work_item_id)

        lease_field: dict[str, Any] | None = None
        if expected_lease is not None:
            lease_field = {
                "worker_ref": _string(
                    "CasWorkItemMetadata.expected_lease.worker_ref",
                    expected_lease["worker_ref"],
                ),
                "lease_epoch": _integer(
                    "CasWorkItemMetadata.expected_lease.lease_epoch",
                    int(expected_lease["lease_epoch"]),
                ),
                "fencing_token": _integer(
                    "CasWorkItemMetadata.expected_lease.fencing_token",
                    int(expected_lease["fencing_token"]),
                ),
            }

        request = {
            "schema_version": "1",
            "tenant_ref": tenant,
            "work_item_id": work_item_id,
            "expected_lease": lease_field,
            "expected_status": list(expected_status),
            "expected_checkpoint_id": expected_checkpoint_id,
            "set_checkpoint_id": set_checkpoint_id,
            "expected_metadata_msgpack": (
                _pack_binary_msgpack(expected_metadata or {})
                if set_metadata is not None
                else None
            ),
            "set_metadata_msgpack": (
                _pack_binary_msgpack(set_metadata) if set_metadata is not None else None
            ),
            "expected_prio_bucket": expected_prio_bucket,
            "set_prio_bucket": set_prio_bucket,
            "now_ms": int(now_ms),
        }
        result = await self._client._send("CasWorkItemMetadata", {"request": request})
        answer = _exact_mapping(
            "CasWorkItemMetadata result",
            result,
            frozenset(
                {"schema_version", "outcome", "work_item_id", "changed_work_item_ids"}
            ),
        )
        if answer["schema_version"] != "1":
            raise ValueError("CasWorkItemMetadata result schema_version must be 1")
        if answer["outcome"] not in {"applied", "conflict", "not_found"}:
            raise ValueError("CasWorkItemMetadata result outcome is invalid")
        changed = answer["changed_work_item_ids"]
        if not isinstance(changed, list):
            raise ValueError("CasWorkItemMetadata result changed ids are invalid")
        if answer["outcome"] == "applied" and changed != [work_item_id]:
            raise ValueError(
                "applied CasWorkItemMetadata result changed ids are invalid"
            )
        if answer["outcome"] != "applied" and changed != []:
            raise ValueError(
                "non-applied CasWorkItemMetadata result must not change any row"
            )
        return answer

    async def _require_resource_method(self, method: str) -> None:
        """Negotiate the additive method before sending it to an older engine."""

        supports = getattr(self._client, "supports", None)
        if supports is None:
            raise NativeResourceReservationUnavailable(
                NativeResourceReservationUnavailable.code
            )
        advertised = await supports(method)
        if advertised is not True:
            raise NativeResourceReservationUnavailable(
                NativeResourceReservationUnavailable.code
            )

    async def reserve(self, request: dict[str, Any]) -> dict[str, Any]:
        """Atomically reserve host resources for one exact WorkItem attempt."""

        payload = _resource_reservation_request(request)
        await self._require_resource_method("ReserveWorkItemResources")
        value = await self._client._send(
            "ReserveWorkItemResources", {"request": payload}
        )
        return _resource_reservation_result(value)

    async def release(self, request: dict[str, Any]) -> dict[str, Any]:
        """Atomically release a current/terminal reservation and retain its tombstone."""

        payload = _resource_reservation_request(request)
        await self._require_resource_method("ReleaseWorkItemResources")
        value = await self._client._send(
            "ReleaseWorkItemResources", {"request": payload}
        )
        return _resource_reservation_result(value)

    async def reclaim(self, request: dict[str, Any]) -> dict[str, Any]:
        """Atomically reclaim an expired or superseded reservation."""

        payload = _resource_reservation_request(request)
        await self._require_resource_method("ReclaimWorkItemResources")
        value = await self._client._send(
            "ReclaimWorkItemResources", {"request": payload}
        )
        return _resource_reservation_result(value)

    async def query_reservation(self, request: dict[str, Any]) -> dict[str, Any]:
        """Read one native reservation/tombstone; local mirrors are not authority."""

        await self._require_resource_method("QueryWorkItemReservation")
        value = await self._client._send(
            "QueryWorkItemReservation", {"request": _resource_status_request(request)}
        )
        return _resource_reservation_result(value)

    async def status(self, request: dict[str, Any]) -> dict[str, Any]:
        """Return bounded native reservation reconciliation/status."""

        await self._require_resource_method("ResourceReservationStatus")
        request = _resource_status_request(request)
        value = await self._client._send(
            "ResourceReservationStatus", {"request": request}
        )
        result = _resource_reservation_status_result(value)
        if len(result["reservations"]) > request["limit"]:
            raise ValueError("ResourceReservationStatus result exceeds requested limit")
        return result

    async def update_host(self, request: dict[str, Any]) -> dict[str, Any]:
        """Publish monotonic host telemetry while preserving native held totals."""

        update = _resource_host_update_request(request)
        await self._require_resource_method("UpdateResourceHost")
        value = await self._client._send("UpdateResourceHost", {"request": update})
        return _resource_host_update_result(value)


class CapacityLeaseClient:
    """Engine-native bounded CapacityCell/CapacityLease namespace."""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def _require_method(self, method: str) -> None:
        supports = getattr(self._client, "supports", None)
        if supports is None or await supports(method) is not True:
            raise NativeCapacityLeaseUnavailable(NativeCapacityLeaseUnavailable.code)

    @staticmethod
    def _lease_result(value: Any, name: str) -> dict[str, Any]:
        result = _exact_mapping(
            f"{name} result",
            value,
            frozenset({"schema_version", "decision", "leases", "message"}),
        )
        if (
            result["schema_version"] != "1"
            or result["decision"] not in _CAPACITY_DECISIONS
        ):
            raise ValueError(f"{name} result schema/decision is invalid")
        if not isinstance(result["leases"], list) or len(result["leases"]) > 16:
            raise ValueError(f"{name} result leases must contain at most 16 entries")
        if result["message"] is not None:
            _string(f"{name} result.message", result["message"])
        return result

    async def acquire(self, request: dict[str, Any]) -> dict[str, Any]:
        value = _exact_mapping(
            "AcquireCapacity request",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "owner_digest",
                    "idempotency_key",
                    "priority",
                    "demands",
                    "lease_id",
                    "ttl_ms",
                    "now_ms",
                    "cost_budget_micros",
                    "token_budget",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("AcquireCapacity schema_version must be 1")
        for field in ("tenant_ref", "work_item_id", "owner_digest", "idempotency_key"):
            _string(f"AcquireCapacity.{field}", value[field])
            if len(value[field].encode("utf-8")) > 512:
                raise ValueError(f"AcquireCapacity.{field} exceeds 512 bytes")
        if value["lease_id"] is not None:
            _string("AcquireCapacity.lease_id", value["lease_id"])
            if len(value["lease_id"].encode("utf-8")) > 512:
                raise ValueError("AcquireCapacity.lease_id exceeds 512 bytes")
        if value["priority"] not in {
            "interactive",
            "orchestration",
            "hydration",
            "background_ingestion",
        }:
            raise ValueError("AcquireCapacity.priority is invalid")
        demands = value["demands"]
        if not isinstance(demands, list) or not 1 <= len(demands) <= 16:
            raise ValueError("AcquireCapacity.demands must contain 1..16 entries")
        for demand in demands:
            row = _exact_mapping(
                "AcquireCapacity demand",
                demand,
                frozenset({"cell_id", "resource_class", "amount"}),
            )
            _string("AcquireCapacity demand.cell_id", row["cell_id"])
            if row["resource_class"] not in {
                "llm_generator",
                "llm_embedding",
                "gpu",
                "worker",
                "cpu",
                "broker",
            }:
                raise ValueError("AcquireCapacity demand.resource_class is invalid")
            _integer("AcquireCapacity demand.amount", row["amount"], minimum=1)
            if row["amount"] > 1_000_000_000:
                raise ValueError("AcquireCapacity demand.amount exceeds 1e9")
        _integer("AcquireCapacity.ttl_ms", value["ttl_ms"], minimum=1)
        if value["ttl_ms"] > 24 * 60 * 60 * 1000:
            raise ValueError("AcquireCapacity.ttl_ms exceeds 24h")
        _integer("AcquireCapacity.now_ms", value["now_ms"])
        for field in ("cost_budget_micros", "token_budget"):
            if value[field] is not None:
                _integer(f"AcquireCapacity.{field}", value[field])
                if value[field] > 1_000_000_000_000:
                    raise ValueError(f"AcquireCapacity.{field} exceeds native bound")
        await self._require_method("AcquireCapacity")
        result = await self._client._send("AcquireCapacity", {"request": value})
        answer = _exact_mapping(
            "AcquireCapacity result",
            result,
            frozenset({"schema_version", "decision", "leases", "available", "message"}),
        )
        if (
            answer["schema_version"] != "1"
            or answer["decision"] not in _CAPACITY_DECISIONS
        ):
            raise ValueError("AcquireCapacity result schema/decision is invalid")
        if (
            not isinstance(answer["leases"], list)
            or len(answer["leases"]) > 16
            or not isinstance(answer["available"], list)
            or len(answer["available"]) > 16
        ):
            raise ValueError("AcquireCapacity result list fields are invalid")
        return answer

    async def renew(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._mutate("RenewCapacity", request, renew=True)

    async def release(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._mutate("ReleaseCapacity", request, renew=False)

    async def _mutate(
        self, method: str, request: dict[str, Any], *, renew: bool
    ) -> dict[str, Any]:
        value = _exact_mapping(
            f"{method} request",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "owner_digest",
                    "leases",
                    "now_ms",
                    "ttl_ms",
                    "idempotency_key",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError(f"{method} schema_version must be 1")
        _string(f"{method}.tenant_ref", value["tenant_ref"])
        _string(f"{method}.owner_digest", value["owner_digest"])
        if any(
            len(value[field].encode("utf-8")) > 512
            for field in ("tenant_ref", "owner_digest")
        ):
            raise ValueError(f"{method} identity field exceeds 512 bytes")
        leases = value["leases"]
        if not isinstance(leases, list) or not 1 <= len(leases) <= 16:
            raise ValueError(f"{method}.leases must contain 1..16 entries")
        for fence in leases:
            row = _exact_mapping(
                f"{method} lease fence",
                fence,
                frozenset({"lease_id", "lease_epoch", "fence_token"}),
            )
            _string(f"{method}.lease_id", row["lease_id"])
            if len(row["lease_id"].encode("utf-8")) > 512:
                raise ValueError(f"{method}.lease_id exceeds 512 bytes")
            _integer(f"{method}.lease_epoch", row["lease_epoch"], minimum=1)
            _integer(f"{method}.fence_token", row["fence_token"], minimum=1)
        _integer(f"{method}.now_ms", value["now_ms"])
        # Mirror the native authority exactly: capacity_lease.rs bounds ttl_ms
        # only on RENEWAL (`if renew && request.ttl_ms.is_some_and(...)`).
        # ReleaseCapacity hands a lease back, so a supplied ttl is meaningless
        # and the server ignores it. Validating it here unconditionally made the
        # client STRICTER than the engine, rejecting a release the engine would
        # have accepted -- a client must never invent a contract the authority
        # does not enforce.
        if renew and value["ttl_ms"] is not None:
            _integer(f"{method}.ttl_ms", value["ttl_ms"], minimum=1)
            if value["ttl_ms"] > 24 * 60 * 60 * 1000:
                raise ValueError(f"{method}.ttl_ms exceeds 24h")
        if value["idempotency_key"] is not None:
            _string(f"{method}.idempotency_key", value["idempotency_key"])
        await self._require_method(method)
        return self._lease_result(
            await self._client._send(method, {"request": value}), method
        )

    async def reclaim(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._reclaim_or_status(
            "ReclaimExpiredCapacity", request, reclaim=True
        )

    async def status(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._reclaim_or_status("CapacityStatus", request, reclaim=False)

    async def reconcile(self, request: dict[str, Any]) -> dict[str, Any]:
        return await self._reclaim_or_status(
            "ReconcileCapacity", request, reclaim=False
        )

    async def _reclaim_or_status(
        self, method: str, request: dict[str, Any], *, reclaim: bool
    ) -> dict[str, Any]:
        allowed = {"schema_version", "tenant_ref", "cell_id", "max_count", "cursor"}
        if not reclaim:
            allowed.add("lease_id")
        value = _exact_mapping(f"{method} request", request, frozenset(allowed))
        if value["schema_version"] != "1":
            raise ValueError(f"{method} schema_version must be 1")
        _string(f"{method}.tenant_ref", value["tenant_ref"])
        if value["cell_id"] is not None:
            _string(f"{method}.cell_id", value["cell_id"])
        if not reclaim and value["lease_id"] is not None:
            _string(f"{method}.lease_id", value["lease_id"])
        if value["cursor"] is not None:
            _string(f"{method}.cursor", value["cursor"])
        _integer(f"{method}.max_count", value["max_count"], minimum=1)
        if value["max_count"] > 128:
            raise ValueError(f"{method}.max_count exceeds 128")
        await self._require_method(method)
        result = await self._client._send(method, {"request": value})
        if reclaim:
            answer = _exact_mapping(
                f"{method} result",
                result,
                frozenset(
                    {"schema_version", "decision", "reclaimed_lease_ids", "next_cursor"}
                ),
            )
            if (
                answer["schema_version"] != "1"
                or answer["decision"] not in _CAPACITY_DECISIONS
            ):
                raise ValueError(f"{method} result schema/decision is invalid")
            if (
                not isinstance(answer["reclaimed_lease_ids"], list)
                or len(answer["reclaimed_lease_ids"]) > 128
            ):
                raise ValueError(f"{method} result lease ids are invalid")
            return answer
        answer = _exact_mapping(
            f"{method} result",
            result,
            frozenset({"schema_version", "cells", "leases", "next_cursor"}),
        )
        if (
            answer["schema_version"] != "1"
            or not isinstance(answer["cells"], list)
            or len(answer["cells"]) > 128
            or not isinstance(answer["leases"], list)
            or len(answer["leases"]) > 128
        ):
            raise ValueError(f"{method} result shape is invalid")
        return answer

    async def update_cell(self, request: dict[str, Any]) -> dict[str, Any]:
        value = _exact_mapping(
            "UpdateCapacityCell request",
            request,
            frozenset({"schema_version", "cell", "expected_epoch", "now_ms"}),
        )
        if value["schema_version"] != "1" or not isinstance(value["cell"], dict):
            raise ValueError("UpdateCapacityCell request shape is invalid")
        _integer("UpdateCapacityCell.now_ms", value["now_ms"])
        if value["expected_epoch"] is not None:
            _integer(
                "UpdateCapacityCell.expected_epoch", value["expected_epoch"], minimum=0
            )
        await self._require_method("UpdateCapacityCell")
        result = await self._client._send("UpdateCapacityCell", {"request": value})
        answer = _exact_mapping(
            "UpdateCapacityCell result",
            result,
            frozenset({"schema_version", "decision", "cell", "message"}),
        )
        if (
            answer["schema_version"] != "1"
            or answer["decision"] not in _CAPACITY_DECISIONS
        ):
            raise ValueError("UpdateCapacityCell result schema/decision is invalid")
        return answer


# CONCEPT:EG-KG.txn.per-graph-write-isolation — RMDD-28 native development-lane hold
# vocabulary/validators, mirroring the ``WorkItemClient`` validation style
# above field-for-field against the frozen RMDD-28 protocol
# (``crates/eg-types/src/epistemic_operations.rs`` / ``protocol.rs``).
_DEVELOPMENT_LANE_DECISIONS = frozenset(
    {
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "input_conflict",
        "quota",
        "policy",
        "drained",
        "not_found",
        "wrong_kind",
        "wrong_tenant",
        "wrong_owner",
        "wrong_attempt",
        "wrong_lease_epoch",
        "wrong_fence",
        "expired",
        "terminal",
        "cleanup_required",
        "exclusivity",
        "invalid",
    }
)

_DEVELOPMENT_LANE_QUOTA_DECISIONS = frozenset(
    {
        "accepted",
        "idempotent",
        "stale",
        "conflict",
        "quota",
        "policy",
        "drained",
        "invalid",
    }
)

_DEVELOPMENT_LANE_HOST_TARGET_KINDS = frozenset({"local", "inventory_alias"})

_DEVELOPMENT_LANE_HOLD_STATES = frozenset(
    {
        "allocating",
        "active",
        "submitted",
        "released",
        "expired",
        "cleanup_pending",
        "cleaned",
        "aborted",
        "absent",
    }
)

_DEVELOPMENT_LANE_TERMINAL_STATES = frozenset(
    {"succeeded", "failed", "cancelled", "dead_letter"}
)


def _development_lane_decision(name: str, value: Any) -> str:
    if value not in _DEVELOPMENT_LANE_DECISIONS:
        raise ValueError(f"{name} is not a valid development-lane decision")
    return cast(str, value)


def _development_lane_quota_charge(name: str, value: Any) -> dict[str, Any]:
    charge = _exact_mapping(
        name,
        value,
        frozenset(
            {
                "schema_version",
                "tenant_count",
                "owner_count",
                "session_count",
                "workspace_count",
                "repository_count",
                "host_count",
                "global_count",
                "tenant_predicted_disk_bytes",
                "owner_predicted_disk_bytes",
                "session_predicted_disk_bytes",
                "workspace_predicted_disk_bytes",
                "repository_predicted_disk_bytes",
                "host_predicted_disk_bytes",
                "global_predicted_disk_bytes",
                "tenant_observed_disk_bytes",
                "owner_observed_disk_bytes",
                "session_observed_disk_bytes",
                "workspace_observed_disk_bytes",
                "repository_observed_disk_bytes",
                "host_observed_disk_bytes",
                "global_observed_disk_bytes",
                "tenant_retained_disk_bytes",
                "owner_retained_disk_bytes",
                "session_retained_disk_bytes",
                "workspace_retained_disk_bytes",
                "repository_retained_disk_bytes",
                "host_retained_disk_bytes",
                "global_retained_disk_bytes",
                "revision",
                "policy_revision",
            }
        ),
    )
    if charge["schema_version"] != "1":
        raise ValueError(f"{name}.schema_version must be 1")
    for field in charge:
        if field == "schema_version":
            continue
        _integer(f"{name}.{field}", charge[field])
    return charge


def _development_lane_quota_policy(name: str, value: Any) -> dict[str, Any]:
    policy = _exact_mapping(
        name,
        value,
        frozenset(
            {
                "schema_version",
                "policy_name",
                "policy_version",
                "tenant_count_limit",
                "owner_count_limit",
                "session_count_limit",
                "workspace_count_limit",
                "repository_count_limit",
                "host_count_limit",
                "global_count_limit",
                "tenant_predicted_disk_bytes",
                "owner_predicted_disk_bytes",
                "session_predicted_disk_bytes",
                "workspace_predicted_disk_bytes",
                "repository_predicted_disk_bytes",
                "host_predicted_disk_bytes",
                "global_predicted_disk_bytes",
                "tenant_observed_disk_bytes",
                "owner_observed_disk_bytes",
                "session_observed_disk_bytes",
                "workspace_observed_disk_bytes",
                "repository_observed_disk_bytes",
                "host_observed_disk_bytes",
                "global_observed_disk_bytes",
                "tenant_retained_disk_bytes",
                "owner_retained_disk_bytes",
                "session_retained_disk_bytes",
                "workspace_retained_disk_bytes",
                "repository_retained_disk_bytes",
                "host_retained_disk_bytes",
                "global_retained_disk_bytes",
                "min_ttl_ms",
                "max_ttl_ms",
                "max_observation_staleness_ms",
                "drain_only",
            }
        ),
    )
    if policy["schema_version"] != "1":
        raise ValueError(f"{name}.schema_version must be 1")
    _string(f"{name}.policy_name", policy["policy_name"])
    _string(f"{name}.policy_version", policy["policy_version"])
    for field in policy:
        if field in ("schema_version", "policy_name", "policy_version", "drain_only"):
            continue
        _integer(f"{name}.{field}", policy[field])
    _boolean(f"{name}.drain_only", policy["drain_only"])
    return policy


def _development_lane_intent(value: Any) -> dict[str, Any]:
    intent = _exact_mapping(
        "DevelopmentLaneIntent",
        value,
        frozenset(
            {
                "schema_version",
                "tenant_ref",
                "request_id",
                "lane_id",
                "repository_id",
                "base_ref",
                "base_sha",
                "branch",
                "host_target_kind",
                "host_target_alias",
                "host_ref",
                "resource_reservation_id",
                "workspace_ref",
                "worktree_locator",
                "owner_id",
                "session_id",
                "fairness_group",
                "quota_policy_name",
                "quota_policy_version",
                "predicted_disk_bytes",
                "ttl_ms",
                "input_fingerprint",
            }
        ),
    )
    if intent["schema_version"] != "1":
        raise ValueError("DevelopmentLaneIntent.schema_version must be 1")
    for field in (
        "tenant_ref",
        "request_id",
        "lane_id",
        "repository_id",
        "base_ref",
        "base_sha",
        "branch",
        "host_ref",
        "resource_reservation_id",
        "workspace_ref",
        "worktree_locator",
        "owner_id",
        "session_id",
        "fairness_group",
        "quota_policy_name",
        "quota_policy_version",
        "input_fingerprint",
    ):
        _string(f"DevelopmentLaneIntent.{field}", intent[field])
    if intent["host_target_kind"] not in _DEVELOPMENT_LANE_HOST_TARGET_KINDS:
        raise ValueError("DevelopmentLaneIntent.host_target_kind is invalid")
    if intent["host_target_alias"] is not None:
        _string("DevelopmentLaneIntent.host_target_alias", intent["host_target_alias"])
    _integer(
        "DevelopmentLaneIntent.predicted_disk_bytes", intent["predicted_disk_bytes"]
    )
    _integer("DevelopmentLaneIntent.ttl_ms", intent["ttl_ms"], minimum=1)
    return intent


def _development_lane_hold(value: Any) -> dict[str, Any]:
    hold = _exact_mapping(
        "DevelopmentLaneHold",
        value,
        frozenset(
            {
                "schema_version",
                "hold_id",
                "lane_id",
                "tenant_ref",
                "request_id",
                "work_item_id",
                "owner_id",
                "session_id",
                "fairness_group",
                "workspace_ref",
                "repository_id",
                "base_ref",
                "base_sha",
                "branch",
                "worktree_locator",
                "host_target_kind",
                "host_target_alias",
                "host_ref",
                "quota_policy_name",
                "quota_policy_version",
                "input_fingerprint",
                "predicted_disk_bytes",
                "observed_disk_bytes",
                "retained_disk_bytes",
                "active_count_charged",
                "quota_charge",
                "state",
                "attempt",
                "lease_epoch",
                "fencing_token",
                "work_item_fence",
                "hold_revision",
                "lifecycle_revision",
                "allocation_revision",
                "cleanup_revision",
                "expires_at_ms",
                "last_renewed_at_ms",
                "cleanup_work_item_id",
                "cleanup_work_item_fence",
                "cleanup_attempt",
                "cleanup_lease_epoch",
                "cleanup_fencing_token",
                "tombstone",
            }
        ),
    )
    if hold["schema_version"] != "1":
        raise ValueError("DevelopmentLaneHold.schema_version must be 1")
    for field in (
        "hold_id",
        "lane_id",
        "tenant_ref",
        "request_id",
        "work_item_id",
        "owner_id",
        "session_id",
        "fairness_group",
        "workspace_ref",
        "repository_id",
        "base_ref",
        "base_sha",
        "branch",
        "worktree_locator",
        "host_ref",
        "quota_policy_name",
        "quota_policy_version",
        "input_fingerprint",
        "work_item_fence",
    ):
        _string(f"DevelopmentLaneHold.{field}", hold[field])
    if hold["host_target_kind"] not in _DEVELOPMENT_LANE_HOST_TARGET_KINDS:
        raise ValueError("DevelopmentLaneHold.host_target_kind is invalid")
    if hold["host_target_alias"] is not None:
        _string("DevelopmentLaneHold.host_target_alias", hold["host_target_alias"])
    if hold["state"] not in _DEVELOPMENT_LANE_HOLD_STATES:
        raise ValueError("DevelopmentLaneHold.state is invalid")
    _development_lane_quota_charge(
        "DevelopmentLaneHold.quota_charge", hold["quota_charge"]
    )
    for field in (
        "predicted_disk_bytes",
        "observed_disk_bytes",
        "retained_disk_bytes",
        "attempt",
        "lease_epoch",
        "fencing_token",
        "hold_revision",
        "lifecycle_revision",
        "allocation_revision",
        "cleanup_revision",
        "expires_at_ms",
        "last_renewed_at_ms",
    ):
        _integer(f"DevelopmentLaneHold.{field}", hold[field])
    _boolean("DevelopmentLaneHold.active_count_charged", hold["active_count_charged"])
    _boolean("DevelopmentLaneHold.tombstone", hold["tombstone"])
    if hold["cleanup_work_item_id"] is not None:
        _string(
            "DevelopmentLaneHold.cleanup_work_item_id", hold["cleanup_work_item_id"]
        )
    if hold["cleanup_work_item_fence"] is not None:
        _string(
            "DevelopmentLaneHold.cleanup_work_item_fence",
            hold["cleanup_work_item_fence"],
        )
    for field in ("cleanup_attempt", "cleanup_lease_epoch", "cleanup_fencing_token"):
        if hold[field] is not None:
            _integer(f"DevelopmentLaneHold.{field}", hold[field])
    return hold


def _development_lane_hold_result(name: str, value: Any) -> dict[str, Any]:
    """Validate the shared result shape of reserve/renew/observe/finish/cleanup.

    All five mutating RMDD-28 operations return the identical
    ``{schema_version, decision, hold, hold_revision, lifecycle_revision,
    tombstone, changed_work_item_ids, quota_charge}`` envelope.
    """

    result = _exact_mapping(
        name,
        value,
        frozenset(
            {
                "schema_version",
                "decision",
                "hold",
                "hold_revision",
                "lifecycle_revision",
                "tombstone",
                "changed_work_item_ids",
                "quota_charge",
            }
        ),
    )
    if result["schema_version"] != "1":
        raise ValueError(f"{name}.schema_version must be 1")
    _development_lane_decision(f"{name}.decision", result["decision"])
    if result["hold"] is not None:
        _development_lane_hold(result["hold"])
    _integer(f"{name}.hold_revision", result["hold_revision"])
    _integer(f"{name}.lifecycle_revision", result["lifecycle_revision"])
    _boolean(f"{name}.tombstone", result["tombstone"])
    changed = result["changed_work_item_ids"]
    if not isinstance(changed, list) or not all(
        isinstance(item, str) for item in changed
    ):
        raise ValueError(f"{name}.changed_work_item_ids must be a list of strings")
    if result["quota_charge"] is not None:
        _development_lane_quota_charge(f"{name}.quota_charge", result["quota_charge"])
    return result


class DevelopmentLaneClient:
    """Engine-native RMDD-28 development-lane hold/quota namespace.

    Mirrors :class:`WorkItemClient` exactly in shape, error handling, and
    capability negotiation: each method sends one ``{"request": ...}``
    envelope for its Method name, strictly validates the request before
    sending and the result after receiving (frozen protocol in
    ``crates/eg-types/src/epistemic_operations.rs`` /
    ``crates/eg-capabilities/src/lib.rs``), and does not fall back to a local
    approximation. Capability negotiation is the caller's responsibility via
    :meth:`EpistemicGraphClient.supports` (see
    ``agent_utilities.orchestration.development_lane.
    EngineNativeDevelopmentLaneTransport``, which this namespace's method
    names/shapes were written against method-for-method: ``reserve``,
    ``renew``, ``observe``, ``finish``, ``cleanup_complete``, ``query``,
    ``status``, ``update_quota``).
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def reserve(self, request: dict[str, Any]) -> dict[str, Any]:
        """Atomically win one branch/worktree/quota hold for a lane WorkItem attempt."""

        value = _exact_mapping(
            "DevelopmentLaneReserveRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "owner_id",
                    "attempt",
                    "lease_epoch",
                    "fencing_token",
                    "work_item_fence",
                    "intent",
                    "idempotency_key",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("DevelopmentLaneReserveRequest.schema_version must be 1")
        for field in (
            "tenant_ref",
            "work_item_id",
            "owner_id",
            "work_item_fence",
            "idempotency_key",
        ):
            _string(f"DevelopmentLaneReserveRequest.{field}", value[field])
        for field in ("attempt", "lease_epoch", "fencing_token", "now_ms"):
            _integer(f"DevelopmentLaneReserveRequest.{field}", value[field])
        _development_lane_intent(value["intent"])
        result = await self._client._send("ReserveDevelopmentLane", {"request": value})
        return _development_lane_hold_result("DevelopmentLaneResult", result)

    async def renew(self, request: dict[str, Any]) -> dict[str, Any]:
        """Renew the current hold in place, bound to the live WorkItem lease."""

        value = _exact_mapping(
            "DevelopmentLaneRenewRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "owner_id",
                    "attempt",
                    "lease_epoch",
                    "fencing_token",
                    "work_item_fence",
                    "hold_id",
                    "expected_hold_revision",
                    "ttl_ms",
                    "idempotency_key",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("DevelopmentLaneRenewRequest.schema_version must be 1")
        for field in (
            "tenant_ref",
            "work_item_id",
            "owner_id",
            "work_item_fence",
            "hold_id",
            "idempotency_key",
        ):
            _string(f"DevelopmentLaneRenewRequest.{field}", value[field])
        for field in (
            "attempt",
            "lease_epoch",
            "fencing_token",
            "expected_hold_revision",
            "now_ms",
        ):
            _integer(f"DevelopmentLaneRenewRequest.{field}", value[field])
        _integer("DevelopmentLaneRenewRequest.ttl_ms", value["ttl_ms"], minimum=1)
        result = await self._client._send("RenewDevelopmentLane", {"request": value})
        return _development_lane_hold_result("DevelopmentLaneRenewResult", result)

    async def observe(self, request: dict[str, Any]) -> dict[str, Any]:
        """Replace the monotonic retained-footprint charge with a fresh observation."""

        value = _exact_mapping(
            "DevelopmentLaneObserveRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "owner_id",
                    "attempt",
                    "lease_epoch",
                    "fencing_token",
                    "work_item_fence",
                    "hold_id",
                    "expected_hold_revision",
                    "observed_disk_bytes",
                    "observation_revision",
                    "idempotency_key",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("DevelopmentLaneObserveRequest.schema_version must be 1")
        for field in (
            "tenant_ref",
            "work_item_id",
            "owner_id",
            "work_item_fence",
            "hold_id",
            "idempotency_key",
        ):
            _string(f"DevelopmentLaneObserveRequest.{field}", value[field])
        for field in (
            "attempt",
            "lease_epoch",
            "fencing_token",
            "expected_hold_revision",
            "observed_disk_bytes",
            "observation_revision",
            "now_ms",
        ):
            _integer(f"DevelopmentLaneObserveRequest.{field}", value[field])
        result = await self._client._send("ObserveDevelopmentLane", {"request": value})
        return _development_lane_hold_result("DevelopmentLaneObserveResult", result)

    async def finish(self, request: dict[str, Any]) -> dict[str, Any]:
        """Commit terminal WorkItem completion and release active-count charge."""

        value = _exact_mapping(
            "DevelopmentLaneFinishRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "owner_id",
                    "attempt",
                    "lease_epoch",
                    "fencing_token",
                    "work_item_fence",
                    "hold_id",
                    "expected_hold_revision",
                    "terminal_state",
                    "idempotency_key",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("DevelopmentLaneFinishRequest.schema_version must be 1")
        for field in (
            "tenant_ref",
            "work_item_id",
            "owner_id",
            "work_item_fence",
            "hold_id",
            "idempotency_key",
        ):
            _string(f"DevelopmentLaneFinishRequest.{field}", value[field])
        for field in (
            "attempt",
            "lease_epoch",
            "fencing_token",
            "expected_hold_revision",
            "now_ms",
        ):
            _integer(f"DevelopmentLaneFinishRequest.{field}", value[field])
        if value["terminal_state"] not in _DEVELOPMENT_LANE_TERMINAL_STATES:
            raise ValueError("DevelopmentLaneFinishRequest.terminal_state is invalid")
        result = await self._client._send("FinishDevelopmentLane", {"request": value})
        return _development_lane_hold_result("DevelopmentLaneFinishResult", result)

    async def cleanup_complete(self, request: dict[str, Any]) -> dict[str, Any]:
        """Release retained disk/exclusivity after guarded local removal succeeds."""

        value = _exact_mapping(
            "DevelopmentLaneCleanupCompleteRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "work_item_id",
                    "owner_id",
                    "attempt",
                    "lease_epoch",
                    "fencing_token",
                    "work_item_fence",
                    "cleanup_work_item_id",
                    "cleanup_work_item_fence",
                    "cleanup_attempt",
                    "cleanup_lease_epoch",
                    "cleanup_fencing_token",
                    "hold_id",
                    "expected_hold_revision",
                    "removal_proof_ref",
                    "idempotency_key",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError(
                "DevelopmentLaneCleanupCompleteRequest.schema_version must be 1"
            )
        for field in (
            "tenant_ref",
            "work_item_id",
            "owner_id",
            "work_item_fence",
            "cleanup_work_item_id",
            "cleanup_work_item_fence",
            "hold_id",
            "removal_proof_ref",
            "idempotency_key",
        ):
            _string(f"DevelopmentLaneCleanupCompleteRequest.{field}", value[field])
        for field in (
            "attempt",
            "lease_epoch",
            "fencing_token",
            "cleanup_attempt",
            "cleanup_lease_epoch",
            "cleanup_fencing_token",
            "expected_hold_revision",
            "now_ms",
        ):
            _integer(f"DevelopmentLaneCleanupCompleteRequest.{field}", value[field])
        result = await self._client._send("CleanupDevelopmentLane", {"request": value})
        return _development_lane_hold_result(
            "DevelopmentLaneCleanupCompleteResult", result
        )

    async def query(self, request: dict[str, Any]) -> dict[str, Any]:
        """Read one native hold/tombstone by exact id; local mirrors are not authority."""

        value = _exact_mapping(
            "DevelopmentLaneQueryRequest",
            request,
            frozenset({"schema_version", "tenant_ref", "hold_id", "now_ms"}),
        )
        if value["schema_version"] != "1":
            raise ValueError("DevelopmentLaneQueryRequest.schema_version must be 1")
        _string("DevelopmentLaneQueryRequest.tenant_ref", value["tenant_ref"])
        _string("DevelopmentLaneQueryRequest.hold_id", value["hold_id"])
        _integer("DevelopmentLaneQueryRequest.now_ms", value["now_ms"])
        result = await self._client._send("QueryDevelopmentLane", {"request": value})
        answer = _exact_mapping(
            "DevelopmentLaneQueryResult",
            result,
            frozenset(
                {
                    "schema_version",
                    "decision",
                    "hold",
                    "hold_revision",
                    "lifecycle_revision",
                    "tombstone",
                }
            ),
        )
        if answer["schema_version"] != "1":
            raise ValueError("DevelopmentLaneQueryResult.schema_version must be 1")
        _development_lane_decision(
            "DevelopmentLaneQueryResult.decision", answer["decision"]
        )
        if answer["hold"] is not None:
            _development_lane_hold(answer["hold"])
        _integer("DevelopmentLaneQueryResult.hold_revision", answer["hold_revision"])
        _integer(
            "DevelopmentLaneQueryResult.lifecycle_revision",
            answer["lifecycle_revision"],
        )
        _boolean("DevelopmentLaneQueryResult.tombstone", answer["tombstone"])
        return answer

    async def status(self, request: dict[str, Any]) -> dict[str, Any]:
        """Return bounded tenant-scoped hold status with maintained counters."""

        value = _exact_mapping(
            "DevelopmentLaneStatusRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "hold_id",
                    "lane_id",
                    "work_item_id",
                    "limit",
                    "cursor",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError("DevelopmentLaneStatusRequest.schema_version must be 1")
        _string("DevelopmentLaneStatusRequest.tenant_ref", value["tenant_ref"])
        for field in ("hold_id", "lane_id", "work_item_id", "cursor"):
            if value[field] is not None:
                _string(f"DevelopmentLaneStatusRequest.{field}", value[field])
        limit = _integer(
            "DevelopmentLaneStatusRequest.limit", value["limit"], minimum=1
        )
        _integer("DevelopmentLaneStatusRequest.now_ms", value["now_ms"])
        result = await self._client._send("DevelopmentLaneStatus", {"request": value})
        answer = _exact_mapping(
            "DevelopmentLaneStatusResult",
            result,
            frozenset(
                {
                    "schema_version",
                    "complete",
                    "next_cursor",
                    "holds",
                    "counters",
                    "tenant_active_count",
                    "tenant_retained_disk_bytes",
                    "tombstone",
                }
            ),
        )
        if answer["schema_version"] != "1":
            raise ValueError("DevelopmentLaneStatusResult.schema_version must be 1")
        _boolean("DevelopmentLaneStatusResult.complete", answer["complete"])
        if answer["next_cursor"] is not None:
            _string("DevelopmentLaneStatusResult.next_cursor", answer["next_cursor"])
        holds = answer["holds"]
        if not isinstance(holds, list):
            raise ValueError("DevelopmentLaneStatusResult.holds must be a list")
        if len(holds) > limit:
            raise ValueError(
                "DevelopmentLaneStatusResult.holds exceeded the requested limit"
            )
        for hold in holds:
            _development_lane_hold(hold)
        _development_lane_quota_charge(
            "DevelopmentLaneStatusResult.counters", answer["counters"]
        )
        _integer(
            "DevelopmentLaneStatusResult.tenant_active_count",
            answer["tenant_active_count"],
        )
        _integer(
            "DevelopmentLaneStatusResult.tenant_retained_disk_bytes",
            answer["tenant_retained_disk_bytes"],
        )
        _boolean("DevelopmentLaneStatusResult.tombstone", answer["tombstone"])
        return answer

    async def update_quota(self, request: dict[str, Any]) -> dict[str, Any]:
        """Publish a controller/admin-only monotonic quota-policy update."""

        value = _exact_mapping(
            "DevelopmentLaneQuotaUpdateRequest",
            request,
            frozenset(
                {
                    "schema_version",
                    "tenant_ref",
                    "policy",
                    "expected_policy_revision",
                    "expected_policy_version",
                    "idempotency_key",
                    "now_ms",
                }
            ),
        )
        if value["schema_version"] != "1":
            raise ValueError(
                "DevelopmentLaneQuotaUpdateRequest.schema_version must be 1"
            )
        _string("DevelopmentLaneQuotaUpdateRequest.tenant_ref", value["tenant_ref"])
        _string(
            "DevelopmentLaneQuotaUpdateRequest.idempotency_key",
            value["idempotency_key"],
        )
        _development_lane_quota_policy(
            "DevelopmentLaneQuotaUpdateRequest.policy", value["policy"]
        )
        _integer(
            "DevelopmentLaneQuotaUpdateRequest.expected_policy_revision",
            value["expected_policy_revision"],
        )
        if value["expected_policy_version"] is not None:
            _string(
                "DevelopmentLaneQuotaUpdateRequest.expected_policy_version",
                value["expected_policy_version"],
            )
        _integer("DevelopmentLaneQuotaUpdateRequest.now_ms", value["now_ms"])
        result = await self._client._send(
            "UpdateDevelopmentLaneQuota", {"request": value}
        )
        answer = _exact_mapping(
            "DevelopmentLaneQuotaUpdateResult",
            result,
            frozenset(
                {
                    "schema_version",
                    "decision",
                    "policy",
                    "counters",
                    "policy_revision",
                }
            ),
        )
        if answer["schema_version"] != "1":
            raise ValueError(
                "DevelopmentLaneQuotaUpdateResult.schema_version must be 1"
            )
        if answer["decision"] not in _DEVELOPMENT_LANE_QUOTA_DECISIONS:
            raise ValueError("DevelopmentLaneQuotaUpdateResult.decision is invalid")
        if answer["policy"] is not None:
            _development_lane_quota_policy(
                "DevelopmentLaneQuotaUpdateResult.policy", answer["policy"]
            )
        _development_lane_quota_charge(
            "DevelopmentLaneQuotaUpdateResult.counters", answer["counters"]
        )
        _integer(
            "DevelopmentLaneQuotaUpdateResult.policy_revision",
            answer["policy_revision"],
        )
        return answer


class ChangeEnvelopeClient:
    """Governed engine-native external-change namespace.

    The namespace emits the exact ordered wire contract the Rust signer verifies;
    verified tenant/principal/policy/request fields are bound by the client at send
    time and cannot be supplied as persisted caller identity.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    @staticmethod
    def _position(value: dict[str, Any]) -> dict[str, Any]:
        kind = value["kind"]
        position = value["value"]
        if kind == "opaque":
            position = {
                "cursor_type": position.get(
                    "cursor_type", position.get("version_type")
                ),
                "value": position["value"],
            }
            # Content versions name the opaque discriminator differently from
            # source cursors. Preserve the caller's declared wire field while
            # still fixing its order for the verified v2 signature.
            if "version_type" in value["value"]:
                position = {
                    "version_type": value["value"]["version_type"],
                    "value": value["value"]["value"],
                }
        return {"kind": kind, "value": position}

    @classmethod
    def _canonical(cls, source: dict[str, Any]) -> dict[str, Any]:
        envelope = copy.deepcopy(source)
        mutation_in = _closed_mapping(
            "mutation",
            envelope["mutation"],
            frozenset(
                {
                    "schema_version",
                    "batch_id",
                    "context",
                    "tenant",
                    "graph",
                    "placement_epoch",
                    "idempotency_key",
                    "operations",
                    "outbox",
                    "created_at_ms",
                }
            ),
            frozenset(
                {"expected_graph_version", "fencing_token", "authoritative_state"}
            ),
        )
        if _integer("mutation.schema_version", mutation_in["schema_version"]) != 2:
            raise ValueError("mutation.schema_version must be 2")
        context_in = _closed_mapping(
            "mutation.context",
            mutation_in["context"],
            frozenset({"request_id", "principal"}),
            frozenset({"purpose", "policy_fingerprint", "trace_id"}),
        )
        context: dict[str, Any] = {
            "request_id": int(context_in["request_id"]),
            "principal": str(context_in["principal"]),
        }
        for key in ("purpose", "policy_fingerprint", "trace_id"):
            if context_in.get(key) is not None:
                context[key] = context_in[key]

        operations: list[dict[str, Any]] = []
        for index, operation_value in enumerate(mutation_in["operations"]):
            operation_in = _exact_mapping(
                f"mutation.operations[{index}]",
                operation_value,
                frozenset({"ordinal", "surface", "domain", "method"}),
            )
            method_in = _closed_mapping(
                f"mutation.operations[{index}].method",
                operation_in["method"],
                frozenset({"method"}),
                frozenset({"params"}),
            )
            method_name = method_in["method"]
            method: dict[str, Any] = {"method": method_name}
            if "params" in method_in:
                method["params"] = _canonicalize_method_value(
                    method_in["params"], method=method_name
                )
            operations.append(
                {
                    "ordinal": int(operation_in["ordinal"]),
                    "surface": operation_in["surface"],
                    "domain": operation_in["domain"],
                    "method": method,
                }
            )

        mutation: dict[str, Any] = {
            "schema_version": 2,
            "batch_id": mutation_in["batch_id"],
            "context": context,
            "tenant": mutation_in["tenant"],
            "graph": mutation_in["graph"],
            "placement_epoch": int(mutation_in["placement_epoch"]),
            "idempotency_key": mutation_in["idempotency_key"],
        }
        for key in ("expected_graph_version", "fencing_token"):
            if mutation_in.get(key) is not None:
                mutation[key] = int(mutation_in[key])
        if mutation_in.get("authoritative_state") is not None:
            state = _exact_mapping(
                "mutation.authoritative_state",
                mutation_in["authoritative_state"],
                frozenset(
                    {
                        "algorithm",
                        "digest",
                        "source_graph_version",
                        "target_graph_version",
                    }
                ),
            )
            mutation["authoritative_state"] = {
                "algorithm": state["algorithm"],
                "digest": state["digest"],
                "source_graph_version": int(state["source_graph_version"]),
                "target_graph_version": int(state["target_graph_version"]),
            }
        mutation["operations"] = operations
        outbox: list[dict[str, Any]] = []
        for index, intent_value in enumerate(mutation_in["outbox"]):
            intent = _exact_mapping(
                f"mutation.outbox[{index}]",
                intent_value,
                frozenset({"topic", "key", "payload", "headers"}),
            )
            row: dict[str, Any] = {
                "topic": intent["topic"],
                "key": intent["key"],
                "payload": bytes(intent["payload"]),
                "headers": {
                    key: intent["headers"][key] for key in sorted(intent["headers"])
                },
            }
            outbox.append(row)
        mutation["outbox"] = outbox
        mutation["created_at_ms"] = int(mutation_in["created_at_ms"])

        version_in = envelope["content_version"]
        version: dict[str, Any] = {
            "object_id": version_in["object_id"],
            "digest_algorithm": version_in.get("digest_algorithm", "sha256"),
            "digest": version_in["digest"],
        }
        if version_in.get("previous_digest") is not None:
            version["previous_digest"] = version_in["previous_digest"]
        version["source_version"] = cls._position(version_in["source_version"])

        result: dict[str, Any] = {
            "schema_version": int(envelope["schema_version"]),
            "envelope_id": envelope["envelope_id"],
            "mutation": mutation,
            "content_version": version,
        }
        if envelope.get("cursor") is not None:
            cursor_in = envelope["cursor"]
            cursor: dict[str, Any] = {
                "source": cursor_in["source"],
                "partition": cursor_in.get("partition", ""),
                "position": cls._position(cursor_in["position"]),
            }
            if cursor_in.get("expected_previous") is not None:
                cursor["expected_previous"] = cls._position(
                    cursor_in["expected_previous"]
                )
            result["cursor"] = cursor

        result["blobs"] = [
            {
                "blob_id": row["blob_id"],
                "operation": row["operation"],
                "digest_algorithm": row.get("digest_algorithm", "sha256"),
                "digest": row["digest"],
                "media_type": row["media_type"],
                "length": int(row["length"]),
            }
            for row in envelope.get("blobs", [])
        ]
        result["features"] = [
            {
                "feature_id": row["feature_id"],
                "operation": row["operation"],
                "object_id": row["object_id"],
                "kind": row["kind"],
                "value_msgpack": bytes(row["value_msgpack"]),
                "model_version": row["model_version"],
            }
            for row in envelope.get("features", [])
        ]
        result["evidence"] = [
            {
                "evidence_id": row["evidence_id"],
                "operation": row["operation"],
                "object_id": row["object_id"],
                "modality": row["modality"],
                "locus_msgpack": bytes(row["locus_msgpack"]),
                "content_digest": row["content_digest"],
            }
            for row in envelope.get("evidence", [])
        ]
        result["policies"] = [
            {
                "policy_id": row["policy_id"],
                "operation": row["operation"],
                "object_id": row["object_id"],
                "tenant": row.get("tenant", ""),
                "classification": row["classification"],
                "policy_version": row["policy_version"],
                "subject_set_digest": row["subject_set_digest"],
                "retention_policy": row.get("retention_policy", ""),
                "legal_hold": bool(row.get("legal_hold", False)),
            }
            for row in envelope.get("policies", [])
        ]
        result["lineage"] = [
            {
                "lineage_id": row["lineage_id"],
                "operation": row["operation"],
                "object_id": row["object_id"],
                "source_artifact_digest": row["source_artifact_digest"],
                "transform_name": row["transform_name"],
                "transform_version": row["transform_version"],
                "parent_content_digests": list(row.get("parent_content_digests", [])),
            }
            for row in envelope.get("lineage", [])
        ]
        privacy = envelope["privacy"]
        result["privacy"] = {
            "policy_version": privacy["policy_version"],
            "sanitizer_version": privacy["sanitizer_version"],
            "sanitized_payload_digest": privacy["sanitized_payload_digest"],
        }
        return result

    async def apply(self, envelope: dict[str, Any]) -> dict[str, Any]:
        canonical = self._canonical(envelope)
        mutation = canonical["mutation"]
        return await self._client._send(
            "ApplyChangeEnvelope",
            {"envelope": canonical},
            graph=mutation["graph"],
            idempotency_key=mutation["idempotency_key"],
        )

    async def apply_batch(
        self, envelopes: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Commit a batch of envelopes that all target ONE graph in a single
        ``ApplyChangeEnvelopes`` round-trip (CONCEPT:EG-KG.ingest.batched-change-envelopes).

        The engine lands the whole group in ONE coalesced redb transaction and returns
        one result per envelope, in the SAME order as ``envelopes``. Each result carries
        a ``status`` of ``applied`` / ``idempotent_skip`` / ``conflict`` (the same
        outcome vocabulary the single :meth:`apply` produces), so a caller can advance a
        watermark through the contiguous success prefix.

        A batch idempotency PRE-read (``get_many``) is deliberately omitted: the engine
        checks each envelope's idempotency key inside the commit transaction and reports
        a replay as ``idempotent_skip``, so a client-side pre-read would only add a
        round-trip without changing the outcome — defeating the point of batching.

        Every envelope must target the same graph (the connector's KG); a mixed-graph
        call raises. The engine itself supports mixed-graph batches (per-graph
        sub-transactions), but a single connector page is single-graph by construction.
        """
        if not envelopes:
            return []
        canonicals = [self._canonical(envelope) for envelope in envelopes]
        graph = canonicals[0]["mutation"]["graph"]
        for canonical in canonicals:
            if canonical["mutation"]["graph"] != graph:
                raise ValueError(
                    "changes.apply_batch requires every envelope to target one graph"
                )
        # Deterministic transport idempotency key over the batch's per-envelope keys;
        # per-envelope idempotency is enforced authoritatively server-side.
        material = "\0".join(
            sorted(canonical["mutation"]["idempotency_key"] for canonical in canonicals)
        ).encode("utf-8")
        batch_key = "change-batch:sha256:" + hashlib.sha256(material).hexdigest()
        result = await self._client._send(
            "ApplyChangeEnvelopes",
            {"envelopes": canonicals},
            graph=graph,
            idempotency_key=batch_key,
        )
        results = result.get("results") if isinstance(result, dict) else None
        return results if isinstance(results, list) else []

    async def get(self, envelope_id: str) -> dict[str, Any] | None:
        tenant = self._client._verified_tenant()
        return await self._client._send(
            "GetChangeEnvelope", {"envelope_id": envelope_id, "tenant": tenant}
        )

    async def content_version(self, object_id: str) -> dict[str, Any] | None:
        tenant = self._client._verified_tenant()
        return await self._client._send(
            "GetContentVersion", {"object_id": object_id, "tenant": tenant}
        )

    async def cursor(self, source: str, partition: str = "") -> dict[str, Any] | None:
        tenant = self._client._verified_tenant()
        return await self._client._send(
            "GetChangeCursor",
            {"source": source, "partition": partition, "tenant": tenant},
        )


class EdgeClient:
    """CONCEPT:AU-KG.query.object-graph-mapper — Topology Edge Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add(
        self, source_id: str, target_id: str, properties: dict[str, Any] | None = None
    ) -> None:
        await self._client._send(
            "AddEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "properties_msgpack": _pack_binary_msgpack(properties or {}),
            },
        )

    async def remove(self, source_id: str, target_id: str) -> None:
        await self._client._send(
            "RemoveEdge", {"source_id": source_id, "target_id": target_id}
        )

    async def invalidate(
        self,
        source_id: str,
        target_id: str,
        relationship: str,
        invalid_at: int,
        tx_now: int,
    ) -> int:
        """Non-destructively close a contradicted edge's temporal windows (KG-2.251).

        Sets the matching edge's ``valid_until = invalid_at`` and ``tx_to = tx_now``
        instead of deleting it, so an ``AS OF`` before ``invalid_at`` still sees the
        fact. Returns the number of edge blobs updated.
        """
        return await self._client._send(
            "InvalidateEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "relationship": relationship,
                "invalid_at": int(invalid_at),
                "tx_now": int(tx_now),
            },
        )

    async def supersede(
        self,
        source_id: str,
        target_id: str,
        prior_source: str,
        prior_target: str,
        prior_relationship: str,
        valid_at: int,
        tx_now: int,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Atomically supersede a prior edge with a new one (KG-2.251).

        Closes the prior edge's validity window AND inserts the new edge under one
        write guard — non-destructive, so the prior edge survives for history. The
        new edge's ``properties`` should carry ``valid_from = valid_at`` and a
        ``supersedes`` provenance pointer.
        """
        await self._client._send(
            "SupersedeEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "properties_msgpack": _pack_binary_msgpack(properties or {}),
                "prior_source": prior_source,
                "prior_target": prior_target,
                "prior_relationship": prior_relationship,
                "valid_at": int(valid_at),
                "tx_now": int(tx_now),
            },
        )

    async def has(self, source_id: str, target_id: str) -> bool:
        return await self._client._send(
            "HasEdge", {"source_id": source_id, "target_id": target_id}
        )

    async def list(self) -> builtins.list[tuple[str, str, builtins.list[int] | bytes]]:
        """Dump EVERY edge in the graph (unbounded full-graph read).

        On a large graph this is refused by the engine's overload backstop
        (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation): if the graph has more than
        ``EPISTEMIC_GRAPH_MAX_RESPONSE_EDGES`` edges (default 50_000), this raises
        :class:`ResultTooLargeError` instead of materializing a gigabyte-scale
        frame that would reset the connection. Use :meth:`list_page` (bounded
        by ``limit``) to paginate for large graphs.
        """
        return await self._client._send("GetEdges")

    async def list_page(
        self,
        limit: int = 0,
        *,
        after: tuple[str, str, int] | None = None,
    ) -> builtins.list[tuple[str, str, int, builtins.list[int] | bytes]]:
        """Return one deterministic keyset page of edges.

        Each row is ``(source, target, ordinal, properties)``, ordered by
        ``(source, target, ordinal)`` — ``ordinal`` distinguishes multiple
        parallel edges stored under the same ``(source, target)`` pair.
        ``after`` is an exclusive ``(source, target, ordinal)`` cursor; advance
        it to the last row returned in each non-empty page. ``limit=0`` is
        uncapped and intended only for small graphs.
        """
        return await self._client._send(
            "GetEdgesPage",
            {
                "after": list(after) if after is not None else None,
                "limit": int(limit),
            },
        )

    async def properties(self, source_id: str, target_id: str) -> dict[str, Any] | None:
        raw_val = await self._client._send(
            "GetEdgeProperties", {"source_id": source_id, "target_id": target_id}
        )
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

    async def properties_batch(
        self, edges: builtins.list[tuple[str, str]]
    ) -> builtins.list[builtins.list[dict[str, Any]]]:
        """Fetch properties for many edges in ONE round-trip (CONCEPT:EG-KG.memory.forgetting-curve-decay).

        Returns a list parallel to ``edges``; each element is the list of property
        dicts for that ``(source, target)`` pair (a pair may carry multiple edges;
        an empty list means no such edge).
        """
        pairs = [list(e) for e in edges]
        rows = await self._client._send("GetEdgePropertiesBatch", {"edges": pairs})
        out: builtins.list[builtins.list[dict[str, Any]]] = []
        for per_edge in rows or []:
            out.append(
                [
                    msgpack.unpackb(blob, raw=False)
                    for blob in per_edge
                    if blob is not None
                ]
            )
        return out

    async def count(self) -> int:
        return await self._client._send("EdgeCount")


class GraphOperationsClient:
    """CONCEPT:AU-KG.research.research-pipeline-runner — Graph Algorithms Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def clear(self) -> None:
        await self._client._send("ClearGraph")

    async def parse_file(self, file_path: str, source: bytes) -> dict[str, Any]:
        return await self._client._send(
            "ParseFile",
            {"file_path": _logical_source_name(file_path), "source": source},
        )

    async def parse_files(self, files: list[tuple[str, bytes]]) -> list[dict[str, Any]]:
        """Parse many files in ONE round-trip (CONCEPT:EG-KG.memory.forgetting-curve-decay batch op).

        ``files`` is a list of ``(file_path, source_bytes)``. Returns one parse
        result per input file, **in input order**, each with the same shape as
        :meth:`parse_file`. The payload mirrors the ``BatchUpdate`` convention: a
        single MessagePack blob (``Vec<(String, bytes)>`` engine-side).
        """
        blob = msgpack.packb(
            [[_logical_source_name(fp), src] for fp, src in files],
            use_bin_type=True,
        )
        return await self._client._send("ParseFiles", {"files_msgpack": blob})

    async def index_repository(self, files: list[tuple[str, bytes]]) -> dict[str, Any]:
        """Parse a batch AND resolve cross-file edges in ONE round-trip
        (CONCEPT:EG-KG.compute.turn-each-project).

        ``files`` is a list of ``(file_path, source_bytes)`` — the SAME blob as
        :meth:`parse_files`, but the batch is treated as one resolution scope (a
        repository, or a delta set). Unlike :meth:`parse_files` (one raw result
        per file), this returns a SINGLE merged ``IndexResult`` dict::

            {"nodes": [...], "edges": [...],          # IMPLEMENTS + resolved
             "symbols_extracted": int, "files_parsed": int,
             "calls_resolved": int, "calls_unresolved": int,
             "imports_resolved": int, "imports_unresolved": int}

        ``edges`` carry resolved ``calls`` (symbol→symbol) and ``depends_on``
        (file→file) edge types pointing at real node ids — the cross-file step
        feature clustering / impact analysis run over. Use this to ingest a
        repository's symbol graph; use :meth:`parse_files` only when per-file raw
        results are wanted.
        """
        blob = msgpack.packb(
            [[_logical_source_name(fp), src] for fp, src in files],
            use_bin_type=True,
        )
        return await self._client._send("IndexRepository", {"files_msgpack": blob})

    async def observe_screen(
        self,
        png: bytes,
        *,
        session_id: str,
        frame_seq: int = 0,
        prev_frame_id: str = "",
        prev_hash: int = 0,
        elements: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Turn a captured desktop frame into durable graph entities in ONE round-trip
        (CONCEPT:AU-KG.ontology.owl-screen-bridge).

        ``png`` is the screenshot bytes (only its dimensions + content hash are kept,
        for frame-diff — the image itself is not persisted). ``elements`` is the AT-SPI
        accessibility tree (``[{role,name,x,y,w,h}, ...]``). Returns a single
        ``ScreenObservationResult``::

            {"nodes": [...], "edges": [...],   # session + frame + UIElement nodes,
             "frame_id": str, "width": int, "height": int,
             "hash": int, "changed": bool, "element_count": int}

        ``edges`` carry ``hasObservation`` (session→frame), ``hasElement``
        (frame→element) and ``succeededBy`` (prev→frame, only when the frame changed).
        Pass the returned ``hash``/``frame_id`` back as ``prev_hash``/``prev_frame_id``
        on the next call to chain the frames.
        """
        blob = _pack_binary_msgpack(
            {
                "session_id": session_id,
                "frame_seq": frame_seq,
                "prev_frame_id": prev_frame_id,
                "prev_hash": prev_hash,
                "png": png,
                "elements": elements or [],
            }
        )
        return await self._client._send("ObserveScreen", {"obs_msgpack": blob})

    async def add_embedding(self, node_id: str, embedding: list[float]) -> None:
        await self._client._send(
            "AddEmbedding", {"node_id": node_id, "embedding": embedding}
        )

    async def semantic_search(
        self, query_embedding: list[float], n_results: int = 5
    ) -> list[tuple[str, float]]:
        return await self._client._send(
            "SemanticSearch",
            {"query_embedding": query_embedding, "n_results": n_results},
        )

    async def discover(
        self,
        keywords: list[str],
        query_embedding: list[float],
        k: int = 5,
    ) -> list[dict[str, Any]]:
        """One-round-trip hybrid discovery (CONCEPT:EG-KG.retrieval.one-round-trip-discovery).

        Ranks nodes by BOTH lexical keyword overlap (over ``name``/``description``/
        ``type``) AND semantic similarity to ``query_embedding``, returning the
        top-``k`` hydrated with their human-readable text::

            [{"id", "name", "description", "type", "score"}, ...]

        Complements :meth:`semantic_search` (which returns bare ``(id, score)``
        pairs): Discover folds the keyword signal into the ranking and hydrates the
        result text in a single call, so a router/orchestrator gets a ready-to-read
        shortlist with no N+1 metadata fetch. ``keywords`` is the caller's
        de-duplicated token set; ``query_embedding`` may be empty (embedder/vLLM
        unavailable), degrading to a bounded keyword-only scan. Gate on
        :meth:`supports` (``"Discover"``) against an engine built before this op.
        """
        return await self._client._send(
            "Discover",
            {"keywords": keywords, "query_embedding": query_embedding, "k": k},
        )

    async def match_ontology_terms(self, query: str) -> list[dict[str, Any]]:
        """CONCEPT:EG-ORCH.routing.lexical-capability-escalation — embedding-free lexical classification gate.

        Returns the capability-node terms (Tool/Skill/MCPServer names+synonyms)
        that appear as whole words in ``query``, each as
        ``{term, node_type, label, score}``. The "free" tier between structural
        routing and semantic search: a non-empty result means the turn names a
        real fleet capability and should escalate to the full graph.
        """
        return await self._client._send(
            "MatchOntologyTerms",
            {"query": query},
        )

    async def batch_l2_normalize(self, vectors: list[list[float]]) -> list[list[float]]:
        """L2-normalize a batch of vectors IN-ENGINE via the eg-numeric kernel
        (CONCEPT:EG-KG.compute.l2-normalize-batch-vectors, compute-near-data). Returns each row's unit vector `v/‖v‖`
        (a zero vector is returned unchanged). Requires the engine's `numeric` feature."""
        return await self._client._send("BatchL2Normalize", {"vectors": vectors})

    async def vf2_subgraph_match(
        self,
        pattern: EpistemicGraphClient,
        *,
        max_results: int = 0,
        max_steps: int = 0,
    ) -> dict[str, Any]:
        """VF2 subgraph isomorphism match of ``pattern`` against this graph.

        VF2 subgraph isomorphism is NP-hard with no bound otherwise, so the
        engine's backtracking search stops early once it collects
        ``max_results`` matches or spends ``max_steps`` candidate-pair
        attempts (whichever first); ``0`` for either uses the engine's
        conservative built-in default. Returns
        ``{"matches": [{pattern_node_id: host_node_id, ...}, ...], "truncated": bool}``
        — ``truncated`` is ``True`` when the search stopped early (a PARTIAL
        result, not proof no further match exists); pass an explicit
        ``max_results``/``max_steps`` to see more.
        """
        return await self._client._send(
            "Vf2SubgraphMatch",
            {
                "pattern_graph_name": pattern._graph_name,
                "max_results": int(max_results),
                "max_steps": int(max_steps),
            },
        )

    async def topological_sort(self) -> list[str]:
        return await self._client._send("TopologicalSort")

    async def find_cycle(self) -> list[str] | None:
        return await self._client._send("FindCycle")

    async def shortest_path(self, source_id: str, target_id: str) -> list[str] | None:
        return await self._client._send(
            "GetShortestPath", {"source_id": source_id, "target_id": target_id}
        )

    async def blast_radius(self, node_id: str, max_depth: int) -> list[str]:
        return await self._client._send(
            "GetBlastRadius", {"node_id": node_id, "max_depth": max_depth}
        )

    async def get_subgraph(self, node_ids: list[str]) -> dict[str, Any]:
        """Batch-fetch the induced subgraph in ONE round-trip.

        Returns ``{"nodes": [{"id", "properties"}, ...], "edges":
        [{"source", "target", "properties"}, ...]}`` with properties already
        decoded server-side. Replaces N per-node ``GetNodeProperties`` calls plus
        a full ``GetEdges`` scan — ship the node-id set, get everything back once.
        """
        return await self._client._send("GetSubgraph", {"node_ids": node_ids})

    async def connected_components(self) -> list[list[str]]:
        return await self._client._send("ConnectedComponents")

    async def strongly_connected_components(self) -> list[list[str]]:
        """CONCEPT:EG-KG.memory.forgetting-curve-decay — Tarjan's SCC via Tokio service."""
        return await self._client._send("StronglyConnectedComponents")

    async def minimum_spanning_tree(self) -> list[tuple[str, str, float]]:
        """CONCEPT:EG-KG.memory.forgetting-curve-decay — Kruskal's MST via Tokio service."""
        return await self._client._send("MinimumSpanningTree")

    async def community_detection(self, resolution: float = 1.0) -> list[list[str]]:
        return await self._client._send(
            "CommunityDetection", {"resolution": resolution}
        )

    async def community_detect_ephemeral(
        self,
        node_ids: list[str],
        edges: list[tuple[str, str]],
        resolution: float = 1.0,
    ) -> list[list[str]]:
        """Stateless community detection over an inline call graph (Phase: holistic).

        Runs detection on the passed nodes/edges WITHOUT loading them into a tenant
        — no bulk-load round-trip, no throwaway tenant, no persistence. Replaces the
        load-tenant-then-detect pattern for the ingest community pass.
        """
        return await self._client._send(
            "CommunityDetectEphemeral",
            {"node_ids": node_ids, "edges": edges, "resolution": resolution},
        )

    async def graph_coloring(self) -> list[tuple[str, int]]:
        return await self._client._send("GraphColoring")

    async def compute_similarity_edges(self, threshold: float) -> int:
        return await self._client._send(
            "ComputeSimilarityEdges", {"threshold": threshold}
        )

    async def resolve_candidates(
        self,
        sim_threshold: float = 0.8,
        merge_threshold: float = 0.92,
        node_type: str | None = None,
    ) -> list[dict]:
        """Native entity-resolution candidate generation (CONCEPT:AU-KG.compute.when-exposes-native).

        Composes embedding similarity + clustering server-side into ONE read op and
        returns merge proposals — each ``{canonical, members, score, kind}`` where
        ``kind`` is ``"same_as"`` (mergeable duplicates) or ``"extends"`` (a
        subtype/version link). READ/propose only: nothing is mutated; apply accepted
        proposals via ``batch_update``. This is the escalation tier the
        agent-utilities dedup ladder routes its residual through instead of an
        O(N²) client-side embedding pass.
        """
        return await self._client._send(
            "ResolveCandidates",
            {
                "sim_threshold": sim_threshold,
                "merge_threshold": merge_threshold,
                "node_type": node_type,
            },
        )


class AnalyticsClient:
    """CONCEPT:AU-KG.research.research-pipeline-runner — Analytics and Centrality Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def degree_centrality(self, node_id: str) -> float:
        return await self._client._send("DegreeCentrality", {"node_id": node_id})

    async def degree_centrality_all(self) -> list[tuple[str, float]]:
        return await self._client._send("DegreeCentralityAll")

    async def betweenness_centrality(self) -> list[tuple[str, float]]:
        return await self._client._send("BetweennessCentrality")

    async def pagerank(
        self, damping: float = 0.85, iterations: int = 100
    ) -> list[tuple[str, float]]:
        return await self._client._send(
            "PageRank", {"damping": damping, "iterations": iterations}
        )

    async def personalized_pagerank(
        self,
        seed_nodes: list[tuple[str, float]],
        damping: float = 0.85,
        iterations: int = 100,
    ) -> list[tuple[str, float]]:
        return await self._client._send(
            "PersonalizedPageRank",
            {"seed_nodes": seed_nodes, "damping": damping, "iterations": iterations},
        )


class LifecycleClient:
    """CONCEPT:AU-KG.research.research-pipeline-runner — Lifecycle and State Management Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def prune(self, max_age_secs: int, min_score: float) -> int:
        return await self._client._send(
            "PruneByLifecycle", {"max_age_secs": max_age_secs, "min_score": min_score}
        )

    async def get_context_view(self, agent_id: str, max_tokens: int = 4096) -> str:
        return await self._client._send(
            "GetContextView", {"agent_id": agent_id, "max_tokens": max_tokens}
        )

    async def batch_update(self, operations: list[dict[str, Any]]) -> Any:
        """Atomically apply validated graph/vector operations to the bound graph.

        Supported shapes use ``id`` for nodes/vectors and ``source``/``target``
        for edges: ``add_node``, ``upsert_node``, ``remove_node``, ``add_edge``,
        ``upsert_edge``, ``remove_edge``, and ``add_embedding``. Node/edge
        ``properties`` must be a mapping; ``embedding`` must be a non-empty finite
        number list. ``upsert_edge`` replaces every parallel edge for that ordered
        pair with one row. Removing a node also removes all incident edges and its
        embedding.

        The engine validates the complete list before changing RAM and commits
        graph rows plus semantic-vector changes in one durable transaction. An
        unknown or malformed operation fails the whole batch; no partial-success
        rows are retained.
        """
        return await self._client._send(
            "BatchUpdate", {"operations_msgpack": _pack_binary_msgpack(operations)}
        )

    async def multi_graph_batch_update(
        self, batches: dict[str, list[dict[str, Any]]]
    ) -> dict[str, Any]:
        """Batched CROSS-GRAPH write in ONE round-trip (CONCEPT:EG-KG.storage.multi-graph-batch-write).

        ``batches`` maps ``graph_name → operations`` where each ``operations`` list
        is exactly a :meth:`batch_update` op list. The server applies each graph's
        sub-batch through the normal per-graph write path CONCURRENTLY, so N
        distinct graphs commit across N of the K redb shard writers in parallel —
        instead of the caller serializing N round-trips that each re-acquire one
        write lock. Reuses the existing ``BatchUpdate`` primitive server-side.

        Returns ``{"results": {graph: <batch_result>}, "errors": {graph: msg}}``;
        one graph's failure never aborts the others (partial-success). Encodes as
        ``Vec<(graph_name, operations_msgpack)>`` so the ordering is deterministic.
        """
        encoded = [
            (str(graph), _pack_binary_msgpack(list(ops)))
            for graph, ops in batches.items()
        ]
        return await self._client._send(
            "MultiGraphBatchUpdate",
            {"batches_msgpack": _pack_binary_msgpack(encoded)},
        )

    async def metrics(self) -> dict[str, Any]:
        return await self._client._send("Metrics")

    async def to_msgpack(self) -> bytes:
        return await self._client._send("ToMsgpack")

    async def from_msgpack(self, msgpack_bytes: bytes) -> None:
        await self._client._send("FromMsgpack", {"msgpack": msgpack_bytes})

    async def evict_lru(self, max_nodes: int) -> int:
        """Evict oldest nodes to enforce max_nodes cap. Returns eviction count."""
        return await self._client._send("EvictLRU", {"max_nodes": max_nodes})

    async def decay_sweep(
        self,
        half_life_secs: float = 604_800.0,
        floor: float = 0.0,
        prune: bool = False,
    ) -> dict[str, Any]:
        """CONCEPT:EG-KG.memory.forgetting-curve-decay — Ebbinghaus forgetting-curve decay.

        Decays every node's and edge's belief ``confidence`` by
        ``R = 0.5 ** (Δt / half_life_secs)`` since its last access, persisting the
        result and advancing the access clock so repeated sweeps compound exactly.
        With ``prune=True`` (or a positive ``floor``), items whose decayed
        confidence falls below ``floor`` are removed. The server is the time
        authority. Returns ``{nodes_decayed, edges_decayed, nodes_pruned,
        edges_pruned}``.
        """
        return await self._client._send(
            "DecaySweep",
            {"half_life_secs": half_life_secs, "floor": floor, "prune": prune},
        )

    async def touch_nodes(self, node_ids: list[str]) -> int:
        """Refresh nodes on access (spaced repetition): reset the forgetting clock
        and restore ``confidence = 1.0``. Returns the number of nodes touched."""
        return await self._client._send("TouchNodes", {"node_ids": node_ids})


class ReasoningClient:
    """CONCEPT:EG-KG.compute.compiled-semantic-reasoner — Compiled Semantic Reasoner Namespace.

    Forward-chaining OWL/RDFS inference executed in the Rust engine. Materialises
    inferred edges and type annotations in-place and returns the inferred triples.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def reason(
        self,
        subclass_relations: list[tuple[str, str]] | None = None,
        subproperty_relations: list[tuple[str, str]] | None = None,
        symmetric_properties: list[str] | None = None,
        transitive_properties: list[str] | None = None,
        inverse_properties: list[tuple[str, str]] | None = None,
        domain_rules: list[tuple[str, str]] | None = None,
        range_rules: list[tuple[str, str]] | None = None,
        property_chains: list[tuple[str, str, str]] | None = None,
    ) -> dict[str, Any]:
        """Run one fixpoint of Datalog reasoning plus optional domain/range and
        property-chain inference over the current graph.

        Every rule set is optional; omitted sets are treated as empty. Returns
        ``{"inferred_count": int, "inferred_triples": [{subject, predicate,
        object, inference_type}, ...]}``. The inferred edges/types are also
        persisted into the graph as a side effect.
        """
        return await self._client._send(
            "RunDatalogReasoning",
            {
                "subclass_relations": [list(t) for t in (subclass_relations or [])],
                "subproperty_relations": [
                    list(t) for t in (subproperty_relations or [])
                ],
                "symmetric_properties": list(symmetric_properties or []),
                "transitive_properties": list(transitive_properties or []),
                "inverse_properties": [list(t) for t in (inverse_properties or [])],
                "domain_rules": [list(t) for t in (domain_rules or [])],
                "range_rules": [list(t) for t in (range_rules or [])],
                "property_chains": [list(t) for t in (property_chains or [])],
            },
        )


class LedgerClient:
    """CONCEPT:AU-KG.query.object-graph-mapper — Ledger Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def get(self) -> list[str]:
        """Return this graph's mutation ledger.

        BUG A1 (2026-08-12): the engine now answers with a typed
        ``{"populated": bool, "entries": [...], "watermark": int}`` result
        rather than a bare array, so a genuinely empty ledger (``populated:
        True``, ``entries: []``) can never again be silently confused with a
        ledger that could not be read for this request's scope. This raises
        ``LedgerNotPopulatedError`` in the latter case instead of returning
        ``[]`` — callers must be able to tell "nothing to sync" from "I read
        nothing" and fail loudly on the second.

        This drops ``watermark`` — see :meth:`get_with_watermark` for a
        caller that needs it. The engine's mutation ledger is an in-memory,
        CAPPED ring (not a durable change log): eviction, rehydration, a
        process restart, or exceeding the cap can silently drop entries
        while the underlying mutations stay durably committed server-side, so
        a caller that must distinguish "caught up" from "history was
        silently dropped" needs :meth:`get_with_watermark`, not this method.
        """
        entries, _watermark = await self.get_with_watermark()
        return entries

    async def get_with_watermark(self) -> tuple[list[str], int]:
        """Like :meth:`get`, but also returns the ledger's current watermark.

        BUG A1 follow-up (2026-08-12): the engine's mutation ledger is a
        purely IN-MEMORY, capped ring (drop-oldest-half past 100k entries),
        NOT a durable change log — cold-tenant idle offload/hibernate,
        eviction + lazy rehydrate, a process restart, or exceeding the cap
        can all silently drop entries while the underlying mutations remain
        fully durable server-side (in redb). ``watermark`` is the 0-based
        sequence of the OLDEST entry ``entries`` can vouch for; a caller
        that tracks it across reads (e.g. a periodic flush like
        ``agent_utilities.workflows.epistemic_sync.flush_ledger_to_backend``)
        can detect it advancing (or resetting after a restart) as PROOF that
        history was silently dropped, and fail loudly instead of treating a
        short/empty read as "nothing new to sync".
        """
        result = await self._client._send("GetLedger")
        if not isinstance(result, dict) or not result.get("populated", False):
            raise LedgerNotPopulatedError(
                "GetLedger: the ledger could not be read for this request's "
                "scope (this is NOT the same as a genuinely empty ledger) — "
                f"raw result: {result!r}"
            )
        return list(result.get("entries", [])), int(result.get("watermark", 0))

    async def clear(self) -> None:
        await self._client._send("ClearLedger")

    async def apply(self, transactions: list[str]) -> None:
        await self._client._send("ApplyLedger", {"transactions": transactions})


class ChannelsClient:
    """CONCEPT:AU-KG.query.object-graph-mapper — Dynamic Communication Channels Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def create(
        self,
        channel_id: str,
        channel_type: str = "Group",
        creator: str = "",
        initial_members: list[str] | None = None,
    ) -> None:
        await self._client._send(
            "CreateChannel",
            {
                "channel_id": channel_id,
                "channel_type": channel_type,
                "creator": creator,
                "initial_members": initial_members or [],
            },
        )

    async def join(self, channel_id: str, agent_id: str) -> None:
        await self._client._send(
            "JoinChannel", {"channel_id": channel_id, "agent_id": agent_id}
        )

    async def leave(self, channel_id: str, agent_id: str) -> Any:
        return await self._client._send(
            "LeaveChannel", {"channel_id": channel_id, "agent_id": agent_id}
        )

    async def close(
        self,
        channel_id: str,
        summary_embedding: list[float] | None = None,
        topic_metadata: str | None = None,
    ) -> Any:
        return await self._client._send(
            "CloseChannel",
            {
                "channel_id": channel_id,
                "summary_embedding": summary_embedding,
                "topic_metadata": topic_metadata,
            },
        )

    async def send_message(self, channel_id: str, sender: str, payload: str) -> None:
        await self._client._send(
            "SendMessage",
            {"channel_id": channel_id, "sender": sender, "payload": payload},
        )

    async def get_messages(
        self, channel_id: str, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await self._client._send(
            "GetChannelMessages", {"channel_id": channel_id, "limit": limit}
        )

    async def list(self) -> builtins.list[dict[str, Any]]:
        return await self._client._send("ListChannels")

    async def get_members(self, channel_id: str) -> builtins.list[str]:
        return await self._client._send("GetChannelMembers", {"channel_id": channel_id})


#: The engine's closed `GraphType` wire enum (`crates/eg-types/src/protocol.rs`
#: `pub enum GraphType { Agent, Team, Global, Commons }`). Pinned against that
#: exact set by `tests/test_create_graph_type_allowlist.py`.
#:
#: U-96/U-98: a semantic content label (e.g. `"Ontology"`) is NOT a member of
#: this closed enum. Before this allowlist, an unsupported value was sent over
#: the wire, failed to deserialize server-side, and the server's decode-failure
#: path answered under a synthetic correlation id `0` -- which this client's
#: `_pending` map never has a future for, so the caller silently timed out and
#: retried instead of failing fast with a clear error. Validating client-side,
#: before `_send`, turns that multi-minute timeout into an immediate,
#: unambiguous `ValueError` and guarantees no request is ever transmitted for
#: an unsupported type.
VALID_GRAPH_TYPES = frozenset({"Agent", "Team", "Global", "Commons"})


class MultiTenantClient:
    """CONCEPT:AU-KG.research.research-pipeline-runner — Multi-Tenant Management Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def create(self, graph_name: str, graph_type: str = "Agent") -> None:
        if graph_type not in VALID_GRAPH_TYPES:
            raise ValueError(
                f"unsupported graph_type {graph_type!r}; the engine's closed "
                f"GraphType wire enum only accepts one of "
                f"{sorted(VALID_GRAPH_TYPES)} (U-96: a semantic content label "
                "like 'Ontology' is not a lifecycle/isolation graph category "
                "-- keep ontology semantics in governed graph contents instead)"
            )
        await self._client._send(
            "CreateGraph", {"graph_name": graph_name, "graph_type": graph_type}
        )

    async def delete(self, graph_name: str) -> None:
        await self._client._send("DeleteGraph", {"graph_name": graph_name})

    async def list(self) -> list[dict[str, str]]:
        return await self._client._send("ListGraphs")


class ReshardingClient:
    """CONCEPT:EG-KG.sharding.resharding-admin-api — M3 catalog-driven resharding admin namespace.

    Drives, over the wire, the M3 ops the engine has building blocks for: online
    single-node resharding (EG-032), the durable tenant catalog (EG-031), and the
    rebalancing planner (EG-035) + its execution (EG-039). The mandatory main build
    includes the durable redb engine these operations require.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def reshard(self, graph: str, to_shard: int) -> dict[str, Any]:
        """Online-move ``graph``'s durable rows to ``to_shard`` while the engine runs,
        then flip the catalog route (EG-032). Returns a reshard report (counts +
        ``delta_nodes``/``delta_edges`` = the rows copied under the brief write-pause)."""
        return await self._client._send(
            "Reshard", {"graph": graph, "to_shard": to_shard}
        )

    async def catalog_assign(
        self, graph: str, shard: int, node: int | None = None
    ) -> bool:
        """Populate / assign an explicit catalog placement for ``graph`` (EG-031). Flips
        the ROUTE only — to MOVE the rows too use :meth:`reshard`."""
        return await self._client._send(
            "CatalogAssign", {"graph": graph, "shard": shard, "node": node}
        )

    async def catalog_reassign(self, graph: str, shard: int) -> bool:
        """Re-place ``graph`` onto ``shard``, preserving its node placement (EG-031)."""
        return await self._client._send(
            "CatalogReassign", {"graph": graph, "shard": shard}
        )

    async def catalog_remove(self, graph: str) -> bool:
        """Drop ``graph``'s catalog row; the engine chooses its unplaced policy."""
        return await self._client._send("CatalogRemove", {"graph": graph})

    async def catalog_list(self) -> dict[str, Any]:
        """List every explicit catalog placement ``{graph, shard, node}`` (EG-031)."""
        return await self._client._send("CatalogList")

    async def rebalance_plan(
        self, tolerance: float | None = None, max_moves: int | None = None
    ) -> dict[str, Any]:
        """Compute (do NOT execute) a rebalance plan over live per-shard/per-graph load
        (EG-035). Returns ``{moves: [...], shards: [...]}``."""
        return await self._client._send(
            "RebalancePlan", {"tolerance": tolerance, "max_moves": max_moves}
        )

    async def rebalance_execute(
        self, tolerance: float | None = None, max_moves: int | None = None
    ) -> dict[str, Any]:
        """Compute a rebalance plan AND execute it move-by-move via online resharding
        (EG-039) — online, one graph at a time. Returns ``{executed: [report, ...]}``."""
        return await self._client._send(
            "RebalanceExecute", {"tolerance": tolerance, "max_moves": max_moves}
        )


class PlacementClient:
    """CONCEPT:EG-KG.sharding.placement-route-rpc / EG-KG.sharding.placement-catalog-admin-rpc —
    DIST-P2-4/DIST-P2-5 placement-catalog wire consumer + admin namespace.

    Exposes the engine's ``raft::placement::PlacementCatalog`` (DIST-P2-1's ONE
    placement authority for virtual partitions — durable, versioned, spans-multiple-
    groups routing with a fenced-cutover epoch) over the wire: :meth:`route` (a read,
    ``Method::PlacementRoute``) plus the admin mutation trio :meth:`assign` /
    :meth:`move` / :meth:`abort_move` (DIST-P2-5, ``Method::PlacementAdmin``'s
    ``assign``/``move``/``abort_move`` ops). Before the admin trio existed, the
    catalog's assign/split/merge/online-move machinery was reachable only from
    in-process Rust — even on a real multi-node Raft cluster there was no way for an
    external caller to trigger a placement decision or drive an online move. All four
    methods require a `raft`/`cluster`-feature engine build with `MultiRaft` running;
    a single-node build answers `route` with the authoritative unplaced policy and
    the three admin methods with a typed "not available" error.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def route(
        self, tenant: str, sub_key: str, client_epoch: int = 0
    ) -> dict[str, Any]:
        """Resolve ``(tenant, sub_key)``'s current placement.

        ``client_epoch`` is the caller's last-known routing epoch for this
        partition (``0`` if it never resolved one before). Every success is a
        complete engine-authored ``PlacementRoute``. Unplaced/single-node policy is
        still complete (`placed=False`, normally group/epoch 0); callers never
        hash. A durable placement must have a non-zero epoch and its numeric fence
        must match the returned group.

        ``answer["endpoints"]`` (ADR-1 / W1.1) is the resolved group's
        client-reachable member endpoints, LEADER FIRST — empty when no
        cluster topology is known yet (single-node, or no member has
        self-reported). ``pool.py``'s ``ShardRouter``/AU's
        ``placement_catalog.py`` consume it as the primary resolution source,
        ahead of the static ``GRAPH_RAFT_GROUP_ENDPOINTS`` override.
        """
        answer = await self._client._send(
            "PlacementRoute",
            {
                "request": {
                    "schema_version": "1",
                    "tenant_ref": tenant,
                    "partition_ref": sub_key,
                    "client_epoch": client_epoch,
                }
            },
        )
        answer = _exact_mapping(
            "PlacementRoute",
            answer,
            frozenset(
                {
                    "schema_version",
                    "route_id",
                    "tenant_ref",
                    "partition_ref",
                    "authoritative",
                    "placed",
                    "group",
                    "epoch",
                    "fencing_token",
                    "stale",
                    "leader_ref",
                    "endpoints",
                }
            ),
        )
        if answer["schema_version"] != "1" or answer["authoritative"] is not True:
            raise ValueError("engine returned a non-authoritative placement route")
        _string("PlacementRoute.route_id", answer["route_id"])
        if answer["tenant_ref"] != tenant or answer["partition_ref"] != sub_key:
            raise ValueError("engine returned a route for a different partition")
        group = _integer("PlacementRoute.group", answer["group"])
        epoch = _integer("PlacementRoute.epoch", answer["epoch"])
        fence = _integer("PlacementRoute.fencing_token", answer["fencing_token"])
        placed = _boolean("PlacementRoute.placed", answer["placed"])
        _boolean("PlacementRoute.stale", answer["stale"])
        if (
            not isinstance(placed, bool)
            or group < 0
            or epoch < 0
            or fence != group
            or (placed and epoch == 0)
        ):
            raise ValueError("engine returned an invalid placement fence")
        endpoints = answer["endpoints"]
        if not isinstance(endpoints, list) or not all(
            isinstance(e, str) and e for e in endpoints
        ):
            raise ValueError(
                "PlacementRoute.endpoints must be a list of non-empty strings"
            )
        return answer

    async def assign(self, tenant: str, group: int) -> int:
        """Assign the WHOLE keyspace of ``tenant`` to ``group`` (the placement
        DECISION leg, DIST-P2-5, ``Method::PlacementAdmin`` op ``assign``). Collapses
        any prior split. Raft/cluster-only. Returns the new routing epoch — every
        subsequent :meth:`route` call observes it immediately."""
        result = await self._client._send(
            "PlacementAdmin",
            {"op": {"operation": "assign", "tenant": tenant, "group": group}},
        )
        return _integer("PlacementAdmin.assign.epoch", result["epoch"])

    async def move(
        self, tenant: str, range_start: int, range_end: int, target: int
    ) -> dict[str, Any]:
        """Online-move ``tenant``'s partition ``[range_start, range_end]`` to
        ``target`` (DIST-P2-5, ``Method::PlacementAdmin`` op ``move``) — the full PLAN
        -> EXECUTE -> CATALOG-UPDATE leg: snapshot, per-graph durability-barrier
        catch-up, then a fenced cutover, reusing the engine's already-proven
        ``TenantManager::move_partition`` state machine (crash-safe via its durable
        move journal). Raft/cluster-only. Returns the engine's
        ``PlacementMoveReport``: ``{tenant, range: [start, end], target, epoch,
        graphs: [{graph, from_group, to_group, nodes_transferred}, ...]}``."""
        return await self._client._send(
            "PlacementAdmin",
            {
                "op": {
                    "operation": "move",
                    "tenant": tenant,
                    "range_start": range_start,
                    "range_end": range_end,
                    "target": target,
                }
            },
        )

    async def abort_move(self, move_id: str) -> bool:
        """Abort an in-flight online move identified by ``move_id`` before its
        cutover fence (DIST-P2-5, ``Method::PlacementAdmin`` op ``abort_move``). A
        move already past its epoch fence is rejected — recovery is roll-forward
        only. Raft/cluster-only."""
        return await self._client._send(
            "PlacementAdmin", {"op": {"operation": "abort_move", "move_id": move_id}}
        )


class ClusterTopologyClient:
    """CONCEPT:EG-KG.sharding.cluster-topology — ADR-1 / W1.1 engine-authoritative cluster
    discovery (``reports/wave1/ADR-scale-trio.md`` §ADR-1).

    Exposes ``Method::ClusterMembers`` — every known Raft group's members, each
    with its role (``leader``/``follower``/``learner``), health, immutable member
    identity, certificate-rotation metadata, and client-reachable endpoint,
    sourced from the engine's durable ``NodeInfoStore``.  The response is a
    bounded HMAC-signed snapshot bound to the verified tenant/principal/agent
    context and carries monotonic membership and placement epochs.  Answered
    from ANY reachable node, not just the leader (unlike
    :class:`PlacementClient`'s ``route``), so a client can re-resolve via any
    healthy seed contact. Gated ``cluster:topology-read`` — an ordinary service
    role's scopes already cover it, not just a cluster operator's
    ``admin:cluster-read``. Replaces the static hand-maintained
    ``GRAPH_RAFT_GROUP_ENDPOINTS`` client map.
    """

    _SCHEMA_VERSION = 1
    _DISCOVERY_DOMAIN = b"epistemic-graph/cluster-discovery/v1\0"
    _MAX_GROUPS = 1_024
    _MAX_MEMBERS = 4_096
    _MAX_FIELD_BYTES = 4 * 1024
    _MAX_CERTIFICATE_ID_BYTES = 512
    _MAX_U64 = (1 << 64) - 1

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client
        self._cluster_id: str | None = None
        self._membership_epoch = 0
        self._placement_epoch = 0

    @staticmethod
    def _is_digest(value: Any, *, prefix: str = "sha256:") -> bool:
        return (
            isinstance(value, str)
            and value.startswith(prefix)
            and len(value) == len(prefix) + 64
            and all(
                character in "0123456789abcdefABCDEF"
                for character in value[len(prefix) :]
            )
        )

    @classmethod
    def _member_identity(cls, cluster_id: str, node_id: int) -> str:
        cluster_bytes = cluster_id.encode("utf-8")
        return (
            "sha256:"
            + hashlib.sha256(
                cls._DISCOVERY_DOMAIN.replace(b"cluster-discovery", b"member-identity")
                + len(cluster_bytes).to_bytes(8, "big")
                + cluster_bytes
                + node_id.to_bytes(8, "big")
            ).hexdigest()
        )

    @classmethod
    def _endpoint_is_bounded(cls, endpoint: Any) -> bool:
        if (
            not isinstance(endpoint, str)
            or not endpoint
            or len(endpoint) > cls._MAX_FIELD_BYTES
        ):
            return False
        if any(
            character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F
            for character in endpoint
        ):
            return False
        if not (endpoint.startswith("tcp://") or endpoint.startswith("tls://")):
            return False
        address = endpoint.split("://", 1)[1]
        if any(character in address for character in "/?#@"):
            return False
        if address.startswith("["):
            if "]:" not in address:
                return False
            host, port = address[1:].split("]:", 1)
            if not host or "]" in host:
                return False
        else:
            if ":" not in address:
                return False
            host, port = address.rsplit(":", 1)
            if not host or ":" in host:
                return False
        try:
            numeric_port = int(port)
        except (TypeError, ValueError):
            return False
        return 0 < numeric_port <= 65_535

    @classmethod
    def _certificate(cls, member: dict[str, Any]) -> tuple[Any, ...]:
        certificate = member.get("certificate")
        if not isinstance(certificate, dict) or set(certificate) != {
            "id",
            "rotation_epoch",
            "not_before_ms",
            "not_after_ms",
        }:
            raise ValueError("ClusterMembers certificate metadata is malformed")
        certificate_id = certificate["id"]
        if certificate_id is not None and (
            not isinstance(certificate_id, str)
            or not certificate_id
            or len(certificate_id) > cls._MAX_CERTIFICATE_ID_BYTES
            or any(
                character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F
                for character in certificate_id
            )
        ):
            raise ValueError("ClusterMembers certificate id is malformed")
        rotation_epoch = certificate["rotation_epoch"]
        not_before = certificate["not_before_ms"]
        not_after = certificate["not_after_ms"]
        if (
            isinstance(rotation_epoch, bool)
            or not isinstance(rotation_epoch, int)
            or not 0 <= rotation_epoch <= cls._MAX_U64
        ):
            raise ValueError("ClusterMembers certificate rotation epoch is malformed")
        for value in (not_before, not_after):
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= cls._MAX_U64
            ):
                raise ValueError("ClusterMembers certificate validity is malformed")
        if not_before is not None and not_after is not None and not_before > not_after:
            raise ValueError("ClusterMembers certificate validity is inverted")
        if rotation_epoch > 0 and certificate_id is None:
            raise ValueError("ClusterMembers certificate rotation requires an id")
        return certificate_id, rotation_epoch, not_before, not_after

    def _validate_and_verify(
        self,
        answer: dict[str, Any],
        *,
        expected_cluster_id: str | None,
        min_membership_epoch: int | None,
        min_placement_epoch: int | None,
    ) -> dict[str, Any]:
        if expected_cluster_id is not None and not self._is_digest(expected_cluster_id):
            raise ValueError("expected_cluster_id is malformed")
        for floor, name in (
            (min_membership_epoch, "min_membership_epoch"),
            (min_placement_epoch, "min_placement_epoch"),
        ):
            if floor is not None and (
                isinstance(floor, bool) or not isinstance(floor, int) or floor < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer")
        if set(answer) != {
            "schema_version",
            "cluster_id",
            "epoch",
            "membership_epoch",
            "placement_epoch",
            "leader",
            "leaders",
            "groups",
            "auth_binding",
            "signature",
        }:
            raise ValueError("ClusterMembers response has unexpected or missing fields")
        if (
            isinstance(answer["schema_version"], bool)
            or answer["schema_version"] != self._SCHEMA_VERSION
        ):
            raise ValueError("ClusterMembers schema version is unsupported")
        cluster_id = answer["cluster_id"]
        if not self._is_digest(cluster_id):
            raise ValueError("ClusterMembers.cluster_id is malformed")
        if expected_cluster_id is not None and cluster_id != expected_cluster_id:
            raise ValueError("ClusterMembers belongs to a different cluster")
        if self._cluster_id is not None and cluster_id != self._cluster_id:
            raise ValueError("ClusterMembers cluster identity changed")

        def non_negative_int(value: Any, field: str) -> int:
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= self._MAX_U64
            ):
                raise ValueError(
                    f"ClusterMembers.{field} must be a non-negative integer"
                )
            return value

        epoch = non_negative_int(answer["epoch"], "epoch")
        membership_epoch = non_negative_int(
            answer["membership_epoch"], "membership_epoch"
        )
        placement_epoch = non_negative_int(answer["placement_epoch"], "placement_epoch")
        if epoch != membership_epoch:
            raise ValueError(
                "ClusterMembers epoch alias does not match membership_epoch"
            )
        if min_membership_epoch is not None and membership_epoch < min_membership_epoch:
            raise ValueError("ClusterMembers membership snapshot is stale")
        if min_placement_epoch is not None and placement_epoch < min_placement_epoch:
            raise ValueError("ClusterMembers placement snapshot is stale")
        if (
            membership_epoch < self._membership_epoch
            or placement_epoch < self._placement_epoch
        ):
            raise ValueError("ClusterMembers snapshot moved backwards")

        groups = answer["groups"]
        leaders = answer["leaders"]
        if not isinstance(groups, list) or len(groups) > self._MAX_GROUPS:
            raise ValueError("ClusterMembers.groups exceeds resource limits")
        if not isinstance(leaders, list) or len(leaders) > self._MAX_GROUPS:
            raise ValueError("ClusterMembers.leaders exceeds resource limits")

        canonical_groups: list[list[Any]] = []
        expected_leaders: list[dict[str, int]] = []
        seen_groups: set[int] = set()
        member_count = 0
        for group in groups:
            if not isinstance(group, dict) or set(group) != {
                "group_id",
                "leader_id",
                "members",
            }:
                raise ValueError("ClusterMembers group entry is malformed")
            group_id = group["group_id"]
            if (
                isinstance(group_id, bool)
                or not isinstance(group_id, int)
                or not 0 <= group_id <= self._MAX_U64
            ):
                raise ValueError("ClusterMembers group_id is malformed")
            if group_id in seen_groups:
                raise ValueError("ClusterMembers contains duplicate groups")
            seen_groups.add(group_id)
            leader_id = group["leader_id"]
            if leader_id is not None and (
                isinstance(leader_id, bool)
                or not isinstance(leader_id, int)
                or not 0 <= leader_id <= self._MAX_U64
            ):
                raise ValueError("ClusterMembers leader_id is malformed")
            members = group["members"]
            if not isinstance(members, list):
                raise ValueError("ClusterMembers members must be a list")
            canonical_members: list[list[Any]] = []
            seen_members: set[int] = set()
            for member in members:
                if not isinstance(member, dict) or set(member) != {
                    "node_id",
                    "member_identity",
                    "role",
                    "client_endpoint",
                    "tls_name",
                    "health",
                    "certificate",
                }:
                    raise ValueError("ClusterMembers member entry is malformed")
                node_id = member["node_id"]
                if (
                    isinstance(node_id, bool)
                    or not isinstance(node_id, int)
                    or not 0 <= node_id <= self._MAX_U64
                ):
                    raise ValueError("ClusterMembers node_id is malformed")
                if node_id in seen_members:
                    raise ValueError("ClusterMembers contains duplicate members")
                seen_members.add(node_id)
                identity = member["member_identity"]
                if not self._is_digest(identity) or identity != self._member_identity(
                    cluster_id, node_id
                ):
                    raise ValueError("ClusterMembers member identity is invalid")
                role = member["role"]
                if role not in ("leader", "follower", "learner"):
                    raise ValueError("ClusterMembers member role is invalid")
                endpoint = member["client_endpoint"]
                if not self._endpoint_is_bounded(endpoint):
                    raise ValueError("ClusterMembers client endpoint is invalid")
                tls_name = member["tls_name"]
                if tls_name is not None and (
                    not isinstance(tls_name, str)
                    or not tls_name
                    or len(tls_name) > self._MAX_FIELD_BYTES
                    or any(
                        character.isspace()
                        or ord(character) < 0x20
                        or ord(character) == 0x7F
                        for character in tls_name
                    )
                ):
                    raise ValueError("ClusterMembers TLS name is invalid")
                health = member["health"]
                if health not in ("healthy", "degraded", "unknown"):
                    raise ValueError("ClusterMembers member health is invalid")
                certificate_id, rotation_epoch, not_before, not_after = (
                    self._certificate(member)
                )
                canonical_members.append(
                    [
                        node_id,
                        identity,
                        role,
                        endpoint,
                        tls_name,
                        health,
                        certificate_id,
                        rotation_epoch,
                        not_before,
                        not_after,
                    ]
                )
                member_count += 1
                if member_count > self._MAX_MEMBERS:
                    raise ValueError("ClusterMembers members exceed resource limits")
            if leader_id is not None:
                if leader_id not in seen_members:
                    raise ValueError("ClusterMembers leader is not a member")
                if (
                    sum(
                        1
                        for member in members
                        if member["node_id"] == leader_id and member["role"] == "leader"
                    )
                    != 1
                ):
                    raise ValueError("ClusterMembers leader role is inconsistent")
                expected_leaders.append({"group_id": group_id, "node_id": leader_id})
            canonical_groups.append([group_id, leader_id, canonical_members])

        for leader in leaders:
            if not isinstance(leader, dict) or set(leader) != {"group_id", "node_id"}:
                raise ValueError("ClusterMembers leader entry is malformed")
            if any(
                isinstance(leader[field], bool)
                or not isinstance(leader[field], int)
                or not 0 <= leader[field] <= self._MAX_U64
                for field in ("group_id", "node_id")
            ):
                raise ValueError("ClusterMembers leader entry is malformed")
        if leaders != expected_leaders:
            raise ValueError("ClusterMembers leaders do not match group leaders")
        expected_leader = expected_leaders[0] if expected_leaders else None
        if answer["leader"] is not None and (
            not isinstance(answer["leader"], dict)
            or set(answer["leader"]) != {"group_id", "node_id"}
            or any(
                isinstance(answer["leader"][field], bool)
                or not isinstance(answer["leader"][field], int)
                or not 0 <= answer["leader"][field] <= self._MAX_U64
                for field in ("group_id", "node_id")
            )
        ):
            raise ValueError("ClusterMembers leader is malformed")
        if answer["leader"] != expected_leader:
            raise ValueError("ClusterMembers leader does not match group leaders")

        binding = answer["auth_binding"]
        if not isinstance(binding, dict) or set(binding) != {
            "tenant_digest",
            "principal_digest",
            "agent_digest",
        }:
            raise ValueError("ClusterMembers auth binding is malformed")
        context = self._client._effective_verified_context()
        expected_binding = {
            "tenant_digest": "sha256:"
            + hashlib.sha256(str(context["tenant"]).encode("utf-8")).hexdigest(),
            "principal_digest": "sha256:"
            + hashlib.sha256(str(context["principal"]).encode("utf-8")).hexdigest(),
            "agent_digest": "sha256:"
            + hashlib.sha256(str(context["agent_id"]).encode("utf-8")).hexdigest(),
        }
        if binding != expected_binding:
            raise ValueError(
                "ClusterMembers snapshot is bound to a different request context"
            )

        signature = answer["signature"]
        if (
            not isinstance(signature, str)
            or not signature.startswith("hmac-sha256:")
            or len(signature) != len("hmac-sha256:") + 64
            or any(
                character not in "0123456789abcdefABCDEF"
                for character in signature[len("hmac-sha256:") :]
            )
        ):
            raise ValueError("ClusterMembers signature is missing or malformed")
        payload = json.dumps(
            [
                "cluster-discovery-v1",
                cluster_id,
                membership_epoch,
                placement_epoch,
                str(context["tenant"]),
                str(context["principal"]),
                str(context["agent_id"]),
                canonical_groups,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        expected_signature = (
            "hmac-sha256:"
            + hmac.new(
                self._client._auth_secret.encode("utf-8"),
                self._DISCOVERY_DOMAIN + payload,
                hashlib.sha256,
            ).hexdigest()
        )
        if not hmac.compare_digest(signature, expected_signature):
            raise ValueError("ClusterMembers signature verification failed")
        self._cluster_id = cluster_id
        self._membership_epoch = membership_epoch
        self._placement_epoch = placement_epoch
        return answer

    async def members(
        self,
        *,
        expected_cluster_id: str | None = None,
        min_membership_epoch: int | None = None,
        min_placement_epoch: int | None = None,
    ) -> dict[str, Any]:
        """Read the current cluster topology.

        Returns a verified schema-v1 snapshot with cluster identity, monotonic
        membership/placement epochs, health, member identity, certificate
        metadata, and signed request-context binding.  Unsigned, stale,
        cross-cluster, malformed, or differently scoped responses are rejected
        before any endpoint is exposed to a caller.  The method accepts only
        identity/epoch expectations; it has no caller-supplied endpoint
        authority.
        """
        answer = await self._client._send("ClusterMembers")
        if not isinstance(answer, dict):
            raise TypeError("ClusterMembers must be a mapping")
        return self._validate_and_verify(
            answer,
            expected_cluster_id=expected_cluster_id,
            min_membership_epoch=min_membership_epoch,
            min_placement_epoch=min_placement_epoch,
        )


class ServerRegistryClient:
    """CONCEPT:EG-KG.sharding.server-registry — W2.5 engine-native fleet server registry.

    Exposes ``Method::RegisterServer``: a push-registration + lease-TTL
    heartbeat RPC that writes a REAL, queryable ``:Server`` graph node into
    ``__commons__`` — unlike :class:`ClusterTopologyClient` (cluster Raft
    nodes, deliberately NOT graph nodes), a ``:Server`` row here IS a
    first-class KG entity the fleet queries (``MATCH
    (s:Server)-[:PROVIDES]->(r:CallableResource)``), the SAME shape
    ``agent_utilities.knowledge_graph.core.engine_ingestion.ingest_mcp_server``
    writes today via Cypher ``MERGE``. Every fleet MCP server self-registers
    at startup and re-calls :meth:`register` periodically to renew its lease
    (wired into ``mcp/server_factory.py`` so every fleet server gets it for
    free); the au config-sync ingestion becomes a reconciler that repairs
    drift through this SAME RPC instead of being the sole writer.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def register(
        self,
        name: str,
        url: str,
        *,
        resources: dict[str, Any] | None = None,
        ttl_secs: int = 300,
    ) -> bool:
        """Push-register (or renew) ``name``'s fleet identity.

        A repeat call with the SAME ``name`` renews the lease — call this
        periodically (well inside ``ttl_secs``) as a heartbeat; a server that
        stops renewing is reaped by the engine's stale-lease sweep once its
        lease lapses (CONCEPT:EG-KG.sharding.server-registry). The server computes the
        absolute lease expiry from its own clock — it never trusts a
        caller-supplied timestamp. ``url`` should be a bounded, privacy-safe
        endpoint reference (never a raw credentialed URL). ``resources`` is
        optional, non-sensitive, size-bounded metadata (encoded as opaque
        JSON). Returns ``True`` on success.
        """
        if not isinstance(name, str) or not name:
            raise ValueError("RegisterServer.name is required")
        if not isinstance(url, str) or not url:
            raise ValueError("RegisterServer.url is required")
        if isinstance(ttl_secs, bool) or not isinstance(ttl_secs, int) or ttl_secs <= 0:
            raise ValueError("RegisterServer.ttl_secs must be a positive integer")
        if resources is not None and not isinstance(resources, dict):
            raise ValueError("RegisterServer.resources must be a mapping")
        resources_json = (
            json.dumps(resources, separators=(",", ":"), sort_keys=True)
            if resources
            else ""
        )
        result = await self._client._send(
            "RegisterServer",
            {
                "name": name,
                "url": url,
                "resources_json": resources_json,
                "ttl_secs": ttl_secs,
            },
        )
        return bool(result)


class RaftAdminClient:
    """CONCEPT:EG-KG.storage.kg-kg-2 — Raft cluster-membership admin namespace
    (``cluster_deployment.md`` §5 item 2).

    Drives ``MultiRaft::add_group_learner``/``change_group_voters`` (raft/multi.rs)
    over the wire via ``Method::RaftAddLearner``/``Method::RaftChangeMembership``,
    the missing "attach a node to a live cluster" entrypoint the M2 soak flagged.
    Both ops are leader-only; a follower answers ``OPERATION_REDIRECTED`` naming the
    current leader (the same shape ``PlacementRoute``'s stale route uses), and an
    engine with no live ``MultiRaft`` raises a clean error.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add_learner(
        self, node_id: int, addr: str, *, group: int | None = None
    ) -> bool:
        """Attach ``node_id`` (reachable at ``addr``) to ``group`` (default: the
        single-group deployment's group 0) as a NON-VOTING LEARNER. Starts
        replication immediately and blocks until the learner's log is caught up,
        but does NOT change the voter set — quorum size and fault tolerance are
        unaffected. MUST be issued against the group's current leader. The safe
        first step before optionally promoting the node with
        :meth:`change_membership`."""
        return await self._client._send(
            "RaftAddLearner",
            {"group": group, "node_id": node_id, "addr": addr},
        )

    async def change_membership(
        self, voters: list[int], *, group: int | None = None
    ) -> bool:
        """Set ``group``'s (default: group 0) VOTER set to exactly ``voters``. The
        usual way to PROMOTE one or more learners added via :meth:`add_learner`:
        pass the full desired voter set (existing voters plus the learner(s) being
        promoted). Refuses to produce an empty voter set. MUST be issued against
        the group's current leader."""
        return await self._client._send(
            "RaftChangeMembership",
            {"group": group, "voters": voters},
        )


class AgentIdentity(TypedDict):
    """Wire shape of ``Method::GetIdentity``'s ``Some(...)`` result — mirrors the
    Rust ``eg_types::acl::AgentIdentity`` struct (``crates/eg-types/src/acl.rs:78-88``).

    ``role`` is either the bare string ``"System"``/``"Agent"`` or the single-key
    mapping ``{"Manager": {"subordinates": [...]}}`` — the same shape
    :meth:`ConsensusClient.register_identity` accepts as input.
    """

    agent_id: str
    role: str | dict[str, Any]
    teams: list[str]
    roles: list[str]


class ConsensusClient:
    """CONCEPT:AU-KG.research.research-pipeline-runner — Zero-Trust Consensus Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def register_identity(
        self,
        agent_id: str,
        role: str | dict[str, Any],
        teams: list[str],
        roles: list[str],
        *,
        signer_id: str,
        signer_key: str,
    ) -> str:
        """Register an identity with a detached current-context signature.

        ``signer_id`` and ``agent_id`` are explicit opaque identifiers. The
        signer key is used only for this call and is never added to request
        parameters or retained by the client.
        """

        if not isinstance(agent_id, str) or not agent_id.strip():
            raise ValueError("agent_id must be a non-empty opaque identifier")
        teams = _validate_explicit_string_list("teams", teams)
        roles = _validate_explicit_string_list("roles", roles)
        if isinstance(role, str):
            if role not in {"System", "Agent"}:
                raise ValueError("role must be System, Agent, or a Manager value")
        elif isinstance(role, dict):
            manager = role.get("Manager") if set(role) == {"Manager"} else None
            if not isinstance(manager, dict) or set(manager) != {"subordinates"}:
                raise ValueError("Manager role must contain only subordinates")
            role = {
                "Manager": {
                    "subordinates": _validate_explicit_string_list(
                        "Manager.subordinates", manager["subordinates"]
                    )
                }
            }
        else:
            raise TypeError("role must be System, Agent, or a Manager value")
        idempotency_key = self._client._new_operation_idempotency_key()
        params: dict[str, Any] = {
            "agent_id": agent_id,
            "role": role,
            "teams": teams,
            "signature": "",
            "roles": roles,
        }
        params["signature"] = self._client._sign_context_operation(
            domain="eg-register-identity-v2",
            method="RegisterIdentity",
            params=params,
            graph="__commons__",
            idempotency_key=idempotency_key,
            signer_id=signer_id,
            signer_key=signer_key,
        )
        return await self._client._send(
            "RegisterIdentity",
            params,
            graph="__commons__",
            idempotency_key=idempotency_key,
        )

    async def bootstrap_system_identity(
        self, *, agent_id: str, signer_id: str, signer_key: str
    ) -> str:
        """Create the first system identity through the current bootstrap gate."""

        context = self._client._effective_verified_context()
        if (
            context["principal"] != agent_id
            or context["agent_id"] != agent_id
            or signer_id != agent_id
            or context["roles"]
            or context["scopes"] != ["security:bootstrap"]
            or context["delegation"]
        ):
            raise ValueError(
                "bootstrap requires matching explicit identities and only security:bootstrap authority"
            )
        return await self.register_identity(
            agent_id,
            "System",
            [],
            [],
            signer_id=signer_id,
            signer_key=signer_key,
        )

    async def get_identity(self, agent_id: str) -> AgentIdentity | None:
        """Read back ``agent_id``'s current identity — the read half of
        :meth:`register_identity` (``Method::GetIdentity``, read-only,
        ``authz_action: security:admin``).

        Three, and only three, outcomes:

        * ``agent_id`` has never been registered → returns ``None``.
        * ``agent_id`` is registered but currently holds no RBAC roles →
          returns an :class:`AgentIdentity` with ``roles: []`` (and possibly
          ``teams: []``). This is a *confirmed empty* result, not an unknown
          one, and callers (e.g. admission code merging ``existing_roles``
          into a :meth:`register_identity` upsert) must not treat it as
          equivalent to "not registered".
        * The call itself fails (timeout, transport error, engine error,
          malformed response) → raises, exactly like every other RPC on this
          client. A failure is never translated into ``None`` — doing so
          would recreate the ambiguity this RPC exists to remove.
        """

        if not isinstance(agent_id, str) or not agent_id.strip():
            raise ValueError("agent_id must be a non-empty opaque identifier")
        result = await self._client._send(
            "GetIdentity",
            {"agent_id": agent_id},
            graph="__commons__",
        )
        if result is None:
            return None
        return cast(
            AgentIdentity,
            _exact_mapping(
                "AgentIdentity",
                result,
                frozenset({"agent_id", "role", "teams", "roles"}),
            ),
        )

    async def apply_multisig_mutation(
        self,
        signer_keys: dict[str, str],
        threshold: int,
        mutation_type: str,
        query: str,
    ) -> str:
        """Apply an administrative mutation signed by explicit trusted signers."""

        if (
            not isinstance(signer_keys, dict)
            or not isinstance(threshold, int)
            or isinstance(threshold, bool)
            or threshold <= 0
            or len(signer_keys) < threshold
        ):
            raise ValueError("threshold requires at least that many explicit signers")
        if not isinstance(mutation_type, str) or not mutation_type.strip():
            raise ValueError("mutation_type and query must be non-empty strings")
        if not isinstance(query, str) or not query.strip():
            raise ValueError("mutation_type and query must be non-empty strings")
        if any(
            not isinstance(signer_id, str)
            or not signer_id.strip()
            or not isinstance(signer_key, str)
            or not signer_key
            for signer_id, signer_key in signer_keys.items()
        ):
            raise ValueError("operation signer ids and keys must be non-empty strings")
        idempotency_key = self._client._new_operation_idempotency_key()
        params: dict[str, Any] = {
            "signatures": [],
            "threshold": threshold,
            "mutation_type": mutation_type,
            "query": query,
        }
        signatures = [
            self._client._sign_context_operation(
                domain="eg-multisig-mutation-v2",
                method="ApplyMultisigMutation",
                params=params,
                graph="__commons__",
                idempotency_key=idempotency_key,
                signer_id=signer_id,
                signer_key=signer_keys[signer_id],
                require_context_principal=False,
            )
            for signer_id in sorted(signer_keys)
        ]
        params["signatures"] = signatures
        return await self._client._send(
            "ApplyMultisigMutation",
            params,
            graph="__commons__",
            idempotency_key=idempotency_key,
        )


class FinanceClient:
    """CONCEPT:AU-KG.research.research-pipeline-runner — Quantitative Finance Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def optimize_portfolio(
        self,
        expected_returns: list[float],
        cov_matrix: list[list[float]],
        risk_free_rate: float,
        min_weight: float | None = None,
        max_weight: float | None = None,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceOptimizePortfolio",
            {
                "expected_returns": expected_returns,
                "cov_matrix": cov_matrix,
                "risk_free_rate": risk_free_rate,
                "min_weight": min_weight,
                "max_weight": max_weight,
            },
        )

    async def risk_parity(self, cov_matrix: list[list[float]]) -> dict[str, Any]:
        return await self._client._send(
            "FinanceRiskParity",
            {"cov_matrix": cov_matrix},
        )

    async def black_litterman(
        self,
        market_weights: list[float],
        cov_matrix: list[list[float]],
        views: list[float],
        pick_matrix: list[list[float]],
        tau: float,
        risk_aversion: float,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceBlackLitterman",
            {
                "market_weights": market_weights,
                "cov_matrix": cov_matrix,
                "views": views,
                "pick_matrix": pick_matrix,
                "tau": tau,
                "risk_aversion": risk_aversion,
            },
        )

    async def efficient_frontier(
        self,
        expected_returns: list[float],
        cov_matrix: list[list[float]],
        target_return: float,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceEfficientFrontier",
            {
                "expected_returns": expected_returns,
                "cov_matrix": cov_matrix,
                "target_return": target_return,
            },
        )

    # ── Risk metrics ──────────────────────────────────────────────────
    async def var(self, returns: list[float], confidence: float = 0.95) -> float:
        return await self._client._send(
            "FinanceVar", {"returns": returns, "confidence": confidence}
        )

    async def cvar(self, returns: list[float], confidence: float = 0.95) -> float:
        return await self._client._send(
            "FinanceCvar", {"returns": returns, "confidence": confidence}
        )

    async def max_drawdown(self, returns: list[float]) -> float:
        return await self._client._send("FinanceMaxDrawdown", {"returns": returns})

    async def drawdown_series(self, returns: list[float]) -> list[float]:
        return await self._client._send("FinanceDrawdownSeries", {"returns": returns})

    async def downside_deviation(
        self, returns: list[float], target: float = 0.0
    ) -> float:
        return await self._client._send(
            "FinanceDownsideDeviation", {"returns": returns, "target": target}
        )

    async def risk_metrics(
        self, returns: list[float], risk_free_rate: float = 0.0
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceRiskMetrics",
            {"returns": returns, "risk_free_rate": risk_free_rate},
        )

    async def monte_carlo_var(
        self,
        mean: float,
        std_dev: float,
        n_simulations: int = 10000,
        confidence: float = 0.95,
    ) -> float:
        return await self._client._send(
            "FinanceMonteCarloVar",
            {
                "mean": mean,
                "std_dev": std_dev,
                "n_simulations": n_simulations,
                "confidence": confidence,
            },
        )

    async def stress_test(
        self,
        weights: list[float],
        expected_returns: list[float],
        cov_matrix: list[list[float]],
        shock_factors: list[float],
    ) -> list[float]:
        return await self._client._send(
            "FinanceStressTest",
            {
                "weights": weights,
                "expected_returns": expected_returns,
                "cov_matrix": cov_matrix,
                "shock_factors": shock_factors,
            },
        )

    # ── Regime detection (HMM) ────────────────────────────────────────
    async def detect_regimes(
        self,
        observations: list[float],
        n_states: int = 2,
        max_iter: int = 100,
        tol: float = 1e-4,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceDetectRegimes",
            {
                "observations": observations,
                "n_states": n_states,
                "max_iter": max_iter,
                "tol": tol,
            },
        )

    # ── Signals / alpha ───────────────────────────────────────────────
    async def rolling_zscore(self, values: list[float], window: int) -> list[float]:
        return await self._client._send(
            "FinanceRollingZscore", {"values": values, "window": window}
        )

    async def ewma(self, values: list[float], span: int) -> list[float]:
        return await self._client._send("FinanceEwma", {"values": values, "span": span})

    async def signal_decay(self, signal: list[float], half_life: float) -> list[float]:
        return await self._client._send(
            "FinanceSignalDecay", {"signal": signal, "half_life": half_life}
        )

    async def combine_alphas(
        self, signals: list[list[float]], weights: list[float]
    ) -> list[float]:
        return await self._client._send(
            "FinanceCombineAlphas", {"signals": signals, "weights": weights}
        )

    async def cross_sectional_rank(
        self, cross_section: list[list[float]]
    ) -> list[list[float]]:
        return await self._client._send(
            "FinanceCrossSectionalRank", {"cross_section": cross_section}
        )

    async def momentum(self, prices: list[float], lookback: int) -> list[float]:
        return await self._client._send(
            "FinanceMomentum", {"prices": prices, "lookback": lookback}
        )

    async def mean_reversion(self, values: list[float], window: int) -> list[float]:
        return await self._client._send(
            "FinanceMeanReversion", {"values": values, "window": window}
        )

    async def information_coefficient(
        self, signal: list[float], forward_returns: list[float]
    ) -> float:
        return await self._client._send(
            "FinanceInformationCoefficient",
            {"signal": signal, "forward_returns": forward_returns},
        )

    # ── Execution / microstructure ────────────────────────────────────
    async def twap(
        self,
        total_quantity: float,
        n_slices: int,
        start_time: int = 0,
        interval_secs: int = 60,
    ) -> list[tuple[int, float]]:
        return await self._client._send(
            "FinanceTwap",
            {
                "total_quantity": total_quantity,
                "n_slices": n_slices,
                "start_time": start_time,
                "interval_secs": interval_secs,
            },
        )

    async def vwap(
        self,
        total_quantity: float,
        volume_profile: list[float],
        start_time: int = 0,
        interval_secs: int = 60,
    ) -> list[tuple[int, float]]:
        return await self._client._send(
            "FinanceVwap",
            {
                "total_quantity": total_quantity,
                "volume_profile": volume_profile,
                "start_time": start_time,
                "interval_secs": interval_secs,
            },
        )

    async def market_impact(
        self,
        daily_volatility: float,
        order_quantity: float,
        average_daily_volume: float,
        impact_coefficient: float = 0.1,
    ) -> float:
        return await self._client._send(
            "FinanceMarketImpact",
            {
                "daily_volatility": daily_volatility,
                "order_quantity": order_quantity,
                "average_daily_volume": average_daily_volume,
                "impact_coefficient": impact_coefficient,
            },
        )

    async def pairs_trading(
        self, prices_a: list[float], prices_b: list[float], lookback: int
    ) -> list[float]:
        return await self._client._send(
            "FinancePairsTrading",
            {"prices_a": prices_a, "prices_b": prices_b, "lookback": lookback},
        )

    async def match_orders(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Match a limit-order book. Each order: {id, side, price, quantity, timestamp}."""
        return await self._client._send("FinanceMatchOrders", {"orders": orders})

    # ── Market making / microstructure (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ──────────────
    async def avellaneda_stoikov(
        self,
        mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        kappa: float,
        tau: float,
    ) -> dict[str, Any]:
        """Optimal AS quotes around a freely-drifting mid. Returns
        {bid, ask, reservation, half_spread, withdraw}."""
        return await self._client._send(
            "FinanceAvellanedaStoikov",
            {
                "mid": mid,
                "inventory": inventory,
                "sigma": sigma,
                "gamma": gamma,
                "kappa": kappa,
                "tau": tau,
            },
        )

    async def glt_quotes(
        self,
        mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        kappa: float,
        a: float,
    ) -> dict[str, Any]:
        """Guéant-Lehalle-Fernandez-Tapia closed-form quotes with inventory skew."""
        return await self._client._send(
            "FinanceGltQuotes",
            {
                "mid": mid,
                "inventory": inventory,
                "sigma": sigma,
                "gamma": gamma,
                "kappa": kappa,
                "a": a,
            },
        )

    async def logit_quotes(
        self,
        p_mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        kappa: float,
        tau: float,
        boundary_m: float = 0.0,
    ) -> dict[str, Any]:
        """Logit-space AS quotes for bounded (0,1) prediction-market prices, with
        a boundary-aware inventory cap. ``withdraw=True`` ⇒ pull quotes."""
        return await self._client._send(
            "FinanceLogitQuotes",
            {
                "p_mid": p_mid,
                "inventory": inventory,
                "sigma": sigma,
                "gamma": gamma,
                "kappa": kappa,
                "tau": tau,
                "boundary_m": boundary_m,
            },
        )

    async def glosten_milgrom_spread(self, alpha: float, p: float) -> float:
        return await self._client._send(
            "FinanceGlostenMilgromSpread", {"alpha": alpha, "p": p}
        )

    async def expected_pnl_rate(
        self,
        delta: float,
        a: float,
        kappa: float,
        alpha: float,
        p: float,
        v_h: float = 1.0,
        v_l: float = 0.0,
    ) -> float:
        return await self._client._send(
            "FinanceExpectedPnlRate",
            {
                "delta": delta,
                "a": a,
                "kappa": kappa,
                "alpha": alpha,
                "p": p,
                "v_h": v_h,
                "v_l": v_l,
            },
        )

    async def breakeven_alpha(
        self, delta: float, p: float, v_h: float = 1.0, v_l: float = 0.0
    ) -> float:
        return await self._client._send(
            "FinanceBreakevenAlpha", {"delta": delta, "p": p, "v_h": v_h, "v_l": v_l}
        )

    async def ofi_series(
        self,
        ts: list[float],
        bid_px: list[float],
        bid_sz: list[float],
        ask_px: list[float],
        ask_sz: list[float],
        window_secs: float = 1.0,
    ) -> list[float]:
        """Cont-Kukanov-Stoikov rolling order-flow imbalance over book events."""
        return await self._client._send(
            "FinanceOfiSeries",
            {
                "ts": ts,
                "bid_px": bid_px,
                "bid_sz": bid_sz,
                "ask_px": ask_px,
                "ask_sz": ask_sz,
                "window_secs": window_secs,
            },
        )

    async def microprice_series(
        self,
        bid_px: list[float],
        bid_sz: list[float],
        ask_px: list[float],
        ask_sz: list[float],
    ) -> list[float]:
        return await self._client._send(
            "FinanceMicropriceSeries",
            {"bid_px": bid_px, "bid_sz": bid_sz, "ask_px": ask_px, "ask_sz": ask_sz},
        )

    async def vpin_pm(
        self,
        buy_vol: list[float],
        sell_vol: list[float],
        p_mean: list[float],
    ) -> float:
        """VPIN toxicity normalised for binary-payoff variance (prediction markets)."""
        return await self._client._send(
            "FinanceVpinPm",
            {"buy_vol": buy_vol, "sell_vol": sell_vol, "p_mean": p_mean},
        )

    async def hawkes_mle(
        self, times: list[float], t_horizon: float, max_iter: int = 200
    ) -> dict[str, Any]:
        """Fit an exponential-kernel Hawkes process. Returns mu/alpha/beta plus
        branching_ratio (>0.95 ⇒ near-critical / crash early-warning)."""
        return await self._client._send(
            "FinanceHawkesMle",
            {"times": times, "t_horizon": t_horizon, "max_iter": max_iter},
        )

    async def hardiman_bouchaud(
        self, times: list[float], t_horizon: float, n_windows: int = 100
    ) -> float:
        """Model-free Hawkes branching ratio from count over-dispersion."""
        return await self._client._send(
            "FinanceHardimanBouchaud",
            {"times": times, "t_horizon": t_horizon, "n_windows": n_windows},
        )

    # ── Kyle insider/stealth surveillance (CONCEPT:EG-KG.domains.concept-2) ───────────
    async def kyle_lambda(
        self, price_changes: list[float], signed_order_flow: list[float]
    ) -> float:
        """Empirical Kyle's λ — price impact (depth) per unit signed net order flow."""
        return await self._client._send(
            "FinanceKyleLambda",
            {"price_changes": price_changes, "signed_order_flow": signed_order_flow},
        )

    async def surveillance_risk(
        self,
        buy_vol: list[float],
        sell_vol: list[float],
        p_mean: list[float],
        signed_flow: list[float],
        price_changes: list[float],
        baseline_sigma: float = 0.0,
    ) -> dict[str, Any]:
        """Kyle insider/stealth-trading surveillance scores (CONCEPT:EG-KG.domains.concept-2).

        Returns ``kyle_lambda``, ``informed_share`` (VPIN α), ``detection_hazard``,
        ``cumulative_suspicion``, ``stealth_ratio`` and ``legal_risk_score`` ∈ [0,1].
        DEFENSIVE use: informed-flow detection + maker adverse-selection protection.
        Pass ``baseline_sigma`` ≤ 0 to use the sample std of ``signed_flow``.
        """
        return await self._client._send(
            "FinanceSurveillanceRisk",
            {
                "buy_vol": buy_vol,
                "sell_vol": sell_vol,
                "p_mean": p_mean,
                "signed_flow": signed_flow,
                "price_changes": price_changes,
                "baseline_sigma": baseline_sigma,
            },
        )

    # ── Position sizing (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ─────────────────────────────
    async def kelly_fraction(self, q: float, c: float, fraction: float = 0.25) -> float:
        """Fractional Kelly for a YES contract: f* = (q−c)/(1−c), scaled."""
        return await self._client._send(
            "FinanceKellyFraction", {"q": q, "c": c, "fraction": fraction}
        )

    async def bayesian_kelly(
        self, alpha: float, beta: float, c: float, n_quadrature: int = 50
    ) -> float:
        """Kelly under a Beta(α,β) posterior over the true probability — shrinks
        the bet as posterior variance grows."""
        return await self._client._send(
            "FinanceBayesianKelly",
            {"alpha": alpha, "beta": beta, "c": c, "n_quadrature": n_quadrature},
        )

    async def posterior_credible_interval(
        self, alpha: float, beta: float, level: float = 0.05
    ) -> dict[str, float]:
        return await self._client._send(
            "FinancePosteriorCredibleInterval",
            {"alpha": alpha, "beta": beta, "level": level},
        )

    # ── Backtest validation (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest) ─────────────────────────
    async def purged_cpcv(
        self,
        n_samples: int,
        n_groups: int = 6,
        n_test_groups: int = 2,
        purge_window: int = 0,
        embargo: int = 0,
    ) -> list[dict[str, list[int]]]:
        """Purged combinatorial CV splits — each {train: [...], test: [...]}."""
        return await self._client._send(
            "FinancePurgedCpcv",
            {
                "n_samples": n_samples,
                "n_groups": n_groups,
                "n_test_groups": n_test_groups,
                "purge_window": purge_window,
                "embargo": embargo,
            },
        )

    async def deflated_sharpe(
        self, observed_sr: float, n_trials: int, sr_returns: list[float]
    ) -> float:
        """Probability the observed Sharpe beats zero after deflating for trials
        and non-normality (Bailey & López de Prado). DSR > 0.95 = strong."""
        return await self._client._send(
            "FinanceDeflatedSharpe",
            {
                "observed_sr": observed_sr,
                "n_trials": n_trials,
                "sr_returns": sr_returns,
            },
        )

    async def probability_backtest_overfit(
        self, insample: list[list[float]], oos: list[list[float]]
    ) -> float:
        """PBO — rows = CV splits, cols = strategies. < 0.3 robust; > 0.5 overfit."""
        return await self._client._send(
            "FinanceProbabilityBacktestOverfit",
            {"insample": insample, "oos": oos},
        )

    async def diebold_mariano(
        self, losses_a: list[float], losses_b: list[float], h: int = 1
    ) -> dict[str, Any]:
        """Test of equal predictive accuracy (Newey-West HAC for h>1)."""
        return await self._client._send(
            "FinanceDieboldMariano",
            {"losses_a": losses_a, "losses_b": losses_b, "h": h},
        )

    # ── Forensic accounting (CONCEPT:EG-KG.domains.forensic-accounting-kernels) ─────────────────────────
    async def forensic_report(
        self, this_year: dict[str, Any], prior_year: dict[str, Any]
    ) -> dict[str, Any]:
        """Beneish M / Altman Z / Piotroski F / Sloan accruals over two fiscal
        years. Returns scores + flags + verdict (INVESTIGATE | CLEAN). Each year
        dict carries standardized line items (sales, cogs, net_income, cfo, ...)."""
        return await self._client._send(
            "FinanceForensicReport",
            {"this_year": this_year, "prior_year": prior_year},
        )

    # ── State-space / stat-arb (CONCEPT:EG-KG.domains.state-space-statistical-arbitrage) ──────────────────────
    async def kalman_filter_1d(
        self,
        observations: list[float],
        f: float = 1.0,
        q: float = 1e-5,
        h: float = 1.0,
        r: float = 1e-3,
        x0: float = 0.0,
        p0: float = 1.0,
    ) -> dict[str, Any]:
        """Scalar Kalman filter — returns {states, variances} per step."""
        return await self._client._send(
            "FinanceKalmanFilter1d",
            {
                "observations": observations,
                "f": f,
                "q": q,
                "h": h,
                "r": r,
                "x0": x0,
                "p0": p0,
            },
        )

    async def kalman_beta(
        self,
        market_returns: list[float],
        asset_returns: list[float],
        q: float = 1e-5,
        r: float = 1e-3,
        beta0: float = 1.0,
        p0: float = 1.0,
    ) -> dict[str, Any]:
        """Dynamic (time-varying) beta via Kalman filter — {states (betas), variances}.
        OLS gives the average; this gives the current hidden beta with uncertainty."""
        return await self._client._send(
            "FinanceKalmanBeta",
            {
                "market_returns": market_returns,
                "asset_returns": asset_returns,
                "q": q,
                "r": r,
                "beta0": beta0,
                "p0": p0,
            },
        )

    async def kalman_volatility(
        self,
        returns: list[float],
        q: float = 0.1,
        r: float = 1.0,
        log_var0: float | None = None,
        p0: float = 1.0,
        annualization: float = 252.0,
    ) -> list[float]:
        """Kalman volatility tracker (log-variance state) — annualised vol series.
        Tells you what volatility *is* now, not what it was (vs GARCH/EWMA)."""
        return await self._client._send(
            "FinanceKalmanVolatility",
            {
                "returns": returns,
                "q": q,
                "r": r,
                "log_var0": log_var0,
                "p0": p0,
                "annualization": annualization,
            },
        )

    async def adf_test(self, series: list[float], max_lag: int = 1) -> dict[str, Any]:
        """Augmented Dickey-Fuller cointegration/stationarity test — returns
        {statistic, crit_5pct, stationary_5pct, ...}."""
        return await self._client._send(
            "FinanceAdfTest", {"series": series, "max_lag": max_lag}
        )

    async def ou_calibrate(
        self, spread: list[float], dt: float = 1.0
    ) -> dict[str, Any]:
        """Calibrate an Ornstein-Uhlenbeck mean-reversion process from a spread —
        {theta, mu, sigma, half_life, sigma_eq}."""
        return await self._client._send(
            "FinanceOuCalibrate", {"spread": spread, "dt": dt}
        )

    async def ou_optimal_thresholds(
        self,
        theta: float,
        mu: float,
        sigma: float,
        sigma_eq: float,
        cost: float = 0.0,
    ) -> dict[str, Any]:
        """MFPT-optimal OU entry/exit band — {entry_long, entry_short, exit, z,
        expected_return_per_unit_time}."""
        return await self._client._send(
            "FinanceOuOptimalThresholds",
            {
                "theta": theta,
                "mu": mu,
                "sigma": sigma,
                "sigma_eq": sigma_eq,
                "cost": cost,
            },
        )

    async def markov_transition_matrix(
        self, states: list[int], n_states: int
    ) -> list[list[float]]:
        """Laplace-smoothed row-stochastic transition matrix from a state sequence
        (cross-venue lead-lag / regime transitions)."""
        return await self._client._send(
            "FinanceMarkovTransitionMatrix", {"states": states, "n_states": n_states}
        )

    # ── Signal combination / sizing / calibration (CONCEPT:EG-KG.domains.quant-finance) ───
    async def order_book_imbalance(
        self, v_bid: list[float], v_ask: list[float]
    ) -> list[float]:
        """Level-1 order-book imbalance series ∈ [−1, 1]."""
        return await self._client._send(
            "FinanceOrderBookImbalance", {"v_bid": v_bid, "v_ask": v_ask}
        )

    async def queue_imbalance(
        self,
        bid_q: list[float],
        ask_q: list[float],
        bid_rate: list[float],
        ask_rate: list[float],
    ) -> dict[str, Any]:
        """Queue-position / time-to-fill signal at the best bid/ask. Returns
        {skew, bid_fill_time, ask_fill_time}; skew = (ask_q−bid_q)/(ask_q+bid_q)
        (positive ⇒ ask queue heavier ⇒ resting bid fills faster)."""
        return await self._client._send(
            "FinanceQueueImbalance",
            {
                "bid_q": bid_q,
                "ask_q": ask_q,
                "bid_rate": bid_rate,
                "ask_rate": ask_rate,
            },
        )

    async def realized_vol_tick(
        self, mid: list[float], window: int = 20
    ) -> list[float]:
        """Tick-level rolling realized volatility of the mid-price (model-free;
        distinct from the kalman_volatility state-space filter)."""
        return await self._client._send(
            "FinanceRealizedVolTick", {"mid": mid, "window": window}
        )

    async def spread_reversion(
        self, bid_px: list[float], ask_px: list[float], window: int = 20
    ) -> dict[str, Any]:
        """Spread mean-reversion feature. Returns {zscore, signal} where the
        rolling z-score of (ask−bid) drives signal = −zscore (wide ⇒ expect
        tighten). Lightweight rolling stats, NOT the OU calibration."""
        return await self._client._send(
            "FinanceSpreadReversion",
            {"bid_px": bid_px, "ask_px": ask_px, "window": window},
        )

    async def information_ratio(self, ic: float, n_independent: float) -> float:
        """Fundamental law of active management: IR = IC · √(N_independent)."""
        return await self._client._send(
            "FinanceInformationRatio", {"ic": ic, "n_independent": n_independent}
        )

    async def effective_independent_n(self, returns_matrix: list[list[float]]) -> float:
        """Effective number of independent signals (eigenvalue participation ratio)
        — correlated signals collapse, exposing the real N in IR = IC·√N."""
        return await self._client._send(
            "FinanceEffectiveIndependentN", {"returns_matrix": returns_matrix}
        )

    async def alpha_combination_engine(
        self, returns_matrix: list[list[float]], lookback: int = 20
    ) -> list[float]:
        """Combine N signals into weights that reward independent edge and penalise
        shared variance (the IR = IC·√N combination engine). Rows = signals."""
        return await self._client._send(
            "FinanceAlphaCombinationEngine",
            {"returns_matrix": returns_matrix, "lookback": lookback},
        )

    async def brier_score(self, forecasts: list[float], outcomes: list[float]) -> float:
        """Brier score of probabilistic forecasts vs binary outcomes (< 0.25 =
        production-grade calibration)."""
        return await self._client._send(
            "FinanceBrierScore", {"forecasts": forecasts, "outcomes": outcomes}
        )

    async def convergence_gate(
        self, strengths: list[float], strong_threshold: float = 0.6, min_agree: int = 5
    ) -> dict[str, Any]:
        """Conviction gate — require ≥min_agree of N signals to STRONGLY agree on a
        direction before trading. Returns {agree, total, fraction, direction, pass}."""
        return await self._client._send(
            "FinanceConvergenceGate",
            {
                "strengths": strengths,
                "strong_threshold": strong_threshold,
                "min_agree": min_agree,
            },
        )

    async def empirical_kelly(
        self,
        p: float,
        b: float,
        historical_returns: list[float],
        n_simulations: int = 10000,
        seed: int = 42,
    ) -> float:
        """Uncertainty-adjusted Kelly: f* · (1 − CV_edge), with CV_edge from a
        seeded bootstrap of the historical returns. Shrinks bets when edge is noisy."""
        return await self._client._send(
            "FinanceEmpiricalKelly",
            {
                "p": p,
                "b": b,
                "historical_returns": historical_returns,
                "n_simulations": n_simulations,
                "seed": seed,
            },
        )

    # ── Derivatives: SABR volatility surface (CONCEPT:AU-KG.domains.derivatives) ─────────
    async def sabr_implied_vol(
        self,
        f: float,
        k: float,
        t: float,
        alpha: float,
        beta: float,
        rho: float,
        nu: float,
    ) -> float:
        """SABR lognormal (Black) implied volatility for one strike (Hagan 2002)."""
        return await self._client._send(
            "FinanceSabrImpliedVol",
            {
                "f": f,
                "k": k,
                "t": t,
                "alpha": alpha,
                "beta": beta,
                "rho": rho,
                "nu": nu,
            },
        )

    async def sabr_smile(
        self,
        f: float,
        strikes: list[float],
        t: float,
        alpha: float,
        beta: float,
        rho: float,
        nu: float,
    ) -> list[float]:
        """SABR implied-vol smile across strikes."""
        return await self._client._send(
            "FinanceSabrSmile",
            {
                "f": f,
                "strikes": strikes,
                "t": t,
                "alpha": alpha,
                "beta": beta,
                "rho": rho,
                "nu": nu,
            },
        )

    async def sabr_calibrate(
        self,
        f: float,
        t: float,
        strikes: list[float],
        market_vols: list[float],
        beta: float = 0.5,
    ) -> dict[str, Any]:
        """Calibrate SABR (α, ρ, ν) to a market smile with β fixed — returns
        {alpha, beta, rho, nu, rmse, converged}."""
        return await self._client._send(
            "FinanceSabrCalibrate",
            {
                "f": f,
                "t": t,
                "strikes": strikes,
                "market_vols": market_vols,
                "beta": beta,
            },
        )


class DataScienceClient:
    """CONCEPT:EG-KG.compute.rust-native-training-loss — Data Science Primitives Namespace.

    Rust-backed OLS / K-means / PCA / dataset-stats / split. Arrays are shipped
    whole per call (one round-trip) — never loop per row over the wire.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def linear_regression(
        self, x: list[list[float]], y: list[float]
    ) -> dict[str, Any]:
        return await self._client._send("DsLinearRegression", {"x": x, "y": y})

    async def kmeans(
        self, data: list[list[float]], k: int, max_iter: int = 100
    ) -> dict[str, Any]:
        return await self._client._send(
            "DsKMeans", {"data": data, "k": k, "max_iter": max_iter}
        )

    async def pca(self, data: list[list[float]], n_components: int) -> dict[str, Any]:
        return await self._client._send(
            "DsPca", {"data": data, "n_components": n_components}
        )

    async def compute_stats(self, data: list[list[float]]) -> dict[str, Any]:
        return await self._client._send("DsComputeStats", {"data": data})

    async def train_test_split(
        self,
        data: list[list[float]],
        labels: list[float],
        test_ratio: float = 0.2,
        shuffle: bool = True,
        seed: int = 42,
    ) -> dict[str, Any]:
        return await self._client._send(
            "DsTrainTestSplit",
            {
                "data": data,
                "labels": labels,
                "test_ratio": test_ratio,
                "shuffle": shuffle,
                "seed": seed,
            },
        )

    async def fit_estimator(
        self,
        estimator: str,
        x: list[list[float]],
        y: list[float],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fit a regression estimator (ridge/lasso/elasticnet/decisiontree/
        randomforest/gradientboosting/adaboost/svr). Returns a serializable
        fitted-model blob to pass back to ``predict_estimator``."""
        return await self._client._send(
            "DsFitEstimator",
            {"estimator": estimator, "x": x, "y": y, "params": params or {}},
        )

    async def predict_estimator(
        self, model: dict[str, Any], x: list[list[float]]
    ) -> list[float]:
        """Predict with a model blob returned by ``fit_estimator``."""
        return await self._client._send("DsPredictEstimator", {"model": model, "x": x})

    # ── Training loss / optimizer kernels (CONCEPT:EG-KG.compute.rust-native-training-loss) ──────────────────
    # The Rust performance path for the in-house training substrate (Wave C / C1),
    # mirroring data-science-mcp `trainers/objectives.py`. Batch a step over the
    # wire instead of marshalling per element.

    async def softmax(
        self, logits: list[float], temperature: float = 1.0
    ) -> list[float]:
        """Numerically-stable softmax with temperature."""
        return await self._client._send(
            "DsSoftmax", {"logits": logits, "temperature": temperature}
        )

    async def log_softmax(self, logits: list[float]) -> list[float]:
        """Numerically-stable log-softmax."""
        return await self._client._send("DsLogSoftmax", {"logits": logits})

    async def cross_entropy(
        self, logits: list[list[float]], labels: list[int]
    ) -> dict[str, Any]:
        """Mean categorical cross-entropy → ``{loss, grad}`` (grad = softmax−onehot)."""
        return await self._client._send(
            "DsCrossEntropy", {"logits": logits, "labels": labels}
        )

    async def dpo_loss(
        self,
        policy_chosen: list[float],
        policy_rejected: list[float],
        ref_chosen: list[float],
        ref_rejected: list[float],
        beta: float = 0.1,
    ) -> dict[str, Any]:
        """Bradley-Terry DPO loss → ``{loss, grad_chosen, grad_rejected}``."""
        return await self._client._send(
            "DsDpoLoss",
            {
                "policy_chosen": policy_chosen,
                "policy_rejected": policy_rejected,
                "ref_chosen": ref_chosen,
                "ref_rejected": ref_rejected,
                "beta": beta,
            },
        )

    async def grpo_surrogate(
        self,
        logprob: list[float],
        old_logprob: list[float],
        advantage: list[float],
        clip_eps: float = 0.2,
    ) -> dict[str, Any]:
        """GRPO clipped surrogate (loss to minimise) → ``{loss, grad}``."""
        return await self._client._send(
            "DsGrpoSurrogate",
            {
                "logprob": logprob,
                "old_logprob": old_logprob,
                "advantage": advantage,
                "clip_eps": clip_eps,
            },
        )

    async def kl_divergence(
        self, logprob: list[float], ref_logprob: list[float]
    ) -> float:
        """Schulman k3 low-variance KL estimate (≥0)."""
        return await self._client._send(
            "DsKlDivergence", {"logprob": logprob, "ref_logprob": ref_logprob}
        )

    async def adam_step(
        self,
        params: list[float],
        grads: list[float],
        *,
        lr: float,
        t: int,
        m: list[float] | None = None,
        v: list[float] | None = None,
        beta1: float = 0.9,
        beta2: float = 0.999,
        eps: float = 1e-8,
    ) -> dict[str, Any]:
        """One Adam step with bias correction → ``{params, m, v}``."""
        return await self._client._send(
            "DsAdamStep",
            {
                "params": params,
                "grads": grads,
                "m": m or [],
                "v": v or [],
                "lr": lr,
                "beta1": beta1,
                "beta2": beta2,
                "eps": eps,
                "t": t,
            },
        )

    async def sgd_step(
        self, params: list[float], grads: list[float], lr: float
    ) -> list[float]:
        """One plain SGD step ``params − lr·grads``."""
        return await self._client._send(
            "DsSgdStep", {"params": params, "grads": grads, "lr": lr}
        )


class MiningClient:
    """CONCEPT:EG-KG.mining.frequent-itemset-mining — Data-mining Namespace.

    Descriptive, pattern-oriented mining that runs compute-near-data over the
    resident graph. Phase 1 exposes association-rule mining; later phases add
    ``cluster``/``anomaly``/``sequence``/``forecast``/``subgraph`` onto this same
    subclient. Mirrors the ``graph_mine`` MCP verb + the ``/api/mining/*`` REST twin.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def associate(
        self,
        transactions: list[list[str]] | None = None,
        *,
        source: dict[str, Any] | None = None,
        min_support: float = 0.1,
        min_confidence: float = 0.5,
        algorithm: str = "fpgrowth",
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Mine association rules (support / confidence / lift).

        Provide EITHER explicit ``transactions`` (each a set of item labels) OR a
        graph-derived ``source`` spec — ``{"node_label", "direction", "item_field",
        "relation", "limit"}`` — that turns node neighborhoods into transactions
        (mine directly over resident graph data). ``algorithm`` is one of
        ``fpgrowth`` (default) / ``apriori`` / ``eclat`` (all agree). With
        ``writeback=True`` each rule is materialized as a typed ``:AssociationRule``
        node linked to its item nodes. Returns
        ``{rules: [{antecedent, consequent, support, confidence, lift}], ...}``.
        """
        # Key insertion order here matters, not just presence: the server signs
        # the `eg2.` MAC over a hash of the params as ITS `Method::MineAssociate`
        # struct re-serializes them (`rmp_serde::to_vec_named`, which always
        # emits fields in Rust declaration order -- transactions, source,
        # min_support, min_confidence, algorithm, writeback), while this
        # client's own canonical hash packs the dict in PYTHON insertion order.
        # Building this out of declaration order used to fail every associate()
        # call with "Authentication failed" even after `source`/`transactions`
        # were each individually present-or-omitted correctly on both sides.
        params: dict[str, Any] = {}
        if transactions is not None:
            params["transactions"] = transactions
        if source is not None:
            params["source"] = source
        params["min_support"] = min_support
        params["min_confidence"] = min_confidence
        params["algorithm"] = algorithm
        params["writeback"] = writeback
        return await self._client._send("MineAssociate", params)

    async def cluster(
        self,
        features: list[list[float]] | None = None,
        *,
        source: dict[str, Any] | None = None,
        plan: list[dict[str, Any]] | None = None,
        algorithm: str = "dbscan",
        eps: float = 0.5,
        min_pts: int = 5,
        k: int = 3,
        linkage: str = "average",
        max_iter: int = 100,
        seed: int = 0,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Cluster a feature matrix (CONCEPT:EG-KG.mining.dbscan-density).

        Provide explicit ``features``, a graph-derived ``source`` spec —
        ``{"node_label", "limit"}`` — that gathers the stored embeddings of a node
        label as the rows (the cross-modal "cluster the vectors of these nodes"
        hook), OR a fused upstream retrieval ``plan`` (CONCEPT:EG-KG.mining.fused-plan-source
        — the SAME externally-tagged ``Op`` list :meth:`unified_query` takes, e.g.
        ``[{"Scan": {"label": "Doc"}}, {"Rank": {"query": [...]}}, {"Limit":
        {"k": 50}}]``): the plan runs FIRST over the resident graph/vector/SQL/time
        modalities, compute-near-data, and each resulting row's stored embedding
        becomes a feature row — so ``retrieve → cluster → writeback`` is ONE round
        trip, no client marshalling between retrieve and mine. Precedence:
        ``features`` > ``plan`` > ``source``. ``algorithm`` is one of ``dbscan``
        (default) / ``hierarchical`` / ``gmm`` / ``kmedoids``; DBSCAN uses
        ``eps``/``min_pts``, the rest use ``k`` (hierarchical also ``linkage`` ∈
        ``single|complete|average``; GMM/k-medoids use ``max_iter``, GMM also
        ``seed``). With ``writeback=True`` each non-noise cluster is materialized as
        a typed ``:Cluster`` node linked to its member nodes. Returns
        ``{clusters: [{cluster_id, members, centroid, score}], labels, ...}`` (GMM
        also returns ``responsibilities``).
        """
        params: dict[str, Any] = {
            "algorithm": algorithm,
            "eps": eps,
            "min_pts": min_pts,
            "k": k,
            "linkage": linkage,
            "max_iter": max_iter,
            "seed": seed,
            "writeback": writeback,
        }
        if features is not None:
            params["features"] = features
        if plan is not None:
            params["plan"] = {"ops": plan}
        if source is not None:
            params["source"] = source
        return await self._client._send("MineCluster", params)

    async def anomaly(
        self,
        features: list[list[float]] | None = None,
        *,
        values: list[float] | None = None,
        source: dict[str, Any] | None = None,
        plan: list[dict[str, Any]] | None = None,
        algorithm: str = "zscore",
        k: int = 20,
        n_trees: int = 100,
        sample_size: int = 256,
        seed: int = 0,
        nu: float = 0.1,
        gamma: float = 0.0,
        kernel: str = "rbf",
        threshold: float | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Detect anomalies / outliers in a feature matrix (CONCEPT:EG-KG.mining.isolation-forest).

        Provide an explicit ``features`` matrix, a 1-D ``values`` series (each
        scalar becomes one row — the tsdb root-cause path), a graph-derived
        ``source`` (node embeddings), OR a fused upstream retrieval ``plan``
        (CONCEPT:EG-KG.mining.fused-plan-source, same shape as :meth:`unified_query`'s —
        e.g. TsScan/Traverse/Rank a candidate set, then anomaly-detect it in one
        round trip). Precedence: ``features`` > ``values`` > ``plan`` > ``source``.
        ``algorithm`` is one of ``zscore`` (default,
        robust MAD) / ``isoforest`` (Isolation Forest — ``n_trees``, ``sample_size``,
        ``seed``) / ``lof`` (Local Outlier Factor — ``k`` neighbors) / ``ocsvm``
        (One-Class SVM — ``nu``, ``kernel`` ∈ ``rbf|linear``, ``gamma``). Rows over
        ``threshold`` (per-algorithm default when ``None``) are flagged. With
        ``writeback=True`` each flagged row is materialized as a typed ``:Anomaly``
        node linked to its source node. Returns
        ``{rows: [{id, anomaly_score, is_anomaly}], n_anomalies, threshold, ...}``.
        """
        params: dict[str, Any] = {
            "algorithm": algorithm,
            "k": k,
            "n_trees": n_trees,
            "sample_size": sample_size,
            "seed": seed,
            "nu": nu,
            "gamma": gamma,
            "kernel": kernel,
            "writeback": writeback,
        }
        if threshold is not None:
            params["threshold"] = threshold
        if features is not None:
            params["features"] = features
        if values is not None:
            params["values"] = values
        if plan is not None:
            params["plan"] = {"ops": plan}
        if source is not None:
            params["source"] = source
        return await self._client._send("MineAnomaly", params)

    async def classify_fit(
        self,
        x: list[list[float]] | None = None,
        y: list[int] | None = None,
        *,
        source: dict[str, Any] | None = None,
        plan: list[dict[str, Any]] | None = None,
        algorithm: str = "gaussiannb",
        k: int = 5,
        alpha: float = 1.0,
        lr: float = 0.1,
        epochs: int = 300,
        l2: float = 0.0,
        c: float = 1.0,
    ) -> dict[str, Any]:
        """Fit a classifier (PREDICTIVE) → a serializable model blob (CONCEPT:EG-KG.mining.naive-bayes).

        Provide an explicit ``x`` feature matrix, a graph-derived ``source`` spec —
        ``{"node_label", "limit"}`` — (node embeddings + ontology features), OR a
        fused upstream retrieval ``plan`` (CONCEPT:EG-KG.mining.fused-plan-source, same
        shape as :meth:`unified_query`'s), plus integer ``y`` labels (one per row —
        must align by position with the plan's resulting row order). ``algorithm``
        is one of ``gaussiannb`` (default) / ``multinomialnb`` / ``knn`` (``k``
        neighbors) / ``logistic`` (``lr``, ``epochs``, ``l2``) / ``svc`` (``c``,
        ``epochs``, ``lr``). Returns ``{model, classes, n_samples, ...}``; pass the
        returned ``model`` back to :meth:`classify_predict`. Read-only (no graph
        mutation).
        """
        params: dict[str, Any] = {
            "y": y or [],
            "algorithm": algorithm,
            "k": k,
            "alpha": alpha,
            "lr": lr,
            "epochs": epochs,
            "l2": l2,
            "c": c,
        }
        if x is not None:
            params["x"] = x
        if plan is not None:
            params["plan"] = {"ops": plan}
        if source is not None:
            params["source"] = source
        return await self._client._send("MineClassifyFit", params)

    async def classify_predict(
        self,
        model: dict[str, Any],
        x: list[list[float]] | None = None,
        *,
        source: dict[str, Any] | None = None,
        plan: list[dict[str, Any]] | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Predict labels + probabilities from a fitted ``model`` (CONCEPT:EG-KG.mining.naive-bayes).

        ``model`` is the blob returned by :meth:`classify_fit`. Rows come from an
        explicit ``x`` matrix, a graph-derived ``source`` (node embeddings — the
        cross-modal "classify these nodes" hook), OR a fused upstream retrieval
        ``plan`` (CONCEPT:EG-KG.mining.fused-plan-source). With ``writeback=True`` each
        prediction is materialized as a typed ``:Classification`` node linked to its
        source node. Returns ``{rows: [{id, label, proba}], classes, ...}``.
        """
        params: dict[str, Any] = {"model": model, "writeback": writeback}
        if x is not None:
            params["x"] = x
        if plan is not None:
            params["plan"] = {"ops": plan}
        if source is not None:
            params["source"] = source
        return await self._client._send("MineClassifyPredict", params)

    async def reduce(
        self,
        x: list[list[float]] | None = None,
        *,
        source: dict[str, Any] | None = None,
        plan: list[dict[str, Any]] | None = None,
        labels: list[int] | None = None,
        algorithm: str = "svd",
        n_components: int = 2,
        n_neighbors: int = 15,
        min_dist: float = 0.1,
        perplexity: float = 30.0,
        epochs: int = 300,
        lr: float = 100.0,
        seed: int = 0,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Reduce a feature matrix to low-D coords (DESCRIPTIVE, CONCEPT:EG-KG.mining.truncated-svd).

        Provide an explicit ``x`` matrix, a graph-derived ``source`` (node
        embeddings — "reduce these node vectors for the graphviz"), OR a fused
        upstream retrieval ``plan`` (CONCEPT:EG-KG.mining.fused-plan-source, same shape
        as :meth:`unified_query`'s — e.g. vector-``Rank`` a neighborhood, then
        reduce it for the graphviz in one round trip). ``algorithm`` is
        one of ``svd`` (default, truncated SVD) / ``lda`` (supervised — needs
        ``labels``) / ``umap`` (``n_neighbors``, ``min_dist``, ``epochs``, ``seed``) /
        ``tsne`` (``perplexity``, ``epochs``, ``lr``, ``seed``); ``n_components`` sets
        the target dimensionality. With ``writeback=True`` each row's reduced vector is
        materialized as a typed ``:Embedding2D`` node linked to its source node.
        Returns ``{rows: [{id, coords}], n_components, ...}`` (svd also returns
        ``singular_values``). UMAP/t-SNE are approximate + small-N by design.
        """
        params: dict[str, Any] = {
            "algorithm": algorithm,
            "n_components": n_components,
            "n_neighbors": n_neighbors,
            "min_dist": min_dist,
            "perplexity": perplexity,
            "epochs": epochs,
            "lr": lr,
            "seed": seed,
            "writeback": writeback,
        }
        if x is not None:
            params["x"] = x
        if plan is not None:
            params["plan"] = {"ops": plan}
        if source is not None:
            params["source"] = source
        if labels is not None:
            params["labels"] = labels
        return await self._client._send("MineReduce", params)

    async def sequence(
        self,
        sequences: list[list[str]] | None = None,
        *,
        source: dict[str, Any] | None = None,
        min_support: float = 0.1,
        algorithm: str = "prefixspan",
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Mine frequent sequential patterns (CONCEPT:EG-KG.mining.prefixspan — Phase 4).

        Provide EITHER explicit ``sequences`` (each a time-ordered list of item
        labels — an item may repeat) OR a graph-derived ``source`` spec —
        ``{"node_label", "direction", "item_field", "relation", "limit"}`` —
        that turns each node's ordered neighbor list (chronological edge order)
        into one sequence (the "what reliably follows what" hook: evolution/
        commit timelines, event streams). ``algorithm`` is one of ``prefixspan``
        (default) / ``gsp`` (both agree — GSP is the sequence analog of Apriori,
        PrefixSpan a projection-based no-candidate-generation engine). With
        ``writeback=True`` each pattern is materialized as a typed
        ``:SequentialPattern`` node linked to its resident item nodes. Returns
        ``{patterns: [{items, support, count}], n_sequences, n_patterns, ...}``.
        """
        params: dict[str, Any] = {
            "min_support": min_support,
            "algorithm": algorithm,
            "writeback": writeback,
        }
        if sequences is not None:
            params["sequences"] = sequences
        if source is not None:
            params["source"] = source
        return await self._client._send("MineSequence", params)

    async def forecast(
        self,
        values: list[float],
        *,
        algorithm: str = "arima",
        horizon: int = 10,
        p: int = 1,
        d: int = 1,
        q: int = 0,
        period: int = 0,
        alpha: float = 0.3,
        beta: float = 0.1,
        gamma: float = 0.1,
        confidence: float = 0.95,
        series_id: str = "",
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Forecast `horizon` future points from a 1-D series (CONCEPT:EG-KG.mining.arima — Phase 4).

        `values` is a tsdb window handed in by the caller (mirrors
        :meth:`anomaly`'s client-supplied ``values`` cut). ``algorithm`` is one
        of ``arima`` (default — Hannan-Rissanen AR(``p``)/MA(``q``) after
        ``d``-order differencing) / ``holtwinters`` (additive level/trend/
        seasonal exponential smoothing — ``alpha``/``beta``/``gamma``,
        seasonal ``period``; degrades to Holt linear-trend when ``period`` is
        0) / ``stl`` (classical decomposition + trend/seasonal extrapolation,
        also returns ``trend``/``seasonal``/``residual``). ``confidence`` sets
        the two-sided forecast-band level (e.g. ``0.95``). With
        ``writeback=True`` the forecast is materialized as a typed
        ``:Forecast`` node — linked to a resident node named ``series_id``
        when one exists. Returns ``{forecast, lower, upper, horizon, ...}``.
        """
        params: dict[str, Any] = {
            "values": values,
            "algorithm": algorithm,
            "horizon": horizon,
            "p": p,
            "d": d,
            "q": q,
            "period": period,
            "alpha": alpha,
            "beta": beta,
            "gamma": gamma,
            "confidence": confidence,
            "series_id": series_id,
            "writeback": writeback,
        }
        return await self._client._send("MineForecast", params)

    async def text(
        self,
        docs: list[list[str]] | None = None,
        *,
        source: dict[str, Any] | None = None,
        algorithm: str = "tfidf",
        k: int = 3,
        alpha: float = 0.1,
        beta: float = 0.01,
        iterations: int = 200,
        seed: int = 0,
        top_n: int = 10,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Mine a text corpus: TF-IDF or topic modeling (CONCEPT:EG-KG.mining.tfidf — Phase 4).

        Provide EITHER explicit `docs` (each a pre-tokenized ``list[str]`` —
        e.g. lowercased words) OR a graph-derived ``source`` spec —
        ``{"node_label", "field", "limit"}`` — that tokenizes a text property
        off a node label (compute-near-data, no Tantivy/eg-text dependency).
        ``algorithm`` is one of ``tfidf`` (default — descriptive per-document
        term weights, read-only) / ``lda`` (Latent Dirichlet Allocation via
        collapsed Gibbs sampling — ``alpha``/``beta`` priors, ``iterations``
        sweeps) / ``nmf`` (Non-negative Matrix Factorization by multiplicative
        updates on the TF-IDF matrix). ``k`` sets the topic count for
        ``lda``/``nmf``; ``top_n`` caps how many terms are kept per
        document/topic row. With ``writeback=True`` (``lda``/``nmf`` only)
        each topic is materialized as a typed ``:Topic`` node, linked
        ``HAS_TOPIC`` from every resident document whose DOMINANT topic it is.
        Returns ``{doc_terms: [...]}`` (tfidf) or ``{topics: [...],
        doc_topics: [...]}`` (lda/nmf).
        """
        params: dict[str, Any] = {
            "algorithm": algorithm,
            "k": k,
            "alpha": alpha,
            "beta": beta,
            "iterations": iterations,
            "seed": seed,
            "top_n": top_n,
            "writeback": writeback,
        }
        if docs is not None:
            params["docs"] = docs
        if source is not None:
            params["source"] = source
        return await self._client._send("MineText", params)

    async def subgraph(
        self,
        *,
        label: str | None = None,
        min_support: float = 0.1,
        max_edges: int = 3,
        algorithm: str = "gspan",
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Frequent subgraph mining + motif counting (CONCEPT:EG-KG.mining.gspan-frequent-subgraph — Phase 4).

        UNLIKE every other ``mining`` method, this one mines the RESIDENT
        GRAPH's own topology directly — no rows/vectors to pass in. ``label``,
        when given, restricts the scanned host graph to nodes of that ONE
        type (``None`` scans the whole resident graph heterogeneously).
        ``algorithm`` is one of ``gspan`` (default — level-wise frequent
        connected-subgraph pattern growth up to ``max_edges`` edges,
        canonicalized + exactly re-counted; ``min_support`` is a fraction of
        the host's total edge count) or ``motif`` (a label-agnostic
        topological census: open wedges, triangles, directed 3-cycles — reads
        ``min_support``/``max_edges`` are ignored). With ``writeback=True``
        (``gspan`` only) each frequent pattern is materialized as a typed
        ``:FrequentSubgraph`` node linked to every host node in any of its
        embeddings. Returns ``{patterns: [{nodes, edges, support, count}],
        ...}`` (gspan) or ``{motifs: {wedge, triangle, directed_cycle3}, ...}``
        (motif).
        """
        params: dict[str, Any] = {
            "min_support": min_support,
            "max_edges": max_edges,
            "algorithm": algorithm,
            "writeback": writeback,
        }
        if label is not None:
            params["label"] = label
        return await self._client._send("MineSubgraph", params)

    async def entity_resolve(
        self,
        records: list[list[str]] | None = None,
        *,
        block_keys: list[str] | None = None,
        vectors: list[list[float]] | None = None,
        source: dict[str, Any] | None = None,
        ids: list[str] | None = None,
        bucket_precision: int = 1,
        threshold: float = 0.5,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Entity resolution / record linkage (CONCEPT:EG-KG.mining.entity-resolution).

        Provide EITHER ``records`` (token-attribute rows for Jaccard record linkage,
        blocked by the parallel ``block_keys``) OR embedding rows — explicit
        ``vectors`` or a graph-derived ``source`` spec (``{"node_label", "limit"}``,
        same shape as :meth:`cluster`'s ``source``) — for cosine entity resolution,
        blocked by a ``bucket_precision``-rounded grid. ``ids`` optionally names the
        explicit rows (``records``/``vectors``); a graph-derived ``source`` supplies
        its own resident node ids. ``threshold`` is the minimum similarity (Jaccard or
        cosine, ``[0,1]``) to emit a match. With ``writeback=True`` each match is
        materialized as a typed ``:EntityMatch`` node linked to both members (when
        resident node ids). Returns ``{matches: [{left, right, score}], ...}``.
        """
        params: dict[str, Any] = {
            "bucket_precision": bucket_precision,
            "threshold": threshold,
            "writeback": writeback,
        }
        if records is not None:
            params["records"] = records
        if block_keys is not None:
            params["block_keys"] = block_keys
        if vectors is not None:
            params["vectors"] = vectors
        if source is not None:
            params["source"] = source
        if ids is not None:
            params["ids"] = ids
        return await self._client._send("MineEntityResolve", params)

    async def causal_impact(
        self,
        series: list[float],
        *,
        control: list[float] | None = None,
        intervention_index: int = 0,
        series_id: str | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Causal impact estimation (CONCEPT:EG-KG.mining.causal-impact): interrupted
        time series (``series`` alone) or difference-in-differences (``series`` +
        non-empty ``control``), split at ``intervention_index`` (the first
        post-intervention observation, in BOTH series for DiD). With
        ``writeback=True`` materializes the estimate as a typed ``:CausalEffect``
        node (``series_id`` names it; empty ⇒ derived from the input)."""
        params: dict[str, Any] = {
            "series": series,
            "intervention_index": intervention_index,
            "writeback": writeback,
        }
        if control is not None:
            params["control"] = control
        if series_id is not None:
            params["series_id"] = series_id
        return await self._client._send("MineCausalImpact", params)

    async def process(
        self,
        traces: list[list[str]],
        *,
        process_id: str | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Process mining (CONCEPT:EG-KG.mining.process-mining): directly-follows graph
        + alpha-miner-lite footprint (causal/parallel/choice relations, start/end
        activity sets) over ordered event ``traces`` (each a time-ordered activity
        sequence; an activity may repeat within a trace). With ``writeback=True``
        materializes the footprint as a typed ``:ProcessModel`` node (``process_id``
        names it; empty ⇒ derived from the mined footprint's own shape)."""
        params: dict[str, Any] = {"traces": traces, "writeback": writeback}
        if process_id is not None:
            params["process_id"] = process_id
        return await self._client._send("MineProcess", params)

    async def root_cause(
        self,
        nodes: list[str],
        scores: list[float],
        edges: list[tuple[str, str, float]],
        symptom: str,
        *,
        max_hops: int = 5,
        decay: float = 0.85,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Root-cause propagation (CONCEPT:EG-KG.mining.root-cause): given a directed
        weighted dependency graph ``edges`` (``(cause_id, effect_id, weight)``) and a
        per-node anomaly ``scores`` vector (index-aligned with ``nodes`` — e.g.
        :meth:`anomaly`'s own output), find the most-likely upstream root cause of the
        already-flagged ``symptom`` node. ``max_hops`` caps search depth; ``decay`` is
        the per-hop score decay ``(0,1]`` (mirrors PageRank's damping). With
        ``writeback=True`` materializes the top candidate as a typed ``:RootCause``
        node linked to the symptom."""
        params: dict[str, Any] = {
            "nodes": nodes,
            "scores": scores,
            "edges": [list(e) for e in edges],
            "symptom": symptom,
            "max_hops": max_hops,
            "decay": decay,
            "writeback": writeback,
        }
        return await self._client._send("MineRootCause", params)

    async def risk_propagation(
        self,
        nodes: list[str],
        seed: list[float],
        edges: list[tuple[str, str, float]],
        *,
        damping: float = 0.85,
        tolerance: float = 1e-9,
        max_iterations: int = 100,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Seeded risk propagation (CONCEPT:EG-KG.mining.risk-propagation): personalized
        PageRank over a directed weighted graph ``edges`` (``(from_id, to_id,
        weight)``), restarting to the ``seed`` risk distribution (index-aligned with
        ``nodes``; any non-negative scale, normalized internally) instead of
        teleporting uniformly. With ``writeback=True`` materializes each node's
        propagated score as a typed ``:RiskScore`` node."""
        params: dict[str, Any] = {
            "nodes": nodes,
            "seed": seed,
            "edges": [list(e) for e in edges],
            "damping": damping,
            "tolerance": tolerance,
            "max_iterations": max_iterations,
            "writeback": writeback,
        }
        return await self._client._send("MineRiskPropagation", params)

    async def ontology_gap(
        self,
        *,
        label: str | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Ontology-gap detection (CONCEPT:EG-KG.mining.ontology-gap): scans the
        resident graph's own type/relation-tagged class nodes (graph-native — no
        ``rdf``/OWL-reasoner dependency) for completeness gaps: no declared
        properties, an unresolved ``subClassOf`` parent (an orphan subclass), or a
        fully disconnected class. ``label`` restricts the scan to class nodes of that
        one type (``None`` ⇒ every node whose type is ``Class``/``OwlClass``). With
        ``writeback=True`` materializes each gap as a typed ``:OntologyGap`` node
        linked to its class."""
        params: dict[str, Any] = {"writeback": writeback}
        if label is not None:
            params["label"] = label
        return await self._client._send("MineOntologyGap", params)

    async def retrieval_quality(
        self,
        traces: list[dict[str, Any]],
        *,
        k: int = 0,
        query_id: str | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Retrieval-quality evaluation (CONCEPT:EG-KG.mining.retrieval-quality):
        precision@k / recall@k / MRR over stored retrieval ``traces`` (each
        ``{"retrieved": [id, ...], "relevant": [id, ...]}``). ``k`` is the cutoff
        (``0`` ⇒ each trace's full retrieved list). With ``writeback=True``
        materializes the aggregate report as a typed ``:RetrievalQuality`` node
        (``query_id`` names it; empty ⇒ derived from the input traces)."""
        params: dict[str, Any] = {"traces": traces, "k": k, "writeback": writeback}
        if query_id is not None:
            params["query_id"] = query_id
        return await self._client._send("MineRetrievalQuality", params)

    async def community(
        self,
        *,
        label: str | None = None,
        algorithm: str = "louvain",
        resolution: float = 1.0,
        max_iterations: int = 100,
        seed: int = 0,
        weighted: bool = True,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Community detection as a mining family (CONCEPT:EG-KG.mining.community-writeback):
        wraps the EXISTING GDS Louvain / label-propagation kernels (no new
        algorithm, only the epistemic writeback). Runs over the resident graph,
        optionally restricted to one node ``label`` (like :meth:`subgraph`).
        ``algorithm`` is ``"louvain"`` (default, uses ``resolution`` + a
        ``seed``-ed deterministic shuffle) or ``"labelprop"`` (uses ``weighted``
        to weight neighbor votes by edge weight); both use ``max_iterations`` as
        their sweep cap. With ``writeback=True`` materializes each community as a
        typed ``:Community`` node linked to its members."""
        params: dict[str, Any] = {
            "algorithm": algorithm,
            "resolution": resolution,
            "max_iterations": max_iterations,
            "seed": seed,
            "weighted": weighted,
            "writeback": writeback,
        }
        if label is not None:
            params["label"] = label
        return await self._client._send("MineCommunity", params)


class GraphLearnClient:
    """CONCEPT:EG-KG.graphlearn.link-predictor — Graph-learning / neuro-symbolic Namespace.

    A pure-Rust KAN (Kolmogorov-Arnold) link-predictor learned over the resident
    graph. Unlike a black-box scorer, its learned per-feature edge functions ARE
    queryable KG artifacts (``:EdgeFunction`` nodes), so *why* two nodes are predicted
    linked is answerable from Cypher/SQL. Mirrors the ``graph_learn`` MCP verb + the
    ``/api/graphlearn/*`` REST twin. Heavy multi-layer KAN-GNN training stays a
    data-science-mcp/torch job whose distilled outputs flow back through this seam.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def fit(
        self,
        node_label: str,
        *,
        direction: str = "any",
        relation: str | None = None,
        limit: int = 0,
        basis: str = "chebyshev",
        degree: int = 4,
        hidden: int = 0,
        epochs: int = 200,
        lr: float = 0.05,
        neg_ratio: float = 1.0,
        seed: int = 42,
        alpha: float = 0.5,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Fit a KAN link-predictor over a graph-derived subgraph (CONCEPT:EG-KG.graphlearn.link-predictor).

        The subgraph is every node carrying ``node_label``; edges among them (following
        ``direction`` ∈ ``any|out|in``, optionally filtered to ``relation``) are the
        positive links; non-edges are sampled negatives. ``basis`` is ``chebyshev``
        (default) or ``jacobi``; ``degree`` is the polynomial degree per edge function;
        ``hidden=0`` (default) gives a single interpretable layer (one ``KanEdgeFn`` per
        structural feature). With ``writeback=True`` each learned per-feature curve is
        materialized as a typed ``:EdgeFunction`` node. Returns
        ``{model, n_nodes, n_edges, train_auc, edge_functions: [{feature, coefficients}], ...}``.
        The returned ``model`` blob is passed back to :meth:`predict`.
        """
        params: dict[str, Any] = {
            "basis": basis,
            "degree": degree,
            "hidden": hidden,
            "epochs": epochs,
            "lr": lr,
            "neg_ratio": neg_ratio,
            "seed": seed,
            "alpha": alpha,
        }
        source: dict[str, Any] = {
            "node_label": node_label,
            "direction": direction,
            "limit": limit,
        }
        if relation is not None:
            source["relation"] = relation
        return await self._client._send(
            "GraphLearnFit",
            {"source": source, "params": params, "writeback": writeback},
        )

    async def predict(
        self,
        model: dict[str, Any],
        node_label: str,
        *,
        direction: str = "any",
        relation: str | None = None,
        limit: int = 0,
        candidate_pairs: list[tuple[str, str]] | list[list[str]] | None = None,
        top_k: int = 50,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Score candidate links with a fitted model (CONCEPT:EG-KG.graphlearn.predicted-edge-writeback).

        Provide the ``model`` blob returned by :meth:`fit`. Score either explicit
        ``candidate_pairs`` (``[(src, dst), ...]``) or — when omitted — the ``top_k``
        highest-probability MISSING links across the subgraph. With ``writeback=True``
        each scored pair is materialized as a typed ``:PredictedEdge`` node linked to its
        endpoints. Returns ``{predicted: [{src, dst, score}], n_predicted, model, ...}``.
        """
        source: dict[str, Any] = {
            "node_label": node_label,
            "direction": direction,
            "limit": limit,
        }
        if relation is not None:
            source["relation"] = relation
        params: dict[str, Any] = {
            "model": model,
            "source": source,
            "top_k": top_k,
            "writeback": writeback,
        }
        if candidate_pairs is not None:
            params["candidate_pairs"] = [list(p) for p in candidate_pairs]
        return await self._client._send("GraphLearnPredict", params)


class PipelineClient:
    """CONCEPT:EG-KG.mining.ml-pipeline — composable ML pipeline namespace.

    A composable ``train → eval → serve → predict`` lifecycle over a VERSIONED
    ``:Model`` artifact that GENERALIZES the KAN one-off: a ``spec`` of
    ``feature steps → split → a pluggable model family`` (``classify`` for node
    classification, ``estimator`` for regression, ``graphlearn`` for the KAN
    link-predictor). Two trained versions are queryable ``:Model`` nodes and
    comparable by their held-out metrics. Mirrors the ``graph_pipeline`` MCP verb +
    the ``/api/pipeline/*`` REST twin. Heavy/deep model training stays a
    data-science-mcp job; this is the pure-Rust in-engine path.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def train(
        self,
        name: str,
        spec: dict[str, Any],
        *,
        source: dict[str, Any] | None = None,
        x: list[list[float]] | None = None,
        y: list[int] | None = None,
        writeback: bool = True,
    ) -> dict[str, Any]:
        """Fit the pipeline's model over composed features, evaluate on a held-out
        split, and (when ``writeback``) persist a versioned ``:Model`` artifact.

        ``spec`` is ``{features: [...], split: {...}, label_property: "...",
        model: {family, algorithm, params}}``. Build features from a graph-derived
        ``source`` (``{node_label, direction, relation, limit}``) or pass an explicit
        ``x`` matrix; labels come from ``spec.label_property`` (per-node) or explicit
        ``y``. Returns ``{name, version, model_id, family, algorithm, metrics:
        {train, test}, n_train, n_test, ...}``.
        """
        params: dict[str, Any] = {"name": name, "spec": spec, "writeback": writeback}
        if source is not None:
            params["source"] = source
        if x is not None:
            params["x"] = x
        if y is not None:
            params["y"] = y
        return await self._client._send("MiningPipelineTrain", params)

    async def evaluate(
        self,
        name: str,
        *,
        version: int = 0,
        source: dict[str, Any] | None = None,
        x: list[list[float]] | None = None,
        y: list[int] | None = None,
    ) -> dict[str, Any]:
        """Score a stored model (``version``; ``0`` ⇒ the served version) against a
        labeled set, rebuilding features via the model's own recipe. Returns
        ``{name, version, family, metrics, n}``. Read-only.
        """
        params: dict[str, Any] = {"name": name, "version": version}
        if source is not None:
            params["source"] = source
        if x is not None:
            params["x"] = x
        if y is not None:
            params["y"] = y
        return await self._client._send("MiningPipelineEvaluate", params)

    async def serve(self, name: str, version: int) -> dict[str, Any]:
        """Deploy a versioned ``:Model`` as the served version (writes a
        ``:ServedModel`` pointer so :meth:`predict` with ``version=0`` resolves it).
        """
        return await self._client._send(
            "MiningPipelineServe", {"name": name, "version": version}
        )

    async def predict(
        self,
        name: str,
        *,
        version: int = 0,
        source: dict[str, Any] | None = None,
        x: list[list[float]] | None = None,
        writeback: bool = False,
    ) -> dict[str, Any]:
        """Predict with a stored model (``version``; ``0`` ⇒ served). Rebuilds
        features via the model's recipe from ``source`` (or explicit ``x``). With
        ``writeback=True`` each prediction is materialized as a typed ``:Prediction``
        node linked to its source node. Returns ``{model_id, family, rows, n_rows,
        written_back, ...}``.
        """
        params: dict[str, Any] = {
            "name": name,
            "version": version,
            "writeback": writeback,
        }
        if source is not None:
            params["source"] = source
        if x is not None:
            params["x"] = x
        return await self._client._send("MiningPipelinePredict", params)

    async def compare(
        self, name: str, version_a: int, version_b: int
    ) -> dict[str, Any]:
        """Diff two model versions' held-out metrics. Returns ``{name, version_a,
        version_b, metrics_a, metrics_b, diff}`` where ``diff`` is per-metric
        ``b − a``. Read-only.
        """
        return await self._client._send(
            "MiningPipelineCompare",
            {"name": name, "version_a": version_a, "version_b": version_b},
        )


# Per-RPC timeouts (CONCEPT:EG-KG.query.wire-protocol). A wedged or overloaded engine must never
# hang a caller forever — every request is bounded. Normal CRUD uses the short
# default; known-heavy ops (full-graph parse/scan/algorithms) get a generous
# budget so a legitimately long job is not aborted. Both are overridable per
# client or via env; set the timeout to 0/None to disable (not recommended).
_DEFAULT_RPC_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_RPC_TIMEOUT", "60") or 60)
_HEAVY_RPC_TIMEOUT = float(
    os.environ.get("GRAPH_SERVICE_HEAVY_RPC_TIMEOUT", "1200") or 1200
)
#: Establishing the socket connection must also be bounded — a peer that accepts
#: the connection but never completes the handshake would otherwise hang the
#: caller forever (the connect path is outside the per-RPC read budget).
_CONNECT_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_CONNECT_TIMEOUT", "10") or 10)
#: Flushing a request must be bounded INDEPENDENTLY of (and no longer than) the
#: read budget. A healthy engine drains a local socket in microseconds; a write
#: that backs up means the engine has stopped reading (wedged) — detect that in
#: seconds even for a "heavy" method whose *response* may legitimately take long.
_WRITE_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_WRITE_TIMEOUT", "30") or 30)
#: GOC-81 W02: shutdown waits inside `close()` (joining the cancelled reader
#: task, awaiting `writer.wait_closed()`) must also be bounded — a wedged
#: transport must never make `close()` itself hang forever.
_CLOSE_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_CLOSE_TIMEOUT", "5") or 5)
#: Methods whose work is O(graph) / batch-sized and may legitimately run long.
_HEAVY_RPC_METHODS = frozenset(
    {
        "ParseFile",
        "ParseFiles",
        "IndexRepository",
        "CommunityDetection",
        "CommunityDetectEphemeral",
        "ComputeSimilarityEdges",
        "ResolveCandidates",
        "BatchL2Normalize",
        "Vf2SubgraphMatch",
        "BetweennessCentrality",
        "PageRank",
        "PersonalizedPagerank",
        "BatchUpdate",
        "MultiGraphBatchUpdate",
        "FromMsgpack",
        "ToMsgpack",
        "Reconcile",
        "RunDatalogReasoning",
        "GetSubgraph",
        "GetNodes",
        "GetEdges",
        # ML pipeline train/eval/predict compose embeddings + model fit over the whole
        # subgraph (CONCEPT:EG-KG.mining.ml-pipeline) — give them the heavy budget.
        "MiningPipelineTrain",
        "MiningPipelineEvaluate",
        "MiningPipelinePredict",
        "KnowledgeStream",
        "ServedModality",
        # SQL scans the whole node set (CONCEPT:EG-KG.query.read-only-sql-query) — give it the heavy budget.
        "Sql",
        # Cypher MATCH/BFS scans the node set too (CONCEPT:EG-KG.query.dep-free-behind).
        "CypherQuery",
        # A txn commit (CONCEPT:EG-KG.txn.multi-op-occ-acid) applies the whole staged write-set under
        # one lock — a large multi-op commit may legitimately take longer.
        "Commit",
        # An online backup (CONCEPT:EG-KG.sharding.reshard-on-restore) streams a per-shard MVCC snapshot verbatim to
        # a bundle dir — give it the heavy budget. Restore stages a rebuilt copy likewise.
        "Backup",
        "Restore",
    }
)


class QueryClient:
    """CONCEPT:EG-KG.query.read-only-sql-query — Read-only SQL Query Namespace.

    ``SELECT ... FROM nodes WHERE ... LIMIT ...`` over the connection's graph,
    served by the DataFusion surface included in the mandatory main build.
    Schema-on-read: node property keys become columns; a raw
    ``props`` blob column plus ``json_get(props, key)`` /
    ``json_get_f64`` / ``json_get_i64`` UDFs reach fields the inferred schema
    widened or dropped.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def sql(
        self, query: str, params_msgpack: bytes = b""
    ) -> list[dict[str, Any]]:
        """Run ``query`` and return a list of row dicts keyed by column name.

        The engine returns ``{"columns": [...], "rows": [<msgpack-blob>, ...]}``
        (a ``Raw`` payload the transport already double-unpacks); each row blob is
        a list of cell values aligned to ``columns``. We zip them into dicts so a
        caller gets ordinary records.

        U-144: ``params_msgpack`` is ALWAYS sent explicitly (defaulting to
        ``b""``, matching the wire struct's own default), never omitted. The
        server's HMAC verification recomputes the signed body hash from the
        FULLY DESERIALIZED ``Method::Sql {{ query, params_msgpack }}`` —
        `serde`'s `#[serde(default)]` only relaxes what a DEcode may omit, it
        does not make ENcode skip the field, so `canonical_body_bytes()`
        always includes `params_msgpack` in the map it hashes. Omitting the
        key client-side (as this method used to) signs a *different*,
        shorter map than the one the server reconstructs and re-hashes, so
        the MAC never matches and every call fails closed with the generic
        `"Authentication failed"` -- under the exact same verified session
        that reaches `CypherQuery` fine, because `CypherQuery`'s `query`/
        `mode` fields have no such optional/omittable field for the plain
        Python wrapper to under-specify.
        """
        result = await self._client._send(
            "Sql", {"query": query, "params_msgpack": params_msgpack}
        )
        return self._rows_to_dicts(result)

    async def cypher_read(self, query: str) -> list[dict[str, Any]]:
        """Run a Cypher-subset ``query`` and return a list of row dicts keyed by
        RETURN column.

        ``MATCH (a:Label)-[:REL]->(b:Label2) WHERE a.prop = 'x' RETURN a, b LIMIT
        k`` over the connection's graph (CONCEPT:EG-KG.query.dep-free-behind). On the
        engine side it compiles to the label index / VF2 / BFS and does not invoke
        DataFusion; Cypher is included in the mandatory main build.

        Supports: node ``:Label`` predicates, typed directed edges
        (``-[:REL]->`` / ``<-[:REL]-``), variable-length paths (``-[:REL*1..3]->``),
        ``WHERE`` equality/comparison on properties, ``RETURN`` of bound variables
        and ``var.prop`` accesses, and ``LIMIT``. The result has the SAME
        ``{"columns": [...], "rows": [<msgpack-blob>, ...]}`` shape as ``sql`` (a
        ``Raw`` payload the transport already double-unpacks); each row blob is a
        list of cell values aligned to ``columns``.
        """
        result = await self._client._send(
            "CypherQuery", {"query": query, "mode": "read"}
        )
        return self._rows_to_dicts(result)

    async def cypher_write(self, query: str) -> list[dict[str, Any]]:
        """Execute an explicitly authorized Cypher mutation.

        The engine parses the complete statement and rejects read/write mode
        mismatches before execution. Callers should prefer typed mutation APIs;
        this surface exists for governed query-language mutations.
        """
        result = await self._client._send(
            "CypherQuery", {"query": query, "mode": "write"}
        )
        return self._rows_to_dicts(result)

    async def graphql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Run a GraphQL READ ``query`` and return the GraphQL ``{"data": …}`` dict
        (CONCEPT:EG-KG.query.sparql-completeness).

        The query's root fields are node TYPES (labels) with optional ``first``/
        ``limit`` and property-equality arguments and nested EDGE selections, e.g.::

            { Person(name: "Alice", first: 10) { name KNOWS { name } } }

        On the engine side it is compiled to scans + BFS over the SAME ``GraphView``
        the Cypher executor reads (DEP-FREE — no async-graphql / DataFusion), so a
        GraphQL query returns the SAME nodes/fields as the equivalent Cypher query.
        GraphQL is included in the mandatory main build. Returns the parsed GraphQL
        JSON (a ``Raw`` payload the transport already double-unpacks).

        Mutations / subscriptions / fragments are not supported (read-only surface);
        the engine returns a clear parse error for them.
        """
        return await self._client._send(
            "GraphQl", {"query": query, "variables": variables}
        )

    async def import_sqlite_file(self, path: str) -> dict[str, Any]:
        """Import every user table (+ rows) from logical ``.db`` filename ``path``
        under the configured private transfer root (CONCEPT:EG-KG.query.eg-feature).

        The file is read via the bundled C sqlite3. The ``sqlite-file`` feature is part
        of the mandatory main build. Each table is REPLACED if a same-name table already
        exists, so the import mirrors the file; an imported table is immediately visible
        to :meth:`sql` (``SELECT … FROM <table>``). ONE round-trip reads the whole file.

        Returns aggregate table counts without exposing a host path.
        """
        return await self._client._send("ImportSqliteFile", {"path": path})

    async def export_sqlite_file(
        self, path: str, tables: list[str] | None = None
    ) -> dict[str, Any]:
        """Export user tables to logical ``.db`` filename ``path`` under the configured
        private transfer root (CONCEPT:EG-KG.query.full-protocol).

        ``tables`` ``None``/empty ⇒ every user table; else exactly the named tables (each
        must exist). Publication is private and atomic. Written via the
        bundled C sqlite3 included in the mandatory main build. ONE round-trip per
        table (a single ``scan``), then a bulk sqlite transaction.

        Returns aggregate table counts without exposing a host path.
        """
        return await self._client._send(
            "ExportSqliteFile", {"path": path, "tables": list(tables or [])}
        )

    async def unified(
        self,
        plan: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Run ONE cross-modal plan (CONCEPT:AU-KG.compute.vector/209) and return ranked rows.

        ``plan`` is an ordered list of operator dicts — a CLOSED algebra over a
        shared ``RowSet`` (ordered ids + optional scores). Each op is the
        externally-tagged form of the engine's ``Op`` enum, e.g.::

            [
                {"Scan": {"label": "Doc"}},
                {"Filter": {"preds": [{"GtNum": {"prop": "year", "n": 2024.0}}]}},
                {"Traverse": {"rel": "CITES", "min": 1, "max": 2}},
                {"Rank": {"query": [1.0, 0.0, 0.0, 0.0]}},
                {"Limit": {"k": 10}},
            ]

        The engine sequences the EXISTING legs over one off-lock snapshot —
        ``Filter`` via real DataFusion, ``Traverse`` via petgraph BFS, ``Rank`` via
        the vector kNN — instead of three siloed round-trips (requires a server
        built with the ``query`` feature). The full cost optimizer derives its
        own selectivity and ordering from the plan and current statistics.

        Returns a list of ``{"id": str, "score": float | None}`` rows, in the plan's
        final order (descending score after a ``Rank``).
        """
        result = await self._client._send("UnifiedQuery", {"plan": {"ops": plan}})
        rows = result or []
        return [{"id": id_, "score": score} for id_, score in rows]

    async def uql(
        self,
        text: str,
    ) -> list[dict[str, Any]]:
        """Run a UQL TEXT query (CONCEPT:AU-KG.query.top-nodes-by-degree) — the human/agent-writable
        front-end over :meth:`unified`.

        ``text`` is a UQL pipeline that the engine PARSES into the SAME cross-modal
        ``Plan`` AST :meth:`unified` carries, then runs through the IDENTICAL
        executor (no new execution path). One query expresses filter (relational) +
        traverse (graph) + rank (vector) across modalities, e.g.::

            MATCH (:Doc) WHERE year > 2024
              |> TRAVERSE -[:CITES]->{1,2}
              |> RANK BY ~[1.0, 0.0, 0.0, 0.0]
              |> LIMIT 10

        Grammar (this increment): ``MATCH (:Label) [WHERE preds]`` seeds the scan
        (an inline ``WHERE`` is sugar for a ``|> WHERE`` filter stage); pipeline
        stages are ``TRAVERSE -[:REL]->{min,max}`` (or bare ``TRAVERSE REL{min,max}``;
        ``{n}`` = exactly n hops, absent = 1 hop), ``RANK BY ~[v0, v1, …]`` (an inline
        literal query vector), ``LIMIT k``, and a later-stage ``WHERE``. Predicates are
        ``prop > num`` / ``prop < num`` / ``prop = value`` joined by ``AND``; keywords
        are case-insensitive. The query surface is included in the mandatory main build.

        On a syntax error the engine returns a clear, caret-annotated parse error
        (raised as the transport's error). Returns the same
        ``{"id": str, "score": float | None}`` rows as :meth:`unified`.
        """
        result = await self._client._send("UnifiedQueryText", {"text": text})
        rows = result or []
        return [{"id": id_, "score": score} for id_, score in rows]

    async def explain_plan(self, plan: list[dict[str, Any]]) -> dict[str, Any]:
        """``EXPLAIN PLAN`` (CONCEPT:EG-KG.query.plan-dag) — serialize ``plan`` (the SAME
        externally-tagged ``Op`` list :meth:`unified` takes) as a `PlanDag` both BEFORE
        and AFTER the DAG-aware cost optimizer, plus the active optimizer rule set. No
        execution occurs beyond planning. Returns ``{"before": [{"id", "op", "inputs"},
        ...], "after": [...], "applied_rules": [str, ...]}``.
        """
        return await self._client._send("ExplainPlan", {"plan": {"ops": plan}})

    async def explain_provenance(self, plan: list[dict[str, Any]]) -> dict[str, Any]:
        """``EXPLAIN PROVENANCE`` — run ``plan`` (the SAME plan :meth:`unified` takes)
        and resolve its EVIDENCE-FOR provenance into the schema-generated
        ``EvidenceBundle``. The mandatory main build includes the epistemic resolver;
        ``score``/``confidence``/``valid_time``/``tx_time`` are populated alongside
        any resolved evidence (CONCEPT:EG-KB-CURRENCY).
        """
        return _evidence_bundle(
            await self._client._send("ExplainProvenance", {"plan": {"ops": plan}})
        )

    async def explain_provenance_by_ids(self, ids: list[str]) -> dict[str, Any]:
        """``EXPLAIN PROVENANCE BY IDS`` (CONCEPT:EG-KB-CURRENCY) — the ID-seeded sibling
        of :meth:`explain_provenance`: resolve the SAME per-row epistemic columns
        directly for ``ids``, with no ``Op`` plan needed. This is the seam a caller
        with ids from ANY other read path (a Cypher ``MATCH``, a SQL ``SELECT``, a
        prior :meth:`unified`) uses to "currency-upgrade" a plain id list into
        calibrated, cited, time-versioned claims. Returns the IDENTICAL shape
        :meth:`explain_provenance` does."""
        return _evidence_bundle(
            await self._client._send("ExplainProvenanceByIds", {"ids": ids})
        )

    async def explain_policy(self, plan: list[dict[str, Any]]) -> dict[str, Any]:
        """``EXPLAIN POLICY`` (CONCEPT:EG-KG.sharding.row-level-security) — run ``plan``
        against BOTH the caller's RLS-filtered snapshot and the unfiltered snapshot,
        reporting which rows the policy denied. Returns ``{"visible_ids": [str, ...],
        "policy_denied_ids": [str, ...]}`` — ``policy_denied_ids`` is empty when no
        caller/RLS filtering applies on this connection. Security and query support
        are included in the mandatory main build."""
        return await self._client._send("ExplainPolicy", {"plan": {"ops": plan}})

    async def explain_belief(
        self, node_id: str, disclosure_level: str | None = None
    ) -> dict[str, Any]:
        """``EXPLAIN BELIEF <node_id>`` — the justification tree
        (``eg_epistemic::JustificationGraph``) rooted at ``node_id``.

        With ``disclosure_level=None`` (the default — byte-for-byte the classic
        path), returns the FULL un-flattened tree: ``{"root": {"claim", "rule",
        "confidence", "premises": [<same shape>, ...]}}`` — ``rule`` is one of
        ``"Asserted"``/``"DerivedSupport"``/``"DerivedContradiction"``/
        ``"BayesianUpdate"``.

        ``disclosure_level`` (EPI-P3-4, L51) opts INTO a policy-aware, RLS-redacted
        proof instead — one of ``"Full"``/``"Skeleton"``/``"ExistenceOnly"`` (least to
        most redacted). It is a CAP, never a grant: it can only make the response MORE
        redacted than the caller's own RLS access would already produce, never less
        (e.g. always request ``"ExistenceOnly"`` for a privacy-conscious display
        regardless of who is asking). When set, the response shape changes to
        ``{"level", "existence", "root"}`` — ``existence`` is one of
        ``"Supported"``/``"Contradicted"``/``"Uncertain"``, and ``root`` is the
        (possibly redacted — ``claim: None`` + a ``redaction_label`` for a hidden
        node) tree, or ``None`` entirely at ``"ExistenceOnly"``. The mandatory main
        build includes epistemic reasoning, query, and policy-aware redaction.

        Read-only."""
        params: dict[str, Any] = {"node_id": node_id}
        if disclosure_level is not None:
            params["disclosure_level"] = disclosure_level
        return await self._client._send("ExplainBelief", params)

    async def explain_evidence(self, node_id: str) -> dict[str, Any]:
        """CONCEPT:EG-X1 — resolve ``node_id``'s cited multimodal evidence: walk the
        SAME support/contradiction/attack topology :meth:`explain_belief` walks and
        return every transitively-reachable node that carries a located evidence
        locus (PDF page+box, audio/video interval, SQL row version, code range,
        trace span, …). Returns ``{"citations": [{"evidence_id", "kind", "locus",
        "resolved"}, ...]}`` — ``kind`` is one of ``"Supports"``/``"Contradicts"``/
        ``"Attacks"``. Each ``locus`` contains opaque ``id``/``subject``/policy/
        derivation references plus a tagged numeric or opaque ``address``; it is
        never absent on a returned citation. The evidence graph is included in the
        mandatory main build. Read-only."""
        return await self._client._send("ExplainEvidence", {"node_id": node_id})

    async def epistemic_status(self, node_id: str) -> dict[str, Any]:
        """CONCEPT:EPI-P3-5 — the acceptance-query capstone: for ``node_id`` return
        "what do we believe, why, on exactly which evidence, under whose authority,
        at what time, with what uncertainty, and what would invalidate it" in one
        typed call (``eg_epistemic::epistemic_status``). Returns an
        ``EpistemicStatusResult`` (belief + evidence + authority + valid/tx time +
        uncertainty + proof + minimal-flip invalidation set). The epistemic TMS is
        included in the mandatory main build. Read-only."""
        return await self._client._send("EpistemicStatus", {"node_id": node_id})

    async def what_changed(self, tx_from: int, tx_to: int) -> dict[str, Any]:
        """CONCEPT:EPI-P3-5 — between two transaction times, which beliefs changed and
        why (``eg_epistemic::what_changed``) — a whole-graph temporal DIFF, distinct
        from :meth:`epistemic_status`'s single-claim view. Returns a
        ``WhatChangedResult``. The epistemic TMS is included in the mandatory main
        build. Read-only."""
        return await self._client._send(
            "WhatChanged", {"tx_from": tx_from, "tx_to": tx_to}
        )

    async def recompute_materialization(
        self, derived_id: str, expected_source_graph_version: int
    ) -> dict[str, Any]:
        """Fenced recompute/writeback for a stale materialization. The expected
        version must match the durable per-graph reasoning projection watermark.
        Provenance is resolved from the authoritative graph post-image and the
        refreshed projection is fsync'd before this call returns."""
        return await self._client._send(
            "RecomputeMaterialization",
            {
                "derived_id": derived_id,
                "expected_source_graph_version": expected_source_graph_version,
            },
        )

    async def materialization_status(self, id: str) -> dict[str, Any]:
        """Seam 3 — the current status (``"Fresh"``/``"Stale"``/``"Retracted"``, or
        ``None`` if absent) from the durable per-graph reasoning authority. The
        result also carries its source graph version."""
        return await self._client._send("MaterializationStatus", {"id": id})

    async def stale_materializations(self) -> dict[str, Any]:
        """Seam 3 follow-up (SURPASS gap-closure: "give staleness a consumer") --
        every opaque materialization reference CURRENTLY ``Stale`` in this graph's
        durable projection, plus the projection source graph version."""
        return await self._client._send("StaleMaterializations", {})

    async def resolve_conflict(
        self, node_ids: list[str], semantics: str = "grounded"
    ) -> dict[str, Any]:
        """EPI-P3-7 (gap-fill) — standalone Dung abstract-argumentation conflict
        resolution: for each id in ``node_ids``, is it justified (survives), defeated,
        or stuck UNDECIDED (an unresolved/paraconsistent conflict), given the
        support/contradiction/attack topology around it? This is the SAME
        grounded/preferred/stable extension machinery :meth:`epistemic_status` already
        composes internally for one claim's acceptance — reachable here directly,
        across multiple claims, with the semantics you choose.

        ``semantics`` is one of:

        * ``"grounded"`` (default) — the unique, always-defined SKEPTICAL extension.
          A claim ``survives`` iff every one of its attackers is itself defeated;
          ``defeated`` iff attacked by a surviving claim; otherwise ``undecided``
          (e.g. two claims that directly contradict each other with no other
          evidence — the textbook non-explosive paraconsistent case: neither is
          accepted NOR rejected).
        * ``"preferred"`` / ``"stable"`` — CREDULOUS: potentially several admissible
          "sides" (extensions) resolving the same conflict differently. A claim
          ``survives`` iff it is accepted in EVERY computed extension (unanimous);
          ``defeated`` iff accepted in NONE; otherwise ``undecided`` (accepted under
          only some resolutions — contested). ``"stable"`` may legitimately compute
          NO extension at all (e.g. an odd attack cycle) — every requested id then
          reports ``undecided`` rather than a fabricated verdict.

        Returns ``{"semantics", "surviving": [id, ...], "defeated": [id, ...],
        "undecided": [id, ...], "extension_sets": [[id, ...], ...]}`` — every id in
        ``node_ids`` appears in exactly one of the first three lists;
        ``extension_sets`` is the raw extension(s) (over the WHOLE graph, not just
        ``node_ids``) the verdict was computed from: exactly one for ``"grounded"``,
        zero-or-more for ``"preferred"``/``"stable"``. The epistemic TMS is included
        in the mandatory main build. Read-only — no graph node is written."""
        return await self._client._send(
            "ResolveConflict", {"node_ids": node_ids, "semantics": semantics}
        )

    async def causal_estimate(
        self,
        variables: list[dict[str, Any]],
        do_values: dict[str, float],
        mode: str = "Intervene",
    ) -> dict[str, Any]:
        """EPI-P3-3/P3-6 — a calibrated query over a request-carried linear-Gaussian
        structural causal model. ``mode`` selects which of the crate's two
        non-counterfactual queries ``do_values`` feeds:

        * ``"Intervene"`` — a **do-calculus intervention**
          ``P(· | do(X₁=x₁, X₂=x₂, …))``:
          genuine graph surgery (Pearl, *Causality* ch. 3) — the named variables'
          incoming edges are CUT, not conditioned on, so no information flows
          backward through them (this is what distinguishes it from a naive
          conditional/regression estimate under confounding).
        * ``"Observe"`` — the **observational** query ``P(· | X₁=x₁, X₂=x₂, …)``:
          ordinary multivariate-Gaussian conditioning on the UNMUTILATED joint.
          Unlike ``"Intervene"``, evidence propagates BACKWARD to ancestors too
          (e.g. a confounder) — exactly what distinguishes "seeing X=x" from
          "doing X=x", and why the two modes can (and under confounding, will)
          disagree on the very same ``do_values``/evidence input.

        ``variables`` defines the DAG in topological (parents-before-children)
        order, one dict per variable::

            {"id": "z", "parents": [], "bias": 0.0, "noise_var": 1.0}
            {"id": "x", "parents": [["z", 1.0]], "bias": 0.0, "noise_var": 0.25}
            {"id": "y", "parents": [["z", 1.0], ["x", 0.5]], "bias": 0.0, "noise_var": 0.25}

        ``parents`` is a list of ``[parent_id, weight]`` pairs, each of which MUST
        already appear as an earlier entry in ``variables``. ``do_values`` fixes the
        named variables (``"Intervene"``) or supplies their evidence
        (``"Observe"``), e.g. ``{"x": 2.0}``.

        Returns ``{"estimates": [[var_id, {"mean", "variance", "interval":
        [lo, hi], "level"}], ...]}``, one calibrated estimate per variable in the
        SAME order as ``variables``. A pure function over the request — no graph
        node is read. Epistemic-causal support is included in the mandatory main build."""
        params: dict[str, Any] = {
            "variables": variables,
            "do_values": do_values,
            "mode": mode,
        }
        return await self._client._send("CausalEstimate", params)

    async def causal_counterfactual(
        self,
        variables: list[dict[str, Any]],
        actual: dict[str, float],
        do_values: dict[str, float],
    ) -> dict[str, Any]:
        """EPI-P3-6 — Pearl's point-**counterfactual** recipe (*Causality* ch. 7)
        over the SAME linear-Gaussian SCM shape :meth:`causal_estimate` takes:
        "given that unit ``actual`` (a FULLY-observed assignment of every variable
        in ``variables``) really happened, what would its variables have been had
        ``do_values`` held instead?" — abduction (infer each variable's realized
        exogenous noise from ``actual``), action (apply ``do_values`` via the same
        graph surgery :meth:`causal_estimate`'s ``"Intervene"`` mode uses), then
        prediction (replay forward with the SAME inferred noise).

        DETERMINISTIC given ``actual`` — not a calibrated distribution like
        :meth:`causal_estimate` — so a variable NOT downstream of any ``do_values``
        name reproduces its ``actual`` value exactly, while a downstream variable
        gets its counterfactual value.

        ``variables``/``do_values`` are the same shapes :meth:`causal_estimate`
        takes; ``actual`` is a fully-observed unit — every variable named in
        ``variables`` MUST have an entry (an engine error otherwise, since the
        recipe needs to abduce every variable's noise).

        Returns ``{"values": [[var_id, point_value], ...]}``, in the SAME order as
        ``variables``. A pure function over the request — no graph node is read.
        Epistemic-causal support is included in the mandatory main build."""
        return await self._client._send(
            "CausalCounterfactual",
            {"variables": variables, "actual": actual, "do_values": do_values},
        )

    async def rank_by_provenance(
        self,
        candidates: list[dict[str, Any]],
        weights: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """EPI-P3-3 — provenance-aware retrieval ranking: order request-carried
        ``candidates`` by a weighted blend of similarity AND evidence quality/
        provenance (source reliability, corroboration, calibration precision,
        freshness) rather than similarity alone — a well-sourced, well-corroborated
        result should not be outranked by a merely-more-similar, unsourced one.

        Each candidate dict is::

            {"id": "doc-1", "similarity": 0.7, "source_reliability": 0.95,
             "freshness": 0.9,
             "calibration": {"interval": [0.85, 0.95], "level": 0.95, "evidence_count": 5}}

        ``calibration`` is optional (``None``/omitted for a candidate with no
        evidence-graph backing — it then ranks on similarity/reliability/freshness
        alone). ``weights`` is ``{"similarity": w1, "evidence_quality": w2}``,
        defaulting to ``{0.5, 0.5}`` (equal-weighted) when omitted.

        Returns ``{"ranked": [{"id", "score", "similarity", "evidence_quality"},
        ...]}``, highest score first. A pure function over the request — no graph
        node is read. Epistemic-causal support is included in the mandatory main build."""
        params: dict[str, Any] = {"candidates": candidates}
        if weights is not None:
            params["weights"] = weights
        return await self._client._send("RankByProvenance", params)

    async def register_foreign_source(self, name: str, source: dict[str, Any]) -> str:
        """Register a named EXTERNAL source for query federation (CONCEPT:EG-KG.query.query-federation,
        Lane P), returning the registered name.

        ``source`` is the externally-tagged ``ForeignSourceSpec``: either a REMOTE
        epistemic-graph engine, queried over the engine's own transport::

            {"RemoteEngine": {
                "endpoint": "host:port", "graph": "__commons__",
                "secret": "<remote hmac secret>",
                "uql": "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2}",
            }}

        or a generic HTTP/JSON API (a pure-Rust rustls client on the engine side)::

            {"HttpJson": {
                "url": "https://api.example.com/papers",
                "json_path": "data",
                "field_map": {"id": "doi", "score": "relevance"},
            }}

        or an EXTERNAL relational-SQL database — Postgres/MySQL (CONCEPT:EG-KG.query.feature); the
        engine runs the SQL OUT to the foreign RDBMS over a pure-Rust/rustls ``sqlx``
        client and fuses the rows in-plan (the "engine federates external SQL" half that
        sql-mcp alone cannot give). Federation SQL is included in the mandatory main build::

            {"Sql": {
                "dsn": "postgres://user:pw@host:5432/papers",
                "query": "SELECT doi, relevance FROM cited WHERE published > 2023",
                "id_field": "doi",
                "score_field": "relevance",
            }}

        A federated :meth:`unified` / :meth:`uql` plan reads such a source as a
        ``RowSet`` via a ``ForeignScan`` op and composes it with the local
        graph/vector/SQL ops in ONE plan — e.g. JOIN a foreign source with the local
        graph::

            [
                {"Scan": {"label": "Doc"}},
                {"Filter": {"preds": [{"GtNum": {"prop": "year", "n": 2023.0}}]}},
                {"ForeignScan": {"source": {"HttpJson": {...}}, "join": True}},
                {"Rank": {"query": [1.0, 0.0, 0.0, 0.0]}},
                {"Limit": {"k": 10}},
            ]

        A ``ForeignScan`` with ``join`` true intersects the foreign rows with the
        current candidate set (foreign∩local, keyed on id); ``join`` false makes it a
        pure SOURCE that REPLACES the input (like ``Scan``). Federation is included in
        the mandatory main build.
        """
        return await self._client._send(
            "RegisterForeignSource", {"name": name, "source": source}
        )

    async def nl_query(
        self, text: str, graph: str | None = None
    ) -> list[dict[str, Any]]:
        """CONCEPT:EG-KG.ingest.broker-streams-namespaces — Natural-language → executable query → rows (EG-078/EG-080).

        Send free-text ``text`` to the engine's ``Method::NlQuery``: the configured/injected
        ``NlPlanner`` (an OpenAI-compatible endpoint, e.g. agent-utilities' LLM, set via
        config or ``EPISTEMIC_GRAPH_NL_ENDPOINT``) turns it into a UQL query STRING which then
        rides the IDENTICAL deterministic :meth:`uql` pipeline (no LLM in the engine core, no
        new execution path). NL-query support is included in the mandatory main build;
        the deployment must also configure a planner, otherwise this call returns a clear
        error (never a panic). ``graph`` defaults to the connection's graph.

        Returns the query's result rows (a ``Raw`` payload the transport already
        double-unpacks) — a list of row dicts, exactly as the produced UQL yields."""
        target_graph = graph or self._client._graph_name
        result = await self._client._send(
            "NlQuery", {"text": text, "graph": target_graph}, graph=target_graph
        )
        return result or []

    @staticmethod
    def _rows_to_dicts(result: Any) -> list[dict[str, Any]]:
        """Zip a ``{columns, rows}`` query result into per-row dicts. Shared by
        ``sql`` and ``cypher`` — both return the identical wire shape."""
        if not result:
            return []
        columns: list[str] = result.get("columns", [])
        out: list[dict[str, Any]] = []
        for row_blob in result.get("rows", []):
            cells = msgpack.unpackb(bytes(row_blob), raw=False)
            out.append(dict(zip(columns, cells, strict=False)))
        return out


class TxnClient:
    """CONCEPT:EG-KG.txn.multi-op-occ-acid — Multi-op OCC ACID Transaction Namespace.

    Optimistic, snapshot-isolation, server-staged transactions. ``begin()``
    returns a server-issued ``txn_id``; the ``add_node``/``remove_node``/
    ``add_edge``/``remove_edge``/``cas`` calls STAGE durable mutations server-side
    (nothing touches the graph until commit), and ``commit()`` validates the OCC
    read-set and applies the whole write-set atomically — returning ``False`` on
    conflict (a true rollback: nothing applied or persisted). ``rollback()``
    discards the staged transaction. Usage::

        txn = await client.txn.begin()
        await client.txn.add_node(txn, "a", {"type": "Doc"})
        await client.txn.add_edge(txn, "a", "b", {})
        ok = await client.txn.commit(txn)   # False ⇒ OCC conflict, retry
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def begin(self, graph: str | None = None) -> str:
        """Open a transaction and return its server-issued ``txn_id``. The target
        graph defaults to the connection's graph; pass ``graph`` to override."""
        params: dict[str, Any] = {"graph": graph, "isolation": None}
        return await self._client._send("BeginTxn", params, graph=graph)

    async def add_node(
        self,
        txn_id: str,
        node_id: str,
        properties: dict[str, Any] | None = None,
        graph: str | None = None,
    ) -> bool:
        """Stage an add-node. ``graph`` (CONCEPT:EG-KG.txn.routes-cross-shard-txn) targets a graph OTHER than
        the txn's default — making the txn multi-graph (cross-shard if it spans Raft
        groups, routed through 2PC at commit); omit for the single-graph default."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "node_id": node_id,
            "properties_msgpack": _pack_binary_msgpack(properties or {}),
            "graph": graph,
        }
        return await self._client._send("TxnAddNode", params)

    async def remove_node(
        self, txn_id: str, node_id: str, graph: str | None = None
    ) -> bool:
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "node_id": node_id,
            "graph": graph,
        }
        return await self._client._send("TxnRemoveNode", params)

    async def add_edge(
        self,
        txn_id: str,
        source_id: str,
        target_id: str,
        properties: dict[str, Any] | None = None,
        graph: str | None = None,
    ) -> bool:
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "source_id": source_id,
            "target_id": target_id,
            "properties_msgpack": _pack_binary_msgpack(properties or {}),
            "graph": graph,
        }
        return await self._client._send("TxnAddEdge", params)

    async def remove_edge(
        self, txn_id: str, source_id: str, target_id: str, graph: str | None = None
    ) -> bool:
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "source_id": source_id,
            "target_id": target_id,
            "graph": graph,
        }
        return await self._client._send("TxnRemoveEdge", params)

    async def cas(
        self,
        txn_id: str,
        node_id: str,
        conditions: dict[str, Any],
        updates: dict[str, Any],
        graph: str | None = None,
    ) -> bool:
        """Stage an atomic compare-and-set on ``node_id`` (applied at commit)."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "node_id": node_id,
            "conditions_msgpack": _pack_binary_msgpack(conditions),
            "updates_msgpack": _pack_binary_msgpack(updates),
            "graph": graph,
        }
        return await self._client._send("TxnCas", params)

    async def add_embedding(
        self,
        txn_id: str,
        node_id: str,
        embedding: list[float],
        graph: str | None = None,
    ) -> bool:
        """Stage a VECTOR upsert (CONCEPT:EG-KG.txn.reader-never-sees-node — cross-modal ACID). The embedding
        lands atomically WITH the txn's graph/property/blob-ref writes in ONE redb
        WriteTransaction at commit (requires the redb persistence backend)."""
        return await self._client._send(
            "TxnAddEmbedding",
            {
                "txn_id": txn_id,
                "node_id": node_id,
                "embedding": embedding,
                "graph": graph,
            },
        )

    async def blob_ref(
        self,
        txn_id: str,
        node_id: str,
        digest: str,
        graph: str | None = None,
    ) -> bool:
        """Stage a BLOB REFERENCE (CONCEPT:EG-KG.txn.reader-never-sees-node). Records a durable graph-side
        ``__blob__`` link to an already-stored content-addressed blob; lands
        atomically with the node/vector/property at commit."""
        return await self._client._send(
            "TxnBlobRef",
            {
                "txn_id": txn_id,
                "node_id": node_id,
                "digest": digest,
                "graph": graph,
            },
        )

    async def add_measurement(
        self,
        txn_id: str,
        series: str,
        points: list[tuple[int, list[float]]],
        graph: str | None = None,
    ) -> bool:
        """Stage a TIME-SERIES measurement batch (CONCEPT:EG-KG.backend.cross-modal-atomic-commit — extended cross-modal
        staging). The points land atomically WITH the txn's graph/property/vector/blob
        writes in ONE redb ``WriteTransaction`` at commit. ``points`` are
        ``(ts_ns, [values])`` — the SAME shape :meth:`TimeSeriesClient.append` carries.
        Time-series support is included in the mandatory main build."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "series": series,
            "points": _pack_binary_msgpack(
                [[int(ts), [float(v) for v in vals]] for ts, vals in points]
            ),
            "graph": graph,
        }
        return await self._client._send("TxnAddMeasurement", params)

    async def axiom(self, txn_id: str, turtle: str, graph: str | None = None) -> bool:
        """Stage OWL AXIOMS as Turtle (CONCEPT:EG-KG.txn.extended-cross-modal). At commit they lower to graph
        node/edge writes in the SAME atomic ``WriteTransaction`` so the OWL reasoner
        sees them consistently with the txn's other staged modalities. OWL support is
        included in the mandatory main build."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "turtle": turtle,
            "graph": graph,
        }
        return await self._client._send("TxnAxiom", params)

    async def construct(
        self, txn_id: str, sparql: str, graph: str | None = None
    ) -> bool:
        """Stage a SPARQL CONSTRUCT (CONCEPT:EG-KG.query.extended-cross-modal). At commit the produced triples
        lower to graph node/edge writes in the SAME atomic ``WriteTransaction``.
        SPARQL support is included in the mandatory main build."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "sparql": sparql,
            "graph": graph,
        }
        return await self._client._send("TxnConstruct", params)

    async def plan_writeback(
        self,
        txn_id: str,
        plan: list[dict[str, Any]],
        anchor_id: str,
        relationship: str,
        graph: str | None = None,
    ) -> bool:
        """Stage a PLANNER WRITEBACK into the txn (CONCEPT:EG-KG.query.plan-dag, D7 —
        the planner-writeback ACID seam). ``plan`` (the SAME ``Op`` list :meth:`
        QueryClient.unified` takes) runs READ-ONLY against the txn's committed
        snapshot; each id in its result row set becomes an ``AddEdge`` from
        ``anchor_id`` to that id carrying ``relationship`` — e.g. materializing a
        Reason/Traverse-inferred edge set — staged into the SAME atomic write
        transaction as the txn's other modalities (mirrors :meth:`axiom`/
        :meth:`construct`). Query support is included in the mandatory main build."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "plan": {"ops": plan},
            "anchor_id": anchor_id,
            "relationship": relationship,
            "graph": graph,
        }
        return await self._client._send("TxnPlanWriteback", params)

    async def materialize_belief(
        self, txn_id: str, node_id: str, graph: str | None = None
    ) -> bool:
        """Stage a MATERIALIZE-BELIEF op into the txn (CONCEPT:EG-KG.epistemic.epistemic-substrate,
        D5 — the explicit, AUDITED "materialize belief" op). Computes the propagated
        belief for ``node_id`` over the graph's SUPPORTS/CONTRADICTS/ATTACKS evidence
        topology (read from the txn's committed snapshot) and stages an unconditional
        compare-and-set that writes it onto that node's stored confidence, landing
        atomically with the txn's other staged modalities at commit — the ONLY path
        that ever writes a derived belief back onto stored confidence. Epistemic
        support is included in the mandatory main build."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "node_id": node_id,
            "graph": graph,
        }
        return await self._client._send("TxnMaterializeBelief", params)

    async def unified_query(
        self,
        txn_id: str,
        text: str,
    ) -> list[dict[str, Any]]:
        """Run a UNIFIED cross-modal UQL read INSIDE the txn with read-your-own-writes
        (CONCEPT:EG-KG.query.txn-cross-modal-ryow — in-txn cross-modal RYOW). ``text`` is the SAME UQL surface
        :meth:`QueryClient.unified_query_text` parses; the read runs over a snapshot
        OVERLAID with THIS txn's staged (uncommitted) write-set, so a staged
        node/edge/embedding is visible before commit and invisible off-txn until
        commit. Returns the same ``{"id", "score"}`` rows as ``unified``. Query
        support is included in the mandatory main build."""
        result = await self._client._send(
            "TxnUnifiedQueryText", {"txn_id": txn_id, "text": text}
        )
        rows = result or []
        return [{"id": id_, "score": score} for id_, score in rows]

    async def unified_query_plan(
        self,
        txn_id: str,
        plan: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """In-txn cross-modal RYOW read from a pre-built ``Op`` plan (CONCEPT:EG-KG.query.txn-cross-modal-ryow) —
        the AST counterpart of :meth:`unified_query`, mirroring :meth:`QueryClient.unified`.
        ``plan`` is the SAME ordered list of externally-tagged operator dicts ``unified``
        carries; the read runs over a snapshot OVERLAID with THIS txn's staged writes.
        Returns the same ``{"id", "score"}`` rows. Query support is included in the
        mandatory main build."""
        result = await self._client._send(
            "TxnUnifiedQuery", {"txn_id": txn_id, "plan": {"ops": plan}}
        )
        rows = result or []
        return [{"id": id_, "score": score} for id_, score in rows]

    async def commit(self, txn_id: str, *, idempotency_key: str | None = None) -> bool:
        """Commit the transaction. ``True`` ⇒ applied + persisted; ``False`` ⇒ OCC
        conflict (nothing applied — a true rollback; re-begin and retry).

        ``idempotency_key`` (optional, B-9, 2026-08-13) makes a RETRY of this
        exact logical commit provably safe, extending the SAME durable
        ``(tenant, graph, idempotency_key)``-scoped dedup mechanism
        :meth:`ChangesClient.apply`/``ApplyChangeEnvelope`` uses onto ``Commit``.
        Without a key, retry-safety depends on the caller still holding the
        exact server-issued ``txn_id`` — already durable via this txn's commit
        receipt, and fine for "commit succeeded, response lost, retry with the
        SAME txn_id" — but NOT for "I lost track of ``txn_id`` and had to
        ``begin()`` + re-stage from scratch": a fresh ``txn_id`` looks like a
        brand-new transaction. Pass the SAME ``idempotency_key`` on every retry
        of the identical logical commit (even across a re-stage under a new
        ``txn_id``) to close that gap: a repeat applies the write-set AT MOST
        ONCE. Use :meth:`commit_with_outcome` if you need to know whether a
        given call was the original apply or a replay of an earlier one."""
        result = await self._client._send(
            "Commit",
            {"txn_id": txn_id, "idempotency_key": idempotency_key},
            idempotency_key=idempotency_key,
        )
        if isinstance(result, dict):
            return bool(result.get("committed", False))
        return bool(result)

    async def commit_with_outcome(
        self, txn_id: str, *, idempotency_key: str
    ) -> tuple[bool, bool]:
        """Like :meth:`commit`, but requires ``idempotency_key`` and reports
        ``(committed, replayed)`` (B-9, 2026-08-13) — ``replayed`` is ``True``
        when this call's result is the CACHED outcome of an earlier commit
        under the same key rather than a fresh apply, mirroring
        ``ApplyChangeEnvelope``'s ``applied``/``idempotent_skip`` vocabulary."""
        result = await self._client._send(
            "Commit",
            {"txn_id": txn_id, "idempotency_key": idempotency_key},
            idempotency_key=idempotency_key,
        )
        if not isinstance(result, dict):
            raise TypeError(
                "commit_with_outcome requires the engine's keyed Commit response "
                f"shape (a dict carrying 'committed'/'replayed'); got {result!r}"
            )
        return bool(result.get("committed", False)), bool(result.get("replayed", False))

    async def rollback(self, txn_id: str) -> bool:
        """Discard the staged transaction (nothing was applied/persisted)."""
        return await self._client._send("Rollback", {"txn_id": txn_id})


class TimeSeriesClient:
    """CONCEPT:AU-KG.retrieval.god-nodes-communities/211 — Native Time-Series Namespace.

    Append/scan/query time-partitioned series stored beside the graph (their own
    ``series.redb``), using the time-series surface included in the mandatory main build. Series are
    keyed by ``series_id`` (independent of the connection's graph). Points are
    ``(ts_ns, [field0, field1, ...])`` — a scalar series is one field per point;
    OHLCV is several. The native primitives (ASOF / gap-fill / windowed aggregate)
    do not invoke DataFusion; the time-series surface is included in the mandatory
    main build.

    Usage::

        await client.timeseries.append("px", [(0, [100.0]), (1_000_000_000, [101.0])])
        pts  = await client.timeseries.range("px", 0, 2_000_000_000)
        vals = await client.timeseries.asof_join("px", [500_000_000])  # -> [100.0]
        bars = await client.timeseries.window("px", 0, 60_000_000_000, 60_000_000_000, "mean")
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def register_series(
        self,
        series_id: str,
        *,
        entity_id: str | None = None,
        field_names: list[str] | None = None,
        bucket_ns: int = 3_600_000_000_000,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a ``:Series`` node in the connection's graph linking the series
        to a KG entity — the series-id registry shape (CONCEPT:AU-KG.retrieval.god-nodes-communities).

        The series data itself lives in the time-series store (keyed by
        ``series_id``); this writes a small node into the GRAPH so the series is
        discoverable + linkable from the ontology. Node shape::

            :Series {
              id:          "series:<series_id>",
              type:        "Series",
              series_id:   "<series_id>",
              entity_id:   "<kg-node-id>",     # the entity this series measures
              field_names: ["px", "vol", ...],
              bucket_ns:   <int>,
              ... metadata
            }

        ``entity_id`` is the KG node the series describes (e.g. a ``:Instrument`` or
        ``:Memory``); a downstream caller adds the ``:measures`` edge in its ontology
        layer (agent-utilities owns the OWL mapping)."""
        props: dict[str, Any] = {
            "type": "Series",
            "series_id": series_id,
            "field_names": field_names or [],
            "bucket_ns": int(bucket_ns),
        }
        if entity_id is not None:
            props["entity_id"] = entity_id
        if metadata:
            props.update(metadata)
        await self._client.nodes.add(f"series:{series_id}", props)

    async def append(
        self,
        series_id: str,
        points: list[tuple[int, list[float]]],
        *,
        n_fields: int | None = None,
        bucket_ns: int = 3_600_000_000_000,
        field_names: list[str] | None = None,
    ) -> int:
        """Append a batch of ``(ts_ns, [values])`` points in ONE round-trip. Returns
        the number of points appended. ``bucket_ns``/``field_names`` are used only
        when the series is NEW (default bucket = 1h); ``n_fields`` defaults to the
        width of the first point. A scalar series defaults to the field name
        ``"value"``; multi-field series require explicit names. Out-of-order / late
        points are handled."""
        if not points:
            return 0
        nf = n_fields if n_fields is not None else len(points[0][1])
        if isinstance(nf, bool) or not isinstance(nf, int) or nf <= 0:
            raise ValueError("n_fields must be a positive integer")

        normalized_points: list[list[Any]] = []
        for index, (ts, values) in enumerate(points):
            if not isinstance(values, list | tuple) or len(values) != nf:
                raise ValueError(
                    f"points[{index}] must contain exactly {nf} field values"
                )
            normalized_points.append([int(ts), [float(value) for value in values]])

        if field_names is None:
            if nf != 1:
                raise ValueError(
                    "field_names must be explicit for a multi-field series"
                )
            names = ["value"]
        else:
            if not isinstance(field_names, list):
                raise TypeError("field_names must be a list of strings")
            if len(field_names) != nf:
                raise ValueError(f"field_names must contain exactly {nf} names")
            if not all(isinstance(name, str) for name in field_names):
                raise TypeError("field_names must be a list of strings")
            names = list(field_names)

        blob = _pack_binary_msgpack(normalized_points)
        return await self._client._send(
            "TsAppend",
            {
                "series_id": series_id,
                "n_fields": nf,
                "bucket_ns": int(bucket_ns),
                "field_names": names,
                "points_msgpack": blob,
            },
        )

    async def range(
        self, series_id: str, from_ts: int, to_ts: int
    ) -> list[tuple[int, list[float]]]:
        """Scan ``[from_ts, to_ts)`` of a series in ts order. Returns
        ``(ts_ns, [values])`` points (empty for an unknown series)."""
        rows = await self._client._send(
            "TsRange", {"series_id": series_id, "from": int(from_ts), "to": int(to_ts)}
        )
        return [(int(ts), [float(v) for v in vals]) for ts, vals in (rows or [])]

    async def asof_join(
        self, series_id: str, left_ts: list[int], *, tolerance_ns: int | None = None
    ) -> list[float | None]:
        """ASOF join: for each event ts in ``left_ts``, the series' field-0 value as
        of (nearest at-or-before) that time. Results are in the SAME order as
        ``left_ts``; an unmatched / out-of-tolerance event yields ``None``."""
        blob = _pack_binary_msgpack([int(t) for t in left_ts])
        return await self._client._send(
            "TsAsofJoin",
            {
                "series_id": series_id,
                "left_ts_msgpack": blob,
                "tolerance": -1 if tolerance_ns is None else int(tolerance_ns),
            },
        )

    async def window(
        self, series_id: str, from_ts: int, to_ts: int, width_ns: int, agg: str = "mean"
    ) -> list[tuple[int, float, int]]:
        """Windowed aggregate over ``[from_ts, to_ts)`` in ``width_ns`` buckets.
        ``agg`` ∈ first/last/min/max/mean/sum/count. Returns
        ``(bucket_start_ns, value, count)`` per non-empty bucket."""
        rows = await self._client._send(
            "TsWindow",
            {
                "series_id": series_id,
                "from": int(from_ts),
                "to": int(to_ts),
                "width": int(width_ns),
                "agg": agg,
            },
        )
        return [(int(b), float(v), int(c)) for b, v, c in (rows or [])]

    async def gap_fill(
        self, series_id: str, from_ts: int, to_ts: int, step_ns: int
    ) -> list[tuple[int, float | None, bool]]:
        """Gap-fill (LOCF) on a fixed grid from ``from_ts`` to ``to_ts`` every
        ``step_ns``. Returns ``(grid_ts_ns, value_or_None, carried_forward)`` —
        ``value`` is ``None`` before the first observation (encoded as NaN on the
        wire); ``carried_forward`` is ``True`` when no real obs landed on that grid ts."""
        rows = await self._client._send(
            "TsGapFill",
            {
                "series_id": series_id,
                "from": int(from_ts),
                "to": int(to_ts),
                "step": int(step_ns),
            },
        )
        out: list[tuple[int, float | None, bool]] = []
        for ts, val, filled in rows or []:
            v = (
                None if isinstance(val, float) and val != val else float(val)
            )  # NaN -> None
            out.append((int(ts), v, bool(filled)))
        return out


class RdfClient:
    """CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / KG-2.218 — Native RDF/SPARQL Namespace.

    The RDF dataset maps onto the SAME property-graph the rest of the engine uses
    (a resource object becomes a typed edge, a literal object a typed property cell
    preserving xsd datatype + ``@lang``, ``rdf:type`` the engine ``type`` label, a
    named graph the connection's graph). RDF and SPARQL are included in the mandatory
    main build.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def validate_shacl(
        self,
        shapes: str,
        data_graph: str,
    ) -> dict[str, Any]:
        """Validate inline Turtle data against inline Turtle SHACL shapes.

        Validation runs in the engine's native Rust SHACL implementation and
        returns its structured validation report.  Supplying the data graph
        explicitly keeps connector admission independent from any graph state
        that may already be materialized.
        """
        return await self._client._send(
            "ShaclValidate",
            {"shapes": shapes, "data_graph": data_graph},
        )

    async def icv_configure(
        self,
        shapes: str,
        *,
        graph: str | None = None,
        mode: str = "enforce",
    ) -> bool:
        """X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard) -- (re)register the
        connection's graph's SHACL shapes as WRITE-TIME closed-world integrity
        constraints. ``AddTriples``/``RemoveTriples``/``ApplyMutation`` (SPARQL
        UPDATE) all REQUIRE a graph to have a registered policy before they accept
        any RDF write at all -- there is no "no policy configured" pass-through.
        ``mode`` must be ``"enforce"`` (the only current mode: a violating change
        aborts the commit with the introduced violations). ``shapes`` is a
        non-empty SHACL shapes Turtle document; a shape whose ``sh:targetClass``
        matches nothing in the graph registers a policy that validates
        successfully without constraining anything. ``graph`` (optional) must
        match the connection's own graph when given -- ``None`` (the default)
        configures the connection's graph directly."""
        return await self._client._send(
            "IcvConfigure",
            {"graph": graph, "mode": mode, "shapes": shapes},
        )

    async def add_triples(
        self,
        turtle: str | None = None,
        ntriples: str | None = None,
    ) -> dict[str, int]:
        """Parse Turtle OR N-Triples (exactly one) into the connection's graph.

        Returns a ``LoadReport`` dict ``{triples, multivalue, dropped_multivalue}``.
        ``dropped_multivalue`` is non-zero only when a multi-valued literal predicate
        was seen AND the server has no lossless quad store (no persist dir) — the
        extras beyond the first value are then reported, never silently lost.
        """
        if (turtle is None) == (ntriples is None):
            raise ValueError(
                "add_triples: provide exactly one of `turtle` or `ntriples`"
            )
        return await self._client._send(
            "AddTriples",
            {"turtle": turtle or "", "ntriples": ntriples or ""},
        )

    async def get_rdf(self) -> str:
        """Serialize the connection's graph back OUT to N-Triples (datatype/lang
        faithful — the inverse of :meth:`add_triples`)."""
        return await self._client._send("GetRdf")

    async def remove_triples(
        self,
        turtle: str | None = None,
        ntriples: str | None = None,
    ) -> dict[str, int]:
        """Physically RETRACT Turtle OR N-Triples from the connection's graph (CONCEPT:EG-KG.query.named-graph-support).

        The inverse of :meth:`add_triples`: parses the document and surgically removes
        each triple (a literal triple drops the property cell; a resource triple removes
        the one matching typed edge). Durable. Returns a count dict. The retract op the
        ontology UNLOAD path + SPARQL ``DELETE DATA`` build on. RDF support is included
        in the mandatory main build.
        """
        if (turtle is None) == (ntriples is None):
            raise ValueError(
                "remove_triples: provide exactly one of `turtle` or `ntriples`"
            )
        return await self._client._send(
            "RemoveTriples",
            {"turtle": turtle or "", "ntriples": ntriples or ""},
        )

    async def drop_named_graph(self, graph: str) -> str:
        """DROP a named RDF graph (CONCEPT:EG-KG.query.named-graph-support): physically clear ALL of its RDF
        content (property-graph nodes/edges + the lossless multi-valued-literal quad
        rows) in one op. Durable. The coarse-grained retract used when an ontology owns
        a dedicated named graph; the SPARQL ``DROP/CLEAR GRAPH`` op routes here. The op
        targets the request's graph, so ``graph`` is sent via the request envelope.
        RDF support is included in the mandatory main build.
        """
        return await self._client._send("DropNamedGraph", graph=graph)

    async def sparql(
        self,
        query: str,
        base_iri: str = "",
        type_convention: str = "",
    ) -> list[dict[str, str | None]]:
        """Run a SPARQL 1.1 ``SELECT`` over the connection's graph and return a list
        of row dicts keyed by projected variable (``None`` for an unbound OPTIONAL
        variable). SPARQL is included in the mandatory main build.

        ``base_iri`` + ``type_convention`` select the LPG→RDF projection vocabulary
        (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary). Both default to empty ⇒ the IDENTITY projection (node-type
        and property keys emitted verbatim, no ``rdf:type`` synthesis), preserving the
        prior behavior. A caller that passes ``base_iri`` (e.g. agent-utilities'
        ``http://agent-utilities.dev/ontology#``) + ``type_convention="camel"`` makes
        the engine project the live property graph into that vocabulary, so a by-class
        query (``?s a au:Agent``) resolves natively — the engine, not rdflib, answers.

        The engine returns ``{"vars": [...], "rows": [[cell, ...], ...]}`` (a ``Raw``
        payload the transport already double-unpacks); we zip each row to its vars.
        """
        result = await self._client._send(
            "Sparql",
            {
                "query": query,
                "base_iri": base_iri,
                "type_convention": type_convention,
            },
        )
        if not result:
            return []
        vars_: list[str] = result.get("vars", [])
        rows: list[dict[str, str | None]] = []
        for row in result.get("rows", []):
            rows.append(dict(zip(vars_, row, strict=False)))
        return rows

    async def owl_reason(
        self,
        ontology: str | None = None,
        target_class: str | None = None,
        class_base: str | None = None,
        min_confidence: float = 0.0,
    ) -> dict[str, Any]:
        """Run the native OWL 2 (EL⁺ + RL) reasoner over the connection's graph and
        materialize entailments — confidence-weighted (CONCEPT:EG-KG.ontology.incremental-materialization / KG-2.236).
        Classifies the OWL axioms already in the graph (loaded via :meth:`add_triples`)
        plus any extra ``ontology`` Turtle, then returns::

            {
                "subclasses": [[sub, sup], ...],    # the classification hierarchy
                "subclass_conf": [c, ...],          # per-subsumption confidence in [0,1],
                                                    #   aligned index-for-index
                "instances":  [[inst, class], ...], # inferred memberships (incl. ones
                                                    #   reached only through ∃-restrictions
                                                    #   / role chains), conf >= min_confidence
                "instance_conf": [c, ...],          # per-membership confidence in [0,1]
                "consistent": bool,                 # False if a class is unsatisfiable
                "unsatisfiable": [class, ...],
            }

        Axioms may carry an ``eg:confidence`` annotation and facts their per-node
        ``confidence`` (decayed by age on the Ebbinghaus curve); the closure propagates
        them — a derived entailment's confidence is ``axiom_conf x product(premise_conf)``
        (max over alternative derivations). ``min_confidence`` (tau) drops entailments
        below the threshold. ``target_class`` restricts ``instances`` to that class's
        inferred members and is EMPTY-OK ("all classes") by design. ``class_base`` is
        the absolute namespace a bare string node ``type`` (e.g. ``"Agent"``) is bridged
        into before classification — independent of ``target_class`` (BUG-281: the two
        used to be conflated server-side, so an empty ``target_class`` could never
        supply a namespace and "reason over everything" always errored). Empty
        ``class_base`` falls back to deriving one from an absolute ``target_class``.
        OWL is included in the mandatory main build. Read-only.
        """
        return await self._client._send(
            "OwlReason",
            {
                "ontology": ontology or "",
                "target_class": target_class or "",
                "class_base": class_base or "",
                "min_confidence": float(min_confidence),
            },
        )

    async def owl_reason_distributed(
        self,
        graphs: list[str],
        ontology: str | None = None,
        target_class: str | None = None,
        class_base: str | None = None,
        min_confidence: float = 0.0,
    ) -> dict[str, Any]:
        """Distributed (cross-shard) confidence-weighted OWL reasoning over the UNION of
        ``graphs`` (CONCEPT:EG-KG.ontology.concept-13). Gathers each graph/shard's TBox axioms + decayed-
        confidence type facts, runs ONE weighted EL⁺/RL closure over the union (the
        cross-shard union-read seam), and returns the SAME shape as :meth:`owl_reason` —
        provably identical to reasoning over the same axioms in a single graph. The
        single-shard fast path stays :meth:`owl_reason`. OWL is included in the mandatory
        main build. Read-only.
        """
        return await self._client._send(
            "OwlReasonDistributed",
            {
                "graphs": list(graphs),
                "ontology": ontology or "",
                "target_class": target_class or "",
                "class_base": class_base or "",
                "min_confidence": float(min_confidence),
            },
        )

    async def explain(
        self,
        sub: str,
        sup: str,
        ontology: str | None = None,
    ) -> dict[str, Any]:
        """OWL proof-tree EXPLANATION (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation) — Stardog's
        flagship "explanation" feature, native here. Classifies the connection's graph
        (its own TBox axioms, loaded via :meth:`add_triples`, plus any extra ``ontology``
        Turtle) with confidence propagation, then reconstructs the FULL recursive proof
        tree for the named-class subsumption ``sub`` ⊑ ``sup`` — WHICH axiom(s) and
        WHICH premise subsumption(s) derived it, recursively down to the asserted/
        reflexive leaves (CONCEPT:EG-KG.ontology.justification-tracking). Returns::

            {
                "found": bool,       # sub ⊑ sup holds under the classification
                "tree": {            # None when not found
                    "sub": str, "sup": str,
                    "rule": str,      # "asserted" at a LEAF, else a completion rule
                                      # name ("CR-sub", "CR-some+", ...)
                    "axioms": [str, ...],   # axiom label(s) this node's rule cited
                    "confidence": float,    # this node's own confidence in [0,1]
                    "premises": [<same shape>, ...],  # recursive — empty at a leaf
                },
                "consistent": bool,
                "unsatisfiable": [class, ...],
            }

        ``sub``/``sup`` accept a bare IRI or the canonical ``<iri>`` form (both are
        canonicalized the same way ``target_class`` is elsewhere). OWL is included in
        the mandatory main build. Read-only.
        """
        return await self._client._send(
            "OwlExplain",
            {
                "ontology": ontology or "",
                "sub": sub,
                "sup": sup,
            },
        )

    async def sparql_virtual(
        self,
        query: str,
        mapping: str,
        tables: list[str],
        external_sources: list[dict[str, str]] | None = None,
    ) -> list[dict[str, str | None]]:
        """OBDA / R2RML VIRTUAL GRAPH query (CONCEPT:EG-KG.query.r2rml-virtual-graph /
        CONCEPT:EG-KG.query.obda-query-rewrite) — Ontology-Based Data Access: run a
        SPARQL query against the engine's OWN SQL user table(s) (created via
        :class:`QueryClient`/``Method.Sql`` DDL or :meth:`import_sqlite_file`)
        EXPOSED AS RDF through an R2RML-style ``mapping``, WITHOUT ever materializing
        the whole table.

        ``tables`` names the user table(s) the mapping's ``TriplesMap``\\ s reference as
        their ``logical_source`` — each is registered as a foreign source under its own
        table name before the mapping is parsed and the query runs. ``mapping`` is
        either a standard R2RML Turtle document (``@prefix rr: <http://www.w3.org/ns/
        r2rml#> .`` ...) or the compact textual form::

            SOURCE  <table_name>
            SUBJECT http://example.org/person/{id}
            CLASS   http://example.org/Person
            COLUMN  http://example.org/name  name
            REF     http://example.org/knows http://example.org/person/{friend_id}

        The query rewrites to a projection-pushed scan of ONLY the query-relevant
        table columns, materializes ONLY the query-relevant triples into a transient
        view, and evaluates the SAME SPARQL engine over it — a real query-rewrite OBDA
        path, not an ETL/materialize step; the user table is never mutated and nothing
        is persisted into any graph. Returns the same row-dict shape as :meth:`sparql`.
        OBDA, SPARQL, and query support are included in the mandatory main build.
        Read-only.

        ``external_sources`` (CONCEPT:EG-KG.query.obda-predicate-pushdown, W4.11) registers
        LIVE external relational sources IN ADDITION to ``tables`` — each a mapping of
        ``{"name": <logical_source>, "dsn": "postgres://…"|"mysql://…", "table": <table>}``.
        The query's column projection AND its row-level ``FILTER``s are pushed down into a
        real ``SELECT … WHERE …`` against the external database (the whole table is never
        scanned). The live SQL path needs a server built with ``federation-sql``.
        """
        result = await self._client._send(
            "SparqlVirtual",
            {
                "query": query,
                "mapping": mapping,
                "tables": list(tables),
                "external_sources": [dict(s) for s in (external_sources or [])],
            },
        )
        if not result:
            return []
        vars_: list[str] = result.get("vars", [])
        rows: list[dict[str, str | None]] = []
        for row in result.get("rows", []):
            rows.append(dict(zip(vars_, row, strict=False)))
        return rows


class StreamingClient:
    """CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230 — Streaming / CDC / subscriptions / triggers.

    A reactive surface over the engine's per-graph durable change record (the
    ledger): every durable mutation emits an ordered, cursor-addressable change into
    a per-graph in-memory feed. From that ONE feed four surfaces are served over the
    SAME framed-MessagePack transport (cursor / long-poll — NO side-channel socket):

      * **CDC feed** (``cdc_read``) — tail the ordered ``CdcEvent`` changes since a
        ``from_seq`` cursor; re-read from ``last["seq"] + 1`` to skip what you've seen.
        The foundation for incremental matviews, mirrors, and external sinks.
      * **Continuous queries** (``register_continuous_query`` / ``read_continuous_query``)
        — a named aggregate (count / sum) maintained INCREMENTALLY on each change.
      * **Subscriptions / triggers** (``watch`` / ``register_trigger`` / ``fired_triggers``)
        — a LISTEN/NOTIFY-style long-poll over a graph/label cursor, plus
        condition→action triggers whose firings are pollable.
      * **Live CEP standing queries** (``cep_subscribe`` / ``cep_poll`` /
        ``cep_unsubscribe``) — register a complex-event-processing pattern ONCE, then
        pull the matches it detects as CDC changes flow. Delivery is PULL by
        default (``cep_poll`` long-polls, like ``watch``); the engine ADDITIONALLY
        pushes each match onto a broker exchange when
        ``EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE`` is configured — both delivery modes
        coexist, ``cep_poll`` is never the only way to consume a match once that is
        armed.

    The ``streaming`` feature is part of the mandatory main build and remains present in
    the source-built ``cluster`` and ``full-extras`` layers. Live CEP additionally
    requires the engine's `stream` feature (also in `full`); a `streaming`-only build
    (e.g. `pi`) drops ``Cep*`` to the "not available in this build" catch-all.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def cdc_read(
        self, graph: str, from_seq: int = 0, *, limit: int = 0
    ) -> list[dict[str, Any]]:
        """Read the ordered CDC feed for ``graph`` from cursor ``from_seq`` (inclusive),
        up to ``limit`` (0 ⇒ a server default). Each event is a dict with ``seq``,
        ``kind`` (AddNode/RemoveNode/UpdateNode/AddEdge/RemoveEdge), ``node_id``,
        ``target_id``, ``label``, and the ``before``/``after`` property blobs (raised as
        ``bytes``; ``had_before``/``had_after`` flag presence). Re-read from
        ``events[-1]["seq"] + 1`` to skip seen.

        Raises :class:`CdcGapError` when ``from_seq`` could not be served
        contiguously (B-8, 2026-08-13) -- CALLERS THAT PERSIST A CURSOR ACROSS
        RESTARTS SHOULD USE :meth:`cdc_read_with_watermark` INSTEAD, since a
        restart is the one gap shape this method's plain raise cannot help a
        caller recover from on its own (see that method's doc for why)."""
        events, _watermark, _head_seq, _epoch = await self.cdc_read_with_watermark(
            graph, from_seq, limit=limit
        )
        return events

    async def cdc_read_with_watermark(
        self, graph: str, from_seq: int = 0, *, limit: int = 0
    ) -> tuple[list[dict[str, Any]], int, int, int]:
        """Like :meth:`cdc_read`, but also returns ``(watermark, head_seq, epoch)``.

        B-8 (2026-08-13): the engine's CDC feed (``CdcHub``, ``src/server/cdc.rs``)
        is a bounded, purely IN-MEMORY ring per graph (default 65,536 events,
        ``EPISTEMIC_GRAPH_CDC_RING`` to override) -- DELIBERATELY EPHEMERAL, not
        durable (see that module's doc for the reasoning; making it durable is a
        separate storage-format/migration decision, out of scope here). A
        persisted ``from_seq`` cursor can therefore become unservicable in two
        distinct ways:

          1. **within-epoch trim** -- the ring dropped entries older than
             ``from_seq`` (still the same process). This method raises
             :class:`CdcGapError` for you.
          2. **epoch reset** -- the engine process restarted (or the graph's
             feed was rewound by ``ClearGraph``/``FromMsgpack``/``Reconcile``),
             so ``from_seq`` may be numerically "in range" of the FRESH epoch's
             ``[watermark, head_seq]`` window without naming the same events at
             all -- a case a bare bounds check cannot detect on its own. This
             method also raises for the numerically-detectable slice of that
             case; for the remaining slice, a caller that persists a cursor
             ACROSS PROCESS RESTARTS must additionally persist the returned
             ``epoch`` alongside it and compare on its NEXT read -- a different
             ``epoch`` is PROOF the feed restarted even when no exception was
             raised.

        ``watermark`` is the oldest seq this epoch's ring can currently vouch
        for; ``head_seq`` is the current head (next seq to be assigned).
        """
        result = await self._client._send(
            "CdcRead", {"graph": graph, "from_seq": int(from_seq), "limit": int(limit)}
        )
        if not isinstance(result, dict):
            raise TypeError(
                "CdcRead must return the typed CdcReadResult shape (a dict); "
                f"got {result!r}"
            )
        watermark = int(result.get("watermark", 0))
        head_seq = int(result.get("head_seq", 0))
        epoch = int(result.get("epoch", 0))
        if result.get("gap", False):
            raise CdcGapError(
                f"CdcRead: cursor {from_seq} for graph {graph!r} could not be "
                f"served contiguously (watermark={watermark}, head_seq={head_seq}, "
                f"epoch={epoch}) -- this is NOT the same as a genuinely caught-up "
                "read; re-seed the cursor (and compare `epoch` if you persist "
                "cursors across restarts)"
            )
        return list(result.get("events", [])), watermark, head_seq, epoch

    async def register_continuous_query(
        self, name: str, graph: str, agg: str, *, label: str = "", field: str = ""
    ) -> str:
        """Register (or replace) an incrementally-maintained query ``name`` over
        ``graph``'s CDC feed. ``agg`` is ``"count"`` (live count of matching nodes) or
        ``"sum"`` (running sum of numeric node property ``field``). ``label`` (empty ⇒
        all nodes) filters by node label. The view is SEEDED from the graph's current
        state at registration, then maintained on delta. Returns ``name``."""
        if agg == "count":
            spec_agg: Any = "Count"
        elif agg == "sum":
            if not field:
                raise ValueError("sum continuous query requires a field")
            spec_agg = {"Sum": {"field": field}}
        else:
            raise ValueError(f"unknown agg '{agg}' (expected 'count' or 'sum')")
        spec = {"graph": graph, "label": label, "agg": spec_agg}
        return await self._client._send(
            "RegisterContinuousQuery",
            {"name": name, "spec_msgpack": _pack_binary_msgpack(spec)},
        )

    async def read_continuous_query(self, name: str) -> dict[str, Any]:
        """Read the current incrementally-maintained result of continuous query
        ``name`` → ``{"name", "value", "through_seq"}`` (the value + the CDC seq it
        reflects)."""
        return await self._client._send("ReadContinuousQuery", {"name": name})

    async def drop_continuous_query(self, name: str) -> bool:
        """Drop a continuous query. Returns ``True`` if it existed."""
        return await self._client._send("DropContinuousQuery", {"name": name})

    async def watch(
        self, graph: str, from_seq: int = 0, *, label: str = "", timeout_ms: int = 0
    ) -> dict[str, Any]:
        """LISTEN/NOTIFY-style long-poll subscription: return the matching CDC changes
        for ``graph`` since ``from_seq`` (filtered by ``label``, empty ⇒ all). If none
        are pending, block up to ``timeout_ms`` for the first one (0 ⇒ don't block).
        Returns ``{"events": [...], "next_seq": int, "watermark": int, "head_seq": int,
        "epoch": int}`` — pass ``next_seq`` back to keep tailing. One Request → one
        Response; re-issue to continue watching.

        Raises :class:`CdcGapError` when ``from_seq`` fell off the SAME ephemeral
        ring :meth:`cdc_read` reads (B-8, 2026-08-13) — surfaced immediately,
        never silently swallowed into an empty ``events`` batch. Compare the
        returned ``epoch`` across resumed calls to also catch a restart that a
        bare bounds check cannot detect on its own (see
        :meth:`cdc_read_with_watermark`'s doc for the full reasoning)."""
        result = await self._client._send(
            "Watch",
            {
                "graph": graph,
                "from_seq": int(from_seq),
                "label": label,
                "timeout_ms": int(timeout_ms),
            },
        )
        if isinstance(result, dict) and result.get("gap", False):
            raise CdcGapError(
                f"Watch: cursor {from_seq} for graph {graph!r} could not be served "
                f"contiguously (watermark={result.get('watermark')}, "
                f"head_seq={result.get('head_seq')}, epoch={result.get('epoch')}) -- "
                "re-seed the cursor"
            )
        return result

    async def register_trigger(
        self,
        name: str,
        graph: str,
        op: str,
        *,
        label: str = "",
        action: dict[str, Any] | None = None,
    ) -> str:
        """Register a trigger/reaction: when a CDC change in ``graph`` matches ``label``
        (empty ⇒ any) + ``op`` (``"add"``/``"remove"``/``"update"``/``"any"``), record a
        firing carrying ``action`` (an opaque reaction payload — e.g. a notification
        topic / webhook spec). Poll firings with ``fired_triggers``. Returns ``name``."""
        return await self._client._send(
            "RegisterTrigger",
            {
                "name": name,
                "graph": graph,
                "label": label,
                "op": op,
                "action_msgpack": _pack_binary_msgpack(action or {}),
            },
        )

    async def drop_trigger(self, name: str) -> bool:
        """Drop a trigger. Returns ``True`` if it existed."""
        return await self._client._send("DropTrigger", {"name": name})

    async def list_triggers(self, graph: str) -> list[dict[str, Any]]:
        """List the triggers registered on ``graph`` (``name``/``op``/``label``/
        ``fire_count``)."""
        return await self._client._send("ListTriggers", {"graph": graph})

    async def fired_triggers(
        self, graph: str, from_seq: int = 0, *, limit: int = 0
    ) -> list[dict[str, Any]]:
        """Poll the fired-trigger log for ``graph`` from cursor ``from_seq``: the
        reactions that fired, each ``{"fire_seq", "trigger", "change_seq", "node_id",
        "action"}`` (``action`` raised as ``bytes``). Resume from
        ``fired[-1]["fire_seq"] + 1``.

        Raises :class:`CdcGapError` when ``from_seq`` fell off the fired-trigger
        log's own bounded ring (B-8 follow-up, 2026-08-13) -- a SECOND ephemeral
        ring with the identical gap shapes :meth:`cdc_read` documents, over its
        own ``fire_seq`` cursor rather than the CDC ``seq`` cursor."""
        result = await self._client._send(
            "FiredTriggers",
            {"graph": graph, "from_seq": int(from_seq), "limit": int(limit)},
        )
        if not isinstance(result, dict):
            raise TypeError(
                "FiredTriggers must return the typed FiredTriggersResult shape "
                f"(a dict); got {result!r}"
            )
        if result.get("gap", False):
            raise CdcGapError(
                f"FiredTriggers: cursor {from_seq} for graph {graph!r} could not "
                f"be served contiguously (watermark={result.get('watermark')}, "
                f"head_seq={result.get('head_seq')}, epoch={result.get('epoch')}) "
                "-- re-seed the cursor"
            )
        return list(result.get("fired", []))

    async def cep_subscribe(
        self,
        pattern: dict[str, Any],
        *,
        window: dict[str, Any],
        buffer: int = 0,
    ) -> int:
        """Register a live CEP standing query (CONCEPT:EG-KG.query.protocol-types) — the PUSH
        half of the event-stream + complex-event-processing modality, fed by the SAME
        CDC hub ``watch``/``register_trigger`` use: each detected match is keyed by the
        changed node/edge's ``label`` (falling back to the change kind when unlabeled).
        ``pattern`` is a ``CepNodeSpec`` dict, one of:
        ``{"Sequence": [matcher, ...]}`` (matchers in order, other events may occur
        between steps, the whole chain inside ``window``);
        ``{"Within": {"within": int, "pattern": <CepNodeSpec>}}`` (tighten an inner
        pattern to complete within ``within`` time units); or
        ``{"Absence": {"a": matcher, "b": matcher, "within": int}}`` (emit a match at
        every event matching ``a`` that is NOT followed by ``b`` within ``within``).
        Each ``matcher`` is ``{"key": str | None, "preds": [pred, ...]}`` where ``key``
        filters by event key (``None`` ⇒ any) and each ``pred`` is
        ``{"Eq": {"field": str, "value": Any}}``, ``{"Gt": {"field": str, "value": float}}``,
        ``{"Lt": {"field": str, "value": float}}``, or ``{"Exists": {"field": str}}``.
        ``window`` is ``{"Sliding": {"size": int}}`` or ``{"Tumbling": {"size": int}}``.
        ``buffer`` (0 ⇒ a server default) bounds how many unconsumed matches are
        retained for a lagging poller before the oldest are dropped. Delivery is PULL
        by default — poll with :meth:`cep_poll` — and is ADDITIONALLY pushed to a
        broker exchange when the engine has ``EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE``
        configured (consume with ``client.broker.consume``); both delivery modes
        coexist, neither replaces the other. Returns the subscription id, passed to
        :meth:`cep_poll` / :meth:`cep_unsubscribe`."""
        spec = {"pattern": pattern, "window": window}
        return await self._client._send(
            "CepSubscribe",
            {"pattern_msgpack": _pack_binary_msgpack(spec), "buffer": int(buffer)},
        )

    async def cep_poll(
        self, sub_id: int, *, timeout_ms: int = 0
    ) -> list[dict[str, Any]]:
        """Long-poll CEP subscription ``sub_id`` for the matches pushed since the last
        poll (CONCEPT:EG-KG.query.protocol-types) — mirrors :meth:`watch`'s long-poll shape:
        returns immediately if any are buffered, else blocks up to ``timeout_ms`` for
        the FIRST one (0 ⇒ don't block), then returns whatever arrived. Each match is
        ``{"events": [{"ts": int, "key": str, "attrs": {...}}, ...], "start_ts": int,
        "end_ts": int}``. An empty list means "nothing yet" — re-poll to keep tailing.
        Raises if ``sub_id`` was dropped (unsubscribed, or never registered)."""
        return await self._client._send(
            "CepPoll", {"sub_id": int(sub_id), "timeout_ms": int(timeout_ms)}
        )

    async def cep_unsubscribe(self, sub_id: int) -> bool:
        """Drop CEP standing query ``sub_id`` and its subscriber (CONCEPT:EG-KG.query.protocol-types).
        Returns ``True`` if it existed."""
        return await self._client._send("CepUnsubscribe", {"sub_id": int(sub_id)})


class BlobClient:
    """CONCEPT:EG-KG.storage.blob-namespace — Streamed content-addressed BLOB namespace.

    Store / fetch large media (image / audio / video) bytes as a content-addressed,
    deduplicated, refcount-GC'd blob beside the graph. The whole file is never
    resident on either side: an upload streams as N fixed-size chunks sharing ONE
    server-side cursor (each chunk hashed + stored on arrival), a commit assembles
    the manifest → a stable blob digest; a fetch mirrors it (open cursor → pull
    chunks → reassemble). Identical bytes ⇒ identical digest ⇒ ZERO new chunks
    (dedup). The ``blob`` feature is part of the mandatory main build and requires a
    persist dir.

    The CONTENT lives here keyed by digest (graph-independent); a caller links it
    into the graph with a ``:MediaAsset``/``:Media`` node + a ``blob_ref`` (the
    cross-modal ACID path, CONCEPT:EG-KG.txn.reader-never-sees-node). Usage::

        digest = await client.blob.store(image_bytes)        # content-addressed
        same   = await client.blob.store(image_bytes)        # == digest, deduped
        out    = await client.blob.fetch(digest)             # == image_bytes
        await client.blob.incref(digest)                     # a :Media now refs it
    """

    #: Default chunk size for an upload when the caller passes none. Matches the
    #: engine default; small enough that one chunk is never a large allocation.
    DEFAULT_CHUNK_SIZE = 1 << 20  # 1 MiB

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def begin(self, chunk_size: int = 0) -> int:
        """Open an upload cursor (server allocates an id). ``chunk_size`` 0 ⇒ engine
        default. Push chunks with :meth:`chunk_put`, finalize with :meth:`commit`."""
        return await self._client._send("BlobBegin", {"chunk_size": int(chunk_size)})

    async def chunk_put(self, cursor: int, data: bytes) -> int:
        """Push one chunk into an open upload cursor (hashed + stored on arrival).
        Returns the running chunk count on the cursor."""
        return await self._client._send(
            "BlobChunkPut", {"cursor": int(cursor), "data": data}
        )

    async def commit(self, cursor: int) -> str:
        """Finalize an upload cursor → store the manifest content-addressed; returns
        the blob digest (the hash of the manifest, a stable content address)."""
        return await self._client._send("BlobCommit", {"cursor": int(cursor)})

    async def store(self, data: bytes, *, chunk_size: int = 0) -> str:
        """Store ``data`` as a content-addressed blob in ONE call (begin → stream
        chunks → commit) and return its digest. Streams in ``chunk_size`` chunks
        (default :attr:`DEFAULT_CHUNK_SIZE`) so a large payload is never re-buffered
        whole server-side. Identical bytes always yield the same digest (dedup)."""
        cs = int(chunk_size) or self.DEFAULT_CHUNK_SIZE
        cursor = await self.begin(cs)
        for off in range(0, len(data), cs):
            await self.chunk_put(cursor, data[off : off + cs])
        return await self.commit(cursor)

    async def fetch_begin(self, digest: str) -> tuple[int, int]:
        """Open a fetch cursor for ``digest``; returns ``(cursor, n_chunks)``."""
        cursor, n = await self._client._send("BlobFetchBegin", {"digest": digest})
        return int(cursor), int(n)

    async def chunk_get(self, cursor: int, idx: int) -> bytes:
        """Pull chunk ``idx`` of an open fetch cursor as raw bytes."""
        out = await self._client._send(
            "BlobChunkGet", {"cursor": int(cursor), "idx": int(idx)}
        )
        return bytes(out)

    async def fetch_end(self, cursor: int) -> bool:
        """Close a fetch cursor (idempotent)."""
        return await self._client._send("BlobFetchEnd", {"cursor": int(cursor)})

    async def fetch(self, digest: str) -> bytes:
        """Fetch a whole blob by digest in ONE call (open → pull every chunk →
        reassemble → close). Returns the exact stored bytes."""
        cursor, n = await self.fetch_begin(digest)
        try:
            chunks = [await self.chunk_get(cursor, i) for i in range(n)]
        finally:
            await self.fetch_end(cursor)
        return b"".join(chunks)

    async def incref(self, digest: str) -> int:
        """Increment a blob's GC refcount (a ``:Media`` node now references it).
        Returns the new count."""
        return await self._client._send("BlobRef", {"digest": digest})

    async def unref(self, digest: str) -> int:
        """Decrement a blob's GC refcount (a reference was removed). Returns the new
        count; a blob at 0 is eligible for the next :meth:`gc`."""
        return await self._client._send("BlobUnref", {"digest": digest})

    async def gc(self) -> tuple[int, int]:
        """Run the refcount mark-and-sweep GC; returns ``(blobs, chunks)`` reclaimed."""
        blobs, chunks = await self._client._send("BlobGc")
        return int(blobs), int(chunks)


def _as_bytes(value: Any) -> Any:
    """Normalize a msgpack-decoded byte payload to ``bytes``.

    The engine returns message payloads as a raw byte sequence; depending on the
    ``serde`` tagging msgpack surfaces it as ``bytes``/``bytearray`` (a ``bin``) or as a
    ``list[int]`` (an un-tagged ``Vec<u8>``). Coerce both to ``bytes``; leave anything
    else untouched."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, list) and all(isinstance(b, int) for b in value):
        return bytes(value)
    return value


class BrokerClient:
    """CONCEPT:EG-KG.ingest.broker-streams-namespaces — Native message-broker + streams namespace (EG-275..284/314).

    A thin, typed binding over the engine's RabbitMQ/Kafka-class broker built on the
    KG-2.303 work-queue: exchange/queue admin + routed publish + consumer-group
    consume/ack/reject with DLQ/TTL/priority/delay policy (EG-275..280), publisher
    confirms + tag-addressed acks (EG-284), effectively-once idempotent publish
    (EG-314), and replayable append-log **streams** (EG-283). Every mutation is
    deterministic from its explicit args (the caller supplies ``now_ms`` — no server
    clock — so WAL/Raft replay reproduces byte-identical state), exactly as the engine
    contract requires.

    Broker support is included in the mandatory main build. The AMQP/MQTT/STOMP wire
    adapters + ``graph_bus`` reach the SAME ops — this is the in-process Python surface
    for them.

    Usage::

        await client.broker.declare_exchange("events", "topic")
        await client.broker.declare_queue("q1")
        await client.broker.bind_queue("events", "q1", "user.*")
        n = await client.broker.publish("events", "user.signup", b"payload")
        msg = await client.broker.consume("q1", group="g", consumer="c1", now_ms=now)
        if msg:
            node_id, props = msg
            await client.broker.ack("q1", node_id)
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    # ── Exchange / binding admin (EG-275) ─────────────────────────────
    async def declare_exchange(self, exchange: str, kind: str = "direct") -> str:
        """Idempotently upsert an exchange. ``kind`` is ``direct``/``topic``/``fanout``.
        Returns ``"ok"`` (an unknown kind is a clear engine error)."""
        return await self._client._send(
            "DeclareExchange", {"exchange": exchange, "kind": kind}
        )

    async def delete_exchange(self, exchange: str) -> bool:
        """Delete an exchange and all its bindings (queues/messages untouched).
        Returns ``True`` if it existed."""
        return await self._client._send("DeleteExchange", {"exchange": exchange})

    async def bind_queue(self, exchange: str, queue: str, routing_key: str) -> str:
        """Bind ``queue`` to ``exchange`` under ``routing_key`` (idempotent). Returns
        ``"ok"``."""
        return await self._client._send(
            "BindQueue",
            {"exchange": exchange, "queue": queue, "routing_key": routing_key},
        )

    async def unbind_queue(self, exchange: str, queue: str, routing_key: str) -> bool:
        """Remove a specific ``exchange``/``queue``/``routing_key`` binding. Returns
        ``True`` if the binding existed."""
        return await self._client._send(
            "UnbindQueue",
            {"exchange": exchange, "queue": queue, "routing_key": routing_key},
        )

    # ── Queue policy: DLQ / TTL / priority (EG-276/277/278) ────────────
    async def declare_queue(
        self,
        queue: str,
        *,
        dl_exchange: str | None = None,
        dl_routing_key: str | None = None,
        max_delivery_count: int | None = None,
        message_ttl_ms: int | None = None,
        queue_expiry_ms: int | None = None,
        max_priority: int | None = None,
    ) -> str:
        """Idempotently upsert a queue's policy node. All fields optional — an all-``None``
        policy keeps the queue behaving exactly as the plain EG-275 work-queue. ``dl_*`` +
        ``max_delivery_count`` configure dead-lettering (EG-276), ``message_ttl_ms`` /
        ``queue_expiry_ms`` TTL (EG-277), ``max_priority`` the priority ceiling (EG-278).
        Returns ``"ok"``."""
        return await self._client._send(
            "DeclareQueue",
            {
                "queue": queue,
                "dl_exchange": dl_exchange,
                "dl_routing_key": dl_routing_key,
                "max_delivery_count": max_delivery_count,
                "message_ttl_ms": message_ttl_ms,
                "queue_expiry_ms": queue_expiry_ms,
                "max_priority": max_priority,
            },
        )

    # ── Publish (EG-275/277/278/279/284/314) ──────────────────────────
    async def publish(self, exchange: str, routing_key: str, payload: bytes) -> int:
        """Publish ``payload`` to ``exchange`` with ``routing_key``; the engine routes it
        to all matched queues atomically. Returns the delivered-queue count."""
        return await self._client._send(
            "Publish",
            {"exchange": exchange, "routing_key": routing_key, "payload": payload},
        )

    async def publish_ex(
        self,
        exchange: str,
        routing_key: str,
        payload: bytes,
        *,
        priority: int = 0,
        delay_ms: int | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> int:
        """Policy-carrying publish (superset of :meth:`publish`): stamps per-message
        ``priority`` (EG-278) and — resolving against the EXPLICIT ``now_ms`` — a
        ``delay_ms`` eta (EG-279) and a ``ttl_ms`` deadline (EG-277). With ``priority == 0``
        and all options ``None`` it is byte-identical to a plain :meth:`publish`. Returns
        the delivered-queue count."""
        return await self._client._send(
            "PublishEx",
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "payload": payload,
                "priority": int(priority),
                "delay_ms": delay_ms,
                "ttl_ms": ttl_ms,
                "now_ms": now_ms,
            },
        )

    async def publish_confirmed(
        self,
        exchange: str,
        routing_key: str,
        payload: bytes,
        *,
        priority: int = 0,
        delay_ms: int | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        """Publish with a publisher confirm (EG-284) — a superset of :meth:`publish_ex`
        that also allocates a broker-wide monotonic delivery-tag. Returns a
        ``ConfirmToken`` dict ``{"delivery_tag": int, "confirmed": bool}`` (``confirmed``
        is ``False`` — a nack — on an unknown exchange)."""
        return await self._client._send(
            "PublishConfirmed",
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "payload": payload,
                "priority": int(priority),
                "delay_ms": delay_ms,
                "ttl_ms": ttl_ms,
                "now_ms": now_ms,
            },
        )

    async def publish_idempotent(
        self,
        exchange: str,
        routing_key: str,
        payload: bytes,
        *,
        producer_id: str | None = None,
        seq: int = 0,
        priority: int = 0,
        delay_ms: int | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        """Effectively-once publish (EG-314) — a superset of :meth:`publish_confirmed`.
        With ``producer_id is None`` it is the plain at-least-once path. With a
        ``producer_id`` the broker dedups against that producer's durable monotonic
        high-water mark: a ``seq`` at/under the mark is a DUPLICATE (dropped but still
        confirmed); a ``seq`` above it advances the mark and enqueues. Returns an
        ``IdempotentPublish`` dict ``{"confirmed": bool, "duplicate": bool,
        "delivered": int}``."""
        return await self._client._send(
            "PublishIdempotent",
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "payload": payload,
                "producer_id": producer_id,
                "seq": int(seq),
                "priority": int(priority),
                "delay_ms": delay_ms,
                "ttl_ms": ttl_ms,
                "now_ms": now_ms,
            },
        )

    # ── Consume / ack / reject (EG-280/276/284) ───────────────────────
    async def consume(
        self,
        queue: str,
        *,
        group: str,
        consumer: str,
        now_ms: int,
        lease_ms: int = 0,
        prefetch: int = 0,
    ) -> tuple[str, dict[str, Any]] | None:
        """Consume one message from ``queue`` for consumer-group member
        ``(group, consumer)`` (EG-280), honoring TTL/priority/delay. Claims the
        highest-priority, oldest, DUE, non-expired message, enforcing ``prefetch``
        (0 ⇒ unlimited) and taking a ``lease_ms`` visibility lease (0 ⇒ non-expiring;
        explicit ack/nack required). Lazily
        dead-letters expired messages it steps over. Returns ``(node_id, properties)`` or
        ``None`` if nothing is deliverable."""
        claimed = await self._client._send(
            "BrokerConsume",
            {
                "queue": queue,
                "group": group,
                "consumer": consumer,
                "now_ms": int(now_ms),
                "lease_ms": int(lease_ms),
                "prefetch": int(prefetch),
            },
        )
        if not claimed:
            return None
        node_id, props = claimed
        return node_id, props

    async def ack(self, queue: str, node_id: str) -> bool:
        """Acknowledge (remove) a claimed message, freeing the consumer's in-flight slot
        (EG-280). Returns ``True`` if the message existed."""
        return await self._client._send(
            "BrokerAck", {"queue": queue, "node_id": node_id}
        )

    async def reject(
        self, queue: str, node_id: str, *, requeue: bool, now_ms: int
    ) -> str:
        """Reject a claimed message (EG-276). If ``requeue`` and the delivery count is
        under the queue's ``max_delivery_count`` it returns to claimable; otherwise it is
        dead-lettered or dropped. Returns the outcome string (``requeued``/
        ``dead-lettered``/``dropped``/``absent``)."""
        return await self._client._send(
            "BrokerReject",
            {
                "queue": queue,
                "node_id": node_id,
                "requeue": bool(requeue),
                "now_ms": int(now_ms),
            },
        )

    async def ack_tag(self, delivery_tag: int, *, consumer: str) -> bool:
        """Acknowledge a claimed message by its consumer ``delivery_tag`` (EG-284) — the
        tag-addressed sibling of :meth:`ack`. Status, tag, and current owner are
        fenced atomically; stale or foreign deliveries return ``False``."""
        return await self._client._send(
            "BrokerAckTag",
            {"delivery_tag": int(delivery_tag), "consumer": consumer},
        )

    async def nack_tag(
        self,
        delivery_tag: int,
        *,
        consumer: str,
        requeue: bool,
        now_ms: int,
    ) -> str:
        """Nack a claimed message by its consumer ``delivery_tag`` (EG-284) — the
        tag-addressed sibling of :meth:`reject`. Only the current owner can end the
        current tag generation. Returns the outcome string."""
        return await self._client._send(
            "BrokerNackTag",
            {
                "delivery_tag": int(delivery_tag),
                "consumer": consumer,
                "requeue": bool(requeue),
                "now_ms": int(now_ms),
            },
        )

    async def renew_tag(
        self,
        delivery_tag: int,
        *,
        consumer: str,
        now_ms: int,
        lease_ms: int,
    ) -> bool:
        """Extend a still-live delivery lease for its current owning consumer.

        ``now_ms`` is explicit for deterministic durable replay. The requested
        deadline must move the current deadline forward. Missing, expired, stale,
        foreign-owner, non-extending, and zero-duration renewals return ``False``.
        A failed renewal never retires an otherwise-current ack/nack generation.
        """
        return await self._client._send(
            "BrokerRenewTag",
            {
                "delivery_tag": int(delivery_tag),
                "consumer": consumer,
                "now_ms": int(now_ms),
                "lease_ms": int(lease_ms),
            },
        )

    async def sweep_expired(self, now_ms: int) -> int:
        """Reaper sweep (EG-277): dead-letter/drop messages whose TTL has passed and
        return lease-expired messages to claimable, across every queue. Returns the count
        of messages acted on. Called periodically by a scheduler with the current clock."""
        return await self._client._send("SweepExpired", {"now_ms": int(now_ms)})

    # ── Replayable append-log streams (EG-283) ────────────────────────
    async def stream_declare(
        self,
        stream: str,
        *,
        max_messages: int | None = None,
        max_age_ms: int | None = None,
    ) -> str:
        """Idempotently upsert a stream's retention policy (EG-283). Both bounds optional —
        an all-``None`` policy is an unbounded append log a trim never touches. Also ensures
        the offset counter so the stream is publishable. Returns ``"ok"``."""
        return await self._client._send(
            "StreamDeclare",
            {"stream": stream, "max_messages": max_messages, "max_age_ms": max_age_ms},
        )

    async def stream_publish(self, stream: str, payload: bytes, now_ms: int) -> int:
        """Append ``payload`` to ``stream``, returning its assigned monotonic offset
        (EG-283). The message is RETAINED (read by offset), never auto-consumed. ``now_ms``
        is stamped as the message ``ts`` for age-based retention."""
        return await self._client._send(
            "StreamPublish",
            {"stream": stream, "payload": payload, "now_ms": int(now_ms)},
        )

    async def stream_read(
        self, stream: str, *, from_offset: int = 0, max: int = 0
    ) -> list[tuple[int, bytes]]:
        """Read up to ``max`` retained messages from ``stream`` starting at ``from_offset``
        WITHOUT deleting (EG-283 — replay). ``from_offset < 0`` ⇒ only-new (from the current
        end); ``0`` ⇒ earliest; otherwise that explicit offset. ``max == 0`` ⇒ uncapped.
        Returns ``[(offset, payload), ...]`` ascending by offset. Read-only."""
        msgs = await self._client._send(
            "StreamRead",
            {"stream": stream, "from_offset": int(from_offset), "max": int(max)},
        )
        # The engine serializes each payload as a raw byte sequence; msgpack surfaces it
        # as ``bytes`` or, for an un-tagged ``Vec<u8>``, a list of ints — normalize both
        # back to ``bytes`` so a publish→read round-trip is byte-clean.
        return [(int(off), _as_bytes(payload)) for off, payload in (msgs or [])]

    async def stream_trim(self, stream: str, now_ms: int) -> int:
        """Trim ``stream`` per its declared retention (EG-283): drop messages beyond
        ``max_messages`` (oldest first) and/or older than ``max_age_ms``. Returns the count
        removed. An undeclared/unbounded stream trims nothing."""
        return await self._client._send(
            "StreamTrim", {"stream": stream, "now_ms": int(now_ms)}
        )

    async def stream_commit_offset(self, stream: str, group: str, offset: int) -> str:
        """Commit a consumer-group's read ``offset`` on ``stream`` so it can resume
        (EG-283). Idempotent upsert; returns ``"ok"``."""
        return await self._client._send(
            "StreamCommitOffset",
            {"stream": stream, "group": group, "offset": int(offset)},
        )

    async def stream_committed_offset(self, stream: str, group: str) -> int | None:
        """Read a consumer-group's committed offset on ``stream`` (EG-283). Returns the
        offset, or ``None`` if the group has never committed. Read-only."""
        return await self._client._send(
            "StreamCommittedOffset", {"stream": stream, "group": group}
        )


class RbacClient:
    """CONCEPT:EG-KG.ingest.broker-streams-namespaces — RBAC policy administration namespace (EG-092).

    A thin binding over ``Method::RbacAdmin`` (an admin/governance op, not a
    graph call): manage durable roles + a role hierarchy + resource/action grants that
    the engine's read/plan-path ``GraphView`` filter enforces. Security and the handler
    are included in the mandatory main build.

    Grants bind a role to a ``(resource, action, effect)`` triple. ``resource`` is a
    :class:`ResourceSelector` dict (``"All"`` / ``{"Pattern": s}`` / ``{"Label": s}`` /
    ``{"Graph": s}``), ``action`` is ``"Read"``/``"Write"``/``"Admin"``, ``effect`` is
    ``"Allow"``/``"Deny"``. Most-specific-resource wins.

    Usage::

        await client.rbac.add_role("reader")
        await client.rbac.add_grant(
            "reader", {"Graph": "agent:planner"}, "Read", "Allow"
        )
        policy = await client.rbac.list()   # {"roles": [...], "grants": [...]}
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add_role(self, name: str, parents: list[str] | None = None) -> str:
        """Add (or replace) a durable role that transitively inherits every grant of its
        ``parents`` (a role hierarchy). Returns ``"role_added"``."""
        role = {"name": name, "parents": list(parents or [])}
        return await self._client._send("RbacAdmin", {"op": {"AddRole": role}})

    async def remove_role(self, name: str) -> str:
        """Remove a role. Returns ``"role_removed"``."""
        return await self._client._send("RbacAdmin", {"op": {"RemoveRole": name}})

    async def add_grant(
        self,
        role: str,
        resource: dict[str, Any] | str,
        action: str,
        effect: str = "Allow",
    ) -> str:
        """Add a grant binding ``role`` to ``(resource, action, effect)``. ``resource`` is a
        :class:`ResourceSelector` (``"All"`` or ``{"Pattern"|"Label"|"Graph": s}``),
        ``action`` ∈ ``{Read, Write, Admin}``, ``effect`` ∈ ``{Allow, Deny}``. Returns
        ``"grant_added"``."""
        grant = {
            "role": role,
            "resource": resource,
            "action": action,
            "effect": effect,
        }
        return await self._client._send("RbacAdmin", {"op": {"AddGrant": grant}})

    async def remove_grant(
        self,
        role: str,
        resource: dict[str, Any] | str,
        action: str,
        effect: str = "Allow",
    ) -> dict[str, Any]:
        """Remove the grant matching ``(role, resource, action, effect)`` exactly. Returns
        ``{"removed": bool}``."""
        grant = {
            "role": role,
            "resource": resource,
            "action": action,
            "effect": effect,
        }
        return await self._client._send("RbacAdmin", {"op": {"RemoveGrant": grant}})

    async def list(self) -> dict[str, Any]:
        """List the current policy → ``{"roles": [...], "grants": [...]}``. Read-only."""
        return await self._client._send("RbacAdmin", {"op": "List"})


class JobsClient:
    """CONCEPT:INT-P2-1 — the durable analytics-job plane: async submit/status/
    cancel/resume over a redb-backed ``AnalyticsJob`` state machine (``eg-jobs``),
    reached over ONE ``Method::AnalyticsJob { op }`` wrapping an internal ``JobOp``
    (mirrors :class:`RbacClient`'s ``RbacAdmin { op }`` shape). Jobs are NOT
    graph-scoped (keyed by their own ``job_id`` in ``jobs.redb``), so a submitted job
    outlives this connection and can be polled/resumed from any client pointed at the
    same engine.

    A job runs ASYNCHRONOUSLY off the request: :meth:`submit` returns as soon as the
    job is durably recorded ``Submitted`` (not once it finishes) — poll :meth:`status`
    for progress/results. On success the engine ALSO commits the result as a
    provenance'd ``:Claim``/``:Evidence`` pair in the target graph (the same
    ``eg-epistemic`` convention every other belief/evidence write uses), so a
    finished job's output is queryable through the ordinary graph/belief surface too,
    not just :meth:`status`.

    The job plane includes association-rule mining and, when the server is built
    with ``program-optimization``, graph-native LM-program optimization::

        job = await client.jobs.submit(
            "agent:planner",
            {"MineAssociate": {"transactions": [["a", "b"], ["a", "c"]]}},
        )
        status = await client.jobs.status(job["job_id"])
        await client.jobs.cancel(job["job_id"])   # if still running
        await client.jobs.resume(job["job_id"])   # after a crash-orphaned run

        program_job = await client.jobs.submit_program_optimization(
            "agent:planner", request_msgpack
        )

    A program job returns uniform typed rows. ``kind == "program_candidate"`` rows
    describe deterministic candidates; ``kind ==
    "program_optimization_plan_step"`` rows form a dependency-ordered, bounded
    plan for an existing engine similarity/model/evaluator/trainer runtime. Durable
    rows contain opaque references and fixed labels, never prompt or response bodies.

    Every method returns the durable ``AnalyticsJob`` record as a dict (``job_id``,
    ``input_snapshot``, ``algo``, ``policy``, ``cancel_requested``, ``state``, …).
    ``state`` mirrors the Rust ``JobState`` enum's own externally-tagged shape:
    the bare string ``"Submitted"`` for that one unit variant, or
    ``{"Running": {"checkpoint": ...}}`` / ``{"Succeeded": {"result_ref", "checkpoint"}}``
    / ``{"Failed": {"reason", "checkpoint"}}`` / ``{"Cancelled": {"checkpoint"}}`` for
    the rest — check ``isinstance(state, str)`` first, else take the dict's one key
    as the state name. Authenticated standalone executors use the fenced
    ``worker_*`` methods below; they never open ``jobs.redb`` directly. Durable jobs
    are included in the mandatory main build.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def submit(
        self,
        graph: str,
        kind: dict[str, Any],
        *,
        tenant: str = "",
        actor: str = "",
        purpose: str = "",
        priority: int = 0,
        deadline_unix_ms: int | None = None,
        quota_cpu_ms: int | None = None,
        memory_bytes: int | None = None,
        io_bytes: int | None = None,
        output_bytes: int | None = None,
        worker_pool: str = "",
        worker_region: str = "",
        required_capabilities: list[str] | None = None,
        max_attempts: int = 1,
        backoff_ms: int = 0,
    ) -> dict[str, Any]:
        """Submit a new job against ``graph`` (both the tenancy anchor for the
        eventual result claim AND the input-snapshot handle's graph — the engine
        stamps in the graph's live version at submit time; it is never
        client-supplied). ``kind`` is the externally-tagged ``JobKind`` payload,
        e.g. ``{"MineAssociate": {"transactions": [...], "min_support": 0.1,
        "min_confidence": 0.5, "algorithm": "fpgrowth"}}``. Returns the freshly
        durable ``Submitted`` job record (including its server-issued ``job_id``),
        not the eventual result — the job itself keeps running after this returns.
        CPU/memory/IO/output reservations and pool/region/capability placement are
        optional; admission and matching remain coordinator-owned."""
        spec: dict[str, Any] = {
            "graph": graph,
            "tenant": tenant,
            "actor": actor,
            "purpose": purpose,
            "priority": priority,
            "deadline_unix_ms": deadline_unix_ms,
            "quota_cpu_ms": quota_cpu_ms,
            "memory_bytes": memory_bytes,
            "io_bytes": io_bytes,
            "output_bytes": output_bytes,
            "worker_pool": worker_pool,
            "worker_region": worker_region,
            "required_capabilities": list(required_capabilities or ()),
            "max_attempts": max_attempts,
            "backoff_ms": backoff_ms,
            "kind": kind,
        }
        return await self._client._send("AnalyticsJob", {"op": {"Submit": spec}})

    async def submit_program_optimization(
        self,
        graph: str,
        request_msgpack: bytes | bytearray | memoryview,
        **job_options: Any,
    ) -> dict[str, Any]:
        """Submit a versioned ``eg_program::OptimizationRequest``.

        ``request_msgpack`` must be named-field MessagePack. The engine performs
        bounded decoding, replaces caller policy scope with verified authority,
        injects the ``program.optimization`` worker capability, and persists only
        the governed reference-only request. Poll :meth:`status`; on success,
        inspect ``output.rows`` by ``kind``. Provider-dependent optimizers return
        executable plan-step rows and are resubmitted with governed
        ``optimizer_artifacts`` after the named engine runtime materializes them.
        """
        if not isinstance(request_msgpack, (bytes, bytearray, memoryview)):
            raise TypeError("request_msgpack must be bytes-like")
        payload = bytes(request_msgpack)
        if not payload or len(payload) > 16 * 1024 * 1024:
            raise ValueError("request_msgpack must be non-empty and at most 16 MiB")
        return await self.submit(
            graph,
            {"ProgramOptimize": {"request_msgpack": payload}},
            **job_options,
        )

    async def status(self, job_id: str) -> dict[str, Any]:
        """Fetch ``job_id``'s current durable state, including its checkpoint/
        progress. Read-only."""
        return await self._client._send(
            "AnalyticsJob", {"op": {"Status": {"job_id": job_id}}}
        )

    async def cancel(self, job_id: str) -> dict[str, Any]:
        """Cooperatively cancel ``job_id`` — immediate (transitions straight to
        ``Cancelled``) if it is still ``Submitted``; otherwise sets
        ``cancel_requested`` and the running executor observes it at its next
        checkpoint and stops. Raises ``RuntimeError`` if ``job_id`` is already in a
        TERMINAL state (``Succeeded``/``Failed``/``Cancelled``) — cancelling a
        finished job is an explicit invalid-transition error, not a silent no-op (an
        already-finished job's result is not retroactively discarded). Distinct from
        :meth:`EpistemicGraphClient.cancel_request`, which cancels an in-flight RPC on
        this connection (and IS a harmless no-op if that request already finished),
        not a durable job."""
        return await self._client._send(
            "AnalyticsJob", {"op": {"Cancel": {"job_id": job_id}}}
        )

    async def resume(self, job_id: str) -> dict[str, Any]:
        """Resume ``job_id`` from its last checkpoint — either a ``Failed`` job with
        retries remaining, or a ``Running`` job orphaned by a crashed/restarted
        engine process (same checkpoint, cleared cancel flag). Raises
        ``RuntimeError`` for any other state — a ``Cancelled`` job is a deliberate
        terminal stop (resubmit instead), a ``Succeeded`` job is already done (fetch
        its ``result_ref`` instead), and a ``Failed`` job with no retries remaining
        cannot be resumed either."""
        return await self._client._send(
            "AnalyticsJob", {"op": {"Resume": {"job_id": job_id}}}
        )

    async def worker_claim(
        self,
        worker_instance: str,
        capabilities: list[str],
        *,
        lease_ms: int = 60_000,
    ) -> dict[str, Any] | None:
        """Claim one governed analytics job using verified worker identity.

        ``worker_instance`` is an opaque process-slot nonce.  The server hashes it
        with the authenticated principal before persistence and returns ``None``
        when no compatible job is ready.
        """
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerClaim": {
                        "worker_instance": worker_instance,
                        "capabilities": capabilities,
                        "lease_ms": lease_ms,
                    }
                }
            },
        )

    async def worker_renew(
        self,
        job_id: str,
        worker_instance: str,
        lease_epoch: int,
        *,
        lease_ms: int = 60_000,
    ) -> dict[str, Any]:
        """Renew one exact fenced worker lease."""
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerRenew": {
                        "job_id": job_id,
                        "worker_instance": worker_instance,
                        "lease_epoch": lease_epoch,
                        "lease_ms": lease_ms,
                    }
                }
            },
        )

    async def worker_checkpoint(
        self,
        job_id: str,
        worker_instance: str,
        lease_epoch: int,
        *,
        progress: float,
        stage: str,
        state_ref: str | None = None,
    ) -> dict[str, Any]:
        """Persist a bounded, opaque checkpoint under the current lease epoch."""
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerCheckpoint": {
                        "job_id": job_id,
                        "worker_instance": worker_instance,
                        "lease_epoch": lease_epoch,
                        "progress": progress,
                        "stage": stage,
                        "state_ref": state_ref,
                    }
                }
            },
        )

    async def worker_stage(
        self,
        job_id: str,
        worker_instance: str,
        lease_epoch: int,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """Durably stage a complete typed KnowledgeBatch-shaped result."""
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerStage": {
                        "job_id": job_id,
                        "worker_instance": worker_instance,
                        "lease_epoch": lease_epoch,
                        "result": result,
                    }
                }
            },
        )

    async def worker_publish(
        self, job_id: str, worker_instance: str, lease_epoch: int
    ) -> dict[str, Any]:
        """Publish a staged result through the authoritative graph gateway."""
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerPublish": {
                        "job_id": job_id,
                        "worker_instance": worker_instance,
                        "lease_epoch": lease_epoch,
                    }
                }
            },
        )

    async def worker_fail(
        self,
        job_id: str,
        worker_instance: str,
        lease_epoch: int,
        reason_code: str,
    ) -> dict[str, Any]:
        """Release a failed attempt using a bounded server-governed reason code."""
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerFail": {
                        "job_id": job_id,
                        "worker_instance": worker_instance,
                        "lease_epoch": lease_epoch,
                        "reason_code": reason_code,
                    }
                }
            },
        )

    async def worker_cancel(
        self, job_id: str, worker_instance: str, lease_epoch: int
    ) -> dict[str, Any]:
        """Confirm cooperative cancellation for the exact fenced lease."""
        return await self._client._send(
            "AnalyticsJob",
            {
                "op": {
                    "WorkerCancel": {
                        "job_id": job_id,
                        "worker_instance": worker_instance,
                        "lease_epoch": lease_epoch,
                    }
                }
            },
        )


class KnowledgeStreamClient:
    """Current native Arrow result stream for every served query family.

    The client exposes exactly one pull operation because the engine exposes one
    authority-, placement-, query-, and snapshot-bound stream contract. Alternate
    projections and direct-family aliases are intentionally absent.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def pull(
        self,
        query: KnowledgeStreamQuery,
        *,
        batch_size: int,
        cursor: KnowledgeStreamCursor | None = None,
    ) -> KnowledgeStreamBatch:
        """Pull one bounded Arrow IPC batch and its integrity-bound resume cursor."""

        current_query, family = _knowledge_query(query)
        current_batch_size = _integer(
            "batch_size", batch_size, minimum=1, maximum=65_536
        )
        request: dict[str, Any] = {
            "schema_version": 1,
            "query": current_query,
            "batch_size": current_batch_size,
            "projection": "arrow_ipc_v1",
        }
        if cursor is not None:
            current_cursor = _knowledge_cursor(cursor)
            if (
                current_cursor["family"] != family
                or current_cursor["batch_size"] != current_batch_size
            ):
                raise ValueError("cursor family and batch size must match the request")
            request["cursor"] = current_cursor
        result = await self._client._send("KnowledgeStream", {"request": request})
        return _knowledge_batch(result, family=family, batch_size=current_batch_size)


class ServedModalityClient:
    """Governed document, image, audio, and video serving operations.

    Every method emits one current ``ServedModalityOp`` shape. Authority always
    comes from the signed request context; callers cannot add tenant, policy,
    purpose, classification, or deployment fields to an operation.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def authority(self) -> ModalityAuthority:
        """Return the opaque policy references required to certify a bundle."""

        result = await self._client._send(
            "ServedModality", {"op": {"operation": "authority"}}
        )
        return _modality_authority(result)

    async def ingest(
        self,
        modality: str,
        *,
        idempotency_ref: str,
        target_occurrence_id: str,
        bundle_msgpack: bytes,
        source_bytes: bytes,
        expected_version: int | None = None,
    ) -> ModalityApplyOutcome:
        """Decode and atomically create or version-update one certified occurrence."""

        operation: dict[str, Any] = {
            "operation": "ingest",
            "modality": _served_modality("modality", modality),
            "idempotency_ref": _opaque_ref("idempotency_ref", idempotency_ref),
            "target_occurrence_id": _opaque_ref(
                "target_occurrence_id",
                target_occurrence_id,
                namespace="occurrence",
            ),
            "expected_version": (
                None
                if expected_version is None
                else _integer("expected_version", expected_version, minimum=1)
            ),
            "bundle_msgpack": _bytes(
                "bundle_msgpack", bundle_msgpack, allow_empty=False
            ),
            "source_bytes": _bytes("source_bytes", source_bytes, allow_empty=False),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_outcome(result)

    async def ingest_stream(
        self,
        modality: str,
        items: list[dict[str, Any]],
    ) -> list[ModalityApplyOutcome]:
        """Atomically decode and apply a bounded stream of two to 64 records."""

        if not isinstance(items, list) or not 2 <= len(items) <= 64:
            raise ValueError("items must contain between two and 64 ingest records")
        encoded: list[dict[str, Any]] = []
        fields = {
            "idempotency_ref",
            "target_occurrence_id",
            "expected_version",
            "bundle_msgpack",
            "source_bytes",
        }
        for index, raw in enumerate(items):
            if not isinstance(raw, dict) or set(raw) != fields:
                raise ValueError(f"items[{index}] must contain the exact ingest fields")
            expected = raw["expected_version"]
            encoded.append(
                {
                    "idempotency_ref": _opaque_ref(
                        f"items[{index}].idempotency_ref", raw["idempotency_ref"]
                    ),
                    "target_occurrence_id": _opaque_ref(
                        f"items[{index}].target_occurrence_id",
                        raw["target_occurrence_id"],
                        namespace="occurrence",
                    ),
                    "expected_version": (
                        None
                        if expected is None
                        else _integer(
                            f"items[{index}].expected_version", expected, minimum=1
                        )
                    ),
                    "bundle_msgpack": _bytes(
                        f"items[{index}].bundle_msgpack",
                        raw["bundle_msgpack"],
                        allow_empty=False,
                    ),
                    "source_bytes": _bytes(
                        f"items[{index}].source_bytes",
                        raw["source_bytes"],
                        allow_empty=False,
                    ),
                }
            )
        operation = {
            "operation": "ingest_stream",
            "modality": _served_modality("modality", modality),
            "items": encoded,
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_outcomes(result, len(encoded))

    async def query(
        self,
        modality: str,
        *,
        segment_kind: str | None = None,
        after_occurrence_id: str | None = None,
        limit: int = 100,
        include_cold: bool = False,
    ) -> ServedModalityPage:
        """Return one stable, authority-filtered page of visible served records."""

        if segment_kind is not None:
            segment_kind = _string("segment_kind", segment_kind)
            if segment_kind not in _SERVED_SEGMENTS:
                raise ValueError("segment_kind is not a current served segment kind")
        if after_occurrence_id is not None:
            after_occurrence_id = _opaque_ref(
                "after_occurrence_id",
                after_occurrence_id,
                namespace="occurrence",
            )
        operation = {
            "operation": "query",
            "modality": _served_modality("modality", modality),
            "segment_kind": segment_kind,
            "after_occurrence_id": after_occurrence_id,
            "limit": _integer("limit", limit, minimum=1, maximum=1_000),
            "include_cold": _boolean("include_cold", include_cold),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_page(result)

    async def _native_query(
        self,
        predicate: dict[str, Any],
        *,
        after_occurrence_id: str | None,
        limit: int,
        include_cold: bool,
    ) -> ServedModalityPage:
        if after_occurrence_id is not None:
            after_occurrence_id = _opaque_ref(
                "after_occurrence_id",
                after_occurrence_id,
                namespace="occurrence",
            )
        operation = {
            "operation": "native_query",
            "predicate": predicate,
            "after_occurrence_id": after_occurrence_id,
            "limit": _integer("limit", limit, minimum=1, maximum=1_000),
            "include_cold": _boolean("include_cold", include_cold),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_page(result)

    async def search_documents(
        self,
        term: str,
        *,
        page: int | None = None,
        after_occurrence_id: str | None = None,
        limit: int = 100,
        include_cold: bool = False,
    ) -> ServedModalityPage:
        """Run an authority-keyed lexical lookup without persisting query text."""

        term = _string("term", term).strip()
        if len(term.encode("utf-8")) > 128 or not term.isalnum():
            raise ValueError("term must be one bounded alphanumeric lexeme")
        normalized_page = (
            None
            if page is None
            else _integer("page", page, minimum=1, maximum=(1 << 32) - 1)
        )
        return await self._native_query(
            {"predicate": "document_lexical", "term": term, "page": normalized_page},
            after_occurrence_id=after_occurrence_id,
            limit=limit,
            include_cold=include_cold,
        )

    async def query_image_region(
        self,
        *,
        x: float,
        y: float,
        width: float,
        height: float,
        after_occurrence_id: str | None = None,
        limit: int = 100,
        include_cold: bool = False,
    ) -> ServedModalityPage:
        """Query normalized image-space regions through the native grid index."""

        x = _finite_f32("x", x, minimum=0.0, maximum=1.0)
        y = _finite_f32("y", y, minimum=0.0, maximum=1.0)
        width = _finite_f32("width", width, minimum=0.0, maximum=1.0)
        height = _finite_f32("height", height, minimum=0.0, maximum=1.0)
        if width == 0.0 or height == 0.0 or x + width > 1.0 or y + height > 1.0:
            raise ValueError("image region must be a non-empty normalized rectangle")
        return await self._native_query(
            {
                "predicate": "image_region",
                "x": x,
                "y": y,
                "width": width,
                "height": height,
            },
            after_occurrence_id=after_occurrence_id,
            limit=limit,
            include_cold=include_cold,
        )

    async def query_similar_images(
        self,
        perceptual_hash: int,
        *,
        maximum_distance: int = 8,
        after_occurrence_id: str | None = None,
        limit: int = 100,
        include_cold: bool = False,
    ) -> ServedModalityPage:
        """Query bounded multi-probe pHash postings with exact Hamming filtering."""

        predicate = {
            "predicate": "image_perceptual_hash",
            "hash": _integer("perceptual_hash", perceptual_hash, maximum=(1 << 64) - 1),
            "maximum_distance": _integer(
                "maximum_distance", maximum_distance, maximum=15
            ),
        }
        return await self._native_query(
            predicate,
            after_occurrence_id=after_occurrence_id,
            limit=limit,
            include_cold=include_cold,
        )

    async def query_audio_window(
        self,
        *,
        start_ms: int,
        end_ms: int,
        minimum_rms: float = 0.0,
        after_occurrence_id: str | None = None,
        limit: int = 100,
        include_cold: bool = False,
    ) -> ServedModalityPage:
        """Query indexed native waveform windows by time and RMS energy."""

        start_ms, end_ms = _native_temporal_window(start_ms, end_ms)
        return await self._native_query(
            {
                "predicate": "audio_window",
                "start_ms": start_ms,
                "end_ms": end_ms,
                "minimum_rms": _finite_f32(
                    "minimum_rms", minimum_rms, minimum=0.0, maximum=1.0
                ),
            },
            after_occurrence_id=after_occurrence_id,
            limit=limit,
            include_cold=include_cold,
        )

    async def query_video_window(
        self,
        *,
        start_ms: int,
        end_ms: int,
        keyframes_only: bool = False,
        after_occurrence_id: str | None = None,
        limit: int = 100,
        include_cold: bool = False,
    ) -> ServedModalityPage:
        """Query native frame timing and keyframe indexes."""

        start_ms, end_ms = _native_temporal_window(start_ms, end_ms)
        return await self._native_query(
            {
                "predicate": "video_window",
                "start_ms": start_ms,
                "end_ms": end_ms,
                "keyframes_only": _boolean("keyframes_only", keyframes_only),
            },
            after_occurrence_id=after_occurrence_id,
            limit=limit,
            include_cold=include_cold,
        )

    async def delete(
        self,
        modality: str,
        *,
        idempotency_ref: str,
        occurrence_id: str,
        expected_version: int,
    ) -> ModalityApplyOutcome:
        """Apply the governed OCC delete and payload-erasure transition."""

        operation = {
            "operation": "delete",
            "modality": _served_modality("modality", modality),
            "idempotency_ref": _opaque_ref("idempotency_ref", idempotency_ref),
            "occurrence_id": _opaque_ref(
                "occurrence_id", occurrence_id, namespace="occurrence"
            ),
            "expected_version": _integer(
                "expected_version", expected_version, minimum=1
            ),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_outcome(result)

    async def move_to_cold(
        self, modality: str, *, occurrence_id: str
    ) -> ModalityApplyOutcome:
        """Move one authorized active occurrence to governed cold state."""

        operation = {
            "operation": "move_to_cold",
            "modality": _served_modality("modality", modality),
            "occurrence_id": _opaque_ref(
                "occurrence_id", occurrence_id, namespace="occurrence"
            ),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_outcome(result)

    async def restore(
        self, modality: str, *, occurrence_id: str
    ) -> ModalityApplyOutcome:
        """Restore one authorized cold occurrence to active state."""

        operation = {
            "operation": "restore",
            "modality": _served_modality("modality", modality),
            "occurrence_id": _opaque_ref(
                "occurrence_id", occurrence_id, namespace="occurrence"
            ),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_outcome(result)

    async def events(
        self,
        modality: str,
        *,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> list[ServedModalityEvent]:
        """Read a bounded, monotonic, authority-filtered event page."""

        operation = {
            "operation": "events",
            "modality": _served_modality("modality", modality),
            "after_sequence": _integer("after_sequence", after_sequence),
            "limit": _integer("limit", limit, minimum=1, maximum=10_000),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_events(result)

    async def stats(self, modality: str) -> ServedModalityStats:
        """Return privacy-safe live record, index, event, and snapshot cardinalities."""

        operation = {
            "operation": "stats",
            "modality": _served_modality("modality", modality),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_stats(result)

    async def collect_tombstones(
        self, modality: str, *, through_event_sequence: int
    ) -> int:
        """Collect tombstones through an observed durable delete-event fence."""

        operation = {
            "operation": "collect_tombstones",
            "modality": _served_modality("modality", modality),
            "through_event_sequence": _integer(
                "through_event_sequence", through_event_sequence, minimum=1
            ),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        value = _exact_mapping(
            "served modality collection", result, frozenset({"collected"})
        )
        return _integer("collected", value["collected"])

    async def capabilities(self, modality: str) -> ServedModalityCapabilities:
        """Require the concrete modality component to report 12 PASS and zero N/A."""

        operation = {
            "operation": "capabilities",
            "modality": _served_modality("modality", modality),
        }
        result = await self._client._send("ServedModality", {"op": operation})
        return _modality_capabilities(result)


class AdminClient:
    """CONCEPT:EG-KG.ingest.broker-streams-namespaces — Ops / maintenance namespace: online backup + restore (EG-090).

    A thin binding over the ``Method::Backup`` / ``Method::Restore`` / ``Method::
    AuditVerify`` / ``Method::AuditProveInclusion`` admin RPCs.
    :meth:`backup` takes an ONLINE consistent snapshot (per-shard ``begin_read()`` MVCC,
    no quiesce) into an operator-provisioned private backup root. The methods accept
    logical bundle names, never host paths. :meth:`restore` STAGES a rebuilt copy in
    a sibling dir (the running engine holds an exclusive lock on its live store) for the
    operator to swap in after stopping the engine — an in-place restore uses the offline
    ``restore`` CLI. Redb-only; a non-redb build returns "not available".

    :meth:`audit_verify` / :meth:`audit_prove_inclusion` are the tamper-evident audit
    surface (CONCEPT:EG-KG.sharding.row-level-security, feature `security`): both walk the target
    graph's durable, hash-chained audit log under the `kg:admin` capability gate — an
    ops/maintenance read, not an ordinary graph row read, which is why they sit here
    rather than on `.ledger` (the in-memory transaction ledger is a different durable
    concern entirely). Both require a durable redb backend.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def backup(
        self, destination: str, label: str | None = None
    ) -> dict[str, Any]:
        """Take an online consistent backup named ``destination`` under the configured
        private backup root, tagged with ``label`` (EG-090). Returns aggregate counts;
        no local path or raw label is returned or persisted."""
        return await self._client._send(
            "Backup", {"destination": destination, "label": label}
        )

    async def restore(self, source: str, *, target_shards: int) -> dict[str, Any]:
        """Restore the logical bundle name ``source`` from the configured private root
        (EG-090). Stages an engine-owned copy for an offline swap-in and returns only an
        opaque stage reference plus aggregate counts."""
        return await self._client._send(
            "Restore",
            {
                "source": source,
                "target_shards": _integer(
                    "target_shards", target_shards, minimum=1, maximum=64
                ),
            },
        )

    async def audit_verify(self) -> dict[str, Any]:
        """Walk this graph's durable, hash-chained audit log (CONCEPT:EG-KG.sharding.row-level-security)
        and report whether it verifies clean, or where the first break is. `kg:admin`-
        gated; requires a durable redb backend. Returns ``{"graph", "ok", "entries",
        "first_broken_seq", "detail"}`` — ``ok`` is ``False`` and ``first_broken_seq``
        names the offending audit-chain seq the moment ANY entry's hash link breaks
        (tampering or corruption), never before that point."""
        return await self._client._send("AuditVerify")

    async def audit_prove_inclusion(
        self, node_id: str, *, anchor_seq: int | None = None
    ) -> dict[str, Any]:
        """Produce + server-side-verify a Merkle inclusion proof that ``node_id``'s
        CURRENT durable content matches what a prior provenance anchor committed
        (CONCEPT:EG-KG.sharding.row-level-security) — the extension that lets :meth:`audit_verify`'s
        tamper-evidence reach an anchored NODE's CONTENT, not just mutation ordering.
        ``anchor_seq`` selects a specific anchor by its audit-chain seq (``None`` ⇒
        this graph's most recent anchor). `kg:admin`-gated; requires a durable redb
        backend and at least one provenance anchor already written for this graph
        (``EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS``). Returns ``{"graph", "node_id",
        "anchor_seq", "window_size", "included", "verified", "anchored_root_sha256",
        "computed_root_sha256", "proof", "detail"}`` — ``included=False`` (not an
        error) means ``node_id`` simply was not part of that anchor's window;
        ``verified=False`` means the node's durable bytes changed after anchoring,
        whether by tampering or an ordinary later overwrite. Raises if the graph has
        no anchor yet or ``anchor_seq`` names an entry that is not one."""
        return await self._client._send(
            "AuditProveInclusion",
            {"node_id": node_id, "anchor_seq": anchor_seq},
        )


class _TlsDecision(NamedTuple):
    """A typed, explicit transport-encryption decision (GOC-81 W01).

    Transport encryption is an explicit property of the endpoint/profile,
    decided by precedence (explicit client argument > named graph-service
    endpoint profile > product default) alone. Ambient CA variables
    belonging to unrelated HTTP libraries (``SSL_CERT_FILE``,
    ``REQUESTS_CA_BUNDLE``) may only ever supply TRUST MATERIAL for a
    connection whose mode this decision already selected -- they are never
    consulted to pick the mode itself. ``profile`` and ``trust_source`` are
    sanitized diagnostic tags only; neither this type nor any diagnostic
    built from it ever carries certificate/key material.
    """

    enabled: bool
    profile: str
    server_name: str | None
    trust_source: str


class EpistemicGraphClient:
    """CONCEPT:EG-KG.query.wire-protocol — Epistemic Graph Core Client

    Async client for the epistemic-graph Tokio service using Composition.

    Usage::

        context: RequestContextClaims = {
            "principal": "service:planner",
            "tenant": "tenant:default",
            "audience": "epistemic-graph",
            "agent_id": "service:planner",
            "roles": ["graph-client"],
            "scopes": ["graph:read", "graph:write"],
            "policy_version": "policy:current",
            "delegation": [],
        }
        client = await EpistemicGraphClient.connect(
            socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
            auth_secret=os.environ["GRAPH_SERVICE_AUTH_SECRET"],
            verified_context=context,
            graph_name="agent:planner",
        )
        await client.nodes.add("node1", {"type": "Agent"})
        ranks = await client.analytics.pagerank(damping=0.85, iterations=100)
        await client.close()
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        auth_secret: str,
        graph_name: str,
        *,
        verified_context: RequestContextClaims | dict[str, Any],
        timeout: float | None = _DEFAULT_RPC_TIMEOUT,
        heavy_timeout: float | None = _HEAVY_RPC_TIMEOUT,
        node_id: str | None = None,
    ) -> None:
        if not isinstance(auth_secret, str) or not auth_secret:
            raise ValueError("a non-empty authentication secret is required")
        self._reader = reader
        self._writer = writer
        self._auth_secret = auth_secret
        self._graph_name = graph_name
        # Per-RPC read timeouts (0/None disables). Heavy ops use heavy_timeout.
        self._timeout = timeout if timeout else None
        self._heavy_timeout = heavy_timeout if heavy_timeout else None
        # ADR-3 / W1.9 node-bound envelopes: which cluster node THIS connection
        # talks to, if known -- connection/endpoint metadata, not an identity
        # claim, so it lives here rather than in ``verified_context``. `None`
        # (the default) means "unknown" and the minted envelope omits the
        # `node` claim entirely, exactly like a client built before node
        # binding existed. See `_compute_verified_token`.
        self._node_id = node_id
        self._verified_context = validate_request_context(verified_context)
        self._verified_context_override: contextvars.ContextVar[
            RequestContextClaims | None
        ] = contextvars.ContextVar("eg_verified_context_override", default=None)
        # Seed from a random base rather than 0. The server derives a durable
        # replay/idempotency identity for row-local mutations (`ClearGraph`,
        # `AddNode`, ...) from `(graph, id, method+params)` alone
        # (`opaque_request_key` in `src/server/mutation_batch.rs`) -- it has no
        # visibility into which CONNECTION minted `id`. Every connection's `id`
        # sequence used to start at 1, so two independently-connected clients
        # issuing the same first call (e.g. "connect, then ClearGraph") on the
        # same graph collided on the exact same durable batch id: the SECOND
        # connection's call was silently treated as an idempotent replay of the
        # first and its `apply` never ran (observed as `ClearGraph` calls from
        # a freshly reconnected pytest fixture silently no-op'ing, leaking prior
        # tests' nodes through a "cleared" graph). A random ~48-bit start makes
        # a same-graph/same-position/same-payload collision between two
        # connections astronomically unlikely, matching how the envelope's own
        # nonce is minted, without weakening the same-connection retry-replay
        # semantics the mechanism is meant to provide (a single connection's
        # ids are still monotonically increasing and never reused).
        self._request_id = secrets.randbits(48)
        # ── GOC-81 W02 — close-lifecycle state (deliberately split in two) ──
        # ``_closed`` is ADMISSION state: True means "the next call must
        # reconnect before sending" (flipped by a dead reader, a timeout, or
        # an explicit close). ``_closing`` is OWNERSHIP
        # state: True only once :meth:`close` itself has run the transport
        # teardown to completion. Conflating the two was the leak this split
        # closes -- a reader-EOF flipping ``_closed`` must never, by itself,
        # cause `close()` to skip `writer.wait_closed()`.
        self._closed = False
        self._closing = False
        self._terminal_error: BaseException | None = None
        # Bumped on every successful (re)connect. A background reader task
        # captures the generation it was started for; its own EOF/error
        # handling is a no-op once superseded, so a stale reader callback can
        # never mark a NEWER connection dead (lane invariant 5).
        self._generation = 0
        # ── CONCEPT:EG-KG.backend.framed-response — single-connection request PIPELINING (demux) ──
        # The engine (src/server/transport.rs) processes many requests on ONE
        # connection concurrently and writes responses back OUT OF ORDER, each
        # tagged with its `Response.id`. So instead of a lock held across the
        # whole write→round-trip→read (which serialized one connection), the
        # client runs a background reader task that resolves the matching pending
        # future by id. ``_send`` registers a future under the request id, writes
        # the frame, and awaits ONLY its own future — so per-caller ordering is
        # automatic (each await blocks on its own id) while INDEPENDENT concurrent
        # calls pipeline on the one connection. ``_lock`` now guards only the
        # connect/reconnect lifecycle; ``_write_lock`` serializes just the frame
        # write so two callers never interleave bytes on the wire.
        self._lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        # One shared teardown task gives concurrent callers (and a caller that
        # is itself cancelled) the same idempotent writer-shutdown boundary.
        self._close_task: asyncio.Task[None] | None = None
        # How we connected — remembered so a dropped connection can be
        # transparently re-established on the next call (see _reconnect).
        # Populated by connect(); a directly-constructed client cannot self-heal.
        self._socket_path: str | None = None
        self._tcp_addr: str | None = None
        self._connect_timeout: float | None = _CONNECT_TIMEOUT
        self._tls_context: ssl.SSLContext | None = None
        self._tls_server_hostname: str | None = None
        # Server capability set, negotiated lazily on first use (see supports());
        # reset on reconnect so a fresh connection re-negotiates.
        self._server_ops: set[str] | None = None

        # Namespaced Sub-Clients (Composition)
        self.nodes = NodeClient(self)
        self.work_items = WorkItemClient(self)
        self.capacity_leases = CapacityLeaseClient(self)
        self.development_lanes = DevelopmentLaneClient(self)
        self.changes = ChangeEnvelopeClient(self)
        self.edges = EdgeClient(self)
        self.graph = GraphOperationsClient(self)
        self.analytics = AnalyticsClient(self)
        self.lifecycle = LifecycleClient(self)
        self.reasoning = ReasoningClient(self)
        self.ledger = LedgerClient(self)
        self.channels = ChannelsClient(self)
        self.tenants = MultiTenantClient(self)
        self.resharding = ReshardingClient(self)
        self.placement = PlacementClient(self)
        self.cluster_topology = ClusterTopologyClient(self)
        self.server_registry = ServerRegistryClient(self)
        self.raft_admin = RaftAdminClient(self)
        self.consensus = ConsensusClient(self)
        self.finance = FinanceClient(self)
        self.datascience = DataScienceClient(self)
        self.mining = MiningClient(self)
        self.graphlearn = GraphLearnClient(self)
        self.pipeline = PipelineClient(self)
        self.query = QueryClient(self)
        self.knowledge = KnowledgeStreamClient(self)
        self.modalities = ServedModalityClient(self)
        self.txn = TxnClient(self)
        self.timeseries = TimeSeriesClient(self)
        self.rdf = RdfClient(self)
        self.streaming = StreamingClient(self)
        self.blob = BlobClient(self)
        # CONCEPT:EG-KG.ingest.broker-streams-namespaces — B1.7 multi-lang client drivers: broker/streams (EG-275..284/314),
        # RBAC admin (EG-092), backup/restore (EG-090). NlQuery (EG-080) lives on `query`.
        self.broker = BrokerClient(self)
        self.rbac = RbacClient(self)
        self.admin = AdminClient(self)
        self.jobs = JobsClient(self)
        self.statechart = StatechartClient(self)
        self.viz = VizClient(self)
        self.quantum = QuantumClient(self)
        self.asr = AsrClient(self)

    @staticmethod
    def _resolve_tls_decision(
        tls: bool | ssl.SSLContext | None,
        *,
        client_cert: str | None,
        client_key: str | None,
        server_hostname: str | None,
    ) -> _TlsDecision:
        """Decide transport-encryption MODE by explicit precedence alone (GOC-81 W01).

        Precedence: explicit client argument (the ``tls`` parameter, or an
        explicit ``client_cert``/``client_key`` call argument) > named
        graph-service endpoint profile (``GRAPH_SERVICE_TLS``,
        ``GRAPH_SERVICE_TLS_CA[_DIRECTORY]``,
        ``GRAPH_SERVICE_TLS_CLIENT_CERT/_KEY``) > product default (plaintext).
        Ambient CA variables belonging to unrelated HTTP libraries
        (``SSL_CERT_FILE``, ``SSL_CERT_DIR``, ``REQUESTS_CA_BUNDLE``) never
        appear in this decision -- their bare presence must never flip the
        protocol mode.
        Contradictory inputs (TLS explicitly disabled while a client
        certificate is supplied) are rejected here, not silently resolved.
        """
        env_client_cert = str(
            os.environ.get("GRAPH_SERVICE_TLS_CLIENT_CERT", "") or ""
        ).strip()
        env_client_key = str(
            os.environ.get("GRAPH_SERVICE_TLS_CLIENT_KEY", "") or ""
        ).strip()
        has_cert_arg = bool(str(client_cert or "").strip())
        has_key_arg = bool(str(client_key or "").strip())

        configured = str(os.environ.get("GRAPH_SERVICE_TLS", "")).strip().lower()
        profile_ca = str(os.environ.get("GRAPH_SERVICE_TLS_CA", "") or "").strip()
        profile_ca_directory = str(
            os.environ.get("GRAPH_SERVICE_TLS_CA_DIRECTORY", "") or ""
        ).strip()
        profile_selects_tls = bool(
            profile_ca
            or profile_ca_directory
            or env_client_cert
            or env_client_key
            or configured in {"1", "true", "yes", "on"}
        )
        profile_disables_tls = configured in {"0", "false", "no", "off"}

        if tls is True:
            enabled, profile = True, "explicit-arg"
        elif tls is False:
            enabled, profile = False, "explicit-arg"
        elif has_cert_arg or has_key_arg:
            # An explicit mTLS credential passed as a CALL argument is itself
            # an explicit selection (tier 1), distinct from the named-profile
            # tier below.
            enabled, profile = True, "explicit-arg"
        elif profile_disables_tls:
            enabled, profile = False, "named-profile"
        elif profile_selects_tls:
            enabled, profile = True, "named-profile"
        else:
            enabled, profile = False, "default"

        if not enabled and (
            has_cert_arg or has_key_arg or env_client_cert or env_client_key
        ):
            raise ValueError(
                "a client certificate was supplied but TLS is explicitly disabled "
                "for this connection"
            )

        trust_source = "none"
        if enabled:
            if profile_ca:
                trust_source = "ca_bundle"
            elif os.environ.get("SSL_CERT_FILE") or os.environ.get(
                "REQUESTS_CA_BUNDLE"
            ):
                trust_source = "ca_bundle"
            elif os.environ.get("GRAPH_SERVICE_TLS_CA_DIRECTORY") or os.environ.get(
                "SSL_CERT_DIR"
            ):
                trust_source = "ca_directory"
            else:
                trust_source = "system_default"

        return _TlsDecision(enabled, profile, server_hostname, trust_source)

    @classmethod
    def _resolve_tls(
        cls,
        tls: bool | ssl.SSLContext | None,
        *,
        client_cert: str | None,
        client_key: str | None,
        server_hostname: str | None = None,
    ) -> ssl.SSLContext | None:
        """Build the TLS context for a connection, if any (GOC-81 W01).

        The protocol MODE is decided exclusively by :meth:`_resolve_tls_decision`.
        Ambient CA variables belonging to unrelated HTTP libraries
        (``SSL_CERT_FILE``, ``SSL_CERT_DIR``, ``REQUESTS_CA_BUNDLE``) are
        consulted only below, to supply TRUST MATERIAL for a connection whose
        mode is already final -- never to select the mode itself.
        """
        if isinstance(tls, ssl.SSLContext):
            if client_cert or client_key:
                raise ValueError(
                    "client certificate paths cannot be combined with an injected TLS context"
                )
            return tls

        decision = cls._resolve_tls_decision(
            tls,
            client_cert=client_cert,
            client_key=client_key,
            server_hostname=server_hostname,
        )
        if not decision.enabled:
            return None

        allowed = str(
            os.environ.get("GRAPH_SERVICE_TLS_ALLOWED_SERVER_NAMES", "") or ""
        ).strip()
        if allowed and decision.server_name:
            allowed_names = {n.strip() for n in allowed.split(",") if n.strip()}
            if decision.server_name not in allowed_names:
                raise ValueError(
                    "TLS server name is outside the configured endpoint allowlist"
                )

        env_client_cert = str(
            os.environ.get("GRAPH_SERVICE_TLS_CLIENT_CERT", "") or ""
        ).strip()
        env_client_key = str(
            os.environ.get("GRAPH_SERVICE_TLS_CLIENT_KEY", "") or ""
        ).strip()
        profile_ca = str(os.environ.get("GRAPH_SERVICE_TLS_CA", "") or "").strip()
        # Trust material: the named profile's OWN CA source first; a generic,
        # unrelated-library CA variable is consulted only as a FALLBACK once
        # TLS is already selected -- the mode decision above is already final
        # and this can never reopen it.
        ca_bundle = (
            profile_ca
            or str(os.environ.get("SSL_CERT_FILE") or "").strip()
            or str(os.environ.get("REQUESTS_CA_BUNDLE") or "").strip()
        )
        ca_directory = str(
            os.environ.get("GRAPH_SERVICE_TLS_CA_DIRECTORY")
            or os.environ.get("SSL_CERT_DIR")
            or ""
        ).strip()
        try:
            context = ssl.create_default_context(
                cafile=ca_bundle or None,
                capath=ca_directory or None,
            )
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        except (OSError, ssl.SSLError, ValueError):
            raise ValueError(
                "native TCP TLS trust material is unavailable or invalid"
            ) from None
        cert = str(client_cert or env_client_cert).strip()
        key = str(client_key or env_client_key).strip()
        if bool(cert) != bool(key):
            raise ValueError("native TCP mTLS requires both client certificate and key")
        if cert and key:
            key_password = os.environ.get("GRAPH_SERVICE_TLS_CLIENT_KEY_PASSWORD")
            try:
                context.load_cert_chain(
                    certfile=cert,
                    keyfile=key,
                    password=key_password or None,
                )
            except (OSError, ssl.SSLError, ValueError):
                raise ValueError(
                    "native TCP mTLS identity is unavailable or invalid"
                ) from None
        # Sanitized diagnostics only -- mode/profile/trust-source TAGS, never
        # certificate/key paths or contents (GOC-81 W01 invariant 3).
        logger.debug(
            "epistemic-graph TLS mode selected: enabled=%s profile=%s trust_source=%s",
            decision.enabled,
            decision.profile,
            decision.trust_source,
        )
        return context

    @staticmethod
    async def _open_streams(
        socket_path: str | None,
        tcp_addr: str | None,
        connect_timeout: float | None,
        tls_context: ssl.SSLContext | None,
        tls_server_hostname: str | None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, str]:
        """Dial a fresh reader/writer pair to the engine.

        Returns ``(reader, writer, resolved_socket)`` — ``resolved_socket`` is the
        UDS path actually used (so reconnects target the same socket), or ``""``
        for a TCP endpoint. Shared by :meth:`connect` and :meth:`_reconnect`.
        """
        _conn_to = connect_timeout if connect_timeout else None
        if tcp_addr:
            if tcp_addr.startswith("[") and "]:" in tcp_addr:
                host, port_str = tcp_addr[1:].split("]:", 1)
            else:
                host, port_str = tcp_addr.rsplit(":", 1)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host,
                        int(port_str),
                        ssl=tls_context,
                        server_hostname=(
                            (tls_server_hostname or host)
                            if tls_context is not None
                            else None
                        ),
                        ssl_handshake_timeout=(
                            10.0 if tls_context is not None else None
                        ),
                    ),
                    _conn_to,
                )
            except (asyncio.TimeoutError, TimeoutError) as e:
                raise TimeoutError("epistemic-graph TCP connection timed out") from e
            logger.info(
                "Connected to epistemic-graph via native TCP (tls=%s)",
                tls_context is not None,
            )
            return reader, writer, ""

        _socket = socket_path or os.environ.get(
            "GRAPH_SERVICE_SOCKET",
            os.path.join(
                os.environ.get("XDG_RUNTIME_DIR", "/tmp"),  # nosec B108
                "epistemic-graph.sock",
            ),
        )
        if not os.path.exists(_socket):
            _tmp_socket = "/tmp/epistemic-graph.sock"  # nosec B108
            if os.path.exists(_tmp_socket):
                _socket = _tmp_socket
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(_socket), _conn_to
            )
        except (asyncio.TimeoutError, TimeoutError) as e:
            raise TimeoutError("epistemic-graph local connection timed out") from e
        logger.info("Connected to epistemic-graph via a private local socket")
        return reader, writer, _socket

    @classmethod
    async def connect(
        cls,
        socket_path: str | None = None,
        tcp_addr: str | None = None,
        auth_secret: str | None = None,
        graph_name: str = "__commons__",
        *,
        verified_context: RequestContextClaims | dict[str, Any],
        timeout: float | None = _DEFAULT_RPC_TIMEOUT,
        heavy_timeout: float | None = _HEAVY_RPC_TIMEOUT,
        connect_timeout: float | None = _CONNECT_TIMEOUT,
        tls: bool | ssl.SSLContext | None = None,
        tls_server_hostname: str | None = None,
        tls_client_cert: str | None = None,
        tls_client_key: str | None = None,
        node_id: str | None = None,
    ) -> EpistemicGraphClient:
        _secret = auth_secret or os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "")
        if not _secret:
            raise ValueError("a non-empty authentication secret is required")
        context = validate_request_context(verified_context)
        resolved_tls_server_hostname = (
            str(
                tls_server_hostname
                or os.environ.get("GRAPH_SERVICE_TLS_SERVER_NAME", "")
                or ""
            ).strip()
            or None
        )
        tls_context = cls._resolve_tls(
            tls,
            client_cert=tls_client_cert,
            client_key=tls_client_key,
            server_hostname=resolved_tls_server_hostname,
        )

        reader, writer, resolved_socket = await cls._open_streams(
            socket_path,
            tcp_addr,
            connect_timeout,
            tls_context,
            resolved_tls_server_hostname,
        )

        client = cls(
            reader,
            writer,
            _secret,
            graph_name,
            verified_context=context,
            timeout=timeout,
            heavy_timeout=heavy_timeout,
            node_id=node_id,
        )
        # Remember the endpoint so a dropped connection self-heals (KG-2.19).
        client._socket_path = resolved_socket or socket_path
        client._tcp_addr = tcp_addr
        client._connect_timeout = connect_timeout
        client._tls_context = tls_context
        client._tls_server_hostname = resolved_tls_server_hostname
        return client

    async def _reconnect(self) -> None:
        """Re-establish a dropped connection in place, on the same endpoint.

        A long-lived client's connection can die between calls — engine
        restart, an idle close, or a prior RPC that closed a poisoned stream
        (see ``_send``). Without recovery the client is permanently broken and
        the engine circuit breaker latches OPEN forever. Callers hold no
        reference to the underlying reader/writer, so dialing a fresh stream and
        swapping them in is transparent. Must be called with ``self._lock`` held
        (``close()`` also takes ``self._lock`` for its whole teardown, so a
        concurrent ``close()`` and a reconnect-in-progress can never interleave
        — GOC-81 W02's "close during connect" case).
        """
        # Tear down the old demux reader and fail any calls still bound to the
        # dead connection (CONCEPT:EG-KG.backend.framed-response) before swapping in the fresh stream.
        # Capture the task BEFORE `_mark_dead` (which requests cancellation and
        # clears `self._reader_task`) so it can be awaited below.
        old_task = self._reader_task
        self._mark_dead(ConnectionError("connection reset; reconnecting"))
        if old_task is not None and not old_task.done():
            # Await the old reader's actual cancellation instead of merely
            # requesting it (GOC-81 W02 invariant 5): without this, the old
            # task's except-clause could still be mid-flight when the fresh
            # reader/writer are swapped in below and (pre-fix) could flip
            # `_closed` back on for a connection it never belonged to. The
            # generation guard in `_read_loop` makes this belt-and-suspenders,
            # not the only line of defense. Bounded: a wedged old task must
            # never make reconnection itself hang forever.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(old_task, _CLOSE_TIMEOUT)
        self._close_writer_once()  # discard the poisoned stream
        self._reader, self._writer, _ = await self._open_streams(
            self._socket_path,
            self._tcp_addr,
            self._connect_timeout,
            self._tls_context,
            self._tls_server_hostname,
        )
        # New lifecycle generation: a reader task bound to the OLD generation
        # (already awaited above, but kept as a hard guard) can never mark
        # THIS connection dead.
        self._generation += 1
        self._closed = False
        # Re-negotiate capabilities against the fresh connection.
        self._server_ops = None

    # ── Internal ──────────────────────────────────────────────────────────

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def fresh_bolt_auth_token(self, graph: str | None = None) -> dict[str, str]:
        """Return one current signed auth-token map for a new Bolt connection.

        The credential is a hex-encoded MessagePack ``Health`` request carrying
        the same ``eg2.`` context envelope as the native protocol. Its nonce is
        consumed when Bolt accepts the connection, so callers must invoke this
        method once per physical connection instead of caching the result. The
        display principal is an opaque digest and is never an authority claim.
        """

        target_graph = graph or self._graph_name
        request: dict[str, Any] = {
            "id": self._next_id(),
            "graph": target_graph,
            "auth_token": "",  # nosec B105 - empty placeholder, real token computed below
            "method": "Health",
            "agent_id": str(self._effective_verified_context()["agent_id"]),
        }
        request["auth_token"] = self._compute_verified_token(request, None)
        principal = str(self._effective_verified_context()["principal"])
        return {
            "scheme": "epistemic",
            "principal": "principal:sha256:"
            + hashlib.sha256(principal.encode("utf-8")).hexdigest(),
            "credentials": msgpack.packb(request, use_bin_type=True).hex(),
        }

    def _effective_verified_context(self) -> RequestContextClaims:
        return self._verified_context_override.get() or self._verified_context

    @contextlib.contextmanager
    def use_verified_context(self, context: RequestContextClaims | dict[str, Any]):
        """Bind current claims for this task without mutating a shared client.

        ``ContextVar`` isolation makes this safe when one long-lived client is
        shared by concurrent GraphSessions.  The prior task-local binding is
        restored even when the routed operation raises.
        """

        value = validate_request_context(context)
        token = self._verified_context_override.set(value)
        try:
            yield self
        finally:
            self._verified_context_override.reset(token)

    def _verified_tenant(self) -> str:
        context = self._effective_verified_context()
        return str(context["tenant"])

    @staticmethod
    def _new_operation_idempotency_key() -> str:
        return "operation:sha256:" + hashlib.sha256(secrets.token_bytes(32)).hexdigest()

    def _sign_context_operation(
        self,
        *,
        domain: str,
        method: str,
        params: dict[str, Any],
        graph: str,
        idempotency_key: str,
        signer_id: str,
        signer_key: str,
        require_context_principal: bool = True,
    ) -> str:
        context = self._effective_verified_context()
        if (
            not isinstance(signer_id, str)
            or not signer_id.strip()
            or not isinstance(signer_key, str)
            or not signer_key
        ):
            raise ValueError("operation signer id and key must be non-empty")
        if require_context_principal and signer_id != context["principal"]:
            raise ValueError("identity signer must match the verified principal")
        unsigned_params = copy.deepcopy(params)
        if method == "RegisterIdentity":
            unsigned_params["signature"] = ""
        elif method == "ApplyMultisigMutation":
            unsigned_params["signatures"] = []
        else:
            raise ValueError("unsupported detached-signature operation")
        method_body = _canonical_method_body(method, unsigned_params)
        canonical = bytearray()
        for value in (
            domain,
            context["principal"],
            context["tenant"],
            context["audience"],
            context["agent_id"],
        ):
            _put_operation_bytes(canonical, value.encode("utf-8"))
        _put_operation_list(canonical, context["roles"])
        _put_operation_list(canonical, context["scopes"])
        _put_operation_bytes(canonical, context["policy_version"].encode("utf-8"))
        _put_operation_list(canonical, context["delegation"])
        _put_operation_bytes(canonical, idempotency_key.encode("utf-8"))
        _put_operation_bytes(canonical, graph.encode("utf-8"))
        _put_operation_bytes(canonical, method_body)
        digest = hashlib.sha256(canonical).digest()
        tag = hmac.new(signer_key.encode("utf-8"), digest, hashlib.sha256).hexdigest()
        return f"{signer_id}:{tag}"

    def _bind_change_envelope(
        self,
        params: dict[str, Any],
        *,
        request_id: int,
        graph: str,
    ) -> dict[str, Any]:
        verified_context = self._effective_verified_context()
        bound = copy.deepcopy(params)
        envelope = bound["envelope"]
        mutation = envelope["mutation"]
        if mutation["graph"] != graph:
            raise ValueError(
                "ChangeEnvelope mutation graph does not match the request graph"
            )
        tenant = str(verified_context["tenant"])
        mutation["tenant"] = tenant
        context_in = mutation["context"]
        context: dict[str, Any] = {
            "request_id": int(request_id),
            "principal": "principal:sha256:"
            + hashlib.sha256(
                str(verified_context["principal"]).encode("utf-8")
            ).hexdigest(),
        }
        if context_in.get("purpose") is not None:
            context["purpose"] = context_in["purpose"]
        context["policy_fingerprint"] = str(verified_context["policy_version"])
        if context_in.get("trace_id") is not None:
            context["trace_id"] = context_in["trace_id"]
        mutation["context"] = context
        for policy in envelope.get("policies", []):
            policy["tenant"] = tenant
        return bound

    def _bind_change_envelopes(
        self,
        params: dict[str, Any],
        *,
        request_id: int,
        graph: str,
    ) -> dict[str, Any]:
        """Stamp the verified request authority into EVERY envelope of a batch — the
        plural of :meth:`_bind_change_envelope`. All envelopes must target ``graph``."""
        verified_context = self._effective_verified_context()
        bound = copy.deepcopy(params)
        tenant = str(verified_context["tenant"])
        principal = (
            "principal:sha256:"
            + hashlib.sha256(
                str(verified_context["principal"]).encode("utf-8")
            ).hexdigest()
        )
        policy_version = str(verified_context["policy_version"])
        for envelope in bound["envelopes"]:
            mutation = envelope["mutation"]
            if mutation["graph"] != graph:
                raise ValueError(
                    "ChangeEnvelope batch mutation graph does not match the request graph"
                )
            mutation["tenant"] = tenant
            context_in = mutation["context"]
            context: dict[str, Any] = {
                "request_id": int(request_id),
                "principal": principal,
            }
            if context_in.get("purpose") is not None:
                context["purpose"] = context_in["purpose"]
            context["policy_fingerprint"] = policy_version
            if context_in.get("trace_id") is not None:
                context["trace_id"] = context_in["trace_id"]
            mutation["context"] = context
            for policy in envelope.get("policies", []):
                policy["tenant"] = tenant
        return bound

    def _compute_verified_token(
        self, request: dict[str, Any], idempotency_key: str | None
    ) -> str:
        context = self._effective_verified_context()
        method_name = str(request["method"])
        body = _canonical_method_body(
            method_name,
            request["params"] if "params" in request else None,
        )
        body_hash = hashlib.sha256(body).hexdigest()
        if not idempotency_key:
            material = (
                f"{request['id']}\0{request['graph']}\0{method_name}\0{body_hash}"
            ).encode()
            idempotency_key = "rpc:sha256:" + hashlib.sha256(material).hexdigest()
        timestamp = int(time.time())
        nonce = secrets.token_hex(24)
        roles = [str(value) for value in context.get("roles", [])]
        scopes = [str(value) for value in context.get("scopes", [])]
        delegation = [str(value) for value in context.get("delegation", [])]
        # ADR-3 / W1.9 node-bound envelopes: an explicit `node` claim on the
        # effective verified_context wins (a caller deliberately overrode it,
        # e.g. via `use_verified_context`); otherwise fall back to this
        # CONNECTION's own `node_id` (set by `connect()`/`ConnectionPool`,
        # i.e. which endpoint this client actually talks to). `None` when
        # neither is known -- the common case today, absent any topology
        # discovery -- and the claim is omitted entirely, exactly like a
        # client built before node binding existed.
        node_id = context.get("node")
        if not isinstance(node_id, str) or not node_id.strip():
            node_id = self._node_id

        canonical = bytearray()
        _put_v2_text(canonical, "eg-envelope-v2")
        canonical.extend(int(request["id"]).to_bytes(8, "big"))
        _put_v2_text(canonical, str(request["graph"]))
        _put_v2_text(canonical, method_name)
        _put_v2_text(canonical, body_hash)
        _put_v2_text(canonical, str(context["principal"]))
        _put_v2_text(canonical, str(context["tenant"]))
        _put_v2_text(canonical, str(context["audience"]))
        _put_v2_text(canonical, str(context["agent_id"]))
        _put_v2_list(canonical, roles)
        _put_v2_list(canonical, scopes)
        _put_v2_text(canonical, str(context["policy_version"]))
        _put_v2_list(canonical, delegation)
        canonical.extend(timestamp.to_bytes(8, "big"))
        _put_v2_text(canonical, nonce)
        _put_v2_text(canonical, idempotency_key)
        # Appended ONLY when a node id is known -- see `build_envelope_v2_bytes`
        # (crates/eg-types/src/protocol.rs) for the matching Rust encoding and
        # why this must stay byte-for-byte additive.
        if node_id:
            canonical.append(1)
            _put_v2_text(canonical, node_id)
        # W2.4 engine-native QoS lanes: the advisory admission-priority claim,
        # MAC-covered as a SECOND optional trailer with a DISTINCT tag byte
        # (`2`, vs the node trailer's `1`) so the two trailers stay unambiguous.
        # Matches the Rust `build_envelope_v2_bytes` encoding byte-for-byte.
        priority = context.get("priority")
        if isinstance(priority, str) and priority.strip():
            priority = priority.strip()
            canonical.append(2)
            _put_v2_text(canonical, priority)
        else:
            priority = None
        # ADR-4 decision 5: the optional OIDC bearer/assertion. Deliberately NOT
        # folded into the canonical MAC bytes (no tag-3 trailer) -- it rides as
        # a SIBLING top-level envelope field below, matching the Rust decode
        # shape (`EnvelopeV2.oidc_token`) and its own documented rationale: the
        # token's own RSA/JWKS signature is the trust anchor, and the engine's
        # `bind_verified_identity` independently cross-checks its subject/
        # tenant against this SAME `context`, so MAC coverage would add no
        # real protection (see `RequestContextClaims.oidc_token`'s docstring).
        oidc_token = context.get("oidc_token")
        if isinstance(oidc_token, str) and oidc_token.strip():
            oidc_token = oidc_token.strip()
        else:
            oidc_token = None
        mac = hmac.new(
            self._auth_secret.encode("utf-8"), bytes(canonical), hashlib.sha256
        ).hexdigest()
        context_payload: dict[str, Any] = {
            "principal": str(context["principal"]),
            "tenant": str(context["tenant"]),
            "audience": str(context["audience"]),
            "agent_id": str(context["agent_id"]),
            "roles": roles,
            "scopes": scopes,
            "policy_version": str(context["policy_version"]),
            "delegation": delegation,
        }
        if node_id:
            context_payload["node"] = node_id
        if priority:
            context_payload["priority"] = priority
        envelope: dict[str, Any] = {
            "context": context_payload,
            "timestamp": timestamp,
            "nonce": nonce,
            "idempotency_key": idempotency_key,
            "mac": mac,
        }
        if oidc_token:
            envelope["oidc_token"] = oidc_token
        payload = json.dumps(
            envelope, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return "eg2." + payload.hex()

    # ── CONCEPT:EG-KG.backend.framed-response — pipelined connection: reader/demux internals ──────────

    @staticmethod
    def _retrieve_exc(fut: asyncio.Future) -> None:
        # Mark a failed future "retrieved" so a caller that already moved on (e.g.
        # its write raced the reader's EOF and never reached ``await fut``) does not
        # emit a noisy "Future exception was never retrieved" warning.
        if not fut.cancelled():
            with contextlib.suppress(Exception):
                fut.exception()

    def _fail_pending(self, exc: BaseException) -> None:
        """Resolve every in-flight future with ``exc`` (a connection died)."""
        pending = self._pending
        self._pending = {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(exc)
                fut.add_done_callback(self._retrieve_exc)

    def _set_terminal_error(self, exc: BaseException) -> None:
        """Preserve the FIRST terminal error across repeated dead/close events
        (GOC-81 W02 invariant 6) — a later, possibly less informative error
        (e.g. "client closed" racing a real connection error) never overwrites
        the original cause.
        """
        if self._terminal_error is None:
            self._terminal_error = exc

    def _close_writer_once(self) -> None:
        """Request shutdown of the current writer at most once.

        Error/reconnect paths may mark a stream dead before the eventual
        explicit ``close()`` joins it.  ``StreamWriter.close()`` is itself
        idempotent, but avoiding the duplicate call keeps ownership explicit
        and lets the close path's single ``wait_closed()`` remain the durable
        shutdown boundary.  Small writer doubles used by engine-free tests do
        not necessarily expose ``is_closing()``, so absence means "not yet".
        """
        with contextlib.suppress(Exception):
            is_closing = getattr(self._writer, "is_closing", None)
            if callable(is_closing) and is_closing():
                return
            self._writer.close()

    def _mark_dead(self, exc: BaseException) -> None:
        """Tear the connection down: stop the reader, fail all in-flight calls.

        Idempotent. Called on any connection-fatal event detected from a
        *live* calling context (a timed-out/failed ``_send``, or the start of
        ``_reconnect``/``close``) — never from the background reader task
        itself, which uses :meth:`_on_reader_terminated` instead so a stale
        callback from a superseded connection cannot reach here. Marks
        ``_closed`` (ADMISSION state) so the next call self-heals via
        :meth:`_reconnect`; this alone never tears down the writer's final
        state — that is ``close()``'s job (GOC-81 W02).
        """
        self._closed = True
        self._set_terminal_error(exc)
        task = self._reader_task
        self._reader_task = None
        if task is not None and not task.done():
            task.cancel()
        self._close_writer_once()
        self._fail_pending(exc)

    def _on_reader_terminated(self, generation: int, exc: BaseException) -> None:
        """Handle the background reader's own EOF/decode-error termination.

        Only mutates shared state if ``generation`` still matches the LIVE
        connection (GOC-81 W02 invariant 5): a reader task bound to a
        connection already superseded by :meth:`_reconnect` must never be
        able to mark the CURRENT connection dead. This — not
        ``_mark_dead`` — is what a reader's own except-clauses call, so this
        generation check is the only path by which the reader can affect
        client state.
        """
        if generation != self._generation:
            return  # stale callback from a superseded connection generation
        self._closed = True
        self._set_terminal_error(exc)
        self._fail_pending(exc)

    async def _read_loop(self, reader: asyncio.StreamReader, generation: int) -> None:
        """Background demultiplexer: read frames, resolve futures by ``id``.

        One task per live connection, bound to the lifecycle ``generation``
        that was current when it started (GOC-81 W02). Responses arrive in
        ANY order (the engine pipelines, CONCEPT:EG-KG.backend.framed-response); each is
        routed to its caller by the ``Response.id`` correlation id the
        protocol already carries. On EOF / transport error every in-flight
        call is failed so no caller hangs and the next call reconnects —
        unless this task's generation has already been superseded, in which
        case its termination is a no-op (see :meth:`_on_reader_terminated`).
        """
        try:
            while True:
                len_buf = await reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                if msg_len <= 0 or msg_len > _MAX_RESPONSE_BYTES:
                    raise ConnectionError(
                        "epistemic-graph response exceeded the configured resource limit"
                    )
                body = await reader.readexactly(msg_len)
                resp = msgpack.unpackb(body, raw=False)
                if not isinstance(resp, dict) or not isinstance(resp.get("id"), int):
                    raise ConnectionError(
                        "epistemic-graph response is missing its correlation id"
                    )
                fut = self._pending.pop(resp["id"], None)
                if fut is not None and not fut.done():
                    fut.set_result(resp)
                # A response with no matching pending future (e.g. a late reply
                # for a timed-out call) is dropped — the demux keeps the stream
                # in sync regardless, which is exactly why one
                # slow/timed-out call no longer desyncs the others.
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, OSError):
            self._on_reader_terminated(
                generation, ConnectionError("Connection closed by server")
            )
        except Exception as e:  # noqa: BLE001 — surface any decode error to callers
            self._on_reader_terminated(
                generation,
                ConnectionError(
                    f"epistemic-graph response failed ({type(e).__name__})"
                ),
            )

    async def _ensure_connection(self) -> None:
        """Ensure a live stream + a running reader task (lifecycle lock held)."""
        async with self._lock:
            if self._closing:
                raise ConnectionError("client is closed")
            if self._closed:
                # A prior call closed a poisoned/dead stream. Re-dial in place so
                # this call succeeds instead of reusing a dead writer — otherwise
                # the engine circuit breaker latches OPEN permanently.
                await self._reconnect()
            if self._reader_task is None or self._reader_task.done():
                self._reader_task = asyncio.ensure_future(
                    self._read_loop(self._reader, self._generation)
                )

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        req_id = self._next_id()
        target_graph = graph or self._graph_name
        if method == "ApplyChangeEnvelope":
            if params is None:
                raise ValueError("ApplyChangeEnvelope requires an envelope")
            params = self._bind_change_envelope(
                params, request_id=req_id, graph=target_graph
            )
        elif method == "ApplyChangeEnvelopes":
            if params is None:
                raise ValueError("ApplyChangeEnvelopes requires envelopes")
            params = self._bind_change_envelopes(
                params, request_id=req_id, graph=target_graph
            )
        request: dict[str, Any] = {
            "id": req_id,
            "graph": target_graph,
            "auth_token": "",  # nosec B105 - empty placeholder, real token computed below
            "method": method,
        }
        verified_context = self._effective_verified_context()
        request["agent_id"] = str(verified_context["agent_id"])
        if params:
            request["params"] = params
        request["auth_token"] = self._compute_verified_token(request, idempotency_key)

        payload = msgpack.packb(request, use_bin_type=True)
        length_prefix = len(payload).to_bytes(4, byteorder="big")

        # Heavy ops (full-graph parse/scan/algorithms) get the longer read budget.
        timeout = self._heavy_timeout if method in _HEAVY_RPC_METHODS else self._timeout
        # The request flush is bounded separately and never longer than the read
        # budget: a slow drain means the engine has stopped reading (wedged), and
        # that is independent of how long its *response* may legitimately take.
        write_timeout = _WRITE_TIMEOUT if _WRITE_TIMEOUT else None
        if timeout is not None:
            write_timeout = min(timeout, write_timeout) if write_timeout else timeout

        # Establish/heal the connection and the background demux reader.
        await self._ensure_connection()

        # Register this call's future under its id BEFORE writing, so the reader
        # can never miss the response (the engine can't reply before it reads).
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            # Serialize ONLY the frame write so two callers never interleave bytes;
            # the round-trip itself is NOT held under any lock — that is what lets
            # independent concurrent calls pipeline on the one connection.
            async with self._write_lock:
                self._writer.write(length_prefix)
                self._writer.write(payload)
                await asyncio.wait_for(self._writer.drain(), write_timeout)
            # Await ONLY our own response; per-caller ordering is automatic.
            resp = await asyncio.wait_for(fut, timeout)
        except asyncio.CancelledError:
            # Caller cancellation is not a transport failure: keep the shared
            # connection reusable, but do not leave a cancelled request future
            # in the demux map until a late response happens to arrive.  A
            # late response is still safely dropped by the reader loop.
            self._pending.pop(req_id, None)
            raise
        except (asyncio.TimeoutError, TimeoutError) as e:
            # Bounded per-call timeout. Connection-fatal (parity with the pre-pipeline
            # contract): a wedged engine that stops replying must not strand the
            # connection. Tear it down so the pool/breaker reconnects on a clean
            # stream; the demux already kept the wire in sync, but a timeout still
            # means the peer is unhealthy.
            self._pending.pop(req_id, None)
            self._mark_dead(TimeoutError(f"epistemic-graph RPC {method!r} timed out"))
            raise TimeoutError(
                f"epistemic-graph RPC {method!r} timed out (connection closed; "
                "retry will reconnect)"
            ) from e
        except asyncio.IncompleteReadError as e:
            self._pending.pop(req_id, None)
            self._mark_dead(ConnectionError("Connection closed by server"))
            raise ConnectionError("Connection closed by server") from e
        except OSError as e:
            # Any transport-level error during write/drain — broken pipe, reset,
            # etc. (all OSError subclasses). A ConnectionError raised from our own
            # future (the reader saw EOF) also lands here. Mark dead so the NEXT
            # call reconnects instead of reusing a dead writer (which latched the
            # breaker OPEN forever). Re-raise unchanged; it trips the breaker.
            self._pending.pop(req_id, None)
            self._mark_dead(e)
            raise

        if resp.get("error") is not None:
            err_msg = resp.get("error", "Unknown error")
            detail = resp.get("result")
            if isinstance(detail, (bytes, bytearray)):
                with contextlib.suppress(Exception):
                    detail = msgpack.unpackb(detail, raw=False)
            if not isinstance(detail, dict) and isinstance(err_msg, str):
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    parsed = json.loads(err_msg)
                    if isinstance(parsed, dict):
                        detail = parsed
            if isinstance(detail, dict) and detail.get("status") == "redirected":
                operation = _exact_mapping(
                    "OperationResult",
                    detail,
                    frozenset(
                        {
                            "schema_version",
                            "operation_id",
                            "status",
                            "result_kind",
                            "result_ref",
                            "error",
                            "redirect",
                        }
                    ),
                )
                route = _exact_mapping(
                    "OperationRedirect",
                    operation["redirect"],
                    frozenset(
                        {
                            "kind",
                            "target_ref",
                            "group",
                            "epoch",
                            "fencing_token",
                            "leader_ref",
                        }
                    ),
                )
                if operation["schema_version"] != "1" or route["kind"] != "placement":
                    raise RuntimeError("invalid operation redirect")
                raise StaleRouteError("placement route redirected", route)
            # The engine's overload backstop (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation) returns a typed
            # RESULT_TOO_LARGE error for an oversize full-graph dump. Surface it as
            # a dedicated, catchable exception (still a RuntimeError subclass) so a
            # caller can fall back to a bounded query without string-matching.
            if isinstance(err_msg, str) and err_msg.startswith("RESULT_TOO_LARGE"):
                raise ResultTooLargeError(err_msg)
            raise RuntimeError(err_msg)
        result = resp.get("result")
        # Compact result encoding (engine Phase C-D): heavy algorithm results and
        # node/edge property blobs come back as a top-level MessagePack `bin` (the
        # `Raw`/`PropertiesMsgpack` payloads) — the server skips building a JSON
        # tree. Decode that second layer here so every caller receives the method's
        # declared result structure.
        if isinstance(result, (bytes, bytearray)):
            result = msgpack.unpackb(result, raw=False)
        return result

    # ── Connection Management ─────────────────────────────────────────────

    async def _finish_close(self, reader_task: asyncio.Task[None] | None) -> None:
        """Complete the one owned transport teardown for :meth:`close`.

        This runs in a task separate from the caller so cancellation of one
        close waiter cannot strand a writer after ``_closing`` is set.  The
        task is shared by all later close callers and every wait is bounded.
        """
        if reader_task is not None and not reader_task.done():
            reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(reader_task, _CLOSE_TIMEOUT)
        self._fail_pending(self._terminal_error or ConnectionError("client closed"))
        self._close_writer_once()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(self._writer.wait_closed(), _CLOSE_TIMEOUT)

    async def close(self) -> None:
        """Idempotent shutdown (GOC-81 W02).

        Safe under repeated calls, concurrent calls, peer EOF before close,
        close during an in-flight reconnect, and close during an in-flight
        request. A client that owns a transport closes it EXACTLY ONCE and
        always awaits final writer shutdown — regardless of reader-EOF
        ordering. The gate is ``self._closing`` (ownership state), never
        ``self._closed`` (admission state, which the background reader also
        flips on EOF/error): the old code's early return on ``self._closed``
        was exactly the bug — a reader that already observed EOF made every
        subsequent ``close()`` a silent no-op that never reached
        ``writer.close()``/``writer.wait_closed()``, leaking the transport.

        ``self._lock`` is the SAME lock ``_reconnect`` holds for its entire
        dial, so a close racing an in-flight reconnect simply waits for that
        dial to finish (bounded by the connect timeout) rather than needing
        separate close/reconnect coordination.
        """
        async with self._lock:
            if self._close_task is None:
                self._closing = True
                self._closed = True  # stop admitting new requests immediately
                exc = ConnectionError("client closed")
                self._set_terminal_error(exc)
                reader_task = self._reader_task
                self._reader_task = None
                self._close_task = asyncio.create_task(self._finish_close(reader_task))
            close_task = self._close_task

        assert close_task is not None
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            # Keep ownership of teardown in the shared task.  Finish it before
            # honoring the caller's cancellation so a canceled close cannot
            # leave `_closing=True` with an unjoined writer.
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(close_task)
            raise

    async def __aenter__(self) -> EpistemicGraphClient:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.close()

    # ── Service-Level ─────────────────────────────────────────────────────

    async def ping(self) -> str:
        return await self._send("Ping")

    async def health(self) -> dict[str, Any]:
        return await self._send("Health")

    async def cancel_request(self, target_req_id: int) -> bool:
        """Cooperatively cancel an IN-FLIGHT request by its ``target_req_id`` (L36) —
        e.g. a long-running :meth:`QueryClient.sql`/:meth:`~JobsClient.status` call
        still in flight on this SAME connection. Trips the target's
        ``CancellationToken`` if one is still registered; a streaming SQL read
        observes it at the next batch boundary and stops short.

        Returns ``True`` iff a live cancellable request was found and cancelled,
        ``False`` when it already finished, was never cancellable, or never
        existed — never an error (cancelling an already-completed request is a
        harmless no-op). Unconditional — no feature gate. Distinct from
        :meth:`JobsClient.cancel`, which cancels a DURABLE ``AnalyticsJob`` (a
        server-orchestrated background job, not an in-flight RPC on this
        connection)."""
        return await self._send("CancelRequest", {"target_req_id": target_req_id})

    @staticmethod
    def _validate_resource_stats_response(
        snapshot: Any, *, limit: int, summary: bool
    ) -> dict[str, Any]:
        if not isinstance(snapshot, dict):
            raise RuntimeError("ResourceStats response must be an object")
        graphs = snapshot.get("graphs", [])
        tenants = snapshot.get("tenants", [])
        if not isinstance(graphs, list) or not isinstance(tenants, list):
            raise RuntimeError("ResourceStats response arrays are malformed")
        if len(graphs) > limit:
            raise RuntimeError("ResourceStats response exceeded its bounded graph page")
        if len(tenants) > _MAX_RESOURCE_STATS_TENANTS:
            raise RuntimeError(
                "ResourceStats response exceeded its bounded tenant page"
            )
        if summary and (graphs or tenants):
            raise RuntimeError("summary ResourceStats response must omit detail arrays")
        next_cursor = snapshot.get("next_cursor")
        if next_cursor is not None and (
            not isinstance(next_cursor, str)
            or not next_cursor
            or len(next_cursor.encode("utf-8")) > _MAX_RESOURCE_STATS_CURSOR_BYTES
        ):
            raise RuntimeError("ResourceStats response carried an invalid next_cursor")
        if not isinstance(snapshot.get("has_more", False), bool):
            raise RuntimeError("ResourceStats response has_more must be boolean")
        return snapshot

    async def resource_stats(
        self,
        *,
        cursor: str | None = None,
        limit: int = _DEFAULT_RESOURCE_STATS_LIMIT,
        summary: bool = False,
    ) -> dict[str, Any]:
        """Return the per-tenant / per-graph resource snapshot (CONCEPT:EG-KG.compute.lane-v).

        The autoscale signals an external autoscaler (agent-utilities OS-5.27)
        consumes in ONE round-trip: per-graph + per-tenant resident memory, node/edge
        counts, in-flight admission depth, hibernated-vs-resident counts, effective
        cgroup capacity, coalescer queue gauges, and cumulative budget totals, plus a
        process aggregate.  The legacy no-argument call is intentionally preserved,
        but now receives the same finite default page as ``ResourceStatsPage``.

        ``cursor`` is the exclusive ``next_cursor`` from a prior page.  ``summary``
        suppresses both detail arrays while retaining aggregate signals; it cannot be
        combined with a cursor.
        The ``cost`` feature is part of the mandatory main build and remains present in
        the source-built ``cluster`` and ``full-extras`` layers.
        """
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("ResourceStats limit must be an integer")
        if not isinstance(summary, bool):
            raise TypeError("ResourceStats summary must be a boolean")
        if limit < 1 or limit > _MAX_RESOURCE_STATS_LIMIT:
            raise ValueError(
                f"ResourceStats limit must be between 1 and {_MAX_RESOURCE_STATS_LIMIT}"
            )
        if cursor is not None:
            if not isinstance(cursor, str):
                raise TypeError("ResourceStats cursor must be a string or None")
            cursor_bytes = cursor.encode("utf-8")
            if not cursor or len(cursor_bytes) > _MAX_RESOURCE_STATS_CURSOR_BYTES:
                raise ValueError(
                    "ResourceStats cursor must be non-empty and at most "
                    f"{_MAX_RESOURCE_STATS_CURSOR_BYTES} bytes"
                )
            if "\x00" in cursor:
                raise ValueError("ResourceStats cursor must not contain NUL")
        if summary and cursor is not None:
            raise ValueError("summary ResourceStats cannot be combined with cursor")

        # Keep the exact legacy unit request/MAC shape for the default call.  A
        # caller asking for any non-default behavior uses the typed page variant.
        if cursor is None and not summary and limit == _DEFAULT_RESOURCE_STATS_LIMIT:
            result = await self._send("ResourceStats")
        else:
            result = await self._send(
                "ResourceStatsPage",
                {"cursor": cursor, "limit": limit, "summary": summary},
            )
        return self._validate_resource_stats_response(
            result, limit=limit, summary=summary
        )

    async def supports(self, op: str) -> bool:
        """True if the connected engine advertises protocol op ``op``.

        Capability negotiation (CONCEPT:EG-KG.query.wire-protocol): the current
        server's ``Health`` response carries a required string ``ops`` list. The
        probe is cached for the connection's life; an incomplete response is a
        protocol error.
        """
        ops = getattr(self, "_server_ops", None)
        if ops is None:
            health = await self.health()
            advertised = health.get("ops") if isinstance(health, dict) else None
            if not isinstance(advertised, list) or not all(
                isinstance(value, str) and value for value in advertised
            ):
                raise RuntimeError(
                    "current protocol Health response must include a string ops list"
                )
            ops = set(advertised)
            self._server_ops = ops
        return op in ops

    async def reconcile(self, graph_name: str, json_str: str) -> str:
        return await self._send(
            "Reconcile", {"graph_name": graph_name, "json_str": json_str}
        )

    async def shutdown(self) -> str:
        return await self._send("Shutdown")

    async def apply_mutation(self, event_type: str, query: str) -> str:
        return await self._send(
            "ApplyMutation", {"event_type": event_type, "query": query}
        )


class SyncEpistemicGraphClient:
    """Synchronous wrapper around the namespaced async client."""

    def __init__(
        self,
        async_client: EpistemicGraphClient,
        loop: asyncio.AbstractEventLoop,
        thread: threading.Thread,
    ) -> None:
        self._client = async_client
        self._loop = loop
        self._thread = thread
        self._close_lock = threading.Lock()
        self._async_client_closed = False

        # We need to wrap the namespaces synchronously as well
        self.nodes = self._SyncWrapper(self._client.nodes, self._loop)
        self.work_items = self._SyncWrapper(self._client.work_items, self._loop)
        self.capacity_leases = self._SyncWrapper(
            self._client.capacity_leases, self._loop
        )
        self.development_lanes = self._SyncWrapper(
            self._client.development_lanes, self._loop
        )
        self.changes = self._SyncWrapper(self._client.changes, self._loop)
        self.edges = self._SyncWrapper(self._client.edges, self._loop)
        self.graph = self._SyncWrapper(self._client.graph, self._loop)
        self.analytics = self._SyncWrapper(self._client.analytics, self._loop)
        self.lifecycle = self._SyncWrapper(self._client.lifecycle, self._loop)
        self.reasoning = self._SyncWrapper(self._client.reasoning, self._loop)
        self.ledger = self._SyncWrapper(self._client.ledger, self._loop)
        self.channels = self._SyncWrapper(self._client.channels, self._loop)
        self.tenants = self._SyncWrapper(self._client.tenants, self._loop)
        self.resharding = self._SyncWrapper(self._client.resharding, self._loop)
        self.placement = self._SyncWrapper(self._client.placement, self._loop)
        self.cluster_topology = self._SyncWrapper(
            self._client.cluster_topology, self._loop
        )
        self.server_registry = self._SyncWrapper(
            self._client.server_registry, self._loop
        )
        self.raft_admin = self._SyncWrapper(self._client.raft_admin, self._loop)
        self.consensus = self._SyncWrapper(self._client.consensus, self._loop)
        self.finance = self._SyncWrapper(self._client.finance, self._loop)
        self.datascience = self._SyncWrapper(self._client.datascience, self._loop)
        self.mining = self._SyncWrapper(self._client.mining, self._loop)
        self.graphlearn = self._SyncWrapper(self._client.graphlearn, self._loop)
        self.pipeline = self._SyncWrapper(self._client.pipeline, self._loop)
        self.query = self._SyncWrapper(self._client.query, self._loop)
        self.knowledge = self._SyncWrapper(self._client.knowledge, self._loop)
        self.modalities = self._SyncWrapper(self._client.modalities, self._loop)
        self.txn = self._SyncWrapper(self._client.txn, self._loop)
        self.timeseries = self._SyncWrapper(self._client.timeseries, self._loop)
        self.rdf = self._SyncWrapper(self._client.rdf, self._loop)
        self.streaming = self._SyncWrapper(self._client.streaming, self._loop)
        self.blob = self._SyncWrapper(self._client.blob, self._loop)
        # CONCEPT:EG-KG.ingest.broker-streams-namespaces — B1.7 broker/streams + RBAC + backup namespaces.
        self.broker = self._SyncWrapper(self._client.broker, self._loop)
        self.rbac = self._SyncWrapper(self._client.rbac, self._loop)
        self.admin = self._SyncWrapper(self._client.admin, self._loop)
        self.jobs = self._SyncWrapper(self._client.jobs, self._loop)
        self.statechart = self._SyncWrapper(self._client.statechart, self._loop)
        self.viz = self._SyncWrapper(self._client.viz, self._loop)
        self.quantum = self._SyncWrapper(self._client.quantum, self._loop)
        self.asr = self._SyncWrapper(self._client.asr, self._loop)

    def clear(self) -> None:
        """Synchronously clear the graph (used primarily by the test suite teardown)."""
        future = asyncio.run_coroutine_threadsafe(
            self._client._send("ClearGraph"), self._loop
        )
        return future.result()

    def supports(self, op: str) -> bool:
        """Synchronously negotiate one advertised operation.

        Keep this explicit rather than relying on ``__getattr__`` so adapters can
        probe capability on both client variants without reaching into the owned
        async client.  The same bounded future helper used by every synchronous
        namespace preserves ``sync_call_deadline`` semantics.
        """
        future = asyncio.run_coroutine_threadsafe(self._client.supports(op), self._loop)
        return bool(_sync_result_before_deadline(future))

    class _SyncWrapper:
        def __init__(
            self, async_namespace: Any, loop: asyncio.AbstractEventLoop
        ) -> None:
            self._namespace = async_namespace
            self._loop = loop

        def __getattr__(self, name: str) -> Any:
            attr = getattr(self._namespace, name)
            if inspect.iscoroutinefunction(attr):

                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    future = asyncio.run_coroutine_threadsafe(
                        attr(*args, **kwargs), self._loop
                    )
                    return _sync_result_before_deadline(future)

                return sync_wrapper
            return attr

    @classmethod
    def connect(cls, **kwargs: Any) -> SyncEpistemicGraphClient:
        loop = asyncio.new_event_loop()

        def run_loop() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()

        future = asyncio.run_coroutine_threadsafe(
            EpistemicGraphClient.connect(**kwargs), loop
        )
        async_client: EpistemicGraphClient | None = None
        try:
            async_client = future.result()
            return cls(async_client, loop, thread)
        except BaseException:
            if not future.done():
                future.cancel()
                with contextlib.suppress(BaseException):
                    future.result(timeout=5)
            if async_client is not None:
                cls._close_async_client(async_client, loop)
            cls._stop_loop(loop, thread)
            raise

    @staticmethod
    def _close_async_client(
        async_client: EpistemicGraphClient,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Best-effort close of the async transport while its loop still runs."""
        if loop.is_closed():
            return
        close_coro = async_client.close()
        try:
            future = asyncio.run_coroutine_threadsafe(close_coro, loop)
        except RuntimeError:
            close_coro.close()
            logger.debug(
                "Sync client loop stopped before async client close could be queued",
                exc_info=True,
            )
            return
        try:
            future.result(timeout=5)
        except Exception:
            logger.debug("Error closing sync client async transport", exc_info=True)

    @staticmethod
    def _stop_loop(
        loop: asyncio.AbstractEventLoop,
        thread: threading.Thread,
    ) -> None:
        """Stop one owned loop and release its selector resources after joining."""
        if not loop.is_closed():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                logger.debug("Sync client loop closed before stop", exc_info=True)
        if thread is not threading.current_thread():
            thread.join(timeout=2)
        if thread.is_alive():
            logger.error("Sync client loop thread did not stop within two seconds")
            return
        if not loop.is_closed():
            try:
                loop.close()
            except Exception:
                logger.debug("Error closing sync client event loop", exc_info=True)

    def close(self) -> None:
        with self._close_lock:
            close_async_client = not self._async_client_closed
            self._async_client_closed = True
        if close_async_client:
            self._close_async_client(self._client, self._loop)
        self._stop_loop(self._loop, self._thread)

    def __enter__(self) -> SyncEpistemicGraphClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if inspect.iscoroutinefunction(attr):

            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                future = asyncio.run_coroutine_threadsafe(
                    attr(*args, **kwargs), self._loop
                )
                return _sync_result_before_deadline(future)

            return sync_wrapper
        return attr
