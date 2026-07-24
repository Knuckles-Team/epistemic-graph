"""Verified v2 signing and privacy-safe ChangeEnvelope client contracts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

import msgpack
import pytest

from epistemic_graph.client import (
    ChangeEnvelopeClient,
    EpistemicGraphClient,
    TimeSeriesClient,
    _canonical_method_body,
    validate_request_context,
)

# Every test in this file is pure client-side logic: `EpistemicGraphClient` is
# always constructed over dummy `object()` reader/writer (never a real
# connection), and the two `async def` tests monkeypatch `client._send`
# instead of performing I/O. No test here needs the shared native engine.
pytestmark = pytest.mark.no_engine


def _context() -> dict[str, object]:
    return {
        "principal": "subject-opaque",
        "tenant": "tenant-fixture",
        "audience": "engine-fixture",
        "agent_id": "agent-fixture",
        "roles": ["ingestor"],
        "scopes": ["ingest:*"],
        "policy_version": "policy-v1",
        "delegation": ["subject-opaque", "agent-fixture"],
    }


def _envelope() -> dict[str, object]:
    digest = "a" * 64
    return {
        "schema_version": 1,
        "envelope_id": "envelope-fixture",
        "mutation": {
            "schema_version": 2,
            "batch_id": "batch-fixture",
            "context": {"request_id": 0, "principal": "caller-value"},
            "tenant": "caller-value",
            "graph": "graph-fixture",
            "placement_epoch": 0,
            "idempotency_key": "idempotency-fixture",
            "operations": [
                {
                    "ordinal": 0,
                    "surface": "graph",
                    "domain": "graph_rows",
                    "method": {
                        "method": "AddNode",
                        "params": {
                            "node_id": "node-fixture",
                            "properties_msgpack": b"\x80",
                        },
                    },
                }
            ],
            "outbox": [],
            "created_at_ms": 1,
        },
        "content_version": {
            "object_id": "node-fixture",
            "digest_algorithm": "sha256",
            "digest": digest,
            "source_version": {"kind": "sequence", "value": 1},
        },
        "blobs": [],
        "features": [],
        "evidence": [],
        "policies": [
            {
                "policy_id": "policy-fixture",
                "operation": "upsert",
                "object_id": "node-fixture",
                "classification": "internal",
                "policy_version": "policy-v1",
                "subject_set_digest": "b" * 64,
            }
        ],
        "lineage": [],
        "privacy": {
            "policy_version": "privacy-v1",
            "sanitizer_version": "sanitizer-v1",
            "sanitized_payload_digest": "c" * 64,
        },
    }


def test_change_binding_uses_verified_tenant_and_opaque_principal() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    canonical = ChangeEnvelopeClient._canonical(_envelope())
    bound = client._bind_change_envelope(
        {"envelope": canonical}, request_id=7, graph="graph-fixture"
    )
    mutation = bound["envelope"]["mutation"]
    assert mutation["tenant"] == "tenant-fixture"
    assert mutation["context"]["request_id"] == 7
    assert mutation["context"]["principal"] == (
        "principal:sha256:" + hashlib.sha256(b"subject-opaque").hexdigest()
    )
    assert mutation["context"]["principal"] != "subject-opaque"
    assert bound["envelope"]["policies"][0]["tenant"] == "tenant-fixture"


def test_change_canonical_rejects_old_or_incomplete_mutation_contract() -> None:
    old = _envelope()
    old["mutation"]["schema_version"] = 1  # type: ignore[index]
    with pytest.raises(ValueError, match="schema_version must be 2"):
        ChangeEnvelopeClient._canonical(old)

    missing_outbox = _envelope()
    del missing_outbox["mutation"]["outbox"]  # type: ignore[index]
    with pytest.raises(ValueError, match="missing required fields: outbox"):
        ChangeEnvelopeClient._canonical(missing_outbox)

    unknown = _envelope()
    unknown["mutation"]["retired_domain_hint"] = "graph"  # type: ignore[index]
    with pytest.raises(ValueError, match="unsupported fields: retired_domain_hint"):
        ChangeEnvelopeClient._canonical(unknown)


def test_bolt_auth_token_is_fresh_signed_request_with_opaque_display_principal() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    first = client.fresh_bolt_auth_token()
    second = client.fresh_bolt_auth_token()
    assert first["scheme"] == "epistemic"
    assert first["principal"].startswith("principal:sha256:")
    assert first["principal"] != "subject-opaque"
    assert first["credentials"] != second["credentials"]
    request = msgpack.unpackb(bytes.fromhex(first["credentials"]), raw=False)
    assert request["method"] == "Health"
    assert request["graph"] == "graph-fixture"
    assert request["agent_id"] == "agent-fixture"
    assert request["auth_token"].startswith("eg2.")


def test_v2_token_contains_no_secret_and_binds_stable_idempotency_key() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    request = {
        "id": 9,
        "graph": "graph-fixture",
        "method": "GetContentVersion",
        "params": {"object_id": "node-fixture", "tenant": "tenant-fixture"},
    }
    token = client._compute_verified_token(request, "idempotency-fixture")
    assert token.startswith("eg2.")
    payload = json.loads(bytes.fromhex(token.removeprefix("eg2.")).decode("utf-8"))
    assert payload["idempotency_key"] == "idempotency-fixture"
    assert payload["context"] == _context()
    assert "fixture-secret" not in token


# ── ADR-3 / W1.9: node-bound envelopes ───────────────────────────────────────


def _node_bound_request() -> dict[str, object]:
    return {
        "id": 20,
        "graph": "graph-fixture",
        "method": "GetContentVersion",
        "params": {"object_id": "node-fixture", "tenant": "tenant-fixture"},
    }


def _decode_envelope(token: str) -> dict[str, object]:
    return json.loads(bytes.fromhex(token.removeprefix("eg2.")).decode("utf-8"))


def test_v2_token_includes_node_claim_when_connection_node_id_is_known() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
        node_id="node-a",
    )
    payload = _decode_envelope(
        client._compute_verified_token(_node_bound_request(), "idempotency-fixture")
    )
    assert payload["context"] == {**_context(), "node": "node-a"}


def test_v2_token_omits_node_claim_when_unknown() -> None:
    """The default -- no `node_id` on connect -- must be genuinely additive:
    the exact pre-ADR-3 payload shape, not a `"node": null` key."""

    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    payload = _decode_envelope(
        client._compute_verified_token(_node_bound_request(), "idempotency-fixture")
    )
    assert "node" not in payload["context"]
    assert payload["context"] == _context()


def test_v2_token_node_claim_changes_the_mac() -> None:
    """Different target nodes must not share a MAC (ADR-3 / W1.9): the node
    claim is part of the SIGNED context, not an unsigned label appended after
    signing."""

    client_a = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
        node_id="node-a",
    )
    client_b = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
        node_id="node-b",
    )
    payload_a = _decode_envelope(
        client_a._compute_verified_token(_node_bound_request(), "idempotency-fixture")
    )
    payload_b = _decode_envelope(
        client_b._compute_verified_token(_node_bound_request(), "idempotency-fixture")
    )
    assert payload_a["context"]["node"] == "node-a"
    assert payload_b["context"]["node"] == "node-b"
    assert payload_a["mac"] != payload_b["mac"]


def test_two_connections_with_different_node_ids_mint_independently() -> None:
    """Simulates ShardRouter/ConnectionPool routing to a DIFFERENT node on a
    later call (ADR-3: "On failover retry to a different node, re-mint"): a
    second client bound to a different endpoint's node_id naturally signs its
    own envelope with its own claim -- no shared, stale, or cached state."""

    request = _node_bound_request()
    first = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
        node_id="node-a",
    )
    second = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
        node_id="node-b",
    )
    token_first = first._compute_verified_token(dict(request), None)
    token_second = second._compute_verified_token(dict(request), None)
    assert _decode_envelope(token_first)["context"]["node"] == "node-a"
    assert _decode_envelope(token_second)["context"]["node"] == "node-b"
    # Re-minting on the SAME connection also produces a fresh nonce/mac each
    # call (never a cached/reused envelope).
    token_first_again = first._compute_verified_token(dict(request), None)
    assert _decode_envelope(token_first_again)["nonce"] != _decode_envelope(token_first)["nonce"]


def test_explicit_context_node_overrides_connection_node_id() -> None:
    """A caller that deliberately sets `node` on the effective verified_context
    (e.g. via `use_verified_context`) wins over the connection's own node_id."""

    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
        node_id="connection-node",
    )
    with client.use_verified_context({**_context(), "node": "override-node"}):
        payload = _decode_envelope(
            client._compute_verified_token(_node_bound_request(), "idempotency-fixture")
        )
    assert payload["context"]["node"] == "override-node"


def test_validate_request_context_accepts_optional_node_claim() -> None:
    validated = validate_request_context({**_context(), "node": "node-a"})
    assert validated["node"] == "node-a"
    validated_absent = validate_request_context(_context())
    assert "node" not in validated_absent


@pytest.mark.parametrize("bad_node", ["", "   ", 42])
def test_validate_request_context_rejects_invalid_node_claim(bad_node: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        validate_request_context({**_context(), "node": bad_node})


# ── ADR-4 decision 5 / W2.1-1: the optional OIDC bearer-token claim ─────────
#
# Unlike `node`/`priority` (MAC-covered tag-1/tag-2 trailers), `oidc_token`
# rides as a SIBLING top-level envelope field -- matching the Rust decode
# shape (`EnvelopeV2.oidc_token`, `src/server/auth.rs`) -- and is deliberately
# NOT folded into the canonical MAC bytes: the token's own RSA/JWKS signature
# is the trust anchor, and the engine's `bind_verified_identity` independently
# cross-checks its subject/tenant against `context`, so MAC coverage would add
# no real protection (see `build_envelope_v2_bytes`'s doc comment in
# `crates/eg-types/src/protocol.rs`).


def test_v2_token_includes_oidc_token_when_present() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context={**_context(), "oidc_token": "eyJhbGciOiJSUzI1NiJ9.fixture.sig"},
    )
    payload = _decode_envelope(
        client._compute_verified_token(_node_bound_request(), "idempotency-fixture")
    )
    # Carried at the TOP level, sibling to `context` -- never nested inside it
    # (the Rust `RequestContextClaims` struct is `deny_unknown_fields`, so
    # nesting it there would break decoding).
    assert payload["oidc_token"] == "eyJhbGciOiJSUzI1NiJ9.fixture.sig"
    assert "oidc_token" not in payload["context"]
    assert payload["context"] == _context()


def test_v2_token_omits_oidc_token_when_absent() -> None:
    """Genuinely additive: no claim set -> no `oidc_token` key at all (not a
    `null`), so an un-upgraded caller's envelope is byte-for-byte the same
    shape as before this claim existed."""

    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    payload = _decode_envelope(
        client._compute_verified_token(_node_bound_request(), "idempotency-fixture")
    )
    assert "oidc_token" not in payload
    assert payload["context"] == _context()


def test_v2_token_oidc_token_does_not_change_the_mac(monkeypatch) -> None:
    """The inverse of `test_v2_token_node_claim_changes_the_mac`: the token's
    own signature is the trust anchor, not the HMAC, so two envelopes that
    differ ONLY in `oidc_token` must sign IDENTICALLY.

    `_compute_verified_token` mints a fresh timestamp/nonce every call (both
    MAC-covered), so a naive cross-call MAC comparison would differ for that
    reason alone regardless of `oidc_token` -- freeze both to isolate
    `oidc_token` as the ONLY variable between calls.
    """
    import epistemic_graph.client as client_module

    monkeypatch.setattr(client_module.time, "time", lambda: 1_700_000_000)
    monkeypatch.setattr(client_module.secrets, "token_hex", lambda _n: "fixed-nonce")

    client_no_token = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    client_with_token = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context={**_context(), "oidc_token": "token-a"},
    )
    client_with_other_token = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context={**_context(), "oidc_token": "token-b"},
    )
    request = _node_bound_request()
    payload_absent = _decode_envelope(
        client_no_token._compute_verified_token(dict(request), "idempotency-fixture")
    )
    payload_a = _decode_envelope(
        client_with_token._compute_verified_token(dict(request), "idempotency-fixture")
    )
    payload_b = _decode_envelope(
        client_with_other_token._compute_verified_token(dict(request), "idempotency-fixture")
    )
    assert payload_absent["nonce"] == payload_a["nonce"] == payload_b["nonce"], (
        "the nonce freeze must actually be effective, or this test proves nothing"
    )
    assert "oidc_token" not in payload_absent
    assert payload_a["oidc_token"] == "token-a"
    assert payload_b["oidc_token"] == "token-b"
    assert payload_a["mac"] == payload_absent["mac"] == payload_b["mac"], (
        "oidc_token must NOT affect the MAC-covered bytes"
    )


def test_validate_request_context_accepts_optional_oidc_token_claim() -> None:
    validated = validate_request_context({**_context(), "oidc_token": "token-value"})
    assert validated["oidc_token"] == "token-value"
    validated_absent = validate_request_context(_context())
    assert "oidc_token" not in validated_absent


@pytest.mark.parametrize("bad_oidc_token", ["", "   ", 42])
def test_validate_request_context_rejects_invalid_oidc_token_claim(
    bad_oidc_token: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        validate_request_context({**_context(), "oidc_token": bad_oidc_token})


def test_signed_f32_body_matches_rust_rmp_serde_fixture() -> None:
    """The fixture is Rust ``rmp_serde::to_vec_named(Method::AddEmbedding)``."""

    params = {"node_id": "v", "embedding": [1.0, 0.5]}
    body = _canonical_method_body("AddEmbedding", params)
    assert body.hex() == (
        "82a66d6574686f64ac416464456d62656464696e67"
        "a6706172616d7382a76e6f64655f6964a176"
        "a9656d62656464696e6792ca3f800000ca3f000000"
    )
    assert params == {"node_id": "v", "embedding": [1.0, 0.5]}


def test_v2_signer_derives_idempotency_from_typed_f32_body() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    request = {
        "id": 11,
        "graph": "graph-fixture",
        "method": "AddEmbedding",
        "params": {"node_id": "v", "embedding": [1.0, 0.5]},
    }
    token = client._compute_verified_token(request, None)
    payload = json.loads(bytes.fromhex(token.removeprefix("eg2.")).decode("utf-8"))
    body_hash = hashlib.sha256(
        _canonical_method_body(request["method"], request["params"])
    ).hexdigest()
    material = (
        f"{request['id']}\0{request['graph']}\0{request['method']}\0{body_hash}"
    ).encode()
    assert payload["idempotency_key"] == (
        "rpc:sha256:" + hashlib.sha256(material).hexdigest()
    )


def test_signer_preserves_every_current_rust_f32_schema_location() -> None:
    root = Path(__file__).parents[1] / "crates" / "eg-types" / "src"
    declared = Counter(
        (path.relative_to(root).as_posix(), line.strip())
        for path in root.rglob("*.rs")
        for line in path.read_text(encoding="utf-8").splitlines()
        if re.search(r"\bf32\b", line) and not line.lstrip().startswith("//")
    )
    assert declared == Counter(
        [
            ("knowledge_stream.rs", "query_embedding: Vec<f32>,"),
            ("modality.rs", "minimum_rms: f32,"),
            ("protocol.rs", "embedding: Vec<f32>,"),
            ("protocol.rs", "embedding: Vec<f32>,"),
            ("protocol.rs", "query_embedding: Vec<f32>,"),
            ("protocol.rs", "query_embedding: Vec<f32>,"),
            ("protocol.rs", "summary_embedding: Option<Vec<f32>>,"),
            # NodeData is a stored/result DTO, never nested in Method request bodies.
            ("types.rs", "pub embedding: Option<Vec<f32>>,"),
            ("wire.rs", "FuseRrf { branches: Vec<Vec<Op>>, k: f32 },"),
            ("wire.rs", "Rank { query: Vec<f32> },"),
            ("wire.rs", "RankMmr { lambda: f32, k: usize },"),
        ]
    )
    method_carriers = Counter(
        (path.relative_to(root).as_posix(), line.strip())
        for path in root.rglob("*.rs")
        for line in path.read_text(encoding="utf-8").splitlines()
        if re.search(r"\bpub method: Method,", line)
    )
    assert method_carriers == Counter(
        [
            ("mutation_batch.rs", "pub method: Method,"),
            ("protocol.rs", "pub method: Method,"),
        ]
    )

    direct_cases = {
        "CloseChannel": {
            "channel_id": "c",
            "summary_embedding": [1.0, 0.5],
            "topic_metadata": None,
        },
        "AddEmbedding": {"node_id": "v", "embedding": [1.0, 0.5]},
        "SemanticSearch": {"query_embedding": [1.0, 0.5], "n_results": 2},
        "Discover": {"keywords": [], "query_embedding": [1.0, 0.5], "k": 2},
        "TxnAddEmbedding": {
            "txn_id": "t",
            "node_id": "v",
            "embedding": [1.0, 0.5],
            "graph": None,
        },
    }
    for method, params in direct_cases.items():
        body = _canonical_method_body(method, params)
        assert body.count(b"\xca") == 2
        assert body.count(b"\xcb") == 0

    plan = {
        "ops": [
            {"SpatialScan": {"layer": "l", "bbox": [0.0, 0.0, 1.0, 1.0]}},
            {"Rank": {"query": [1.0, 0.5]}},
            {"RankMmr": {"lambda": 0.25, "k": 2}},
            {
                "FuseRrf": {
                    "branches": [[{"Rank": {"query": [0.75]}}]],
                    "k": 60.0,
                }
            },
        ]
    }
    plan_methods = (
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
    )
    protocol_lines = (root / "protocol.rs").read_text(encoding="utf-8").splitlines()
    current_variant = ""
    declared_plan_methods: set[str] = set()
    in_method_enum = False
    for line in protocol_lines:
        if line == "pub enum Method {":
            in_method_enum = True
            continue
        if not in_method_enum:
            continue
        if line == "}":
            break
        variant = re.fullmatch(r"    ([A-Za-z][A-Za-z0-9_]*) \{", line)
        if variant:
            current_variant = variant.group(1)
        if re.search(
            r"\bplan: (?:crate::wire::Plan|Option<crate::wire::Plan>),", line
        ):
            assert current_variant
            declared_plan_methods.add(current_variant)
    assert declared_plan_methods == set(plan_methods)

    for method in plan_methods:
        plan_body = _canonical_method_body(method, {"plan": plan})
        assert plan_body.count(b"\xca") == 5, method
        assert plan_body.count(b"\xcb") == 4, method

    knowledge_body = _canonical_method_body(
        "KnowledgeStream",
        {
            "request": {
                "query": {
                    "family": "vector",
                    "keywords": [],
                    "query_embedding": [1.0, 0.5],
                    "k": 2,
                },
                "batch_size": 2,
                "projection": "arrow_ipc_v1",
            }
        },
    )
    assert knowledge_body.count(b"\xca") == 2

    modality_body = _canonical_method_body(
        "ServedModality",
        {
            "op": {
                "operation": "native_query",
                "predicate": {
                    "predicate": "audio_window",
                    "start_ms": 0,
                    "end_ms": 1,
                    "minimum_rms": 0.25,
                },
                "after_occurrence_id": None,
                "limit": 1,
                "include_cold": False,
            }
        },
    )
    assert modality_body.count(b"\xca") == 1

    nested_body = _canonical_method_body(
        "ApplyChangeEnvelope",
        {
            "envelope": {
                "mutation": {
                    "operations": [
                        {
                            "method": {
                                "method": "AddEmbedding",
                                "params": {
                                    "node_id": "v",
                                    "embedding": [1.0, 0.5],
                                },
                            }
                        }
                    ]
                }
            }
        },
    )
    assert nested_body.count(b"\xca") == 2


@pytest.mark.parametrize(
    ("method", "params"),
    [
        (
            "GraphQl",
            {
                "query": "query Fixture { fixture }",
                "variables": {
                    "plan": {"ops": [{"Rank": {"query": [1.0, 0.5]}}]},
                    "method": "AddEmbedding",
                    "params": {"node_id": "v", "embedding": [0.25]},
                },
            },
        ),
        (
            "GraphLearnPredict",
            {
                "model": {
                    "plan": {"ops": [{"Rank": {"query": [1.0, 0.5]}}]},
                    "method": "AddEmbedding",
                    "params": {"node_id": "v", "embedding": [0.25]},
                },
                "source": {
                    "node_label": "Fixture",
                    "direction": "any",
                    "limit": 0,
                },
                "candidate_pairs": [],
                "top_k": 1,
                "writeback": False,
            },
        ),
    ],
)
def test_signer_never_marks_f32_inside_arbitrary_json(
    method: str, params: dict[str, object]
) -> None:
    body = _canonical_method_body(method, params)
    assert body.count(b"\xca") == 0
    assert body.count(b"\xcb") == 3


class _CaptureClient:
    def __init__(self) -> None:
        self.method = ""
        self.params: dict[str, object] = {}

    async def _send(self, method: str, params: dict[str, object]) -> int:
        self.method = method
        self.params = params
        return 2


def test_timeseries_scalar_append_supplies_unambiguous_default_schema() -> None:
    capture = _CaptureClient()
    result = asyncio.run(
        TimeSeriesClient(capture).append(  # type: ignore[arg-type]
            "series-fixture", [(1, [1.25]), (2, [1.5])]
        )
    )
    assert result == 2
    assert capture.method == "TsAppend"
    assert capture.params["n_fields"] == 1
    assert capture.params["field_names"] == ["value"]
    assert msgpack.unpackb(capture.params["points_msgpack"], raw=False) == [
        [1, [1.25]],
        [2, [1.5]],
    ]


def test_timeseries_multifield_append_requires_exact_explicit_schema() -> None:
    capture = _CaptureClient()
    with pytest.raises(ValueError, match="explicit for a multi-field series"):
        asyncio.run(
            TimeSeriesClient(capture).append(  # type: ignore[arg-type]
                "series-fixture", [(1, [1.25, 2.5])]
            )
        )
    assert capture.method == ""

    result = asyncio.run(
        TimeSeriesClient(capture).append(  # type: ignore[arg-type]
            "series-fixture",
            [(1, [1.25, 2.5])],
            field_names=["price", "volume"],
        )
    )
    assert result == 2
    assert capture.params["field_names"] == ["price", "volume"]


def test_task_local_verified_context_restores_shared_client_authority() -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    override = {
        **_context(),
        "principal": "second-opaque-subject",
        "tenant": "second-tenant",
        "agent_id": "second-agent",
        "delegation": ["second-opaque-subject", "second-agent"],
    }
    assert client._verified_tenant() == "tenant-fixture"
    with client.use_verified_context(override):
        assert client._verified_tenant() == "second-tenant"
    assert client._verified_tenant() == "tenant-fixture"


@pytest.mark.parametrize(
    "context",
    [
        {key: value for key, value in _context().items() if key != "scopes"},
        {**_context(), "roles": ["ingestor", "ingestor"]},
        {**_context(), "scopes": [""]},
        {**_context(), "delegation": ["agent-fixture", "subject-opaque"]},
        {**_context(), "environment_specific_identity": "must-not-pass"},
    ],
)
def test_request_context_rejects_incomplete_or_ambiguous_claims(context) -> None:
    with pytest.raises((TypeError, ValueError)):
        validate_request_context(context)


def test_client_requires_secret_and_current_context() -> None:
    with pytest.raises(ValueError, match="authentication secret"):
        EpistemicGraphClient(  # type: ignore[arg-type]
            object(),
            object(),
            "",
            "graph-fixture",
            verified_context=_context(),
        )
    with pytest.raises(TypeError, match="verified_context"):
        EpistemicGraphClient(  # type: ignore[call-arg,arg-type]
            object(), object(), "fixture-secret", "graph-fixture"
        )


@pytest.mark.asyncio
async def test_bootstrap_operation_is_detached_signed_and_context_bound() -> None:
    context = {
        **_context(),
        "principal": "service:bootstrap",
        "agent_id": "service:bootstrap",
        "roles": [],
        "scopes": ["security:bootstrap"],
        "delegation": [],
    }
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "__commons__",
        verified_context=context,
    )
    captured: dict[str, object] = {}

    async def capture(method, params=None, graph=None, *, idempotency_key=None):
        captured.update(
            method=method,
            params=params,
            graph=graph,
            idempotency_key=idempotency_key,
        )
        return "ok"

    client._send = capture  # type: ignore[method-assign]
    result = await client.consensus.bootstrap_system_identity(
        agent_id="service:bootstrap",
        signer_id="service:bootstrap",
        signer_key="fixture-operation-key",
    )
    assert result == "ok"
    assert captured["method"] == "RegisterIdentity"
    assert captured["graph"] == "__commons__"
    params = captured["params"]
    assert isinstance(params, dict)
    assert params["role"] == "System"
    assert params["teams"] == []
    assert params["roles"] == []
    assert str(params["signature"]).startswith("service:bootstrap:")
    assert str(captured["idempotency_key"]).startswith("operation:sha256:")
    assert "fixture-operation-key" not in json.dumps(captured)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "host_path",
    [
        "/private/source.py",
        "C:\\private\\source.py",
        "C:private\\source.py",
        "file:///private/source.py",
    ],
)
async def test_parse_file_rejects_host_paths(host_path: str) -> None:
    client = EpistemicGraphClient(  # type: ignore[arg-type]
        object(),
        object(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    with pytest.raises(ValueError, match="host filesystem"):
        await client.graph.parse_file(host_path, b"pass\n")
