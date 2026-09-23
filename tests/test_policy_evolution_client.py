"""PolicyEvolutionClient (EH-346/EH-347): generated senders, typed refusals."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from _client_fixtures import RecordingTransport, SentCall
from epistemic_graph.generated.policy_evolution import (
    OpenWeightPolicyCapability,
    PolicyCapture,
)

import epistemic_graph
from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.policy_evolution import (
    PolicyEvolutionClient,
    PolicyEvolutionRefused,
    refusal_of,
)

pytestmark = pytest.mark.no_engine


def _hex(byte: int) -> str:
    return f"{byte:02x}" * 32


def _capability() -> OpenWeightPolicyCapability:
    return OpenWeightPolicyCapability.model_validate(
        {
            "provider": "vllm",
            "endpoint_ref": "endpoint:gb10",
            "base_checkpoint_digest": _hex(1),
            "tokenizer_digest": _hex(2),
            "decode_params_digest": _hex(3),
            "artifact_destination_ref": "artifacts:new",
            "logprobs": {"chosen_token": True},
            "probe_digest": _hex(4),
            "probed_at_ms": 1,
        }
    )


def _blob(byte: int, length: int, encoding: str) -> dict[str, Any]:
    return {"digest": _hex(byte), "length": length, "encoding": encoding, "elements": 1}


def _receipt(kind: str, disposition: str = "written") -> dict[str, Any]:
    return {
        "record_id": f"polcap:{_hex(5)}",
        "kind": kind,
        "disposition": disposition,
        "eligibility": None,
        "observed_at_ms": 9,
    }


class _Transport(RecordingTransport):
    def __init__(self, answer: Any) -> None:
        super().__init__()
        self.answer = answer

    def reply(self, call: SentCall) -> Any:
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def _client(answer: Any) -> tuple[PolicyEvolutionClient, _Transport]:
    transport = _Transport(answer)
    engine = cast(EpistemicGraphClient, transport)
    client = PolicyEvolutionClient(engine, "tenant-graph")
    return client, transport


def test_put_capability_sends_the_typed_op_to_the_request_graph() -> None:
    client, transport = _client(_receipt("capability"))
    receipt = asyncio.run(client.put_capability(_capability()))
    assert receipt.disposition == "written"
    method, params, graph, _key = transport.sent[0]
    assert (method, graph) == ("PolicyEvolution", "tenant-graph")
    assert params is not None
    assert params["op"]["op"] == "put_capability"
    request = params["op"]["request"]
    assert request["logprobs"]["chosen_token"] is True
    # Controls default off: an omitted control never enables anything.
    assert not request.get("controls", {}).get("capture", {}).get("enabled", False)


def test_an_engine_refusal_becomes_a_typed_refusal() -> None:
    client, _ = _client(RuntimeError("POLICY_CAPTURE_DISABLED: "))
    capture = PolicyCapture.model_validate(
        {
            "capability_id": f"polcap:{_hex(5)}",
            "sampler_version_id": f"polver:{_hex(6)}",
            "trajectory_id": "trajectory:0001",
            "trajectory_steps": 1,
            "completion": "terminal",
            "token_count": 1,
            "policy_token_count": 1,
            "token_ids": _blob(7, 4, "u32_le"),
            "log_q": _blob(8, 4, "f32_le"),
            "action_mask": _blob(9, 1, "u8_mask"),
            "purpose": "training",
            "trace_fidelity": "full",
            "captured_at_ms": 2,
        }
    )
    with pytest.raises(PolicyEvolutionRefused) as refused:
        asyncio.run(client.commit_capture(capture))
    assert refused.value.code == "POLICY_CAPTURE_DISABLED"


def test_other_engine_errors_pass_through_unchanged() -> None:
    client, _ = _client(RuntimeError("ACCESS_DENIED: nope"))
    with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
        asyncio.run(client.get(f"polver:{_hex(6)}"))
    assert refusal_of(RuntimeError("ACCESS_DENIED: nope")) is None
    parsed = refusal_of(RuntimeError("POLICY_RECORD_MISSING: polver:x"))
    assert parsed is not None
    assert (parsed.code, parsed.detail) == ("POLICY_RECORD_MISSING", "polver:x")


def test_get_answers_none_for_an_absent_record() -> None:
    client, transport = _client(None)
    assert asyncio.run(client.get(f"polver:{_hex(6)}")) is None
    assert transport.sent[0].params == {
        "op": {"op": "get", "request": {"record_id": f"polver:{_hex(6)}"}}
    }


def test_the_client_is_exported_and_mounted() -> None:
    assert epistemic_graph.PolicyEvolutionClient is PolicyEvolutionClient
    assert epistemic_graph.PolicyEvolutionRefused is PolicyEvolutionRefused
