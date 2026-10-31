"""R050.2: generated consumers must preserve, not discard, declared media facts."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from medulla.generated.agent_component import AgentComponentFactsModelProfile
from medulla.generated.connector_pack import PackModelFacts

pytestmark = [pytest.mark.no_engine, pytest.mark.spec("MEDULLA-DECISION-ENGINE-R050.2")]


def _profile(component: bool) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "provider": "fixture",
        "model_identity": "fixture/model",
        "context_window_tokens": 8192,
        "max_output_tokens": 1024,
        "supports_tools": False,
        "supports_structured_output": False,
        "supports_vision": True,
    }
    if component:
        profile["facts"] = "model_profile"
    return profile


def _capabilities() -> dict[str, Any]:
    return {
        "media_types": ["text/plain", "image/png", "image/jpeg", "image/webp"],
        "max_aggregate_bytes": 4096,
        "max_parts": 4,
    }


@pytest.mark.parametrize("component", [False, True], ids=["pack", "component"])
def test_generated_models_round_trip_declared_input_capabilities(
    component: bool,
) -> None:
    model = AgentComponentFactsModelProfile if component else PackModelFacts
    wire = {**_profile(component), "input_capabilities": _capabilities()}
    facts = model.model_validate(wire)
    assert facts.model_dump(mode="json")["input_capabilities"] == _capabilities()
    assert model.model_validate(facts.model_dump(mode="json")) == facts


@pytest.mark.parametrize("component", [False, True], ids=["pack", "component"])
def test_generated_models_keep_missing_and_null_capabilities_unknown(
    component: bool,
) -> None:
    model = AgentComponentFactsModelProfile if component else PackModelFacts
    missing = model.model_validate(_profile(component))
    explicit_null = model.model_validate(
        {**_profile(component), "input_capabilities": None}
    )
    assert missing == explicit_null
    assert missing.model_dump(mode="json")["input_capabilities"] is None
    assert "input_capabilities" not in missing.model_dump(
        mode="json", exclude_none=True
    )


@pytest.mark.parametrize("component", [False, True], ids=["pack", "component"])
@pytest.mark.parametrize("invalid", ["unknown_media", "extra_field"])
def test_generated_capability_refusals_follow_successful_decoding(
    component: bool, invalid: str
) -> None:
    model = AgentComponentFactsModelProfile if component else PackModelFacts
    caps = _capabilities()
    # A missing generated field must fail this positive control, not accidentally
    # pass the negative case by refusing every input_capabilities object.
    model.model_validate({**_profile(component), "input_capabilities": caps})
    if invalid == "unknown_media":
        caps["media_types"] = ["image/gif"]
    else:
        caps["unrestricted"] = True
    with pytest.raises(ValidationError):
        model.model_validate({**_profile(component), "input_capabilities": caps})
