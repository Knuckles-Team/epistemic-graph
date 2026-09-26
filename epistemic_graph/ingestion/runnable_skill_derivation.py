"""Pure graph projection for a privacy-screened runnable skill."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

_KNOWN_SKILL_TYPES = frozenset({"skill", "workflow", "graph", "mcp_skill"})


def skill_slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_") or "step"


def skill_reference(name: str) -> str:
    """Stable reference independent of runtime discovery paths."""
    return f"skill://{skill_slug(name).replace('_', '-')}"


def runnable_skill_digest(instructions: str) -> str:
    """Digest of the exact normalized instructions admitted for execution."""
    return hashlib.sha256(instructions.strip().encode("utf-8")).hexdigest()


def classify_skill_type(skill_type: str | None) -> tuple[str, str]:
    """Normalize a declaration to EG's closed SkillType vocabulary."""
    normalized = str(skill_type or "").strip().lower()
    if normalized not in _KNOWN_SKILL_TYPES:
        normalized = "skill"
    return normalized, normalized


@dataclass(frozen=True)
class RunnableSkillProjection:
    """Rows and relations to submit using an already verified write session."""

    resource_id: str
    nodes: tuple[tuple[str, str, dict[str, Any]], ...]
    edges: tuple[tuple[str, str, str], ...]


def runnable_skill_projection(
    *,
    name: str,
    description: str,
    body: str,
    provider: str,
    disabled: bool,
    mcp_server: str,
    skill_type: str | None,
    privacy_redactions: int,
    governance: dict[str, Any],
) -> RunnableSkillProjection:
    """Derive canonical Skill, CallableResource and Provenance graph material.

    Inputs are already sanitized by the AU admission boundary. This function
    performs no IO, session resolution, ACL decision, or write.
    """
    source_ref = skill_reference(name)
    slug = source_ref.removeprefix("skill://")
    digest = runnable_skill_digest(body)
    provider_ref = f"provider://{skill_slug(provider)}"
    skill_id = f"skill:{slug}"
    resource_id = f"resource:{skill_id}"
    provenance_id = f"provenance:skill:{digest}"
    normalized_skill_type, _ = classify_skill_type(skill_type)
    common = {
        **governance,
        "name": name,
        "description": description,
        "source_ref": source_ref,
        "provider_ref": provider_ref,
        "instruction_digest": digest,
        "disabled": bool(disabled),
        "skill_type": normalized_skill_type,
        "privacy_redactions": privacy_redactions,
    }
    if mcp_server:
        common["mcp_server"] = mcp_server
    return RunnableSkillProjection(
        resource_id=resource_id,
        nodes=(
            ("Skill", skill_id, {**common, "body": body, "instruction": body}),
            (
                "CallableResource",
                resource_id,
                {
                    **common,
                    "resource_type": "AGENT_SKILL",
                    "system_prompt": body,
                    "runnable_bound": True,
                },
            ),
            (
                "Provenance",
                provenance_id,
                {
                    **governance,
                    "kind": "installed-skill-provider",
                    "source_ref": source_ref,
                    "provider_ref": provider_ref,
                    "content_digest": digest,
                },
            ),
        ),
        edges=(
            (skill_id, resource_id, "BINDS_RUNNABLE"),
            (skill_id, provenance_id, "DERIVED_FROM"),
            (resource_id, provenance_id, "DERIVED_FROM"),
        ),
    )
