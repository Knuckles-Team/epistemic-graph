"""Pure identity and graph-property derivation for parsed skill workflows."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def workflow_content_hash(parsed: dict[str, Any]) -> str:
    """Stable hash over parsed semantics for idempotent re-ingest."""
    payload = {
        "name": parsed["name"],
        "description": parsed["description"],
        "domain": parsed["domain"],
        "tags": parsed["tags"],
        "steps": [
            {
                "step": step["step"],
                "component": step["component"],
                "skill_name": step["skill_name"],
                "depends_on": step["depends_on"],
                "tools": step["tools"],
                "kind": step.get("kind", "task"),
                "condition": step.get("condition", "on_success"),
                "on_reject": step.get("on_reject"),
            }
            for step in parsed["steps"]
        ],
    }
    raw = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def workflow_properties(
    parsed: dict[str, Any],
    governance: dict[str, Any],
    *,
    content_hash: str,
    timestamp: str,
) -> dict[str, Any]:
    """Build the persisted WorkflowDefinition properties."""
    steps = parsed["steps"]
    nl_lines = [
        f"Step {step['step']}: {step['component']}"
        + (
            f" [depends_on: {', '.join(step['depends_on'])}]"
            if step["depends_on"]
            else ""
        )
        for step in steps
    ]
    props: dict[str, Any] = {
        **governance,
        "name": parsed["name"],
        "description": parsed["description"],
        "domain": parsed["domain"],
        "source": "universal-skills",
        "tags_json": json.dumps(parsed["tags"], default=str),
        "specialist_ids_json": json.dumps(parsed["specialist_ids"], default=str),
        "nl_spec": parsed["description"] + "\n\nSteps:\n" + "\n".join(nl_lines),
        "step_count": len(steps),
        "content_hash": content_hash,
        "source_ref": parsed["source_ref"],
        "last_used": timestamp,
        "use_count": 0,
        "version": 1,
        "skill_type": "workflow",
    }
    if parsed.get("concept"):
        props["concept"] = str(parsed["concept"])
    return props


def workflow_step_properties(
    step: dict[str, Any],
    step_id: str,
    governance: dict[str, Any],
    resolved_deps: list[str],
    on_reject_id: str | None,
) -> dict[str, Any]:
    """Build the persisted WorkflowStep properties without performing writes."""
    props: dict[str, Any] = {
        **governance,
        "step_id": step_id,
        "step_order": step["step"],
        "component": step["component"],
        "skill_name": step["skill_name"],
        "is_parallel": not resolved_deps,
        "timeout": 120.0,
        "status": "pending",
        "depends_on_json": json.dumps(resolved_deps),
        "kind": step.get("kind") or "task",
        "condition": step.get("condition") or "on_success",
    }
    if step.get("tools"):
        props["tools_json"] = json.dumps(step["tools"], default=str)
    if step.get("description"):
        props["refined_subtask"] = step["description"]
    if on_reject_id:
        props["on_reject"] = on_reject_id
    return props
