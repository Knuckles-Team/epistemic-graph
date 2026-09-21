//! Render the shared generated Python runtime module.

use super::HEADER;

/// The runtime module's docstring, `OpaqueResult`, `ContractViolation` and `_violation`,
/// verbatim: Python text is one literal here, not a `push_str` per line.
const RUNTIME_DECLARATIONS: &str = r#""""Shared runtime for the generated engine-contract client.

OpaqueResult is what a method returns when this client does not model its
declared body (a DTO, a caller-shaped body, or a result the contract has not
classified): the decoded payload paired with the method id that produced it,
so a caller can never mistake it for a value the client validated.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from ._ids import METHOD_IDS


class OpaqueResult(NamedTuple):
    method: str
    payload: Any


class ContractViolation(RuntimeError):
    """The engine returned a shape the contract does not declare for the method.

    The engine encodes every declared result through a compile-checked marker, so a
    violation means the client and the engine were built from different contracts.
    The generated send checks the decoded payload rather than returning it under a
    signature that would lie, and names the method, the declared encoding and the
    shape actually observed.
    """


def _violation(method: str, claimed: str, payload: Any) -> ContractViolation:
    return ContractViolation(
        f"{method}: contract claims ResultPayload::{claimed}, engine returned "
        f"{type(payload).__name__}"
    )


"#;

fn push_runtime_declarations_and_violation(out: &mut String) {
    out.push_str(RUNTIME_DECLARATIONS);
}

fn push_runtime_result_checkers(out: &mut String) {
    out.push_str("def expect_bool(method: str, payload: Any) -> bool:\n");
    out.push_str("    if not isinstance(payload, bool):\n");
    out.push_str("        raise _violation(method, \"Bool\", payload)\n    return payload\n\n\n");
    out.push_str("def expect_count(method: str, payload: Any) -> int:\n");
    out.push_str("    if isinstance(payload, bool) or not isinstance(payload, int):\n");
    out.push_str("        raise _violation(method, \"Count\", payload)\n    return payload\n\n\n");
    out.push_str("def expect_float(method: str, payload: Any) -> float:\n");
    out.push_str("    # An f64 that happens to be integral arrives from MessagePack as an int.\n");
    out.push_str("    if isinstance(payload, bool) or not isinstance(payload, (int, float)):\n");
    out.push_str(
        "        raise _violation(method, \"Float\", payload)\n    return float(payload)\n\n\n",
    );
    out.push_str("def expect_string(method: str, payload: Any) -> str:\n");
    out.push_str("    if not isinstance(payload, str):\n");
    out.push_str("        raise _violation(method, \"String\", payload)\n    return payload\n\n\n");
    out.push_str("def expect_ids(method: str, payload: Any) -> list[str]:\n");
    out.push_str("    if not isinstance(payload, list) or not all(\n");
    out.push_str("        isinstance(item, str) for item in payload\n    ):\n");
    out.push_str("        raise _violation(method, \"Ids\", payload)\n    return payload\n\n\n");
    out.push_str("def _rows(method: str, claimed: str, payload: Any, width: int) -> list[Any]:\n");
    out.push_str("    if not isinstance(payload, list) or not all(\n");
    out.push_str(
        "        isinstance(row, (list, tuple)) and len(row) == width for row in payload\n    ):\n",
    );
    out.push_str("        raise _violation(method, claimed, payload)\n    return payload\n\n\n");
    out.push_str("def expect_nodelist(method: str, payload: Any) -> list[Any]:\n");
    out.push_str("    return _rows(method, \"NodeList\", payload, 2)\n\n\n");
    out.push_str("def expect_edgelist(method: str, payload: Any) -> list[Any]:\n");
    out.push_str("    return _rows(method, \"EdgeList\", payload, 3)\n\n\n");
}

fn push_runtime_send_by_id(out: &mut String) {
    out.push_str("async def send_by_id(\n");
    out.push_str("    client: Any,\n    method: str,\n");
    out.push_str("    params: dict[str, Any] | None = None,\n");
    out.push_str("    graph: str | None = None,\n    *,\n");
    out.push_str("    idempotency_key: str | None = None,\n) -> Any:\n");
    out.push_str("    \"\"\"Send a method chosen at run time.\n\n");
    out.push_str(
        "    The hand-written helpers that pick a method from a parameter reach the wire\n",
    );
    out.push_str(
        "    through this one entry point, so no method id is spelled outside generated code.\n",
    );
    out.push_str("    \"\"\"\n");
    out.push_str("    if method not in METHOD_IDS:\n");
    out.push_str("        raise ValueError(f\"{method} is not an engine-contract method\")\n");
    out.push_str("    return await client._send(\n");
    out.push_str("        method,\n        params,\n        graph,\n        idempotency_key=idempotency_key,\n    )\n");
}

pub(super) fn runtime_module() -> String {
    let mut out = String::from(HEADER);
    push_runtime_declarations_and_violation(&mut out);
    push_runtime_result_checkers(&mut out);
    push_runtime_send_by_id(&mut out);
    out
}
