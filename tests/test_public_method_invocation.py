"""The public method path uses generated request and packaged result contracts."""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.contract.invocation import (
    ContractViolation,
    validate_method_params,
    validate_method_result,
)
from epistemic_graph.generated import OpaqueResult


def _row(method: str) -> dict[str, object]:
    result_domain = "graph" if method == "AddNode" else "cluster"
    return {
        "id": method,
        "is_wire_callable": True,
        "request_schema": {
            "schema": f"contract/schemas/method.request.json#/methods/{method}"
        },
        "result_schema": {
            "schema": f"contract/schemas/result.{result_domain}.json#/methods/{method}"
        },
    }


class _Transport:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, ...]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, object] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> object:
        self.calls.append((method, params, graph, idempotency_key))
        return self.result


class PublicMethodInvocation(unittest.TestCase):
    def test_validated_result_and_request_context_reach_transport(self) -> None:
        transport = _Transport("pong")
        with patch(
            "epistemic_graph.contract.invocation._methods",
            return_value={"Ping": _row("Ping")},
        ):
            result = asyncio.run(
                EpistemicGraphClient.invoke_method(
                    transport, "Ping", graph="tenant-graph", idempotency_key="key-1"
                )
            )
        self.assertEqual(result, "pong")
        self.assertEqual(transport.calls, [("Ping", {}, "tenant-graph", "key-1")])

    def test_invalid_params_fail_before_transport(self) -> None:
        transport = _Transport("node-1")
        with patch(
            "epistemic_graph.contract.invocation._methods",
            return_value={"AddNode": _row("AddNode")},
        ):
            with self.assertRaises(ValueError):
                asyncio.run(
                    EpistemicGraphClient.invoke_method(transport, "AddNode", {})
                )
        self.assertEqual(transport.calls, [])

    def test_validate_params_rejects_extras_without_a_transport(self) -> None:
        with patch(
            "epistemic_graph.contract.invocation._methods",
            return_value={"Ping": _row("Ping")},
        ):
            self.assertEqual(validate_method_params("Ping", {}), {})
            with self.assertRaises(ValueError):
                validate_method_params("Ping", {"principal": "forged"})

    def test_opaque_result_must_match_method_and_schema(self) -> None:
        with patch(
            "epistemic_graph.contract.invocation._methods",
            return_value={"Ping": _row("Ping")},
        ):
            self.assertEqual(
                validate_method_result("Ping", {}, OpaqueResult("Ping", "pong")),
                "pong",
            )
            with self.assertRaises(ContractViolation):
                validate_method_result("Ping", {}, OpaqueResult("Health", "pong"))

    def test_unknown_method_and_bad_result_fail_closed(self) -> None:
        transport = _Transport(9)
        with patch(
            "epistemic_graph.contract.invocation._methods",
            return_value={"Ping": _row("Ping")},
        ):
            with self.assertRaises(ValueError):
                asyncio.run(EpistemicGraphClient.invoke_method(transport, "NoSuchMethod"))
            with self.assertRaises(ContractViolation):
                asyncio.run(EpistemicGraphClient.invoke_method(transport, "Ping"))
        self.assertEqual(len(transport.calls), 1)


if __name__ == "__main__":
    unittest.main()
