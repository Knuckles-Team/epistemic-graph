"""U-144: `client.query.sql()` failed closed with the generic "Authentication
failed" under the EXACT SAME verified session that reaches `CypherQuery` and
plain node reads fine.

ROOT CAUSE: the server's HMAC verification recomputes the signed body hash
from the fully DESERIALIZED `Method` (`Method::canonical_body_bytes`,
`crates/eg-types/src/protocol.rs`) — `rmp_serde::to_vec_named(self)` always
serializes every declared struct field, including `Method::Sql`'s
`params_msgpack: Vec<u8>` (its `#[serde(default, with = "serde_bytes")]`
attribute only relaxes what a DEcode may omit; it does not skip the field on
ENcode). The old `QueryClient.sql()` sent (and therefore signed) `{"query":
query}` with NO `params_msgpack` key at all, so the client's signed body map
had ONE key while the server's reconstructed, re-hashed body map always had
TWO — a guaranteed digest/MAC mismatch on every single call. `CypherQuery`
has no such optional/omittable field on this client's convenience wrapper
(`query` and `mode` are always both sent), so it was never affected —
matching the live symptom "authentication failure independently of the
Cypher [...] issue" under an identical session.

This is an end-to-end test against the REAL server binary (not a
Python-side-only reasoning check) so it proves the actual HMAC verifier
accepts/rejects exactly as claimed.
"""

from __future__ import annotations

import pytest
from conftest import request_context


@pytest.mark.concept("CONCEPT:EG-KG.query.read-only-sql-query")
def test_sql_query_authenticates_under_the_same_session_as_cypher(clean_graph):
    clean_graph.nodes.add("A", {"label": "sql-fixture"})

    # The live U-144 symptom: this used to fail closed with "Authentication
    # failed" even though the identical session/connection reaches Cypher and
    # plain node reads fine (asserted right below).
    rows = clean_graph.query.sql("SELECT count(*) AS n FROM nodes")
    assert rows, "SQL query returned no rows"
    assert int(rows[0]["n"]) >= 1

    cypher_rows = clean_graph.query.cypher_read("MATCH (n) RETURN count(n) AS n")
    assert cypher_rows
    assert clean_graph.nodes.has("A") is True


@pytest.mark.concept("CONCEPT:EG-KG.query.read-only-sql-query")
def test_sql_query_omitting_params_msgpack_from_the_signed_body_fails_closed():
    """Direct reproduction of the exact wire-level defect: hand-construct the
    OLD (broken) request shape -- a `Sql` params map missing
    `params_msgpack` entirely -- and confirm the real server's HMAC verifier
    genuinely rejects it with "Authentication failed", proving this is a
    signed-body-hash mismatch and not a coincidental error string.
    """
    import asyncio
    import os

    from epistemic_graph.client import EpistemicGraphClient

    socket_path = os.environ.get("GRAPH_SERVICE_SOCKET")
    assert socket_path is not None

    async def _run() -> str | None:
        client = await EpistemicGraphClient.connect(
            socket_path=socket_path,
            verified_context=request_context(),
        )
        try:
            # Bypass QueryClient.sql() entirely -- send the pre-fix wire shape
            # directly through the low-level `_send`, which signs exactly the
            # params dict it is given.
            try:
                await client._send("Sql", {"query": "SELECT count(*) AS n FROM nodes"})
            except Exception as exc:  # noqa: BLE001 -- want the exact server error text
                return str(exc)
            return None
        finally:
            await client.close()

    error = asyncio.run(_run())
    assert error is not None, (
        "sending Sql without params_msgpack unexpectedly succeeded -- either "
        "the server no longer recomputes the body hash from the full "
        "deserialized Method, or this reproduction no longer matches "
        "QueryClient.sql()'s pre-fix shape"
    )
    assert "Authentication failed" in error
