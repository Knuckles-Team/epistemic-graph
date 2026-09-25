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
def test_sql_query_omitting_params_msgpack_signs_the_server_canonical_body(
    clean_graph,
):
    """The U-144 wire shape -- a `Sql` params map with no `params_msgpack` --
    now authenticates. The client signs `Method::canonical_body_bytes()` of the
    request the engine will DECODE (5e64e9715: serde defaults materialized by
    the same eg-types codec), so an omitted defaulted field can no longer split
    the signed body from the one the server re-derives. Before that fix this
    exact send failed closed with "Authentication failed"; it must now return
    the same rows as `QueryClient.sql()`.
    """
    import asyncio
    import os

    from epistemic_graph.client import EpistemicGraphClient

    clean_graph.nodes.add("A", {"label": "sql-fixture"})
    query = "SELECT count(*) AS n FROM nodes"
    expected = clean_graph.query.sql(query)
    socket_path = os.environ.get("GRAPH_SERVICE_SOCKET")
    assert socket_path is not None

    async def _run():
        client = await EpistemicGraphClient.connect(
            socket_path=socket_path,
            verified_context=request_context(),
        )
        try:
            # Bypass QueryClient.sql(): send the pre-fix shape through `_send`.
            raw = await client._send("Sql", {"query": query})
            return client.query._rows_to_dicts(raw)
        finally:
            await client.close()

    assert asyncio.run(_run()) == expected
