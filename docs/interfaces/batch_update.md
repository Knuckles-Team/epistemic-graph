# Atomic batch updates

`BatchUpdate` applies an ordered list of graph and vector mutations to one graph.
The complete payload is decoded and validated before RAM changes. The authoritative
redb path commits node rows, edge rows, semantic vectors, batch status, and outbox
metadata in one transaction; malformed input or a missing required endpoint aborts
the whole batch.

Use the Python client through `client.lifecycle.batch_update(operations)`. Operation
objects use one canonical set of field names:

| `op` | Required fields | Effect |
|---|---|---|
| `add_node` | `id`; optional object `properties` | Add or replace the node property document |
| `upsert_node` | `id`; optional object `properties` | Create a missing node, or merge the supplied top-level fields into an existing node without removing unrelated fields |
| `remove_node` | `id` | Remove the node, every incoming/outgoing edge, and its vector |
| `add_edge` | `source`, `target`; optional object `properties` | Add one directed edge; both endpoints must exist at that point in the list |
| `upsert_edge` | `source`, `target`; optional object `properties` | Replace all parallel edges for the ordered pair with one edge |
| `remove_edge` | `source`, `target` | Remove all directed edges for the ordered pair |
| `add_embedding` | `id`, non-empty finite-number `embedding` | Add or replace the existing node's vector |

For example:

```python
result = await client.lifecycle.batch_update(
    [
        {"op": "upsert_node", "id": "document:1", "properties": {"text": "example"}},
        {"op": "upsert_node", "id": "collection:1", "properties": {"type": "Collection"}},
        {"op": "add_embedding", "id": "document:1", "embedding": [0.25, 0.75]},
        {
            "op": "upsert_edge",
            "source": "document:1",
            "target": "collection:1",
            "properties": {"relationship": "MEMBER_OF"},
        },
    ]
)
```

The response retains the original aggregate counters and adds explicit upsert/vector
counters:

```json
{
  "added_nodes": 2,
  "upserted_nodes": 2,
  "removed_nodes": 0,
  "added_edges": 1,
  "upserted_edges": 1,
  "removed_edges": 0,
  "added_embeddings": 1,
  "errors": []
}
```

`errors` is the current typed batch-error field. Validated batches either succeed as
a unit with an empty list or return a request error. There is no
partial-success mode inside one graph. `MultiGraphBatchUpdate` composes these same
per-graph atomic batches and may report success for some graphs and errors for others.

`upsert_node` is a shallow property merge: each supplied top-level field replaces
that field's prior value, while omitted top-level fields remain unchanged. Nested
objects are values and are therefore replaced as a whole rather than recursively
merged. An existing non-object property document fails the whole batch rather than
being overwritten. `add_node` retains its replace-document behavior.

Do not use the older internal names `node_id`, `source_id`, `target_id`, or
`properties_json` in this interface. They are not accepted aliases, which prevents a
payload from appearing successful while its rows were skipped.
