# EH-497 inline graph coloring contract

`GraphColoring` reads a persisted graph. AU's `chromatic_schedule` instead accepts
an in-memory undirected conflict graph. `GraphColorEphemeral` is a separate
read-only compute method so scheduling never creates or changes graph rows.

The request carries ordered `node_ids` and `(source, target)` edges. Admission
requires at most 4096 unique nonempty IDs and 65536 edges; every edge endpoint
must exist and self-conflicts are rejected. The response is an ordered list of
`(node_id, color)` pairs. Greedy coloring follows input node order and assigns
the smallest available nonnegative color. An isolate receives color zero. A
caller can validate both the complete node mapping and edge constraints.

`compute:graph-algo` or aggregate `kg:read` authorizes this snapshot operation.
The handler uses caller-supplied data only; it does not read tenant graph rows.
The existing persisted `GraphColoring` operation and its wire shape remain
unchanged. The Python generated send/decode contract must be regenerated from
`eg-capabilities` before the AU adapter is merged. Rust/kernel and served
authorization gates remain required.
