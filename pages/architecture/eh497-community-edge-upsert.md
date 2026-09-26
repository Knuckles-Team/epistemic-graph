# EH-497 relationship-scoped BatchUpdate edge upsert

The existing `upsert_edge` operation replaces every parallel edge for an
ordered `(source, target)` pair. AU's legacy `PART_OF_COMMUNITY` merge targets
only that relationship, so replacing all pairs could erase an unrelated fact.

`upsert_edge_relationship` is an operation inside the existing governed
`BatchUpdate` method. It requires nonempty canonical
`properties.relationship`, validates both endpoints before mutation, removes
only parallel rows whose decoded relationship equals that value, then appends
one replacement. RAM topology and property vectors update under one write
guard. The durable redb adapter removes only matching ordinals in its enclosing
write transaction; replay decodes the same operation. All other relationship
types and their property blobs remain present. Repeating the operation keeps
one matching relationship, and the normal `upsert_edge` pair-wide behavior is
unchanged.

AU's governed typed batch seam chooses the scoped operation explicitly. The
community caller must still prove legacy IDs, edge direction, properties,
actor stamps, and stored readback in a served test before default cutover.
