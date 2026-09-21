# Durable MCP resource and template ingestion

`ConnectorPack.import` is the durable ingestion boundary for a served MCP
catalog snapshot. A pack carries the complete catalog identity that GraphOS
observed:

- configuration revision;
- monotonic catalog generation;
- canonical snapshot digest;
- child-connection generation; and
- authorization-scope digest.

That identity is part of the pack digest and is copied into the immutable
import record, terminal receipt, and current head. `ConnectorPack.status`
returns the same binding through the generated Python client, so refresh
reconciliation compares exact durable evidence rather than a process-local
counter.

## Resources and resource templates

`PackEntryKind::Resource` accepts any absolute MCP resource URI. It requires a
content/result schema section. `PackEntryKind::ResourceTemplate` accepts any
absolute URI template and requires both argument and result schema sections.
Neither kind is restricted to engine-invented `skill://` or `prompt://`
schemes.

Both kinds materialize as `McpResource` Agent Components with MCP-server
provenance. Their component attributes retain the resource-vs-template kind,
upstream URI, catalog generation, and snapshot digest. Body and schema bytes
remain sections of the content-addressed ConnectorPack archive; the existing
Blob CAS body-holder lifecycle owns those bytes. The import transaction commits
the pack head, membership, component revisions, body holders, import record,
receipt, and outbox atomically.

## Ownership boundary

EG persists and answers the catalog snapshot. It does not list an MCP server,
schedule refreshes, call resources, or own a second catalog. GraphOS owns the
served event-loop reconciler. Source records read from a resource still enter
through `SourceIngest`, whose mapped graph/provenance/cursor effect commits
through the existing `ChangeEnvelope` authority. A pack import therefore
records *what was served*; `SourceIngest` records *what that source returned*.

An identical `ConnectorPack.import` operation replays by its deterministic
pack identity. A changed generation or digest changes the bound pack identity;
a stale expected head fails the existing compare-and-set rather than silently
claiming convergence.
