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

## Who issues the catalog binding

The binding is EG authority. A caller never synthesizes it; it obtains it from
one of two `ConnectorPack` writes, both of which require the verified tenant,
the `connector:catalog-attest` scope **and** the `admin:connector-pack` action,
re-read the live, enabled `__commons__` registration (registry revision and
digest, and the registration configuration digest over its URL and resource
map) under the registry graph lock, and compare-and-set the catalog generation
(`expected_catalog_generation`: `None` only for the first binding, then the
generation last read). The attester is always the verified principal.

| Producer | Operation | What EG pins |
|---|---|---|
| A mounted child: EG reaches the catalog through a child connection | `reconcile_catalog` | The published `McpServer` component (revision and content digest) and the child's identity, catalog epoch and connection generation. |
| A self-served producer: the MCP server serves its own content in process | `attest_self_served_catalog` | The content digest of the exact `McpServer` entry its next pack carries (`server_entry_digest` = `index.server.body.sha256`). |

A self-served producer's first pack import is what publishes its server
component, so it cannot be reconciled like a mounted child. Instead EG pins the
server entry the import will publish: revision 0 until that import lands, then
its published revision. The import must carry exactly EG's current binding for
the server and exactly the pinned server entry, or it is rejected
(`MALFORMED_INDEX`). The attestation and the import serialize on the same
tenant pack lock, so a re-pin cannot interleave with an import. The attester
must also be the connector's bound importer.

Re-running an attestation with the same catalog returns the same binding, even
after the import published the pinned entry and after registration heartbeats
(a self-served snapshot does not fold in the registry digest, whose lease
timestamps move on every heartbeat), so an unchanged pack imports as
`Unchanged`. A changed catalog digest advances the catalog generation; a
changed server entry or registration configuration also advances the
configuration revision. A stale expected generation is refused. A row keeps the
kind it was born with: a mounted child cannot take over a self-served row, nor
the reverse. `catalog_authority_status` and `catalog_binding_status` read the
current binding for either kind.

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
