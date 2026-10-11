# EG-MEDIA-VOCAB — Test specification

Status: PROPOSED. Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Unit | load media-v1.ttl, content-v1.ttl with shapes | classes resolve, `:Transcript` not redefined, SHACL pass/fail fixtures behave | PENDING |
| T-002 | FR-002 | Contract | per-connector mapping fixture | every row names an existing class/property and `vg:sourceKind` mcp | PENDING |
| T-003 | FR-003 | Integration | recorded arr, qbittorrent, jellyfin responses | films downloaded but not in library, provenance per leg | PENDING |
| T-004 | FR-004 | Negative | connector fixture lacking the operation | typed abstention naming the operation and owner | PENDING |

## Negative and boundary cases

Missing leg (jellyfin unconnected): typed abstention. Raw email in Subscriber: SHACL violation. Hash case mismatch: normalised to lower case. Distinct queries returning the same URL keep distinct SearchResult identities. Two posts linking the same article remain separate. Repeated same-source records remain idempotent. Same numeric provider IDs for Film and TelevisionSeries do not merge. Ontology collision fixtures keep Team unchanged, socialScheduledAt distinct from scheduledFor, and downloadRequestState distinct from TRM requestState. Invalid timestamp/state values fail the domain-specific SHACL shapes. Tenant denial follows the engine contract.

## Quality and release proof

`cargo test -p eg-core`, repo CCCC/jscpd/dupehound/KISS gates; record revision and output link per row.

## Permanent-ID lineage validation

Documentation-only validation for `EG-MEDIA-VOCAB-X-R001` through
`EG-MEDIA-VOCAB-X-R013` and their active replacements `EG-MEDIA-VOCAB-R021`
through `EG-MEDIA-VOCAB-R033`:

- Compare the original rows at `b52f912f` with the active rows after applying
  only the one-to-one ID substitution; acceptance and verification must match.
- Require exactly thirteen RETIRED historical rows, each naming its unique
  active replacement, and preserve every pre-existing active row verbatim.
- Regenerate `status.json` with `pipelines-hook spec-status --write`; require
  all thirteen historical rows to be RETIRED, with the named replacement in
  the reason, and all pre-existing status/evidence records unchanged.
- Run `pipelines-hook spec-standard`, the scoped status freshness check, and
  changed-file pre-commit gates. No connector implementation test or delivery
  credit follows from these documentation checks.
