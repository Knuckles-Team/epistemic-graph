# EG-OBJECT-STORAGE-BACKEND — Object-Storage-Native Durable Store and Comparative Cost Gate

Owner: epistemic-graph (`EG`). Sole owner.

## User outcome

A graph's durable state can live in an S3-compatible object store. It can live there instead of
the local redb store. Or alongside it. Any node opens any tenant graph from the shared root.
Commits are commit-before-ack over immutable versioned segments. Every storage operation has a
measured cost across releases. A comparative gate carries the numbers. The local store remains
the default. Object storage is a declared, typed backend. It is not a second engine.

This answers the object-storage-native posture of the comparator (ModernRelay/omnigraph,
surveyed 2026-10-19 at `0a62bb7b`). That engine stores Lance datasets on S3-compatible object
storage as its primary substrate. The stated advantage is the control plane. EG's object-store
backend stays under the existing multi-raft consensus, placement, and resharding authorities. Its
comparative cost gate also counts cross-shard federation actions. A single-store harness cannot
express those.

## Requirements

Every requirement ID this spec owns lives in [requirements.md](requirements.md). The IDs are
`EG-OBJECT-STORAGE-BACKEND-R001`–`R005`. Architecture and live wiring live in
[plan.md](plan.md). The test contract lives in [test-spec.md](test-spec.md). Ordered work lives
in [tasks.md](tasks.md). Machine-readable delivery state lives in [status.json](status.json).

## Acceptance

The spec accepts when every row reaches `VERIFIED` in [status.json](status.json). Each row
carries its bound test evidence.

- A backend spec round-trips. It admits or refuses before any effect (R001).
- Versioned reads over object storage are consistent. Corruption refuses. It never
  reinterprets (R002).
- Commits are commit-before-ack with an atomic manifest publish. Partial publishes leave the
  prior version intact (R003).
- Multiple nodes open the same root. They serve identical state. They use no second consensus
  layer (R004).
- The comparative cost gate catches unexplained growth in the fixed open cost. It records
  claim-eligible, environment-stamped evidence (R005).

States and evidence follow the ecosystem spec standard. A planned or tested item is not a landed
item.
