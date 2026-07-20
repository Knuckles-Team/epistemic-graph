# Exact-binary performance certification

The G-37 gate certifies one immutable `epistemic-graph-server` artifact against a
fixed synthetic workload and predeclared regression thresholds. It is distinct
from the exploratory benchmark scripts: a missing metric, unsupported required
surface, digest mismatch, failed workload, or exceeded threshold makes the gate
fail.

The committed contracts are:

- `protocols/performance/v1/dataset.json`: seed, scale points, workload sizes,
  sample counts, and the expected generated-workload digest.
- `protocols/performance/v1/thresholds.json`: the exact deployment profile and
  every latency, throughput, memory, and empirical complexity threshold.
- `protocols/performance/v1/scenarios.schema.json`: the strict Draft 2020-12
  schema for bounded hot-path scenario families, rows, resource limits,
  equivalence checks, and thresholds.
- `protocols/performance/v1/scenarios.json`: 30 serialized scenario families
  covering every one of the 54 implemented ledger rows exactly once.
- `docs/architecture/hot-path-complexity.md`: the stable `G37-HP-001` through
  `G37-HP-054` row identities and their qualified source-level bounds.
- `scripts/certify_exact_performance.py`: the serialized release harness and
  path-safe JSON/Markdown evidence renderer.

## What it proves

One run stages the digest-verified executable into a private Linux-native work
directory and starts exactly one engine with:

- a private Unix socket;
- the authoritative redb store and commit-before-ack behavior;
- one redb shard, so runs do not hide parallel compilation or multi-process work;
- fixed global, per-graph, reserved-read, and client timeout limits;
- an `eg2.` verified request context and an opaque, one-run bootstrap identity;
- no optional listener or inherited engine setting.

The harness waits for the staged process's owner-only socket and connects only
after that socket is verified; ambient endpoint discovery cannot satisfy readiness.

It then measures:

| Area | Exact workload |
|---|---|
| Cold start | Process spawn through opaque identity bootstrap and signed `Health` readiness |
| Routing | Signed engine-authoritative `PlacementRoute` samples at every graph scale point |
| Ingest | Atomic `BatchUpdate` node and edge batches into the durable store |
| Query | Fixed-width property batches plus native PageRank |
| Jobs | Durable association-job submit through terminal success |
| Modalities | The 12-pass component probe plus governed ingest and native indexed lookup at every scale point for document, image, audio, and video |
| Memory | Ready, peak, and growth RSS sampled from the exact engine process |
| Complexity | State-growth ratios for routing, fixed-width point reads, fixed-size ingestion batches, and indexed modality lookup |
| Implemented hot paths | 30 bounded exact-binary scenario subcommands producing raw work, owned-memory, measured-latency, resource, and semantic-equivalence evidence for all 54 ledger rows |

The modality coverage block records ingest counts, native-query sample counts, and an
index-growth ratio separately for all four modalities. G-14 consumes this JSON by
digest and rejects a report for another engine or one missing any modality.

The complexity ratios are empirical regression guards, not mathematical proofs.
The source-level bounds and their qualifications remain authoritative in the
[hot-path complexity ledger](architecture/hot-path-complexity.md).

## Exact hot-path scenario contract

The certifier loads the scenario schema, manifest, and complexity ledger before
it stages the executable. It rejects a missing, duplicate, reordered scenario,
unknown row, duplicate row, missing implementation reference, malformed scale,
unbounded resource declaration, or incomplete threshold/equivalence inventory.
The ledger registry and manifest must form an exact one-to-one 54-row set.

Each scenario runs serially through a hidden stdin-driven mode in the same staged
server executable whose digest is being certified. The mode is entered before
listeners, ambient configuration, or server authority initialization. It accepts
only the committed scenario/driver/row/check inventory, three increasing bounded
scales, bounded repetitions, and a digest-shaped workload identity. Its private
scratch directory is removed after every invocation.

Every row emits positive operation work, owned-memory accounting, independently
measured latency samples, and the declared semantic-equivalence outcomes. The
certifier rejects constant placeholder timing, malformed or incomplete raw output,
failed equivalence, threshold growth, elapsed-time/RSS/output bounds, and any
coverage gap. Evidence for each row is bound to digests of the exact executable,
dataset manifest and generated workload, threshold manifest, scenario manifest and
schema, complexity ledger, authority context, and abstract hardware class. Paths,
endpoints, raw identities, secrets, and source bodies are never evidence fields.

## Authority configuration

The authority file is deployment-owned, owner-only (`0600` or stricter), and is
never copied into an artifact or report. Identifiers must use opaque 64-hex references; the certifier
context has one explicit `kg:admin` scope because it creates an isolated graph and
exercises administrative, job, and modality surfaces in the same process.

```json
{
  "schema_version": "1",
  "auth_secret": "<runtime-secret-at-least-32-characters>",
  "signer_id": "eg:certifier:<64-lowercase-hex-token>",
  "signer_key": "<runtime-signer-key-at-least-32-characters>",
  "context": {
    "principal": "eg:certifier:<same-64-lowercase-hex-token>",
    "tenant": "eg:tenant:<64-lowercase-hex-token>",
    "audience": "eg:audience:<64-lowercase-hex-token>",
    "agent_id": "eg:certifier:<same-64-lowercase-hex-token>",
    "roles": ["certifier"],
    "scopes": ["kg:admin"],
    "policy_version": "eg:policy:<64-lowercase-hex-token>",
    "delegation": []
  }
}
```

Supply the executable, its independently recorded digest, external configuration,
Linux-native work root, and two new evidence targets explicitly:

```bash
python3 scripts/certify_exact_performance.py \
  --engine-binary "$ENGINE_BINARY" \
  --engine-sha256 "$ENGINE_SHA256" \
  --authority-config "$G37_AUTHORITY_CONFIG" \
  --work-root "$G37_LINUX_WORK_ROOT" \
  --json-output "$G37_JSON_EVIDENCE" \
  --markdown-output "$G37_MARKDOWN_EVIDENCE"
```

The work root must be on the Linux-native WSL filesystem, not a mounted host
volume. Evidence files must not already exist. The harness always stops its engine
and removes the private database, staged executable, socket, and logs after the
measurement. Evidence contains only artifact and contract digests, aggregate
measurements, abstract hardware class, threshold decisions, and coverage counts.
It contains no local path, endpoint, raw identity, source body, or secret.

Run this lane only after the exact release artifact has been assembled. Do not run
it concurrently with a native compilation or another engine certification.
