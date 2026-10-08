# Delivery and integration plan

## Design decisions

The implementation extends EG's current typed pack, component, schema, outbox and capability registry. It does not add a separate pack database, a dynamic response envelope, a second contract generator or a graph projection side channel. The public wire is generated from `eg-capabilities`; canonical receipt content must be compared byte for byte. Changes to a shared wire shape land as one coordinated revision across Rust, generated Python/Go/JS and the SDK TCK.

The order below minimizes incompatible intermediate states. A step may be a verification or repair of existing code; the existence of a module is not proof that the step is accepted. Each step maps to `tasks.md` and `test-spec.md`.

1. **Freeze contract and baseline.** Record the exact default-branch revision, current pack schema version and generated receipt. Run contract generation in check mode, full test inventory, CCCC, jscpd, Dupehound and KISS scans. Compare committed artifact *content* across all output directories. Classify findings as fixed, open or unrelated with exact evidence.
2. **Type and encoding proof.** Audit `eg-types` bounds, kinds, digest framing, sorted lists, optionals and canonical fixture vectors. Make the SDK's JSON serialization and deterministic idempotency key a shared TCK contract. Extend generated method/result descriptors rather than hand-writing client DTO copies.
3. **Admission and atomicity proof.** Audit G1–G21, tenant/importer checks, reserved IDs, preflight size checks and stable codes. Ensure the import planner resolves staged pins, stages engine bodies with holder/refcount invariants, and commits all owner rows plus N outbox intents atomically. Test all crash windows and replay identities.
4. **Lifecycle and catalog proof.** Verify carry-forward, changed pin cascade, reversible withdrawal, permanent retirement, content reads, no-op import and mass-withdrawal override. Confirm typed ServerRegistry owns liveness and component/pack owner owns content; remove any shadow catalog after consumer migration.
5. **Schema and projection proof.** Reuse one G13–G16/K1–K7 validator for operator, pack and ingestion sources. Attach current pack schema in one graph commit. Mark visibility only after attach; use durable failure/retry and least-privilege row-scoped graph grant. Verify static `AttachPack` staleness and `GraphSchemaClasses` output. Preserve exact `IcvConfigure` behavior.
6. **Consumer and release proof.** Run an end-to-end connector fixture from SDK package creation through Import, projection, GraphOS typed read and an authorized body read. Run Go/JS/Python/Rust contract vectors and full integration suites on the exact commit. Include scanner and architecture gates, release workflow parity, and a crash/restart restore check. Publish exact evidence before setting a requirement to `ACCEPTED`.

## Interfaces and ownership

| Interface | EG owner | Caller/consumer contract |
|---|---|---|
| Pack wire and digest | `eg-types` connector-pack modules; generated fixtures | SDK creates index/archive and uses exact golden vectors. |
| Import and admin ops | `src/server/handlers/admin/connector_pack/` with typed method descriptor | SDK uses `Status` then deterministic Import; admin calls Bind/Unbind/Retire/Reproject. |
| Durable rows and body liveness | Agent Library owner tables plus `engine_bodies` | No client writes component rows or body holder state directly. |
| Projection and graph schema | Outbox worker; `GraphSchema` graph commit; visibility gate | Typed catalog, assembly and search consume only visible head state. |
| Fleet catalog | ServerRegistry liveness plus component content | GraphOS joins typed reads and may cache by version, without becoming content authority. |
| Contract receipt | `eg-capabilities` registry and `gen_contract` | Python/Go/JS senders and SDK verify method/result/signing vectors. |

## R016 digest v4: package version is provenance only

EG-TYPED-PACKS-R016 requires equal server pins across package releases with equal content. The v3 definition digest mixes `SourcePackage.package_version` into every server component digest. Two independent stores that import the same content under different releases therefore mint different server pins.

- **Versioned digest.** New writes mint digest v4 under domain `au-eg/agent-component-definition/v4`. v4 omits `package_version` from the provenance input. The entry still stores the package version as provenance text.
- **Read compatibility.** `AgentComponentEntry::validate` accepts a digest that matches v4 or v3. The v3 path keeps domain `au-eg/agent-component-definition/v3` and the old input set. No write mints v3.
- **No migration.** The record shape does not change, so `AGENT_COMPONENT_SCHEMA_VERSION` stays 3. Stored v3 rows validate in place. Retire keeps a row's recorded digest.
- **Domain separation.** Distinct domains stop a v4 digest from colliding with a v3 digest over different inputs.
- **One-time revision.** The first import after the upgrade recomputes the server pin under v4. A changed pin issues one ordinary revision for the server and its dependents. Later releases with equal content stay `Unchanged`.
- **Proof.** A unit test proves v4 equality across package versions and v3 read compatibility. The connector-pack test `package_versions_produce_equal_pins_in_independent_stores` returns.

## Migration and rollback

The current connector pack schema is version 2; compatible additions keep existing method semantics and require regenerated artifacts. An incompatible version bump must refuse old payloads with a stable code and be coupled to client TCK changes. Agent Library layout changes use a registered layout lineage and named manifest refusal; no implicit destructive upgrade is allowed. Deployment rolls forward with dual-reader compatibility only if explicitly tested; rollback to an older binary is rejected when its layout cannot read the new owner file. A projection failure leaves the committed pack invisible and re-drivable. ReconcileBodies reclaims only unheld bodies after the grace period. A static schema attachment on another graph remains at its recorded pack revision until an administrator reattaches it.

## Completion evidence

For each PACK requirement, attach a PR/merged commit link, contract receipt digest, focused test output, full release workflow run and, for runtime behavior, served fixture result. `evidence.md` can be added when a tested implementation is available; this proposed spec makes no passing claim. The acceptance matrix in `test-spec.md` is the authoritative proof list.
