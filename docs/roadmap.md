# Release qualification and product direction

> This is an **internal, contributor-facing** page, not a capability ledger. The
> authoritative operation-by-operation status lives in the
> **[capability matrix](capabilities.md)** and its generated companion. The
> authoritative `CONCEPT:EG-*` definitions live in [concepts](concepts.md).

The current engine contract is defined by the capability matrix, the owning
architecture pages, and executable evidence in the repository.
The **[North Star: Seamless](north_star.md)** page defines the cross-modal seam
contract; the native, SQL-wire, GraphQL, RDF, timeseries, and transaction surfaces
route through shared current-only machinery.

## Current recommendation-program status

The 2026-07-13 Epistemic Graph/Agent Utilities recommendation program has no
source-level items intentionally postponed. Its P0-P3 implementation surfaces,
architecture gates, deployment manifests, failure harnesses, and certification
commands are present. The generated capability ledger and the program closure
report record the exact validation result for the current worktree.

Exact-release production qualification is evidence gathered by running those
shipped harnesses against the operator's real multi-node, identity, broker,
object-store, observability, accelerator, and recovery targets. A result that
needs external infrastructure is reported as a **certification prerequisite**,
never as an implemented source capability and never as a source-code deferral.

## Current capability closeout

The current implementation includes
native DDS/RTPS legs, the Python LMCache driver, CUDA distance/tensor dispatch and
device-gated parity tests, Iceberg v2 Avro manifests with column statistics,
raster tile pyramids, PL/pgSQL bodies, memory-to-weights export, Calvin OLLP and
deterministic epoch routing, SQLite file import/export, the Rust numeric kernel,
and its Agent Utilities migration. See the capability matrix and owning deep
dives for exact scope.

## Product directions outside the current recommendation program

An admin-console UI, a dashboard-authoring UI, and additional accelerator kernels
are possible product expansions. They are not advertised as current engine
capabilities and are not release blockers for the recommendation program. New
work enters the capability ledger only when it has an implementation, an owner,
and an executable gate.

---

## Current release references

- [Capabilities and parity](capabilities.md) is the human-readable operation matrix.
- [Generated capability ledger](capabilities.generated.md) is the machine-checked per-method contract.
- [North Star](north_star.md) records cross-modal seam coverage.
- [Build feature composition](architecture/tiers.md) defines the main build, `cluster`, and `full-extras`.
- [Native program optimization](architecture/native-program-optimization.md) defines the 13-family, 14-modality optimization plane.
- [Numeric kernel](architecture/numeric_kernel.md) defines the Rust/Python packaging boundary and in-engine analytics.
- [Deployment](deployment.md) and [binary promotion](deploy/binary_promotion.md) define release qualification.
