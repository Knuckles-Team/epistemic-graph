# LTAP lakehouse interop (eg-lake, EG-KG.storage.lsn-as-snapshot-returns)

`epistemic-graph` is transactional (redb-authoritative, cross-modal ACID) **and** analytical (DataFusion
SQL, columnar segments, window frames). Program B adds the third leg — **lakehouse interoperability** —
making the engine an **LTAP** (Lakehouse-Transactional-Analytical Processing) superset: external lakehouse
engines read the engine's own tables as open **Parquet + Delta/Iceberg** with **zero ETL**, while writes
still land through the one ACID write path.

This is the `eg-lake` crate (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns), gated behind the `lake`/`lake-rest`
features (a maintained Polars native-Parquet codec + pure-Rust `apache-avro`, both `default-features = false`).
As of W4.8, `lake` and `lake-rest` are part of the **one main `full` build** (`cargo build`, the published
wheel) — the measured release-binary size delta stayed inside the Pi-4 budget (see the W4.8 report / the
`lake =` feature comment in the root `Cargo.toml`). The materialization tier and the Iceberg-REST listener
each remain **opt-in at runtime**: nothing runs unless `GRAPH_SERVICE_PERSIST_DIR` +
`EPISTEMIC_GRAPH_LAKE_MATERIALIZE_INTERVAL_SECS` / `--iceberg-addr` are explicitly configured, exactly like
`--metrics-addr`/`--obs-addr`.

> Positioning: this is what makes the engine a drop-in in front of a **Databricks / Spark / Trino / DuckDB**
> lakehouse — the tables it serves over pgwire/native are *also* an open-format lake the analytical engines
> read directly, with no export pipeline and no second copy of the data to keep in sync.

---

## What it does

```mermaid
flowchart LR
    subgraph Engine["epistemic-graph (LTAP)"]
        TBL["User tables + columnar segments<br/>(eg-query / TableStore)"]
        SNAP["Versioned snapshots + Op::AsOf<br/>(LSN-style as-of)"]
        LAKE["eg-lake: async columnar materializer"]
    end
    subgraph Object["Object store (blob CAS / S3 / MinIO)"]
        PARQ["Parquet data files"]
        DELTA["_delta_log (Delta transaction log)"]
        ICE["Iceberg metadata + snapshots"]
    end
    subgraph Readers["External lakehouse readers (zero ETL)"]
        DBX["Databricks / Spark"]
        TRINO["Trino / Presto"]
        DUCK["DuckDB / Polars"]
    end
    CAT["Iceberg-REST catalog endpoint"]

    TBL --> LAKE
    SNAP --> LAKE
    LAKE --> PARQ
    LAKE --> DELTA
    LAKE --> ICE
    CAT --> ICE
    DBX --> DELTA
    DBX --> CAT
    TRINO --> CAT
    DUCK --> PARQ
```

- **Parquet materialization.** An async tier transcodes an engine table's (or a columnar segment's) rows
  into Arrow record batches and writes **Parquet** data files onto the object store (the blob CAS, or an
  `s3`/MinIO backend behind the same `ChunkStore` trait). No second storage system — the lake files live in
  the engine's own object tier.
- **Delta transaction log.** Each materialization appends to a Delta `_delta_log` (add/remove file actions +
  schema), so a Delta reader (Databricks / `delta-rs` / Spark) sees a consistent table version.
- **Iceberg logs + REST catalog + real Avro manifest.** Iceberg table metadata + snapshot lineage is
  emitted, an **Iceberg-REST catalog** endpoint lets a Trino/Spark catalog resolve the table by name, and a
  spec-compliant **Iceberg v2 Avro manifest + manifest-list writer** (EG-KG.storage.eg-iceberg-avro-manifest/EG-KG.storage.iceberg-manifest-list, `iceberg_avro.rs`,
  behind `lake` via pure-Rust `apache-avro`) is shipped — a committed snapshot's `metadata.json` references
  real Avro that Spark/Trino/DuckDB follow, with per-column stats (`value_counts`/`null_value_counts`/
  `lower_bounds`/`upper_bounds`, keyed by field-id) gathered at materialize time for predicate pushdown /
  file skipping (EG-KG.storage.iceberg-avro-manifest-carries). Partition `field_summary` is null by design (the spec is unpartitioned).
- **LSN-style as-of snapshots.** Materialization reuses the engine's versioned snapshots + `Op::AsOf`
  (bi-temporal, EG-KG.compute.preserved/2.250) so a lake snapshot corresponds to a durable engine LSN — an external reader
  can pin a **time-travel** read that matches an exact engine version, not a fuzzy nightly dump.

---

## Why it is an LTAP superset

| Leg | In epistemic-graph |
|-----|--------------------|
| **T**ransactional | redb-authoritative, commit-before-ack, cross-modal ACID `WriteTransaction`, multi-Raft + cross-shard 2PC |
| **A**nalytical | DataFusion 43 `SELECT` (joins/CTE/window), columnar struct-of-arrays segments (EG-089), PromQL/TSDB |
| **L**akehouse interop | **eg-lake (EG-KG.storage.lsn-as-snapshot-returns)**: Parquet + Delta + Iceberg + LSN as-of + Iceberg-REST catalog — external engines read with zero ETL |

The write path is unchanged and stays the single source of truth; `eg-lake` is a **read-side, additive**
projection. A build that excludes the `lake` feature (e.g. `--no-default-features`) is byte-for-byte the
prior engine; the standard `full` build links it but runs nothing extra unless the materialization interval
or the catalog address is also configured.

---

## Reaching it

- **Feature:** the default `full` build (`cargo build`, the published wheel) already links `lake` + `lake-rest`
  (W4.8); a slim/no-default-features build adds them back with `--features "lake lake-rest server"`. Either
  way, the catalog + materialization surface stay opt-in **at runtime**, activated only by their own
  environment/flag (`EPISTEMIC_GRAPH_LAKE_MATERIALIZE_INTERVAL_SECS`, `--iceberg-addr`). `lake` is a Rust build
  feature only — it is not selected by a Python installation extra (the compiled server binary always carries
  it once built with `full`).
- **Delta readers** point at the `_delta_log` table path on the object store.
- **Iceberg readers** point a catalog at the Iceberg-REST endpoint.
- **Time-travel** reads pin a snapshot that corresponds to an engine LSN (`Op::AsOf`).
- **Known gap (W4.8, `reports/issue-register.md`):** the production listener wiring
  (`serve_with_security`) currently denies every Iceberg-REST request via the shared,
  cross-cutting `server::unauthenticated_carrier_denied` stub — the same pre-existing gap affects
  `obs`/`s3-api`/`sparql-http`/`federation-search`/`kvcache-server`, not something specific to lake. The
  endpoint shapes are correct and covered by `src/server/lake/rest.rs`'s own (non-security) test suite and by
  `tests/test_lake_iceberg_delta_parity.py`'s pyiceberg/deltalake read-parity tests, which drive the real
  `eg-lake` write path directly rather than through the gated listener.

See the [capability matrix](../capabilities.md#lakehouse-interop-eg-lake-ltap) row and
[concepts](../concepts.md) `CONCEPT:EG-KG.storage.lsn-as-snapshot-returns` for the authoritative definition, and
[subsystems](subsystems.md#lakehouse-interop) for how it composes on the one store.

---

**See also:** [Capabilities matrix](../capabilities.md) · [SQL & pgwire](../interfaces/sql.md) · [Analytics Program](analytics_program.md) · [Key-value & Blob](../interfaces/kv_blob.md) · [Cluster Deployment](cluster_deployment.md).
