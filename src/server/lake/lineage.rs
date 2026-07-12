//! OpenLineage `RunEvent` construction + (optional) HTTP export (CONCEPT:EG-317,
//! INT-P2-3) for the lake materialization tier.
//!
//! Every `LakeManager` materialize/compact/delete run builds one OpenLineage
//! `RunEvent` — the spec's job/run/dataset shape (`openlineage.io/spec/…/OpenLineage.json`)
//! — carrying:
//!  * a **job** (`epistemic-graph.lake` / `materialize.<namespace>.<table>`),
//!  * a **run** (a stable, deterministic `runId`),
//!  * an **input dataset** (the tsdb series the rows were drained from, when known —
//!    `epistemic-graph.tsdb` / `<series_id>`), and
//!  * an **output dataset** (`<namespace>` / `<namespace>.<table>`) carrying the
//!    canonical `schema` (`SchemaDatasetFacet`), `dataSource` (`DatasourceDatasetFacet`)
//!    and `outputStatistics` (`OutputStatisticsOutputDatasetFacet`) facets, a
//!    `lifecycleStateChange` facet on CREATE/OVERWRITE/TRUNCATE runs (the OpenLineage
//!    enum's structural-change values — a plain incremental APPEND carries none, since
//!    it changes no lifecycle state), and a small vendor **custom facet**
//!    (`epistemicGraphLake`) carrying the engine-specific LSN/Iceberg-snapshot
//!    correlation OpenLineage's custom-facet extensibility model is FOR.
//!
//! Dependency-free (no `chrono`/`uuid` — a hand-rolled ISO-8601 formatter + an FNV
//! hash-derived UUID-shaped run id, mirroring `eg_lake::stable_table_id`'s own
//! convention). The (optional) HTTP push reuses the SAME pure-Rust `ureq` client
//! `sparql-service`/`federation-search`/`otel-export` already link — no new HTTP dep.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use eg_lake::schema::LakeSchema;

use super::LakeOp;

/// Env var carrying an OpenLineage HTTP collector base endpoint (e.g. a Marquez
/// instance, `http://marquez:5000`). Unset ⇒ events are still built + kept in the
/// in-memory ring + traced, but never pushed over HTTP.
pub const OPENLINEAGE_URL_ENV: &str = "EPISTEMIC_GRAPH_OPENLINEAGE_URL";
/// The `producer` OpenLineage stamps on every event this tier emits.
const PRODUCER: &str = "https://github.com/knucklessg1/epistemic-graph/tree/main/crates/eg-lake";
const SCHEMA_URL: &str = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent";
const JOB_NAMESPACE: &str = "epistemic-graph.lake";

/// Wall-clock now, epoch milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Format epoch-ms as an ISO-8601 UTC instant. Minimal hand-rolled formatter (no
/// chrono — the Pi contract): days-since-epoch → civil date via Howard Hinnant's
/// algorithm, mirroring `crate::server::s3`'s own `iso8601` helper (kept as a small,
/// self-contained duplicate rather than a cross-feature dependency on `s3-api`).
fn iso8601_ms(epoch_ms: u64) -> String {
    let secs = epoch_ms / 1000;
    let ms = epoch_ms % 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{sec:02}.{ms:03}Z")
}

/// A deterministic, UUID-shaped run id derived from the table + lsn (FNV-1a 64,
/// mirroring `eg_lake`'s own `stable_table_id` convention) — stable across a retry so
/// pushing the SAME event twice doesn't mint two "different" runs, and avoids a `uuid`
/// dependency.
fn run_id(namespace: &str, table: &str, lsn: u64) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in format!("{namespace}/{table}@{lsn}").bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let lo: u64 = h.rotate_left(17) ^ 0x9e3779b97f4a7c15;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (h >> 32) as u32,
        (h >> 16) as u16,
        h as u16,
        (lo >> 48) as u16,
        lo & 0xffff_ffff_ffff
    )
}

/// The Iceberg logical type name a `LakeSchema` field renders in the OpenLineage
/// `SchemaDatasetFacet` (the same type vocabulary the Iceberg `metadata.json` itself
/// uses, so the two agree).
fn schema_facet(schema: &LakeSchema) -> Value {
    let fields: Vec<Value> = schema
        .fields
        .iter()
        .map(|f| json!({ "name": f.name, "type": f.ty.iceberg_type_name() }))
        .collect();
    json!({
        "_producer": PRODUCER,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
        "fields": fields,
    })
}

/// Build one OpenLineage `RunEvent` for a completed materialize/compact/delete run.
/// `input_dataset`, when `Some((namespace, name))`, names the upstream tsdb series the
/// rows were drained from (absent for a pure `compact`/`delete_where`/REST-commit
/// rewrite, which reads its OWN prior files rather than an external source).
#[allow(clippy::too_many_arguments)]
pub fn build_run_event(
    namespace: &str,
    table: &str,
    op: LakeOp,
    schema: &LakeSchema,
    num_rows: u64,
    bytes_len: u64,
    location: &str,
    lsn: u64,
    iceberg_snapshot_id: i64,
    input_dataset: Option<(&str, &str)>,
) -> Value {
    let ts_ms = now_ms();
    let job_name = format!("materialize.{namespace}.{table}");

    let mut output_facets = serde_json::Map::new();
    output_facets.insert("schema".to_string(), schema_facet(schema));
    output_facets.insert(
        "dataSource".to_string(),
        json!({
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
            "name": location,
            "uri": location,
        }),
    );
    output_facets.insert(
        "outputStatistics".to_string(),
        json!({
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet",
            "rowCount": num_rows,
            "size": bytes_len,
        }),
    );
    // A plain incremental append changes no dataset LIFECYCLE state (no create/drop/
    // overwrite/truncate), so the facet is only emitted for the structural ops.
    if !matches!(op, LakeOp::Append) {
        output_facets.insert(
            "lifecycleStateChange".to_string(),
            json!({
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
                "lifecycleStateChange": op.as_str(),
            }),
        );
    }
    output_facets.insert(
        "epistemicGraphLake".to_string(),
        json!({
            "_producer": PRODUCER,
            "op": op.as_str(),
            "lsn": lsn,
            "icebergSnapshotId": iceberg_snapshot_id,
            "format": "parquet",
            "location": location,
        }),
    );

    let inputs: Vec<Value> = input_dataset
        .map(|(ns, name)| vec![json!({ "namespace": ns, "name": name, "facets": {} })])
        .unwrap_or_default();

    json!({
        "eventType": "COMPLETE",
        "eventTime": iso8601_ms(ts_ms),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {
            "runId": run_id(namespace, table, lsn),
            "facets": {},
        },
        "job": {
            "namespace": JOB_NAMESPACE,
            "name": job_name,
            "facets": {
                "documentation": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json#/$defs/DocumentationJobFacet",
                    "description": format!(
                        "eg-lake LTAP materialization of {namespace}.{table} (op={})",
                        op.as_str()
                    ),
                },
            },
        },
        "inputs": inputs,
        "outputs": [
            {
                "namespace": namespace,
                "name": format!("{namespace}.{table}"),
                "facets": Value::Object(output_facets),
            }
        ],
    })
}

/// Best-effort push of an already-built event to `OPENLINEAGE_URL_ENV`, when set.
/// Silently a no-op with the var unset (events still traced + ring-buffered by the
/// caller) — lineage export must never fail or block a materialization run.
#[cfg(feature = "lake")]
pub fn maybe_push_http(event: &Value) {
    let Ok(endpoint) = std::env::var(OPENLINEAGE_URL_ENV) else {
        return;
    };
    if endpoint.trim().is_empty() {
        return;
    }
    let url = format!("{}/api/v1/lineage", endpoint.trim_end_matches('/'));
    let body = event.to_string();
    let _ = ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&body);
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_lake::schema::{LakeField, LakeType};

    #[test]
    fn iso8601_formats_epoch_correctly() {
        assert_eq!(iso8601_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_ms(1_700_000_000_000), "2023-11-14T22:13:20.000Z");
    }

    #[test]
    fn run_id_is_deterministic_and_uuid_shaped() {
        let a = run_id("ns", "t", 7);
        let b = run_id("ns", "t", 7);
        let c = run_id("ns", "t", 8);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 36);
        assert_eq!(a.matches('-').count(), 4);
    }

    #[test]
    fn build_run_event_carries_job_run_dataset_and_facets() {
        let schema = LakeSchema::new(vec![
            LakeField::required("ts", LakeType::Timestamp),
            LakeField::new("value", LakeType::Double),
        ]);
        let ev = build_run_event(
            "engine",
            "sensor1",
            LakeOp::Overwrite,
            &schema,
            10,
            2048,
            "lake://engine/sensor1",
            42,
            42,
            Some(("epistemic-graph.tsdb", "sensor1")),
        );
        assert_eq!(ev["eventType"], "COMPLETE");
        assert_eq!(ev["job"]["namespace"], "epistemic-graph.lake");
        assert_eq!(ev["job"]["name"], "materialize.engine.sensor1");
        assert!(ev["run"]["runId"].as_str().unwrap().len() == 36);
        assert_eq!(ev["inputs"][0]["name"], "sensor1");
        let out = &ev["outputs"][0];
        assert_eq!(out["name"], "engine.sensor1");
        assert_eq!(out["facets"]["schema"]["fields"][1]["name"], "value");
        assert_eq!(out["facets"]["outputStatistics"]["rowCount"], 10);
        assert_eq!(out["facets"]["outputStatistics"]["size"], 2048);
        assert_eq!(
            out["facets"]["lifecycleStateChange"]["lifecycleStateChange"],
            "OVERWRITE"
        );
        assert_eq!(out["facets"]["epistemicGraphLake"]["lsn"], 42);
    }

    #[test]
    fn append_op_omits_lifecycle_facet() {
        let schema = LakeSchema::new(vec![LakeField::required("ts", LakeType::Timestamp)]);
        let ev = build_run_event(
            "engine",
            "s",
            LakeOp::Append,
            &schema,
            1,
            8,
            "lake://engine/s",
            2,
            2,
            None,
        );
        assert!(ev["outputs"][0]["facets"]["lifecycleStateChange"].is_null());
        assert!(ev["inputs"].as_array().unwrap().is_empty());
    }
}
