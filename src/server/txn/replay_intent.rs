//! Stable semantic transaction identity, independent of ephemeral OCC observations.

use super::*;

/// Only caller intent participates in idempotency. The complete observations
/// remain in the separately sealed `DurableTxnPlan` for recovery validation.
#[derive(serde::Serialize)]
struct DurableTxnIntent<'a> {
    schema_version: u16,
    graph: &'a str,
    tenant_scope: &'a str,
    write_set: &'a [Method],
    read_keys: std::collections::BTreeSet<&'a str>,
    isolation: IsolationLevel,
    predicates: Vec<&'a PredicateRead>,
    extra_writes: BTreeMap<&'a str, &'a [Method]>,
    vectors: &'a [(String, Vec<f32>)],
    blob_refs: &'a [(String, String)],
    measurements: &'a [StagedMeasurement],
    axioms: &'a [Method],
    constructs: &'a [Method],
    plan_writeback: &'a [Method],
}

impl GraphTxnState {
    pub(crate) fn replay_intent_digest(&self) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        let intent = DurableTxnIntent {
            schema_version: 1,
            graph: &self.graph,
            tenant_scope: &self.tenant_scope,
            write_set: &self.write_set,
            read_keys: self.read_set.keys().map(String::as_str).collect(),
            isolation: self.isolation,
            predicates: self
                .predicate_reads
                .iter()
                .map(|(predicate, _)| predicate)
                .collect(),
            extra_writes: self
                .extra_writes
                .iter()
                .map(|(graph, methods)| (graph.as_str(), methods.as_slice()))
                .collect(),
            vectors: &self.vectors,
            blob_refs: &self.blob_refs,
            measurements: &self.measurements,
            axioms: &self.axioms,
            constructs: &self.constructs,
            plan_writeback: &self.plan_writeback,
        };
        let bytes = rmp_serde::to_vec_named(&intent)
            .map_err(|_| "transaction replay intent encode failed".to_string())?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }
}
