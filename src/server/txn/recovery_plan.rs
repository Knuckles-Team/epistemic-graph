//! Authenticated private recovery representation.

use super::*;

/// Canonical private recovery body for a prepared transaction coordinator.  It is
/// never written to a coordinator batch, outbox, log message, or trace: callers
/// serialize this deterministic shape, encrypt it with the environment-managed
/// data key, and atomically attach only the ciphertext to the parent receipt.
///
/// `agent` and wall-clock activity are deliberately absent.  Retry authorization
/// is bound by the parent's principal fingerprint, avoiding durable raw identity;
/// idle bookkeeping is reconstructed in memory.
#[derive(serde::Serialize, serde::Deserialize)]
struct DurableTxnPlan {
    schema_version: u16,
    graph: String,
    tenant_scope: String,
    begin_version: u64,
    write_set: Vec<Method>,
    read_set: BTreeMap<String, NodeFingerprint>,
    isolation: IsolationLevel,
    predicate_reads: Vec<(PredicateRead, u64)>,
    extra_writes: BTreeMap<String, Vec<Method>>,
    vectors: Vec<(String, Vec<f32>)>,
    blob_refs: Vec<(String, String)>,
    measurements: Vec<StagedMeasurement>,
    axioms: Vec<Method>,
    constructs: Vec<Method>,
    plan_writeback: Vec<Method>,
}

const DURABLE_TXN_PLAN_VERSION: u16 = 2;
const MAX_DURABLE_TXN_PLAN_BYTES: usize = 64 * 1024 * 1024;
const MAX_DURABLE_TXN_PLAN_ITEMS: usize = 1_000_000;

impl GraphTxnState {
    /// Serialize the complete staged transaction into a stable canonical ordering.
    /// The returned bytes are plaintext only in process memory and MUST be sealed
    /// before persistence.
    pub(crate) fn encode_recovery_plan(&self) -> Result<Vec<u8>, String> {
        let plan = DurableTxnPlan {
            schema_version: DURABLE_TXN_PLAN_VERSION,
            graph: self.graph.clone(),
            tenant_scope: self.tenant_scope.clone(),
            begin_version: self.begin_version,
            write_set: self.write_set.clone(),
            read_set: self
                .read_set
                .iter()
                .map(|(node, fingerprint)| (node.clone(), fingerprint.clone()))
                .collect(),
            isolation: self.isolation,
            predicate_reads: self.predicate_reads.clone(),
            extra_writes: self
                .extra_writes
                .iter()
                .map(|(graph, methods)| (graph.clone(), methods.clone()))
                .collect(),
            vectors: self.vectors.clone(),
            blob_refs: self.blob_refs.clone(),
            measurements: self.measurements.clone(),
            axioms: self.axioms.clone(),
            constructs: self.constructs.clone(),
            plan_writeback: self.plan_writeback.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&plan)
            .map_err(|_| "transaction recovery plan encode failed".to_string())?;
        eg_types::msgpack::validate_single_value(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_DURABLE_TXN_PLAN_BYTES,
                MAX_DURABLE_TXN_PLAN_ITEMS,
                64,
            ),
        )
        .map_err(|_| "transaction recovery plan exceeds limits".to_string())?;
        Ok(bytes)
    }

    /// Reconstruct an ephemeral staged transaction from authenticated private
    /// recovery bytes.  The retrying caller is held only in RAM; its durable scope
    /// was already verified against the parent receipt before this method is called.
    pub(crate) fn decode_recovery_plan(bytes: &[u8], agent: String) -> Result<Self, String> {
        let plan: DurableTxnPlan = eg_types::msgpack::decode_bounded(
            bytes,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_DURABLE_TXN_PLAN_BYTES,
                MAX_DURABLE_TXN_PLAN_ITEMS,
                64,
            ),
        )
        .map_err(|_| "transaction recovery plan is corrupt".to_string())?;
        if plan.schema_version != DURABLE_TXN_PLAN_VERSION {
            return Err(format!(
                "unsupported transaction recovery plan version {}",
                plan.schema_version
            ));
        }
        if plan.graph.is_empty() || plan.tenant_scope.is_empty() {
            return Err("transaction recovery plan has incomplete authority".to_string());
        }
        Ok(GraphTxnState {
            graph: plan.graph,
            tenant_scope: plan.tenant_scope,
            begin_version: plan.begin_version,
            write_set: plan.write_set,
            read_set: plan.read_set.into_iter().collect(),
            isolation: plan.isolation,
            predicate_reads: plan.predicate_reads,
            agent,
            last_active_ms: now_ms(),
            extra_writes: plan.extra_writes.into_iter().collect(),
            vectors: plan.vectors,
            blob_refs: plan.blob_refs,
            measurements: plan.measurements,
            axioms: plan.axioms,
            constructs: plan.constructs,
            plan_writeback: plan.plan_writeback,
        })
    }
}
