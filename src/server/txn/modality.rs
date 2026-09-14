//! Cross-modal staging on the detached transaction.

use super::*;

impl GraphTxnState {
    /// Stage a VECTOR upsert into the cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). The
    /// node it targets is captured into the OCC read-set (so a concurrent change to
    /// that node still conflicts), then the embedding is queued to land atomically at
    /// commit. Only the DEFAULT graph's vectors participate in the one-txn barrier.
    pub(crate) fn stage_vector(
        &mut self,
        core: &GraphCore,
        node_id: String,
        embedding: Vec<f32>,
        now_ms: u64,
    ) {
        self.observe(core, &node_id);
        self.vectors.push((node_id, embedding));
        self.last_active_ms = now_ms;
    }

    /// Stage a BLOB REFERENCE into the cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). The
    /// node is captured into the OCC read-set; the `(node_id, digest)` ref lands
    /// atomically with the node/vector/property at commit.
    pub(crate) fn stage_blob_ref(
        &mut self,
        core: &GraphCore,
        node_id: String,
        digest: String,
        now_ms: u64,
    ) {
        self.observe(core, &node_id);
        self.blob_refs.push((node_id, digest));
        self.last_active_ms = now_ms;
    }

    /// Stage a TIME-SERIES measurement batch into the cross-modal write-set
    /// (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Measurements are not graph nodes, so nothing is added to the OCC
    /// node read-set; the batch is queued to land atomically with the txn's other
    /// modalities at commit.
    pub(crate) fn stage_measurement(&mut self, measurement: StagedMeasurement, now_ms: u64) {
        self.measurements.push(measurement);
        self.last_active_ms = now_ms;
    }

    /// Stage OWL AXIOM writes, pre-lowered to `AddNode`/`AddEdge` methods (CONCEPT:EG-KG.txn.extended-cross-modal).
    /// Each method's referenced nodes are captured into the OCC read-set (so a concurrent
    /// change to a touched node still conflicts), then the methods are queued to land in
    /// the SAME cross-modal commit.
    pub(crate) fn stage_axiom(&mut self, core: &GraphCore, methods: Vec<Method>, now_ms: u64) {
        for m in &methods {
            self.observe_method(core, m);
        }
        self.axioms.extend(methods);
        self.last_active_ms = now_ms;
    }

    /// Stage SPARQL CONSTRUCT results, pre-lowered to `AddNode`/`AddEdge` methods
    /// (CONCEPT:EG-KG.txn.construct-evaluated). Same OCC read-set capture + atomic-commit semantics as
    /// [`Self::stage_axiom`].
    pub(crate) fn stage_construct(&mut self, core: &GraphCore, methods: Vec<Method>, now_ms: u64) {
        for m in &methods {
            self.observe_method(core, m);
        }
        self.constructs.extend(methods);
        self.last_active_ms = now_ms;
    }

    /// Stage PLANNER WRITEBACK methods, pre-lowered to `AddNode`/`AddEdge` methods
    /// (CONCEPT:EG-KG.query.plan-dag, D7 — the planner-writeback ACID seam). Same OCC
    /// read-set capture + atomic-commit semantics as [`Self::stage_axiom`] /
    /// [`Self::stage_construct`] — copied verbatim, this is the well-precedented shape
    /// every staged-and-lowered modality shares.
    pub(crate) fn stage_plan_writeback(
        &mut self,
        core: &GraphCore,
        methods: Vec<Method>,
        now_ms: u64,
    ) {
        for m in &methods {
            self.observe_method(core, m);
        }
        self.plan_writeback.extend(methods);
        self.last_active_ms = now_ms;
    }
}
