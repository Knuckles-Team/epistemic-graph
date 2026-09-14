use super::*;

impl GraphCore {
    pub fn invalidate_edge(
        &self,
        source_id: &str,
        target_id: &str,
        relationship: &str,
        invalid_at: u64,
        tx_now: u64,
    ) -> usize {
        self.txn()
            .invalidate_edge(source_id, target_id, relationship, invalid_at, tx_now)
    }

    /// One-shot atomic edge supersession (CONCEPT:AU-KG.ingest.list-durable-media). See
    /// [`GraphTxn::supersede_edge`]; the close-prior + insert-new run under ONE guard.
    #[allow(clippy::too_many_arguments)]
    pub fn supersede_edge(
        &self,
        new_source: String,
        new_target: String,
        new_properties_msgpack: Vec<u8>,
        prior_source: &str,
        prior_target: &str,
        prior_relationship: &str,
        valid_at: u64,
        tx_now: u64,
    ) -> Result<(), String> {
        self.txn().supersede_edge(
            new_source,
            new_target,
            new_properties_msgpack,
            prior_source,
            prior_target,
            prior_relationship,
            valid_at,
            tx_now,
        )
    }
}
