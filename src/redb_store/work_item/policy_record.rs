//! EH-346 policy-evolution record rows. `PolicyEvolution` admits a record
//! against the request graph, then stores it here through the internal
//! `PolicyEvolutionStore` method -- inside the SAME durable WorkItem
//! MutationBatch kernel as the control leases (replicated, audited, identity
//! from the record id). This is the only writer of a policy-evolution row: the
//! row guard refuses every generic graph write that would create, change or
//! remove one.
//!
//! A record is content-addressed and immutable, so a store is create-only: a
//! row already holding the id is left untouched and answers `created: false`.

use eg_types::policy_evolution::{PolicyRecordStored, StoredPolicyRecord};
use eg_types::result_contract::graph::PolicyEvolutionStore;

use super::*;

/// The policy-record arm of the WorkItem-family applier: `None` for any other
/// method.
pub(crate) fn apply_policy_record_rows(
    graph: &str,
    method: &Method,
    nodes: &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let Method::PolicyEvolutionStore { request } = method else {
        return Ok(None);
    };
    store_policy_record_row(graph, request, nodes, crypto).map(Some)
}

fn store_policy_record_row(
    graph: &str,
    request: &StoredPolicyRecord,
    nodes: &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<crate::protocol::ResultPayload, String> {
    request
        .verify_identity()
        .map_err(|refusal| refusal.to_string())?;
    let created = nodes.get((graph, request.record_id.as_str()))?.is_none();
    if created {
        let mut row = request.row()?;
        write_work_item_props(nodes, graph, &request.record_id, &mut row, crypto)?;
    }
    crate::protocol::ResultPayload::of::<PolicyEvolutionStore>(PolicyRecordStored {
        created,
        changed_work_item_ids: created
            .then(|| request.record_id.clone())
            .into_iter()
            .collect(),
    })
}

#[cfg(test)]
mod tests;
