//! The dedicated host-start issuer writes the existing protected lease row.
//! Session liveness and the external launch are not atomic with this record.
use super::*;
use medulla_types::host_start::{HostStartProvenance, IssueHostStartGrantRequest};

pub(crate) fn issue(
    graph: &str,
    request: &IssueHostStartGrantRequest,
    admitted_actor: Option<&str>,
    nodes: &mut NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>,
    committed_at_ms: u64,
) -> Result<ControlLeaseIssued, String> {
    let issuer_ref = admitted_actor.ok_or("ACCESS_DENIED: host-start issuer unavailable")?;
    let provenance = HostStartProvenance {
        schema_version: 1,
        issuer_ref: issuer_ref.into(),
    };
    provenance.validate()?;
    let lease = request.lease_at(committed_at_ms)?;
    if load_row(nodes, graph, &lease.lease_id, crypto)?.is_some() {
        return Ok(ControlLeaseIssued {
            outcome: ControlLeaseIssueOutcome::Collision,
            lease: None,
            changed_work_item_ids: Vec::new(),
        });
    }
    let mut row = lease.row();
    row.insert(
        "host_start_provenance".into(),
        serde_json::to_value(provenance).map_err(|e| e.to_string())?,
    );
    write_work_item_props(nodes, graph, &lease.lease_id, &mut row, crypto)?;
    Ok(ControlLeaseIssued {
        outcome: ControlLeaseIssueOutcome::Issued,
        lease: Some(ControlLeaseView::from_row(&lease.lease_id, &row)?),
        changed_work_item_ids: vec![lease.lease_id],
    })
}

#[cfg(test)]
mod tests;
