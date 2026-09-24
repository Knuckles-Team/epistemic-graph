//! EH-403: `GraphSchema.AttachApproved` -- the one way a governed schema
//! candidate reaches a graph's schema sources.
//!
//! Before anything is staged, the handler reads the named approval lease from
//! the request graph's native control-lease rows (tenant-bound: another
//! tenant's lease is simply not visible) and checks it with
//! [`eg_types::graph_schema::approval::verify_schema_approval`] against the
//! exact candidate digest. Only then does the attach enter the ordinary
//! GraphSchema gateway commit (audit, CDC, ordering, full entering-schema
//! validation), under the reserved `approved:` key with an `Approved` origin
//! that records the lease id as provenance.

use std::sync::Arc;

use eg_types::graph_schema::approval::{approved_candidate_digest, verify_schema_approval};
use eg_types::graph_schema::GraphSchemaOp;

use crate::protocol::{Method, Response};
use crate::server::mutation::{MutationCtx, MutationPlan};

/// Verify the approval, then commit the attach through the gateway.
pub(super) async fn handle_gateway(
    tenant_id: &str,
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    op: &GraphSchemaOp,
) -> Response {
    if let Err(error) = admit(tenant_id, ctx, op).await {
        return Response::err(ctx.req_id, error);
    }
    let op = op.clone();
    let graph_name = ctx.graph_name.to_string();
    crate::server::handlers::graph_ops::commit_gateway(ctx, plan, method, move |core| {
        super::apply(core, &graph_name, &op)
    })
    .await
}

/// The approval check that must pass before the attach is staged.
async fn admit(tenant_id: &str, ctx: &MutationCtx<'_>, op: &GraphSchemaOp) -> Result<(), String> {
    op.validate()?;
    let GraphSchemaOp::AttachApproved {
        source_id,
        shapes_ttl,
        ontology_ttl,
        approval_lease_id,
        ..
    } = op
    else {
        return Err("attach_approved handler received another operation".to_string());
    };
    let candidate = approved_candidate_digest(source_id, shapes_ttl.as_deref(), ontology_ttl.as_deref());
    let lease = read_lease(ctx, tenant_id, approval_lease_id).await?;
    verify_schema_approval(
        lease.as_ref(),
        source_id,
        &candidate,
        crate::server::txn::now_ms(),
    )
}

#[cfg(feature = "redb")]
async fn read_lease(
    ctx: &MutationCtx<'_>,
    tenant_id: &str,
    lease_id: &str,
) -> Result<Option<eg_types::control_lease::ControlLeaseView>, String> {
    let backend = ctx
        .persistence
        .and_then(|backend| backend.as_redb())
        .ok_or_else(approval_store_unavailable)?;
    backend
        .read_control_lease(&crate::persist::sanitize(ctx.graph_name), tenant_id, lease_id)
        .await
}

/// Approval leases live only in the redb authority; without it no approval
/// can be proven, so the attach is refused.
#[cfg(not(feature = "redb"))]
async fn read_lease(
    _ctx: &MutationCtx<'_>,
    _tenant_id: &str,
    _lease_id: &str,
) -> Result<Option<eg_types::control_lease::ControlLeaseView>, String> {
    Err(approval_store_unavailable())
}

fn approval_store_unavailable() -> String {
    format!(
        "{}: this engine build holds no native approval leases",
        eg_types::graph_schema::GraphSchemaErrorCode::ApprovalRequired.as_str()
    )
}

/// The `Approved`-origin source a validated `AttachApproved` installs.
pub(super) fn source(op: &GraphSchemaOp) -> Result<crate::graph::GraphSchemaSource, String> {
    let GraphSchemaOp::AttachApproved {
        source_id,
        shapes_ttl,
        ontology_ttl,
        approval_lease_id,
        ..
    } = op
    else {
        return Err("approved schema source needs an attach_approved operation".to_string());
    };
    let name = source_id
        .strip_prefix(eg_types::graph_schema::approval::APPROVED_SOURCE_PREFIX)
        .ok_or_else(|| "approved schema source id lacks its namespace".to_string())?;
    crate::graph::GraphSchemaSource::new(
        crate::graph::SchemaSourceOrigin::Approved {
            name: name.to_string(),
            approval_lease_id: approval_lease_id.clone(),
        },
        shapes_ttl.as_deref().map(Arc::from),
        ontology_ttl.as_deref().map(Arc::from),
        0,
    )
}

#[cfg(all(test, feature = "redb"))]
mod tests;
