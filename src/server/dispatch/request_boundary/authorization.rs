use super::*;

#[derive(Clone, Copy)]
pub(crate) struct DispatchAuthority {
    pub(super) state_machine_authorized: bool,
    pub(super) identity_bootstrap: bool,
}

/// Bind the generated WorkItem command context to the already verified carrier.
/// The command carries a full GOC-15 `RequestContext` for durable provenance,
/// but it is not a second authority: tenant, graph, agent, audience, policy,
/// and every downstream scope must be derived from (or narrower than) the
/// authenticated envelope before the command can be proposed or committed.
fn validate_submit_context(
    graph: &str,
    context: &crate::epistemic_operations::RequestContext,
    verified_context: &VerifiedRequestContext,
) -> Result<(), String> {
    if context.schema_version != crate::epistemic_operations::RequestContextSchemaVersion::V2 {
        return Err("SubmitWorkItem context schema_version is unsupported".to_string());
    }
    if context.graph != graph {
        return Err("SubmitWorkItem context graph does not match request graph".to_string());
    }
    // The tenant/agent/audience/policy_version identity check is the one comparison
    // every request boundary shares -- both this native `SubmitWorkItem` command
    // binding and the `kg-delegate` context validation
    // (`server::handlers::delegation::validate_request_context`) run the exact same
    // four-field comparison over the same verified-authority carrier before going on
    // to check their own request's time window / scope bounds, which ARE
    // surface-specific and stay local to each caller. `context_matches_verified_authority`
    // is defined once, in `handlers::delegation` (its `pub(crate)` home), and called
    // from both boundaries.
    if !crate::server::handlers::delegation::context_matches_verified_authority(
        context,
        verified_context,
    ) {
        return Err("SubmitWorkItem context does not match verified request authority".to_string());
    }
    if !submit_context_within_carrier_bounds(context, verified_context) {
        return Err("SubmitWorkItem context violates the verified carrier bounds".to_string());
    }
    Ok(())
}

fn submit_context_within_carrier_bounds(
    context: &crate::epistemic_operations::RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    !context.request_id.trim().is_empty()
        && !context.subject_id.trim().is_empty()
        && !context.trace_id.trim().is_empty()
        && !context
            .scopes
            .iter()
            .any(|scope| scope.trim().is_empty() || !verified_context.allows_action(scope))
        && context.expires_at_ms >= context.issued_at_ms
}

fn required_resource_controller_scope(method: &Method) -> Option<&'static str> {
    match method {
        Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. } => Some("resource:reserve"),
        Method::UpdateResourceHost { .. } => Some("resource:host"),
        _ => None,
    }
}

fn required_capacity_controller_scope(method: &Method) -> Option<&'static str> {
    match method {
        Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. } => Some("capacity:lease"),
        Method::UpdateCapacityCell { .. } => Some("capacity:admin"),
        _ => None,
    }
}

/// One controller-scope gate. `authority` names the surface in the refusal text
/// so the caller's message is byte-identical to the inline checks this replaced.
fn enforce_controller_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    required_scope: Option<&str>,
    authority: &str,
) -> Result<(), Response> {
    let Some(required_scope) = required_scope else {
        return Ok(());
    };
    if verified_context.allows_action(required_scope) || verified_context.allows_action("kg:admin")
    {
        return Ok(());
    }
    crate::metrics::access_denied();
    Err(Response::err(
        req.id,
        format!(
            "ACCESS_DENIED: {authority} authority requires controller scope '{required_scope}'"
        ),
    ))
}

fn check_resource_and_capacity_controller_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
) -> Result<(), Response> {
    // Resource authority is checked BEFORE capacity authority; a request that
    // violates both must keep reporting the resource refusal.
    enforce_controller_scope(
        req,
        verified_context,
        required_resource_controller_scope(&req.method),
        "resource",
    )?;
    enforce_controller_scope(
        req,
        verified_context,
        required_capacity_controller_scope(&req.method),
        "capacity",
    )
}

async fn check_resource_and_capacity_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    authority: DispatchAuthority,
) -> Result<(), Response> {
    if !authority.state_machine_authorized && !authority.identity_bootstrap {
        if matches!(
            &req.method,
            Method::MintWorkItemClaimCapability { .. }
                | Method::VerifyWorkItemClaimCapability { .. }
        ) {
            let capability_authorized = verified_context.allows_action("work:claim-capability")
                || verified_context.allows_action("kg:admin");
            if !capability_authorized {
                crate::metrics::access_denied();
                return Err(Response::err(
                    req.id,
                    "ACCESS_DENIED: WorkItem claim capability requires work:claim-capability",
                ));
            }
        }
        check_resource_and_capacity_controller_scope(req, verified_context)?;
    }
    Ok(())
}

/// The tenant a resource body names, if any. This is a correlation carried by
/// the request body, NOT an authority claim.
fn requested_resource_tenant_ref(method: &Method) -> Option<&str> {
    match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => Some(request.tenant_ref.as_str()),
        Method::QueryWorkItemReservation { request }
        | Method::ResourceReservationStatus { request } => Some(request.tenant_ref.as_str()),
        Method::UpdateResourceHost { request } => Some(request.tenant_ref.as_str()),
        _ => None,
    }
}

/// The tenant a capacity body names, if any. This remains a correlation carried
/// by the request body, NOT an authority claim.
fn requested_capacity_tenant_ref(method: &Method) -> Option<&str> {
    match method {
        Method::AcquireCapacity { request } => Some(request.tenant_ref.as_str()),
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            Some(request.tenant_ref.as_str())
        }
        Method::ReclaimExpiredCapacity { request } => Some(request.tenant_ref.as_str()),
        Method::ReconcileCapacity { request } | Method::CapacityStatus { request } => {
            Some(request.tenant_ref.as_str())
        }
        _ => None,
    }
}

/// The tenant a WorkItem body names, if any. This is a correlation carried by
/// the request body, NOT an authority claim.
fn requested_work_item_tenant_ref(method: &Method) -> Option<&str> {
    match method {
        Method::SubmitWorkItem { request } => Some(request.context.tenant_id.as_str()),
        Method::KgDelegate { request } => Some(request.context.tenant_id.as_str()),
        Method::SubmitWorkItems { request } => Some(request.context.tenant_id.as_str()),
        _ => None,
    }
}

/// The tenant a resource/capacity/work-item body names, if any. This is a
/// correlation carried by the request body, NOT an authority claim.
fn requested_tenant_ref(method: &Method) -> Option<&str> {
    requested_resource_tenant_ref(method)
        .or_else(|| requested_capacity_tenant_ref(method))
        .or_else(|| requested_work_item_tenant_ref(method))
}

/// Only `kg:admin`, or an explicitly privileged aggregate READER on the two
/// reconciliation surfaces, may name a tenant other than the verified one.
fn cross_tenant_access_allowed(method: &Method, verified_context: &VerifiedRequestContext) -> bool {
    verified_context.allows_action("kg:admin")
        || (matches!(
            method,
            Method::QueryWorkItemReservation { .. } | Method::ResourceReservationStatus { .. }
        ) && verified_context.allows_action("resource:read:aggregate"))
        || (matches!(
            method,
            Method::ReconcileCapacity { .. } | Method::CapacityStatus { .. }
        ) && verified_context.allows_action("capacity:read:aggregate"))
}

fn check_cross_tenant_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    authority: DispatchAuthority,
) -> Result<(), Response> {
    if authority.state_machine_authorized || authority.identity_bootstrap {
        return Ok(());
    }
    let names_other_tenant =
        requested_tenant_ref(&req.method).is_some_and(|tenant| tenant != verified_context.tenant());
    if !names_other_tenant || cross_tenant_access_allowed(&req.method, verified_context) {
        return Ok(());
    }
    crate::metrics::access_denied();
    Err(Response::err(
        req.id,
        "ACCESS_DENIED: resource tenant must match verified request tenant",
    ))
}

pub(crate) async fn check_scope_and_admin_authority(
    state: &Arc<RwLock<ServerState>>,
    req: &Request,
    verified_context: &VerifiedRequestContext,
    authority: DispatchAuthority,
    action: &'static str,
    mutates: bool,
) -> Result<(), Response> {
    check_resource_and_capacity_scope(req, verified_context, authority).await?;
    // The tenant in a resource body is a correlation, not an authority claim.
    // Bind ordinary callers to the verified request tenant before the native
    // backend sees the request.  Only an explicitly privileged aggregate reader
    // (for reconciliation) or `kg:admin` may inspect another tenant's rows.
    check_cross_tenant_scope(req, verified_context, authority)?;
    if !authority.state_machine_authorized
        && !authority.identity_bootstrap
        && !verified_context.allows_method(action, mutates)
    {
        crate::metrics::access_denied();
        return Err(Response::err(
            req.id,
            format!("ACCESS_DENIED: verified request context lacks required scope '{action}'"),
        ));
    }
    if !authority.state_machine_authorized
        && is_admin_authz_action(action)
        && !authority.identity_bootstrap
    {
        let s = timed_read(state).await;
        let result = require_admin_capability(&s.isolation, req.agent_id.as_deref(), action);
        drop(s);
        if let Err(msg) = result {
            return Err(Response::err(req.id, msg));
        }
    }
    Ok(())
}

/// Every carrier context a submit method asserts, in the order the inline
/// checks validated them: the envelope's own context first, then each child's.
/// That order is load-bearing — a batch invalid at both levels must keep
/// reporting the envelope's failure.
fn submit_work_item_contexts(method: &Method) -> Vec<&crate::epistemic_operations::RequestContext> {
    match method {
        Method::SubmitWorkItem { request } => vec![&request.context],
        Method::KgDelegate { request } => vec![&request.context],
        Method::SubmitWorkItems { request } => std::iter::once(&request.context)
            .chain(request.requests.iter().map(|child| &child.context))
            .collect(),
        _ => Vec::new(),
    }
}

pub(crate) fn check_submit_work_item_context(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    authority: DispatchAuthority,
) -> Result<(), Response> {
    if authority.state_machine_authorized || authority.identity_bootstrap {
        return Ok(());
    }
    for context in submit_work_item_contexts(&req.method) {
        if let Err(error) = validate_submit_context(&req.graph, context, verified_context) {
            return Err(Response::err(req.id, error));
        }
    }
    Ok(())
}
