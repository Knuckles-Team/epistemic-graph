use super::*;

impl SemanticIndexServerAdapter {
    pub(crate) fn new(service: Arc<SemanticIndexService>) -> Self {
        Self { service }
    }

    /// Validate the request DTO against the immutable transport carrier.
    /// `validate_at` checks DTO consistency and approvals; these comparisons
    /// bind it to the private verified authority and therefore close the
    /// request-body actor/tenant substitution path.
    pub(crate) fn authorize(
        &self,
        request: &SemanticIndexRequest,
        authority: &CarrierAuthority,
        now_ms: u64,
    ) -> Result<(), String> {
        Self::authorize_request(request, authority, now_ms)
    }

    /// Bind a public request to the verified carrier before opening any owner.
    pub(crate) fn authorize_request(
        request: &SemanticIndexRequest,
        authority: &CarrierAuthority,
        now_ms: u64,
    ) -> Result<(), String> {
        request
            .validate_at(now_ms)
            .map_err(|error| request_validation_refusal(&error).to_string())?;
        if request.tenant_id != authority.tenant_scope()
            || request.actor_scope != authority.actor_scope()
            || request.effective_actor_scope != authority.agent_id()
        {
            return Err(
                "ACCESS_DENIED: semantic request identity does not match verified carrier"
                    .to_string(),
            );
        }
        let writes = matches!(
            &request.command,
            SemanticIndexCommand::CreateBinding { .. }
                | SemanticIndexCommand::RefreshBinding { .. }
                | SemanticIndexCommand::DisableBinding { .. }
                | SemanticIndexCommand::DropBinding { .. }
        );
        if writes && !authority.can_write() {
            return Err("ACCESS_DENIED: semantic mutation requires kg:write".to_string());
        }
        if !writes && !authority.can_read() {
            return Err("ACCESS_DENIED: semantic read requires kg:read".to_string());
        }
        Ok(())
    }

    pub(crate) fn authorize_binding_worker(
        &self,
        binding: &SemanticBinding,
        authority: &CarrierAuthority,
    ) -> Result<(), String> {
        binding
            .validate()
            .map_err(|_| "INVALID_ARGUMENT: semantic binding rejected".to_string())?;
        if binding.tenant_id != authority.tenant_scope() || !authority.can_write() {
            return Err(
                "ACCESS_DENIED: semantic worker tenant or capability does not match verified carrier"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// The first callable server operation: create/admit a binding and return
    /// its durable receipt.  Refresh/disable/drop/search/status remain routed
    /// by the protocol integration slice and therefore cannot accidentally run
    /// through an unverified fallback here.
    pub(crate) fn admit_binding(
        &self,
        request: &SemanticIndexRequest,
        authority: &CarrierAuthority,
        now_ms: u64,
    ) -> Result<eg_core::compute::semantic_ann_codes::SemanticMutationReceipt, String> {
        self.authorize(request, authority, now_ms)?;
        let SemanticIndexCommand::CreateBinding { draft } = &request.command else {
            return Err(
                "INVALID_ARGUMENT: semantic operation is not binding admission".to_string(),
            );
        };
        let binding = SemanticBinding::create((**draft).clone())
            .map_err(|_| "INVALID_ARGUMENT: semantic binding rejected".to_string())?;
        request
            .validate_against_binding(&binding, now_ms)
            .map_err(|error| request_validation_refusal(&error).to_string())?;
        self.service
            .admit_binding_operation(
                &binding,
                now_ms,
                authority.agent_id(),
                authority.idempotency_key(),
                authority.attempt_nonce().ok_or_else(|| {
                    "ACCESS_DENIED: semantic mutation requires a verified attempt nonce".to_string()
                })?,
            )
            .map_err(|error| {
                crate::server::handlers::semantic_index::semantic_failure_message(&error)
                    .to_string()
            })
    }

    /// Subscribe one authenticated semantic worker to this tenant's durable
    /// stage stream. A worker may differ from the actor whose source ACL
    /// decision is embedded in the binding; its verified agent id is always
    /// the durable consumer identity. The core service owns the queue and
    /// subscription record.
    pub(crate) fn subscribe_stage_consumer(
        &self,
        binding: &SemanticBinding,
        authority: &CarrierAuthority,
    ) -> Result<(), String> {
        self.authorize_binding_worker(binding, authority)?;
        self.service
            .subscribe_stage_consumer(authority.agent_id())
            .map_err(|error| {
                crate::server::handlers::semantic_index::semantic_failure_message(&error)
                    .to_string()
            })
    }

    /// Claim a bounded page from the existing durable semantic outbox. The
    /// caller retains the core budget and lease types, so the server does not
    /// create another queue or replay authority.
    pub(crate) fn claim_stage_leases(
        &self,
        binding: &SemanticBinding,
        authority: &CarrierAuthority,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        self.authorize_binding_worker(binding, authority)?;
        self.service
            .claim_stage_leases(authority.agent_id(), budget)
            .map_err(|error| {
                crate::server::handlers::semantic_index::semantic_failure_message(&error)
                    .to_string()
            })
    }
}
