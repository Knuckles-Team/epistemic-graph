//! Attempt-specific authority context and its exact target scope.

use serde::{Deserialize, Serialize};

use crate::contract::{
    ActorId, AudienceId, BoundedVec, Digest256, IdempotencyKey, IngressSurface,
    Nonce, OpaqueId, Operation, PolicyRevision, ProtocolId, PurposeKind, ResourceId,
    ScopeKind, TenantId, UtcUnixNanos, MAX_SCOPE_COMPONENTS,
};

pub const AUTHORITY_CONTEXT_SCHEMA_V1: &str = "authority-context.v1";
pub const AUTHORITY_PROTOCOL_V1: &str = "au-eg.query.v1";

/// A materialized scope derived from verified authority. Parent scopes must be
/// sorted and unique so equal authority never has multiple byte spellings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuthorityScope {
    pub kind: ScopeKind,
    pub scope_id: ResourceId,
    pub tenant: Option<TenantId>,
    pub parent_scope_ids: BoundedVec<ResourceId, MAX_SCOPE_COMPONENTS>,
    pub graph_incarnation: Option<OpaqueId>,
}

impl AuthorityScope {
    pub fn validate(&self) -> Result<(), String> {
        if self
            .parent_scope_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err("authority parent scopes must be strictly sorted and unique".into());
        }
        match self.kind.as_str() {
            "tenant" => {
                let tenant = self
                    .tenant
                    .as_ref()
                    .ok_or_else(|| "tenant authority scope requires tenant binding".to_string())?;
                if self.scope_id.as_str() != tenant.as_str()
                    || !self.parent_scope_ids.is_empty()
                    || self.graph_incarnation.is_some()
                {
                    return Err("tenant authority scope must exactly target its tenant".into());
                }
                Ok(())
            }
            "incarnation" if self.graph_incarnation.is_none() => {
                Err("incarnation authority scope requires graph_incarnation".into())
            }
            kind if kind != "incarnation" && self.graph_incarnation.is_some() => {
                Err("only an incarnation authority scope may carry graph_incarnation".into())
            }
            _ => {
                let tenant = self
                    .tenant
                    .as_ref()
                    .ok_or_else(|| "authority scope requires tenant binding".to_string())?;
                if !self
                    .parent_scope_ids
                    .iter()
                    .any(|parent| parent.as_str() == tenant.as_str())
                {
                    return Err("authority scope parents must contain its exact tenant".into());
                }
                Ok(())
            }
        }
    }

    pub fn digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        let mut fields: Vec<&[u8]> = Vec::with_capacity(self.parent_scope_ids.len() + 4);
        fields.push(self.kind.as_str().as_bytes());
        fields.push(self.scope_id.as_str().as_bytes());
        fields.push(optional_tenant_bytes(self.tenant.as_ref()));
        fields.push(optional_opaque_bytes(self.graph_incarnation.as_ref()));
        for parent in &self.parent_scope_ids {
            fields.push(parent.as_str().as_bytes());
        }
        Digest256::framed(b"eg/authority-scope/v1", &fields)
    }
}

/// Complete attempt-specific authority context. Its digest intentionally binds
/// request/trace identity, timestamps, and nonce and therefore MUST NOT be used
/// as the stable operation replay identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AuthorityContext {
    pub schema_version: ResourceId,
    pub protocol_id: ProtocolId,
    pub catalog_digest: Digest256,
    pub request_id: OpaqueId,
    pub trace_id: OpaqueId,
    pub ingress_surface: IngressSurface,
    pub actor: ActorId,
    pub audience: AudienceId,
    pub tenant: TenantId,
    pub authority_scope: AuthorityScope,
    pub purpose_kind: PurposeKind,
    pub purpose_resource: Option<ResourceId>,
    pub operation: Operation,
    pub policy_revision: PolicyRevision,
    pub policy_epoch: u64,
    pub policy_decision_id: OpaqueId,
    pub policy_digest: Digest256,
    pub issued_at: UtcUnixNanos,
    pub expires_at: UtcUnixNanos,
    pub nonce: Nonce,
    pub idempotency_key: Option<IdempotencyKey>,
    pub context_digest: Digest256,
}

impl AuthorityContext {
    pub fn validate(&self) -> Result<(), String> {
        self.validate_header()?;
        self.authority_scope.validate()?;
        if self.authority_scope.tenant.as_ref() != Some(&self.tenant) {
            return Err("authority scope tenant differs from context tenant".into());
        }
        validate_operation_purpose(
            &self.operation,
            &self.purpose_kind,
            self.purpose_resource.as_ref(),
            &self.authority_scope,
        )?;
        if self.expires_at <= self.issued_at {
            return Err("authority context expiry must follow issuance".into());
        }
        if operation_requires_idempotency(&self.operation) && self.idempotency_key.is_none() {
            return Err("effectful authority context requires an idempotency key".into());
        }
        if self.context_digest != self.recompute_context_digest()? {
            return Err("authority context digest does not match its canonical fields".into());
        }
        Ok(())
    }

    pub fn recompute_context_digest(&self) -> Result<Digest256, String> {
        let scope_digest = self.authority_scope.digest()?;
        let purpose_digest = digest_purpose(
            &self.operation,
            &self.purpose_kind,
            self.purpose_resource.as_ref(),
            &self.authority_scope,
        )?;
        let epoch = self.policy_epoch.to_be_bytes();
        let issued_at = self.issued_at.get().to_be_bytes();
        let expires_at = self.expires_at.get().to_be_bytes();
        Digest256::framed(
            b"eg/authority-context/v1",
            &[
                self.schema_version.as_str().as_bytes(),
                self.protocol_id.as_str().as_bytes(),
                self.catalog_digest.as_bytes(),
                self.request_id.as_str().as_bytes(),
                self.trace_id.as_str().as_bytes(),
                self.ingress_surface.as_str().as_bytes(),
                self.actor.as_str().as_bytes(),
                self.audience.as_str().as_bytes(),
                self.tenant.as_str().as_bytes(),
                scope_digest.as_bytes(),
                purpose_digest.as_bytes(),
                self.operation.as_str().as_bytes(),
                self.policy_revision.as_str().as_bytes(),
                &epoch,
                self.policy_decision_id.as_str().as_bytes(),
                self.policy_digest.as_bytes(),
                &issued_at,
                &expires_at,
                self.nonce.as_bytes(),
                optional_idempotency_bytes(self.idempotency_key.as_ref()),
            ],
        )
    }

    fn validate_header(&self) -> Result<(), String> {
        if self.schema_version.as_str() != AUTHORITY_CONTEXT_SCHEMA_V1 {
            return Err("authority context schema must be authority-context.v1".into());
        }
        if self.protocol_id.as_str() != AUTHORITY_PROTOCOL_V1 {
            return Err("authority context protocol must be au-eg.query.v1".into());
        }
        Ok(())
    }
}

fn operation_requires_idempotency(operation: &Operation) -> bool {
    matches!(
        operation.as_str(),
        "mutation" | "extract" | "ingest" | "backfeed" | "configure" | "audit"
    )
}

pub(super) fn validate_operation_purpose(
    operation: &Operation,
    kind: &PurposeKind,
    resource: Option<&ResourceId>,
    scope: &AuthorityScope,
) -> Result<(), String> {
    let expected_purpose = match operation.as_str() {
        "query" => "graph_read",
        "mutation" => "graph_write",
        "extract" => "source_extract",
        "ingest" => "source_ingest",
        "backfeed" => "source_backfeed",
        "configure" => "config_admin",
        "audit" => "audit",
        _ => return Err("operation has no purpose mapping".into()),
    };
    if kind.as_str() != expected_purpose {
        return Err("authority operation and purpose do not match".into());
    }
    let resource =
        resource.ok_or_else(|| "authority purpose requires a target resource".to_string())?;
    if resource != &scope.scope_id {
        return Err("authority purpose resource differs from scope target".into());
    }
    Ok(())
}

pub(super) fn digest_purpose(
    operation: &Operation,
    kind: &PurposeKind,
    resource: Option<&ResourceId>,
    scope: &AuthorityScope,
) -> Result<Digest256, String> {
    validate_operation_purpose(operation, kind, resource, scope)?;
    Digest256::framed(
        b"eg/authority-purpose/v1",
        &[
            operation.as_str().as_bytes(),
            kind.as_str().as_bytes(),
            resource.map_or(b"".as_slice(), |value| value.as_str().as_bytes()),
        ],
    )
}

fn optional_tenant_bytes(value: Option<&TenantId>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_str().as_bytes())
}

fn optional_opaque_bytes(value: Option<&OpaqueId>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_str().as_bytes())
}

fn optional_idempotency_bytes(value: Option<&IdempotencyKey>) -> &[u8] {
    value.map_or(b"".as_slice(), |item| item.as_str().as_bytes())
}
