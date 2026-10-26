//! Durable approval of one held connector-pack source registration.
//!
//! The source declaration is already owned by the current member row. This
//! module only flips its admission state under the existing Agent Library
//! mutation kernel after an exact current-head and body-digest CAS.

use eg_types::agent_library::AgentLibraryMutationContext;
use eg_types::connector_pack::{
    ConnectorPackSourceApprovalResult, PackHeadRef, PackSourceApproval,
    CONNECTOR_PACK_SCHEMA_VERSION,
};
use eg_types::contract::{Digest256, ResourceId};
use eg_types::mutation_batch::{DurabilityDomain, MutationOperation, MutationSurface};
use eg_types::protocol::Method;
use redb::ReadableTable;

use super::admitted::{PackOwnerWrite, PackWriteEffects, PackWriteIdentity, PackWriteStaged};
use super::{ConnectorPackHeadRow, ConnectorPackMemberRow};
use crate::server::persistence::agent_library::AgentLibraryStore;

pub(crate) const CONNECTOR_PACK_SOURCE_APPROVAL_RESULT_SCHEMA_ID: &str =
    "connector-pack-source-approval-result.v1";

impl AgentLibraryStore {
    /// Approve one current held source registration after the caller's
    /// admitted policy decision. The member row is the only source of truth;
    /// no declaration or approval bit is accepted from the request.
    pub(crate) fn approve_connector_pack_source(
        &self,
        context: AgentLibraryMutationContext,
        connector: &ResourceId,
        contract_iri: &str,
        resource_uri: &str,
        expected_head: &PackHeadRef,
        contract_digest: Digest256,
    ) -> Result<ConnectorPackSourceApprovalResult, String> {
        eg_types::agent_library::validate_key(&context.tenant_id, connector.as_str())?;
        eg_types::connector_pack::validate_connector(connector)?;
        if contract_iri.is_empty() || resource_uri.is_empty() {
            return Err("SOURCE_REGISTRATION_NOT_FOUND: source selector is empty".to_string());
        }
        let discriminator = approval_discriminator(
            connector,
            contract_iri,
            resource_uri,
            expected_head,
            contract_digest,
        )?;
        let identity = PackWriteIdentity {
            purpose: "connector-pack:source-approval",
            kind: "connector-pack-source-approval",
            slug: "connector-pack-source-approval",
            subject: connector.as_str(),
            revision: expected_head.binding_revision,
            discriminator: Some(&discriminator),
            result_schema: CONNECTOR_PACK_SOURCE_APPROVAL_RESULT_SCHEMA_ID,
            noun: "connector pack source approval",
        };
        let effects = PackWriteEffects {
            operations: approval_operations(connector, &discriminator),
            outbox: Vec::new(),
        };
        self.commit_pack_write(&context, identity, effects, |write, staged| {
            apply_source_approval(
                write,
                &context,
                connector,
                contract_iri,
                resource_uri,
                expected_head,
                contract_digest,
                staged,
            )
        })
    }

    /// Re-read the durable approval that a trusted host binding is about to use.
    ///
    /// The result receipt is a reference, not authority: the current head,
    /// published member, body digest, and exact registration are all checked
    /// again from the Agent Library snapshot.  This keeps a stale or modified
    /// receipt from turning the request-scoped registry seam into an approval
    /// bypass.
    pub(crate) fn verify_connector_pack_source_approval(
        &self,
        receipt: &ConnectorPackSourceApprovalResult,
    ) -> Result<(), String> {
        if receipt.schema_version != CONNECTOR_PACK_SCHEMA_VERSION {
            return Err(format!(
                "SOURCE_APPROVAL_SCHEMA_MISMATCH: expected {}, got {}",
                CONNECTOR_PACK_SCHEMA_VERSION, receipt.schema_version
            ));
        }
        eg_types::agent_library::validate_key(&receipt.tenant_id, receipt.connector.as_str())?;
        eg_types::connector_pack::validate_connector(&receipt.connector)?;
        if receipt.registration.approval != PackSourceApproval::Approved {
            return Err("SOURCE_APPROVAL_NOT_ADMITTED: registration is not approved".to_string());
        }
        if receipt.registration.connector != receipt.connector {
            return Err("SOURCE_APPROVAL_MISMATCH: registration connector differs".to_string());
        }

        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::CONNECTOR_PACK_HEADS)?;
        let head_row = heads
            .get((receipt.tenant_id.as_str(), receipt.connector.as_str()))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "PACK_HEAD_CONFLICT: connector has no current pack head".to_string())?;
        let head: ConnectorPackHeadRow =
            crate::server::persistence::agent_row::decode(head_row.value(), "connector pack head")?;
        let current_ref = PackHeadRef {
            binding_revision: head.head.binding_revision,
            pack_digest: head.head.pack_digest,
        };
        if current_ref != receipt.head {
            return Err("PACK_HEAD_CONFLICT: source approval receipt is stale".to_string());
        }

        let members = read.open_owner_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
        let mut current_member = None;
        for row in members
            .range((receipt.tenant_id.as_str(), receipt.connector.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (tenant, connector, uri) = key.value();
            if tenant != receipt.tenant_id || connector != receipt.connector.as_str() {
                break;
            }
            if uri != receipt.registration.resource_uri {
                continue;
            }
            current_member = Some(crate::server::persistence::agent_row::decode::<
                ConnectorPackMemberRow,
            >(value.value(), "connector pack member")?);
            break;
        }
        let member = current_member.ok_or_else(|| {
            format!(
                "SOURCE_REGISTRATION_NOT_FOUND: resource {} is not current",
                receipt.registration.resource_uri
            )
        })?;
        if member.last_record_id != head.head.record_id
            || member.lifecycle != eg_types::agent_library::AgentLibraryLifecycle::Published
        {
            return Err(
                "SOURCE_REGISTRATION_NOT_FOUND: source member is not on the current published head"
                    .to_string(),
            );
        }
        if member.body_sha256 != receipt.contract_digest {
            return Err(format!(
                "SOURCE_CONTRACT_MISMATCH: current body digest is {}, receipt names {}",
                member.body_sha256, receipt.contract_digest
            ));
        }
        if !member
            .source_registrations
            .iter()
            .any(|registration| registration == &receipt.registration)
        {
            return Err(
                "SOURCE_REGISTRATION_NOT_FOUND: approved registration is not current".to_string(),
            );
        }
        Ok(())
    }
}

fn approval_discriminator(
    connector: &ResourceId,
    contract_iri: &str,
    resource_uri: &str,
    expected_head: &PackHeadRef,
    contract_digest: Digest256,
) -> Result<String, String> {
    let revision = expected_head.binding_revision.to_be_bytes();
    Digest256::framed(
        b"eg/connector-pack-source-approval/v1",
        &[
            connector.as_str().as_bytes(),
            contract_iri.as_bytes(),
            resource_uri.as_bytes(),
            &revision,
            expected_head.pack_digest.as_bytes(),
            contract_digest.as_bytes(),
        ],
    )
    .map(Digest256::to_hex)
}

fn approval_operations(connector: &ResourceId, discriminator: &str) -> Vec<MutationOperation> {
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: "connector_pack_source_approval".to_string(),
            query: format!("{}:{discriminator}", connector.as_str()),
        },
    }]
}

fn apply_source_approval(
    write: &PackOwnerWrite<'_>,
    context: &AgentLibraryMutationContext,
    connector: &ResourceId,
    contract_iri: &str,
    resource_uri: &str,
    expected_head: &PackHeadRef,
    contract_digest: Digest256,
    staged: &PackWriteStaged,
) -> Result<ConnectorPackSourceApprovalResult, String> {
    let tenant = context.tenant_id.as_str();
    let head = current_head(write, tenant, connector)?;
    let current_ref = PackHeadRef {
        binding_revision: head.head.binding_revision,
        pack_digest: head.head.pack_digest,
    };
    if current_ref != *expected_head {
        return Err(format!(
            "PACK_HEAD_CONFLICT: expected binding revision {} / digest {}, current is {} / {}",
            expected_head.binding_revision,
            expected_head.pack_digest,
            current_ref.binding_revision,
            current_ref.pack_digest
        ));
    }

    let mut members = write.open_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
    let mut member = current_member(&mut members, tenant, connector, resource_uri, &head)?;
    if member.body_sha256 != contract_digest {
        return Err(format!(
            "SOURCE_CONTRACT_MISMATCH: current body digest is {}, request names {}",
            member.body_sha256, contract_digest
        ));
    }
    let registration_index = member
        .source_registrations
        .iter()
        .position(|registration| {
            registration.connector == *connector
                && registration.contract_iri == contract_iri
                && registration.resource_uri == resource_uri
        })
        .ok_or_else(|| {
            format!(
                "SOURCE_REGISTRATION_NOT_FOUND: {contract_iri} on {resource_uri} is not current"
            )
        })?;
    let mut registration = member.source_registrations[registration_index].clone();
    if registration.approval == PackSourceApproval::Held {
        registration.approval = PackSourceApproval::Approved;
        member.source_registrations[registration_index] = registration.clone();
        let bytes = eg_storage::encode_bounded(&member, "connector pack member")?;
        members
            .insert(
                (tenant, connector.as_str(), member.uri.as_str()),
                bytes.as_slice(),
            )
            .map_err(|error| error.to_string())?;
    }

    Ok(ConnectorPackSourceApprovalResult {
        schema_version: CONNECTOR_PACK_SCHEMA_VERSION,
        tenant_id: tenant.to_string(),
        connector: connector.clone(),
        head: current_ref,
        registration,
        contract_digest,
        batch_id: staged.batch_id.clone(),
        committed_version: staged.committed_version,
    })
}

fn current_head(
    write: &PackOwnerWrite<'_>,
    tenant: &str,
    connector: &ResourceId,
) -> Result<ConnectorPackHeadRow, String> {
    let heads = write.open_table(eg_storage::CONNECTOR_PACK_HEADS)?;
    let row = heads
        .get((tenant, connector.as_str()))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "PACK_HEAD_CONFLICT: connector has no current pack head".to_string())?;
    crate::server::persistence::agent_row::decode(row.value(), "connector pack head")
}

fn current_member(
    members: &mut redb::Table<'_, (&str, &str, &str), &[u8]>,
    tenant: &str,
    connector: &ResourceId,
    resource_uri: &str,
    head: &ConnectorPackHeadRow,
) -> Result<ConnectorPackMemberRow, String> {
    for row in members
        .range((tenant, connector.as_str(), "")..)
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_tenant, row_connector, row_uri) = key.value();
        if row_tenant != tenant || row_connector != connector.as_str() {
            break;
        }
        if row_uri != resource_uri {
            continue;
        }
        let member: ConnectorPackMemberRow =
            crate::server::persistence::agent_row::decode(value.value(), "connector pack member")?;
        if member.last_record_id != head.head.record_id
            || member.lifecycle != eg_types::agent_library::AgentLibraryLifecycle::Published
        {
            return Err(
                "SOURCE_REGISTRATION_NOT_FOUND: source member is not on the current published head"
                    .to_string(),
            );
        }
        return Ok(member);
    }
    Err(format!(
        "SOURCE_REGISTRATION_NOT_FOUND: resource {resource_uri} is not current"
    ))
}
