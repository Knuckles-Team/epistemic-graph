//! Durable ConnectorPack schema-projection preparation and completion.
//!
//! Preparation resolves the exact current head, retained import record, and
//! engine-owned schema body holders from one Agent Library snapshot. Completion
//! changes the retained record, receipt, and visibility pointer in one admitted
//! owner transaction guarded by an exact-head CAS.

use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::connector_pack::{
    McpCatalogSnapshotBinding, PackHeadRef, PackImportReceipt, PackImportRecord, PackImportResult,
    PackProjectionState, CONNECTOR_PACK_RESULT_SCHEMA_ID,
};
use eg_types::contract::{Digest256, ResourceId};
use eg_types::mutation::MutationResult;
use eg_types::mutation_batch::{DurabilityDomain, MutationOperation, MutationSurface};
use eg_types::protocol::Method;
use redb::ReadableTable;

use super::{ConnectorPackBodyHolderRow, ConnectorPackHeadRow, ConnectorPackMemberRow};
use crate::server::persistence::agent_library::{
    admitted_context, agent_library_operation_identity, owner_batch_receipt, resolve_nonce_first,
    validate_context, AgentLibraryStore,
};
use crate::server::persistence::agent_revision::{
    apply_owner_rows, finish_committed_ledger, finish_replayed, policy_admitted_context,
    recorded_receipt, stage_batch, within_write,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackProjectionBody {
    pub(crate) uri: String,
    pub(crate) kind: eg_types::connector_pack::PackEntryKind,
    pub(crate) body_sha256: Digest256,
    pub(crate) engine_manifest_digest: String,
    pub(crate) length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectorPackProjectionPlan {
    pub(crate) tenant_id: String,
    pub(crate) connector: ResourceId,
    pub(crate) head: PackHeadRef,
    pub(crate) catalog: McpCatalogSnapshotBinding,
    pub(crate) record_id: String,
    pub(crate) bodies: Vec<PackProjectionBody>,
}

impl AgentLibraryStore {
    pub(crate) fn prepare_connector_pack_projection(
        &self,
        tenant_id: &str,
        connector: &ResourceId,
    ) -> Result<ConnectorPackProjectionPlan, String> {
        eg_types::agent_library::validate_key(tenant_id, connector.as_str())?;
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::CONNECTOR_PACK_HEADS)?;
        let imports = read.open_owner_table(eg_storage::CONNECTOR_PACK_IMPORTS)?;
        let members = read.open_owner_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
        let holders = read.open_owner_table(eg_storage::CONNECTOR_PACK_BODY_HOLDERS)?;
        let head = heads
            .get((tenant_id, connector.as_str()))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "PACK_PROJECTION_NO_HEAD: connector has no committed pack".to_string()
            })?;
        let head: ConnectorPackHeadRow =
            crate::server::persistence::agent_row::decode(head.value(), "connector pack head")?;
        let record = imports
            .get((tenant_id, connector.as_str(), head.head.binding_revision))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "CORRUPT_CONNECTOR_PACK: head has no retained import record".to_string()
            })?;
        let record: PackImportRecord = crate::server::persistence::agent_row::decode(
            record.value(),
            "connector pack import record",
        )?;
        validate_head_record(tenant_id, connector, &head, &record)?;

        let mut bodies = Vec::new();
        for row in members
            .range((tenant_id, connector.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_tenant, row_connector, _) = key.value();
            if row_tenant != tenant_id || row_connector != connector.as_str() {
                break;
            }
            let member: ConnectorPackMemberRow = crate::server::persistence::agent_row::decode(
                value.value(),
                "connector pack member",
            )?;
            if !matches!(
                member.kind,
                eg_types::connector_pack::PackEntryKind::Ontology
                    | eg_types::connector_pack::PackEntryKind::Shapes
            ) || member.lifecycle != AgentLibraryLifecycle::Published
            {
                continue;
            }
            if member.last_record_id != record.record_id {
                return Err(
                    "CORRUPT_CONNECTOR_PACK: schema member is not from the current head".into(),
                );
            }
            let body_hex = member.body_sha256.to_hex();
            let holder = holders
                .get((
                    tenant_id,
                    body_hex.as_str(),
                    member.component_id.as_str(),
                    member.entry_revision,
                ))
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!(
                        "BODY_MISSING: schema member '{}' has no engine body holder",
                        member.uri
                    )
                })?;
            let holder: ConnectorPackBodyHolderRow = crate::server::persistence::agent_row::decode(
                holder.value(),
                "connector pack body holder",
            )?;
            if holder.engine_manifest_digest != member.engine_manifest_digest
                || holder.length != member.body_length
            {
                return Err(
                    "BODY_MISSING: schema member body holder differs from its catalog row".into(),
                );
            }
            bodies.push(PackProjectionBody {
                uri: member.uri,
                kind: member.kind,
                body_sha256: member.body_sha256,
                engine_manifest_digest: holder.engine_manifest_digest,
                length: holder.length,
            });
        }
        bodies.sort_by(|left, right| left.uri.cmp(&right.uri));
        Ok(ConnectorPackProjectionPlan {
            tenant_id: tenant_id.to_string(),
            connector: connector.clone(),
            head: PackHeadRef {
                binding_revision: head.head.binding_revision,
                pack_digest: head.head.pack_digest,
            },
            catalog: head.head.catalog,
            record_id: head.head.record_id,
            bodies,
        })
    }

    pub(crate) fn commit_connector_pack_projection(
        &self,
        context: AgentLibraryMutationContext,
        plan: &ConnectorPackProjectionPlan,
        projection: PackProjectionState,
    ) -> Result<PackImportReceipt, String> {
        validate_context(self, &context)?;
        validate_projection_identity(&context, plan, &projection)?;
        let owner = self.scope_handle(&context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let replay_context = admitted_context(&context, "connector-pack:reproject")?;
        let nonce = resolve_nonce_first(&self.mutations, &txn, &replay_context)?;
        let operation = agent_library_operation_identity(
            &owner,
            &replay_context,
            "connector-pack-reproject",
            plan.connector.as_str(),
            plan.head.binding_revision,
            Some(&plan.record_id),
        )?;
        let replay = self.mutations.resolve_replay(&txn, &operation, &nonce)?;
        if let Some(receipt) = recorded_receipt(replay, "connector pack projection")? {
            let result = decode_replayed_receipt(&receipt)?;
            return finish_replayed(&self.mutations, txn, &operation, &nonce, result, receipt);
        }
        let (txn, committed) = within_write(txn, |txn| {
            let operations = projection_operations(plan, &projection);
            let batch_context = policy_admitted_context(&replay_context, &operations)?;
            let (_, staged) = stage_batch(
                self,
                txn,
                &owner,
                &batch_context,
                (&operation, &nonce),
                "connector pack projection",
                |version, batch_id| {
                    crate::server::persistence::agent_library::native_lifecycle_batch(
                        &owner,
                        &batch_context,
                        batch_id,
                        version,
                        operations,
                        Vec::new(),
                    )
                },
            )?;
            let mut updated = None;
            apply_owner_rows(txn, &owner, &staged.batch, |write| {
                updated = Some(apply_projection_rows(write, plan, projection.clone())?);
                Ok(())
            })?;
            let updated = updated.ok_or_else(|| {
                "connector pack projection owner rows produced no receipt".to_string()
            })?;
            let result = PackImportResult::Imported {
                receipt: Box::new(updated.clone()),
            };
            let mutation_result = crate::server::persistence::agent_row::domain_result(
                &result,
                CONNECTOR_PACK_RESULT_SCHEMA_ID,
                "connector pack projection",
            )?;
            let result_bytes =
                eg_storage::encode_bounded(&mutation_result, "connector pack projection result")?;
            let authority_receipt = owner_batch_receipt(
                &operation,
                &nonce,
                &staged.batch,
                "connector-pack-projection",
                mutation_result,
                staged.committed_version,
                replay_context.created_at_ms,
            )?;
            finish_committed_ledger(
                &self.mutations,
                txn,
                (&staged, &result_bytes),
                replay_context.created_at_ms,
                (&operation, &nonce, &authority_receipt),
                "connector pack projection",
            )?;
            Ok((staged.batch, updated))
        })?;
        self.mutations.commit(txn, &committed.0)?;
        Ok(committed.1)
    }
}

fn validate_head_record(
    tenant_id: &str,
    connector: &ResourceId,
    head: &ConnectorPackHeadRow,
    record: &PackImportRecord,
) -> Result<(), String> {
    if !record_matches_head(tenant_id, connector, head, record)
        || !receipt_matches_record(tenant_id, connector, head, record)
    {
        return Err("CORRUPT_CONNECTOR_PACK: head, record and receipt disagree".into());
    }
    Ok(())
}

fn record_matches_head(
    tenant_id: &str,
    connector: &ResourceId,
    head: &ConnectorPackHeadRow,
    record: &PackImportRecord,
) -> bool {
    record.tenant_id == tenant_id
        && record.connector == *connector
        && record.binding_revision == head.head.binding_revision
        && record.pack_digest == head.head.pack_digest
        && record.catalog == head.head.catalog
        && record.record_id == head.head.record_id
}

fn receipt_matches_record(
    tenant_id: &str,
    connector: &ResourceId,
    head: &ConnectorPackHeadRow,
    record: &PackImportRecord,
) -> bool {
    head.receipt.tenant_id == tenant_id
        && head.receipt.connector == *connector
        && head.receipt.binding_revision == record.binding_revision
        && head.receipt.pack_digest == record.pack_digest
        && head.receipt.catalog == record.catalog
        && head.receipt.record_id == record.record_id
}

fn validate_projection_identity(
    context: &AgentLibraryMutationContext,
    plan: &ConnectorPackProjectionPlan,
    projection: &PackProjectionState,
) -> Result<(), String> {
    if context.tenant_id != plan.tenant_id {
        return Err("connector pack projection tenant differs from its plan".into());
    }
    match projection {
        PackProjectionState::Applied {
            graph,
            graph_version,
        } if reserved_projection_graph(graph) && *graph_version > 0 => Ok(()),
        PackProjectionState::Failed { code } if valid_projection_code(code) => Ok(()),
        PackProjectionState::Applied { .. } => {
            Err("connector pack projection graph identity is invalid".into())
        }
        PackProjectionState::Failed { .. } => {
            Err("connector pack projection failure code is invalid".into())
        }
        PackProjectionState::None | PackProjectionState::Pending => {
            Err("connector pack projection completion must be terminal".into())
        }
    }
}

fn valid_projection_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 256
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn reserved_projection_graph(graph: &str) -> bool {
    graph.strip_prefix("pack__").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn projection_operations(
    plan: &ConnectorPackProjectionPlan,
    projection: &PackProjectionState,
) -> Vec<MutationOperation> {
    let state = match projection {
        PackProjectionState::Applied { .. } => "applied",
        PackProjectionState::Failed { .. } => "failed",
        PackProjectionState::None => "none",
        PackProjectionState::Pending => "pending",
    };
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: format!("connector_pack_projection_{state}"),
            query: format!("{}:{}", plan.record_id, plan.head.pack_digest),
        },
    }]
}

fn apply_projection_rows(
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackProjectionPlan,
    projection: PackProjectionState,
) -> Result<PackImportReceipt, String> {
    let tenant = plan.tenant_id.as_str();
    let connector = plan.connector.as_str();
    let mut heads = write.open_table(eg_storage::CONNECTOR_PACK_HEADS)?;
    let current = heads
        .get((tenant, connector))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "PACK_PLAN_STALE: connector pack head disappeared".to_string())?;
    let mut head: ConnectorPackHeadRow =
        crate::server::persistence::agent_row::decode(current.value(), "connector pack head")?;
    drop(current);
    if head.head.binding_revision != plan.head.binding_revision
        || head.head.pack_digest != plan.head.pack_digest
        || head.head.catalog != plan.catalog
        || head.head.record_id != plan.record_id
    {
        return Err("PACK_PLAN_STALE: connector pack head changed".into());
    }
    let mut imports = write.open_table(eg_storage::CONNECTOR_PACK_IMPORTS)?;
    let current = imports
        .get((tenant, connector, plan.head.binding_revision))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "PACK_PLAN_STALE: connector pack record disappeared".to_string())?;
    let mut record: PackImportRecord = crate::server::persistence::agent_row::decode(
        current.value(),
        "connector pack import record",
    )?;
    drop(current);
    validate_head_record(tenant, &plan.connector, &head, &record)?;
    record.projection = projection.clone();
    head.receipt.projection = projection.clone();
    if matches!(projection, PackProjectionState::Applied { .. }) {
        head.head.visible_record_id = Some(plan.record_id.clone());
    }
    let record_bytes = eg_storage::encode_bounded(&record, "connector pack import record")?;
    imports
        .insert(
            (tenant, connector, plan.head.binding_revision),
            record_bytes.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    let head_bytes = eg_storage::encode_bounded(&head, "connector pack head")?;
    heads
        .insert((tenant, connector), head_bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(head.receipt)
}

fn decode_replayed_receipt(
    receipt: &eg_types::mutation::MutationReceipt,
) -> Result<PackImportReceipt, String> {
    receipt.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = &receipt.result
    else {
        return Err("CORRUPT_MUTATION_LEDGER: projection replay has no result".into());
    };
    if schema_id.as_str() != CONNECTOR_PACK_RESULT_SCHEMA_ID {
        return Err("CORRUPT_MUTATION_LEDGER: projection schema differs".into());
    }
    match crate::server::persistence::agent_row::decode::<PackImportResult>(
        payload.as_slice(),
        "connector pack projection result",
    )? {
        PackImportResult::Imported { receipt } => Ok(*receipt),
        _ => Err("CORRUPT_MUTATION_LEDGER: projection replay is not imported".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
    use eg_types::connector_pack::{
        PackArchiveFacts, PackProducer, PackServerRecord, PACK_IMPORT_RECORD_SCHEMA_VERSION,
    };
    use eg_types::contract::BoundedVec;

    use crate::server::persistence::connector_pack::commit::ConnectorPackCommitPlan;

    fn commit_pack(
        store: &AgentLibraryStore,
        tenant_id: &str,
        connector: &str,
        revision: u64,
        expected_head: Option<PackHeadRef>,
        nonce: u8,
    ) -> PackImportReceipt {
        let digest = |byte| Digest256::from_bytes([byte; 32]);
        let pack_digest = digest(revision as u8);
        let record_id = format!("pack-record-{connector}-{revision}");
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            store,
            tenant_id,
            &format!(
                "connector-pack:{connector}:import:{pack_digest}:{}",
                revision - 1
            ),
            nonce,
            0,
            "connector-pack:import",
        );
        let record = PackImportRecord {
            schema_version: PACK_IMPORT_RECORD_SCHEMA_VERSION,
            tenant_id: tenant_id.to_string(),
            connector: ResourceId::new(connector).unwrap(),
            binding_revision: revision,
            record_id,
            pack_digest,
            catalog: McpCatalogSnapshotBinding {
                configuration_revision: revision,
                catalog_generation: revision,
                snapshot_digest: digest(20 + revision as u8),
                child_connection_generation: revision,
                authorization_scope_digest: digest(40 + revision as u8),
            },
            previous_pack_digest: expected_head.as_ref().map(|head| head.pack_digest),
            server: PackServerRecord {
                name: connector.to_string(),
                contract_version: Some("1".to_string()),
                package_version: revision.to_string(),
                component: ComponentDependency {
                    component_id: format!("pack:{connector}:mcp_server:{connector}"),
                    kind: AgentComponentKind::McpServer,
                    definition_digest: format!("sha256:{}", digest(60 + revision as u8)),
                },
            },
            producer: PackProducer {
                name: "test".to_string(),
                version: "1".to_string(),
            },
            archive: PackArchiveFacts {
                length: 0,
                sha256: digest(80 + revision as u8),
            },
            importer: context.caller_principal.clone(),
            committed_at_ms: context.created_at_ms,
            entries: BoundedVec::new(Vec::new()).unwrap(),
            warnings: BoundedVec::new(Vec::new()).unwrap(),
            projection: PackProjectionState::Pending,
        };
        match store
            .commit_connector_pack(ConnectorPackCommitPlan {
                context,
                expected_head,
                record,
                components: Vec::new(),
                members: Vec::new(),
                holders: Vec::new(),
            })
            .unwrap()
        {
            PackImportResult::Imported { receipt } => *receipt,
            other => panic!("unexpected import result: {other:?}"),
        }
    }

    fn ledger_shape(store: &AgentLibraryStore, tenant_id: &str) -> (u64, usize, usize) {
        let owner = store.scope_handle(tenant_id).unwrap();
        let read = store.kernel.read_scope(&owner).unwrap();
        let batches = eg_transaction::read_batches(&read).unwrap();
        let outbox = batches
            .iter()
            .map(|record| {
                eg_transaction::read_outbox(&read, &record.batch.batch_id)
                    .unwrap()
                    .len()
            })
            .sum();
        (
            eg_transaction::version(&read).unwrap(),
            batches.len(),
            outbox,
        )
    }

    fn plan() -> ConnectorPackProjectionPlan {
        ConnectorPackProjectionPlan {
            tenant_id: "tenant-a".into(),
            connector: ResourceId::new("demo").unwrap(),
            head: PackHeadRef {
                binding_revision: 7,
                pack_digest: Digest256::from_bytes([1; 32]),
            },
            catalog: McpCatalogSnapshotBinding {
                configuration_revision: 1,
                catalog_generation: 2,
                snapshot_digest: Digest256::from_bytes([2; 32]),
                child_connection_generation: 3,
                authorization_scope_digest: Digest256::from_bytes([3; 32]),
            },
            record_id: "pack-record-demo-7".into(),
            bodies: Vec::new(),
        }
    }

    #[test]
    fn projection_completion_requires_typed_terminal_identity() {
        let plan = plan();
        let (_dir, store) = crate::server::persistence::agent_fixtures::open_agent_store();
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            &store,
            "tenant-a",
            "projection-test",
            1,
            0,
            "connector-pack:reproject",
        );
        assert!(validate_projection_identity(
            &context,
            &plan,
            &PackProjectionState::Applied {
                graph: format!("pack__{}", "a".repeat(64)),
                graph_version: 1,
            }
        )
        .is_ok());
        assert!(
            validate_projection_identity(&context, &plan, &PackProjectionState::Pending).is_err()
        );
        assert!(validate_projection_identity(
            &context,
            &plan,
            &PackProjectionState::Failed {
                code: "not a code".into(),
            }
        )
        .is_err());
    }

    #[test]
    fn projection_prepare_fails_closed_without_a_head() {
        let (_dir, store) = crate::server::persistence::agent_fixtures::open_agent_store();
        let error = store
            .prepare_connector_pack_projection("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap_err();
        assert!(error.starts_with("PACK_PROJECTION_NO_HEAD:"), "{error}");
    }

    #[test]
    fn applied_projection_replays_after_restart_without_duplicate_batch_or_outbox() {
        let dir = tempfile::tempdir().unwrap();
        let (receipt, committed_ledger) = {
            let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
            commit_pack(&store, "tenant-a", "demo", 1, None, 1);
            let plan = store
                .prepare_connector_pack_projection("tenant-a", &ResourceId::new("demo").unwrap())
                .unwrap();
            let context = crate::server::persistence::agent_fixtures::mutation_context(
                &store,
                "tenant-a",
                "reproject-attempt-1",
                2,
                0,
                "connector-pack:reproject",
            );
            let receipt = store
                .commit_connector_pack_projection(
                    context,
                    &plan,
                    PackProjectionState::Applied {
                        graph: format!("pack__{}", "a".repeat(64)),
                        graph_version: 11,
                    },
                )
                .unwrap();
            (receipt, ledger_shape(&store, "tenant-a"))
        };
        let reopened = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let plan = reopened
            .prepare_connector_pack_projection("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            &reopened,
            "tenant-a",
            "reproject-attempt-1",
            3,
            0,
            "connector-pack:reproject",
        );
        let replayed = reopened
            .commit_connector_pack_projection(
                context,
                &plan,
                PackProjectionState::Applied {
                    graph: format!("pack__{}", "a".repeat(64)),
                    graph_version: 11,
                },
            )
            .unwrap();
        assert_eq!(replayed, receipt);
        assert_eq!(ledger_shape(&reopened, "tenant-a"), committed_ledger);
    }

    #[test]
    fn stale_projection_plan_cannot_advance_a_newer_head() {
        let (_dir, store) = crate::server::persistence::agent_fixtures::open_agent_store();
        let first = commit_pack(&store, "tenant-a", "demo", 1, None, 1);
        let stale = store
            .prepare_connector_pack_projection("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        commit_pack(
            &store,
            "tenant-a",
            "demo",
            2,
            Some(PackHeadRef {
                binding_revision: first.binding_revision,
                pack_digest: first.pack_digest,
            }),
            2,
        );
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            &store,
            "tenant-a",
            "stale-reproject",
            3,
            0,
            "connector-pack:reproject",
        );
        let error = store
            .commit_connector_pack_projection(
                context,
                &stale,
                PackProjectionState::Applied {
                    graph: format!("pack__{}", "a".repeat(64)),
                    graph_version: 9,
                },
            )
            .unwrap_err();
        assert!(error.starts_with("PACK_PLAN_STALE:"), "{error}");
        let status = store
            .connector_pack_status("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        assert_eq!(status.head.unwrap().binding_revision, 2);
    }

    #[test]
    fn newer_pending_and_failed_head_preserves_prior_visible_record() {
        let (_dir, store) = crate::server::persistence::agent_fixtures::open_agent_store();
        let first = commit_pack(&store, "tenant-a", "demo", 1, None, 1);
        let first_plan = store
            .prepare_connector_pack_projection("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            &store,
            "tenant-a",
            "first-visible",
            2,
            0,
            "connector-pack:reproject",
        );
        store
            .commit_connector_pack_projection(
                context,
                &first_plan,
                PackProjectionState::Applied {
                    graph: format!("pack__{}", "a".repeat(64)),
                    graph_version: 1,
                },
            )
            .unwrap();
        commit_pack(
            &store,
            "tenant-a",
            "demo",
            2,
            Some(PackHeadRef {
                binding_revision: first.binding_revision,
                pack_digest: first.pack_digest,
            }),
            3,
        );
        let second_plan = store
            .prepare_connector_pack_projection("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        let pending = store
            .connector_pack_status("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        assert_eq!(
            pending.head.unwrap().visible_record_id.as_deref(),
            Some(first_plan.record_id.as_str())
        );
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            &store,
            "tenant-a",
            "second-failed",
            4,
            0,
            "connector-pack:reproject",
        );
        store
            .commit_connector_pack_projection(
                context,
                &second_plan,
                PackProjectionState::Failed {
                    code: "BODY_MISSING".into(),
                },
            )
            .unwrap();
        let failed = store
            .connector_pack_status("tenant-a", &ResourceId::new("demo").unwrap())
            .unwrap();
        assert_eq!(
            failed.head.unwrap().visible_record_id.as_deref(),
            Some(first_plan.record_id.as_str())
        );
    }

    #[test]
    fn projection_rows_are_tenant_isolated() {
        let (_dir, store) = crate::server::persistence::agent_fixtures::open_agent_store();
        commit_pack(&store, "tenant-a", "demo", 1, None, 1);
        commit_pack(&store, "tenant-b", "demo", 1, None, 1);
        let connector = ResourceId::new("demo").unwrap();
        let plan = store
            .prepare_connector_pack_projection("tenant-a", &connector)
            .unwrap();
        let context = crate::server::persistence::agent_fixtures::mutation_context(
            &store,
            "tenant-a",
            "tenant-a-reproject",
            2,
            0,
            "connector-pack:reproject",
        );
        store
            .commit_connector_pack_projection(
                context,
                &plan,
                PackProjectionState::Applied {
                    graph: format!("pack__{}", "a".repeat(64)),
                    graph_version: 1,
                },
            )
            .unwrap();
        let untouched = store.connector_pack_status("tenant-b", &connector).unwrap();
        assert_eq!(untouched.projection, PackProjectionState::Pending);
        assert_eq!(untouched.head.unwrap().visible_record_id, None);
    }
}
