//! The administrative ConnectorPack writes: importer binding and permanent
//! retirement (PB3, ruling D7), and the live body-holder set the orphan
//! reconciler compares the Blob CAS against (PA3).
//!
//! Each write is one admitted Agent Library batch through
//! [`AgentLibraryStore::commit_pack_write`], so a lost response replays its
//! recorded result and the same key with a different body is refused.

use std::collections::BTreeSet;

use eg_types::agent_component::{AgentComponentEntry, AgentComponentMutationKind};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::connector_pack::{ConnectorPackBindingResult, PackRetireResult};
use eg_types::contract::{BoundedVec, Digest256, ResourceId};
use eg_types::mutation_batch::{DurabilityDomain, MutationOperation, MutationSurface};
use eg_types::protocol::Method;

use super::admitted::{PackOwnerWrite, PackWriteEffects, PackWriteIdentity};
use super::commit::{
    apply_catalog_rows, component_effects, ConnectorPackComponentCommit, ConnectorPackHolderCommit,
    PackCatalogRows,
};
use super::{ConnectorPackBindingRow, ConnectorPackBodyHolderRow, ConnectorPackMemberRow};
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Typed mutation-result schema of a binding change.
pub(crate) const CONNECTOR_PACK_BINDING_RESULT_SCHEMA_ID: &str = "connector-pack-binding-result.v1";
/// Typed mutation-result schema of a retirement.
pub(crate) const CONNECTOR_PACK_RETIRE_RESULT_SCHEMA_ID: &str = "connector-pack-retire-result.v1";

/// What a binding write does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindingChange<'a> {
    /// Bind the connector to exactly this importer principal.
    Bind { importer: &'a str },
    /// Remove the connector's binding; the bootstrap importer applies again.
    Unbind,
}

impl BindingChange<'_> {
    fn verb(self) -> (&'static str, &'static str) {
        match self {
            Self::Bind { .. } => ("connector-pack:bind", "connector-pack-bind"),
            Self::Unbind => ("connector-pack:unbind", "connector-pack-unbind"),
        }
    }
}

/// A principal persistence id: `principal:sha256:<64 lowercase hex>`, the
/// only form a verified caller is ever compared under.
pub(crate) fn validate_importer(importer: &str) -> Result<(), String> {
    let invalid = || "INVALID_IMPORTER: importer must be a principal persistence id".to_string();
    let digest = importer
        .strip_prefix("principal:sha256:")
        .ok_or_else(invalid)?;
    Digest256::parse(digest).map_err(|_| invalid())?;
    Ok(())
}

impl AgentLibraryStore {
    /// Bind a connector to one importer, or remove its binding.
    pub(crate) fn change_connector_pack_binding(
        &self,
        context: AgentLibraryMutationContext,
        connector: &ResourceId,
        change: BindingChange<'_>,
    ) -> Result<ConnectorPackBindingResult, String> {
        eg_types::agent_library::validate_key(&context.tenant_id, connector.as_str())?;
        eg_types::connector_pack::validate_connector(connector)?;
        let importer = match change {
            BindingChange::Bind { importer } => {
                validate_importer(importer)?;
                Some(importer)
            }
            BindingChange::Unbind => None,
        };
        let (purpose, kind) = change.verb();
        let identity = PackWriteIdentity {
            purpose,
            kind,
            slug: kind,
            subject: connector.as_str(),
            revision: 0,
            discriminator: importer,
            result_schema: CONNECTOR_PACK_BINDING_RESULT_SCHEMA_ID,
            noun: "connector pack binding",
        };
        let effects = PackWriteEffects {
            operations: admin_operation(kind, connector, importer.unwrap_or("")),
            outbox: Vec::new(),
        };
        self.commit_pack_write(&context, identity, effects, |write, staged| {
            apply_binding(write, &context, connector, importer)?;
            Ok(ConnectorPackBindingResult {
                schema_version: eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION,
                tenant_id: context.tenant_id.clone(),
                connector: connector.clone(),
                importer: importer.map(str::to_string),
                bound_by: context.caller_principal.clone(),
                bound_at_ms: context.created_at_ms,
                batch_id: staged.batch_id.clone(),
                committed_version: staged.committed_version,
            })
        })
    }

    /// Permanently retire named members of one connector.
    ///
    /// Each member gets a new component revision in lifecycle `Retired`, its
    /// member row follows, and its body stays held by the new revision (every
    /// revision stays resolvable). A retired member that reappears in a later
    /// pack is refused with `RETIRED_ENTRY_RETURNED`.
    pub(crate) fn retire_connector_pack_entries(
        &self,
        context: AgentLibraryMutationContext,
        connector: &ResourceId,
        uris: &[String],
    ) -> Result<PackRetireResult, String> {
        eg_types::agent_library::validate_key(&context.tenant_id, connector.as_str())?;
        let plan = self.plan_retirement(&context, connector, uris)?;
        let discriminator = retire_discriminator(uris)?;
        let identity = PackWriteIdentity {
            purpose: "connector-pack:retire",
            kind: "connector-pack-retire",
            slug: "connector-pack-retire",
            subject: connector.as_str(),
            revision: 0,
            discriminator: Some(&discriminator),
            result_schema: CONNECTOR_PACK_RETIRE_RESULT_SCHEMA_ID,
            noun: "connector pack retirement",
        };
        let effects = component_effects(&plan.components, &context)?;
        let retired = plan
            .components
            .iter()
            .map(|component| eg_types::agent_component::ComponentDependency {
                component_id: component.entry.component_id.clone(),
                kind: component.entry.kind,
                definition_digest: component.entry.definition_digest.clone(),
            })
            .collect::<Vec<_>>();
        self.commit_pack_write(&context, identity, effects, |write, staged| {
            apply_catalog_rows(
                write,
                &PackCatalogRows {
                    context: &context,
                    connector: connector.as_str(),
                    components: &plan.components,
                    members: &plan.members,
                    holders: &plan.holders,
                },
            )?;
            Ok(PackRetireResult {
                schema_version: eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION,
                tenant_id: context.tenant_id.clone(),
                connector: connector.clone(),
                retired: BoundedVec::new(retired)?,
                batch_id: staged.batch_id.clone(),
                committed_version: staged.committed_version,
            })
        })
    }

    fn plan_retirement(
        &self,
        context: &AgentLibraryMutationContext,
        connector: &ResourceId,
        uris: &[String],
    ) -> Result<RetirePlan, String> {
        let wanted = requested_uris(uris)?;
        let mut plan = RetirePlan::default();
        for member in self.connector_pack_members(&context.tenant_id, connector)? {
            if !wanted.contains(member.uri.as_str()) {
                continue;
            }
            if member.lifecycle == AgentLibraryLifecycle::Retired {
                return Err(format!(
                    "PACK_ENTRY_ALREADY_RETIRED: '{}' is already retired",
                    member.uri
                ));
            }
            let current = self
                .current_component(&context.tenant_id, &member.component_id)?
                .ok_or_else(|| {
                    "CORRUPT_CONNECTOR_PACK: member has no component revision".to_string()
                })?;
            plan.add(member, current, context.created_at_ms)?;
        }
        if plan.members.len() != wanted.len() {
            return Err(
                "UNKNOWN_PACK_ENTRY: every retired uri must name a member of the connector"
                    .to_string(),
            );
        }
        Ok(plan)
    }

    /// Every body digest some component revision of `tenant_id` holds, in one
    /// Agent Library snapshot. The reconciler releases every engine pack body
    /// of the tenant that is NOT in this set.
    pub(crate) fn connector_pack_held_bodies(
        &self,
        tenant_id: &str,
    ) -> Result<BTreeSet<Digest256>, String> {
        eg_types::agent_library::validate_key(tenant_id, "connector-pack")?;
        let read = self.read()?;
        let holders = read.open_owner_table(eg_storage::CONNECTOR_PACK_BODY_HOLDERS)?;
        let mut held = BTreeSet::new();
        for row in holders
            .range((tenant_id, "", "", 0)..)
            .map_err(|error| error.to_string())?
        {
            let (key, _) = row.map_err(|error| error.to_string())?;
            let (row_tenant, body, _, _) = key.value();
            if row_tenant != tenant_id {
                break;
            }
            held.insert(Digest256::parse(body).map_err(|_| {
                "CORRUPT_CONNECTOR_PACK: body holder key is not a digest".to_string()
            })?);
        }
        Ok(held)
    }
}

#[derive(Default)]
struct RetirePlan {
    components: Vec<ConnectorPackComponentCommit>,
    members: Vec<ConnectorPackMemberRow>,
    holders: Vec<ConnectorPackHolderCommit>,
}

impl RetirePlan {
    fn add(
        &mut self,
        member: ConnectorPackMemberRow,
        current: AgentComponentEntry,
        now_ms: u64,
    ) -> Result<(), String> {
        let revision = current
            .entry_revision
            .checked_add(1)
            .ok_or_else(|| "agent component revision overflow".to_string())?;
        let retired = AgentComponentEntry::create(
            current.as_draft(),
            revision,
            AgentLibraryLifecycle::Retired,
            current.created_at_ms,
            now_ms,
        )?;
        self.holders.push(ConnectorPackHolderCommit {
            body_sha256: member.body_sha256,
            component_id: member.component_id.clone(),
            entry_revision: revision,
            row: ConnectorPackBodyHolderRow {
                engine_manifest_digest: member.engine_manifest_digest.clone(),
                length: member.body_length,
            },
        });
        let mut row = member;
        row.entry_revision = revision;
        row.definition_digest = retired.definition_digest.clone();
        row.lifecycle = AgentLibraryLifecycle::Retired;
        self.members.push(row);
        self.components.push(ConnectorPackComponentCommit {
            expected_revision: current.entry_revision,
            kind: AgentComponentMutationKind::Retire,
            entry: retired,
        });
        Ok(())
    }
}

fn requested_uris(uris: &[String]) -> Result<BTreeSet<&str>, String> {
    let wanted: BTreeSet<&str> = uris.iter().map(String::as_str).collect();
    if wanted.is_empty() || wanted.len() != uris.len() {
        return Err(
            "INVALID_RETIREMENT: retire needs at least one uri and no uri twice".to_string(),
        );
    }
    Ok(wanted)
}

/// A content discriminator over the sorted uri set, so a replay of the same
/// retirement matches and a different set under the same key conflicts.
fn retire_discriminator(uris: &[String]) -> Result<String, String> {
    let sorted: BTreeSet<&str> = uris.iter().map(String::as_str).collect();
    let fields: Vec<&[u8]> = sorted.iter().map(|uri| uri.as_bytes()).collect();
    Ok(Digest256::framed(b"eg/connector-pack-retire/v1", &fields)?.to_hex())
}

fn admin_operation(kind: &str, connector: &ResourceId, detail: &str) -> Vec<MutationOperation> {
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: kind.replace('-', "_"),
            query: format!("{}:{detail}", connector.as_str()),
        },
    }]
}

fn apply_binding(
    write: &PackOwnerWrite<'_>,
    context: &AgentLibraryMutationContext,
    connector: &ResourceId,
    importer: Option<&str>,
) -> Result<(), String> {
    let key = (context.tenant_id.as_str(), connector.as_str());
    let mut bindings = write.open_table(eg_storage::CONNECTOR_PACK_BINDINGS)?;
    let Some(importer) = importer else {
        let removed = bindings.remove(key).map_err(|error| error.to_string())?;
        return removed.map(|_| ()).ok_or_else(|| {
            "CONNECTOR_PACK_UNBOUND: connector has no importer binding".to_string()
        });
    };
    let row = ConnectorPackBindingRow {
        importer: importer.to_string(),
        bound_by: context.caller_principal.clone(),
        bound_at_ms: context.created_at_ms,
    };
    let bytes = eg_storage::encode_bounded(&row, "connector pack binding")?;
    bindings
        .insert(key, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::persistence::agent_fixtures::{mutation_context, open_agent_store};

    const IMPORTER: &str =
        "principal:sha256:0000000000000000000000000000000000000000000000000000000000000001";

    fn connector() -> ResourceId {
        ResourceId::new("demo").unwrap()
    }

    fn context(store: &AgentLibraryStore, key: &str, nonce: u8) -> AgentLibraryMutationContext {
        mutation_context(store, "tenant-a", key, nonce, 0, "connector-pack:bind")
    }

    #[test]
    fn importer_must_be_a_principal_persistence_id() {
        assert!(validate_importer(IMPORTER).is_ok());
        for bad in [
            "",
            "principal:sha256:",
            "principal:sha256:XYZ",
            "agent:connector-sync",
            &IMPORTER.to_uppercase(),
        ] {
            let error = validate_importer(bad).unwrap_err();
            assert!(error.starts_with("INVALID_IMPORTER:"), "{bad}: {error}");
        }
    }

    #[test]
    fn retirement_names_each_uri_once_and_its_identity_ignores_order() {
        let a = vec!["tool://demo/a".to_string(), "tool://demo/b".to_string()];
        let b = vec!["tool://demo/b".to_string(), "tool://demo/a".to_string()];
        assert_eq!(
            retire_discriminator(&a).unwrap(),
            retire_discriminator(&b).unwrap()
        );
        assert!(requested_uris(&[]).is_err());
        let twice = vec!["tool://demo/a".to_string(), "tool://demo/a".to_string()];
        assert!(requested_uris(&twice).is_err());
    }

    #[test]
    fn bind_then_unbind_moves_the_importer_and_a_second_unbind_is_refused() {
        let (_dir, store) = open_agent_store();
        let bound = store
            .change_connector_pack_binding(
                context(&store, "bind-1", 1),
                &connector(),
                BindingChange::Bind { importer: IMPORTER },
            )
            .unwrap();
        assert_eq!(bound.importer.as_deref(), Some(IMPORTER));
        assert_eq!(
            store
                .connector_pack_importer("tenant-a", &connector())
                .unwrap()
                .as_deref(),
            Some(IMPORTER)
        );
        let unbound = store
            .change_connector_pack_binding(
                context(&store, "unbind-1", 2),
                &connector(),
                BindingChange::Unbind,
            )
            .unwrap();
        assert_eq!(unbound.importer, None);
        assert_eq!(
            store
                .connector_pack_importer("tenant-a", &connector())
                .unwrap(),
            None
        );
        let error = store
            .change_connector_pack_binding(
                context(&store, "unbind-2", 3),
                &connector(),
                BindingChange::Unbind,
            )
            .unwrap_err();
        assert!(error.starts_with("CONNECTOR_PACK_UNBOUND:"), "{error}");
    }

    #[test]
    fn a_lost_bind_response_replays_and_a_changed_body_conflicts() {
        let (_dir, store) = open_agent_store();
        let first = store
            .change_connector_pack_binding(
                context(&store, "bind-once", 1),
                &connector(),
                BindingChange::Bind { importer: IMPORTER },
            )
            .unwrap();
        let replayed = store
            .change_connector_pack_binding(
                context(&store, "bind-once", 2),
                &connector(),
                BindingChange::Bind { importer: IMPORTER },
            )
            .unwrap();
        assert_eq!(first, replayed);
        let other = IMPORTER.replace("01", "02");
        let error = store
            .change_connector_pack_binding(
                context(&store, "bind-once", 3),
                &connector(),
                BindingChange::Bind { importer: &other },
            )
            .unwrap_err();
        assert!(error.starts_with("IDEMPOTENCY_CONFLICT:"), "{error}");
    }

    #[test]
    fn retiring_an_unknown_member_is_refused_before_any_write() {
        let (_dir, store) = open_agent_store();
        let error = store
            .retire_connector_pack_entries(
                context(&store, "retire-1", 1),
                &connector(),
                &["tool://demo/missing".to_string()],
            )
            .unwrap_err();
        assert!(error.starts_with("UNKNOWN_PACK_ENTRY:"), "{error}");
    }

    #[test]
    fn held_bodies_are_scoped_to_their_tenant() {
        let (_dir, store) = open_agent_store();
        assert!(store
            .connector_pack_held_bodies("tenant-a")
            .unwrap()
            .is_empty());
    }
}
