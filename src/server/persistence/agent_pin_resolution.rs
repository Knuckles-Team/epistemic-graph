//! The shared mechanics of resolving one PINNED cross-record reference.
//!
//! Every layer of the agent hierarchy refers to the layers below it by pin: an
//! id plus the exact `definition_digest`/`shape_digest` of the revision being
//! referred to. RF-ADR-008's argument for resolving one such pin is that
//! *resolution is by (id, digest) alone, so without it a leaked digest is an
//! execution grant*. That argument applies to every pin, and it is why this
//! module exists: validation of a published record is local and structural --
//! well-formed text, a well-formed `sha256:<hex>`, the right kind for the slot,
//! no duplicates -- and never asks whether the referenced thing EXISTS.
//!
//! # What lives here, and what does not
//!
//! The per-pin question -- one head read, one retired check, one bounded
//! fallback scan, one recorded revision check -- plus the two resolvers that
//! ask it of a whole publish: [`resolve_agent_pins_in_write`] for the L2 agent
//! layer and [`resolve_template_pins_in_write`] for the template layer.
//!
//! Both used to live in their own layer module, because each needed that
//! module's row decoder in scope and the decode carried the layer's own row
//! bounds with it. The bounds are now the hierarchy's
//! ([`super::agent_row`]), which leaves each layer's decoder a three-line
//! wrapper it can export `pub(super)` -- so nothing keeps these two apart any
//! more, and keeping them together is what makes [`PinLayer`] and
//! [`resolve_pins`] private to this module rather than a shape every layer
//! assembles for itself.
//!
//! The COMPONENT layer's resolver is still its own, in
//! [`super::agent_component`]: it is the one layer whose pin also names a
//! KIND, and matching that kind is not a question this module asks.
//!
//! # Why INSIDE the write transaction
//!
//! The same reason [`super::agent_graph`]'s composition check is: resolving
//! from a separate read could admit a record whose dependency was retired
//! between the two reads.
//!
//! # What is checked, per pin
//!
//! The record must exist in THIS tenant; some retained revision must carry the
//! exact pinned digest; where the pin also records a revision NUMBER, it must
//! be the revision that digest resolves to; and the record's HEAD must not be
//! retired -- a retired record stays RESOLVABLE, so records that already pin it
//! keep working, but nothing new may be built on something withdrawn. That is
//! the retained-but-not-buildable rule composition already applies to child
//! graphs. The KIND check belongs to the layer: only the component layer has
//! one, and its resolver makes it.

use eg_types::agent_library::AgentLibraryLifecycle;

/// A revision table of one agent layer. All four are keyed and valued alike.
pub(super) type RevisionTable<'a> =
    eg_storage::OwnerReadTable<'a, (&'static str, &'static str, u64), &'static [u8]>;
/// A head table of one agent layer.
pub(super) type HeadTable<'a> = eg_storage::OwnerReadTable<'a, (&'static str, &'static str), u64>;
/// The admitted write a resolution runs inside.
pub(super) type Write<'a> = eg_transaction::AdmittedMutation<'a, eg_storage::AgentLibraryOwner>;

/// Most revision rows one publish may read while resolving its pins.
///
/// The second half of a resolver's cost bound: each layer's pin-count bound
/// caps how many records are looked up, this caps how deep the lookups may
/// search in total when a pin names something other than the record's HEAD. One
/// bound for every layer, because it caps the same resource for the same
/// reason.
pub(super) const MAX_PIN_RESOLUTION_ROWS: usize = 131_072;

/// Most DISTINCT templates one publish may resolve.
///
/// The largest legal count is a graph shape's: one template per `Template`
/// node, and `MAX_NODES` is 256. A library entry pins at most one, through
/// `instantiated_from`. Set above both, so it refuses abuse and never a record
/// that validates.
const MAX_RESOLVED_TEMPLATE_PINS: usize = 1_024;

/// Most DISTINCT Agent Library entries one publish may resolve.
///
/// The L3 -> L2 fan-out: a graph shape pins one agent per `Agent` node, and
/// `MAX_NODES` is 256. Set above that largest legal count so it refuses abuse,
/// never a shape that validates -- the same shape of bound
/// `MAX_RESOLVED_COMPONENT_PINS` is for L1.
const MAX_RESOLVED_AGENT_PINS: usize = 1_024;

/// One pinned reference to a durable template revision.
///
/// `entry_revision` is `Some` only where the pinning site records one --
/// [`eg_types::agent_template::TemplateInstanceRef`] does, an
/// `AgentGraphNodeKind::Template` does not. Where it is recorded it is checked,
/// because a stored revision number that disagreed with the revision the digest
/// actually resolves to is a provenance record that points at the wrong row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TemplatePin<'a> {
    pub template_id: &'a str,
    pub definition_digest: &'a str,
    pub entry_revision: Option<u64>,
}

/// What the head row of a pinned record says about it.
struct ResolvedHead {
    revision: u64,
    definition_digest: String,
    retired: bool,
}

/// Everything about ONE agent layer that resolving a pin against it needs.
///
/// One value rather than six loose arguments: the two durable tables, the name
/// the layer is called by in a refusal, its two bounds, and how to read a row's
/// identity all belong to the same layer and are always supplied together.
///
/// The layer supplies `decode` because only it knows its own record type and
/// which sub-record of a row has to validate; the two callers below are the
/// only ones, which is why neither this struct nor [`resolve_pins`] is visible
/// outside this module.
struct PinLayer<D> {
    pub record_kind: &'static str,
    pub heads: redb::TableDefinition<'static, (&'static str, &'static str), u64>,
    pub revisions: redb::TableDefinition<'static, (&'static str, &'static str, u64), &'static [u8]>,
    /// Most DISTINCT records one publish may resolve in this layer. Set above
    /// the largest LEGAL pin count so it refuses abuse, never a record that
    /// validates.
    pub max_pins: usize,
    /// How deep one lookup may scan when a pin names something other than the
    /// record's HEAD: the layer's own retained-revision bound.
    pub retained_revision_bound: usize,
    pub decode: D,
}

/// Resolve pinned references to L2 AGENT records, inside an admitted write.
///
/// A graph's `Agent` node names an entry by `(agent_id, definition_digest)`
/// and nothing checked that the entry existed. Local validation checks only
/// that the id is well-formed text and the digest a well-formed
/// `sha256:<hex>`, so 64 invented hex characters published a graph claiming
/// -- and, once admitted, executing -- an agent it was never composed
/// against, and the shape's own `shape_digest` attested to the claim.
///
/// There is no kind to match here: the slot is "an agent", and the Agent
/// Library tables are what discriminate it from a component or a template.
pub(super) fn resolve_agent_pins_in_write(
    write: &Write<'_>,
    tenant_id: &str,
    subject: &str,
    pins: &[(&str, &str)],
) -> Result<(), String> {
    // An `Agent` node records no revision number, only the digest.
    let pins: Vec<(&str, &str, Option<u64>)> = pins
        .iter()
        .map(|(agent_id, digest)| (*agent_id, *digest, None))
        .collect();
    resolve_pins(
        PinLayer {
            record_kind: "agent",
            heads: eg_storage::AGENT_LIBRARY_HEADS,
            revisions: eg_storage::AGENT_LIBRARY_REVISIONS,
            max_pins: MAX_RESOLVED_AGENT_PINS,
            retained_revision_bound: super::agent_library::MAX_AGENT_LIBRARY_REVISIONS,
            decode: |bytes: &[u8]| {
                super::agent_library::decode_entry(bytes)
                    .map(|entry| (entry.definition_digest, entry.lifecycle))
            },
        },
        write,
        tenant_id,
        subject,
        &pins,
    )
}

/// Resolve pinned references to TEMPLATE records, inside an admitted write.
pub(super) fn resolve_template_pins_in_write(
    write: &Write<'_>,
    tenant_id: &str,
    subject: &str,
    pins: &[TemplatePin<'_>],
) -> Result<(), String> {
    let pins: Vec<(&str, &str, Option<u64>)> = pins
        .iter()
        .map(|pin| (pin.template_id, pin.definition_digest, pin.entry_revision))
        .collect();
    resolve_pins(
        PinLayer {
            record_kind: "template",
            heads: eg_storage::AGENT_TEMPLATE_HEADS,
            revisions: eg_storage::AGENT_TEMPLATE_REVISIONS,
            max_pins: MAX_RESOLVED_TEMPLATE_PINS,
            retained_revision_bound: super::agent_template::MAX_AGENT_TEMPLATE_REVISIONS,
            decode: |bytes: &[u8]| {
                super::agent_template::decode_template(bytes)
                    .map(|entry| (entry.definition_digest, entry.lifecycle))
            },
        },
        write,
        tenant_id,
        subject,
        &pins,
    )
}

/// Resolve every `(record_id, definition_digest, recorded_revision)` pin one
/// publish makes against one layer.
///
/// Deduplicated: a record that pins the same revision from two slots should
/// cost one lookup, and the fan-out bound should count what is actually
/// resolved.
fn resolve_pins<D>(
    layer: PinLayer<D>,
    write: &Write<'_>,
    tenant_id: &str,
    subject: &str,
    pins: &[(&str, &str, Option<u64>)],
) -> Result<(), String>
where
    D: Fn(&[u8]) -> Result<(String, AgentLibraryLifecycle), String>,
{
    let distinct: std::collections::BTreeSet<(&str, &str, Option<u64>)> =
        pins.iter().copied().collect();
    if distinct.len() > layer.max_pins {
        return Err(format!(
            "{subject} pins more than {} distinct {}s",
            layer.max_pins, layer.record_kind
        ));
    }
    let heads = write.open_read_table(layer.heads)?;
    let revisions = write.open_read_table(layer.revisions)?;
    let mut rows = 0usize;
    for pin in distinct {
        resolve_pin(
            &layer, &heads, &revisions, tenant_id, subject, pin, &mut rows,
        )?;
    }
    Ok(())
}

/// Resolve one pinned `(record_id, definition_digest, recorded_revision)`.
fn resolve_pin<D>(
    layer: &PinLayer<D>,
    heads: &HeadTable<'_>,
    revisions: &RevisionTable<'_>,
    tenant_id: &str,
    subject: &str,
    pin: (&str, &str, Option<u64>),
    rows: &mut usize,
) -> Result<(), String>
where
    D: Fn(&[u8]) -> Result<(String, AgentLibraryLifecycle), String>,
{
    let record_kind = layer.record_kind;
    let decode = &layer.decode;
    let (record_id, definition_digest, recorded_revision) = pin;
    let Some(head) = resolve_head(heads, revisions, tenant_id, record_id, decode)? else {
        return Err(format!(
            "{subject} pins {record_kind} '{record_id}', which does not exist in this tenant"
        ));
    };
    *rows += 1;
    if head.retired {
        return Err(format!(
            "{subject} pins {record_kind} '{record_id}', which is retired"
        ));
    }
    let resolved = match head.definition_digest == definition_digest {
        true => Some(head.revision),
        false => find_revision_with_digest(
            revisions,
            tenant_id,
            record_id,
            definition_digest,
            layer.retained_revision_bound,
            rows,
            decode,
        )?,
    };
    let Some(resolved) = resolved else {
        return Err(format!(
            "{subject} pins a revision of {record_kind} '{record_id}' that was never published: \
             no retained revision matches the pinned definition digest"
        ));
    };
    check_recorded_revision(subject, record_kind, record_id, recorded_revision, resolved)
}

/// The head of one record, or `None` when this tenant holds no such record.
///
/// The HEAD is read first for two reasons: its lifecycle is the one that
/// decides whether anything NEW may be built on the record -- a tombstone is a
/// separate later revision, so the pinned revision stays `Published` forever --
/// and the overwhelmingly common pin IS the head, which turns the whole
/// resolution into one read.
fn resolve_head(
    heads: &HeadTable<'_>,
    revisions: &RevisionTable<'_>,
    tenant_id: &str,
    record_id: &str,
    decode: impl Fn(&[u8]) -> Result<(String, AgentLibraryLifecycle), String>,
) -> Result<Option<ResolvedHead>, String> {
    let Some(revision) = heads
        .get((tenant_id, record_id))?
        .map(|value| value.value())
    else {
        return Ok(None);
    };
    let row = revisions
        .get((tenant_id, record_id, revision))?
        .ok_or_else(|| "agent record head points to a missing revision".to_string())?;
    let (definition_digest, lifecycle) = decode(row.value())?;
    Ok(Some(ResolvedHead {
        revision,
        definition_digest,
        retired: lifecycle == AgentLibraryLifecycle::Retired,
    }))
}

/// The revision of `record_id` carrying `definition_digest`, or `None`.
///
/// `range_from` is open-ended, so the prefix is re-checked per row: without the
/// break this walks into the NEXT record's revisions and could resolve a digest
/// belonging to a different one. The table handle is passed in rather than
/// reopened because redb refuses a second open of the same table while the
/// first handle is alive.
fn find_revision_with_digest(
    revisions: &RevisionTable<'_>,
    tenant_id: &str,
    record_id: &str,
    definition_digest: &str,
    retained_revision_bound: usize,
    rows: &mut usize,
    decode: impl Fn(&[u8]) -> Result<(String, AgentLibraryLifecycle), String>,
) -> Result<Option<u64>, String> {
    let mut scanned = 0usize;
    for row in revisions.range_from((tenant_id, record_id, 0))? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_tenant, row_record, row_revision) = key.value();
        if row_tenant != tenant_id || row_record != record_id {
            break;
        }
        scanned += 1;
        *rows += 1;
        bound_resolution_work(scanned, *rows, retained_revision_bound)?;
        if decode(value.value())?.0 == definition_digest {
            return Ok(Some(row_revision));
        }
    }
    Ok(None)
}

/// Both halves of a scan's cost bound, checked before a row is trusted.
fn bound_resolution_work(
    scanned: usize,
    rows: usize,
    retained_revision_bound: usize,
) -> Result<(), String> {
    if scanned > retained_revision_bound {
        return Err("agent record history exceeds its retained revision bound".to_string());
    }
    if rows > MAX_PIN_RESOLUTION_ROWS {
        return Err(format!(
            "reference resolution exceeds its {MAX_PIN_RESOLUTION_ROWS}-row bound"
        ));
    }
    Ok(())
}

/// A pin that records a revision NUMBER as well as a digest must agree with the
/// revision that digest resolves to: a provenance record that disagrees points
/// at the wrong row, and every traversal that trusts the number reads it.
fn check_recorded_revision(
    subject: &str,
    record_kind: &str,
    record_id: &str,
    recorded: Option<u64>,
    resolved: u64,
) -> Result<(), String> {
    match recorded {
        Some(recorded) if recorded != resolved => Err(format!(
            "{subject} pins {record_kind} '{record_id}' at revision {recorded}, but the pinned \
             definition digest is revision {resolved}"
        )),
        _ => Ok(()),
    }
}

/// Publish one template and return the `definition_digest` that resolves it.
///
/// Shared with the graph test module: a graph's `Template` node pin is resolved
/// at publish admission, so a fixture can no longer invent a digest. The base
/// agent draft and the components it is assembled from are seeded first.
///
/// Idempotent per store, and deterministic for the same reason the agent and
/// component seeds are: a template's definition digest is a hash over its
/// content alone -- no revision, no timestamp.
#[cfg(test)]
pub(crate) fn seed_template_for_test(
    store: &super::agent_library::AgentLibraryStore,
    tenant_id: &str,
    template_id: &str,
    nonce_index: u8,
) -> String {
    if let Some(existing) = store
        .current_template(tenant_id, template_id)
        .expect("read a seeded template")
    {
        return existing.definition_digest;
    }
    let mut base = super::agent_library::seed_agent_draft_for_test(
        tenant_id,
        &format!("{template_id}:base"),
        nonce_index,
    );
    super::agent_component::seed_draft_components_for_test(store, &mut base, nonce_index);
    let mut nonce_bytes = [0xEEu8; 32];
    nonce_bytes[0] = 0x50u8.wrapping_add(nonce_index);
    let policy_digest = super::agent_library::current_agent_library_policy_digest().unwrap();
    store
        .publish_template(eg_types::agent_template::AgentTemplatePublishRequest {
            context: eg_types::agent_library::AgentLibraryMutationContext {
                request_id: 95_000 + u64::from(nonce_index),
                principal: store.owner_principal().to_string(),
                caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
                attempt_nonce: eg_types::contract::Nonce::from_bytes(nonce_bytes),
                tenant_id: tenant_id.to_string(),
                actor_scope: "action-scope:template-seed".to_string(),
                purpose_id: "agent-template:publish".to_string(),
                policy_revision: "policy-v1".to_string(),
                policy_digest: policy_digest.clone(),
                policy_decision_id: "agent-template:decision:policy-v1".to_string(),
                idempotency_key: format!("template-seed:{tenant_id}:{template_id}"),
                expected_revision: Some(0),
                trace_id: None,
                created_at_ms: 5,
            },
            template: eg_types::agent_template::AgentTemplateDraft {
                template_id: template_id.to_string(),
                version: "1.0.0".to_string(),
                base,
                params: Vec::new(),
                tenant_id: tenant_id.to_string(),
                actor_scope: "definition:builder-a".to_string(),
                purpose_id: "agent-template:definition".to_string(),
                policy_digest,
            },
        })
        .expect("the fixture's template publishes")
        .result
        .template
        .definition_digest
}

#[cfg(test)]
mod tests {
    use super::super::agent_library::AgentLibraryStore;
    use eg_types::agent_library::{
        AgentLibraryEntryDraft, AgentLibraryMutationContext, AgentLibraryPublishRequest,
    };
    use eg_types::agent_template::AgentTemplateInstantiateRequest;
    use std::collections::BTreeMap;

    const TEMPLATE_ID: &str = "template:researcher";
    const INSTANCE_ID: &str = "agent:researcher-cheap";

    fn context(store: &AgentLibraryStore, nonce: u8) -> AgentLibraryMutationContext {
        super::super::agent_fixtures::mutation_context(
            store,
            "tenant-a",
            &format!("instance-key-{nonce}"),
            nonce,
            0,
            "agent-library:definition",
        )
    }

    /// A store holding one template at revision 1, and the instance draft it
    /// mints -- ready to publish through the ordinary Agent Library path.
    fn instance_draft() -> (tempfile::TempDir, AgentLibraryStore, AgentLibraryEntryDraft) {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        super::seed_template_for_test(&store, "tenant-a", TEMPLATE_ID, 10);
        let draft = store
            .instantiate_template(&AgentTemplateInstantiateRequest {
                tenant_id: "tenant-a".to_string(),
                template_id: TEMPLATE_ID.to_string(),
                entry_revision: None,
                agent_id: INSTANCE_ID.to_string(),
                bindings: BTreeMap::new(),
            })
            .expect("instantiates");
        assert_eq!(
            draft
                .instantiated_from
                .as_ref()
                .expect("an instance records where it came from")
                .entry_revision,
            1
        );
        (dir, store, draft)
    }

    fn publish_instance(
        store: &AgentLibraryStore,
        entry: AgentLibraryEntryDraft,
    ) -> Result<(), String> {
        store
            .publish(AgentLibraryPublishRequest {
                context: context(store, 3),
                entry,
            })
            .map(|_| ())
    }

    #[test]
    fn an_instance_pinning_a_template_that_does_not_exist_is_refused() {
        // The L2 -> TEMPLATE edge. `instantiated_from` is what makes "which
        // agents came from this template?" a traversal rather than a guess, and
        // unresolved it was a free-text provenance claim that the entry's own
        // `definition_digest` then attested to.
        let (_dir, store, mut draft) = instance_draft();
        draft.instantiated_from.as_mut().unwrap().template_id = "template:ghost".to_string();
        let error =
            publish_instance(&store, draft).expect_err("an unresolvable template pin is refused");
        assert!(
            error.contains("which does not exist in this tenant"),
            "got: {error}"
        );
        assert!(store.current("tenant-a", INSTANCE_ID).unwrap().is_none());
    }

    #[test]
    fn an_instance_pinning_an_invented_template_digest_is_refused() {
        let (_dir, store, mut draft) = instance_draft();
        draft.instantiated_from.as_mut().unwrap().definition_digest =
            format!("sha256:{}", "7".repeat(64));
        let error = publish_instance(&store, draft)
            .expect_err("a digest no template revision carries is refused");
        assert!(error.contains("that was never published"), "got: {error}");
    }

    #[test]
    fn an_instance_recording_the_wrong_template_revision_is_refused() {
        // The pin carries a revision NUMBER as well as a digest. A provenance
        // record whose number disagrees with the revision its digest resolves
        // to points at the wrong row, and every traversal that trusts the
        // number reads the wrong template.
        let (_dir, store, mut draft) = instance_draft();
        draft.instantiated_from.as_mut().unwrap().entry_revision = 2;
        let error =
            publish_instance(&store, draft).expect_err("a mismatched revision number is refused");
        assert!(
            error.contains("at revision 2, but the pinned definition digest is revision 1"),
            "got: {error}"
        );
    }

    #[test]
    fn an_unaltered_instance_publishes() {
        // The discriminating half: every refusal above is caused by the ONE
        // field it mutates, not by the fixture being unpublishable.
        let (_dir, store, draft) = instance_draft();
        publish_instance(&store, draft).expect("an instance publishes like any other agent");
        assert!(store.current("tenant-a", INSTANCE_ID).unwrap().is_some());
    }
}
