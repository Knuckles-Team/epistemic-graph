use super::*;
use eg_types::{IncarnationId, MutationDomain};
use redb::{ReadTransaction, ReadableTable};
use serde::{Deserialize, Deserializer, Serialize};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

const MANIFEST_KEY: &str = "manifest";
const OWNER_LAYOUT_DOMAIN: &[u8] = b"eg/mutation-owner-layout/v1\0";
const OWNER_MANIFEST_DIGEST_DOMAIN: &[u8] = b"eg/mutation-owner-manifest/v1\0";
const OWNER_LAYOUT_NAMES: [&str; 8] = [
    "ledger_only",
    "rbac",
    "jobs",
    "statechart",
    "time_series",
    "kv",
    "blob",
    "semantic_index",
];
const OWNER_LAYOUT_DOMAINS: [MutationDomain; 8] = [
    MutationDomain::ControlPlane,
    MutationDomain::ControlPlane,
    MutationDomain::AnalyticsJob,
    MutationDomain::Lifecycle,
    MutationDomain::TimeSeries,
    MutationDomain::KvStore,
    MutationDomain::BlobStore,
    MutationDomain::SemanticIndex,
];
pub(crate) const LEDGER_TABLE_NAMES: [&str; 15] = [
    "mutation_store_root_v1",
    "mutation_scope_bindings_v1",
    "mutation_owner_manifest_v1",
    "mutation_batches_v1",
    "mutation_idempotency_v1",
    "mutation_versions_v1",
    "mutation_fences_v1",
    "mutation_outbox_v1",
    "mutation_private_payloads_v1",
    "mutation_outbox_topic_index_v1",
    "mutation_outbox_consumers_v1",
    "mutation_outbox_deliveries_v1",
    "mutation_outbox_cursors_v1",
    "mutation_outbox_claim_cursors_v1",
    "mutation_outbox_fairness_v1",
];

/// Operator-supplied identity for a physical authority boundary.
///
/// This is deliberately independent of every logical serving scope. Creating a
/// store therefore cannot accidentally install a synthetic tenant or resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalStoreIdentity {
    name: IncarnationId,
    digest: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalStoreIdentityWire {
    name: IncarnationId,
    digest: [u8; 32],
}

impl PhysicalStoreIdentity {
    pub fn new(name: impl Into<String>) -> Result<Self, String> {
        let name = IncarnationId::new(name)?;
        Ok(Self {
            digest: physical_identity_digest(&name),
            name,
        })
    }

    pub fn name(&self) -> &IncarnationId {
        &self.name
    }

    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.digest != physical_identity_digest(&self.name) {
            return Err("physical store identity digest mismatch".to_string());
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for PhysicalStoreIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PhysicalStoreIdentityWire::deserialize(deserializer)?;
        let identity = Self {
            name: wire.name,
            digest: wire.digest,
        };
        identity.validate().map_err(serde::de::Error::custom)?;
        Ok(identity)
    }
}

/// Closed registry of physical owner-table layouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum OwnerLayout {
    LedgerOnly,
    Rbac,
    Jobs,
    Statechart,
    TimeSeries,
    Kv,
    Blob,
    SemanticIndex,
}

impl OwnerLayout {
    pub const fn canonical_name(self) -> &'static str {
        OWNER_LAYOUT_NAMES[self as usize]
    }

    fn accepts(self, identity: &MutationScopeIdentity) -> bool {
        if self == Self::LedgerOnly {
            return true;
        }
        matches!(
            (self, identity.scope().native_domain()),
            (Self::Rbac, Some(MutationDomain::ControlPlane))
                | (Self::Jobs, Some(MutationDomain::AnalyticsJob))
                | (Self::Statechart, Some(MutationDomain::Lifecycle))
                | (Self::TimeSeries, Some(MutationDomain::TimeSeries))
                | (Self::Kv, Some(MutationDomain::KvStore))
                | (Self::Blob, Some(MutationDomain::BlobStore))
                | (Self::SemanticIndex, Some(MutationDomain::SemanticIndex))
        )
    }

    fn digest(self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(OWNER_LAYOUT_DOMAIN);
        hasher.update(self.canonical_name().as_bytes());
        for table in LEDGER_TABLE_NAMES {
            hash_table_contract(&mut hasher, &table_contract(table, None));
        }
        for table in owner_table_names(self) {
            hash_table_contract(&mut hasher, &table_contract(table, Some(self)));
        }
        hasher.finalize().into()
    }
}

/// Persisted closed-world declaration for one physical store.
use crate::owner_manifest_types::*;
impl OwnerManifest {
    fn new(physical_identity: PhysicalStoreIdentity, layout: OwnerLayout) -> Result<Self, String> {
        physical_identity.validate()?;
        Ok(Self {
            schema_version: MUTATION_STORE_SCHEMA_VERSION,
            physical_identity,
            layout,
            layout_digest: layout.digest(),
            authority_epoch: 0,
            tables: expected_table_contracts(layout),
        })
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != MUTATION_STORE_SCHEMA_VERSION {
            return Err("unsupported mutation owner-manifest schema".to_string());
        }
        self.physical_identity.validate()?;
        if self.layout_digest != self.layout.digest() {
            return Err("mutation owner-layout digest mismatch".to_string());
        }
        if self.tables != expected_table_contracts(self.layout) {
            return Err("mutation owner table registry mismatch".to_string());
        }
        Ok(())
    }

    pub(crate) fn authority_digest(&self, incarnation: &StoreIncarnation) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(OWNER_LAYOUT_DOMAIN);
        hasher.update(incarnation.identity_digest().as_bytes());
        hasher.update(self.physical_identity.digest());
        hasher.update(self.layout_digest);
        hasher.update(self.authority_epoch.to_be_bytes());
        hasher.finalize().into()
    }

    pub(crate) fn digest(&self) -> Result<OwnerManifestDigest, String> {
        self.validate()?;
        let encoded = encode_bounded(self, "mutation owner manifest digest")?;
        let mut hasher = Sha256::new();
        hasher.update(OWNER_MANIFEST_DIGEST_DOMAIN);
        hasher.update(
            u64::try_from(encoded.len())
                .map_err(|_| "mutation owner manifest length overflow".to_string())?
                .to_be_bytes(),
        );
        hasher.update(encoded);
        Ok(OwnerManifestDigest::new(hasher.finalize().into()))
    }
}

pub(crate) fn read_current_manifest(rtx: &ReadTransaction) -> Result<OwnerManifest, String> {
    let table = rtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_manifest(&table)
}

pub(crate) fn validate_manifest_read(
    rtx: &ReadTransaction,
    expected: &PhysicalStoreIdentity,
    layout: OwnerLayout,
) -> Result<OwnerManifest, String> {
    let table = rtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_exact_manifest(&table, expected, layout)
}

pub(crate) fn validate_manifest_write(
    wtx: &WriteTransaction,
    expected: &PhysicalStoreIdentity,
    layout: OwnerLayout,
) -> Result<OwnerManifest, String> {
    let table = wtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_exact_manifest(&table, expected, layout)
}

fn read_exact_manifest<T>(
    table: &T,
    expected: &PhysicalStoreIdentity,
    layout: OwnerLayout,
) -> Result<OwnerManifest, String>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let manifest = read_manifest(table)?;
    if manifest.physical_identity != *expected || manifest.layout != layout {
        return Err("mutation owner manifest does not match requested authority".to_string());
    }
    Ok(manifest)
}

fn read_manifest<T>(table: &T) -> Result<OwnerManifest, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    let mut rows = table.iter().map_err(|error| error.to_string())?;
    let Some(first) = rows.next() else {
        return Err("mutation owner manifest is missing".to_string());
    };
    let (key, value) = first.map_err(|error| error.to_string())?;
    if key.value() != MANIFEST_KEY
        || rows
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("mutation store must contain exactly one owner manifest".to_string());
    }
    let manifest: OwnerManifest = decode_record(value.value())?;
    manifest.validate()?;
    Ok(manifest)
}

fn write_new_manifest(wtx: &WriteTransaction, manifest: &OwnerManifest) -> Result<(), String> {
    let mut table = wtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    if table
        .iter()
        .map_err(|error| error.to_string())?
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("mutation owner manifest already exists".to_string());
    }
    let bytes = encode_bounded(manifest, "mutation owner manifest")?;
    table
        .insert(MANIFEST_KEY, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Create a current-format store. No serving scope is created by this call.
pub fn create(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<MutationStore, String> {
    if path.exists() {
        return Err("mutation store create target already exists".to_string());
    }
    let database = Database::create(path).map_err(|error| error.to_string())?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let manifest = OwnerManifest::new(physical_identity, layout)?;
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let handle = crate::identity::initialize_strict_in(&wtx, &incarnation, &manifest)?;
    open_declared_owner_tables(&wtx, layout)?;
    write_new_manifest(&wtx, &manifest)?;
    wtx.commit().map_err(|error| error.to_string())?;
    Ok(MutationStore::from_parts(
        database,
        handle,
        physical_path,
        private_integrity,
        Some(manifest),
    ))
}

/// Open only an exact current-format store; absent or invalid declarations fail.
pub fn open(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<MutationStore, String> {
    physical_identity.validate()?;
    let database = Database::open(path).map_err(|error| error.to_string())?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let rtx = database.begin_read().map_err(|error| error.to_string())?;
    let manifest = validate_manifest_read(&rtx, &physical_identity, layout)?;
    crate::identity::validate_incarnation_read(&rtx, &incarnation)?;
    validate_declared_owner_tables(&rtx, layout)?;
    let authenticate = |sealed: &[u8], digest: &str| {
        crate::identity::authenticate_with(private_integrity.as_deref(), sealed, digest)
    };
    crate::validate_recovery_content(&incarnation, &rtx, &authenticate)?;
    drop(rtx);
    Ok(MutationStore::from_parts(
        database,
        crate::identity::store_handle(incarnation),
        physical_path,
        private_integrity,
        Some(manifest),
    ))
}

/// Composition-root proof authority. Proof bytes are interpreted only here.
pub trait ScopeGrantVerifier: Send + Sync {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String>;
}

mod sealed {
    pub trait Sealed {}
}

pub trait OwnerDomain: sealed::Sealed {
    const LAYOUT: OwnerLayout;
}

macro_rules! owner_domains {
    ($($owner:ident => $layout:ident),+ $(,)?) => {$(
        #[derive(Debug)]
        pub struct $owner;
        impl sealed::Sealed for $owner {}
        impl OwnerDomain for $owner {
            const LAYOUT: OwnerLayout = OwnerLayout::$layout;
        }
    )+};
}

owner_domains!(
    LedgerOnlyOwner => LedgerOnly,
    RbacOwner => Rbac,
    JobsOwner => Jobs,
    StatechartOwner => Statechart,
    TimeSeriesOwner => TimeSeries,
    KvOwner => Kv,
    BlobOwner => Blob,
    SemanticIndexOwner => SemanticIndex,
);

/// Opaque authenticated authorization for one exact logical serving identity.
pub struct AuthenticatedScopeGrant<D: OwnerDomain> {
    identity: MutationScopeIdentity,
    principal: String,
    authority_digest: [u8; 32],
    _domain: PhantomData<D>,
}

impl MutationStore {
    pub(crate) fn current_owner_manifest(&self) -> Result<OwnerManifest, String> {
        self.validate_physical_root()?;
        let cached = self.strict_manifest()?;
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        let persisted =
            validate_manifest_read(&transaction, &cached.physical_identity, cached.layout)?;
        validate_declared_owner_tables(&transaction, persisted.layout)?;
        if persisted != *cached {
            return Err("cached owner manifest differs from persisted authority".to_string());
        }
        Ok(persisted)
    }

    pub fn authenticate_scope<D: OwnerDomain>(
        &self,
        verifier: &dyn ScopeGrantVerifier,
        identity: MutationScopeIdentity,
        principal: String,
        proof: &[u8],
    ) -> Result<AuthenticatedScopeGrant<D>, String> {
        identity.validate_digest()?;
        let manifest = self.current_owner_manifest()?;
        if manifest.layout != D::LAYOUT || !manifest.layout.accepts(&identity) {
            return Err("serving scope is outside the declared owner layout".to_string());
        }
        verifier.verify(
            &manifest.physical_identity,
            manifest.layout,
            &identity,
            &principal,
            proof,
        )?;
        Ok(AuthenticatedScopeGrant {
            identity,
            principal,
            authority_digest: manifest.authority_digest(self.incarnation()),
            _domain: PhantomData,
        })
    }
}

/// Opaque capability for one authenticated, bound serving scope.
pub struct OwnerHandle<D: OwnerDomain> {
    pub(crate) identity: MutationScopeIdentity,
    pub(crate) principal: String,
    pub(crate) authority_digest: [u8; 32],
    _domain: PhantomData<D>,
}

impl<D: OwnerDomain> OwnerHandle<D> {
    pub fn identity(&self) -> &MutationScopeIdentity {
        &self.identity
    }
}

pub fn bind_serving_scope<D: OwnerDomain>(
    store: &MutationStore,
    grant: AuthenticatedScopeGrant<D>,
    initial_version: u64,
) -> Result<OwnerHandle<D>, String> {
    let manifest = store.current_owner_manifest()?;
    if manifest.layout != D::LAYOUT
        || manifest.authority_digest(store.incarnation()) != grant.authority_digest
        || !manifest.layout.accepts(&grant.identity)
    {
        return Err("authenticated serving grant does not match this store".to_string());
    }
    let write = store.write()?;
    crate::identity::bind_scope_in(
        &write.handle,
        write.transaction(),
        &grant.identity,
        initial_version,
    )?;
    write.commit()?;
    Ok(OwnerHandle {
        identity: grant.identity,
        principal: grant.principal,
        authority_digest: grant.authority_digest,
        _domain: PhantomData,
    })
}

/// Consuming owner-write gate. Dropping it unfinished poisons the outer write.
///
/// ```compile_fail
/// # use eg_mutation_store::{AdmittedOwnerWrite, KvOwner};
/// fn leaks_transaction(token: &AdmittedOwnerWrite<'_, KvOwner>) {
///     let _ = token.transaction();
/// }
/// ```
pub struct AdmittedOwnerWrite<'a, D: OwnerDomain> {
    pub(crate) write: &'a MutationWrite,
    pub(crate) identity: MutationScopeIdentity,
    finished: bool,
    _domain: PhantomData<D>,
}

impl MutationWrite {
    pub fn begin_owner<'a, D: OwnerDomain>(
        &'a self,
        owner: &OwnerHandle<D>,
        batch: &MutationBatch,
    ) -> Result<AdmittedOwnerWrite<'a, D>, String> {
        let manifest = self.strict_manifest()?;
        let persisted = validate_manifest_write(
            self.transaction(),
            &manifest.physical_identity,
            manifest.layout,
        )?;
        if persisted != *manifest
            || manifest.layout != D::LAYOUT
            || manifest.authority_digest(&self.handle.incarnation) != owner.authority_digest
            || owner.identity != batch.identity
            || owner.principal != batch.context.principal
        {
            return Err("owner write capability does not match admitted batch".to_string());
        }
        binding_for_write(self, &batch.identity)?;
        self.open_owner_admission(batch, D::LAYOUT)?;
        Ok(AdmittedOwnerWrite {
            write: self,
            identity: batch.identity.clone(),
            finished: false,
            _domain: PhantomData,
        })
    }
}

impl<D: OwnerDomain> AdmittedOwnerWrite<'_, D> {
    pub fn finish_owner(mut self) -> Result<(), String> {
        self.write.finish_owner_admission(D::LAYOUT)?;
        self.finished = true;
        Ok(())
    }
}

impl<D: OwnerDomain> Drop for AdmittedOwnerWrite<'_, D> {
    fn drop(&mut self) {
        if !self.finished {
            self.write.poison_owner_admission();
        }
    }
}

const CAP_READ: u16 = 1;
const CAP_INSERT: u16 = 1 << 1;
const CAP_UPDATE: u16 = 1 << 2;
const CAP_DELETE: u16 = 1 << 3;
const CAP_CAS: u16 = 1 << 4;

fn expected_table_contracts(layout: OwnerLayout) -> Vec<TableContract> {
    LEDGER_TABLE_NAMES
        .iter()
        .map(|name| table_contract(name, None))
        .chain(
            owner_table_names(layout)
                .iter()
                .map(|name| table_contract(name, Some(layout))),
        )
        .collect()
}

pub(crate) fn table_contract(name: &str, owner: Option<OwnerLayout>) -> TableContract {
    let ownership = if owner.is_some() {
        TableOwnership::Owner
    } else {
        TableOwnership::Ledger
    };
    let index = matches!(
        name,
        "mutation_outbox_topic_index_v1"
            | "analytics_job_ready_by_priority"
            | "analytics_job_ready_by_capability"
            | "analytics_job_scheduler_meta"
            | "analytics_job_lease_by_worker"
            | "analytics_job_lease_by_expiry"
            | "analytics_job_active_totals_by_tenant"
            | "analytics_job_by_deadline"
            | "analytics_job_cancellation_reconcile"
            | "semantic_binding_heads_v1"
            | "semantic_lexical_manifests_v1"
            | "semantic_ann_manifests_v1"
            | "semantic_vectors_v1"
    );
    let shared = matches!(name, "cas_chunks" | "cas_refcount");
    let key_type_id = key_type_id(name);
    let value_type_id = value_type_id(name);
    TableContract {
        table_id: name.to_string(),
        schema_id: format!("eg.redb.{name}.v1"),
        key_type_id: key_type_id.to_string(),
        value_type_id: value_type_id.to_string(),
        key_codec: "redb-native-key-v1".to_string(),
        value_codec: "redb-native-value-v1".to_string(),
        logical_schema_id: format!("eg.logical.{name}.v1"),
        logical_codec_id: logical_codec_id(name).to_string(),
        ownership,
        domain: owner.map(|layout| OWNER_LAYOUT_DOMAINS[layout as usize]),
        scope: if shared {
            TableScope::SharedService
        } else if matches!(
            owner,
            Some(OwnerLayout::Rbac | OwnerLayout::Jobs | OwnerLayout::Statechart | OwnerLayout::Kv)
        ) {
            TableScope::StorePrivate
        } else if owner.is_some()
            || !matches!(
                name,
                "mutation_store_root_v1" | "mutation_owner_manifest_v1"
            )
        {
            TableScope::Serving
        } else {
            TableScope::Physical
        },
        capabilities: table_capabilities(name),
        index,
        derived: index,
    }
}

fn key_type_id(name: &str) -> &'static str {
    ledger_key_type(name)
        .or_else(|| owner_key_type(name))
        .or_else(|| semantic_key_type(name))
        .unwrap_or_else(|| unreachable!("table outside closed owner manifest: {name}"))
}

fn ledger_key_type(name: &str) -> Option<&'static str> {
    match name {
        "mutation_outbox_topic_index_v1" => Some("(&str,&str,u64,u64,&str,u32)"),
        "mutation_outbox_deliveries_v1" => Some("(&str,&str,&str,u32)"),
        "mutation_outbox_v1" => Some("(&str,&str,u32)"),
        "mutation_batches_v1"
        | "mutation_idempotency_v1"
        | "mutation_private_payloads_v1"
        | "mutation_outbox_consumers_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1" => Some("(&str,&str)"),
        "mutation_store_root_v1"
        | "mutation_scope_bindings_v1"
        | "mutation_owner_manifest_v1"
        | "mutation_versions_v1"
        | "mutation_fences_v1" => Some("&str"),
        _ => None,
    }
}

fn owner_key_type(name: &str) -> Option<&'static str> {
    jobs_key_type(name).or_else(|| domain_owner_key_type(name))
}

fn jobs_key_type(name: &str) -> Option<&'static str> {
    match name {
        "analytics_job_ready_by_capability" => Some("(&str,u32,i64,&str)"),
        "analytics_job_ready_by_priority" => Some("(u32,i64,&str)"),
        "analytics_job_lease_by_expiry" | "analytics_job_by_deadline" => Some("(i64,&str)"),
        "analytics_jobs"
        | "analytics_job_committed_results"
        | "job_intents"
        | "job_idempotency_ledger"
        | "analytics_job_knowledge_batches"
        | "analytics_job_scheduler_meta"
        | "analytics_job_lease_by_worker"
        | "analytics_job_active_totals_by_tenant"
        | "analytics_job_cancellation_reconcile" => Some("&str"),
        _ => None,
    }
}

fn domain_owner_key_type(name: &str) -> Option<&'static str> {
    match name {
        "series_chunks" => Some("(&str,u64)"),
        "kv" | "cas_blobs" => Some("(&str,&str)"),
        "cas_uploads" => Some("(&str,u64)"),
        "cas_chunks" | "cas_refcount" => Some("&str"),
        "rbac_v1"
        | "statechart_defs"
        | "statechart_instances"
        | "series_meta"
        | "series_projection_state" => Some("&str"),
        _ => None,
    }
}

fn semantic_key_type(name: &str) -> Option<&'static str> {
    match name {
        "semantic_source_progress_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1"
        | "semantic_vectors_v1" => Some("(&str,&str,u64,&str)"),
        "semantic_stage_transitions_v1" => Some("(&str,&str,&str)"),
        "semantic_dead_letters_v1" => Some("(&str,&str,u32)"),
        "semantic_bindings_v1"
        | "semantic_tombstones_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1" => Some("(&str,&str,u64)"),
        "semantic_binding_heads_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_active_pointers_v1" => Some("(&str,&str)"),
        _ => None,
    }
}
fn value_type_id(name: &str) -> &'static str {
    match name {
        "mutation_versions_v1"
        | "analytics_job_scheduler_meta"
        | "cas_refcount"
        | "semantic_binding_heads_v1" => "u64",
        "analytics_job_active_totals_by_tenant" => "(u64,u64)",
        "mutation_outbox_topic_index_v1"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile" => "()",
        "mutation_idempotency_v1"
        | "mutation_outbox_consumers_v1"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_lease_by_worker" => "&str",
        "mutation_store_root_v1"
        | "mutation_scope_bindings_v1"
        | "mutation_owner_manifest_v1"
        | "mutation_batches_v1"
        | "mutation_fences_v1"
        | "mutation_outbox_v1"
        | "mutation_private_payloads_v1"
        | "mutation_outbox_deliveries_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "rbac_v1"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_knowledge_batches"
        | "statechart_defs"
        | "statechart_instances"
        | "series_chunks"
        | "series_meta"
        | "series_projection_state"
        | "kv"
        | "cas_chunks"
        | "cas_blobs"
        | "cas_uploads"
        | "semantic_bindings_v1"
        | "semantic_stage_transitions_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_source_progress_v1"
        | "semantic_active_pointers_v1"
        | "semantic_dead_letters_v1"
        | "semantic_tombstones_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1"
        | "semantic_vectors_v1" => "&[u8]",
        _ => unreachable!("table outside closed owner manifest: {name}"),
    }
}

fn logical_codec_id(name: &str) -> &'static str {
    match name {
        "mutation_private_payloads_v1" => "authenticated-sealed-bytes-v1",
        "rbac_v1" => "json-utf8-v1",
        "kv" | "cas_chunks" => "raw-bytes-v1",
        "series_chunks" => "packed-timeseries-chunk-v1",
        "mutation_idempotency_v1"
        | "mutation_versions_v1"
        | "mutation_outbox_topic_index_v1"
        | "mutation_outbox_consumers_v1"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_scheduler_meta"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_worker"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_active_totals_by_tenant"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile"
        | "cas_refcount"
        | "semantic_binding_heads_v1" => "redb-scalar-v1",
        "semantic_bindings_v1"
        | "semantic_stage_transitions_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_source_progress_v1"
        | "semantic_active_pointers_v1"
        | "semantic_dead_letters_v1"
        | "semantic_tombstones_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1"
        | "semantic_vectors_v1" => "semantic-index-bytes-v1",
        "mutation_store_root_v1"
        | "mutation_scope_bindings_v1"
        | "mutation_owner_manifest_v1"
        | "mutation_batches_v1"
        | "mutation_fences_v1"
        | "mutation_outbox_v1"
        | "mutation_outbox_deliveries_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_knowledge_batches"
        | "statechart_defs"
        | "statechart_instances"
        | "series_meta"
        | "series_projection_state"
        | "cas_blobs"
        | "cas_uploads" => "msgpack-v1",
        _ => unreachable!("table outside closed owner manifest: {name}"),
    }
}

fn table_capabilities(name: &str) -> u16 {
    match name {
        "mutation_store_root_v1" | "mutation_owner_manifest_v1" => {
            CAP_READ | CAP_INSERT | CAP_UPDATE
        }
        "mutation_idempotency_v1"
        | "mutation_outbox_v1"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_knowledge_batches"
        | "statechart_defs" => CAP_READ | CAP_INSERT,
        "cas_chunks" | "cas_blobs" => CAP_READ | CAP_INSERT | CAP_DELETE,
        "rbac_v1"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_scheduler_meta"
        | "statechart_instances" => CAP_READ | CAP_INSERT | CAP_UPDATE,
        "mutation_private_payloads_v1"
        | "mutation_outbox_topic_index_v1"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_worker"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile"
        | "semantic_binding_heads_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1"
        | "semantic_vectors_v1" => CAP_READ | CAP_INSERT | CAP_DELETE,
        "mutation_versions_v1" | "mutation_fences_v1" | "kv" | "cas_refcount" => {
            CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE | CAP_CAS
        }
        "analytics_job_active_totals_by_tenant"
        | "series_chunks"
        | "series_meta"
        | "series_projection_state"
        | "cas_uploads" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        "mutation_scope_bindings_v1"
        | "mutation_batches_v1"
        | "mutation_outbox_consumers_v1"
        | "mutation_outbox_deliveries_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "semantic_bindings_v1"
        | "semantic_stage_transitions_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_source_progress_v1"
        | "semantic_active_pointers_v1"
        | "semantic_dead_letters_v1"
        | "semantic_tombstones_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        _ => unreachable!("table outside closed owner manifest: {name}"),
    }
}

use crate::owner_registry::*;
#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;
