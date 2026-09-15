//! Live governed document/image/audio/video serving boundary.
//!
//! Authority is derived from the verified request context, never from caller
//! fields. Source bytes exist only for the duration of native decoding. The only
//! graph value written here is an AEAD-sealed `ServedModalityRuntime` snapshot at
//! an HMAC-derived node id; ordinary graph reads therefore expose neither source
//! content nor cross-policy runtime state even if they enumerate internal nodes.

use std::fmt;

use hmac::{Hmac, Mac};
use serde::{de::DeserializeOwned, Serialize};
use sha2::Sha256;

use crate::crypto::ValueCipher;
use crate::graph::GraphCore;
use crate::protocol::ResultPayload;
use eg_modality::{
    tck_report, ApplyDisposition, ApplyOutcome, ArtifactBundle, Classification,
    ConformanceTestable, GovernedModality, ModalityKind, MutationDelta, NativePredicate,
    OccurrenceId, OpaqueRef, SegmentKind, ServedDelete, ServedIngest, ServedModalityRuntime,
    ServedNativeQuery, ServedPolicyScope, ServedQuery, ServedRecord,
};
use eg_types::acl::RequestContextClaims;
use eg_types::{
    ServedModalityIngestItem, ServedModalityKind, ServedModalityOp, ServedNativePredicate,
    ServedSegmentKind,
};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const HARD_MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_BUNDLE_BYTES: usize = 4 * 1024 * 1024;
const HARD_MAX_BUNDLE_BYTES: usize = 32 * 1024 * 1024;
/// Largest ingest stream whose worst-case canonical `ApplyOutcome` response
/// (including `u64::MAX` counters) fits the Raft codec's 4096-byte envelope.
pub(crate) const MAX_INGEST_STREAM_ITEMS: usize = 61;
const MIN_PRIVACY_PROBE_BYTES: usize = 16;

mod dispatch;
mod ingest;
mod migration;
mod response;

use ingest::ingest_stream;

/// Verified, privacy-safe authority for one request. Raw tenant, subject, policy,
/// role, scope, and delegation values never leave request memory.
#[derive(Clone, Debug)]
pub(crate) struct ModalityAuthority {
    scope: ServedPolicyScope,
    can_manage: bool,
    partition_token: String,
    lexeme_key: [u8; 32],
    cipher: ValueCipher,
}

impl ModalityAuthority {
    pub(crate) fn from_verified(
        server_secret: &str,
        claims: &RequestContextClaims,
    ) -> Result<Self, String> {
        if server_secret.is_empty() || claims.tenant.is_empty() || claims.policy_version.is_empty()
        {
            return Err("verified modality authority is incomplete".to_string());
        }
        let tenant_token = keyed_token(server_secret, "tenant", &[&claims.tenant])?;
        let policy_token = keyed_token(
            server_secret,
            "access-policy",
            &[&claims.tenant, &claims.policy_version],
        )?;
        let purpose_token = keyed_token(server_secret, "purpose", &["served-modality-v1"])?;
        let partition_token = keyed_token(
            server_secret,
            "partition",
            &[&claims.tenant, &claims.policy_version, "served-modality-v1"],
        )?;
        let key_material = keyed_bytes(
            server_secret,
            "state-key",
            &[&claims.tenant, &claims.policy_version, "served-modality-v1"],
        )?;
        let lexeme_key = keyed_bytes(
            server_secret,
            "lexeme-key",
            &[&claims.tenant, &claims.policy_version, "served-modality-v1"],
        )?;
        let maximum_classification = if has_scope(claims, "kg:admin")
            || has_scope(claims, "*")
            || has_scope(claims, "modality:classification:restricted")
        {
            Classification::Restricted
        } else if has_scope(claims, "modality:classification:confidential") {
            Classification::Confidential
        } else {
            Classification::Internal
        };
        let can_manage = has_scope(claims, "kg:admin") || has_scope(claims, "*");
        Ok(Self {
            scope: ServedPolicyScope {
                tenant_ref: OpaqueRef::scoped("tenant", &tenant_token)
                    .map_err(|_| "failed to derive modality authority".to_string())?,
                access_policy_ref: OpaqueRef::scoped("access-policy", &policy_token)
                    .map_err(|_| "failed to derive modality authority".to_string())?,
                purpose_ref: OpaqueRef::scoped("purpose", &purpose_token)
                    .map_err(|_| "failed to derive modality authority".to_string())?,
                maximum_classification,
            },
            can_manage,
            partition_token,
            lexeme_key,
            cipher: ValueCipher::from_key_material(&key_material),
        })
    }

    pub(crate) fn node_id(&self, modality: ServedModalityKind) -> String {
        format!(
            "__eg_internal_served_{}_{}",
            modality_name(modality),
            self.partition_token
        )
    }

    fn lexeme_ref(&self, term: &str) -> Result<OpaqueRef, String> {
        let normalized = normalize_lexeme(term)?;
        let mut mac = HmacSha256::new_from_slice(&self.lexeme_key)
            .map_err(|_| "failed to derive modality query".to_string())?;
        mac.update(normalized.as_bytes());
        OpaqueRef::scoped("lexeme", &hex::encode(mac.finalize().into_bytes()))
            .map_err(|_| "failed to derive modality query".to_string())
    }

    fn require_management(&self) -> Result<(), String> {
        if self.can_manage {
            Ok(())
        } else {
            Err("modality management scope is required".to_string())
        }
    }
}

fn normalize_lexeme(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 || !value.chars().all(char::is_alphanumeric) {
        return Err("invalid document lexical query".to_string());
    }
    Ok(value.to_lowercase())
}

fn has_scope(claims: &RequestContextClaims, expected: &str) -> bool {
    claims.scopes.iter().any(|scope| scope == expected)
}

fn keyed_bytes(secret: &str, domain: &str, values: &[&str]) -> Result<[u8; 32], String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| "failed to derive modality authority".to_string())?;
    mac.update(domain.as_bytes());
    for value in values {
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value.as_bytes());
    }
    Ok(mac.finalize().into_bytes().into())
}

fn keyed_token(secret: &str, domain: &str, values: &[&str]) -> Result<String, String> {
    Ok(hex::encode(keyed_bytes(secret, domain, values)?))
}

fn env_limit(name: &str, default: usize, hard_max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
        .min(hard_max)
}

fn validate_request_sizes(bundle: &[u8], source: &[u8]) -> Result<(), String> {
    let bundle_limit = env_limit(
        "EPISTEMIC_GRAPH_MODALITY_MAX_BUNDLE_BYTES",
        DEFAULT_MAX_BUNDLE_BYTES,
        HARD_MAX_BUNDLE_BYTES,
    );
    let source_limit = env_limit(
        "EPISTEMIC_GRAPH_MODALITY_MAX_SOURCE_BYTES",
        DEFAULT_MAX_SOURCE_BYTES,
        HARD_MAX_SOURCE_BYTES,
    );
    if bundle.is_empty() || bundle.len() > bundle_limit {
        return Err("governed modality bundle exceeds the configured resource limit".to_string());
    }
    eg_types::msgpack::validate_single_value(
        bundle,
        eg_types::msgpack::MsgpackLimits::new(bundle_limit, 250_000, 64),
    )
    .map_err(|_| "invalid or over-complex governed modality bundle".to_string())?;
    if source.is_empty() || source.len() > source_limit {
        return Err("modality source exceeds the configured resource limit".to_string());
    }
    Ok(())
}

fn modality_name(modality: ServedModalityKind) -> &'static str {
    match modality {
        ServedModalityKind::Document => "document",
        ServedModalityKind::Image => "image",
        ServedModalityKind::Audio => "audio",
        ServedModalityKind::Video => "video",
    }
}

fn modality_kind(modality: ServedModalityKind) -> ModalityKind {
    match modality {
        ServedModalityKind::Document => ModalityKind::Document,
        ServedModalityKind::Image => ModalityKind::Image,
        ServedModalityKind::Audio => ModalityKind::Audio,
        ServedModalityKind::Video => ModalityKind::Video,
    }
}

fn segment_kind(kind: ServedSegmentKind) -> Result<SegmentKind, String> {
    response::transcode(kind).map_err(|error| format!("invalid served segment kind: {error}"))
}

fn opaque(value: String) -> Result<OpaqueRef, String> {
    OpaqueRef::new(value).map_err(|_| "invalid opaque modality reference".to_string())
}

fn occurrence(value: String) -> Result<OccurrenceId, String> {
    OccurrenceId::from_opaque(opaque(value)?)
        .map_err(|_| "invalid opaque occurrence reference".to_string())
}

fn load_runtime<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
) -> Result<ServedModalityRuntime<T>, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let Some(sealed) = core.get_node_properties(&authority.node_id(modality)) else {
        return Ok(ServedModalityRuntime::new());
    };
    if !crate::crypto::is_sealed(&sealed) {
        return Err("unsealed modality runtime state rejected".to_string());
    }
    let snapshot = authority
        .cipher
        .unseal(&sealed)
        .map_err(|_| "modality runtime state could not be authenticated".to_string())?;
    ServedModalityRuntime::recover(&snapshot).map_err(|error| error.to_string())
}

fn store_runtime<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    runtime: &ServedModalityRuntime<T>,
) -> Result<(), String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let snapshot = runtime.snapshot().map_err(|error| error.to_string())?;
    store_runtime_snapshot(core, authority, modality, &snapshot);
    Ok(())
}

fn store_runtime_snapshot(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    snapshot: &[u8],
) {
    let sealed = authority.cipher.seal(snapshot);
    core.add_node(authority.node_id(modality), sealed);
}

fn store_runtime_excluding_sources<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    runtime: &ServedModalityRuntime<T>,
    sources: &[Vec<u8>],
) -> Result<(), String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let snapshot = runtime.snapshot().map_err(|error| error.to_string())?;
    if sources.iter().any(|source| {
        source.len() >= MIN_PRIVACY_PROBE_BYTES
            && snapshot
                .windows(source.len())
                .any(|candidate| candidate == source.as_slice())
    }) {
        return Err("raw source would enter modality snapshot".to_string());
    }
    store_runtime_snapshot(core, authority, modality, &snapshot);
    Ok(())
}

/// One durable row/event/idempotency-entry/manifest write from a single
/// occurrence mutation (BUG-017 frozen format, `eg_modality::delta_store`) —
/// bounded by that ONE occurrence, never by partition corpus. This is a
/// SHADOW write: called alongside (not instead of) `store_runtime`/
/// `store_runtime_excluding_sources` above, which remain the sole read
/// authority (`load_runtime`) this pass. See `capture_deltas`'s doc for why
/// the read-side cutover is out of scope here.
fn store_delta<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    delta: &MutationDelta<T>,
) -> Result<(), String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let partition = authority.node_id(modality);
    let record_bytes =
        serde_json::to_vec(&delta.record).map_err(|_| "modality row codec failure".to_string())?;
    core.add_node(
        delta_row_node_id(&partition, "record", delta.occurrence_id.as_ref().as_str()),
        authority.cipher.seal(&record_bytes),
    );
    let event_bytes =
        serde_json::to_vec(&delta.event).map_err(|_| "modality row codec failure".to_string())?;
    core.add_node(
        delta_row_node_id(&partition, "event", &delta.event.sequence.to_string()),
        authority.cipher.seal(&event_bytes),
    );
    // `None` for a `move_to_cold`/`restore` lifecycle transition
    // (`MutationDelta::capture_lifecycle`) — those carry no idempotency_ref
    // on the wire, so there is no entry row to write. See
    // `delta_store::MutationDelta::idempotency`'s doc for why this is a
    // strict subset of the ingest/delete row shape, not a format change.
    if let Some((idempotency_key, idempotency_entry)) = &delta.idempotency {
        let idempotency_bytes = serde_json::to_vec(idempotency_entry)
            .map_err(|_| "modality row codec failure".to_string())?;
        core.add_node(
            delta_row_node_id(&partition, "idem", idempotency_key.as_str()),
            authority.cipher.seal(&idempotency_bytes),
        );
    }
    let manifest_bytes = serde_json::to_vec(&DeltaManifest {
        next_sequence: delta.next_sequence,
    })
    .map_err(|_| "modality row codec failure".to_string())?;
    core.add_node(
        delta_row_node_id(&partition, "manifest", ""),
        authority.cipher.seal(&manifest_bytes),
    );
    Ok(())
}

fn delta_row_node_id(partition: &str, kind: &str, key: &str) -> String {
    format!("{partition}_row_{kind}_{key}")
}

/// The tiny (O(1)) per-partition counter a delta write persists alongside the
/// row, so a row-backed reconstruction knows the next event sequence to
/// allocate without scanning every event row. NOT GOC-03's commit sequence —
/// see `served.rs`/`delta_store.rs` module docs.
#[derive(Serialize, serde::Deserialize)]
struct DeltaManifest {
    next_sequence: u64,
}

/// Snapshot each command's PRE-mutation record (`None` for a fresh occurrence)
/// so `capture_deltas` can diff index membership after the batch applies —
/// `ServedModalityRuntime::ingest`'s `remove_from_indexes`/`add_to_indexes`
/// mutate the live index `BTreeSet`s in place and leave no trace of what was
/// removed.
fn capture_befores<T>(
    runtime: &ServedModalityRuntime<T>,
    commands: &[ServedIngest<T>],
) -> Vec<(OccurrenceId, OpaqueRef, Option<ServedRecord<T>>)>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    commands
        .iter()
        .map(|command| {
            (
                command.target_occurrence_id.clone(),
                command.idempotency_ref.clone(),
                runtime.record(&command.target_occurrence_id).cloned(),
            )
        })
        .collect()
}

/// Shadow-write the delta for every command in a just-applied batch. Skips a
/// command whose disposition was `IdempotentReplay` — a replay durably
/// changed nothing new (`MutationDelta::capture` returns `None` for it).
fn shadow_write_deltas<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    runtime: &ServedModalityRuntime<T>,
    before_records: &[(OccurrenceId, OpaqueRef, Option<ServedRecord<T>>)],
    outcomes: &[ApplyOutcome],
) -> Result<(), String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    for ((occurrence_id, idempotency_key, before), outcome) in
        before_records.iter().zip(outcomes.iter())
    {
        let is_replay = outcome.disposition == ApplyDisposition::IdempotentReplay;
        if let Some(delta) = MutationDelta::capture(
            runtime,
            before.as_ref(),
            occurrence_id,
            idempotency_key,
            is_replay,
        ) {
            store_delta(core, authority, modality, &delta)?;
        }
    }
    Ok(())
}

fn validate_content_binding(
    bundle: &ArtifactBundle,
    target: &OccurrenceId,
    modality: ServedModalityKind,
    content_hash: &str,
    authority: &ModalityAuthority,
) -> Result<(), String> {
    // A query returns the record's complete certified bundle. Accepting a bundle
    // with even one occurrence outside this authority would therefore turn an
    // authorized target into a carrier for cross-policy metadata.
    if bundle
        .occurrences
        .iter()
        .any(|occurrence| !authority.scope.authorizes_occurrence(occurrence))
    {
        return Err("modality operation forbidden".to_string());
    }
    let observed = bundle
        .occurrences
        .iter()
        .find(|candidate| candidate.id.as_ref() == target.as_ref())
        .ok_or_else(|| "invalid governed modality bundle".to_string())?;
    if !authority.scope.authorizes_occurrence(observed) {
        return Err("modality operation forbidden".to_string());
    }
    let artifact = bundle
        .artifacts
        .iter()
        .find(|candidate| candidate.id.as_ref() == observed.artifact_id.as_ref())
        .ok_or_else(|| "invalid governed modality bundle".to_string())?;
    let bound_hash = artifact
        .content_ref
        .as_str()
        .rsplit(':')
        .next()
        .unwrap_or_default();
    if artifact.modality != modality_kind(modality)
        || artifact.content_ref.namespace() != "content"
        || bound_hash != content_hash
    {
        return Err("certified artifact does not match decoded source".to_string());
    }
    Ok(())
}

fn ingest(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    item: ServedModalityIngestItem,
) -> Result<ResultPayload, String> {
    let mut outcomes = ingest_stream(core, authority, modality, vec![item])?;
    let outcome = outcomes
        .pop()
        .ok_or_else(|| "single modality ingest produced no outcome".to_string())?;
    response::encode_ingest_result(outcome)
}

fn query<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    requested_segment: Option<ServedSegmentKind>,
    after: Option<String>,
    limit: usize,
    include_cold: bool,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let page = runtime
        .query(&ServedQuery {
            scope: authority.scope.clone(),
            modality: Some(modality_kind(modality)),
            segment_kind: requested_segment.map(segment_kind).transpose()?,
            after: after.map(occurrence).transpose()?,
            limit,
            include_cold,
        })
        .map_err(|error| error.to_string())?;
    response::encode_query_page(page)
}

fn native_query<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    predicate: NativePredicate,
    after: Option<String>,
    limit: usize,
    include_cold: bool,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let (page, _) = runtime
        .query_native(&ServedNativeQuery {
            scope: authority.scope.clone(),
            predicate,
            after: after.map(occurrence).transpose()?,
            limit,
            include_cold,
        })
        .map_err(|error| error.to_string())?;
    response::encode_native_query_page(page)
}

fn native_predicate(
    authority: &ModalityAuthority,
    predicate: ServedNativePredicate,
) -> Result<(ServedModalityKind, NativePredicate), String> {
    Ok(match predicate {
        ServedNativePredicate::DocumentLexical { term, page } => (
            ServedModalityKind::Document,
            NativePredicate::DocumentLexeme {
                lexeme_ref: authority.lexeme_ref(&term)?,
                page,
            },
        ),
        ServedNativePredicate::ImageRegion {
            x,
            y,
            width,
            height,
        } => (
            ServedModalityKind::Image,
            NativePredicate::ImageRegion {
                x,
                y,
                width,
                height,
            },
        ),
        ServedNativePredicate::ImagePerceptualHash {
            hash,
            maximum_distance,
        } => (
            ServedModalityKind::Image,
            NativePredicate::ImagePerceptualHash {
                hash,
                maximum_distance,
            },
        ),
        ServedNativePredicate::AudioWindow {
            start_ms,
            end_ms,
            minimum_rms,
        } => (
            ServedModalityKind::Audio,
            NativePredicate::AudioWindow {
                start_ms,
                end_ms,
                minimum_rms,
            },
        ),
        ServedNativePredicate::VideoWindow {
            start_ms,
            end_ms,
            keyframes_only,
        } => (
            ServedModalityKind::Video,
            NativePredicate::VideoWindow {
                start_ms,
                end_ms,
                keyframes_only,
            },
        ),
    })
}

fn delete<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    idempotency_ref: String,
    occurrence_id: String,
    expected_version: u64,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let mut runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let idempotency_key = opaque(idempotency_ref)?;
    let occurrence_id = occurrence(occurrence_id)?;
    let before = runtime.record(&occurrence_id).cloned();
    let outcome = runtime
        .delete(
            &authority.scope,
            ServedDelete {
                idempotency_ref: idempotency_key.clone(),
                occurrence_id: occurrence_id.clone(),
                expected_version,
            },
        )
        .map_err(|error| error.to_string())?;
    store_runtime(core, authority, modality, &runtime)?;
    if let Some(delta) = MutationDelta::capture(
        &runtime,
        before.as_ref(),
        &occurrence_id,
        &idempotency_key,
        outcome.disposition == ApplyDisposition::IdempotentReplay,
    ) {
        store_delta(core, authority, modality, &delta)?;
    }
    response::encode_delete_result(outcome)
}

fn lifecycle<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    occurrence_id: String,
    restore: bool,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let mut runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let id = occurrence(occurrence_id)?;
    let before = runtime.record(&id).cloned();
    let outcome = if restore {
        runtime.restore(&authority.scope, &id)
    } else {
        runtime.move_to_cold(&authority.scope, &id)
    }
    .map_err(|error| error.to_string())?;
    store_runtime(core, authority, modality, &runtime)?;
    // BUG-017 shadow-row coverage (see `MutationDelta::capture_lifecycle`'s
    // doc): `move_to_cold`/`restore` carry no idempotency_ref, so `capture`
    // cannot be reused here.
    if let Some(delta) = MutationDelta::capture_lifecycle(&runtime, before.as_ref(), &id) {
        store_delta(core, authority, modality, &delta)?;
    }
    response::encode_lifecycle_result(outcome, restore)
}

fn events<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    after_sequence: u64,
    limit: usize,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    let runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    response::encode_events(runtime.events_after_authorized(
        &authority.scope,
        after_sequence,
        limit,
    ))
}

fn stats<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    authority.require_management()?;
    let runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let stats = runtime.stats().map_err(|error| error.to_string())?;
    response::encode_runtime_stats(stats)
}

fn collect_tombstones<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    through_event_sequence: u64,
) -> Result<ResultPayload, String>
where
    T: GovernedModality + Clone + PartialEq + fmt::Debug + Serialize + DeserializeOwned,
{
    authority.require_management()?;
    if through_event_sequence == 0 {
        return Err("modality tombstone collection fence must be positive".to_string());
    }
    let mut runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let collected = runtime.collect_tombstones(&authority.scope, through_event_sequence);
    store_runtime(core, authority, modality, &runtime)?;
    response::encode_tombstone_collection(collected)
}

fn capabilities<T: ConformanceTestable>() -> Result<ResultPayload, String> {
    let report = tck_report::<T>();
    if !report.is_production_ready() || report.pass_count() != 12 || report.na_count() != 0 {
        return Err("served modality failed the component TCK".to_string());
    }
    response::encode_capability_report(report.pass_count(), report.na_count(), 12)
}

/// Execute one graph-scoped served operation. Mutating calls are invoked against
/// the mutation gateway's staged `GraphCore`; read calls receive the live snapshot.
pub(crate) fn handle(
    core: &GraphCore,
    authority: &ModalityAuthority,
    op: ServedModalityOp,
) -> Result<ResultPayload, String> {
    dispatch::handle(core, authority, op)
}

#[cfg(test)]
mod tests {
    use super::migration::migrate_partition_to_rows;
    use super::*;
    use eg_audio::{runtime::NativeAudioRuntime, AudioData};
    use eg_document::{runtime::NativeDocumentRuntime, DocumentData};
    use eg_document::{DocumentDecoder, NativeTextDecoder};
    use eg_image::{runtime::NativeImageRuntime, ImageData};
    use eg_modality::EvidenceAddress;
    use eg_video::{runtime::NativeVideoRuntime, VideoData};

    fn wav_fixture() -> Vec<u8> {
        let samples: [i16; 8] = [0, 8_000, 16_000, 8_000, 0, -8_000, -16_000, -8_000];
        let data_len = (samples.len() * 2) as u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&(36 + data_len).to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&16u16.to_le_bytes());
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&data_len.to_le_bytes());
        for sample in samples {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        bytes
    }

    fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&(body.len() as u32 + 8).to_be_bytes());
        output.extend_from_slice(kind);
        output.extend_from_slice(body);
        output
    }

    fn mp4_fixture() -> Vec<u8> {
        let mut ftyp = Vec::new();
        ftyp.extend_from_slice(b"isom");
        ftyp.extend_from_slice(&0u32.to_be_bytes());
        ftyp.extend_from_slice(b"isom");
        let ftyp = boxed(b"ftyp", &ftyp);
        let mdat = boxed(b"mdat", &[0, 1, 2, 3, 4, 5]);
        let sample_offset = (ftyp.len() + 8) as u32;

        let mut mvhd = vec![0; 100];
        mvhd[12..16].copy_from_slice(&1_000u32.to_be_bytes());
        mvhd[16..20].copy_from_slice(&1_000u32.to_be_bytes());
        let mut tkhd = vec![0; 84];
        tkhd[12..16].copy_from_slice(&1u32.to_be_bytes());
        let mut mdhd = vec![0; 24];
        mdhd[12..16].copy_from_slice(&1_000u32.to_be_bytes());
        mdhd[16..20].copy_from_slice(&1_000u32.to_be_bytes());
        let mut hdlr = vec![0; 24];
        hdlr[8..12].copy_from_slice(b"vide");

        let mut sample_body = vec![0; 78];
        sample_body[6..8].copy_from_slice(&1u16.to_be_bytes());
        sample_body[24..26].copy_from_slice(&2u16.to_be_bytes());
        sample_body[26..28].copy_from_slice(&1u16.to_be_bytes());
        sample_body[40..42].copy_from_slice(&1u16.to_be_bytes());
        sample_body[74..76].copy_from_slice(&24u16.to_be_bytes());
        sample_body[76..78].copy_from_slice(&u16::MAX.to_be_bytes());
        let mut sample_entry = Vec::new();
        sample_entry.extend_from_slice(&(sample_body.len() as u32 + 8).to_be_bytes());
        sample_entry.extend_from_slice(b"raw ");
        sample_entry.extend_from_slice(&sample_body);
        let mut stsd = vec![0; 4];
        stsd.extend_from_slice(&1u32.to_be_bytes());
        stsd.extend_from_slice(&sample_entry);
        let mut stts = vec![0; 4];
        stts.extend_from_slice(&1u32.to_be_bytes());
        stts.extend_from_slice(&1u32.to_be_bytes());
        stts.extend_from_slice(&1_000u32.to_be_bytes());
        let mut stsz = vec![0; 4];
        stsz.extend_from_slice(&6u32.to_be_bytes());
        stsz.extend_from_slice(&1u32.to_be_bytes());
        let mut stco = vec![0; 4];
        stco.extend_from_slice(&1u32.to_be_bytes());
        stco.extend_from_slice(&sample_offset.to_be_bytes());
        let mut stsc = vec![0; 4];
        stsc.extend_from_slice(&1u32.to_be_bytes());
        stsc.extend_from_slice(&1u32.to_be_bytes());
        stsc.extend_from_slice(&1u32.to_be_bytes());
        stsc.extend_from_slice(&1u32.to_be_bytes());
        let mut stbl = boxed(b"stsd", &stsd);
        stbl.extend_from_slice(&boxed(b"stts", &stts));
        stbl.extend_from_slice(&boxed(b"stsz", &stsz));
        stbl.extend_from_slice(&boxed(b"stco", &stco));
        stbl.extend_from_slice(&boxed(b"stsc", &stsc));
        let minf = boxed(b"minf", &boxed(b"stbl", &stbl));
        let mut mdia = boxed(b"mdhd", &mdhd);
        mdia.extend_from_slice(&boxed(b"hdlr", &hdlr));
        mdia.extend_from_slice(&minf);
        let mut trak = boxed(b"tkhd", &tkhd);
        trak.extend_from_slice(&boxed(b"mdia", &mdia));
        let mut moov = boxed(b"mvhd", &mvhd);
        moov.extend_from_slice(&boxed(b"trak", &trak));
        let mut file = ftyp;
        file.extend_from_slice(&mdat);
        file.extend_from_slice(&boxed(b"moov", &moov));
        file
    }

    #[test]
    fn resource_gate_is_bounded_and_every_leaf_is_12_of_12() {
        assert_eq!(env_limit("__EG_TEST_UNSET_SOURCE_LIMIT", 1024, 2048), 1024);
        assert!(validate_request_sizes(&[0x90], &[0; 64]).is_ok());
        assert!(validate_request_sizes(&[], &[1]).is_err());
        assert!(validate_request_sizes(&[0xdd, 0xff, 0xff, 0xff, 0xff], &[1]).is_err());
        for result in [
            capabilities::<DocumentData>(),
            capabilities::<ImageData>(),
            capabilities::<AudioData>(),
            capabilities::<VideoData>(),
        ] {
            assert!(result.is_ok());
        }

        // Concrete native paths use tiny fixtures; this is a correctness/resource
        // gate, deliberately not a large benchmark or compilation stress test.
        let lexeme = |term: &str| {
            Some(format!(
                "eg:lexeme:{}",
                eg_document::content_hash(term.as_bytes())
            ))
        };
        let document = NativeTextDecoder
            .decode(b"non-identifying fixture", &lexeme)
            .expect("UTF-8 fixture");
        assert_eq!(document.pages.len(), 1);

        // A valid 1x1 RGBA PNG (filter-type-0/None scanline). This literal previously
        // had a corrupt zlib Adler-32 trailer (a stale/broken fixture — flate2
        // correctly enforces the RFC 1950 checksum, so `decode_png` correctly
        // returned `None` on it; not a codec bug). Regenerated 2026-08-11 with a
        // real zlib encoder; same 70-byte length as the old literal. Mirrors
        // `eg_image::runtime::PROBE_PNG`.
        let png = vec![
            0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
            0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78,
            0xda, 0x63, 0xf8, 0xcf, 0xc0, 0x50, 0x0f, 0x00, 0x04, 0x80, 0x01, 0x7f, 0xa6, 0x8b,
            0x01, 0x3d, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
        ];
        assert!(NativeImageRuntime::decode_png(&png).is_some());

        let wav = wav_fixture();
        assert!(NativeAudioRuntime::from_wav(&wav)
            .and_then(|runtime| runtime.normalized_data())
            .is_some());

        let mp4 = mp4_fixture();
        assert!(NativeVideoRuntime::decode_isobmff(&mp4).is_some());
        assert!(png.len() + wav.len() + mp4.len() < 1024);
    }

    // ── BUG-017 frozen-format delta write + migration round trip ──────────
    //
    // These exercise the ACTUAL production functions this pass added
    // (`store_delta`, `capture_befores`, `migrate_partition_to_rows`) against
    // a real `GraphCore`, not a hand-computed byte estimate — the "measure
    // against real code" bar the existing `served_runtime.rs` benchmark
    // (which computes AFTER from JSON field extraction, by its own
    // documented design) does not itself meet.

    use eg_modality::{
        Artifact, ArtifactId, Derivation, DerivationId, EvidenceLocus, EvidenceLocusId, Feature,
        FeatureId, FeatureKind, Occurrence, PolicyEnvelope, PrivacyAttestation, Rendition,
        RenditionId, ResourceId, Segment, SegmentId, ARTIFACT_PROTOCOL_VERSION,
    };

    fn test_authority() -> ModalityAuthority {
        let claims = RequestContextClaims {
            tenant: "bug-017-tenant".to_string(),
            policy_version: "v1".to_string(),
            ..Default::default()
        };
        ModalityAuthority::from_verified("bug-017-test-secret", &claims).expect("valid authority")
    }

    fn document_value(authority: &ModalityAuthority) -> DocumentData {
        let lexemes = |term: &str| {
            authority
                .lexeme_ref(term)
                .ok()
                .map(|reference| reference.to_string())
        };
        NativeDocumentRuntime::decode_text(b"bug-017 fixture text content", &lexemes)
            .expect("fixture text decodes")
    }

    /// One occurrence's worth of a certified `ArtifactBundle` (renditions,
    /// segments, features, and evidence loci non-empty, per
    /// `ArtifactBundle::validate_certified`), scoped to `authority`'s own
    /// tenant/policy/purpose so `ServedPolicyScope::authorizes_occurrence`
    /// accepts it. Mirrors `crates/eg-modality/tests/served_runtime.rs`'s
    /// `scale_ingest` fixture shape (a different crate, so not directly
    /// reusable), scaled by `index` so every occurrence is distinct.
    fn document_ingest(authority: &ModalityAuthority, index: u64) -> ServedIngest<DocumentData> {
        let token = |offset: u64| format!("{:016x}", index * 32 + offset);
        let opaque =
            |namespace: &str, offset: u64| OpaqueRef::scoped(namespace, &token(offset)).unwrap();
        let artifact_id = ArtifactId::from_token(&token(1)).unwrap();
        let occurrence_id = OccurrenceId::from_token(&token(2)).unwrap();
        let rendition_id = RenditionId::from_token(&token(3)).unwrap();
        let segment_id = SegmentId::from_token(&token(4)).unwrap();
        let derivation_id = DerivationId::from_token(&token(5)).unwrap();

        let derivation = Derivation {
            id: derivation_id.clone(),
            transform_ref: opaque("transform", 6),
            implementation_ref: opaque("implementation", 7),
            version_ref: opaque("version", 8),
            model_ref: None,
            inputs: vec![ResourceId::Occurrence(occurrence_id.clone())],
        };
        let bundle = ArtifactBundle {
            protocol_version: ARTIFACT_PROTOCOL_VERSION,
            privacy: PrivacyAttestation {
                scanner_ref: opaque("scanner", 9),
                policy_version_ref: opaque("policyversion", 10),
                raw_pii_persisted: false,
                local_identifiers_persisted: false,
            },
            artifacts: vec![Artifact {
                id: artifact_id.clone(),
                content_ref: opaque("content", 11),
                modality: ModalityKind::Document,
                schema_ref: opaque("schema", 12),
                content_version: 1,
            }],
            occurrences: vec![Occurrence {
                id: occurrence_id.clone(),
                artifact_id: artifact_id.clone(),
                source_ref: opaque("source", 13),
                observation_version: 1,
                policy: PolicyEnvelope {
                    tenant_ref: authority.scope.tenant_ref.clone(),
                    access_policy_ref: authority.scope.access_policy_ref.clone(),
                    classification: Classification::Internal,
                    retention_policy_ref: opaque("retention", 14),
                    deletion_policy_ref: opaque("deletion", 15),
                    legal_hold_ref: None,
                    purpose_refs: vec![authority.scope.purpose_ref.clone()],
                },
            }],
            renditions: vec![Rendition {
                id: rendition_id.clone(),
                occurrence_id: occurrence_id.clone(),
                content_ref: opaque("content", 16),
                modality: ModalityKind::Document,
                schema_ref: opaque("schema", 17),
                derivation: derivation.clone(),
            }],
            segments: vec![Segment {
                id: segment_id.clone(),
                rendition_id,
                parent_segment_id: None,
                kind: SegmentKind::Page,
                ordinal: 0,
                schema_ref: opaque("schema", 18),
            }],
            features: vec![Feature {
                id: FeatureId::from_token(&token(19)).unwrap(),
                subject: ResourceId::Segment(segment_id.clone()),
                kind: FeatureKind::Statistic,
                value_ref: opaque("value", 20),
                schema_ref: opaque("schema", 21),
                derivation: derivation.clone(),
            }],
            evidence_loci: vec![EvidenceLocus {
                id: EvidenceLocusId::from_token(&token(22)).unwrap(),
                subject: ResourceId::Segment(segment_id),
                address: EvidenceAddress::CharacterRange { start: 0, end: 4 },
                policy_ref: authority.scope.access_policy_ref.clone(),
                derivation_ref: derivation_id,
            }],
        };
        ServedIngest {
            idempotency_ref: opaque("idempotency", 23),
            target_occurrence_id: occurrence_id,
            expected_version: None,
            bundle,
            value: document_value(authority),
        }
    }

    /// The real BUG-017 acceptance evidence: bytes `store_delta` (the
    /// function every ingest handler now calls) actually writes to a real
    /// `GraphCore` for ONE more occurrence's mutation, measured at increasing
    /// corpus — not a JSON field-extraction estimate.
    #[test]
    fn bug_017_delta_write_bytes_stay_flat_with_corpus() {
        const CHECKPOINTS: [u64; 4] = [64, 512, 4_096, 16_384];
        let core = GraphCore::new();
        let authority = test_authority();
        let modality = ServedModalityKind::Document;
        let partition = authority.node_id(modality);
        let mut runtime: ServedModalityRuntime<DocumentData> = ServedModalityRuntime::new();
        let mut ingested = 0u64;
        let mut real_delta_bytes = Vec::new();

        for &target in &CHECKPOINTS {
            let mut last_bytes = 0usize;
            for index in ingested..target {
                let command = document_ingest(&authority, index);
                let occurrence_id = command.target_occurrence_id.clone();
                let idempotency_key = command.idempotency_ref.clone();
                let before = runtime.record(&occurrence_id).cloned();
                let outcome = runtime.ingest(command).expect("fresh ingest applies");
                let delta = MutationDelta::capture(
                    &runtime,
                    before.as_ref(),
                    &occurrence_id,
                    &idempotency_key,
                    outcome.disposition == ApplyDisposition::IdempotentReplay,
                )
                .expect("a fresh Applied ingest always produces a delta");
                store_delta(&core, &authority, modality, &delta).expect("store delta");

                let record_id =
                    delta_row_node_id(&partition, "record", occurrence_id.as_ref().as_str());
                let event_id =
                    delta_row_node_id(&partition, "event", &delta.event.sequence.to_string());
                let idem_id = delta_row_node_id(&partition, "idem", idempotency_key.as_str());
                let manifest_id = delta_row_node_id(&partition, "manifest", "");
                last_bytes = core.get_node_properties(&record_id).unwrap().len()
                    + core.get_node_properties(&event_id).unwrap().len()
                    + core.get_node_properties(&idem_id).unwrap().len()
                    + core.get_node_properties(&manifest_id).unwrap().len();
            }
            ingested = target;
            real_delta_bytes.push(last_bytes);
        }

        eprintln!(
            "BUG-017 REAL delta-write bytes vs corpus (src/server/handlers/modality.rs, store_delta against a real GraphCore):"
        );
        for (i, &target) in CHECKPOINTS.iter().enumerate() {
            eprintln!(
                "  corpus={target:>6}  real store_delta bytes for ONE more mutation={:>6}",
                real_delta_bytes[i]
            );
        }

        let smallest = real_delta_bytes[0] as f64;
        let largest = *real_delta_bytes.last().unwrap() as f64;
        let growth = largest / smallest;
        assert!(
            growth < 3.0,
            "real store_delta bytes must stay roughly flat as corpus grows 256x — got \
             {growth:.2}x growth ({smallest} -> {largest} bytes); the delta write is leaking \
             corpus-sized state"
        );
    }

    /// Prove the shadow-write migration tool round-trips REAL existing data:
    /// build up runtime state exactly as production does (`ServedModalityRuntime::
    /// ingest`), persist it through the LEGACY whole-snapshot writer
    /// (`store_runtime` — simulating an existing production partition that
    /// predates this change), then run `migrate_partition_to_rows` and assert
    /// it succeeds (it internally asserts row-reconstructed state is
    /// byte-for-byte identical to the snapshot-recovered original via
    /// `eg_modality::verify_round_trip`, reading every row back from the
    /// same real `GraphCore` the rows were written to).
    #[test]
    fn bug_017_migration_round_trips_real_existing_data() {
        let core = GraphCore::new();
        let authority = test_authority();
        let modality = ServedModalityKind::Document;
        let mut runtime: ServedModalityRuntime<DocumentData> = ServedModalityRuntime::new();

        for index in 0..40 {
            runtime
                .ingest(document_ingest(&authority, index))
                .expect("fixture ingest applies");
        }
        // A delete and a cold/restore cycle so the migrated corpus includes a
        // tombstone and a lifecycle-transitioned record, not only fresh
        // ingests.
        let deleted = document_ingest(&authority, 0).target_occurrence_id;
        runtime
            .delete(
                &authority.scope,
                ServedDelete {
                    idempotency_ref: OpaqueRef::scoped(
                        "idempotency",
                        &format!("{:016x}", 999_998u64),
                    )
                    .unwrap(),
                    occurrence_id: deleted,
                    expected_version: 1,
                },
            )
            .expect("delete applies");
        let cold = document_ingest(&authority, 1).target_occurrence_id;
        runtime
            .move_to_cold(&authority.scope, &cold)
            .expect("move_to_cold applies");
        runtime
            .restore(&authority.scope, &cold)
            .expect("restore applies");

        store_runtime(&core, &authority, modality, &runtime).expect("legacy snapshot write");

        migrate_partition_to_rows::<DocumentData>(&core, &authority, modality)
            .expect("migration round-trips real existing data byte-for-byte");
    }

    /// BUG-017 coverage gap found revalidating GOC-45: `move_to_cold`/
    /// `restore` (`ServedModalityOp::MoveToCold`/`Restore`) never called
    /// `shadow_write_deltas`/`store_delta` at all prior to this pass — only
    /// `store_runtime` (the legacy whole-snapshot writer). The row shadow
    /// store therefore silently diverged from production truth on every
    /// lifecycle transition, which would have failed a future digest-compare
    /// cutover check the first time a partition saw one. Proves
    /// `MutationDelta::capture_lifecycle` + `store_delta` now persist a
    /// record row reflecting the POST-transition lifecycle state, with no
    /// idempotency row (lifecycle transitions carry no `idempotency_ref` on
    /// the wire).
    #[test]
    fn bug_017_lifecycle_transition_now_produces_a_shadow_row() {
        use eg_modality::LifecycleState;

        let core = GraphCore::new();
        let authority = test_authority();
        let modality = ServedModalityKind::Document;
        let partition = authority.node_id(modality);
        let mut runtime: ServedModalityRuntime<DocumentData> = ServedModalityRuntime::new();

        let command = document_ingest(&authority, 0);
        let occurrence_id = command.target_occurrence_id.clone();
        runtime.ingest(command).expect("fresh ingest applies");

        // Before this fix: nothing below this line ever ran for a lifecycle
        // transition; only `store_runtime` (whole snapshot) did.
        let before = runtime.record(&occurrence_id).cloned();
        assert_eq!(before.as_ref().unwrap().lifecycle, LifecycleState::Active);
        runtime
            .move_to_cold(&authority.scope, &occurrence_id)
            .expect("move_to_cold applies");
        let delta = MutationDelta::capture_lifecycle(&runtime, before.as_ref(), &occurrence_id)
            .expect("a lifecycle transition on a live occurrence always produces a delta");
        assert!(
            delta.idempotency.is_none(),
            "a lifecycle transition carries no idempotency_ref; the delta must not invent one"
        );
        store_delta(&core, &authority, modality, &delta).expect("store lifecycle delta");

        let record_id = delta_row_node_id(&partition, "record", occurrence_id.as_ref().as_str());
        let sealed = core
            .get_node_properties(&record_id)
            .expect("lifecycle transition wrote a record row");
        let bytes = authority.cipher.unseal(&sealed).expect("row decrypts");
        let stored: ServedRecord<DocumentData> =
            serde_json::from_slice(&bytes).expect("row decodes");
        assert_eq!(
            stored.lifecycle,
            LifecycleState::Cold,
            "the shadow row must reflect the POST-transition state, not the stale ingest-time state"
        );
    }
}
