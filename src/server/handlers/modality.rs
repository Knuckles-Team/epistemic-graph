//! Live governed document/image/audio/video serving boundary.
//!
//! Authority is derived from the verified request context, never from caller
//! fields. Source bytes exist only for the duration of native decoding. The only
//! graph value written here is an AEAD-sealed `ServedModalityRuntime` snapshot at
//! an HMAC-derived node id; ordinary graph reads therefore expose neither source
//! content nor cross-policy runtime state even if they enumerate internal nodes.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use hmac::{Hmac, Mac};
use serde::{de::DeserializeOwned, Serialize};
use sha2::Sha256;

use crate::crypto::ValueCipher;
use crate::graph::GraphCore;
use crate::protocol::ResultPayload;
use eg_audio::runtime::NativeAudioRuntime;
use eg_audio::{AudioData, AudioSegment, AudioServingRuntime};
use eg_document::runtime::NativeDocumentRuntime;
use eg_document::DocumentData;
use eg_image::runtime::NativeImageRuntime;
use eg_image::{ImageData, ImageRegion, ImageServingRuntime};
use eg_modality::{
    tck_report, ApplyOutcome, ArtifactBundle, Classification, ConformanceTestable, EvidenceAddress,
    GovernedModality, ModalityKind, NativePredicate, OccurrenceId, OpaqueRef, SegmentKind,
    ServedDelete, ServedIngest, ServedModalityRuntime, ServedNativeQuery, ServedPolicyScope,
    ServedQuery,
};
use eg_types::acl::RequestContextClaims;
use eg_types::{
    ServedModalityIngestItem, ServedModalityKind, ServedModalityOp, ServedNativePredicate,
    ServedSegmentKind,
};
use eg_video::runtime::NativeVideoRuntime;
use eg_video::{VideoData, VideoServingRuntime, VideoShot};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const HARD_MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_BUNDLE_BYTES: usize = 4 * 1024 * 1024;
const HARD_MAX_BUNDLE_BYTES: usize = 32 * 1024 * 1024;
const MAX_INGEST_STREAM_ITEMS: usize = 64;
const MIN_PRIVACY_PROBE_BYTES: usize = 16;

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

#[derive(Serialize)]
struct AuthorityView<'a> {
    tenant_ref: &'a OpaqueRef,
    access_policy_ref: &'a OpaqueRef,
    purpose_ref: &'a OpaqueRef,
    maximum_classification: Classification,
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

    fn view(&self) -> AuthorityView<'_> {
        AuthorityView {
            tenant_ref: &self.scope.tenant_ref,
            access_policy_ref: &self.scope.access_policy_ref,
            purpose_ref: &self.scope.purpose_ref,
            maximum_classification: self.scope.maximum_classification,
        }
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

fn segment_kind(kind: ServedSegmentKind) -> SegmentKind {
    match kind {
        ServedSegmentKind::Page => SegmentKind::Page,
        ServedSegmentKind::Paragraph => SegmentKind::Paragraph,
        ServedSegmentKind::Table => SegmentKind::Table,
        ServedSegmentKind::Row => SegmentKind::Row,
        ServedSegmentKind::Region => SegmentKind::Region,
        ServedSegmentKind::AudioRange => SegmentKind::AudioRange,
        ServedSegmentKind::VideoShot => SegmentKind::VideoShot,
        ServedSegmentKind::FrameRange => SegmentKind::FrameRange,
        ServedSegmentKind::TimeWindow => SegmentKind::TimeWindow,
        ServedSegmentKind::CodeSymbol => SegmentKind::CodeSymbol,
        ServedSegmentKind::TraceSpan => SegmentKind::TraceSpan,
    }
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

/// Resolve the opaque resource closure owned by the target occurrence. Evidence
/// from a different artifact in the same certified bundle must never become native
/// regions/segments/shots on the target value.
fn target_resource_closure(
    bundle: &ArtifactBundle,
    target: &OccurrenceId,
) -> Result<BTreeSet<String>, String> {
    let occurrence = bundle
        .occurrences
        .iter()
        .find(|candidate| candidate.id.as_ref() == target.as_ref())
        .ok_or_else(|| "invalid governed modality bundle".to_string())?;
    let rendition_ids: BTreeSet<&str> = bundle
        .renditions
        .iter()
        .filter(|rendition| rendition.occurrence_id.as_ref() == target.as_ref())
        .map(|rendition| rendition.id.as_ref().as_str())
        .collect();
    let mut resources = BTreeSet::from([
        target.as_ref().as_str().to_string(),
        occurrence.artifact_id.as_ref().as_str().to_string(),
    ]);
    resources.extend(rendition_ids.iter().map(|value| (*value).to_string()));
    for segment in &bundle.segments {
        if rendition_ids.contains(segment.rendition_id.as_ref().as_str()) {
            resources.insert(segment.id.as_ref().as_str().to_string());
        }
    }

    let mut descendants: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for feature in &bundle.features {
        descendants
            .entry(feature.subject.opaque().as_str().to_string())
            .or_default()
            .push(feature.id.as_ref().as_str().to_string());
    }
    for locus in &bundle.evidence_loci {
        descendants
            .entry(locus.subject.opaque().as_str().to_string())
            .or_default()
            .push(locus.id.as_ref().as_str().to_string());
    }
    let mut queue: Vec<String> = resources.iter().cloned().collect();
    while let Some(resource) = queue.pop() {
        if let Some(children) = descendants.get(&resource) {
            for child in children {
                if resources.insert(child.clone()) {
                    queue.push(child.clone());
                }
            }
        }
    }
    Ok(resources)
}

struct DecodedIngest {
    bundle: ArtifactBundle,
    target: OccurrenceId,
    idempotency: OpaqueRef,
    target_resources: BTreeSet<String>,
    source: Vec<u8>,
}

fn decode_ingest(
    item: ServedModalityIngestItem,
    modality: ServedModalityKind,
    authority: &ModalityAuthority,
) -> Result<DecodedIngest, String> {
    validate_request_sizes(&item.bundle_msgpack, &item.source_bytes)?;
    let bundle: ArtifactBundle = eg_types::msgpack::decode_bounded(
        &item.bundle_msgpack,
        eg_types::msgpack::MsgpackLimits::new(HARD_MAX_BUNDLE_BYTES, 250_000, 64),
    )
    .map_err(|_| "invalid governed modality bundle".to_string())?;
    bundle
        .validate_certified()
        .map_err(|_| "invalid governed modality bundle".to_string())?;
    let target = occurrence(item.target_occurrence_id)?;
    let idempotency = opaque(item.idempotency_ref)?;
    let target_resources = target_resource_closure(&bundle, &target)?;
    let content_hash = match modality {
        ServedModalityKind::Document => eg_document::content_hash(&item.source_bytes),
        ServedModalityKind::Image => eg_image::content_hash(&item.source_bytes),
        ServedModalityKind::Audio => eg_audio::content_hash(&item.source_bytes),
        ServedModalityKind::Video => eg_video::content_hash(&item.source_bytes),
    };
    validate_content_binding(&bundle, &target, modality, &content_hash, authority)?;
    Ok(DecodedIngest {
        bundle,
        target,
        idempotency,
        target_resources,
        source: item.source_bytes,
    })
}

fn ingest_stream(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    items: Vec<ServedModalityIngestItem>,
) -> Result<Vec<ApplyOutcome>, String> {
    if !(1..=MAX_INGEST_STREAM_ITEMS).contains(&items.len()) {
        return Err("modality ingest stream cardinality is outside bounds".to_string());
    }
    let expected_versions: Vec<Option<u64>> =
        items.iter().map(|item| item.expected_version).collect();
    let decoded: Vec<DecodedIngest> = items
        .into_iter()
        .map(|item| decode_ingest(item, modality, authority))
        .collect::<Result<_, _>>()?;
    let sources: Vec<Vec<u8>> = decoded.iter().map(|item| item.source.clone()).collect();
    match modality {
        ServedModalityKind::Document => {
            let lexemes = |term: &str| {
                authority
                    .lexeme_ref(term)
                    .ok()
                    .map(|reference| reference.to_string())
            };
            let commands = decoded
                .into_iter()
                .zip(expected_versions)
                .map(|(item, expected_version)| {
                    let value = NativeDocumentRuntime::decode_text(&item.source, &lexemes)
                        .ok_or_else(|| "modality codec failure".to_string())?;
                    Ok(ServedIngest {
                        idempotency_ref: item.idempotency,
                        target_occurrence_id: item.target,
                        expected_version,
                        bundle: item.bundle,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let mut runtime: ServedModalityRuntime<DocumentData> =
                load_runtime(core, authority, modality)?;
            let outcomes = runtime
                .ingest_stream(commands)
                .map_err(|error| error.to_string())?;
            store_runtime_excluding_sources(core, authority, modality, &runtime, &sources)?;
            Ok(outcomes)
        }
        ServedModalityKind::Image => {
            let commands = decoded
                .into_iter()
                .zip(expected_versions)
                .map(|(item, expected_version)| {
                    let native = NativeImageRuntime::decode_png(&item.source)
                        .ok_or_else(|| "modality codec failure".to_string())?;
                    let mut value = native.normalized_data();
                    value.regions = item
                        .bundle
                        .evidence_loci
                        .iter()
                        .filter(|locus| item.target_resources.contains(locus.id.as_ref().as_str()))
                        .filter_map(|locus| match &locus.address {
                            EvidenceAddress::ImageRegion {
                                x,
                                y,
                                width,
                                height,
                            } => Some(ImageRegion::new(*x, *y, *width, *height)),
                            _ => None,
                        })
                        .collect();
                    Ok(ServedIngest {
                        idempotency_ref: item.idempotency,
                        target_occurrence_id: item.target,
                        expected_version,
                        bundle: item.bundle,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let mut runtime: ImageServingRuntime = load_runtime(core, authority, modality)?;
            let outcomes = runtime
                .ingest_stream(commands)
                .map_err(|error| error.to_string())?;
            store_runtime_excluding_sources(core, authority, modality, &runtime, &sources)?;
            Ok(outcomes)
        }
        ServedModalityKind::Audio => {
            let commands = decoded
                .into_iter()
                .zip(expected_versions)
                .map(|(item, expected_version)| {
                    let native = NativeAudioRuntime::from_wav(&item.source)
                        .ok_or_else(|| "modality codec failure".to_string())?;
                    let mut value = native
                        .normalized_data()
                        .ok_or_else(|| "modality codec failure".to_string())?;
                    value.segments.extend(
                        item.bundle
                            .evidence_loci
                            .iter()
                            .filter(|locus| {
                                item.target_resources.contains(locus.id.as_ref().as_str())
                            })
                            .filter_map(|locus| match &locus.address {
                                EvidenceAddress::AudioRange { start_ms, end_ms } => {
                                    Some(AudioSegment::new(*start_ms, *end_ms))
                                }
                                _ => None,
                            }),
                    );
                    Ok(ServedIngest {
                        idempotency_ref: item.idempotency,
                        target_occurrence_id: item.target,
                        expected_version,
                        bundle: item.bundle,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let mut runtime: AudioServingRuntime = load_runtime(core, authority, modality)?;
            let outcomes = runtime
                .ingest_stream(commands)
                .map_err(|error| error.to_string())?;
            store_runtime_excluding_sources(core, authority, modality, &runtime, &sources)?;
            Ok(outcomes)
        }
        ServedModalityKind::Video => {
            let commands = decoded
                .into_iter()
                .zip(expected_versions)
                .map(|(item, expected_version)| {
                    let native = NativeVideoRuntime::decode_isobmff(&item.source)
                        .ok_or_else(|| "modality codec failure".to_string())?;
                    let mut value = native.normalized_data();
                    value.shots = item
                        .bundle
                        .evidence_loci
                        .iter()
                        .filter(|locus| item.target_resources.contains(locus.id.as_ref().as_str()))
                        .filter_map(|locus| match &locus.address {
                            EvidenceAddress::VideoTimeRange { start_ms, end_ms } => {
                                Some(VideoShot::new(*start_ms, *end_ms))
                            }
                            _ => None,
                        })
                        .collect();
                    Ok(ServedIngest {
                        idempotency_ref: item.idempotency,
                        target_occurrence_id: item.target,
                        expected_version,
                        bundle: item.bundle,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let mut runtime: VideoServingRuntime = load_runtime(core, authority, modality)?;
            let outcomes = runtime
                .ingest_stream(commands)
                .map_err(|error| error.to_string())?;
            store_runtime_excluding_sources(core, authority, modality, &runtime, &sources)?;
            Ok(outcomes)
        }
    }
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
    Ok(ResultPayload::raw(&outcome))
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
            segment_kind: requested_segment.map(segment_kind),
            after: after.map(occurrence).transpose()?,
            limit,
            include_cold,
        })
        .map_err(|error| error.to_string())?;
    Ok(ResultPayload::raw(&page))
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
    Ok(ResultPayload::raw(&page))
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
    let outcome = runtime
        .delete(
            &authority.scope,
            ServedDelete {
                idempotency_ref: opaque(idempotency_ref)?,
                occurrence_id: occurrence(occurrence_id)?,
                expected_version,
            },
        )
        .map_err(|error| error.to_string())?;
    store_runtime(core, authority, modality, &runtime)?;
    Ok(ResultPayload::raw(&outcome))
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
    let outcome = if restore {
        runtime.restore(&authority.scope, &id)
    } else {
        runtime.move_to_cold(&authority.scope, &id)
    }
    .map_err(|error| error.to_string())?;
    store_runtime(core, authority, modality, &runtime)?;
    Ok(ResultPayload::raw(&outcome))
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
    Ok(ResultPayload::raw(&runtime.events_after_authorized(
        &authority.scope,
        after_sequence,
        limit,
    )))
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
    Ok(ResultPayload::raw(&stats))
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
    Ok(ResultPayload::Json(
        serde_json::json!({"collected": collected}),
    ))
}

fn capabilities<T: ConformanceTestable>() -> Result<ResultPayload, String> {
    let report = tck_report::<T>();
    if !report.is_production_ready() || report.pass_count() != 12 || report.na_count() != 0 {
        return Err("served modality failed the component TCK".to_string());
    }
    Ok(ResultPayload::Json(serde_json::json!({
        "component_ready": true,
        "component_pass": report.pass_count(),
        "component_not_applicable": report.na_count(),
        "component_total": 12,
    })))
}

/// Execute one graph-scoped served operation. Mutating calls are invoked against
/// the mutation gateway's staged `GraphCore`; read calls receive the live snapshot.
pub(crate) fn handle(
    core: &GraphCore,
    authority: &ModalityAuthority,
    op: ServedModalityOp,
) -> Result<ResultPayload, String> {
    match op {
        ServedModalityOp::Authority => Ok(ResultPayload::raw(&authority.view())),
        ServedModalityOp::Ingest {
            modality,
            idempotency_ref,
            target_occurrence_id,
            expected_version,
            bundle_msgpack,
            source_bytes,
        } => ingest(
            core,
            authority,
            modality,
            ServedModalityIngestItem {
                idempotency_ref,
                target_occurrence_id,
                expected_version,
                bundle_msgpack,
                source_bytes,
            },
        ),
        ServedModalityOp::IngestStream { modality, items } => {
            if !(2..=MAX_INGEST_STREAM_ITEMS).contains(&items.len()) {
                return Err("modality ingest stream cardinality is outside bounds".to_string());
            }
            let outcomes = ingest_stream(core, authority, modality, items)?;
            Ok(ResultPayload::raw(&outcomes))
        }
        ServedModalityOp::Query {
            modality,
            segment_kind,
            after_occurrence_id,
            limit,
            include_cold,
        } => match modality {
            ServedModalityKind::Document => query::<DocumentData>(
                core,
                authority,
                modality,
                segment_kind,
                after_occurrence_id,
                limit,
                include_cold,
            ),
            ServedModalityKind::Image => query::<ImageData>(
                core,
                authority,
                modality,
                segment_kind,
                after_occurrence_id,
                limit,
                include_cold,
            ),
            ServedModalityKind::Audio => query::<AudioData>(
                core,
                authority,
                modality,
                segment_kind,
                after_occurrence_id,
                limit,
                include_cold,
            ),
            ServedModalityKind::Video => query::<VideoData>(
                core,
                authority,
                modality,
                segment_kind,
                after_occurrence_id,
                limit,
                include_cold,
            ),
        },
        ServedModalityOp::NativeQuery {
            predicate,
            after_occurrence_id,
            limit,
            include_cold,
        } => {
            let (modality, predicate) = native_predicate(authority, predicate)?;
            match modality {
                ServedModalityKind::Document => native_query::<DocumentData>(
                    core,
                    authority,
                    modality,
                    predicate,
                    after_occurrence_id,
                    limit,
                    include_cold,
                ),
                ServedModalityKind::Image => native_query::<ImageData>(
                    core,
                    authority,
                    modality,
                    predicate,
                    after_occurrence_id,
                    limit,
                    include_cold,
                ),
                ServedModalityKind::Audio => native_query::<AudioData>(
                    core,
                    authority,
                    modality,
                    predicate,
                    after_occurrence_id,
                    limit,
                    include_cold,
                ),
                ServedModalityKind::Video => native_query::<VideoData>(
                    core,
                    authority,
                    modality,
                    predicate,
                    after_occurrence_id,
                    limit,
                    include_cold,
                ),
            }
        }
        ServedModalityOp::Delete {
            modality,
            idempotency_ref,
            occurrence_id,
            expected_version,
        } => match modality {
            ServedModalityKind::Document => delete::<DocumentData>(
                core,
                authority,
                modality,
                idempotency_ref,
                occurrence_id,
                expected_version,
            ),
            ServedModalityKind::Image => delete::<ImageData>(
                core,
                authority,
                modality,
                idempotency_ref,
                occurrence_id,
                expected_version,
            ),
            ServedModalityKind::Audio => delete::<AudioData>(
                core,
                authority,
                modality,
                idempotency_ref,
                occurrence_id,
                expected_version,
            ),
            ServedModalityKind::Video => delete::<VideoData>(
                core,
                authority,
                modality,
                idempotency_ref,
                occurrence_id,
                expected_version,
            ),
        },
        ServedModalityOp::MoveToCold {
            modality,
            occurrence_id,
        } => match modality {
            ServedModalityKind::Document => {
                lifecycle::<DocumentData>(core, authority, modality, occurrence_id, false)
            }
            ServedModalityKind::Image => {
                lifecycle::<ImageData>(core, authority, modality, occurrence_id, false)
            }
            ServedModalityKind::Audio => {
                lifecycle::<AudioData>(core, authority, modality, occurrence_id, false)
            }
            ServedModalityKind::Video => {
                lifecycle::<VideoData>(core, authority, modality, occurrence_id, false)
            }
        },
        ServedModalityOp::Restore {
            modality,
            occurrence_id,
        } => match modality {
            ServedModalityKind::Document => {
                lifecycle::<DocumentData>(core, authority, modality, occurrence_id, true)
            }
            ServedModalityKind::Image => {
                lifecycle::<ImageData>(core, authority, modality, occurrence_id, true)
            }
            ServedModalityKind::Audio => {
                lifecycle::<AudioData>(core, authority, modality, occurrence_id, true)
            }
            ServedModalityKind::Video => {
                lifecycle::<VideoData>(core, authority, modality, occurrence_id, true)
            }
        },
        ServedModalityOp::Events {
            modality,
            after_sequence,
            limit,
        } => match modality {
            ServedModalityKind::Document => {
                events::<DocumentData>(core, authority, modality, after_sequence, limit)
            }
            ServedModalityKind::Image => {
                events::<ImageData>(core, authority, modality, after_sequence, limit)
            }
            ServedModalityKind::Audio => {
                events::<AudioData>(core, authority, modality, after_sequence, limit)
            }
            ServedModalityKind::Video => {
                events::<VideoData>(core, authority, modality, after_sequence, limit)
            }
        },
        ServedModalityOp::Stats { modality } => match modality {
            ServedModalityKind::Document => stats::<DocumentData>(core, authority, modality),
            ServedModalityKind::Image => stats::<ImageData>(core, authority, modality),
            ServedModalityKind::Audio => stats::<AudioData>(core, authority, modality),
            ServedModalityKind::Video => stats::<VideoData>(core, authority, modality),
        },
        ServedModalityOp::CollectTombstones {
            modality,
            through_event_sequence,
        } => match modality {
            ServedModalityKind::Document => collect_tombstones::<DocumentData>(
                core,
                authority,
                modality,
                through_event_sequence,
            ),
            ServedModalityKind::Image => {
                collect_tombstones::<ImageData>(core, authority, modality, through_event_sequence)
            }
            ServedModalityKind::Audio => {
                collect_tombstones::<AudioData>(core, authority, modality, through_event_sequence)
            }
            ServedModalityKind::Video => {
                collect_tombstones::<VideoData>(core, authority, modality, through_event_sequence)
            }
        },
        ServedModalityOp::Capabilities { modality } => match modality {
            ServedModalityKind::Document => capabilities::<DocumentData>(),
            ServedModalityKind::Image => capabilities::<ImageData>(),
            ServedModalityKind::Audio => capabilities::<AudioData>(),
            ServedModalityKind::Video => capabilities::<VideoData>(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_document::{DocumentDecoder, NativeTextDecoder};

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
        assert!(validate_request_sizes(&[0x90], &vec![0; 64]).is_ok());
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

        let png = vec![
            0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
            0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78,
            0xda, 0x63, 0xfc, 0xcf, 0xc0, 0x50, 0x0f, 0x00, 0x05, 0xfe, 0x02, 0xfe, 0x42, 0x75,
            0x27, 0x59, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
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
}
