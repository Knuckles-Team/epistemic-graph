use std::collections::{BTreeMap, BTreeSet};

use eg_audio::runtime::NativeAudioRuntime;
use eg_audio::{AudioData, AudioSegment};
use eg_document::runtime::NativeDocumentRuntime;
use eg_document::DocumentData;
use eg_image::runtime::NativeImageRuntime;
use eg_image::{ImageData, ImageRegion};
use eg_modality::{
    ApplyOutcome, ArtifactBundle, EvidenceAddress, GovernedModality, OccurrenceId, OpaqueRef,
    ServedIngest, ServedModalityRuntime,
};
use eg_types::{ServedModalityIngestItem, ServedModalityKind};
use eg_video::runtime::NativeVideoRuntime;
use eg_video::{VideoData, VideoShot};

use super::{
    capture_befores, load_runtime, shadow_write_deltas, store_runtime_excluding_sources,
    validate_content_binding, validate_request_sizes, ModalityAuthority, MAX_INGEST_STREAM_ITEMS,
};
use crate::graph::GraphCore;

struct ResourceClosure {
    resources: BTreeSet<String>,
    descendants: BTreeMap<String, Vec<String>>,
}

impl ResourceClosure {
    fn resolve(bundle: &ArtifactBundle, target: &OccurrenceId) -> Result<BTreeSet<String>, String> {
        let (mut closure, rendition_ids) = Self::seed(bundle, target)?;
        closure.add_segments(bundle, &rendition_ids);
        closure.index_descendants(bundle);
        closure.expand();
        Ok(closure.resources)
    }

    fn seed<'a>(
        bundle: &'a ArtifactBundle,
        target: &OccurrenceId,
    ) -> Result<(Self, BTreeSet<&'a str>), String> {
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
        let mut closure = Self {
            resources: BTreeSet::from([
                target.as_ref().as_str().to_string(),
                occurrence.artifact_id.as_ref().as_str().to_string(),
            ]),
            descendants: BTreeMap::new(),
        };
        closure
            .resources
            .extend(rendition_ids.iter().map(|value| (*value).to_string()));
        Ok((closure, rendition_ids))
    }

    fn add_segments(&mut self, bundle: &ArtifactBundle, rendition_ids: &BTreeSet<&str>) {
        for segment in &bundle.segments {
            if rendition_ids.contains(segment.rendition_id.as_ref().as_str()) {
                self.resources
                    .insert(segment.id.as_ref().as_str().to_string());
            }
        }
    }

    fn index_descendants(&mut self, bundle: &ArtifactBundle) {
        for feature in &bundle.features {
            self.descendants
                .entry(feature.subject.opaque().as_str().to_string())
                .or_default()
                .push(feature.id.as_ref().as_str().to_string());
        }
        for locus in &bundle.evidence_loci {
            self.descendants
                .entry(locus.subject.opaque().as_str().to_string())
                .or_default()
                .push(locus.id.as_ref().as_str().to_string());
        }
    }

    fn expand(&mut self) {
        let mut queue: Vec<String> = self.resources.iter().cloned().collect();
        while let Some(resource) = queue.pop() {
            if let Some(children) = self.descendants.get(&resource) {
                for child in children {
                    if self.resources.insert(child.clone()) {
                        queue.push(child.clone());
                    }
                }
            }
        }
    }
}

struct DecodedIngest {
    bundle: ArtifactBundle,
    target: OccurrenceId,
    idempotency: OpaqueRef,
    expected_version: Option<u64>,
    target_resources: BTreeSet<String>,
    source: Vec<u8>,
}

impl DecodedIngest {
    fn decode(
        item: ServedModalityIngestItem,
        modality: ServedModalityKind,
        authority: &ModalityAuthority,
    ) -> Result<Self, String> {
        validate_request_sizes(&item.bundle_msgpack, &item.source_bytes)?;
        let bundle: ArtifactBundle = eg_types::msgpack::decode_bounded(
            &item.bundle_msgpack,
            eg_types::msgpack::MsgpackLimits::new(super::HARD_MAX_BUNDLE_BYTES, 250_000, 64),
        )
        .map_err(|_| "invalid governed modality bundle".to_string())?;
        bundle
            .validate_certified()
            .map_err(|_| "invalid governed modality bundle".to_string())?;
        let target = super::occurrence(item.target_occurrence_id)?;
        let idempotency = super::opaque(item.idempotency_ref)?;
        let target_resources = ResourceClosure::resolve(&bundle, &target)?;
        let content_hash = match modality {
            ServedModalityKind::Document => eg_document::content_hash(&item.source_bytes),
            ServedModalityKind::Image => eg_image::content_hash(&item.source_bytes),
            ServedModalityKind::Audio => eg_audio::content_hash(&item.source_bytes),
            ServedModalityKind::Video => eg_video::content_hash(&item.source_bytes),
        };
        validate_content_binding(&bundle, &target, modality, &content_hash, authority)?;
        Ok(Self {
            bundle,
            target,
            idempotency,
            expected_version: item.expected_version,
            target_resources,
            source: item.source_bytes,
        })
    }

    fn into_document<F>(self, lexemes: &F) -> Result<(ServedIngest<DocumentData>, Vec<u8>), String>
    where
        F: Fn(&str) -> Option<String>,
    {
        let value = NativeDocumentRuntime::decode_text(&self.source, lexemes)
            .ok_or_else(|| "modality codec failure".to_string())?;
        let source = self.source;
        Ok((
            ServedIngest {
                idempotency_ref: self.idempotency,
                target_occurrence_id: self.target,
                expected_version: self.expected_version,
                bundle: self.bundle,
                value,
            },
            source,
        ))
    }

    fn into_image(self) -> Result<(ServedIngest<ImageData>, Vec<u8>), String> {
        let native = NativeImageRuntime::decode_png(&self.source)
            .ok_or_else(|| "modality codec failure".to_string())?;
        let mut value = native.normalized_data();
        value.regions = self
            .bundle
            .evidence_loci
            .iter()
            .filter(|locus| self.target_resources.contains(locus.id.as_ref().as_str()))
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
        let source = self.source;
        Ok((
            ServedIngest {
                idempotency_ref: self.idempotency,
                target_occurrence_id: self.target,
                expected_version: self.expected_version,
                bundle: self.bundle,
                value,
            },
            source,
        ))
    }

    fn into_audio(self) -> Result<(ServedIngest<AudioData>, Vec<u8>), String> {
        let native = NativeAudioRuntime::from_wav(&self.source)
            .ok_or_else(|| "modality codec failure".to_string())?;
        let mut value = native
            .normalized_data()
            .ok_or_else(|| "modality codec failure".to_string())?;
        value.segments.extend(
            self.bundle
                .evidence_loci
                .iter()
                .filter(|locus| self.target_resources.contains(locus.id.as_ref().as_str()))
                .filter_map(|locus| match &locus.address {
                    EvidenceAddress::AudioRange { start_ms, end_ms } => {
                        Some(AudioSegment::new(*start_ms, *end_ms))
                    }
                    _ => None,
                }),
        );
        let source = self.source;
        Ok((
            ServedIngest {
                idempotency_ref: self.idempotency,
                target_occurrence_id: self.target,
                expected_version: self.expected_version,
                bundle: self.bundle,
                value,
            },
            source,
        ))
    }

    fn into_video(self) -> Result<(ServedIngest<VideoData>, Vec<u8>), String> {
        let native = NativeVideoRuntime::decode_isobmff(&self.source)
            .ok_or_else(|| "modality codec failure".to_string())?;
        let mut value = native.normalized_data();
        value.shots = self
            .bundle
            .evidence_loci
            .iter()
            .filter(|locus| self.target_resources.contains(locus.id.as_ref().as_str()))
            .filter_map(|locus| match &locus.address {
                EvidenceAddress::VideoTimeRange { start_ms, end_ms } => {
                    Some(VideoShot::new(*start_ms, *end_ms))
                }
                _ => None,
            })
            .collect();
        let source = self.source;
        Ok((
            ServedIngest {
                idempotency_ref: self.idempotency,
                target_occurrence_id: self.target,
                expected_version: self.expected_version,
                bundle: self.bundle,
                value,
            },
            source,
        ))
    }
}

struct DecodedBatch {
    items: Vec<DecodedIngest>,
}

impl DecodedBatch {
    fn decode(
        items: Vec<ServedModalityIngestItem>,
        modality: ServedModalityKind,
        authority: &ModalityAuthority,
    ) -> Result<Self, String> {
        if !(1..=MAX_INGEST_STREAM_ITEMS).contains(&items.len()) {
            return Err("modality ingest stream cardinality is outside bounds".to_string());
        }
        Ok(Self {
            items: items
                .into_iter()
                .map(|item| DecodedIngest::decode(item, modality, authority))
                .collect::<Result<_, _>>()?,
        })
    }

    fn into_typed(
        self,
        modality: ServedModalityKind,
        authority: &ModalityAuthority,
    ) -> Result<TypedIngestBatch, String> {
        match modality {
            ServedModalityKind::Document => {
                let lexemes = |term: &str| {
                    authority
                        .lexeme_ref(term)
                        .ok()
                        .map(|reference| reference.to_string())
                };
                let pairs: Vec<(ServedIngest<DocumentData>, Vec<u8>)> = self
                    .items
                    .into_iter()
                    .map(|item| item.into_document(&lexemes))
                    .collect::<Result<_, _>>()?;
                let (commands, sources) = pairs.into_iter().unzip();
                Ok(TypedIngestBatch::Document { commands, sources })
            }
            ServedModalityKind::Image => {
                let pairs: Vec<(ServedIngest<ImageData>, Vec<u8>)> = self
                    .items
                    .into_iter()
                    .map(DecodedIngest::into_image)
                    .collect::<Result<_, _>>()?;
                let (commands, sources) = pairs.into_iter().unzip();
                Ok(TypedIngestBatch::Image { commands, sources })
            }
            ServedModalityKind::Audio => {
                let pairs: Vec<(ServedIngest<AudioData>, Vec<u8>)> = self
                    .items
                    .into_iter()
                    .map(DecodedIngest::into_audio)
                    .collect::<Result<_, _>>()?;
                let (commands, sources) = pairs.into_iter().unzip();
                Ok(TypedIngestBatch::Audio { commands, sources })
            }
            ServedModalityKind::Video => {
                let pairs: Vec<(ServedIngest<VideoData>, Vec<u8>)> = self
                    .items
                    .into_iter()
                    .map(DecodedIngest::into_video)
                    .collect::<Result<_, _>>()?;
                let (commands, sources) = pairs.into_iter().unzip();
                Ok(TypedIngestBatch::Video { commands, sources })
            }
        }
    }
}

enum TypedIngestBatch {
    Document {
        commands: Vec<ServedIngest<DocumentData>>,
        sources: Vec<Vec<u8>>,
    },
    Image {
        commands: Vec<ServedIngest<ImageData>>,
        sources: Vec<Vec<u8>>,
    },
    Audio {
        commands: Vec<ServedIngest<AudioData>>,
        sources: Vec<Vec<u8>>,
    },
    Video {
        commands: Vec<ServedIngest<VideoData>>,
        sources: Vec<Vec<u8>>,
    },
}

impl TypedIngestBatch {
    fn apply(
        self,
        core: &GraphCore,
        authority: &ModalityAuthority,
        modality: ServedModalityKind,
    ) -> Result<Vec<ApplyOutcome>, String> {
        match self {
            Self::Document { commands, sources } => {
                apply_commands(core, authority, modality, commands, sources)
            }
            Self::Image { commands, sources } => {
                apply_commands(core, authority, modality, commands, sources)
            }
            Self::Audio { commands, sources } => {
                apply_commands(core, authority, modality, commands, sources)
            }
            Self::Video { commands, sources } => {
                apply_commands(core, authority, modality, commands, sources)
            }
        }
    }
}

fn apply_commands<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    commands: Vec<ServedIngest<T>>,
    sources: Vec<Vec<u8>>,
) -> Result<Vec<ApplyOutcome>, String>
where
    T: GovernedModality
        + Clone
        + PartialEq
        + std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    let mut runtime: ServedModalityRuntime<T> = load_runtime(core, authority, modality)?;
    let before_records = capture_befores(&runtime, &commands);
    let outcomes = runtime
        .ingest_stream(commands)
        .map_err(|error| error.to_string())?;
    store_runtime_excluding_sources(core, authority, modality, &runtime, &sources)?;
    shadow_write_deltas(
        core,
        authority,
        modality,
        &runtime,
        &before_records,
        &outcomes,
    )?;
    Ok(outcomes)
}

pub(super) fn ingest_stream(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
    items: Vec<ServedModalityIngestItem>,
) -> Result<Vec<ApplyOutcome>, String> {
    let decoded = DecodedBatch::decode(items, modality, authority)?;
    let typed = decoded.into_typed(modality, authority)?;
    typed.apply(core, authority, modality)
}
