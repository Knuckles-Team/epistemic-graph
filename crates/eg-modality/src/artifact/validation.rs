use std::collections::BTreeSet;

use super::{
    Artifact, ArtifactBundle, Derivation, EvidenceAddress, EvidenceLocus, Feature, Occurrence,
    ProtocolError, Rendition, ResourceId, Segment,
};

pub(super) fn validate_evidence_address(address: &EvidenceAddress) -> Result<(), ProtocolError> {
    valid_address(address)
        .then_some(())
        .ok_or(ProtocolError::InvalidEvidenceAddress)
}

fn valid_address(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::CharacterRange { start, end }
        | EvidenceAddress::AudioRange {
            start_ms: start,
            end_ms: end,
        }
        | EvidenceAddress::VideoTimeRange {
            start_ms: start,
            end_ms: end,
        }
        | EvidenceAddress::MetricWindow {
            start_ms: start,
            end_ms: end,
        } => strictly_ordered(*start, *end),
        EvidenceAddress::FrameRange {
            start_frame,
            end_frame,
        } => ordered_or_equal(*start_frame, *end_frame),
        EvidenceAddress::CodeSymbol {
            start_line,
            end_line,
            ..
        } => end_line >= start_line,
        EvidenceAddress::TableCellRange {
            row_start,
            row_end,
            col_start,
            col_end,
        } => ordered_or_equal(*row_start, *row_end) && ordered_or_equal(*col_start, *col_end),
        EvidenceAddress::ImageRegion {
            x,
            y,
            width,
            height,
        }
        | EvidenceAddress::PageRegion {
            x,
            y,
            width,
            height,
            ..
        } => eg_types::contract::valid_evidence_region(*x, *y, *width, *height),
        EvidenceAddress::Point { x, y } => x.is_finite() && y.is_finite(),
        EvidenceAddress::RowVersion { .. } | EvidenceAddress::TraceSpan { .. } => true,
    }
}

fn strictly_ordered(start: u64, end: u64) -> bool {
    end > start
}

fn ordered_or_equal(start: u64, end: u64) -> bool {
    end >= start
}

pub(super) fn validate_bundle(bundle: &ArtifactBundle) -> Result<(), ProtocolError> {
    validate_bundle_header(bundle)?;

    let artifacts = super::unique(bundle.artifacts.iter().map(|value| value.id.as_ref()))?;
    let occurrences = super::unique(bundle.occurrences.iter().map(|value| value.id.as_ref()))?;
    let renditions = super::unique(bundle.renditions.iter().map(|value| value.id.as_ref()))?;
    let segments = super::unique(bundle.segments.iter().map(|value| value.id.as_ref()))?;
    let features = super::unique(bundle.features.iter().map(|value| value.id.as_ref()))?;
    let loci = super::unique(bundle.evidence_loci.iter().map(|value| value.id.as_ref()))?;
    let (derivations, policy_refs) = collect_derivation_metadata(bundle)?;

    validate_artifacts(&bundle.artifacts)?;
    validate_occurrences(&bundle.occurrences, &artifacts)?;
    let validate_derivation = |derivation: &Derivation| {
        bundle.validate_derivation(
            derivation,
            &artifacts,
            &occurrences,
            &renditions,
            &segments,
            &features,
            &loci,
        )
    };
    validate_renditions(&bundle.renditions, &occurrences, &validate_derivation)?;
    validate_segments(&bundle.segments, &renditions, &segments)?;
    let validate_resource = |resource: &ResourceId| {
        super::validate_resource(
            resource,
            &artifacts,
            &occurrences,
            &renditions,
            &segments,
            &features,
            &loci,
        )
    };
    validate_features(&bundle.features, &validate_resource, &validate_derivation)?;
    super::validate_acyclic_derivations(&bundle.renditions, &bundle.features)?;
    validate_loci(
        &bundle.evidence_loci,
        &derivations,
        &policy_refs,
        &validate_resource,
    )?;
    Ok(())
}

fn validate_bundle_header(bundle: &ArtifactBundle) -> Result<(), ProtocolError> {
    if bundle.protocol_version != super::ARTIFACT_PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion);
    }
    bundle.privacy.validate()?;
    if bundle.artifacts.is_empty() || bundle.occurrences.is_empty() {
        return Err(ProtocolError::IncompleteBundle);
    }
    Ok(())
}

fn collect_derivation_metadata(
    bundle: &ArtifactBundle,
) -> Result<(BTreeSet<&str>, BTreeSet<&str>), ProtocolError> {
    let derivation_values: Vec<&Derivation> = bundle
        .renditions
        .iter()
        .map(|value| &value.derivation)
        .chain(bundle.features.iter().map(|value| &value.derivation))
        .collect();
    let derivations: BTreeSet<&str> = derivation_values
        .iter()
        .map(|value| value.id.as_ref().as_str())
        .collect();
    for (index, left) in derivation_values.iter().enumerate() {
        if derivation_values[index + 1..]
            .iter()
            .any(|right| left.id == right.id && left != right)
        {
            return Err(ProtocolError::DuplicateIdentity);
        }
    }
    let policy_refs: BTreeSet<&str> = bundle
        .occurrences
        .iter()
        .map(|value| value.policy.access_policy_ref.as_str())
        .collect();
    Ok((derivations, policy_refs))
}

fn validate_artifacts(artifacts: &[Artifact]) -> Result<(), ProtocolError> {
    if artifacts.iter().any(|value| value.content_version == 0) {
        return Err(ProtocolError::IncompleteBundle);
    }
    Ok(())
}

fn validate_occurrences(
    occurrences: &[Occurrence],
    artifacts: &BTreeSet<&str>,
) -> Result<(), ProtocolError> {
    for occurrence in occurrences {
        if !artifacts.contains(occurrence.artifact_id.as_ref().as_str()) {
            return Err(ProtocolError::DanglingReference);
        }
        if occurrence.observation_version == 0 || occurrence.policy.purpose_refs.is_empty() {
            return Err(ProtocolError::IncompleteBundle);
        }
    }
    Ok(())
}

fn validate_renditions(
    renditions: &[Rendition],
    occurrences: &BTreeSet<&str>,
    validate_derivation: &impl Fn(&Derivation) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError> {
    for rendition in renditions {
        if !occurrences.contains(rendition.occurrence_id.as_ref().as_str()) {
            return Err(ProtocolError::DanglingReference);
        }
        validate_derivation(&rendition.derivation)?;
    }
    Ok(())
}

fn validate_segments(
    segments: &[Segment],
    renditions: &BTreeSet<&str>,
    segment_ids: &BTreeSet<&str>,
) -> Result<(), ProtocolError> {
    for segment in segments {
        if !renditions.contains(segment.rendition_id.as_ref().as_str()) {
            return Err(ProtocolError::DanglingReference);
        }
        if let Some(parent) = &segment.parent_segment_id {
            if !segment_ids.contains(parent.as_ref().as_str()) || parent == &segment.id {
                return Err(ProtocolError::DanglingReference);
            }
        }
    }
    Ok(())
}

fn validate_features(
    features: &[Feature],
    validate_resource: &impl Fn(&ResourceId) -> Result<(), ProtocolError>,
    validate_derivation: &impl Fn(&Derivation) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError> {
    for feature in features {
        validate_resource(&feature.subject)?;
        validate_derivation(&feature.derivation)?;
    }
    Ok(())
}

fn validate_loci(
    loci: &[EvidenceLocus],
    derivations: &BTreeSet<&str>,
    policy_refs: &BTreeSet<&str>,
    validate_resource: &impl Fn(&ResourceId) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError> {
    for locus in loci {
        validate_resource(&locus.subject)?;
        if !derivations.contains(locus.derivation_ref.as_ref().as_str())
            || !policy_refs.contains(locus.policy_ref.as_str())
        {
            return Err(ProtocolError::DanglingReference);
        }
        locus.address.validate()?;
    }
    Ok(())
}
