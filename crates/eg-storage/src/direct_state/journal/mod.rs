use super::{authority::*, contract::*, filesystem::*, generation::*, image::*, *};

mod current;
mod install;

pub use current::{
    CurrentCleanupRecovery, CurrentDurabilityRecovery, CurrentPromotion,
    DirectStateRecoveryCompletion, DurableCurrentImage, PendingAbandonRecovery, PendingAbandonment,
};
pub use install::{
    DurablePreparedJournal, DurablePublishedJournal, PreparedGenerationRecovery,
    PreparedJournalPublication, PreparedJournalRecovery, PublishedJournalPublication,
    PublishedJournalRecovery,
};
// The registry drives the install journal through these entry points; they keep the
// `pub(in crate::direct_state)` visibility they are declared with, and are re-exported
// here only because `install`/`current` are private children of this module.
pub(in crate::direct_state) use current::{
    promote_published_install_to_current, read_current_image, CurrentPointerPublication,
};
pub(in crate::direct_state) use install::{
    load_prepared_journal, load_published_journal, read_install_journal, replace_install_journal,
    VisiblePreparedState,
};

pub(in crate::direct_state) fn decode_canonical_durable_record<T>(
    bytes: &[u8],
    label: &str,
) -> Result<T, String>
where
    T: serde::de::DeserializeOwned + Serialize,
{
    let decoded = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_DIRECT_STATE_MANIFEST_BYTES,
            MAX_DIRECT_STATE_CHUNK_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| format!("{label} is invalid or exceeds resource limits"))?;
    let canonical = rmp_serde::to_vec_named(&decoded)
        .map_err(|error| format!("encode canonical {label}: {error}"))?;
    if canonical != bytes {
        return Err(format!("{label} is not the exact canonical encoding"));
    }
    Ok(decoded)
}

fn validate_canonical_durable_record<T>(file: &File, value: &T, label: &str) -> Result<(), String>
where
    T: Serialize,
{
    let canonical = rmp_serde::to_vec_named(value)
        .map_err(|error| format!("encode canonical {label}: {error}"))?;
    if canonical.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
        return Err(format!("{label} exceeds its byte bound"));
    }
    validate_file_content(file, canonical.len() as u64, &hex_sha256(&canonical))
}

/// One aggregate, crash-recoverable V6 direct-state installation.  Providers do
/// not create per-domain journals.  The snapshot coordinator persists this one
/// canonical record before publishing any staged handle and retains it until all
/// domains and the final applied-index barrier are durable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateInstallJournalV1 {
    pub schema_version: u16,
    pub snapshot_sha256: String,
    pub scope: DirectStateScope,
    pub phase: DirectStateInstallPhase,
    pub sections: Vec<DirectStateInstallSectionV1>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectStateInstallPhase {
    Prepared,
    Published,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateInstallSectionV1 {
    /// Immutable received authority retained until this install becomes Current.
    pub source_manifest: DirectStateSectionManifestV1,
    /// Basename relative to the provider's private incoming/spool directory.
    pub incoming_file: String,
    pub generation_manifest: DirectStateGenerationManifestV1,
    /// Basename relative to the coordinator-owned generation directory.
    pub generation_file: String,
}

/// Mutable post-adoption generation authority. Snapshot-time bytes live only in
/// `source_manifest`; this identity deliberately has no content digest because
/// opening and ordinary writes mutate the live file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateGenerationManifestV1 {
    pub schema_version: u16,
    pub domain: DirectStateDomain,
    pub scope: DirectStateScope,
    pub owner_manifest_sha256: String,
    pub source_manifest_sha256: String,
    /// Immutable incarnation/root identity of the exact mutable generation. For
    /// MutationStore this is `owner_authority_digest()`; PlainRedb providers must
    /// persist and validate an equally non-forgeable per-generation identity.
    pub dynamic_store_authority_digest: Option<[u8; 32]>,
    /// Typed install-time digest of the provider's deterministic logical recovery
    /// evidence after adoption. Pending recomputes it before publication; Current
    /// does not compare it after ordinary writes mutate the live authority.
    pub install_evidence_sha256: [u8; 32],
}

impl DirectStateGenerationManifestV1 {
    pub fn validate(&self, configured_max_bytes: u64) -> Result<(), String> {
        if self.schema_version != DIRECT_STATE_SCHEMA_VERSION {
            return Err("unsupported direct-state generation manifest schema".into());
        }
        self.scope.validate()?;
        validate_sha256("direct-state owner manifest", &self.owner_manifest_sha256)?;
        validate_sha256("direct-state source manifest", &self.source_manifest_sha256)?;
        if configured_max_bytes > HARD_MAX_DIRECT_STATE_BYTES {
            return Err("configured direct-state generation bound exceeds hard limit".into());
        }
        if self.install_evidence_sha256 == [0_u8; 32] {
            return Err("direct-state install evidence digest is absent".into());
        }
        if self
            .dynamic_store_authority_digest
            .is_none_or(|digest| digest == [0_u8; 32])
        {
            return Err("direct-state physical generation authority is absent".into());
        }
        Ok(())
    }

    pub fn sha256(&self) -> Result<String, String> {
        self.validate(HARD_MAX_DIRECT_STATE_BYTES)?;
        let bytes = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state generation manifest: {error}"))?;
        if bytes.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
            return Err("direct-state generation manifest exceeds its byte bound".into());
        }
        Ok(hex_sha256(&bytes))
    }
}

/// Durable pointer to the one fully published generation set.  It survives
/// ordinary process restart; the transient install journal never substitutes for
/// this authority.  All seven sections advance together by atomic pointer replace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateCurrentImageV1 {
    pub schema_version: u16,
    pub snapshot_sha256: String,
    pub scope: DirectStateScope,
    pub sections: Vec<DirectStateInstallSectionV1>,
}

pub(super) fn section_for_domain(
    sections: &[DirectStateInstallSectionV1],
    domain: DirectStateDomain,
) -> Option<&DirectStateInstallSectionV1> {
    sections
        .iter()
        .find(|row| row.generation_manifest.domain == domain)
}

impl DirectStateCurrentImageV1 {
    pub fn validate(&self) -> Result<(), String> {
        DirectStateInstallJournalV1 {
            schema_version: self.schema_version,
            snapshot_sha256: self.snapshot_sha256.clone(),
            scope: self.scope.clone(),
            phase: DirectStateInstallPhase::Published,
            sections: self.sections.clone(),
        }
        .validate()
    }

    pub(super) fn sha256(&self) -> Result<String, String> {
        self.validate()?;
        let bytes = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state Current image: {error}"))?;
        Ok(hex_sha256(&bytes))
    }
}

pub(super) fn current_image_from_journal(
    journal: &DirectStateInstallJournalV1,
) -> Result<DirectStateCurrentImageV1, String> {
    journal.validate()?;
    let current = DirectStateCurrentImageV1 {
        schema_version: journal.schema_version,
        snapshot_sha256: journal.snapshot_sha256.clone(),
        scope: journal.scope.clone(),
        sections: journal.sections.clone(),
    };
    current.validate()?;
    Ok(current)
}

pub(super) fn sections_sha256(sections: &[DirectStateInstallSectionV1]) -> Result<String, String> {
    let bytes = rmp_serde::to_vec_named(sections)
        .map_err(|error| format!("encode direct-state section set: {error}"))?;
    if bytes.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
        return Err("direct-state section set exceeds its byte bound".into());
    }
    Ok(hex_sha256(&bytes))
}

/// Complete closed-domain authority built off-serving and bound to one exact durable
/// Prepared journal. There is no per-domain publication state.
pub struct PreparedWholeGeneration {
    pub(super) journal: DurablePreparedJournal,
    pub(super) assembled: AssembledDirectStateGeneration,
}

impl PreparedWholeGeneration {
    pub fn journal(&self) -> &DurablePreparedJournal {
        &self.journal
    }

    pub fn assembled(&self) -> &AssembledDirectStateGeneration {
        &self.assembled
    }

    pub fn into_assembled(self) -> AssembledDirectStateGeneration {
        self.assembled
    }
}

/// Closed recovery authority for either in-flight journal phase. Construction
/// requires a path-pinned durable token; a structurally valid in-memory journal
/// cannot authorize provider publication.
pub enum DurablePendingJournal<'a> {
    Prepared(&'a DurablePreparedJournal),
    Published(&'a DurablePublishedJournal),
}

impl DurablePendingJournal<'_> {
    pub(super) fn journal(&self) -> &DirectStateInstallJournalV1 {
        match self {
            Self::Prepared(token) => &token.journal,
            Self::Published(token) => &token.journal,
        }
    }

    pub(super) fn validate_live(&self) -> Result<(), String> {
        match self {
            Self::Prepared(token) => token.validate_live(),
            Self::Published(token) => token.validate_live(),
        }
    }

    pub(super) fn validate_affinity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        match self {
            Self::Prepared(token) => token.validate_affinity(expected),
            Self::Published(token) => token.validate_affinity(expected),
        }
    }

    pub(super) fn validate_registry_affinity(
        &self,
        expected: &Arc<DirectStateRegistryIdentity>,
    ) -> Result<(), String> {
        match self {
            Self::Prepared(token) => token.validate_registry_affinity(expected),
            Self::Published(token) => token.validate_registry_affinity(expected),
        }
    }

    pub(super) fn path(&self) -> &Path {
        match self {
            Self::Prepared(token) => &token.path,
            Self::Published(token) => &token.path,
        }
    }

    pub(super) fn try_clone_authority(&self) -> Result<File, String> {
        match self {
            Self::Prepared(token) => token
                .authority_file
                .try_clone()
                .map_err(|error| format!("clone Prepared journal authority: {error}")),
            Self::Published(token) => token
                .authority_file
                .try_clone()
                .map_err(|error| format!("clone Published journal authority: {error}")),
        }
    }
}

impl DirectStateInstallJournalV1 {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != DIRECT_STATE_SCHEMA_VERSION {
            return Err("unsupported direct-state install journal schema".to_string());
        }
        self.scope.validate()?;
        validate_sha256("direct-state snapshot", &self.snapshot_sha256)?;
        if self.sections.len() != DirectStateDomain::ALL.len() {
            return Err(
                "direct-state install journal does not contain the closed domain set".into(),
            );
        }
        let mut previous = None;
        let mut capture_set_sha256: Option<&str> = None;
        let mut source_generation_sha256: Option<&str> = None;
        for (expected_domain, section) in DirectStateDomain::ALL.iter().zip(&self.sections) {
            validate_section_authority(*expected_domain, &self.scope, section)?;
            validate_section_set(
                section,
                &mut previous,
                &mut capture_set_sha256,
                &mut source_generation_sha256,
            )?;
            validate_section_names(section)?;
            previous = Some(section.generation_manifest.domain);
        }
        validate_aggregate_section_bounds(
            self.sections.iter().map(|section| &section.source_manifest),
            HARD_MAX_DIRECT_STATE_BYTES,
        )?;
        let encoded = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state install journal: {error}"))?;
        if encoded.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
            return Err("direct-state install journal exceeds its byte bound".into());
        }
        Ok(())
    }

    pub(super) fn sha256(&self) -> Result<String, String> {
        self.validate()?;
        let bytes = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state install journal: {error}"))?;
        Ok(hex_sha256(&bytes))
    }
}

fn validate_section_authority(
    expected_domain: DirectStateDomain,
    scope: &DirectStateScope,
    section: &DirectStateInstallSectionV1,
) -> Result<(), String> {
    if section.generation_manifest.domain != expected_domain {
        return Err("direct-state install journal domain set is incomplete or reordered".into());
    }
    if section.generation_manifest.scope != *scope {
        return Err("direct-state section scope differs from its install journal".into());
    }
    section
        .source_manifest
        .validate(HARD_MAX_DIRECT_STATE_BYTES)?;
    section
        .generation_manifest
        .validate(HARD_MAX_DIRECT_STATE_BYTES)?;
    if section.source_manifest.domain != expected_domain
        || section.source_manifest.scope != *scope
        || section.source_manifest.owner_manifest_sha256
            != section.generation_manifest.owner_manifest_sha256
        || section.source_manifest.sha256()? != section.generation_manifest.source_manifest_sha256
    {
        return Err("direct-state source and mutable generation authorities disagree".into());
    }
    Ok(())
}

fn validate_section_set<'a>(
    section: &'a DirectStateInstallSectionV1,
    previous: &mut Option<DirectStateDomain>,
    capture_set_sha256: &mut Option<&'a str>,
    source_generation_sha256: &mut Option<&'a str>,
) -> Result<(), String> {
    match (*capture_set_sha256, *source_generation_sha256) {
        (None, None) => {
            *capture_set_sha256 = Some(&section.source_manifest.capture_set_sha256);
            *source_generation_sha256 = Some(&section.source_manifest.source_generation_sha256);
        }
        (Some(capture_set), Some(source_generation))
            if capture_set == section.source_manifest.capture_set_sha256
                && source_generation == section.source_manifest.source_generation_sha256 => {}
        _ => return Err("direct-state install journal mixes capture/source generations".into()),
    }
    if previous.is_some_and(|domain| domain >= section.generation_manifest.domain) {
        return Err("direct-state install journal sections are duplicated or unordered".into());
    }
    Ok(())
}

fn validate_section_names(section: &DirectStateInstallSectionV1) -> Result<(), String> {
    validate_relative_basename(&section.incoming_file)?;
    let expected_incoming = incoming_file_name(
        section.source_manifest.domain,
        &section.source_manifest.sha256()?,
    )?;
    if section.incoming_file != expected_incoming {
        return Err("direct-state incoming name does not match its manifest".into());
    }
    validate_relative_basename(&section.generation_file)?;
    let expected_name = generation_file_name(
        section.generation_manifest.domain,
        &section.generation_manifest.sha256()?,
    )?;
    if section.generation_file != expected_name {
        return Err("direct-state generation name does not match its manifest".into());
    }
    Ok(())
}
