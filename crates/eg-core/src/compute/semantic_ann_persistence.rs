//! Strict arena serde and identity-bound ANN artifact persistence.

use super::*;

/// Store-level identity persisted beside an ANN artifact. `AnnIndex` owns the
/// quantized rows, while this closed manifest binds those rows to the exact
/// raw-vector member set, width, and (when available) complete model space.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedIndexManifest {
    pub(super) version: u8,
    pub(super) dimension: usize,
    pub(super) members: Vec<String>,
    pub(super) space: Option<EmbeddingSpaceRef>,
}

impl Serialize for SemanticStore {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SemanticStore", 4)?;
        state.serialize_field("dim", &self.arena.dim)?;
        state.serialize_field("ids", &self.arena.ids)?;
        state.serialize_field("data", &self.arena.data)?;
        state.serialize_field("space", &self.space)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SemanticStore {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            dim: usize,
            ids: Vec<String>,
            data: Vec<f32>,
            #[serde(default)]
            space: Option<EmbeddingSpaceRef>,
        }
        let raw = Raw::deserialize(deserializer)?;
        if let Some(space) = raw.space.as_ref() {
            space.validate().map_err(serde::de::Error::custom)?;
            if raw.dim != 0 && raw.dim != space.dimensions {
                return Err(serde::de::Error::custom(format!(
                    "semantic store space declares {} dimensions but persisted rows carry {}",
                    space.dimensions, raw.dim
                )));
            }
        }
        if raw.dim > MAX_GENERIC_DIMENSION {
            return Err(serde::de::Error::custom(format!(
                "semantic store dimension {} exceeds the generic maximum of {}",
                raw.dim, MAX_GENERIC_DIMENSION
            )));
        }
        let arena = EmbeddingArena::from_flat(raw.dim, raw.ids, raw.data)
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            arena,
            space: raw.space,
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        })
    }
}

/// One index generation's complete durable image: the IVF-PQ artifact and the
/// encoded manifest that binds it to an exact member set, width and embedding
/// space. Owned bytes; this type reaches no store and no filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticGenerationImage {
    /// The `AnnIndex` artifact (eg-ann code buffers + the row-id sidecar).
    pub index: AnnIndexImage,
    /// The encoded, versioned `PersistedIndexManifest`.
    pub manifest: Vec<u8>,
}

impl SemanticGenerationImage {
    /// The generation's declared model identity: `(dimensions, model digest)`,
    /// decoded from its own manifest.
    ///
    /// This is what a durable binding authority is compared against, so it is
    /// read from the artifact rather than from the resident store: an image
    /// that describes a different model or width than the binding was created
    /// with must be refusable without a resident arena to compare to.
    pub fn identity(&self) -> std::io::Result<(usize, Option<String>)> {
        let manifest = decode_index_manifest(&self.manifest)?;
        Ok((
            manifest.dimension,
            manifest.space.as_ref().map(|space| space.digest.clone()),
        ))
    }
}

impl SemanticStore {
    /// Take this store's index generation as a durable image: the IVF-PQ
    /// artifact plus the exact live member/space manifest.
    ///
    /// RF-RULING-007: the image is returned, never written. Its durable leg is
    /// one admitted `Native(SemanticIndex)` mutation held by
    /// `compute::semantic_ann_codes`, so activation and retirement of a
    /// generation are ordered, fenced and replayable like every other write.
    /// The retired `save_index(dir)` wrote three files and a manifest beside
    /// the process with no admission at all.
    pub fn export_generation(&self) -> std::io::Result<SemanticGenerationImage> {
        self.ensure_index("export_generation");
        let mut index = self.index.write();
        let ann = index.as_mut().ok_or_else(|| {
            std::io::Error::other(
                "no ANN index to persist (store empty, below build threshold, or wider than maintained ANN)",
            )
        })?;
        validate_index_dimensions(ann, self.arena.dim)?;
        // Drop tombstones before taking the identity image. This makes the
        // exported index's rows exactly the current arena member set.
        ann.compact();
        let members = matching_members(ann, &self.arena.ids)?;
        let space = validated_space(self.space.as_ref())?;
        let manifest = IndexManifest {
            version: 1,
            dimension: ann.dim(),
            members,
            space,
        };
        Ok(SemanticGenerationImage {
            index: ann.to_image()?,
            manifest: encode_index_manifest(&manifest)?,
        })
    }

    /// Make an image resident and serving. The generation is adopted only when
    /// its identity matches the resident arena and complete embedding-space
    /// declaration; this is the activation step, and it sets `STATE_READY`.
    pub fn adopt_generation(&self, image: &SemanticGenerationImage) -> std::io::Result<()> {
        let manifest = decode_index_manifest(&image.manifest)?;
        let ann = AnnIndex::from_image(&image.index)?;
        validate_index_dimensions(&ann, self.arena.dim)?;
        validate_member_sets(&manifest, &ann, &self.arena.ids)?;
        validate_loaded_space(&manifest, self.space.as_ref())?;
        let n = ann.live_len();
        *self.index.write() = Some(ann);
        *self.built_len.write() = n;
        self.state.store(STATE_READY, Ordering::Release);
        Ok(())
    }
}

fn validate_index_dimensions(ann: &AnnIndex, arena_dim: usize) -> std::io::Result<()> {
    if ann.dim() == 0 || ann.dim() > MAX_MAINTAINED_DIMENSION {
        return Err(invalid_index_manifest(
            "ANN index dimension exceeds the maintained artifact ceiling",
        ));
    }
    if arena_dim != ann.dim() {
        return Err(invalid_index_manifest(
            "ANN index dimension does not match the resident embedding arena",
        ));
    }
    Ok(())
}

fn matching_members(ann: &AnnIndex, arena_ids: &[String]) -> std::io::Result<Vec<String>> {
    let members = canonical_members(ann.row_ids())?;
    if members != canonical_members(arena_ids)? {
        return Err(invalid_index_manifest(
            "ANN index member set does not match the resident embedding arena",
        ));
    }
    Ok(members)
}

fn validated_space(
    space: Option<&EmbeddingSpaceRef>,
) -> std::io::Result<Option<EmbeddingSpaceRef>> {
    let Some(space) = space else {
        return Ok(None);
    };
    space.validate().map_err(invalid_index_manifest)?;
    if space.dimensions > MAX_MAINTAINED_DIMENSION {
        return Err(invalid_index_manifest(
            "a wider exact-only embedding space cannot own a maintained ANN artifact",
        ));
    }
    Ok(Some(space.clone()))
}

fn validate_member_sets(
    manifest: &IndexManifest,
    ann: &AnnIndex,
    arena_ids: &[String],
) -> std::io::Result<()> {
    let arena_members = canonical_members(arena_ids)?;
    if manifest.members != arena_members || canonical_members(ann.row_ids())? != manifest.members {
        return Err(invalid_index_manifest(
            "persisted ANN member set does not match the resident embedding arena",
        ));
    }
    if ann.live_len() != arena_ids.len() {
        return Err(invalid_index_manifest(
            "persisted ANN live-row count does not match the resident embedding arena",
        ));
    }
    Ok(())
}

fn validate_loaded_space(
    manifest: &IndexManifest,
    current: Option<&EmbeddingSpaceRef>,
) -> std::io::Result<()> {
    if manifest.space.as_ref() != current {
        return Err(invalid_index_manifest(
            "persisted ANN embedding-space identity does not match the resident store",
        ));
    }
    Ok(())
}
