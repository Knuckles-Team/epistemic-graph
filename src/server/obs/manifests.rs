//! BUG-210 segment-manifest persistence: the per-stream durable prune index,
//! its recovery bounds, and the bounded per-stream publication.

use std::cell::Cell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::segment::SegmentManifest;
use super::snapshot::{
    read_snapshot_from_directory, write_snapshot_atomically_blocking_with, SnapshotDirectory,
    MAX_SNAPSHOT_BYTES, SNAPSHOT_WRITE_ERROR,
};
use super::{stream_storage_key, ObsState};

pub(super) const MANIFEST_DIRECTORY_ERROR: &str = "segment manifest directory is unavailable";
pub(super) const MANIFEST_READ_ERROR: &str = "segment manifest is unavailable";
pub(super) const MANIFEST_BOUNDS_ERROR: &str = "segment manifests exceed recovery bounds";
pub(super) const MAX_MANIFEST_FILES: usize = 16_384;
pub(super) const MAX_MANIFEST_TOTAL_BYTES: usize = MAX_SNAPSHOT_BYTES;
pub(super) const MAX_MANIFEST_TOTAL_ITEMS: usize = eg_types::msgpack::MAX_PROPERTY_ITEMS;

/// BUG-210 durable path for one stream's segment manifest list:
/// `{persist_dir}/obs/segments/<sanitized-stream>.msgpack`.
pub(super) fn segment_manifest_path(obs_base: &Path, stream: &str) -> PathBuf {
    obs_base
        .join("segments")
        .join(format!("{}.msgpack", stream_storage_key(stream)))
}

/// BUG-210 durable-tier recovery: rebuild the segment-manifest prune index
/// (`ObsState::segments`) from every `{persist_dir}/obs/segments/*.msgpack` side
/// file written by `persist_stream_segments`. Returns empty maps when the directory
/// does not exist yet in a fresh persist dir. Each file's bytes go through the same bounded
/// structural preflight `load_traces_snapshot` uses.
/// Per-stream `(bytes, items)` and the aggregate totals for the manifest files
/// that have completed durable publication. Named fields, because the bounds
/// check and the publication commit below both read all three together and a
/// transposed tuple index would silently compare bytes against items.
#[derive(Debug, Default)]
pub(super) struct SegmentManifestUsage {
    pub(super) per_stream: HashMap<String, (usize, usize)>,
    pub(super) bytes: usize,
    pub(super) items: usize,
}

/// Everything `load_segment_manifests` rebuilds from the durable side files: the
/// manifests themselves, the per-stream prune index into them, and their bounds.
#[derive(Debug)]
pub(super) struct LoadedSegmentManifests {
    pub(super) manifests: Vec<SegmentManifest>,
    pub(super) positions: HashMap<String, Vec<usize>>,
    pub(super) usage: SegmentManifestUsage,
}

pub(super) fn load_segment_manifests(obs_base: &Path) -> Result<LoadedSegmentManifests, String> {
    let Some(directory) = open_segment_manifest_directory(obs_base)? else {
        return Ok(LoadedSegmentManifests {
            manifests: Vec::new(),
            positions: HashMap::new(),
            usage: SegmentManifestUsage::default(),
        });
    };
    let names = segment_manifest_names(&directory)?;
    let limits = eg_types::msgpack::MsgpackLimits::new(
        eg_types::msgpack::MAX_PROPERTY_BYTES,
        eg_types::msgpack::MAX_PROPERTY_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    );
    let mut manifests = Vec::new();
    let mut positions: HashMap<String, Vec<usize>> = HashMap::new();
    let mut usage = HashMap::new();
    let mut total_bytes = 0usize;
    let mut total_items = 0usize;
    for name in names {
        let bytes = read_snapshot_from_directory(&directory, &name)?
            .ok_or_else(|| MANIFEST_READ_ERROR.to_string())?;
        include_manifest_budget(&mut total_bytes, bytes.len(), MAX_MANIFEST_TOTAL_BYTES)?;
        let stream_manifests: Vec<SegmentManifest> =
            eg_types::msgpack::decode_bounded(&bytes, limits)
                .map_err(|_| "segment manifest is invalid or exceeds its bounds".to_string())?;
        include_manifest_budget(
            &mut total_items,
            stream_manifests.len(),
            MAX_MANIFEST_TOTAL_ITEMS,
        )?;
        let stream = stream_manifests
            .first()
            .map(|manifest| manifest.stream.clone())
            .ok_or_else(|| "segment manifest is invalid or exceeds its bounds".to_string())?;
        let expected_name = format!("{}.msgpack", stream_storage_key(&stream));
        if name != std::ffi::OsStr::new(&expected_name)
            || stream_manifests
                .iter()
                .any(|manifest| manifest.stream != stream)
            || positions.contains_key(&stream)
        {
            return Err("segment manifest is invalid or exceeds its bounds".to_string());
        }
        let first_position = manifests.len();
        positions.insert(
            stream.clone(),
            (first_position..first_position + stream_manifests.len()).collect(),
        );
        usage.insert(stream.clone(), (bytes.len(), stream_manifests.len()));
        manifests.extend(stream_manifests);
    }
    directory.require_still_named(MANIFEST_DIRECTORY_ERROR)?;
    Ok(LoadedSegmentManifests {
        manifests,
        positions,
        usage: SegmentManifestUsage {
            per_stream: usage,
            bytes: total_bytes,
            items: total_items,
        },
    })
}

pub(super) fn include_manifest_budget(
    total: &mut usize,
    increment: usize,
    max: usize,
) -> Result<(), String> {
    *total = (*total)
        .checked_add(increment)
        .filter(|candidate| *candidate <= max)
        .ok_or_else(|| MANIFEST_BOUNDS_ERROR.to_string())?;
    Ok(())
}

fn open_segment_manifest_directory(obs_base: &Path) -> Result<Option<SnapshotDirectory>, String> {
    let base = SnapshotDirectory::open(obs_base, false, MANIFEST_DIRECTORY_ERROR)?;
    base.open_child_directory(std::ffi::OsStr::new("segments"), MANIFEST_DIRECTORY_ERROR)
}

fn segment_manifest_names(
    directory: &SnapshotDirectory,
) -> Result<Vec<std::ffi::OsString>, String> {
    let entries =
        std::fs::read_dir(&directory.io_path).map_err(|_| MANIFEST_DIRECTORY_ERROR.to_string())?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| MANIFEST_DIRECTORY_ERROR.to_string())?;
        let name = entry.file_name();
        if Path::new(&name).extension().and_then(|ext| ext.to_str()) != Some("msgpack") {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|_| MANIFEST_READ_ERROR.to_string())?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(MANIFEST_READ_ERROR.to_string());
        }
        if names.len() >= MAX_MANIFEST_FILES {
            return Err(MANIFEST_BOUNDS_ERROR.to_string());
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

fn encode_bounded_stream_manifests(
    state: &ObsState,
    target_stream: &str,
    pending_usage: &Cell<Option<(usize, usize, usize, usize)>>,
) -> Result<Vec<u8>, String> {
    let segments = state.segments.lock();
    let positions = state.manifest_positions.lock();
    let target_positions = positions
        .get(target_stream)
        .ok_or_else(|| SNAPSHOT_WRITE_ERROR.to_string())?;
    let target: Vec<&SegmentManifest> = target_positions
        .iter()
        .map(|position| {
            segments
                .get(*position)
                .ok_or_else(|| SNAPSHOT_WRITE_ERROR.to_string())
        })
        .collect::<Result<_, _>>()?;
    let bytes = rmp_serde::to_vec_named(&target).map_err(|_| SNAPSHOT_WRITE_ERROR.to_string())?;
    let usage = state.manifest_usage.lock();
    let previous = usage
        .per_stream
        .get(target_stream)
        .copied()
        .unwrap_or((0, 0));
    let file_count = usage.per_stream.len()
        + if usage.per_stream.contains_key(target_stream) {
            0
        } else {
            1
        };
    let total_bytes = usage
        .bytes
        .checked_sub(previous.0)
        .and_then(|total| total.checked_add(bytes.len()))
        .filter(|total| *total <= MAX_MANIFEST_TOTAL_BYTES);
    let total_items = usage
        .items
        .checked_sub(previous.1)
        .and_then(|total| total.checked_add(target_positions.len()))
        .filter(|total| *total <= MAX_MANIFEST_TOTAL_ITEMS);
    if file_count > MAX_MANIFEST_FILES || total_bytes.is_none() || total_items.is_none() {
        return Err(MANIFEST_BOUNDS_ERROR.to_string());
    }
    pending_usage.set(Some((
        bytes.len(),
        target_positions.len(),
        total_bytes.expect("checked above"),
        total_items.expect("checked above"),
    )));
    Ok(bytes)
}

pub(super) fn record_segment_manifest(
    segments: &Mutex<Vec<SegmentManifest>>,
    positions: &Mutex<HashMap<String, Vec<usize>>>,
    manifest: SegmentManifest,
) {
    let stream = manifest.stream.clone();
    let mut segments = segments.lock();
    let mut positions = positions.lock();
    let position = segments.len();
    segments.push(manifest);
    positions.entry(stream).or_default().push(position);
}

impl ObsState {
    /// BUG-210 durable tier: durably persist ONE stream's segment manifest list to
    /// `{persist_dir}/obs/segments/<sanitized-stream>.msgpack` (tmp-file + atomic
    /// rename, the SAME convention `persist_traces` uses). A no-op (`Ok(())`) when
    /// this instance has no configured durable persist dir (ephemeral/test
    /// instances) -- the manifest already lives only in RAM in that mode, mirroring
    /// `text_dir`'s existing "`None` ⇒ in-memory" contract.
    pub(super) fn persist_stream_segments(&self, stream: &str) -> Result<(), String> {
        let Some(base) = self.obs_base.as_deref() else {
            return Ok(());
        };
        let pending_usage = Cell::new(None);
        write_snapshot_atomically_blocking_with(
            segment_manifest_path(base, stream),
            || encode_bounded_stream_manifests(self, stream, &pending_usage),
            || Ok(()),
            || {
                let (bytes, items, total_bytes, total_items) = pending_usage
                    .take()
                    .expect("manifest usage is prepared before publication");
                let mut usage = self.manifest_usage.lock();
                usage.per_stream.insert(stream.to_string(), (bytes, items));
                usage.bytes = total_bytes;
                usage.items = total_items;
            },
        )
    }
}
