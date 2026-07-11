//! OPT-IN video runtime seam (CONCEPT:EG-P1-3), behind this crate's own `runtime`
//! feature (default OFF, no new dependency).
//!
//! `eg-video`'s default build is deliberately metadata/shot-level (see the
//! crate's module docs) — no frame decode, ever, in the default build. Real
//! video-runtime work (container/track inspection, keyframe extraction, shot/
//! scene detection, caption generation, temporal embeddings) needs exactly the
//! heavy native dependencies (`ffmpeg`/ a vision-language model runtime, …) this
//! workspace's Pi contract keeps out of the default build. So this module, gated
//! behind `runtime`, defines ONLY the typed artifacts + the plugin TRAITS a real
//! demuxer/detector/model would implement — it adds no dependency at all, even
//! with `runtime` on, because it contains no implementation to depend on
//! anything with.
//!
//! * [`ContainerInfo`] / [`TrackInfo`] / [`VideoContainerPlugin`] — container +
//!   per-track inspection (video/audio/subtitle tracks, codecs).
//! * [`Keyframe`] / [`KeyframePlugin`] — keyframe extraction.
//! * [`SceneBoundary`] / [`SceneDetectionPlugin`] — shot/scene-boundary detection.
//! * [`Caption`] / [`CaptionPlugin`] — caption/subtitle generation.
//! * [`TemporalEmbeddingWindow`] / [`TemporalEmbeddingPlugin`] — a windowed
//!   temporal-embedding hook.
//!
//! [`NoopVideoRuntime`] implements every trait as a trivial, always-empty
//! placeholder — enough to prove the trait seam end-to-end without claiming any
//! real inspection/detection occurred. Real demuxers/detectors/captioners/
//! embedding models are explicitly OUT of scope — documented follow-ups,
//! implemented as external plugins.

use crate::VideoData;

/// One track inside a container (video/audio/subtitle), with its codec name.
#[derive(Clone, Debug, PartialEq)]
pub struct TrackInfo {
    pub track_id: u32,
    pub kind: String,
    pub codec: String,
}

/// Container-level inspection: the container format name plus its tracks.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ContainerInfo {
    pub container: String,
    pub tracks: Vec<TrackInfo>,
}

/// One extracted keyframe, optionally stored as its own blob.
#[derive(Clone, Debug, PartialEq)]
pub struct Keyframe {
    pub time_ms: u64,
    pub blob_ref: Option<String>,
}

/// One detected shot/scene boundary time range, with an optional label.
#[derive(Clone, Debug, PartialEq)]
pub struct SceneBoundary {
    pub start_ms: u64,
    pub end_ms: u64,
    pub label: Option<String>,
}

/// One generated caption/subtitle over a time range.
#[derive(Clone, Debug, PartialEq)]
pub struct Caption {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
}

/// One windowed temporal-embedding vector.
#[derive(Clone, Debug, PartialEq)]
pub struct TemporalEmbeddingWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub vector: Vec<f32>,
}

/// Inspect a video's container + tracks. `None` means "cannot inspect" (e.g.
/// unrecognized container).
pub trait VideoContainerPlugin {
    fn inspect(&self, video: &VideoData) -> Option<ContainerInfo>;
}

/// Extract keyframes over the whole video.
pub trait KeyframePlugin {
    fn keyframes(&self, video: &VideoData) -> Vec<Keyframe>;
}

/// Detect shot/scene boundaries over the whole video.
pub trait SceneDetectionPlugin {
    fn scenes(&self, video: &VideoData) -> Vec<SceneBoundary>;
}

/// Generate captions over the whole video.
pub trait CaptionPlugin {
    fn captions(&self, video: &VideoData) -> Vec<Caption>;
}

/// Compute windowed temporal embeddings over the whole video.
pub trait TemporalEmbeddingPlugin {
    fn embed(&self, video: &VideoData) -> Vec<TemporalEmbeddingWindow>;
}

/// A trivial, dependency-free placeholder implementing EVERY runtime trait: never
/// fabricates a result (`None`/empty `Vec`s) — proving the trait wiring compiles
/// and runs end-to-end without claiming any real inspection/detection occurred.
/// Real implementations are documented, out-of-scope follow-ups.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopVideoRuntime;

impl VideoContainerPlugin for NoopVideoRuntime {
    fn inspect(&self, _video: &VideoData) -> Option<ContainerInfo> {
        None
    }
}

impl KeyframePlugin for NoopVideoRuntime {
    fn keyframes(&self, _video: &VideoData) -> Vec<Keyframe> {
        Vec::new()
    }
}

impl SceneDetectionPlugin for NoopVideoRuntime {
    fn scenes(&self, _video: &VideoData) -> Vec<SceneBoundary> {
        Vec::new()
    }
}

impl CaptionPlugin for NoopVideoRuntime {
    fn captions(&self, _video: &VideoData) -> Vec<Caption> {
        Vec::new()
    }
}

impl TemporalEmbeddingPlugin for NoopVideoRuntime {
    fn embed(&self, _video: &VideoData) -> Vec<TemporalEmbeddingWindow> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_runtime_never_fabricates_any_result() {
        let video = VideoData::new(9000, "blob-1");
        assert_eq!(
            VideoContainerPlugin::inspect(&NoopVideoRuntime, &video),
            None
        );
        assert!(KeyframePlugin::keyframes(&NoopVideoRuntime, &video).is_empty());
        assert!(SceneDetectionPlugin::scenes(&NoopVideoRuntime, &video).is_empty());
        assert!(CaptionPlugin::captions(&NoopVideoRuntime, &video).is_empty());
        assert!(TemporalEmbeddingPlugin::embed(&NoopVideoRuntime, &video).is_empty());
    }
}
