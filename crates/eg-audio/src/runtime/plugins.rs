use eg_modality::OpaqueRef;

use crate::AudioData;

use super::{DiarizationSegment, TranscriptAlignment};

pub trait DiarizationPlugin {
    fn diarize(&self, audio: &AudioData) -> Vec<DiarizationSegment>;
}

pub trait TranscriptAlignmentPlugin {
    fn align(
        &self,
        audio: &AudioData,
        transcript_ref: &OpaqueRef,
        unit_count: u32,
    ) -> Vec<TranscriptAlignment>;
}
