use super::ProgramModality;
use eg_modality::EvidenceAddress;

#[test]
fn temporal_modalities_distinguish_single_frames_from_empty_time_ranges() {
    let frame = EvidenceAddress::FrameRange {
        start_frame: 3,
        end_frame: 3,
    };
    assert!(ProgramModality::Video.accepts(&frame));
    assert!(!ProgramModality::Audio.accepts(&frame));
    let time = EvidenceAddress::VideoTimeRange {
        start_ms: 3,
        end_ms: 3,
    };
    assert!(!ProgramModality::Video.accepts(&time));
    let audio = EvidenceAddress::AudioRange {
        start_ms: 3,
        end_ms: 4,
    };
    assert!(ProgramModality::Audio.accepts(&audio));
    assert!(!ProgramModality::Video.accepts(&audio));
}

#[test]
fn image_evidence_requires_finite_coordinates_and_positive_extents() {
    let finite = EvidenceAddress::ImageRegion {
        x: -1.0,
        y: -2.0,
        width: 1.0,
        height: 2.0,
    };
    assert!(ProgramModality::Image.accepts(&finite));
    let non_finite = EvidenceAddress::ImageRegion {
        x: f64::NAN,
        y: 0.0,
        width: 1.0,
        height: 2.0,
    };
    assert!(!ProgramModality::Image.accepts(&non_finite));
    let empty = EvidenceAddress::ImageRegion {
        x: 0.0,
        y: 0.0,
        width: 0.0,
        height: 2.0,
    };
    assert!(!ProgramModality::Image.accepts(&empty));
}

#[test]
fn cell_ranges_are_inclusive_and_still_bound_to_table_modalities() {
    let single_cell = EvidenceAddress::TableCellRange {
        row_start: 2,
        row_end: 2,
        col_start: 3,
        col_end: 3,
    };
    assert!(ProgramModality::Table.accepts(&single_cell));
    assert!(ProgramModality::Tensor.accepts(&single_cell));
    assert!(!ProgramModality::Graph.accepts(&single_cell));
    let reversed = EvidenceAddress::TableCellRange {
        row_start: 3,
        row_end: 2,
        col_start: 3,
        col_end: 3,
    };
    assert!(!ProgramModality::Table.accepts(&reversed));
}
