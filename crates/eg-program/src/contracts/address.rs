//! Typed evidence-address compatibility, including modality-specific boundaries.

use super::ProgramModality;
use eg_modality::EvidenceAddress;

pub(super) fn accepts(modality: ProgramModality, address: &EvidenceAddress) -> bool {
    match modality {
        ProgramModality::Text | ProgramModality::Document => accepts_character_range(address),
        ProgramModality::Image => accepts_image_region(address),
        ProgramModality::Audio | ProgramModality::Video | ProgramModality::TimeSeries => {
            accepts_temporal_address(modality, address)
        }
        ProgramModality::Graph | ProgramModality::Vector | ProgramModality::Binary => {
            accepts_row_version(address)
        }
        ProgramModality::Table | ProgramModality::Tensor => accepts_table_address(address),
        ProgramModality::Spatial => accepts_point(address),
        ProgramModality::Code => accepts_code_symbol(address),
        ProgramModality::Trace => accepts_trace_span(address),
    }
}

fn accepts_character_range(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::CharacterRange { start, end } => end > start,
        _ => false,
    }
}

fn accepts_image_region(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::ImageRegion {
            x,
            y,
            width,
            height,
        } => {
            x.is_finite()
                && y.is_finite()
                && width.is_finite()
                && height.is_finite()
                && *width > 0.0
                && *height > 0.0
        }
        _ => false,
    }
}

fn accepts_temporal_address(modality: ProgramModality, address: &EvidenceAddress) -> bool {
    match (modality, address) {
        (ProgramModality::Audio, EvidenceAddress::AudioRange { start_ms, end_ms })
        | (ProgramModality::Video, EvidenceAddress::VideoTimeRange { start_ms, end_ms })
        | (ProgramModality::TimeSeries, EvidenceAddress::MetricWindow { start_ms, end_ms }) => {
            end_ms > start_ms
        }
        (
            ProgramModality::Video,
            EvidenceAddress::FrameRange {
                start_frame,
                end_frame,
            },
        ) => end_frame >= start_frame,
        _ => false,
    }
}

fn accepts_row_version(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::RowVersion { version, .. } => *version > 0,
        _ => false,
    }
}

fn accepts_table_address(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::RowVersion { version, .. } => *version > 0,
        EvidenceAddress::TableCellRange {
            row_start,
            row_end,
            col_start,
            col_end,
        } => row_end >= row_start && col_end >= col_start,
        _ => false,
    }
}

fn accepts_point(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::Point { x, y } => x.is_finite() && y.is_finite(),
        _ => false,
    }
}

fn accepts_code_symbol(address: &EvidenceAddress) -> bool {
    match address {
        EvidenceAddress::CodeSymbol {
            start_line,
            end_line,
            ..
        } => end_line >= start_line,
        _ => false,
    }
}

fn accepts_trace_span(address: &EvidenceAddress) -> bool {
    matches!(address, EvidenceAddress::TraceSpan { .. })
}

#[cfg(test)]
mod tests;
