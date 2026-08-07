//! A from-scratch, minimal single-page PDF writer (D-VZ-1 lane V3a). No new
//! dependency (no `printpdf`): a hand-built object table + xref + content
//! stream of plain PDF path operators (`m`/`l`/`re`/`f`/`S`). PDF's coordinate
//! origin is bottom-left with y growing UP; [`RenderPlan`]'s coordinates are
//! screen-space (origin top-left, y growing DOWN, the same convention
//! [`crate::raster`]/[`crate::svg_writer`] use) — this module is the one place
//! that flips y (`height_px - y`) to PDF's convention, so `RenderPlan` itself
//! stays renderer-agnostic.
//!
//! Circles (Scatter's `DrawOp::Point`) are approximated as a 16-gon — PDF has
//! native Bézier curve operators (`c`), but a straight-edge polygon is simpler
//! and, at the marker radii this lane draws, visually indistinguishable.

use crate::plan::{DrawOp, RenderPlan};

fn unit_color(c: [u8; 4]) -> (f64, f64, f64) {
    (
        c[0] as f64 / 255.0,
        c[1] as f64 / 255.0,
        c[2] as f64 / 255.0,
    )
}

fn circle_polygon(cx: f64, cy: f64, r: f64) -> String {
    const SIDES: usize = 16;
    let mut s = String::new();
    for i in 0..SIDES {
        let theta = 2.0 * std::f64::consts::PI * (i as f64) / SIDES as f64;
        let (x, y) = (cx + r * theta.cos(), cy + r * theta.sin());
        s.push_str(&format!(
            "{x:.2} {y:.2} {}\n",
            if i == 0 { "m" } else { "l" }
        ));
    }
    s.push_str("h f\n");
    s
}

fn content_stream(plan: &RenderPlan) -> String {
    let flip = |y: f32| plan.height_px as f64 - y as f64;
    let mut s = String::new();

    let (br, bg, bb) = unit_color(plan.background);
    s.push_str(&format!("{br:.3} {bg:.3} {bb:.3} rg\n"));
    s.push_str(&format!("0 0 {} {} re f\n", plan.width_px, plan.height_px));

    for op in &plan.ops {
        match *op {
            DrawOp::Point {
                x,
                y,
                radius,
                color,
            } => {
                let (r, g, b) = unit_color(color);
                s.push_str(&format!("{r:.3} {g:.3} {b:.3} rg\n"));
                s.push_str(&circle_polygon(x as f64, flip(y), radius as f64));
            }
            DrawOp::Segment {
                x0,
                y0,
                x1,
                y1,
                color,
            } => {
                let (r, g, b) = unit_color(color);
                s.push_str(&format!("{r:.3} {g:.3} {b:.3} RG\n"));
                s.push_str(&format!(
                    "{:.2} {:.2} m {:.2} {:.2} l S\n",
                    x0,
                    flip(y0),
                    x1,
                    flip(y1)
                ));
            }
            DrawOp::Rect { x, y, w, h, color } => {
                let (r, g, b) = unit_color(color);
                s.push_str(&format!("{r:.3} {g:.3} {b:.3} rg\n"));
                // A screen-space rect's top-left (x, y) with height h becomes,
                // after the y-flip, a PDF rect anchored at its BOTTOM edge.
                s.push_str(&format!(
                    "{:.2} {:.2} {:.2} {:.2} re f\n",
                    x,
                    flip(y) - h as f64,
                    w,
                    h
                ));
            }
        }
    }
    s
}

/// Encode `plan` as a complete, valid single-page PDF document.
pub fn encode(plan: &RenderPlan) -> Vec<u8> {
    let content = content_stream(plan);
    let content_bytes = content.as_bytes();

    let objects: Vec<String> = vec![
        "<< /Type /Catalog /Pages 2 0 R >>".to_string(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
        format!(
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {} {}] /Contents 4 0 R /Resources << >> >>",
            plan.width_px, plan.height_px
        ),
        format!("<< /Length {} >>\nstream\n{}endstream", content_bytes.len(), content),
    ];

    let mut out = Vec::new();
    out.extend_from_slice(b"%PDF-1.4\n");
    // A binary-marker comment (PDF convention, 4 bytes >= 0x80) so byte-sniffing
    // tools recognize this as binary content, matching what real PDF writers emit.
    out.extend_from_slice(&[0x25, 0xE2, 0xE3, 0xCF, 0xD3, 0x0A]);

    let mut offsets = Vec::with_capacity(objects.len());
    for (i, body) in objects.iter().enumerate() {
        offsets.push(out.len());
        out.extend_from_slice(format!("{} 0 obj\n", i + 1).as_bytes());
        out.extend_from_slice(body.as_bytes());
        out.extend_from_slice(b"\nendobj\n");
    }

    let xref_offset = out.len();
    out.extend_from_slice(format!("xref\n0 {}\n", objects.len() + 1).as_bytes());
    out.extend_from_slice(b"0000000000 65535 f \n");
    for offset in &offsets {
        out.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    out.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{}\n%%EOF",
            objects.len() + 1,
            xref_offset
        )
        .as_bytes(),
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_a_document_with_the_pdf_header() {
        let plan = RenderPlan::new(100, 50, [255, 255, 255, 255]);
        let bytes = encode(&plan);
        assert!(bytes.starts_with(b"%PDF-1.4"));
        assert!(bytes.ends_with(b"%%EOF"));
    }

    #[test]
    fn xref_offsets_point_at_real_object_start_markers() {
        let mut plan = RenderPlan::new(10, 10, [0, 0, 0, 255]);
        plan.ops.push(DrawOp::Rect {
            x: 1.0,
            y: 1.0,
            w: 2.0,
            h: 2.0,
            color: [255, 0, 0, 255],
        });
        let bytes = encode(&plan);
        let text = String::from_utf8_lossy(&bytes);

        // Parse the xref table's four object offsets and confirm each one
        // really points at "<n> 0 obj" in the byte stream.
        let xref_start = text.find("\nxref\n").unwrap() + 1;
        let xref_section = &text[xref_start..];
        let mut lines = xref_section.lines().skip(2); // "xref", "0 N"
        let _free_entry = lines.next().unwrap(); // "0000000000 65535 f "
        for expected_obj in 1..=4 {
            let line = lines.next().unwrap();
            let offset: usize = line[0..10].parse().unwrap();
            let marker = format!("{expected_obj} 0 obj");
            assert_eq!(&bytes[offset..offset + marker.len()], marker.as_bytes());
        }
    }
}
