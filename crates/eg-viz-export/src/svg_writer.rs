//! A from-scratch SVG writer (D-VZ-1 lane V3a) — plain XML text generation, no
//! new dependency.

use crate::plan::{DrawOp, RenderPlan};

fn css_rgba(c: [u8; 4]) -> String {
    format!(
        "rgba({},{},{},{:.3})",
        c[0],
        c[1],
        c[2],
        c[3] as f32 / 255.0
    )
}

/// Encode `plan` as a complete, standalone SVG document.
pub fn encode(plan: &RenderPlan) -> Vec<u8> {
    let mut svg = String::new();
    svg.push_str(&format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{}\" height=\"{}\" viewBox=\"0 0 {} {}\">\n",
        plan.width_px, plan.height_px, plan.width_px, plan.height_px
    ));
    svg.push_str(&format!(
        "<rect x=\"0\" y=\"0\" width=\"{}\" height=\"{}\" fill=\"{}\"/>\n",
        plan.width_px,
        plan.height_px,
        css_rgba(plan.background)
    ));
    for op in &plan.ops {
        match *op {
            DrawOp::Point { x, y, radius, color } => svg.push_str(&format!(
                "<circle cx=\"{x:.2}\" cy=\"{y:.2}\" r=\"{radius:.2}\" fill=\"{}\"/>\n",
                css_rgba(color)
            )),
            DrawOp::Segment { x0, y0, x1, y1, color } => svg.push_str(&format!(
                "<line x1=\"{x0:.2}\" y1=\"{y0:.2}\" x2=\"{x1:.2}\" y2=\"{y1:.2}\" stroke=\"{}\" stroke-width=\"1\"/>\n",
                css_rgba(color)
            )),
            DrawOp::Rect { x, y, w, h, color } => svg.push_str(&format!(
                "<rect x=\"{x:.2}\" y=\"{y:.2}\" width=\"{w:.2}\" height=\"{h:.2}\" fill=\"{}\"/>\n",
                css_rgba(color)
            )),
        }
    }
    svg.push_str("</svg>\n");
    svg.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::RenderPlan;

    #[test]
    fn encodes_a_well_formed_svg_document() {
        let mut plan = RenderPlan::new(100, 50, [255, 255, 255, 255]);
        plan.ops.push(DrawOp::Point {
            x: 10.0,
            y: 10.0,
            radius: 2.0,
            color: [255, 0, 0, 255],
        });
        let bytes = encode(&plan);
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("<svg"));
        assert!(text.trim_end().ends_with("</svg>"));
        assert!(text.contains("width=\"100\""));
        assert!(text.contains("<circle"));
    }
}
