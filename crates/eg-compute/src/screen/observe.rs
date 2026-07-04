//! Screen-observation enrichment (CONCEPT:AU-KG.ontology.owl-screen-bridge / KG-2.186).
//!
//! Mirrors the code call-graph enrichment (`parser::resolve::index_repository`,
//! KG-2.100): a pure `fn(input) -> {nodes, edges}` the server persists in one
//! round-trip. It turns a captured desktop frame (a screenshot PNG + its
//! accessibility-tree elements) into durable graph entities — a `ComputerUseSession`,
//! a `ScreenObservation` frame, and one `UIElement` per accessible — so an agent's
//! GUI grounding is a first-class KG query and frames chain across time (`succeededBy`)
//! for replay/RL.
//!
//! Deliberately dep-free: PNG width/height are read straight from the IHDR and a
//! cheap FNV-1a content hash powers frame-diff, so no image-decode crate is pulled
//! and the enrichment compiles in every build (no feature gate).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// An extracted graph node. Mirrors the AST enrichment's node shape
/// (`node_id`/`node_type`/`properties`) so the Python persist path is shared, but
/// is defined locally to keep this module decoupled from the `ast`-gated parser.
#[derive(Serialize, Debug)]
pub struct ExtractedNode {
    pub node_id: String,
    pub node_type: String,
    pub properties: HashMap<String, String>,
}

/// An extracted graph edge (same shape as the AST enrichment's edges).
#[derive(Serialize, Debug)]
pub struct ExtractedEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: HashMap<String, String>,
}

/// One accessible element from the in-sandbox `a11y-dump` (AT-SPI) capture.
#[derive(Deserialize, Default, Clone, Debug)]
pub struct UiElementInput {
    pub role: String,
    pub name: String,
    pub x: i64,
    pub y: i64,
    pub w: i64,
    pub h: i64,
}

/// One captured frame: the screenshot bytes plus its grounded elements, tagged with
/// the owning session and the previous frame (for the diff chain).
pub struct ScreenObservationInput {
    pub session_id: String,
    pub frame_seq: u64,
    pub prev_frame_id: String,
    pub prev_hash: u64,
    pub png: Vec<u8>,
    pub elements: Vec<UiElementInput>,
}

#[derive(Serialize, Debug)]
pub struct ScreenObservationResult {
    /// The session node + the frame node + one node per UI element.
    pub nodes: Vec<ExtractedNode>,
    /// session-`hasObservation`->frame, frame-`hasElement`->element, and
    /// prevframe-`succeededBy`->frame (only when the frame actually changed).
    pub edges: Vec<ExtractedEdge>,
    pub frame_id: String,
    pub width: u32,
    pub height: u32,
    /// FNV-1a hash of the PNG bytes — the caller passes it back as `prev_hash`.
    pub hash: u64,
    /// False when the frame is byte-identical to the previous one (no visual change).
    pub changed: bool,
    pub element_count: usize,
}

/// Read width/height from a PNG IHDR (the first chunk). Returns (0, 0) if not a PNG.
fn png_dimensions(data: &[u8]) -> (u32, u32) {
    const SIG: &[u8] = b"\x89PNG\r\n\x1a\n";
    if data.len() >= 24 && data.starts_with(SIG) && &data[12..16] == b"IHDR" {
        let w = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
        let h = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);
        return (w, h);
    }
    (0, 0)
}

/// FNV-1a over the bytes — a cheap content hash for frame-diff (not cryptographic).
fn fnv1a(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn props(pairs: &[(&str, String)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

pub fn observe_screen(input: &ScreenObservationInput) -> ScreenObservationResult {
    let (width, height) = png_dimensions(&input.png);
    let hash = fnv1a(&input.png);
    // First frame (no prior hash) counts as changed; otherwise diff the content.
    let changed = input.prev_hash == 0 || hash != input.prev_hash;

    let session_node = format!("computerusesession:{}", input.session_id);
    let frame_id = format!("screenobservation:{}:{}", input.session_id, input.frame_seq);

    let mut nodes: Vec<ExtractedNode> = Vec::with_capacity(input.elements.len() + 2);
    let mut edges: Vec<ExtractedEdge> = Vec::with_capacity(input.elements.len() + 2);

    nodes.push(ExtractedNode {
        node_id: session_node.clone(),
        node_type: "computerusesession".to_string(),
        properties: props(&[("session_id", input.session_id.clone())]),
    });
    nodes.push(ExtractedNode {
        node_id: frame_id.clone(),
        node_type: "screenobservation".to_string(),
        properties: props(&[
            ("session_id", input.session_id.clone()),
            ("frame_seq", input.frame_seq.to_string()),
            ("width", width.to_string()),
            ("height", height.to_string()),
            ("hash", hash.to_string()),
            ("element_count", input.elements.len().to_string()),
        ]),
    });
    edges.push(ExtractedEdge {
        source: session_node,
        target: frame_id.clone(),
        edge_type: "hasObservation".to_string(),
        properties: HashMap::new(),
    });
    // Chain frames only when something changed — a static screen doesn't add noise.
    if !input.prev_frame_id.is_empty() && changed {
        edges.push(ExtractedEdge {
            source: input.prev_frame_id.clone(),
            target: frame_id.clone(),
            edge_type: "succeededBy".to_string(),
            properties: HashMap::new(),
        });
    }

    for (i, el) in input.elements.iter().enumerate() {
        let el_id = format!("{}:el-{}", frame_id, i);
        nodes.push(ExtractedNode {
            node_id: el_id.clone(),
            node_type: "uielement".to_string(),
            properties: props(&[
                ("role", el.role.clone()),
                ("name", el.name.clone()),
                ("x", el.x.to_string()),
                ("y", el.y.to_string()),
                ("w", el.w.to_string()),
                ("h", el.h.to_string()),
                ("element_index", i.to_string()),
            ]),
        });
        edges.push(ExtractedEdge {
            source: frame_id.clone(),
            target: el_id,
            edge_type: "hasElement".to_string(),
            properties: HashMap::new(),
        });
    }

    ScreenObservationResult {
        nodes,
        edges,
        frame_id,
        width,
        height,
        hash,
        changed,
        element_count: input.elements.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png(w: u32, h: u32) -> Vec<u8> {
        let mut v = b"\x89PNG\r\n\x1a\n".to_vec();
        v.extend_from_slice(&[0, 0, 0, 13]); // IHDR length
        v.extend_from_slice(b"IHDR");
        v.extend_from_slice(&w.to_be_bytes());
        v.extend_from_slice(&h.to_be_bytes());
        v.extend_from_slice(&[8, 2, 0, 0, 0]);
        v
    }

    #[test]
    fn emits_session_frame_and_elements() {
        let input = ScreenObservationInput {
            session_id: "s1".into(),
            frame_seq: 0,
            prev_frame_id: String::new(),
            prev_hash: 0,
            png: png(1280, 800),
            elements: vec![UiElementInput {
                role: "push button".into(),
                name: "Save".into(),
                x: 100,
                y: 200,
                w: 40,
                h: 20,
            }],
        };
        let r = observe_screen(&input);
        assert_eq!((r.width, r.height), (1280, 800));
        assert!(r.changed);
        assert_eq!(r.element_count, 1);
        // session + frame + 1 element
        assert_eq!(r.nodes.len(), 3);
        assert!(r.nodes.iter().any(|n| n.node_type == "uielement"));
        assert!(r.edges.iter().any(|e| e.edge_type == "hasObservation"));
        assert!(r.edges.iter().any(|e| e.edge_type == "hasElement"));
        // no prior frame -> no succeededBy
        assert!(!r.edges.iter().any(|e| e.edge_type == "succeededBy"));
    }

    #[test]
    fn unchanged_frame_skips_succession_edge() {
        let png_bytes = png(640, 480);
        let h = fnv1a(&png_bytes);
        let input = ScreenObservationInput {
            session_id: "s1".into(),
            frame_seq: 1,
            prev_frame_id: "screenobservation:s1:0".into(),
            prev_hash: h, // identical content
            png: png_bytes,
            elements: vec![],
        };
        let r = observe_screen(&input);
        assert!(!r.changed);
        assert!(!r.edges.iter().any(|e| e.edge_type == "succeededBy"));
    }

    #[test]
    fn changed_frame_links_succession() {
        let input = ScreenObservationInput {
            session_id: "s1".into(),
            frame_seq: 2,
            prev_frame_id: "screenobservation:s1:1".into(),
            prev_hash: 12345, // different from the new frame's hash
            png: png(640, 480),
            elements: vec![],
        };
        let r = observe_screen(&input);
        assert!(r.changed);
        assert!(r
            .edges
            .iter()
            .any(|e| e.edge_type == "succeededBy" && e.source == "screenobservation:s1:1"));
    }
}
