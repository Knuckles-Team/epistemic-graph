//! Agent-orchestration SHACL shapes owned by EG (operator ruling 2026-09-24, EH-470).
//!
//! agent-utilities authored these documents; EG owns them as immutable core sources
//! so every graph's composed GraphSchema carries them. agent-utilities validates by
//! omitting `shapes` from `ShaclValidate` and never ships, loads or parses a shapes
//! document. Because every committed validation composes these, each must only use
//! constructs EG evaluates (the `every_core_shape_target_validates_without_error`
//! guard). The governance document is byte-identical to the former
//! `pack:agent-utilities` body, so a stale deployment that still attaches that pack
//! dedupes against the core copy instead of conflicting with it.

use super::CoreSpec;

pub(super) const AGENT_SHAPE_SPECS: &[CoreSpec] = &[
    CoreSpec {
        module: "agent-governance-shapes",
        version: 1,
        shapes: Some(include_str!(
            "../../../ontology/agent_governance-v1.shapes.ttl"
        )),
        ontology: None,
    },
    CoreSpec {
        module: "harness-shapes",
        version: 1,
        shapes: Some(include_str!("../../../ontology/harness-v1.shapes.ttl")),
        ontology: None,
    },
    CoreSpec {
        module: "process-intelligence-shapes",
        version: 1,
        shapes: Some(include_str!(
            "../../../ontology/process_intelligence-v1.shapes.ttl"
        )),
        ontology: None,
    },
    CoreSpec {
        module: "sdlc-lifecycle-shapes",
        version: 1,
        shapes: Some(include_str!(
            "../../../ontology/sdlc_lifecycle-v1.shapes.ttl"
        )),
        ontology: None,
    },
    CoreSpec {
        module: "temporal-shapes",
        version: 1,
        shapes: Some(include_str!("../../../ontology/temporal-v1.shapes.ttl")),
        ontology: None,
    },
    CoreSpec {
        module: "portfolio-intelligence-shapes",
        version: 1,
        shapes: Some(include_str!(
            "../../../ontology/portfolio_intelligence-v1.shapes.ttl"
        )),
        ontology: None,
    },
];

#[cfg(test)]
mod tests {
    use super::super::current_core_catalog;

    /// The exact document bytes. Governance, process-intelligence, SDLC and
    /// portfolio are byte-identical to what AU shipped at origin/main 2f1610895 (the
    /// governance digest equals AU's frozen `pack:agent-utilities` raw pin); the
    /// harness concentration shape and the temporal superseded-fact shape were
    /// rewritten without GROUP BY / NOT EXISTS, which EG's sh:sparql declines.
    const PINS: &[(&str, &str)] = &[
        (
            "agent-governance-shapes",
            "6c0a88f7d60b0569c0c88d379f0026d434eb7fdadb8146f0ab4a0dbab5fc39e6",
        ),
        (
            "harness-shapes",
            "9adf8d076018d2bb02c80e0a8816c4bc29676f1a2263799755fa1e11e3d33191",
        ),
        (
            "process-intelligence-shapes",
            "eab9238cd36960f68930b80612d661a73a1c0ae76c0f563fa6757f686c1f4083",
        ),
        (
            "sdlc-lifecycle-shapes",
            "97dfe572cca0ec814d9e57158085b6752f0c03f886e2dde83f72eb3dd49e4805",
        ),
        (
            "temporal-shapes",
            "b11887e2ad792018d91a8025e384eb69ca6e47993ea93d14003ab90b4999e9bf",
        ),
        (
            "portfolio-intelligence-shapes",
            "c22fae2842852e9b785cb900af66024fdf5570eaa64f37b96bff0316f535be8b",
        ),
    ];

    #[test]
    fn agent_shape_documents_are_core_shapes_with_pinned_bytes() {
        let catalog = current_core_catalog();
        for (module, digest) in PINS {
            let source = &catalog[&format!("core:{module}@1")];
            assert!(source.ontology_ttl.is_none(), "{module} is shapes-only");
            let shapes = source.shapes_ttl.as_deref().unwrap();
            assert!(shapes.contains("http://knuckles.team/kg"), "{module}");
            assert_eq!(source.shapes_sha256.unwrap().to_hex(), *digest, "{module}");
        }
    }
}
