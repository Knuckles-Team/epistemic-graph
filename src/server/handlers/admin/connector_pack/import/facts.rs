//! Stable AgentComponent facts derived from ConnectorPack annotations.

use eg_types::agent_component::{
    AgentComponentFacts, AgentComponentKind, CostFacts, FactQuality, ModalityFacts, PriceSource,
    ToolEffect,
};
use eg_types::connector_pack::{PackEntry, PackEntryKind};
use eg_types::contract::Digest256;

pub(super) fn facts(entry: &PackEntry, digest: &Digest256) -> Result<AgentComponentFacts, String> {
    let modalities = ModalityFacts {
        input: entry.annotations.modalities_in.as_slice().to_vec(),
        output: entry.annotations.modalities_out.as_slice().to_vec(),
    };
    let cost = entry.annotations.cost.clone().map(|declared| CostFacts {
        declared,
        price_source: PriceSource::ConnectorPack {
            connector: entry.uri.clone(),
            entry_digest: digest.to_hex(),
        },
        quality: FactQuality::Declared,
    });
    Ok(match entry.kind {
        PackEntryKind::Tool => AgentComponentFacts::Tool {
            effect: if entry.annotations.read_only_hint == Some(true)
                && entry.annotations.destructive_hint != Some(true)
            {
                ToolEffect::Read
            } else {
                ToolEffect::Write
            },
            required_scopes: entry.annotations.required_scopes.as_slice().to_vec(),
            input_schema_digest: entry
                .input_schema
                .as_ref()
                .map(|v| format!("sha256:{}", v.sha256.to_hex())),
            output_schema_digest: entry
                .output_schema
                .as_ref()
                .map(|v| format!("sha256:{}", v.sha256.to_hex())),
            read_only_hint: entry.annotations.read_only_hint,
            destructive_hint: entry.annotations.destructive_hint,
            idempotent_hint: entry.annotations.idempotent_hint,
            open_world_hint: entry.annotations.open_world_hint,
            modalities,
            cost,
            latency_declared: entry.annotations.latency_declared,
        },
        PackEntryKind::ModelProfile => {
            let model = entry.annotations.model.as_ref().ok_or_else(|| {
                "INVALID_FACTS: model profile annotations are required".to_string()
            })?;
            AgentComponentFacts::ModelProfile {
                provider: model.provider.clone(),
                model_identity: model.model_identity.clone(),
                context_window_tokens: u32::try_from(model.context_window_tokens)
                    .map_err(|_| "INVALID_FACTS: context window exceeds u32".to_string())?,
                max_output_tokens: u32::try_from(model.max_output_tokens)
                    .map_err(|_| "INVALID_FACTS: output window exceeds u32".to_string())?,
                supports_tools: model
                    .supports_tools
                    .ok_or_else(|| "INVALID_FACTS: supports_tools is required".to_string())?,
                supports_structured_output: model.supports_structured_output.ok_or_else(|| {
                    "INVALID_FACTS: supports_structured_output is required".to_string()
                })?,
                supports_vision: model
                    .supports_vision
                    .ok_or_else(|| "INVALID_FACTS: supports_vision is required".to_string())?,
                modalities,
                cost,
                latency_declared: entry.annotations.latency_declared,
                latency_observed_ref: None,
            }
        }
        _ => AgentComponentFacts::Opaque,
    })
}

pub(super) fn component_kind(kind: PackEntryKind) -> AgentComponentKind {
    kind_semantics(kind).component
}

pub(super) fn uri_prefix(kind: PackEntryKind) -> &'static str {
    kind_semantics(kind).uri_prefix
}

#[derive(Clone, Copy)]
struct KindSemantics {
    pack: PackEntryKind,
    component: AgentComponentKind,
    uri_prefix: &'static str,
}

const KIND_SEMANTICS: [KindSemantics; 12] = [
    KindSemantics {
        pack: PackEntryKind::McpServer,
        component: AgentComponentKind::McpServer,
        uri_prefix: "mcp-server://",
    },
    KindSemantics {
        pack: PackEntryKind::Tool,
        component: AgentComponentKind::Tool,
        uri_prefix: "tool://",
    },
    KindSemantics {
        pack: PackEntryKind::Skill,
        component: AgentComponentKind::Skill,
        uri_prefix: "skill://",
    },
    KindSemantics {
        pack: PackEntryKind::Prompt,
        component: AgentComponentKind::McpPrompt,
        uri_prefix: "prompt://",
    },
    KindSemantics {
        pack: PackEntryKind::Resource,
        component: AgentComponentKind::McpResource,
        uri_prefix: "",
    },
    KindSemantics {
        pack: PackEntryKind::ResourceTemplate,
        component: AgentComponentKind::McpResource,
        uri_prefix: "",
    },
    KindSemantics {
        pack: PackEntryKind::Ontology,
        component: AgentComponentKind::Ontology,
        uri_prefix: "ontology://",
    },
    KindSemantics {
        pack: PackEntryKind::Shapes,
        component: AgentComponentKind::Shapes,
        uri_prefix: "shapes://",
    },
    KindSemantics {
        pack: PackEntryKind::ModelProfile,
        component: AgentComponentKind::ModelProfile,
        uri_prefix: "model-profile://",
    },
    KindSemantics {
        pack: PackEntryKind::A2aCard,
        component: AgentComponentKind::A2aAgentCard,
        uri_prefix: "a2a-card://",
    },
    KindSemantics {
        pack: PackEntryKind::Manifest,
        component: AgentComponentKind::McpResource,
        uri_prefix: "manifest://",
    },
    KindSemantics {
        pack: PackEntryKind::SkillFile,
        component: AgentComponentKind::McpResource,
        uri_prefix: "skill-file://",
    },
];

fn kind_semantics(kind: PackEntryKind) -> KindSemantics {
    KIND_SEMANTICS
        .iter()
        .copied()
        .find(|semantics| semantics.pack == kind)
        .expect("every closed PackEntryKind has component and URI semantics")
}

#[cfg(test)]
mod tests {
    use eg_types::connector_pack::{PackAnnotations, PackSection};
    use eg_types::contract::BoundedVec;

    use super::*;

    fn section(byte: u8) -> PackSection {
        PackSection {
            offset: 0,
            length: 1,
            sha256: Digest256::from_bytes([byte; 32]),
        }
    }

    fn tool_entry(output_schema: Option<PackSection>) -> PackEntry {
        PackEntry {
            kind: PackEntryKind::Tool,
            uri: "tool://demo-mcp/reader".to_string(),
            name: "reader".to_string(),
            media_type: "application/json".to_string(),
            body: section(0x01),
            input_schema: Some(section(0x02)),
            output_schema,
            annotations: PackAnnotations::default(),
            references: BoundedVec::new(Vec::new()).unwrap(),
        }
    }

    fn schema_digests(entry: &PackEntry) -> (Option<String>, Option<String>) {
        match facts(entry, &Digest256::from_bytes([0; 32])).unwrap() {
            AgentComponentFacts::Tool {
                input_schema_digest,
                output_schema_digest,
                ..
            } => (input_schema_digest, output_schema_digest),
            other => panic!("a tool entry derives tool facts, not {other:?}"),
        }
    }

    /// D18 (EH-198): both schema digests are the `sha256:<hex>` of the pack's
    /// sections; an absent output section is "declares none".
    // spec: EG-TYPED-PACKS-R044
    // spec: EG-TYPED-PACKS-R064
    #[test]
    fn tool_facts_carry_both_section_digests() {
        let (input, output) = schema_digests(&tool_entry(Some(section(0x03))));
        assert_eq!(input, Some(format!("sha256:{}", "02".repeat(32))));
        assert_eq!(output, Some(format!("sha256:{}", "03".repeat(32))));
        assert_eq!(schema_digests(&tool_entry(None)).1, None);
    }
}
