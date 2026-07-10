//! `ModalityContract` retrofit for [`MergeProposal`] and [`Symbol`] (CONCEPT:E4/X1).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — `MergeProposal`
//! and `resolve_candidates` already live in the always-on `algorithms` module, and
//! `ast::symbol::Symbol` lives in the always-on `ast` module (only `ast::parser` is
//! gated behind the crate's `ast` feature; the plain `Symbol` DTO is not), so
//! `contract` needs no other feature implied for either impl.
//!
//! `MergeProposal` is the reference non-trivial `provenance()` case, analogous to
//! `eg-rdf`'s: a real "rule" (`kind` — `"same_as"` or `"extends"`) that produced it
//! and real "premises" (the member ids it clustered).
//!
//! `Symbol` is the X1 (multimodal-evidence) reference case for `evidence()`: a
//! parsed AST symbol already carries a real located source position
//! (`file_path`/`line_start`/`line_end`) — exactly the shape `EvidenceSpan::CodeSymbol`
//! wants — so this maps it losslessly rather than leaving the default `None`.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::algorithms::MergeProposal;
use crate::ast::symbol::Symbol;

impl ModalityContract for MergeProposal {
    fn storage_kind(&self) -> &'static str {
        "compute"
    }

    /// The proposal's own cosine-similarity score IS a real, natural rank — mirrors
    /// `eg-rdf::owl::ProofNode` using its own confidence as the row score.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::scored(id, self.score as f32)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A `MergeProposal` is, per its own doc comment, "read/propose only — it never
    /// mutates the graph" — so it is never itself a write, hence never CDC-observable.
    /// This is a stronger, architecturally-permanent claim than the usual "not yet
    /// wired" (compare `eg-tensor`/`eg-geo`'s `None`), similar to how
    /// `eg-epistemic::BeliefState` is a pure derived view.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// Real, non-stub provenance: `kind` names the rule/classification that produced
    /// this proposal (`"same_as"` or `"extends"`), `detail` lists the clustered
    /// members plus the chosen canonical, and `confidence` is the proposal's own
    /// similarity score. Cosine similarity is mathematically bounded to `[-1, 1]`, so
    /// this clamps into `Provenance::confidence`'s documented `[0, 1]` range rather
    /// than assuming the score is already there.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        let mut detail: Vec<String> = self
            .members
            .iter()
            .map(|m| format!("member:{m}"))
            .collect();
        detail.push(format!("canonical:{}", self.canonical));
        Some(Provenance {
            source: self.kind.clone(),
            detail,
            confidence: self.score.clamp(0.0, 1.0),
        })
    }

    /// The crate's real entity-resolution entry point that emits `MergeProposal`s.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["resolve_candidates"]
    }
}

impl ConformanceTestable for MergeProposal {
    fn conformance_sample() -> Self {
        MergeProposal {
            canonical: "entity-1".to_string(),
            members: vec!["entity-1".to_string(), "entity-2".to_string()],
            score: 0.93,
            kind: "same_as".to_string(),
        }
    }
}

eg_modality::modality_conformance_tests!(MergeProposal);

// Direct tests of the real provenance() mapping, beyond the generic "never panics"
// conformance check — mirrors eg-epistemic's `overrides` module.
#[cfg(test)]
mod overrides {
    use super::*;
    use eg_modality::ModalityContract;

    #[test]
    fn kind_maps_to_source() {
        let p = MergeProposal::conformance_sample();
        let prov = p.provenance("x").expect("a MergeProposal always has provenance");
        assert_eq!(prov.source, "same_as");
    }

    #[test]
    fn score_maps_to_clamped_confidence() {
        let p = MergeProposal::conformance_sample();
        let prov = p.provenance("x").unwrap();
        assert_eq!(prov.confidence, 0.93);

        // A score outside [0, 1] (cosine similarity can dip negative, or a
        // caller-supplied score could exceed 1.0) must be clamped defensively.
        let negative = MergeProposal {
            canonical: "entity-3".to_string(),
            members: vec!["entity-3".to_string(), "entity-4".to_string()],
            score: -0.2,
            kind: "extends".to_string(),
        };
        assert_eq!(negative.provenance("x").unwrap().confidence, 0.0);

        let over_one = MergeProposal {
            canonical: "entity-5".to_string(),
            members: vec!["entity-5".to_string(), "entity-6".to_string()],
            score: 1.5,
            kind: "extends".to_string(),
        };
        assert_eq!(over_one.provenance("x").unwrap().confidence, 1.0);
    }

    #[test]
    fn detail_contains_canonical_and_members() {
        let p = MergeProposal::conformance_sample();
        let prov = p.provenance("x").unwrap();
        assert!(prov.detail.contains(&"canonical:entity-1".to_string()));
        assert!(prov.detail.contains(&"member:entity-2".to_string()));
    }
}

impl ModalityContract for Symbol {
    fn storage_kind(&self) -> &'static str {
        "code"
    }

    /// A parsed symbol is a SOURCE-shaped candidate (keyed by `Symbol::id`), not
    /// intrinsically ranked — unranked, exactly like `eg-tsdb::SeriesMeta`/
    /// `eg-geo::Geometry`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A parsed symbol lands via a batch AST-parse pass (`ast::parser`), not a live
    /// transactional write path today — not (yet) on the CDC/streaming surface,
    /// mirroring `MergeProposal`'s `None`.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// The X1 reference `evidence()`: `Symbol` already carries a real located source
    /// position — `file_path` + the `[line_start, line_end]` range this symbol spans
    /// — so this maps LOSSLESSLY onto `EvidenceSpan::CodeSymbol`, ignoring the
    /// caller-supplied `id` (the symbol's own `name` is the more meaningful label,
    /// and `file_path`/line range are already exact — nothing to resolve via `id`).
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        Some(EvidenceSpan::CodeSymbol {
            file_path: self.file_path.clone(),
            symbol: self.name.clone(),
            start_line: self.line_start,
            end_line: self.line_end,
        })
    }
}

impl ConformanceTestable for Symbol {
    fn conformance_sample() -> Self {
        Symbol {
            id: "sym:deadbeef".to_string(),
            name: "handle_request".to_string(),
            qualified_name: "crate::server::handle_request".to_string(),
            symbol_type: crate::ast::symbol::SymbolType::Function,
            file_path: "src/server.rs".to_string(),
            line_start: 42,
            line_end: 88,
            column: 0,
            ast_hash: "deadbeef".to_string(),
            dependencies: Vec::new(),
            documentation: "Handles an inbound request.".to_string(),
            language: "rust".to_string(),
            is_exported: true,
            annotations: Vec::new(),
            byte_start: 900,
            byte_end: 1500,
        }
    }
}

// `modality_conformance_tests!` expands to a FIXED `mod eg_modality_conformance`
// name — nested here (rather than invoked a second time at this file's top level,
// where `MergeProposal`'s invocation already claims that name) so the two batteries
// don't collide (CONCEPT:E4/X1).
mod symbol_conformance {
    use super::Symbol;

    eg_modality::modality_conformance_tests!(Symbol);
}

// A direct test of the `evidence()` mapping itself, beyond the generic
// "never panics" conformance check — the load-bearing proof that `Symbol`'s own
// file_path/line range maps losslessly onto `EvidenceSpan::CodeSymbol` (X1).
#[cfg(test)]
mod symbol_evidence {
    use super::*;

    #[test]
    fn maps_file_path_and_line_range_losslessly() {
        let sym = Symbol::conformance_sample();
        let span = sym.evidence("ignored").expect("a parsed Symbol always has evidence");
        assert_eq!(
            span,
            EvidenceSpan::CodeSymbol {
                file_path: "src/server.rs".to_string(),
                symbol: "handle_request".to_string(),
                start_line: 42,
                end_line: 88,
            }
        );
    }
}
