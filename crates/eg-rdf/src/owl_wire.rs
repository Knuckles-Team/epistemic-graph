//! The wire projection of a reconstructed OWL proof tree
//! (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation): `OwlExplain` and the UQL
//! `WITH PROOF` `REASON` step both return [`ProofNodeWire`].

use eg_types::protocol::ProofNodeWire;

use crate::owl::ProofNode;

/// Recursively project `node` into its wire twin — a field-for-field walk; the tree
/// shape is identical on both sides, this only crosses the eg-rdf → eg-types boundary.
pub fn proof_node_to_wire(node: ProofNode) -> ProofNodeWire {
    ProofNodeWire {
        sub: node.sub,
        sup: node.sup,
        rule: node.rule,
        axioms: node.axioms,
        confidence: node.confidence,
        premises: node.premises.into_iter().map(proof_node_to_wire).collect(),
    }
}
