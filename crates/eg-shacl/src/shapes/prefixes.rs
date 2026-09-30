//! In-graph SPARQL prefix resolution. No imports are fetched.
use super::*;

impl ShapesGraph<'_> {
    /// Resolve a `sh:sparql` constraint node into its [`SparqlConstraint`] descriptor
    /// (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5.1): the `sh:select` text, an optional local
    /// `sh:message` override, and the prefix declarations reachable from `sh:prefixes`.
    pub fn parse_sparql_constraint(&self, id: &Term) -> Option<SparqlConstraint> {
        let select = self.string_object(id, vocab::SELECT)?;
        let message = self.string_object(id, vocab::MESSAGE);
        let mut prefixes = Vec::new();
        let mut seen_decls = std::collections::HashSet::new();
        for target in self.objects(id, vocab::PREFIXES) {
            self.collect_prefixes(&target, &mut prefixes, &mut seen_decls);
        }
        Some(SparqlConstraint {
            select,
            message,
            prefixes,
        })
    }

    /// Resolve the supplied in-graph import closure without recursion or truncation.
    /// Each resource and its outgoing declarations/imports are visited once.
    fn collect_prefixes(
        &self,
        target: &Term,
        out: &mut Vec<(String, String)>,
        seen: &mut std::collections::HashSet<Term>,
    ) {
        let mut pending = vec![target.clone()];
        while let Some(target) = pending.pop() {
            if !seen.insert(target.clone()) {
                continue;
            }
            self.append_prefix_declarations(&target, out);
            pending.extend(self.objects(&target, vocab::OWL_IMPORTS).into_iter().rev());
        }
    }

    fn append_prefix_declarations(&self, target: &Term, out: &mut Vec<(String, String)>) {
        for decl in self.objects(target, vocab::DECLARE) {
            let prefix = self.string_object(&decl, vocab::PREFIX);
            let namespace = self.string_object(&decl, vocab::NAMESPACE);
            if let (Some(p), Some(ns)) = (prefix, namespace) {
                out.push((p, ns));
            }
        }
    }
}
