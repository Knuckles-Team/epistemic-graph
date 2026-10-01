//! Decode supported constraint parameters into the shared shape model.
use super::*;

impl ShapesGraph<'_> {
    /// Cardinality and value-type constraints (W3C SHACL Core §4.1, §4.2).
    pub(super) fn collect_value_type_constraints(&self, id: &Term, out: &mut Vec<Constraint>) {
        if let Some(v) = self.usize_object(id, vocab::MIN_COUNT) {
            out.push(Constraint::MinCount(v));
        }
        if let Some(v) = self.usize_object(id, vocab::MAX_COUNT) {
            out.push(Constraint::MaxCount(v));
        }
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::DATATYPE) {
            out.push(Constraint::Datatype(n));
        }
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::CLASS) {
            out.push(Constraint::Class(n));
        }
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::NODE_KIND) {
            if let Some(k) = NodeKind::from_iri(n.as_str()) {
                out.push(Constraint::NodeKind(k));
            }
        }
    }

    /// Value-range and string-based constraints (W3C SHACL Core §4.3, §4.4).
    pub(super) fn collect_range_and_string_constraints(
        &self,
        id: &Term,
        out: &mut Vec<Constraint>,
    ) {
        for (predicate, kind) in [
            (vocab::MIN_INCLUSIVE, RangeKind::MinInclusive),
            (vocab::MAX_INCLUSIVE, RangeKind::MaxInclusive),
            (vocab::MIN_EXCLUSIVE, RangeKind::MinExclusive),
            (vocab::MAX_EXCLUSIVE, RangeKind::MaxExclusive),
        ] {
            if let Some(v) = self.object(id, predicate) {
                out.push(Constraint::Range(kind, v));
            }
        }
        if let Some(v) = self.usize_object(id, vocab::MIN_LENGTH) {
            out.push(Constraint::MinLength(v));
        }
        if let Some(v) = self.usize_object(id, vocab::MAX_LENGTH) {
            out.push(Constraint::MaxLength(v));
        }
        if let Some(p) = self.string_object(id, vocab::PATTERN) {
            out.push(Constraint::Pattern {
                pattern: p,
                flags: self.string_object(id, vocab::FLAGS),
            });
        }
        if let Some(head) = self.object(id, vocab::LANGUAGE_IN) {
            let langs = self
                .rdf_list(&head)
                .into_iter()
                .filter_map(|t| match t {
                    Term::Literal(l) => Some(l.value().to_string()),
                    _ => None,
                })
                .collect();
            out.push(Constraint::LanguageIn(langs));
        }
    }

    /// Logical, shape-based and other constraints (W3C SHACL Core §4.6, §4.7, §4.8, plus
    /// SHACL-SPARQL `sh:sparql`).
    pub(super) fn collect_logical_and_shape_constraints(
        &self,
        id: &Term,
        out: &mut Vec<Constraint>,
    ) {
        /// Predicate → list-constraint constructor dispatch (`sh:and`/`sh:or`/`sh:xone`,
        /// each of which takes the predicate's whole `rdf:List` as one operand).
        type ListConstraintDispatch = [(&'static str, fn(Vec<Term>) -> Constraint); 3];
        /// Predicate → per-object constraint constructor dispatch (`sh:node`/
        /// `sh:property`/`sh:sparql`, each applied once per matching object).
        type RepeatedConstraintDispatch = [(&'static str, fn(Term) -> Constraint); 3];

        if let Some(head) = self.object(id, vocab::IN) {
            out.push(Constraint::In(self.rdf_list(&head)));
        }
        if let Some(v) = self.object(id, vocab::HAS_VALUE) {
            out.push(Constraint::HasValue(v));
        }
        let lists: ListConstraintDispatch = [
            (vocab::AND, Constraint::And),
            (vocab::OR, Constraint::Or),
            (vocab::XONE, Constraint::Xone),
        ];
        for (predicate, as_constraint) in lists {
            if let Some(head) = self.object(id, predicate) {
                out.push(as_constraint(self.rdf_list(&head)));
            }
        }
        if let Some(v) = self.object(id, vocab::NOT) {
            out.push(Constraint::Not(v));
        }
        let repeated: RepeatedConstraintDispatch = [
            (vocab::NODE, Constraint::Node),
            (vocab::PROPERTY, Constraint::Property),
            (vocab::SPARQL, Constraint::Sparql),
        ];
        for (predicate, as_constraint) in repeated {
            for v in self.objects(id, predicate) {
                out.push(as_constraint(v));
            }
        }
    }

    /// `sh:ignoredProperties` — the IRIs in the list, empty when absent.
    pub(super) fn parse_ignored_properties(&self, id: &Term) -> Vec<NamedNode> {
        match self.object(id, vocab::IGNORED_PROPERTIES) {
            Some(head) => self
                .rdf_list(&head)
                .into_iter()
                .filter_map(|t| match t {
                    Term::NamedNode(n) => Some(n),
                    _ => None,
                })
                .collect(),
            None => Vec::new(),
        }
    }

    /// A boolean shape flag such as `sh:deactivated` or `sh:closed`: true only when the
    /// object is the literal `true`.
    pub(super) fn flag_is_true(&self, id: &Term, predicate: &str) -> bool {
        matches!(
            self.object(id, predicate),
            Some(Term::Literal(ref l)) if matches!(l.value(), "true" | "1")
        )
    }
}
