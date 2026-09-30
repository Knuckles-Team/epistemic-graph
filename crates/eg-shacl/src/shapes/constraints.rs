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
        self.collect_class_constraints(id, out);
        if let Some(Term::NamedNode(n)) = self.object(id, vocab::NODE_KIND) {
            if let Some(k) = NodeKind::from_iri(n.as_str()) {
                out.push(Constraint::NodeKind(k));
            }
        }
    }

    fn collect_class_constraints(&self, id: &Term, out: &mut Vec<Constraint>) {
        for value in self.objects(id, vocab::CLASS) {
            if let Term::NamedNode(class) = value {
                out.push(Constraint::Class(class));
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
        self.collect_pattern_constraints(id, out);
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

    fn collect_pattern_constraints(&self, id: &Term, out: &mut Vec<Constraint>) {
        let mut flags: Vec<Option<String>> = self
            .objects(id, vocab::FLAGS)
            .into_iter()
            .filter_map(|t| match t {
                Term::Literal(l) => Some(Some(l.value().into())),
                _ => None,
            })
            .collect();
        if flags.is_empty() {
            flags.push(None);
        }
        for value in self.objects(id, vocab::PATTERN) {
            if let Term::Literal(pattern) = value {
                out.extend(flags.iter().map(|flag| Constraint::Pattern {
                    pattern: pattern.value().into(),
                    flags: flag.clone(),
                }));
            }
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
        for v in self.objects(id, vocab::HAS_VALUE) {
            out.push(Constraint::HasValue(v));
        }
        let lists: ListConstraintDispatch = [
            (vocab::AND, Constraint::And),
            (vocab::OR, Constraint::Or),
            (vocab::XONE, Constraint::Xone),
        ];
        for (predicate, as_constraint) in lists {
            for head in self.objects(id, predicate) {
                out.push(as_constraint(self.rdf_list(&head)));
            }
        }
        for v in self.objects(id, vocab::NOT) {
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
        let mut lists = self
            .objects(id, vocab::IGNORED_PROPERTIES)
            .into_iter()
            .map(|head| self.rdf_list(&head));
        let mut common = lists.next().unwrap_or_default();
        // Multiple optional parameter values instantiate separate closed-shape
        // constraints. An unlisted property must be ignored by every one.
        for list in lists {
            let allowed: std::collections::HashSet<_> = list.into_iter().collect();
            common.retain(|term| allowed.contains(term));
        }
        common
            .into_iter()
            .filter_map(|term| match term {
                Term::NamedNode(n) => Some(n),
                _ => None,
            })
            .collect()
    }

    /// A boolean shape flag such as `sh:deactivated` or `sh:closed`: true only when the
    /// object is the literal `true`.
    pub(super) fn flag_is_true(&self, id: &Term, predicate: &str) -> bool {
        self.objects(id, predicate)
            .iter()
            .any(|value| matches!(value, Term::Literal(l) if matches!(l.value(), "true" | "1")))
    }
}
