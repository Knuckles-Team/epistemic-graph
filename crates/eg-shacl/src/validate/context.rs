//! Validation context construction and bounded target/value selection.
use super::*;

impl<'a> Validator<'a> {
    pub(super) fn new(
        shapes: &'a Graph,
        data: &'a Graph,
        budget: &'a Budget,
    ) -> Result<Self, String> {
        crate::supported::check(shapes, budget)?;
        Ok(Self {
            shapes: ShapesGraph::new(shapes),
            data,
            budget,
        })
    }

    pub(super) fn parse_shape(&self, term: &Term) -> Result<Shape, String> {
        crate::parse_work::shape(self.shapes.graph(), term, self.budget)?;
        Ok(self.shapes.parse_shape(term))
    }

    /// Focus nodes selected by a shape's targets.
    pub(super) fn focus_nodes(&self, shape: &Shape) -> Result<Vec<Term>, String> {
        let mut out: Vec<Term> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut push = |t: Term, out: &mut Vec<Term>| -> Result<(), String> {
            self.budget.charge(1)?;
            if seen.insert(t.clone()) {
                out.push(t);
            }
            Ok(())
        };
        for target in &shape.targets {
            match target {
                Target::Node(n) => push(n.clone(), &mut out)?,
                Target::Class(c) => {
                    for s in self
                        .data
                        .subjects_for_predicate_object(nn(vocab::RDF_TYPE), c.as_ref())
                    {
                        push(s.into_owned().into(), &mut out)?;
                    }
                }
                Target::SubjectsOf(p) => {
                    for t in self.data.triples_for_predicate(p.as_ref()) {
                        push(t.subject.into_owned().into(), &mut out)?;
                    }
                }
                Target::ObjectsOf(p) => {
                    for t in self.data.triples_for_predicate(p.as_ref()) {
                        push(t.object.into_owned(), &mut out)?;
                    }
                }
            }
        }
        Ok(out)
    }

    /// The value nodes of `shape` for `focus`.
    pub(super) fn value_nodes(&self, shape: &Shape, focus: &Term) -> Result<Vec<Term>, String> {
        match &shape.path {
            None => Ok(vec![focus.clone()]),
            Some(Path::Predicate(p)) => match as_subject_ref(focus) {
                Some(s) => self
                    .data
                    .objects_for_subject_predicate(s, p.as_ref())
                    .map(|t| {
                        self.budget.charge(1)?;
                        Ok(t.into_owned())
                    })
                    .collect(),
                None => Ok(Vec::new()),
            },
            // Complex paths must not silently discard constraints.
            Some(Path::Unsupported) => Err("SHACL property paths must be predicate IRIs".into()),
        }
    }
}

/// Charge the candidates actually scanned by the subject/type index.
pub(super) fn has_class(
    data: &Graph,
    value: &Term,
    class: &NamedNode,
    budget: &Budget,
) -> Result<bool, String> {
    let Some(subject) = as_subject_ref(value) else {
        return Ok(false);
    };
    for candidate in data.objects_for_subject_predicate(subject, nn(vocab::RDF_TYPE)) {
        budget.charge(1)?;
        if candidate == class.as_ref().into() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn class_scan_refuses_exactly_when_candidate_allowance_is_exhausted() {
        let data = crate::graph_from_turtle("<urn:x> a <urn:A>, <urn:B>, <urn:C> .").unwrap();
        let node = Term::NamedNode(NamedNode::new_unchecked("urn:x"));
        let absent = NamedNode::new_unchecked("urn:absent");
        assert_eq!(has_class(&data, &node, &absent, &Budget::new(3)), Ok(false));
        assert_eq!(
            has_class(&data, &node, &absent, &Budget::new(2)),
            Err(crate::WORK_EXCEEDED.into())
        );
    }
}
