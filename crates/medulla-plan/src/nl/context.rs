//! Schema-grounded natural-language planning (MEDULLA-FEDERATED-QUERY-R058.4.7.1, R058.4.8).
//!
//! The planner may only target names the queried graph actually has. A
//! [`SchemaContext`] is read off ONE snapshot: every node label with the
//! property keys stored under it, every edge relationship, and the ontology
//! property names that map onto stored keys. It renders the planner's schema
//! hint, and [`SchemaContext::ground`] validates a parsed candidate against it
//! before anything executes:
//!
//! * a label, property or relationship the graph has passes unchanged;
//! * an ontology name (`assignedTo`) or a spelling variant (`Assigned_To`) of a
//!   stored key (`assigned_to`) is rewritten to the stored key (a repair);
//! * anything else is a typed [`SchemaViolation`], never executed.
//!
//! [`plan_grounded`] runs the bounded correction loop: a parse error or a
//! schema violation goes back to the planner as a typed message, and the loop
//! stops after `max_steps` with every step's text and outcome.

use std::collections::{BTreeMap, BTreeSet};

use medulla_core::graph::GraphView;
use medulla_types::wire::{Op, Plan, Pred};

use super::{enforce_budget, NlPlanner, NlQueryBudget, NlQueryError};

/// Labels kept in the schema hint (MEDULLA-FEDERATED-QUERY-R058.4.2).
pub const MAX_LABELS: usize = 60;
/// Properties listed per label in the schema hint (MEDULLA-FEDERATED-QUERY-R058.4.2).
pub const MAX_PROPERTIES_PER_LABEL: usize = 16;
/// Default and maximum planner steps of one request (MEDULLA-FEDERATED-QUERY-R058.4.1).
pub const DEFAULT_MAX_STEPS: usize = 2;
/// Upper bound on `max_steps`.
pub const MAX_STEPS: usize = 4;

/// One ontology property, reduced to what grounding needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OntologyProperty {
    /// IRI local name, e.g. `assignedTo`.
    pub name: String,
    /// `rdfs:label`, e.g. `assigned to`.
    pub label: Option<String>,
    /// `rdfs:domain` local names; empty means any label.
    pub domains: Vec<String>,
}

/// What the graph stores under one label.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelSchema {
    /// Nodes seen with this label in the snapshot.
    pub nodes: usize,
    /// Property keys stored on those nodes.
    pub fields: BTreeSet<String>,
    /// Ontology property name → the stored key it maps onto.
    pub ontology: BTreeMap<String, String>,
}

/// The labels, properties and relationships one graph snapshot holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaContext {
    labels: BTreeMap<String, LabelSchema>,
    relationships: BTreeSet<String>,
    unlabelled: LabelSchema,
    edge_schemas: BTreeMap<String, LabelSchema>,
    untyped_edges: LabelSchema,
    /// Federation sources the owner-scoped served registry authorizes. An
    /// absent catalog refuses foreign stages rather than inventing authority.
    foreign_sources: Option<BTreeSet<String>>,
}

/// A name in a candidate plan that the graph does not have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaViolation {
    /// `MATCH (:Label)` names a label no node carries.
    UnknownLabel { label: String, known: Vec<String> },
    /// A predicate names a property no node in scope stores.
    UnknownProperty {
        property: String,
        labels: Vec<String>,
        known: Vec<String>,
    },
    /// A traversal names a relationship no edge carries.
    UnknownRelationship {
        relationship: String,
        known: Vec<String>,
    },
    /// `FOREIGN` names a source the caller has not registered.
    UnknownSource { source: String, known: Vec<String> },
    /// An inline foreign adapter has no owner-scoped registry identity to ground.
    UnboundForeignSource,
}

impl SchemaViolation {
    /// The stable reason code a refusal carries.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownLabel { .. } => "NL_SCHEMA_UNKNOWN_LABEL",
            Self::UnknownProperty { .. } => "NL_SCHEMA_UNKNOWN_PROPERTY",
            Self::UnknownRelationship { .. } => "NL_SCHEMA_UNKNOWN_RELATIONSHIP",
            Self::UnknownSource { .. } => "NL_SCHEMA_UNKNOWN_SOURCE",
            Self::UnboundForeignSource => "NL_SCHEMA_UNBOUND_FOREIGN_SOURCE",
        }
    }
}

impl std::fmt::Display for SchemaViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.code();
        match self {
            Self::UnknownLabel { label, known } => write!(
                f,
                "{code}: label `{label}` is not in the graph (labels: {})",
                list(known)
            ),
            Self::UnknownProperty {
                property,
                labels,
                known,
            } => write!(
                f,
                "{code}: property `{property}` is not stored on {} (properties: {})",
                list(labels),
                list(known)
            ),
            Self::UnknownRelationship {
                relationship,
                known,
            } => write!(
                f,
                "{code}: relationship `{relationship}` is not in the graph (relationships: {})",
                list(known)
            ),
            Self::UnboundForeignSource => write!(f,
                "{code}: foreign stage has no owner-scoped schema identity; bind the source registry and name a registered source"),
            Self::UnknownSource { source, known } => write!(
                f,
                "{code}: foreign source `{source}` is not registered (sources: {})",
                list(known)
            ),
        }
    }
}

fn list(names: &[String]) -> String {
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join(", ")
    }
}

/// One rename grounding applied: the planner's name and the stored name it ran as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repair {
    pub from: String,
    pub to: String,
}

/// A candidate plan that only names what the graph has.
#[derive(Debug, Clone, PartialEq)]
pub struct Grounded {
    pub plan: Plan,
    pub repairs: Vec<Repair>,
}

/// One planner step: the text it produced and what happened to it
/// (MEDULLA-FEDERATED-QUERY-R058.4.1). `dialect` is always `uql` today.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NlPlanStep {
    pub dialect: String,
    pub query_text: String,
    pub outcome: String,
}

/// The result of the bounded correction loop: the UQL that will run (the
/// planner's text, or its canonical form after a repair), the parsed plan,
/// and every step.
#[derive(Debug, Clone)]
pub struct GroundedCandidate {
    pub uql: String,
    pub plan: Plan,
    pub steps: Vec<NlPlanStep>,
    pub repairs: Vec<Repair>,
}

/// Lower-case alphanumerics only: `assigned_to`, `assignedTo` and `assigned to`
/// all normalize to `assignedto`.
fn norm(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Which labels the rows flowing into an op can carry.
#[derive(Debug, Clone)]
enum Scope {
    /// Rows come from these labels.
    Labels(BTreeSet<String>),
    /// Rows can be any local node.
    AnyLocal,
    /// Stored edge properties, restricted to a relationship when present.
    Edges(Option<String>),
}

/// Fusion returns the union of its branches' local node scopes. Foreign rows
/// carry ids and scores only; property filters rematerialize the local snapshot.
#[cfg(feature = "text")]
fn union_scope(left: Scope, right: Scope) -> Scope {
    match (left, right) {
        (Scope::Labels(mut left), Scope::Labels(right)) => {
            left.extend(right);
            Scope::Labels(left)
        }
        _ => Scope::AnyLocal,
    }
}

impl SchemaContext {
    /// Collect labels, property keys and relationships from `view`.
    pub fn from_view(view: &GraphView) -> Self {
        let mut context = Self::default();
        for blob in view.node_properties.values() {
            if let Ok(serde_json::Value::Object(props)) =
                medulla_types::msgpack::decode_property_value(blob.as_slice())
            {
                context.observe_node(&props);
            }
        }
        for blobs in view.edge_properties.values() {
            for blob in blobs {
                if let Ok(serde_json::Value::Object(props)) =
                    medulla_types::msgpack::decode_property_value(blob.as_slice())
                {
                    let schema =
                        if let Some(rel) = props.get("relationship").and_then(|v| v.as_str()) {
                            context.observe_relationship(rel);
                            context.edge_schemas.entry(rel.to_string()).or_default()
                        } else {
                            &mut context.untyped_edges
                        };
                    schema.fields.extend(props.keys().cloned());
                }
            }
        }
        context
    }

    /// Record one node's labels and property keys.
    pub fn observe_node(&mut self, props: &serde_json::Map<String, serde_json::Value>) {
        let labels = node_labels(props);
        if labels.is_empty() {
            self.unlabelled.nodes += 1;
            self.unlabelled.fields.extend(props.keys().cloned());
        }
        for label in labels {
            let entry = self.labels.entry(label).or_default();
            entry.nodes += 1;
            // Label-bearing keys remain queryable SQL properties too.
            entry.fields.extend(props.keys().cloned());
        }
    }

    /// Record one edge relationship.
    pub fn observe_relationship(&mut self, relationship: &str) {
        if !relationship.is_empty() {
            self.relationships.insert(relationship.to_string());
        }
    }

    /// Map ontology property names onto the stored keys they name
    /// (MEDULLA-FEDERATED-QUERY-R058.4.8). A property maps onto a stored key of a
    /// label in its domain (any label when it declares none) when the key
    /// normalizes to the property's local name or its `rdfs:label`.
    pub fn link_ontology(&mut self, properties: &[OntologyProperty]) {
        for property in properties {
            let names: BTreeSet<String> = std::iter::once(norm(&property.name))
                .chain(property.label.as_deref().map(norm))
                .filter(|n| !n.is_empty())
                .collect();
            for (label, schema) in &mut self.labels {
                if in_domain(label, &property.domains) {
                    link_property(schema, &property.name, &names);
                }
            }
        }
    }

    /// Declare the federation sources the caller may name in `FOREIGN`.
    pub fn declare_foreign_sources(&mut self, sources: BTreeSet<String>) {
        self.foreign_sources = Some(sources);
    }

    /// True when the snapshot holds no labelled node.
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// The label schema for `label`, if the graph has it.
    pub fn label(&self, label: &str) -> Option<&LabelSchema> {
        self.labels.get(label)
    }

    /// The relationships edges carry.
    pub fn relationships(&self) -> &BTreeSet<String> {
        &self.relationships
    }

    /// The schema hint for `question`: labels with node counts, their properties
    /// (ontology-mapped ones as `ontologyName = stored_key`, then the ones the
    /// question mentions, capped per label), and the relationships.
    pub fn hint(&self, question: &str) -> String {
        if self.labels.is_empty() && self.unlabelled.nodes == 0 {
            return "The graph holds no labelled nodes.".to_string();
        }
        let tokens = question_tokens(question);
        let mut out = String::from(
            "Graph schema. Use ONLY these labels, properties and relationships; \
             property names are case-sensitive. WHERE compares properties; RETURN names \
             score channels only, never properties.\nLabels:\n",
        );
        for (label, schema) in self.ranked_labels(&tokens) {
            out.push_str(&format!(
                "  :{label} ({} nodes): {}\n",
                schema.nodes,
                hint_fields(schema, &tokens).join(", ")
            ));
        }
        if self.unlabelled.nodes > 0 {
            out.push_str(&format!(
                "  Unlabelled ({} nodes, use MATCH ()): {}\n",
                self.unlabelled.nodes,
                hint_fields(&self.unlabelled, &tokens).join(", ")
            ));
        }
        out.push_str(&format!(
            "Relationships: {}\n",
            list(&self.relationships.iter().cloned().collect::<Vec<_>>())
        ));
        for (rel, schema) in &self.edge_schemas {
            out.push_str(&format!(
                "  Edge :{rel} properties: {}\n",
                hint_fields(schema, &tokens).join(", ")
            ));
        }
        if let Some(sources) = &self.foreign_sources {
            out.push_str(&format!(
                "Foreign sources: {}\n",
                list(&sources.iter().cloned().collect::<Vec<_>>())
            ));
        }
        out
    }

    /// Labels in hint order: those the question names first, then by node count.
    fn ranked_labels(&self, tokens: &BTreeSet<String>) -> Vec<(&String, &LabelSchema)> {
        let mut labels: Vec<(&String, &LabelSchema)> = self.labels.iter().collect();
        labels.sort_by_key(|(label, schema)| {
            (
                !mentions(tokens, label),
                std::cmp::Reverse(schema.nodes),
                (*label).clone(),
            )
        });
        labels.truncate(MAX_LABELS);
        labels
    }

    /// Validate `plan` against the snapshot and rewrite ontology names and
    /// spelling variants to the stored names (MEDULLA-FEDERATED-QUERY-R058.4.7.1).
    pub fn ground(&self, plan: &Plan) -> Result<Grounded, SchemaViolation> {
        let mut repairs = Vec::new();
        let (ops, _) = self.ground_ops(&plan.ops, Scope::AnyLocal, &mut repairs)?;
        Ok(Grounded {
            plan: Plan::new(ops),
            repairs,
        })
    }

    fn ground_ops(
        &self,
        ops: &[Op],
        mut scope: Scope,
        repairs: &mut Vec<Repair>,
    ) -> Result<(Vec<Op>, Scope), SchemaViolation> {
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            let (grounded, next) = self.ground_op(op, scope, repairs)?;
            out.push(grounded);
            scope = next;
        }
        Ok((out, scope))
    }

    fn ground_op(
        &self,
        op: &Op,
        scope: Scope,
        repairs: &mut Vec<Repair>,
    ) -> Result<(Op, Scope), SchemaViolation> {
        match op {
            Op::Scan { label } => self.ground_scan(label, repairs),
            Op::ScanAll {} => Ok((op.clone(), Scope::AnyLocal)),
            Op::Filter { preds } => {
                let preds = self.ground_preds(preds, &scope, repairs)?;
                Ok((Op::Filter { preds }, scope))
            }
            Op::Traverse { rel, min, max } => {
                let rel = self.resolve_relationship(rel, repairs)?;
                let (min, max) = (*min, *max);
                Ok((Op::Traverse { rel, min, max }, Scope::AnyLocal))
            }
            Op::Expand { .. } | Op::Propagate { .. } => {
                Ok((self.ground_edge_op(op, repairs)?, Scope::AnyLocal))
            }
            #[cfg(feature = "text")]
            Op::FuseRrf { branches, k } => {
                let mut grounded = Vec::with_capacity(branches.len());
                let mut output_scope: Option<Scope> = None;
                for branch in branches {
                    let (ops, branch_scope) = self.ground_ops(branch, scope.clone(), repairs)?;
                    grounded.push(ops);
                    output_scope = Some(match output_scope {
                        None => branch_scope,
                        Some(previous) => union_scope(previous, branch_scope),
                    });
                }
                Ok((
                    Op::FuseRrf {
                        branches: grounded,
                        k: *k,
                    },
                    output_scope.unwrap_or(scope),
                ))
            }
            Op::Foreign { name } => {
                self.check_foreign_source(name)?;
                Ok((op.clone(), Scope::AnyLocal))
            }
            #[cfg(feature = "federation")]
            Op::ForeignScan { source, .. } => {
                match source.as_ref() {
                    medulla_types::wire::ForeignSourceSpec::Named { name } => {
                        self.check_foreign_source(name)?
                    }
                    _ => return Err(SchemaViolation::UnboundForeignSource),
                }
                // JOIN intersects incoming ids when nonempty, but reseeds from
                // the foreign source when empty. Both paths later rematerialize
                // properties from this same local snapshot.
                Ok((op.clone(), Scope::AnyLocal))
            }
            Op::Attribute {
                input,
                value,
                method,
            } => {
                use medulla_types::wire::{AttributionInput, AttributionMethod};
                let mut input = input.clone();
                let mut method = method.clone();
                if let AttributionInput::Property { name } = &mut input {
                    *name = self.resolve_property(name, &scope, repairs)?;
                }
                if let AttributionMethod::Owen { by } = &mut method {
                    *by = self.resolve_property(by, &scope, repairs)?;
                }
                Ok((
                    Op::Attribute {
                        input,
                        value: *value,
                        method,
                    },
                    scope,
                ))
            }
            #[cfg(feature = "owl")]
            Op::SparqlBgp { .. } | Op::Reason { .. } => Ok((op.clone(), Scope::AnyLocal)),
            #[cfg(feature = "geo")]
            Op::SpatialScan { layer, bbox } => {
                let layer = self.resolve_label(layer, repairs)?;
                Ok((Op::SpatialScan { layer, bbox: *bbox }, Scope::AnyLocal))
            }
            #[cfg(feature = "tensor")]
            Op::TensorScan { layer } => {
                let layer = self.resolve_label(layer, repairs)?;
                Ok((Op::TensorScan { layer }, Scope::AnyLocal))
            }
            Op::DecisionScan { .. } => Ok((op.clone(), Scope::AnyLocal)),
            #[cfg(feature = "timeseries")]
            Op::Window { .. }
            | Op::WindowAgg { .. }
            | Op::TsScan { .. }
            | Op::SensorFuse { .. }
            | Op::SensorAlign { .. }
            | Op::Events { .. } => Ok((op.clone(), Scope::AnyLocal)),
            #[cfg(feature = "wasm-udf")]
            Op::Udf { .. } => Ok((op.clone(), Scope::AnyLocal)),
            #[cfg(feature = "stream")]
            Op::Cep { .. } => Ok((op.clone(), Scope::AnyLocal)),
            #[cfg(feature = "epistemic")]
            Op::EvidenceFor { .. }
            | Op::Contradicts { .. }
            | Op::SupportedBy { .. }
            | Op::ExplainBelief { .. } => Ok((op.clone(), Scope::AnyLocal)),
            _ => Ok((op.clone(), scope)),
        }
    }

    fn check_foreign_source(&self, name: &str) -> Result<(), SchemaViolation> {
        match &self.foreign_sources {
            Some(sources) if !sources.contains(name) => Err(SchemaViolation::UnknownSource {
                source: name.to_string(),
                known: sources.iter().cloned().collect(),
            }),
            None => Err(SchemaViolation::UnboundForeignSource),
            _ => Ok(()),
        }
    }

    /// `MATCH (:label)` always reads the local snapshot, even when a later
    /// stage replaces its rows with foreign data.
    fn ground_scan(
        &self,
        label: &str,
        repairs: &mut Vec<Repair>,
    ) -> Result<(Op, Scope), SchemaViolation> {
        let label = self.resolve_label(label, repairs)?;
        let scope = Scope::Labels(BTreeSet::from([label.clone()]));
        Ok((Op::Scan { label }, scope))
    }

    /// `Expand`/`Propagate`: resolve the optional relationship name.
    fn ground_edge_op(&self, op: &Op, repairs: &mut Vec<Repair>) -> Result<Op, SchemaViolation> {
        let mut op = op.clone();
        let (rel, edge_preds) = match &mut op {
            Op::Expand {
                rel, edge_preds, ..
            }
            | Op::Propagate {
                rel, edge_preds, ..
            } => (rel, edge_preds),
            _ => return Ok(op),
        };
        if let Some(name) = rel.as_ref() {
            *rel = Some(self.resolve_relationship(name, repairs)?);
        }
        *edge_preds = self.ground_preds(edge_preds, &Scope::Edges(rel.clone()), repairs)?;
        Ok(op)
    }

    fn ground_preds(
        &self,
        preds: &[Pred],
        scope: &Scope,
        repairs: &mut Vec<Repair>,
    ) -> Result<Vec<Pred>, SchemaViolation> {
        preds
            .iter()
            .map(|pred| self.ground_pred(pred, scope, repairs))
            .collect()
    }

    fn ground_pred(
        &self,
        pred: &Pred,
        scope: &Scope,
        repairs: &mut Vec<Repair>,
    ) -> Result<Pred, SchemaViolation> {
        let mut pred = pred.clone();
        match &mut pred {
            Pred::And { preds } | Pred::Or { preds } => {
                *preds = self.ground_preds(preds, scope, repairs)?;
            }
            Pred::Not { pred: inner } => {
                **inner = self.ground_pred(inner, scope, repairs)?;
            }
            Pred::JsonPath { path, .. } => {
                // The snapshot schema records top-level keys, not document
                // shapes. Check the root key; deeper path segments keep their
                // existing JSONPath semantics and are never guessed.
                if let Some(medulla_core::jsonpath::Segment::Key(key)) =
                    medulla_core::jsonpath::parse_path(path)
                        .as_deref()
                        .and_then(|p| p.first())
                {
                    let schemas = self.scope_schemas(scope);
                    let fields: BTreeSet<&String> =
                        schemas.iter().flat_map(|s| s.fields.iter()).collect();
                    if !fields.iter().any(|field| field.as_str() == key) {
                        return Err(self.unknown_property(key, scope, &fields));
                    }
                }
            }
            other => {
                if let Some(prop) = pred_property(other) {
                    *prop = self.resolve_property(prop, scope, repairs)?;
                }
            }
        }
        Ok(pred)
    }

    fn resolve_label(
        &self,
        label: &str,
        repairs: &mut Vec<Repair>,
    ) -> Result<String, SchemaViolation> {
        if self.labels.contains_key(label) {
            return Ok(label.to_string());
        }
        match unique_match(self.labels.keys(), label) {
            Some(found) => Ok(repaired(repairs, label, found)),
            None => Err(SchemaViolation::UnknownLabel {
                label: label.to_string(),
                known: self.labels.keys().take(MAX_LABELS).cloned().collect(),
            }),
        }
    }

    fn resolve_relationship(
        &self,
        rel: &str,
        repairs: &mut Vec<Repair>,
    ) -> Result<String, SchemaViolation> {
        if self.relationships.contains(rel) {
            return Ok(rel.to_string());
        }
        match unique_match(self.relationships.iter(), rel) {
            Some(found) => Ok(repaired(repairs, rel, found)),
            None => Err(SchemaViolation::UnknownRelationship {
                relationship: rel.to_string(),
                known: self.relationships.iter().cloned().collect(),
            }),
        }
    }

    fn resolve_property(
        &self,
        prop: &str,
        scope: &Scope,
        repairs: &mut Vec<Repair>,
    ) -> Result<String, SchemaViolation> {
        let schemas = self.scope_schemas(scope);
        if (prop == "id" && !matches!(scope, Scope::Edges(_)))
            || schemas.iter().any(|s| s.fields.contains(prop))
        {
            return Ok(prop.to_string());
        }
        if let Some(stored) = ontology_target(&schemas, prop) {
            return Ok(repaired(repairs, prop, stored));
        }
        let fields: BTreeSet<&String> = schemas.iter().flat_map(|s| s.fields.iter()).collect();
        match unique_match(fields.iter().copied(), prop) {
            Some(found) => Ok(repaired(repairs, prop, found)),
            None => Err(self.unknown_property(prop, scope, &fields)),
        }
    }

    /// The stored schemas against which this operation reads properties.
    fn scope_schemas(&self, scope: &Scope) -> Vec<&LabelSchema> {
        match scope {
            Scope::AnyLocal => self
                .labels
                .values()
                .chain(std::iter::once(&self.unlabelled))
                .collect(),
            Scope::Edges(Some(rel)) => self.edge_schemas.get(rel).into_iter().collect(),
            Scope::Edges(None) => self
                .edge_schemas
                .values()
                .chain(std::iter::once(&self.untyped_edges))
                .collect(),
            Scope::Labels(labels) => labels
                .iter()
                .filter_map(|label| self.labels.get(label))
                .collect(),
        }
    }

    fn unknown_property(
        &self,
        prop: &str,
        scope: &Scope,
        fields: &BTreeSet<&String>,
    ) -> SchemaViolation {
        let labels = match scope {
            Scope::Labels(labels) => labels.iter().cloned().collect(),
            Scope::Edges(Some(rel)) => vec![format!("edges :{rel}")],
            Scope::Edges(None) => vec!["any edge".to_string()],
            _ => vec!["any label".to_string()],
        };
        SchemaViolation::UnknownProperty {
            property: prop.to_string(),
            labels,
            known: fields
                .iter()
                .take(MAX_PROPERTIES_PER_LABEL * 2)
                .map(|f| (*f).clone())
                .collect(),
        }
    }
}

/// The labels a node carries, under the same keys the engine's label scan reads.
fn node_labels(props: &serde_json::Map<String, serde_json::Value>) -> BTreeSet<String> {
    let mut labels: BTreeSet<String> = medulla_core::graph::NODE_LABEL_KEYS
        .iter()
        .filter_map(|key| props.get(*key).and_then(|v| v.as_str()))
        .map(str::to_string)
        .collect();
    if let Some(list) = props.get("labels").and_then(|v| v.as_array()) {
        labels.extend(list.iter().filter_map(|v| v.as_str()).map(str::to_string));
    }
    labels.retain(|label| !label.is_empty());
    labels
}

fn in_domain(label: &str, domains: &[String]) -> bool {
    domains.is_empty() || domains.iter().any(|d| d == label)
}

/// Link `name` to the one stored key of `schema` whose normal form is in `names`.
fn link_property(schema: &mut LabelSchema, name: &str, names: &BTreeSet<String>) {
    if schema.fields.contains(name) {
        return;
    }
    let mut hits = schema.fields.iter().filter(|f| names.contains(&norm(f)));
    if let (Some(stored), None) = (hits.next(), hits.next()) {
        let stored = stored.clone();
        schema.ontology.insert(name.to_string(), stored);
    }
}

/// The stored key an ontology name maps onto, when every label in scope agrees.
fn ontology_target(schemas: &[&LabelSchema], prop: &str) -> Option<String> {
    let targets: BTreeSet<&String> = schemas
        .iter()
        .filter_map(|s| s.ontology.get(prop))
        .collect();
    let mut targets = targets.into_iter();
    match (targets.next(), targets.next()) {
        (Some(stored), None) => Some(stored.clone()),
        _ => None,
    }
}

/// The one candidate whose normal form equals `name`'s; `None` for zero or several.
fn unique_match<'a>(candidates: impl Iterator<Item = &'a String>, name: &str) -> Option<String> {
    let wanted = norm(name);
    let mut hits = candidates.filter(|c| norm(c) == wanted);
    match (hits.next(), hits.next()) {
        (Some(found), None) => Some(found.clone()),
        _ => None,
    }
}

fn repaired(repairs: &mut Vec<Repair>, from: &str, to: String) -> String {
    repairs.push(Repair {
        from: from.to_string(),
        to: to.clone(),
    });
    to
}

/// The node property a leaf predicate reads.
fn pred_property(pred: &mut Pred) -> Option<&mut String> {
    match pred {
        Pred::Eq { prop, .. }
        | Pred::GtNum { prop, .. }
        | Pred::LtNum { prop, .. }
        | Pred::Cmp { prop, .. }
        | Pred::In { prop, .. }
        | Pred::Between { prop, .. }
        | Pred::IsNull { prop } => Some(prop),
        #[cfg(feature = "geo")]
        Pred::SpatialWithin { column, .. }
        | Pred::SpatialDWithin { column, .. }
        | Pred::SpatialContains { column, .. }
        | Pred::SpatialCovers { column, .. }
        | Pred::SpatialTouches { column, .. }
        | Pred::SpatialCrosses { column, .. }
        | Pred::SpatialOverlaps { column, .. }
        | Pred::SpatialEquals { column, .. }
        | Pred::SpatialDisjoint { column, .. } => Some(column),
        _ => None,
    }
}

fn question_tokens(question: &str) -> BTreeSet<String> {
    question
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| t.len() > 2)
        .map(str::to_ascii_lowercase)
        .collect()
}

/// True when a question token matches `name` or one of its `_`-separated parts,
/// ignoring a plural `s`.
fn mentions(tokens: &BTreeSet<String>, name: &str) -> bool {
    let whole = norm(name);
    let parts = name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase);
    std::iter::once(whole).chain(parts).any(|part| {
        !part.is_empty() && (tokens.contains(&part) || tokens.contains(&format!("{part}s")))
    })
}

/// The properties one label lists in the hint: ontology-mapped first (as
/// `ontologyName = stored_key`), then the ones the question mentions, then the
/// rest, capped at [`MAX_PROPERTIES_PER_LABEL`].
fn hint_fields(schema: &LabelSchema, tokens: &BTreeSet<String>) -> Vec<String> {
    let mapped: BTreeSet<&String> = schema.ontology.values().collect();
    let mut out: Vec<String> = schema
        .ontology
        .iter()
        .map(|(name, stored)| format!("{name} = {stored}"))
        .collect();
    let mut rest: Vec<&String> = schema
        .fields
        .iter()
        .filter(|f| !mapped.contains(f))
        .collect();
    rest.sort_by_key(|f| (!mentions(tokens, f), (*f).clone()));
    out.extend(rest.into_iter().cloned());
    out.truncate(MAX_PROPERTIES_PER_LABEL);
    out
}

/// Run the bounded correction loop (MEDULLA-FEDERATED-QUERY-R058.4.2, R058.4.7):
/// plan, enforce the budget, parse with the caller grammar and ground against
/// `context`. A parse error or schema violation goes back to the planner as a
/// typed message; after `max_steps` failed steps the request is refused with
/// [`NlQueryError::Ungrounded`] carrying every step. An over-budget answer is
/// refused at once, as before.
pub fn plan_grounded(
    planner: &dyn NlPlanner,
    nl: &str,
    context: &SchemaContext,
    budget: NlQueryBudget,
    max_steps: usize,
) -> Result<GroundedCandidate, NlQueryError> {
    let hint = context.hint(nl);
    let mut steps: Vec<NlPlanStep> = Vec::new();
    for _ in 0..max_steps.clamp(1, MAX_STEPS) {
        let prompt = feedback_hint(&hint, &steps);
        let text = planner.plan(nl, &prompt).map_err(NlQueryError::Planner)?;
        enforce_budget(&text, budget)?;
        match ground_text(context, &text) {
            Ok((uql, grounded)) => {
                steps.push(step(&text, "accepted"));
                return Ok(GroundedCandidate {
                    uql,
                    plan: grounded.plan,
                    steps,
                    repairs: grounded.repairs,
                });
            }
            Err(outcome) => steps.push(step(&text, &outcome)),
        }
    }
    Err(NlQueryError::Ungrounded { steps })
}

/// Parse and ground one candidate; the disclosed UQL is the planner's text
/// unless grounding rewrote a name, in which case it is the canonical text of
/// the plan that will run.
fn ground_text(context: &SchemaContext, text: &str) -> Result<(String, Grounded), String> {
    let plan = crate::uql::parse(text).map_err(|e| e.render(text))?;
    let grounded = context.ground(&plan).map_err(|v| v.to_string())?;
    if grounded.repairs.is_empty() {
        return Ok((text.to_string(), grounded));
    }
    let uql = grounded
        .plan
        .to_uql()
        .map_err(|e| format!("repaired plan has no canonical UQL: {e:?}"))?;
    Ok((uql, grounded))
}

fn step(text: &str, outcome: &str) -> NlPlanStep {
    NlPlanStep {
        dialect: "uql".to_string(),
        query_text: text.to_string(),
        outcome: outcome.to_string(),
    }
}

/// The schema hint plus the typed outcome of every refused step.
fn feedback_hint(hint: &str, steps: &[NlPlanStep]) -> String {
    let mut out = hint.to_string();
    for step in steps {
        out.push_str(&format!(
            "\nA previous answer was refused.\nAnswer: {}\nReason: {}\nReply with ONE corrected \
             UQL query that uses only the schema above.",
            step.query_text, step.outcome
        ));
    }
    out
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
