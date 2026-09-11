/// A parsed GraphQL value used as an argument. The read subset is ints, floats,
/// strings, booleans (enough for `first: 10` and `name: "Alice"`); writes
/// (CONCEPT:EG-KG.query.mutation) add OBJECT and LIST values so a mutation can carry a `props`
/// map, e.g. `createNode(label: "Person", props: {name: "Alice", tags: ["a", "b"]})`.
/// CONCEPT:EG-KG.query.fragments-variables-directives adds [`GqlValue::Var`] — an unresolved `$name` reference the resolver
/// substitutes from the execution variables before the value is used.
#[derive(Clone, Debug, PartialEq)]
pub enum GqlValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    /// A nested input object — an ordered list of `(field, value)` pairs.
    Object(Vec<(String, GqlValue)>),
    /// A list of values.
    List(Vec<GqlValue>),
    /// The `null` literal.
    Null,
    /// A `$name` variable reference (CONCEPT:EG-KG.query.fragments-variables-directives), substituted at execution time.
    Var(String),
}

/// One field in a selection set: a name, optional arguments, and a nested selection
/// (empty for a scalar leaf). `alias` is the response key when an `alias: name` form is
/// used (GraphQL aliasing); it defaults to `name`.
///
/// This is the DESUGARED field the resolver/mutation paths consume: by the time a
/// [`Field`] exists, fragment spreads have been inlined, `@skip`/`@include` applied, and
/// `$var` argument refs substituted (CONCEPT:EG-KG.query.fragments-variables-directives).
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// The response key — the alias if given, else `name`.
    pub alias: String,
    /// The graph field being resolved (the node label / property / edge relationship).
    pub name: String,
    pub args: Vec<(String, GqlValue)>,
    pub selection: Vec<Field>,
}

/// A parsed query operation: the top-level selection set (its fields are the root node
/// types). The operation name (if any) is irrelevant to execution.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub roots: Vec<Field>,
}

/// A parsed mutation operation (CONCEPT:EG-KG.query.mutation): the top-level selection set whose
/// fields are WRITE root fields (`createNode`/`updateNode`/`deleteNode`/`addEdge`/
/// `removeEdge`). Each field's args carry the write payload; its selection set (if any)
/// shapes the returned object the resolver materializes from the post-write graph.
#[derive(Clone, Debug, PartialEq)]
pub struct Mutation {
    pub roots: Vec<Field>,
}

/// A parsed subscription operation (CONCEPT:EG-KG.query.mutation): structurally identical to a
/// [`Query`] — the resolver serves it as a poll over the current matches (and, when a
/// streaming transport is wired, as a change-stream emitting the same shape).
#[derive(Clone, Debug, PartialEq)]
pub struct Subscription {
    pub roots: Vec<Field>,
}

/// A parsed top-level GraphQL operation: a read query, a write mutation, or a
/// subscription (CONCEPT:EG-KG.query.mutation). [`parse`] returns the [`Query`] case directly for the
/// read path; [`parse_operation`] returns the full enum for callers that also write.
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    Query(Query),
    Mutation(Mutation),
    Subscription(Subscription),
}
