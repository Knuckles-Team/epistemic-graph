//! Modality-specific DTOs referenced by the unified-query wire AST.

#[cfg(any(
    feature = "geo",
    feature = "tensor",
    feature = "timeseries",
    feature = "probabilistic",
    feature = "stream",
    feature = "federation"
))]
use serde::{Deserialize, Serialize};

/// Comparison accepted by the owner-scoped foreign-column read surface.
#[cfg(feature = "federation")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ForeignColumnComparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A predicate over a column named in the registered source mapping.
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignColumnPredicate {
    pub column: String,
    pub comparison: ForeignColumnComparison,
    pub value: serde_json::Value,
}

/// One bounded result row after all local residual predicates and projection.
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignColumnRow {
    pub id: String,
    pub score: Option<f32>,
    pub columns: std::collections::BTreeMap<String, serde_json::Value>,
}

/// SPATIAL — the constructive geometry op applied by `Op::SpatialOp` (CONCEPT:EG-KG.ontology.concept-9).
/// Pure serde here (Pi-safe, no eg-geo dep); the executor maps it to eg-geo's `algebra`
/// behind eg-plan's `geo` gate. Unary ops (`Buffer`/`ConvexHull`/`Simplify`/`Centroid`)
/// derive from the row geometry alone; binary ops (`Union`/`Intersection`/`Difference`)
/// take the second operand as a WKT literal.
#[cfg(feature = "geo")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SpatialOpKind {
    /// Grow the geometry outward by `distance` (a convex buffer polygon).
    Buffer { distance: f64 },
    /// The convex hull of the geometry's vertices.
    ConvexHull,
    /// Douglas–Peucker vertex reduction at `tolerance`.
    Simplify { tolerance: f64 },
    /// The centroid (a `Point`).
    Centroid,
    /// The (convex) union of the row geometry with `wkt`.
    Union { wkt: String },
    /// The intersection of the row geometry with `wkt` (Sutherland–Hodgman; convex clip).
    Intersection { wkt: String },
    /// The difference of the row geometry minus `wkt` (documented subset).
    Difference { wkt: String },
}

/// TENSOR — how `TensorOpKind::Reduce` collapses one axis (CONCEPT:EG-KG.storage.content-addressed-dedup). Mirrors
/// eg-tensor's `ReduceKind`, but defined HERE (pure serde, no eg-tensor dep) so the
/// wire stays Pi-safe; the executor maps it to eg-tensor's enum behind eg-plan's
/// `tensor` gate.
#[cfg(feature = "tensor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TensorReduceKind {
    Sum,
    Mean,
    Max,
    Min,
}

/// TENSOR — the scalar op `TensorOpKind::Elementwise` applies to every element
/// (CONCEPT:EG-KG.storage.content-addressed-dedup). Mirrors eg-tensor's `ElementwiseOp`; pure serde here.
#[cfg(feature = "tensor")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TensorElementwiseOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// TENSOR — the transform carried by `Op::TensorOp` (CONCEPT:EG-KG.storage.content-addressed-dedup): one of the
/// eg-tensor array ops, applied per-row to each row's tensor. `Slice` gathers a
/// hyper-rectangle (`ranges[d] = (start, end)`, one per axis); `Reduce` collapses one
/// `axis` with `kind`; `Elementwise` applies `op` with `scalar` to every element. PURE
/// serde (only `usize`/`f64`/the two plain enums) — the actual N-D array math lives in
/// eg-tensor behind eg-plan's `tensor` gate; this is the wire variant.
#[cfg(feature = "tensor")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TensorOpKind {
    Slice {
        ranges: Vec<(usize, usize)>,
    },
    Reduce {
        axis: usize,
        kind: TensorReduceKind,
    },
    Elementwise {
        op: TensorElementwiseOp,
        scalar: f64,
    },
}

/// FUSE — the per-channel interpolation mode carried by [`FuseStream`]
/// (CONCEPT:EG-KG.query.multi-rate-sensor-stream). Chosen PER STREAM because modalities differ: a pose is
/// `Linear`-interpolable, a discrete mode/label wants `AsofHold`, a noisy raw reading may
/// want `Nearest`. PURE serde — the resampling math lives in `eg_tsdb::fusion` behind
/// eg-plan's `timeseries` gate; this is the wire variant.
#[cfg(feature = "timeseries")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FuseInterp {
    /// The closest sample in time to the grid instant (either side); a tie resolves to the
    /// EARLIER sample so the result is deterministic.
    Nearest,
    /// Linear interpolation between the two samples bracketing the grid instant. An instant
    /// OUTSIDE the sample span is a GAP — no extrapolation.
    Linear,
    /// Last-known value at-or-before the grid instant (forward-fill / zero-order hold). An
    /// instant before the first sample is a GAP.
    AsofHold,
}

/// FUSE — one input stream to [`Op::SensorAlign`] (CONCEPT:EG-KG.query.multi-rate-sensor-stream): the sensor
/// `layer` (a node label / `type`, resolved off the snapshot exactly as `Op::SensorFuse`
/// resolves its streams) plus the [`FuseInterp`] mode that channel is resampled under.
/// PURE serde; this is the wire variant.
#[cfg(feature = "timeseries")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FuseStream {
    pub layer: String,
    pub interp: FuseInterp,
}

/// FUSE — the DECLARED time base [`Op::SensorAlign`] resamples onto
/// (CONCEPT:EG-KG.query.multi-rate-sensor-stream). This is what separates `SensorAlign` from `Op::SensorFuse`:
/// `SensorFuse` fuses onto the UNION clock of the samples themselves (data-driven instants,
/// tolerance/ASOF only), while `SensorAlign` fuses onto a clock the CALLER declares, so the
/// output instants are independent of when the sensors happened to fire.
///
/// * `Uniform { from_ns, to_ns, step_ns }` — the half-open grid `from_ns, from_ns+step_ns,
///   … < to_ns`. One fused row per grid instant.
/// * `Tumbling { width_ns, step_ns }` — EG-067 tumbling windows of `width_ns` aligned as
///   `(t/width)*width` spanning the union sample span, each internally resampled onto a
///   `step_ns` sub-grid. One fused row per window.
///
/// All bounds are INTEGER nanoseconds, not `f64` seconds: a grid instant has to be exact
/// (it is an output identity), and integer ns keeps it so.
#[cfg(feature = "timeseries")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FuseClock {
    Uniform {
        from_ns: i64,
        to_ns: i64,
        step_ns: i64,
    },
    Tumbling {
        width_ns: i64,
        step_ns: i64,
    },
}

/// PROBABILISTIC — the evidence for a conjugate Bayesian update carried by
/// [`ProbQuery::Conditional`] (CONCEPT:EG-KG.compute.uncertainty-values). Mirrors `eg_compute::probabilistic::Evidence`,
/// but defined HERE (pure serde, no eg-compute dep) so the wire stays Pi-safe; the executor
/// maps it to eg-compute's enum behind eg-plan's `probabilistic` gate. `Bernoulli` counts
/// are the sufficient statistic for a Beta prior; `Gaussian` observations (with a KNOWN
/// likelihood variance) for a Gaussian prior over the unknown mean.
#[cfg(feature = "probabilistic")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProbEvidenceSpec {
    Bernoulli {
        successes: f64,
        failures: f64,
    },
    Gaussian {
        observations: Vec<f64>,
        known_variance: f64,
    },
}

/// PROBABILISTIC — the probabilistic query carried by `Op::Probabilistic` (CONCEPT:EG-KG.compute.uncertainty-values):
/// one closed-form question asked of each row's stored `Distribution` VALUE.
/// * `Expectation` — the mean `E[X]`.
/// * `Marginal { at, label }` — the marginal probability: the density `pdf(at)` for the
///   continuous variants, or the mass `pmf(label)` when `label` is set (a `Categorical`).
/// * `Conditional { evidence }` — the posterior MEAN after a conjugate Bayesian update
///   with `evidence` (the "conditional" query).
/// * `Sample { seed }` — one DETERMINISTIC seeded draw (same seed ⇒ same value; no
///   RNG-from-clock, so a plan is reproducible).
///
/// PURE serde — the distribution math lives in eg-types (`Distribution`) + eg-compute
/// (`bayesian_update`) behind eg-plan's `probabilistic` gate; this is the wire variant.
#[cfg(feature = "probabilistic")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProbQuery {
    Expectation,
    Marginal {
        #[serde(default)]
        at: f64,
        #[serde(default)]
        label: Option<String>,
    },
    Conditional {
        evidence: ProbEvidenceSpec,
    },
    Sample {
        seed: u64,
    },
}

/// STREAM — a per-event attribute predicate for a CEP matcher (CONCEPT:EG-KG.query.pipelined-execution). Mirrors
/// eg-stream's `AttrPredicate`, but defined HERE (pure serde, no eg-stream dep) so the
/// wire stays Pi-safe; the executor maps it to eg-stream's enum behind eg-plan's
/// `stream` gate.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CepAttrPredSpec {
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Gt {
        field: String,
        value: f64,
    },
    Lt {
        field: String,
        value: f64,
    },
    Exists {
        field: String,
    },
}

/// STREAM — one event matcher: an optional event `key` + attribute predicates that ALL
/// must hold (CONCEPT:EG-KG.query.pipelined-execution). Mirrors eg-stream's `EventMatcher`; pure serde here.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CepMatcherSpec {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub preds: Vec<CepAttrPredSpec>,
}

/// STREAM — the sliding / tumbling window a CEP pattern runs over (CONCEPT:EG-KG.query.pipelined-execution).
/// Mirrors eg-stream's `Window`; pure serde here.
#[cfg(feature = "stream")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CepWindowSpec {
    Sliding { size: u64 },
    Tumbling { size: u64 },
}

/// STREAM — the CEP pattern tree (CONCEPT:EG-KG.query.pipelined-execution): `Sequence` (matchers in order within
/// the window), `Within` (a duration constraint wrapping an inner pattern), `Absence`
/// (`a` NOT-followed-by `b` within `within`). Mirrors eg-stream's `CepPattern`; pure
/// serde — the actual NFA lives in eg-stream behind eg-plan's `stream` gate.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CepNodeSpec {
    Sequence(Vec<CepMatcherSpec>),
    Within {
        within: u64,
        pattern: Box<CepNodeSpec>,
    },
    Absence {
        a: CepMatcherSpec,
        b: CepMatcherSpec,
        within: u64,
    },
}

/// STREAM — the full CEP spec carried by `Op::Cep` (CONCEPT:EG-KG.query.pipelined-execution): the `pattern` tree
/// and the `window` it is evaluated over. PURE serde — the executor turns it into an
/// eg-stream `run(pattern, events, window)` call behind eg-plan's `stream` gate; this is
/// the wire variant.
#[cfg(feature = "stream")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CepPatternSpec {
    pub pattern: CepNodeSpec,
    pub window: CepWindowSpec,
}

/// A FOREIGN (external) RowSet source for the federation `Op::ForeignScan`
/// (CONCEPT:EG-KG.query.query-federation, Lane P). A federated query reads rows from a source OUTSIDE
/// the local engine and composes them with the local graph/vector/SQL ops — so a
/// `ForeignScan` is just another RowSet leaf, like `Scan`/`Reason`/`SparqlBgp`.
/// Source kinds share one wire enum; the `ForeignSource` trait in eg-plan turns each into
/// a [`crate::RowSet`].
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ForeignSourceSpec {
    /// A REMOTE epistemic-graph engine, reached over the SAME length-prefixed
    /// MessagePack + HMAC transport this engine speaks. The federation client
    /// connects to `endpoint` (a `host:port` TCP address), sends a `Uql` statement
    /// (UQL) — or, when `uql` is empty, a `CypherQuery` — against the remote `graph`,
    /// and projects the result rows into a local RowSet. Every request is an `eg2.`
    /// verified-context envelope; an empty secret or incomplete context fails before
    /// dialing. This composes the engine with ANOTHER engine without a Python
    /// round-trip — the cross-engine federation seam.
    RemoteEngine {
        /// `host:port` of the remote engine's TCP listener.
        endpoint: String,
        /// The remote graph to query (e.g. `__commons__`).
        graph: String,
        /// HMAC-SHA256 secret used by the remote verified-context issuer. Empty is
        /// invalid; native federation never downgrades to insecure or legacy auth.
        #[serde(default)]
        secret: String,
        /// Identity, tenant, audience, capabilities, active policy version, and
        /// delegation path signed into every remote request. The receiving engine
        /// independently checks these claims against deployment policy and RBAC.
        /// Boxed: `RequestContextClaims` is the single field that made this the
        /// largest `ForeignSourceSpec` variant by a wide margin.
        #[serde(default)]
        context: Box<crate::acl::RequestContextClaims>,
        /// A UQL query run on the remote (its rows seed the RowSet). When empty, `cypher`
        /// is used instead.
        #[serde(default)]
        uql: String,
        /// A Cypher query run on the remote when `uql` is empty (must `RETURN` an id
        /// column named by `id_field`).
        #[serde(default)]
        cypher: String,
        /// For the Cypher path: the RETURN column that carries the node id. Ignored on
        /// the UQL path (a UQL plan's rows are already `[id, score?]`).
        #[serde(default)]
        id_field: String,
    },
    /// A GENERIC HTTP/JSON source. The federation client issues an HTTP GET to `url`
    /// (pure-Rust rustls client — NOT openssl, gated OUT of the Pi tier), walks the
    /// JSON response to the array at `json_path` (a dotted path, e.g. `data.items`),
    /// and maps each element into a RowSet row via `field_map`: the element field
    /// named `field_map.id` becomes the row id, and (optionally) `field_map.score`
    /// becomes the row score. Any external REST API thereby becomes a joinable RowSet.
    HttpJson {
        /// The HTTP(S) URL to GET.
        url: String,
        /// Dotted path to the JSON array of rows (empty ⇒ the response is itself the
        /// array).
        #[serde(default)]
        json_path: String,
        /// Maps a JSON element to a RowSet row (which field is the id / the score).
        field_map: HttpFieldMap,
    },
    /// An EXTERNAL relational-SQL database (Postgres/MySQL/…) (CONCEPT:EG-KG.query.feature). The
    /// federation client connects to `dsn`, runs `query`, and maps each result row to a
    /// RowSet row: the column named `id_field` becomes the row id (stringified), and
    /// (optionally) `score_field` becomes the row score. This lets ONE unified plan JOIN
    /// an external SQL table with the LOCAL graph/vector — the "engine federates external
    /// SQL" half that sql-mcp alone cannot give: sql-mcp speaks SQL to the engine, this
    /// lets the engine speak SQL OUT to a foreign RDBMS and fuse the rows in-plan.
    ///
    /// This wire variant is PURE serde (it carries only the DSN + query strings), so it
    /// is present whenever `federation` is on. The actual SQL *driver* (a pure-Rust /
    /// rustls client — NEVER openssl) lives in eg-plan behind its own `federation-sql`
    /// gate and is folded OUT of the Pi tier (the Pi contract: no SQL driver / rustls /
    /// openssl). A `federation` build WITHOUT `federation-sql` accepts + registers a
    /// `Sql` spec but errors clearly if a plan actually tries to fetch it.
    Sql {
        /// The external database DSN (e.g. `postgres://host:5432/db`).
        dsn: String,
        /// The SQL query to run against the external DB. Its result columns are mapped
        /// to RowSet rows via `id_field` / `score_field`.
        query: String,
        /// The result column whose value is the row id (stringified).
        id_field: String,
        /// The result column whose numeric value is the row score (absent ⇒ unscored).
        #[serde(default)]
        score_field: Option<String>,
        /// Query-selected columns the registering owner permits a column query to
        /// read. Empty for legacy id/score-only registrations. Names are checked
        /// as SQL identifiers before projection or filter pushdown (EH-572).
        #[serde(default)]
        columns: Vec<String>,
    },
    /// A NAMED reference to a foreign source registered in the executor's
    /// `ForeignSourceRegistry` (CONCEPT:EG-KG.query.closure-backed-source). Unlike self-describing variants
    /// (which carry their connection spec inline), this carries ONLY a `name`
    /// — the executor resolves it to a concrete, pre-registered [`ForeignSource`] at
    /// plan time. This is the resolution seam the UQL `FOREIGN "<name>"` clause needs:
    /// the parser only ever sees a name, and binding that name to a live source lives
    /// with the server/facade (which owns the registry), not in the wire DTO. A `Named`
    /// spec handed to a source builder WITHOUT a registry is a clean typed error, never
    /// a silent empty set.
    Named {
        /// The registry key naming a pre-registered foreign source.
        name: String,
    },
    /// A Trino/Starburst coordinator. This is a query-time source, not an ingest
    /// cursor. Registration must bind a verified service identity and probe the
    /// catalog before a driver may advertise any pushdown capability. No credential
    /// is serialized into this spec.
    Trino {
        endpoint: String,
        catalog: String,
        schema: String,
        query: String,
        id_field: String,
        #[serde(default)]
        score_field: Option<String>,
    },
    /// A Cypher-capable external graph. AGE uses a PostgreSQL transport; Neo4j
    /// uses Bolt; FalkorDB uses its own protocol. The dialect is explicit so a
    /// generic Cypher claim cannot accidentally select the wrong driver.
    Cypher {
        backend: ForeignCypherBackend,
        endpoint: String,
        graph: String,
        query: String,
        id_field: String,
        #[serde(default)]
        score_field: Option<String>,
    },
    /// Spark output is a sealed batch artifact reference. Interactive Spark SQL
    /// pushdown is deliberately absent: a query may read only a completed,
    /// registered artifact, never launch a job or trust a caller-provided path.
    SparkBatch {
        artifact_ref: String,
        id_field: String,
        #[serde(default)]
        score_field: Option<String>,
    },
    /// An operation-bound REST/HTTP API source (CONCEPT:EG-KG.query.query-federation,
    /// FQR-11). Unlike [`ForeignSourceSpec::HttpJson`] (a raw, inline, unbound GET), an
    /// `Api` spec names a REGISTERED operation that a mapped entity reads through; the
    /// registry resolves `operation` to a concrete, capability-probed endpoint. Never
    /// carries a credential.
    Api {
        /// The registered operation name a mapped entity's read resolves to.
        operation: String,
        /// What this operation declares it can do — notably whether a key filter may
        /// be pushed into the request (CONCEPT:EG-KG.query.query-federation).
        #[serde(default)]
        capabilities: ForeignOperationCapabilities,
    },
    /// An operation-bound MCP tool call (CONCEPT:EG-KG.query.query-federation, FQR-11).
    /// `server` names the registered MCP server; `tool` the tool a mapped entity's
    /// discovered operation invokes.
    Mcp {
        server: String,
        tool: String,
        #[serde(default)]
        capabilities: ForeignOperationCapabilities,
    },
    /// An operation-bound A2A agent skill invocation (CONCEPT:EG-KG.query.query-federation,
    /// FQR-11). `agent` names the registered A2A agent; `skill` the skill a mapped
    /// entity's discovered operation invokes.
    A2a {
        agent: String,
        skill: String,
        #[serde(default)]
        capabilities: ForeignOperationCapabilities,
    },
    /// An operation-bound GraphQL source (CONCEPT:EG-KG.query.query-federation, FQR-11).
    /// `endpoint` names the registered GraphQL endpoint; `operation` the named query
    /// a mapped entity's discovered operation runs.
    GraphQl {
        endpoint: String,
        operation: String,
        #[serde(default)]
        capabilities: ForeignOperationCapabilities,
    },
}

/// Declared capabilities of an operation-bound foreign source
/// (CONCEPT:EG-KG.query.query-federation, FQR-11): what the REGISTRATION asserts the
/// bound operation can do, never introspected from the caller's request. A key filter
/// is pushed into the remote request only when `key_filter_pushdown` is true; EG always
/// keeps the exact local residual filter regardless, so an under-declaring source never
/// returns wrong rows — only a less-pushed-down request.
#[cfg(feature = "federation")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ForeignOperationCapabilities {
    /// The bound operation accepts a server-side key filter (e.g. `id IN (...)`).
    #[serde(default)]
    pub key_filter_pushdown: bool,
}

/// A WRITE-side mirror target (EG-DURABLE-KERNEL-R024's mirror-target half,
/// distinct from the read-side [`ForeignSourceSpec`]). A mirror target
/// receives committed rows fanned out from the local mutation outbox; the
/// wire DTO names the target, the binding/registration lives with
/// `eg-plan`'s mirror builder. This is the typed-model slice (`R024.2.1`):
/// the `FanOut` spec and its validation. Driving real writes through a bound
/// driver is a later child.
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MirrorTargetSpec {
    /// Fans a mirrored row out to every named downstream target. `targets`
    /// must be non-empty and carry no duplicate -- an empty or
    /// self-duplicating fan-out is a configuration error, never a silent
    /// drop of one destination.
    FanOut {
        /// Registry keys of the downstream mirror targets this one fans out
        /// to. Resolved the same way `ForeignSourceSpec::Named` resolves a
        /// foreign source: through the executor's registry, never by this
        /// pure DTO.
        targets: Vec<String>,
    },
    /// A single, directly-addressable downstream mirror target
    /// (EG-DURABLE-KERNEL-R024.4) — the one-destination counterpart to
    /// `FanOut`'s many. Resolved the same way `ForeignSourceSpec::Named`
    /// resolves a foreign source: through the executor's registry, never by
    /// this pure DTO.
    Named {
        /// The registry key naming the one pre-registered mirror target.
        name: String,
    },
}

#[cfg(feature = "federation")]
impl MirrorTargetSpec {
    /// Reject a fan-out spec that cannot name a sensible destination set:
    /// empty, or with a duplicate target name. Does not attempt to resolve
    /// or reach any target -- that is the bound driver's job.
    pub fn validate(&self) -> Result<(), MirrorTargetSpecError> {
        match self {
            Self::FanOut { targets } => {
                if targets.is_empty() {
                    return Err(MirrorTargetSpecError::EmptyFanOut);
                }
                let mut seen = std::collections::HashSet::with_capacity(targets.len());
                for target in targets {
                    if !seen.insert(target.as_str()) {
                        return Err(MirrorTargetSpecError::DuplicateFanOutTarget(target.clone()));
                    }
                }
                Ok(())
            }
            Self::Named { name } => {
                if name.is_empty() {
                    return Err(MirrorTargetSpecError::EmptyNamedTarget);
                }
                Ok(())
            }
        }
    }
}

/// A `MirrorTargetSpec` failed validation before any target was reached.
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MirrorTargetSpecError {
    EmptyFanOut,
    DuplicateFanOutTarget(String),
    EmptyNamedTarget,
}

#[cfg(feature = "federation")]
impl std::fmt::Display for MirrorTargetSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyFanOut => {
                f.write_str("fan-out mirror target spec names no downstream target")
            }
            Self::DuplicateFanOutTarget(name) => {
                write!(
                    f,
                    "fan-out mirror target spec names {name:?} more than once"
                )
            }
            Self::EmptyNamedTarget => {
                f.write_str("named mirror target spec names no downstream target")
            }
        }
    }
}

#[cfg(feature = "federation")]
impl std::error::Error for MirrorTargetSpecError {}

/// External Cypher dialect and transport are a single explicit choice.
#[cfg(feature = "federation")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ForeignCypherBackend {
    Neo4j,
    Age,
    FalkorDb,
}

/// Which JSON element fields become a RowSet row's id / score (for
/// [`ForeignSourceSpec::HttpJson`]).
#[cfg(feature = "federation")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HttpFieldMap {
    /// The element field whose value is the row id (stringified).
    pub id: String,
    /// The element field whose numeric value is the row score (absent ⇒ unscored).
    #[serde(default)]
    pub score: Option<String>,
    /// Mapping-approved output column name to JSON element field. Existing
    /// id/score-only registrations deserialize with no exposed columns.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub columns: std::collections::BTreeMap<String, String>,
}
