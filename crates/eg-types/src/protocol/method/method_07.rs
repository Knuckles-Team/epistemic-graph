macro_rules! __eg_method_chunk_7 {
    () => {
        __eg_method_chunk_8!(@acc [

    /// Atomic compare-and-swap: set `(namespace, key)` to `new` (`None` ⇒ delete) iff
    /// the current value equals `expected` (both absent ⇒ the key must not exist).
    /// Returns whether the swap happened. `expected`/`new` are MessagePack `bin`.
    #[cfg(feature = "kv")]
    KvCas {
        namespace: String,
        key: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(default, with = "serde_bytes")]
        expected: Option<Vec<u8>>,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(default, with = "serde_bytes")]
        new: Option<Vec<u8>>,
    },


    // ── SQLite `.db` file import/export (CONCEPT:EG-KG.query.eg-feature/EG-332) ─────────
    // Read/write a real on-disk `sqlite3` `.db` FILE (the documented EG-075 follow-up),
    // distinct from the `sqlite-wire` NDJSON dialect surface. NOT graph-scoped: both ops
    // accept a logical `.db` filename under an operator-provisioned private transfer root
    // and move rows through the process-global user-table
    // store (the SAME `TableStore` the `Method::Sql` DDL/DML + pgwire paths use), so they
    // self-route in dispatch like the Blob*/Kv* ops. Each is a BATCH op — ONE engine
    // round-trip that reads/writes the whole file, never per-row. The variants only exist
    // with the `sqlite-file` feature (which pulls the bundled C sqlite kept OUT of pi); a
    // build without it drops them from the enum, so a slim/pi build can't reach the arm.
    /// Import every user table (+ its rows) from logical `.db` filename `path`
    /// into the engine's user-table store (CONCEPT:EG-KG.query.eg-feature). A table that already exists
    /// is REPLACED (drop-then-recreate) so the import mirrors the file. Returns a `Json`
    /// report `{"source":"sqlite", "imported_tables":[{"table","rows"},…]}`.
    #[cfg(feature = "sqlite-file")]
    ImportSqliteFile {
        path: String,
    },

    /// Export user tables OUT to a fresh, valid `sqlite3` `.db` logical filename `path` that the
    /// `sqlite3` CLI can open (CONCEPT:EG-KG.query.full-protocol). `tables` empty ⇒ every user table; else
    /// exactly the named tables (each must exist). Publication is private and atomic.
    /// Returns aggregate table counts without a host path.
    #[cfg(feature = "sqlite-file")]
    ExportSqliteFile {
        path: String,
        #[serde(default)]
        tables: Vec<String>,
    },


    // ── RDF/SPARQL (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / KG-2.218 — native semantic-web surface) ──
    // The RDF dataset maps onto the SAME property-graph the rest of the engine uses
    // (resource object ⇒ typed edge `{relationship: predicate}`; literal object ⇒ a typed
    // JSON property cell preserving xsd datatype + @lang; rdf:type ⇒ the engine
    // `type` label; named graph ⇒ the target registry graph). So these are
    // GRAPH-SCOPED ops (they target `req.graph`) and route through the normal
    // dispatch_graph_op chain like Sql/Cypher — NOT a separate top-level store.
    //
    // `AddTriples` is a DURABLE MUTATION: it writes nodes + edges into the target
    // graph. It is replayed by re-parsing its source text (deterministic — the same
    // Turtle yields the same triples ⇒ the same node/edge writes), mirroring how
    // `BatchUpdate` replays. `GetRdf` (serialize OUT) and `Sparql` are read-only.
    /// Parse `turtle` OR `ntriples` (exactly one non-empty) and store the triples
    /// into the request's graph (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql). Returns a `Raw` `LoadReport`
    /// (`{triples, multivalue}`). Gated `rdf`; a build without it
    /// drops the variant → the dispatch not-built catch-all.
    #[cfg(feature = "rdf")]
    AddTriples {
        /// Turtle document (empty ⇒ use `ntriples`).
        #[serde(default)]
        turtle: String,
        /// N-Triples document (empty ⇒ use `turtle`).
        #[serde(default)]
        ntriples: String,
    },

    /// Serialize the request's graph back OUT to RDF as N-Triples (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql).
    /// Returns a `Raw` `String` (the canonical, order-independent form) — the
    /// datatype/lang-faithful inverse of `AddTriples`. Read-only.
    #[cfg(feature = "rdf")]
    GetRdf,

    /// Physically RETRACT triples from the request's graph (CONCEPT:EG-KG.query.named-graph-support) — the
    /// inverse of `AddTriples`. Parses `turtle` OR `ntriples` (exactly one non-empty)
    /// and surgically removes each triple (a literal triple drops the property cell; a
    /// resource triple removes the one matching typed edge). DURABLE (WAL-replayed by
    /// re-parsing + re-removing). This is the reusable retract op the ontology UNLOAD
    /// path + SPARQL `DELETE DATA` build on. Returns a `Raw` count. Gated `rdf`.
    #[cfg(feature = "rdf")]
    RemoveTriples {
        /// Turtle document (empty ⇒ use `ntriples`).
        #[serde(default)]
        turtle: String,
        /// N-Triples document (empty ⇒ use `turtle`).
        #[serde(default)]
        ntriples: String,
    },

    /// DROP the request's named graph (CONCEPT:EG-KG.query.named-graph-support): physically clear ALL of its RDF
    /// content — the property-graph nodes/edges AND the lossless multi-valued-literal
    /// quad-store rows for this graph. DURABLE (WAL-replayed as a clear). The SPARQL
    /// `DROP/CLEAR GRAPH` op + ontology lifecycle teardown route here. Returns a `Raw`
    /// `"ok"`. Gated `rdf`. (Distinct from `DeleteGraph`, which evicts the registry
    /// entry; this empties the graph's RDF while keeping the graph addressable.)
    #[cfg(feature = "rdf")]
    DropNamedGraph,

    /// Evaluate a SPARQL 1.1 SELECT over the request's graph (CONCEPT:EG-KG.ontology.concept-11).
    /// Returns a `Raw` [`SparqlResult`] (`{vars, rows}`; each row a cell list aligned
    /// to `vars`, an unbound cell is `nil`). Read-only. Gated `sparql`.
    ///
    /// `base_iri` + `type_convention` carry an OPTIONAL LPG→RDF projection vocabulary
    /// (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary). Both default to empty ⇒ the IDENTITY projection (node-type
    /// and property keys emitted verbatim, no `rdf:type` synthesis), which preserves
    /// the prior behavior for every existing caller. A caller (e.g. agent-utilities)
    /// that sets `base_iri = "http://agent-utilities.dev/ontology#"` +
    /// `type_convention = "camel"` makes the engine project the LIVE property graph
    /// into that vocabulary — `<node> rdf:type <base + CamelCase(type)>` and
    /// `<node> <base + prop> <v>` — so a by-class query (`?s a au:Agent`) resolves
    /// natively. The engine itself hardcodes NO ontology URL; the vocabulary is the
    /// caller's.
    #[cfg(feature = "sparql")]
    Sparql {
        query: String,
        /// Projection base namespace IRI. Empty ⇒ identity projection.
        #[serde(default)]
        base_iri: String,
        /// `rdf:type` object naming: `"camel"` ⇒ CamelCase the type local name under
        /// `base_iri`; empty / `"raw"` ⇒ verbatim. Only meaningful with `base_iri`.
        #[serde(default)]
        type_convention: String,
    },

    /// OBDA / R2RML VIRTUAL GRAPH query (CONCEPT:EG-KG.query.r2rml-virtual-graph /
    /// CONCEPT:EG-KG.query.obda-query-rewrite) — Ontology-Based Data Access: run a SPARQL query
    /// against a set of foreign tabular sources exposed as RDF via an R2RML-style
    /// mapping, WITHOUT ever materializing the whole dataset. `tables` names the
    /// engine's OWN SQL user tables (the `eg_query::TableStore` behind `query`, the same
    /// store `Method::Sql` DDL/DML and `ImportSqliteFile` write) to register as foreign
    /// sources under their own table name (a [`TriplesMap::logical_source`] target);
    /// `mapping` is either a standard R2RML Turtle document (`@prefix rr: …`) or the
    /// compact EG-101 textual form (`SOURCE`/`SUBJECT`/`CLASS`/`COLUMN`/`REF`/`CONST`
    /// directives) — auto-detected. The query rewrites to a projection-pushed scan of
    /// only the query-relevant table columns (see `eg_rdf::obda`), materializes ONLY
    /// those triples into a transient view, and evaluates the SAME SPARQL engine over
    /// it — so this is a REAL query-rewrite OBDA path, not a full ETL/materialize step.
    /// Returns a `Raw` [`SparqlResult`]. Read-only (never writes the user table OR the
    /// request's graph). Gated `obda` (implies `sparql` + `query`).
    #[cfg(feature = "obda")]
    SparqlVirtual {
        /// The SPARQL query to run against the virtual graph.
        query: String,
        /// An R2RML Turtle document OR the compact EG-101 textual mapping form.
        mapping: String,
        /// The user-table names the mapping's `TriplesMap`s reference as
        /// `logical_source`s — each is registered as a foreign source under its own
        /// name before the mapping is parsed and the query is run.
        tables: Vec<String>,
        /// LIVE external relational sources (Postgres/MySQL) registered as foreign OBDA
        /// sources IN ADDITION to `tables` (CONCEPT:EG-KG.query.obda-predicate-pushdown,
        /// W4.11). Each binds a `logical_source` name to an external DB table; the query's
        /// column projection AND its row-level `FILTER`s are pushed into a real
        /// `SELECT … WHERE …`. Needs a `federation-sql` server build for the live path.
        /// Empty ⇒ engine-own-tables-only (the prior behavior).
        #[serde(default)]
        external_sources: Vec<ObdaExternalSource>,
    },

    /// Run the native OWL 2 (EL⁺ + RL) reasoner over the request's graph and
    /// materialize entailments (CONCEPT:EG-KG.ontology.incremental-materialization). Classifies the OWL axioms already
    /// in the graph (the TBox loaded via `AddTriples`) plus any extra `ontology`
    /// Turtle, then returns a `Raw` [`OwlReasonResult`]: the derived named-class
    /// subsumptions, the inferred instance→class memberships (incl. ones reached only
    /// through existential restrictions / role chains), and a consistency verdict. The
    /// `Op::Reason` plan op reuses the SAME classifier as a RowSet source. Read-only
    /// (it does not mutate the graph). Gated `owl`.
    #[cfg(feature = "owl")]
    OwlReason {
        /// Extra OWL axioms as Turtle (empty ⇒ reason over the graph's own axioms).
        #[serde(default)]
        ontology: String,
        /// When set, restrict the returned instance memberships to this class (its
        /// inferred members) — the materialize-one-class shape. Empty ⇒ all classes.
        #[serde(default)]
        target_class: String,
        /// The absolute namespace a bare string node `type` (e.g. `"Agent"`) is
        /// bridged into before classification (`eg_rdf::owl::bridge_type_to_class`) —
        /// independent of `target_class`, which ONLY controls filtering (BUG-281: the
        /// two used to be conflated, so an empty `target_class` — its own documented
        /// "all classes" case — could never supply a namespace, and a caller wanting
        /// "reason over everything" always hit `OwlReason requires an absolute target
        /// class`). Empty ⇒ fall back to `target_class`'s own namespace when
        /// `target_class` is absolute (the pre-existing convenience for a caller that
        /// only ever set one field); a class bridge for a bare string `type` is only
        /// possible once SOME absolute namespace is available from either field.
        #[serde(default)]
        class_base: String,
        /// Confidence threshold τ in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13). The result carries a
        /// per-entailment confidence (axioms/facts may be uncertain; the closure
        /// propagates it — `eg:confidence` annotations × the per-node confidence ×
        /// Ebbinghaus decay). Only entailments with `confidence ≥ min_confidence` are
        /// returned. `0.0` keeps everything (and a HARD ontology yields all `1.0`).
        min_confidence: f64,
    },

    /// DISTRIBUTED confidence-weighted OWL reasoning over the UNION of `graphs`
    /// (CONCEPT:EG-KG.ontology.concept-13): gathers each graph/shard's TBox axioms + decayed-confidence
    /// type facts, runs ONE weighted EL⁺/RL closure over the union (the cross-shard
    /// union-read seam — KG-2.171), and returns the SAME [`OwlReasonResult`] a
    /// single-graph `OwlReason` would over the same axioms in one graph. The single-
    /// shard fast path stays `OwlReason`. Read-only. Gated `owl`.
    #[cfg(feature = "owl")]
    OwlReasonDistributed {
        /// The graphs (shards) whose axioms + facts to union and reason over.
        graphs: Vec<String>,
        /// Extra OWL axioms as Turtle (a shared TBox over the sharded ABox; empty ⇒
        /// only the axioms already present across the graphs).
        #[serde(default)]
        ontology: String,
        /// Restrict instance memberships to this class (empty ⇒ all classes).
        #[serde(default)]
        target_class: String,
        /// See `OwlReason::class_base` (BUG-281) — independent of `target_class`.
        #[serde(default)]
        class_base: String,
        /// Confidence threshold τ in `[0,1]` (see `OwlReason::min_confidence`).
        #[serde(default)]
        min_confidence: f64,
    },


    /// OWL proof-tree EXPLANATION (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation) — Stardog's flagship
    /// "explanation" feature, native here. Classifies the request's graph (its own TBox
    /// axioms, loaded via `AddTriples`, plus any extra `ontology` Turtle) with confidence
    /// propagation, then reconstructs the FULL recursive proof tree for the ONE named-class
    /// subsumption `sub ⊑ sup` — WHICH axiom(s) + WHICH premise subsumption(s) derived it,
    /// recursively down to the asserted/reflexive leaves — via
    /// [`crate`]-independent reconstruction of `eg_rdf::owl::Classification::explain`'s
    /// justification DAG (CONCEPT:EG-KG.ontology.justification-tracking). Returns a `Raw`
    /// [`OwlExplainResult`]. Read-only (does not mutate the graph). Gated `owl`.
    #[cfg(feature = "owl")]
    OwlExplain {
        /// Extra OWL axioms as Turtle (empty ⇒ reason over the graph's own axioms).
        #[serde(default)]
        ontology: String,
        /// The SUBCLASS side of the subsumption to explain (a class IRI, `<...>` or bare —
        /// canonicalized the same way `target_class` is elsewhere).
        sub: String,
        /// The SUPERCLASS side of the subsumption to explain.
        sup: String,
    },


    // ── Custom-rule reasoning (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog / EG-023 — runtime SWRL/Datalog rules) ──
    // Run a parameterised rule-reasoning request over the request's graph view (its
    // folded TBox axioms + asserted facts) PLUS any inline `ontology_ttl` and the
    // user `rules`, returning the inferred facts. Read-only (it reasons over an
    // off-lock snapshot and never mutates the graph), so it routes through the normal
    // `dispatch_graph_op` chain like `Sparql`/`OwlReason`. The fields mirror eg-rdf's
    // `RuleReasonRequest` 1:1 (kept inline so the protocol crate — at the bottom of the
    // DAG — carries no eg-rdf type); the handler rebuilds the request and calls
    // `eg_rdf::run_rule_reasoning_on_view`. Result is a `Raw` `RuleReasonResponse`.
    // Gated `rdf`; a build without it drops the variant → the dispatch not-built catch-all.
    #[cfg(feature = "rdf")]
    RunRules {
        /// Optional Turtle carrying extra TBox axioms AND/OR ABox facts (empty ⇒ reason
        /// over the graph's own folded axioms/facts only).
        #[serde(default)]
        ontology_ttl: String,
        /// User rule strings (SWRL-ish / Datalog syntax).
        #[serde(default)]
        rules: Vec<String>,
        /// When set, restrict the returned facts to this predicate (IRI or bare name).
        #[serde(default)]
        query_predicate: Option<String>,
        /// Drop facts whose confidence is below this threshold.
        #[serde(default)]
        min_confidence: f64,
        /// When true, return only the DERIVED facts (omit the asserted base).
        #[serde(default)]
        derived_only: bool,
    },


    // ── SHACL Core validation (CONCEPT:EG-KG.ontology.concept-6) ───────────────────────────────
    // Validate an RDF DATA graph against an RDF SHAPES graph, producing an
    // `sh:ValidationReport` (`conforms` + a list of `sh:ValidationResult`). The
    // engine half is the pure-Rust `eg-shacl` crate. This variant is UNCONDITIONAL in
    // the enum (like `Backup`/`Restore`, CONCEPT:EG-KG.sharding.reshard-on-restore); the HANDLER is gated on the
    // `shacl` feature — a build without it drops the handler arm and the request falls
    // through to the dispatch "not available in this build" catch-all. The fields are
    // inline Strings so the protocol crate (bottom of the DAG) carries no eg-shacl type;
    // the handler parses both documents and returns a `Json` report.
    /// Validate `data_graph` against `shapes`, both RDF Turtle documents (CONCEPT:EG-KG.ontology.concept-6).
    /// An EMPTY `data_graph` validates against the LIVE RDF of the request's graph (the
    /// same triples `GetRdf` would export). Returns a `Json` `sh:ValidationReport`.
    /// Read-only. Handler gated `shacl` (implies `rdf`).
    ShaclValidate {
        /// The shapes graph as a Turtle document.
        shapes: String,
        /// The data graph as a Turtle document; empty ⇒ use the request's live graph.
        #[serde(default)]
        data_graph: String,
    },


    /// X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard) — (re)register a graph's
    /// SHACL shapes as WRITE-TIME closed-world integrity constraints (ICV, reusing
    /// `eg-shacl`'s existing `IcvPolicyRegistry`/`WriteGuard` verbatim — no new
    /// validator). The required `mode` value is `"enforce"`: a violating change ABORTS the
    /// `AddTriples`/`RemoveTriples`/`ApplyMutation` commit with the introduced
    /// violations, each carrying its SPARQL witness). `graph` names the target graph;
    /// `None` sets the DEFAULT-graph policy. `shapes` is the SHACL shapes Turtle
    /// document — e.g. the SHACL a connector-manifest compiler emits alongside its
    /// RLS/ABAC policy output (agent-utilities side) and must be non-empty. Like `ShaclValidate`,
    /// this variant is UNCONDITIONAL in the enum; the HANDLER is gated `shacl` — a
    /// build without it drops the handler arm and the request falls through to the
    /// dispatch "not available in this build" catch-all.
    IcvConfigure {
        #[serde(deserialize_with = "deserialize_required_option")]
        graph: Option<String>,
        mode: String,
        shapes: String,
    },


    // ── ShEx (Shape Expressions) Core validation (CONCEPT:EG-KG.compute.concept-2) ────────────
    // The complement to `ShaclValidate` (EG-132): validate that focus nodes of an RDF
    // DATA graph CONFORM to shape expressions in a ShEx schema, driven by a shape map
    // (focus node → shape label). The engine half is the pure-Rust `eg-shex` crate. Like
    // `ShaclValidate`/`Backup` (EG-090), this variant is UNCONDITIONAL in the enum; the
    // HANDLER is gated on the `shex` feature — a build without it drops the handler arm
    // and the request falls through to the dispatch "not available in this build"
    // catch-all. The fields are inline strings so the protocol crate (bottom of the DAG)
    // carries no eg-shex type; the handler parses the ShExJ schema + the data graph and
    // returns a `Json` `ShexReport`.
    /// Validate `data_graph` against a **ShExJ** `schema` for a `shape_map` (a list of
    /// `[node_iri, shape_label]` pairs; `"START"` selects the schema's start shape)
    /// (CONCEPT:EG-KG.compute.concept-2). `data_graph` is an RDF Turtle document; an EMPTY `data_graph`
    /// validates against the LIVE RDF of the request's graph (the same triples `GetRdf`
    /// would export). Returns a `Json` `ShexReport`. Read-only. Handler gated `shex`
    /// (implies `rdf`).
    ShexValidate {
        /// The ShEx schema as a ShExJ (JSON abstract-syntax) document.
        schema: String,
        /// The data graph as a Turtle document; empty ⇒ use the request's live graph.
        #[serde(default)]
        data_graph: String,
        /// The shape map: `[node_iri, shape_label]` pairs. `shape_label` may be `"START"`.
        #[serde(default)]
        shape_map: Vec<[String; 2]>,
    },


    // ── Streaming / CDC / subscriptions / reactivity (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ──
    // A reactive surface over the engine's per-graph durable change record (the
    // ledger). Every durable mutation the dispatch shell records also emits an
    // ordered, cursor-addressable `CdcEvent` (node/edge add/remove/update with
    // before/after) into a per-graph in-memory feed. Built on the SAME one-Response-
    // per-Request transport — a consumer TAILS via a `from_seq` cursor (CdcRead /
    // Watch long-poll), never a side-channel socket or a protocol-v2. All variants
    // are gated `streaming` (folds into pi/node/cluster/full — no heavy dep); a build
    // without it drops them → the dispatch "not available in this build" catch-all.
    /// Read the ordered change feed for `graph` from cursor `from_seq` (inclusive),
    /// up to `limit` events (CONCEPT:EG-KG.query.streaming-cdc-subscriptions). Returns a `Raw`
    /// `CdcReadResult` (B-8, 2026-08-13: `{events, gap, watermark, head_seq, epoch}` —
    /// was a bare `Vec<CdcEvent>`, which made a genuinely-caught-up cursor
    /// indistinguishable from one that silently fell off the ring; see
    /// `CdcReadResult`'s doc). The consumer re-reads from `last.seq + 1` to skip what
    /// it has seen, and MUST check `gap` before treating an empty `events` as "caught
    /// up" — `gap: true` means it is not. `limit` 0 ⇒ a default cap.
    #[cfg(feature = "streaming")]
    CdcRead {
        graph: String,
        from_seq: u64,
        #[serde(default)]
        limit: u32,
    },

    /// Register a continuous query (CONCEPT:EG-KG.query.streaming-cdc-subscriptions): a named, incrementally-
    /// maintained aggregate/filter view over a graph's CDC feed. `spec_msgpack` is a
    /// MessagePack `ContinuousQuerySpec`. Returns a `String` (the name). Re-registering
    /// the same name replaces it (and re-seeds from the current graph state).
    #[cfg(feature = "streaming")]
    RegisterContinuousQuery {
        name: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        spec_msgpack: Vec<u8>,
    },

    /// Read the current incrementally-maintained result of a continuous query. Returns
    /// a `Raw` `ContinuousQueryResult`.
    #[cfg(feature = "streaming")]
    ReadContinuousQuery {
        name: String,
    },

    /// Drop a continuous query. Returns `Bool` (true if it existed).
    #[cfg(feature = "streaming")]
    DropContinuousQuery {
        name: String,
    },

    /// LISTEN/NOTIFY-style long-poll subscription (CONCEPT:EG-KG.query.wire-codec): return the
    /// matching CDC changes for `graph` since `from_seq`, blocking up to `timeout_ms`
    /// for the FIRST one if none are pending yet (then returns what arrived). `label`
    /// (empty ⇒ all) filters by node/edge label. Returns a `Raw` `WatchBatch`
    /// (`{events, next_seq, gap, watermark, head_seq, epoch}` — B-8, 2026-08-13: tails
    /// the identical ephemeral ring `CdcRead` does, so it carries the same gap
    /// signalling; see `CdcReadResult`'s doc); the client passes `next_seq` back to
    /// resume, and MUST check `gap` before treating an empty `events` as "caught up".
    /// Transport-compatible: one Request → one Response, cursor-driven.
    #[cfg(feature = "streaming")]
    Watch {
        graph: String,
        from_seq: u64,
        #[serde(default)]
        label: String,
        #[serde(default)]
        timeout_ms: u64,
    },

    /// Register a trigger/reaction (CONCEPT:EG-KG.query.wire-codec): when a CDC change in `graph`
    /// matches `label` (empty ⇒ any) + `op` ("add"|"remove"|"update"|"any"), record a
    /// firing carrying `action_msgpack` (an opaque reaction payload). Returns the name.
    #[cfg(feature = "streaming")]
    RegisterTrigger {
        name: String,
        graph: String,
        #[serde(default)]
        label: String,
        op: String,
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(default, with = "serde_bytes")]
        action_msgpack: Vec<u8>,
    },

    /// Drop a trigger. Returns `Bool`.
    #[cfg(feature = "streaming")]
    DropTrigger {
        name: String,
    },

    /// List the triggers registered on `graph`. Returns a `Raw` `Vec<TriggerInfo>`.
    #[cfg(feature = "streaming")]
    ListTriggers {
        graph: String,
    },

    /// Poll the fired-trigger log for `graph` from cursor `from_seq` (CONCEPT:EG-KG.query.wire-codec):
    /// the reactions that fired since the cursor. Returns a `Raw` `FiredTriggersResult`
    /// (B-8 follow-up, 2026-08-13: `{fired, gap, watermark, head_seq, epoch}` — the
    /// fired-trigger log is a second bounded ring with the identical ephemerality as
    /// the CDC ring `CdcRead` reads; see `CdcReadResult`'s doc); the consumer
    /// dispatches each action then resumes from `last.fire_seq + 1`, and MUST check
    /// `gap` before treating an empty `fired` as "caught up".
    #[cfg(feature = "streaming")]
    FiredTriggers {
        graph: String,
        from_seq: u64,
        #[serde(default)]
        limit: u32,
    },


    // ── Live CEP standing queries (CONCEPT:EG-KG.query.protocol-types) ───────────────────────────
    // The PUSH half of the event-stream + complex-event-processing modality
    // (CONCEPT:EG-KG.query.pipelined-execution): register a CEP pattern ONCE as a live standing query, then
    // pull the matches it detects as CDC changes flow. The CDC hub (feature
    // `streaming`) is adapted into an `eg_stream::Event` bus that feeds the live
    // `eg_stream::live::CepEngine` (feature `stream`); each detected `Match` is fanned
    // to the registering subscriber over a broadcast channel with drop-oldest + lag
    // backpressure. Transport-compatible with everything else here: one Request → one
    // Response, cursor-free — `CepPoll` LONG-POLLS (like `Watch`) for the next match.
    // Gated `streaming` on the wire so the variants exist wherever the CDC surface
    // does; the ENGINE (and thus a real handler) additionally needs `stream` — a build
    // with `streaming` but not `stream` (e.g. `pi`) drops these to the dispatch
    // "not available in this build" catch-all, exactly like any other feature-off op.
    /// Register a live CEP standing query (CONCEPT:EG-KG.query.protocol-types). `pattern_msgpack` is a
    /// MessagePack `CepPatternSpec` (the same pattern algebra `Op::Cep` carries); `buffer`
    /// (0 ⇒ a default) bounds how many unconsumed matches are retained for a lagging
    /// poller before the oldest are dropped. Returns a `Count` — the subscription id to
    /// pass to `CepPoll` / `CepUnsubscribe`.
    #[cfg(feature = "streaming")]
    CepSubscribe {
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        pattern_msgpack: Vec<u8>,
        #[serde(default)]
        buffer: u32,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_7;
