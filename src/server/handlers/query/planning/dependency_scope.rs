//! Dependency scope for cache reuse across graph version changes.

/// Compute a SOUND dependency set for a UQL/`UnifiedQuery` plan
/// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7 + EH-393), or `None`
/// when the plan's shape cannot be reduced to one — the caller then uses the coarse
/// version-keyed result-cache path (unchanged). What each supported op reads:
///   * `Scan { label }` — a SOURCE: that label (or, unlabeled, the whole node set). Its rows are
///     label-scoped, so a following `Filter` reads only properties the label dimension covers.
///   * `ScanAll` (UQL `MATCH ()`) — a SOURCE over every node: `AllNodes`.
///   * `Project` (UQL `RETURN`) — names score channels; rows pass through unchanged.
///   * `Filter` / `Limit` — narrow the current rows. A `Filter` over rows that are NOT
///     label-scoped (reached by a traversal) reads arbitrary nodes' properties: `AllNodes`.
///   * `Traverse { rel }` — edges of type `rel` ([`Dim::EdgeType`]) through nodes of any label,
///     which the reader's row-security view may hide ([`Dim::RowVisibility`]); a reached node
///     cannot appear or vanish without a `rel` edge being written or cascaded.
///   * `RankNodeDistance` / `RankMentions` — the untyped topology (`AllEdges`) plus visibility.
///   * `Rank` / `RankEmbed` / `RankMmr` — the embedding store: [`Dim::EmbeddingGeneration`] at
///     the live stamp `embedding_generation` (no stamp known ⇒ no dependency set).
///   * `FuseRrf` — every branch's reads, each branch starting from the fused input.
///
/// ANY other op — lexical `RankText` (BM25 over the whole corpus), a temporal `AsOf`, a
/// reasoner/SPARQL/federation/tensor/spatial/tsdb/epistemic leg — reads state the clock does not
/// model, so the WHOLE plan falls back. A dependency set is only ever returned when it PROVABLY
/// captures everything the query reads: a stale hit is a correctness bug.
#[cfg(feature = "result-cache")]
pub(crate) fn plan_dependency_set(
    plan: &eg_plan::Plan,
    embedding_generation: Option<u64>,
) -> Option<eg_core::dep_scope::DepSet> {
    let mut walk = DepWalk {
        dims: Vec::new(),
        has_source: false,
        rows_unscoped: false,
        embedding_generation,
    };
    walk.ops(&plan.ops)?;
    // A plan with no graph SOURCE op (e.g. a pure federation/tsdb seed) is not a bounded graph
    // read — fall back rather than claim a dependency set.
    walk.has_source
        .then(|| eg_core::dep_scope::DepSet::new(walk.dims))
}

/// The accumulator [`plan_dependency_set`] threads through a plan (and each `FuseRrf` branch).
#[cfg(feature = "result-cache")]
struct DepWalk {
    dims: Vec<eg_core::dep_scope::Dim>,
    has_source: bool,
    /// The current rows may be nodes of ANY label (a traversal reached them).
    rows_unscoped: bool,
    embedding_generation: Option<u64>,
}

#[cfg(feature = "result-cache")]
impl DepWalk {
    fn ops(&mut self, ops: &[eg_plan::Op]) -> Option<()> {
        ops.iter().try_for_each(|op| self.op(op))
    }

    fn op(&mut self, op: &eg_plan::Op) -> Option<()> {
        use eg_core::dep_scope::Dim;
        match op {
            eg_plan::Op::Scan { label } => self.source(label),
            eg_plan::Op::ScanAll {} => self.source(""),
            eg_plan::Op::Filter { .. } if self.rows_unscoped => self.dims.push(Dim::AllNodes),
            eg_plan::Op::Filter { .. }
            | eg_plan::Op::Limit { .. }
            | eg_plan::Op::Project { .. } => {}
            eg_plan::Op::Traverse { rel, .. } => {
                self.dims.push(Dim::EdgeType(rel.clone()));
                self.dims.push(Dim::RowVisibility);
                self.rows_unscoped = true;
            }
            eg_plan::Op::RankNodeDistance { .. } | eg_plan::Op::RankMentions {} => {
                self.dims.push(Dim::AllEdges);
                self.dims.push(Dim::RowVisibility);
            }
            eg_plan::Op::Rank { .. }
            | eg_plan::Op::RankEmbed { .. }
            | eg_plan::Op::RankMmr { .. } => {
                self.dims
                    .push(Dim::EmbeddingGeneration(self.embedding_generation?));
            }
            #[cfg(feature = "text")]
            eg_plan::Op::FuseRrf { branches, .. } => self.fuse(branches)?,
            // Any op reading state outside the dependency clock's model ⇒ coarse fallback.
            _ => return None,
        }
        Some(())
    }

    fn source(&mut self, label: &str) {
        use eg_core::dep_scope::Dim;
        self.has_source = true;
        self.rows_unscoped = false;
        self.dims.push(if label.is_empty() {
            Dim::AllNodes
        } else {
            Dim::Label(label.to_string())
        });
    }

    /// Each branch runs over the SAME input rows; the fused rows are unscoped when any branch's
    /// are. A branch's own `Scan` does not make the plan sourced (the fused input is).
    #[cfg(feature = "text")]
    fn fuse(&mut self, branches: &[Vec<eg_plan::Op>]) -> Option<()> {
        let input_unscoped = self.rows_unscoped;
        let has_source = self.has_source;
        let mut fused_unscoped = input_unscoped;
        for branch in branches {
            self.rows_unscoped = input_unscoped;
            self.ops(branch)?;
            fused_unscoped |= self.rows_unscoped;
        }
        self.rows_unscoped = fused_unscoped;
        self.has_source = has_source;
        Some(())
    }
}
