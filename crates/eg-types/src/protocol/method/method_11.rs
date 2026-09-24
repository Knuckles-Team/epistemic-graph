macro_rules! __eg_method_chunk_11 {
    (@acc [$($variants:tt)*]) => {
        __eg_method_finish!(@acc [
$($variants)*

    // ── Typed WorkItem reads (EH-219) ──────────────────────────────────────
    /// The caller's view of one native WorkItem row, or `null` when no row with
    /// this id is visible to `tenant`. `tenant` must equal the verified request
    /// tenant (`ACCESS_DENIED` otherwise); lease owner, epoch and fencing token
    /// are never returned. See [`crate::work_item_read`].
    GetWorkItem {
        tenant: String,
        work_item_id: String,
    },
    /// One bounded page of `tenant`'s WorkItems, optionally of one `kind`.
    /// Pages by the limit / scan / byte bounds of `AgentComponent.Search`; an
    /// empty page may still carry `next_cursor`. The cursor is opaque and
    /// bound to the tenant it was minted for.
    ListWorkItems {
        tenant: String,
        #[serde(default)]
        cursor: Option<String>,
        limit: u32,
        #[serde(default)]
        kind: Option<String>,
        /// Keep only items whose top-level `metadata` holds every one of these
        /// key/value pairs exactly (at most 8 keys) -- e.g. a correlation id or
        /// an event subject (graph-os EG-5). Filters within the same bounded
        /// scan, so it never widens a page's cost.
        #[serde(default)]
        metadata_match: Option<serde_json::Map<String, serde_json::Value>>,
    },

    /// A terminal WorkItem of `tenant` and the provenance (RunTrace /
    /// ToolCall refs and the verified OutcomeEvaluation receipt) its native
    /// `CommitWorkItemResult { outcome_extension }` bound, or `null` when the
    /// item is not visible or carries no committed outcome (graph-os EG-3).
    GetWorkItemOutcome {
        tenant: String,
        work_item_id: String,
    },

    // ── Native control leases (graph-os EG-2) ──────────────────────────────
    /// Issue one active control lease: an immutable, time-boxed grant record.
    /// A row already holding the id answers `collision` and is left untouched.
    /// `request.tenant` must equal the verified request tenant. See
    /// [`crate::control_lease`].
    IssueControlLease {
        request: crate::control_lease::IssueControlLeaseRequest,
    },
    /// Move one control lease along a legal edge (`active -> consumed |
    /// revoked | expired`, `consumed -> revoked | expired`), compare-and-set
    /// on the revision the caller read. A lease never returns to `active`.
    TransitionControlLease {
        request: crate::control_lease::TransitionControlLeaseRequest,
    },
    /// The caller's view of one control lease, or `null` when no lease with
    /// this id is visible to `tenant` (which must equal the verified tenant).
    GetControlLease {
        tenant: String,
        lease_id: String,
    },

    // ── Declared freshness (EH-400) ────────────────────────────────────────
    /// The request graph's per-class invalidation events with `version >
    /// after_version` (at most `limit`; 0 = the default page), its
    /// `eg:volatilityClass` policy when that changed since `policy_after`
    /// (`None` = always include it), and the watermark freshness of every foreign
    /// source the graph holds a watermark for. A caller that sees `gap`, or an
    /// `epoch` different from the one it last saw, must drop everything it cached
    /// for the graph. See [`crate::freshness`].
    FreshnessFeed {
        after_version: u64,
        #[serde(default)]
        limit: u32,
        #[serde(default)]
        policy_after: Option<u64>,
    },

    // ── Policy evolution (EH-346 / EH-347) ────────────────────────────────
    /// Capture-first open-weight policy evolution: the attested capability,
    /// trajectory captures, immutable model-policy versions, external
    /// training-run receipts and held-out evaluation receipts. Every record is
    /// immutable and content-addressed in the request graph; each write
    /// self-translates into exactly one `CreateNodeIfAbsent`. EG records and
    /// relates -- it never trains. See [`crate::policy_evolution`].
    PolicyEvolution {
        op: Box<crate::policy_evolution::PolicyEvolutionOp>,
    },
    /// ENGINE-INTERNAL: the durable WorkItem-kernel write that stores one
    /// record `PolicyEvolution` already admitted. Refused from the wire; the
    /// only way a policy-evolution row can be created (generic graph writes to
    /// such rows are refused by the row guard).
    PolicyEvolutionStore {
        request: Box<crate::policy_evolution::StoredPolicyRecord>,
    },
        ]);
    };
}

pub(crate) use __eg_method_chunk_11;
