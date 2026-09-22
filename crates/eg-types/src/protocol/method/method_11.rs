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
        ]);
    };
}

pub(crate) use __eg_method_chunk_11;
