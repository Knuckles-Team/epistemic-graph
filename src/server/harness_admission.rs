//! EG-ORCH-VOCAB-R801/R802 — engine-side admission of the harness
//! vocabulary's five record kinds — component versions (R801), lessons,
//! adoptions, contrasts and probe runs (R802) — on BOTH write paths.
//!
//! The two paths differ in one thing only: whether the engine may MUTATE the
//! write before commit.
//!
//! * The direct `AddNode` path (the graph-ops gateway) may mutate: the engine
//!   stamps the transaction time (`tx_from`, EG-DURABLE-KERNEL-R078/R081) and
//!   fills the `supersedes_id` pointer when the writer left it absent (versions
//!   and lessons). The mutation happens before the durable commit and before
//!   the apply closure clones the method, so durable and serving state cannot
//!   diverge.
//! * The `ChangeEnvelope` path is byte-stable for replay (the replay check
//!   compares reserialized bytes) and its digest covers the operations, so the
//!   engine only VERIFIES: the generated client stamps `tx_from` itself
//!   (EG-DURABLE-KERNEL-R081) and composes the real supersession edge and the
//!   head-window-close ops, naming the superseded record in `supersedes_id`.
//!   A supersession that leaves the pointer absent while a current record
//!   exists is refused, because such a record would supersede nothing.
//!
//! The shared rules: the record decodes strictly (the closed kind sets, the
//! typed evidence and interval checks), the node id is the record's own
//! identity (content address for versions, lessons, adoptions and contrasts;
//! the trace id for probe runs), a published record never mutates (the same
//! writer fields are an idempotent replay; anything else is
//! `HarnessVersionImmutable` / `HarnessRecordImmutable`), a supersession
//! names the CURRENT record (`HarnessSupersessionStale` when it names an
//! older one, `HarnessSupersessionUnknown` when it names nothing the graph
//! holds), and a contrast agrees with its probe runs (same trace ids, same
//! conditions digest — `ContrastConditionsMismatch` otherwise).
//!
//! The envelope check runs under the graph write lock, before any durable
//! work. A batch commits atomically, so one [`HarnessBoundaryState`] spans
//! the batch's envelopes in order: a supersession may name a record written
//! by an EARLIER envelope of the same write (it is pending, not yet in the
//! graph), and a head-window-close CAS in one op is visible to the
//! supersession check of a later op.

use std::collections::BTreeMap;

use crate::graph::GraphCore;
use crate::protocol::Method;
use eg_types::change_envelope::ChangeEnvelope;
use eg_types::orchestration::improvement::{
    decode_adopted_harness_write, decode_harness_contrast_write, decode_harness_probe_run_write,
    decode_lesson_write, is_adopted_harness_write, is_harness_contrast_write,
    is_harness_lesson_write, is_harness_probe_run_write, ImprovementLibraryRefusal,
};
use eg_types::orchestration::{
    decode_component_write, is_harness_component_write, HarnessComponentRefusal,
};

type Props = serde_json::Map<String, serde_json::Value>;

fn decode_props(blob: &[u8]) -> Props {
    eg_types::msgpack::decode_property_object(blob).unwrap_or_default()
}

fn refuse_component(refusal: HarnessComponentRefusal, detail: String) -> String {
    format!("{}: {}", refusal.code(), detail)
}

fn refuse_improvement(refusal: ImprovementLibraryRefusal, detail: String) -> String {
    format!("{}: {}", refusal.code(), detail)
}

/// The transaction-time stamp a harness record must carry (R801.3/R802.4):
/// `tx_from` set, and a positive number.
fn tx_stamp(props: &Props) -> Option<u64> {
    props
        .get("tx_from")
        .and_then(|value| value.as_u64())
        .filter(|stamp| *stamp > 0)
}

/// The three fields the component-version head-scan reads off every
/// candidate — nothing else. Decoding them off the raw msgpack blob (instead
/// of the full property map) keeps the 1k-version chain scan at a fraction
/// of a millisecond (EG-ORCH-VOCAB-R801.6 budget). Unknown fields are
/// skipped; every engine-admitted node decodes fully, so a probe that decodes
/// is a real record.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct ComponentHeadProbe {
    /// The engine-owned write marker on the node's property object.
    #[serde(rename = "_harness_component")]
    marker: bool,
    /// Valid time: from when the version is current.
    valid_from: u64,
    /// Valid time: until when; absent while the version is still current.
    valid_to: Option<u64>,
}

/// The fields the lesson head-scan reads off every candidate: the marker,
/// the identity the head is scoped by, and the transaction stamp the head
/// orders by.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct LessonHeadProbe {
    #[serde(rename = "_harness_lesson")]
    marker: bool,
    task_class: String,
    deficiency: String,
    tx_from: u64,
}

/// The harness records as a write sees them: the live graph, plus the nodes
/// the SAME write has already touched (pending AddNodes and the field merges
/// of pending CASes). The pending layer wins over the graph, because it is
/// the state the later ops of the write apply against.
struct HarnessView<'a> {
    core: &'a GraphCore,
    pending: &'a BTreeMap<String, Props>,
}

impl<'a> HarnessView<'a> {
    /// The effective property object of one node: the pending layer when the
    /// write touched it, the stored blob otherwise.
    fn props(&self, node_id: &str) -> Option<Props> {
        if let Some(props) = self.pending.get(node_id) {
            return Some(props.clone());
        }
        self.core
            .get_node_properties(node_id)
            .as_deref()
            .map(decode_props)
    }

    /// The stored raw property blob of one node, graph side.
    fn blob(&self, node_id: &str) -> Option<Vec<u8>> {
        self.core.get_node_properties(node_id)
    }

    fn has(&self, node_id: &str) -> bool {
        self.pending.contains_key(node_id) || self.core.has_node(node_id)
    }

    /// The effective probe of one node: off the pending layer when it is
    /// there, the raw stored blob otherwise.
    fn probe<T: for<'de> serde::Deserialize<'de>>(&self, node_id: &str) -> Option<T> {
        if self.pending.contains_key(node_id) {
            return None; // handled by the caller's pending-specific probe
        }
        self.blob(node_id)
            .and_then(|stored| rmp_serde::from_slice::<T>(&stored).ok())
    }

    /// The effective `(marker, valid_from, valid_to)` of one candidate
    /// component version.
    fn component_probe(&self, node_id: &str) -> Option<ComponentHeadProbe> {
        if let Some(props) = self.pending.get(node_id) {
            return Some(ComponentHeadProbe {
                marker: props
                    .get("_harness_component")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false),
                valid_from: props
                    .get("valid_from")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0),
                valid_to: props.get("valid_to").and_then(|value| value.as_u64()),
            });
        }
        self.probe(node_id)
    }

    /// The effective `(marker, task_class, deficiency, tx_from)` of one
    /// candidate lesson.
    fn lesson_probe(&self, node_id: &str) -> Option<LessonHeadProbe> {
        if let Some(props) = self.pending.get(node_id) {
            return Some(LessonHeadProbe {
                marker: props
                    .get("_harness_lesson")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false),
                task_class: props
                    .get("task_class")
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string(),
                deficiency: props
                    .get("deficiency")
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string(),
                tx_from: props
                    .get("tx_from")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0),
            });
        }
        self.probe(node_id)
    }

    /// The probe run stored at `harness-probe-run:<trace_id>`, when the write
    /// sees one (pending first, then the graph).
    fn probe_run(
        &self,
        trace_id: &str,
    ) -> Option<eg_types::orchestration::improvement::HarnessProbeRun> {
        let node_id = format!("harness-probe-run:{trace_id}");
        self.props(&node_id)
            .and_then(|props| decode_harness_probe_run_write(&props).ok())
    }

    /// The current (window-open) version node id of one harness component, by
    /// newest `valid_from` — the supersession target of a new version
    /// (R801.2).
    fn component_head(&self, component_id: &str) -> Option<String> {
        let prefix = format!("harness-component:{component_id}:");
        let mut best: Option<(u64, String)> = None;
        for node_id in self
            .core
            .node_ids()
            .into_iter()
            .chain(self.pending.keys().cloned())
        {
            let Some(rest) = node_id.strip_prefix(&prefix) else {
                continue;
            };
            // The node id is `harness-component:<id>:<content digest>`; a
            // digest always starts with `sha256:`, which keeps a component id
            // that is a prefix of another from aliasing its versions.
            if !rest.starts_with("sha256:") {
                continue;
            }
            let Some(probe) = self.component_probe(&node_id) else {
                continue;
            };
            if !probe.marker || probe.valid_to.is_some() {
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|(valid_from, _)| probe.valid_from >= *valid_from)
            {
                best = Some((probe.valid_from, node_id));
            }
        }
        best.map(|(_, node_id)| node_id)
    }

    /// The current lesson of one task class and deficiency, by newest
    /// `tx_from` — the supersession target of a new lesson (R802.4). The
    /// record's own task class and deficiency are re-checked off the probe:
    /// two identity fields in one node id can alias across colons.
    fn lesson_head(&self, task_class: &str, deficiency: &str) -> Option<String> {
        let prefix = format!("harness-lesson:{task_class}:{deficiency}:");
        let mut best: Option<(u64, String)> = None;
        for node_id in self
            .core
            .node_ids()
            .into_iter()
            .chain(self.pending.keys().cloned())
        {
            if !node_id.starts_with(&prefix) {
                continue;
            }
            let Some(probe) = self.lesson_probe(&node_id) else {
                continue;
            };
            if !probe.marker || probe.task_class != task_class || probe.deficiency != deficiency {
                continue;
            }
            if best.as_ref().is_none_or(|(tx, _)| probe.tx_from >= *tx) {
                best = Some((probe.tx_from, node_id));
            }
        }
        best.map(|(_, node_id)| node_id)
    }
}

/// The graph-relative admission of one harness-record write. `New` when the
/// node does not exist (the write proceeds), `Replay` when it is an
/// idempotent rewrite of the stored record (the write is a no-op). `head`
/// carries the supersession target of the record (when the record supersedes
/// by pointer and one exists) so the direct path can fill an absent
/// `supersedes_id` without a second scan. `engine_fills_pointer`
/// distinguishes the two paths: on the direct AddNode path the engine MAY
/// fill an absent pointer from the current record (so "absent with a live
/// head" is legal), while on the ChangeEnvelope path the bytes are
/// byte-stable for replay and the writer MUST name the current record
/// itself (so "absent with a live head" would publish a record that
/// supersedes nothing — refused).
enum Admission {
    New { head: Option<String> },
    Replay,
}

/// The stored record at `node_id` (pending first, then the graph):
/// `Replay` when it is the same writer fields, `RecordImmutable` when it
/// differs, and the `Err` when the node exists but is not the record kind
/// (or not a harness record at all). `stored_matches` reports whether the
/// stored record is one of the given kinds and carries the same
/// writer-supplied fields.
fn stored_record<T: std::fmt::Debug + PartialEq>(
    view: &HarnessView<'_>,
    node_id: &str,
    is_kind: fn(&Props) -> bool,
    decode: fn(&Props) -> Result<T, ImprovementLibraryRefusal>,
    same: fn(&T, &T) -> bool,
    record: &T,
    immutable_code: &str,
) -> Result<Option<bool>, String> {
    // None = no stored node; Some(true) = replay; Some(false) = mutable
    // rewrite; Err = the node exists and is not this record kind.
    let Some(stored) = view.props(node_id) else {
        return Ok(None);
    };
    if is_kind(&stored) {
        if let Ok(published) = decode(&stored) {
            return if same(&published, record) {
                Ok(Some(true))
            } else {
                Err(format!(
                    "{immutable_code}: node '{node_id}' already holds a different record"
                ))
            };
        }
    }
    Err(format!(
        "HarnessRecordMalformed: node '{node_id}' already exists and is not the expected record"
    ))
}

/// The graph-relative admission of one harness-component version write
/// (R801.3).
fn component_admission(
    view: &HarnessView<'_>,
    node_id: &str,
    props: &Props,
    engine_fills_pointer: bool,
) -> Result<Admission, String> {
    let record = decode_component_write(props).map_err(|refusal| {
        refuse_component(refusal, "harness component version record".to_string())
    })?;
    if record.node_id() != node_id {
        return Err(refuse_component(
            HarnessComponentRefusal::ContentDigestMismatch,
            format!(
                "node id '{node_id}' does not match the record's content address '{}'",
                record.node_id()
            ),
        ));
    }
    record.validate().map_err(|refusal| {
        refuse_component(refusal, format!("component '{}'", record.component_id))
    })?;
    match stored_record(
        view,
        node_id,
        is_harness_component_write,
        |stored| {
            decode_component_write(stored).map_err(|_| ImprovementLibraryRefusal::RecordMalformed)
        },
        |a, b| a.same_writer_fields(b),
        &record,
        "HarnessVersionImmutable",
    )? {
        Some(true) => return Ok(Admission::Replay),
        Some(false) => unreachable!("stored_record refuses a mutable rewrite"),
        None => {}
    }
    // The supersession pointer names the current version, or nothing when the
    // component is new.
    let head = view.component_head(&record.component_id);
    match record.supersedes_id.as_deref() {
        Some(superseded) => match head.as_deref() {
            Some(current) if current == superseded => {}
            Some(current) => {
                return Err(refuse_component(
                    HarnessComponentRefusal::SupersessionStale,
                    format!(
                        "component '{}' is current at version '{current}', not '{superseded}'",
                        record.component_id
                    ),
                ))
            }
            None => {
                if !view.has(superseded) {
                    return Err(refuse_component(
                        HarnessComponentRefusal::SupersessionUnknown,
                        format!(
                            "component '{}' supersedes version '{superseded}' the graph does not hold",
                            record.component_id
                        ),
                    ));
                }
            }
        },
        None if !engine_fills_pointer && head.is_some() => {
            return Err(refuse_component(
                HarnessComponentRefusal::SupersessionStale,
                format!(
                    "component '{}' is current at version '{}'; a new version must name it in `supersedes_id`",
                    record.component_id,
                    head.expect("head is Some")
                ),
            ));
        }
        None => {}
    }
    Ok(Admission::New { head })
}

/// The graph-relative admission of one harness-lesson write (R802.4).
fn lesson_admission(
    view: &HarnessView<'_>,
    node_id: &str,
    props: &Props,
    engine_fills_pointer: bool,
) -> Result<Admission, String> {
    let record = decode_lesson_write(props)
        .map_err(|refusal| refuse_improvement(refusal, "harness lesson record".to_string()))?;
    if record.node_id() != node_id {
        return Err(refuse_improvement(
            ImprovementLibraryRefusal::ContentDigestMismatch,
            format!(
                "node id '{node_id}' does not match the lesson's content address '{}'",
                record.node_id()
            ),
        ));
    }
    record.validate().map_err(|refusal| {
        refuse_improvement(
            refusal,
            format!(
                "task class '{}', deficiency '{}'",
                record.task_class, record.deficiency
            ),
        )
    })?;
    match stored_record(
        view,
        node_id,
        is_harness_lesson_write,
        decode_lesson_write,
        |a, b| a.same_writer_fields(b),
        &record,
        "HarnessRecordImmutable",
    )? {
        Some(true) => return Ok(Admission::Replay),
        Some(false) => unreachable!("stored_record refuses a mutable rewrite"),
        None => {}
    }
    // The lesson's evidence runs are the run nodes the graph (or this same
    // write) holds; a lesson that cites a run the store does not have has no
    // evidence (R802.5).
    for trace in record.evidence_run_ids.iter() {
        if !view.has(&format!("harness-probe-run:{trace}")) {
            return Err(refuse_improvement(
                ImprovementLibraryRefusal::LessonWithoutEvidence,
                format!("lesson '{node_id}' cites evidence run '{trace}' the graph does not hold"),
            ));
        }
    }
    // The supersession pointer names the current lesson of the same task
    // class and deficiency, or nothing when the class and deficiency are new.
    let head = view.lesson_head(&record.task_class, &record.deficiency);
    match record.supersedes_id.as_deref() {
        Some(superseded) => match head.as_deref() {
            Some(current) if current == superseded => {}
            Some(current) => {
                return Err(refuse_improvement(
                    ImprovementLibraryRefusal::SupersessionStale,
                    format!(
                        "task class '{}' deficiency '{}' is current at lesson '{current}', not '{superseded}'",
                        record.task_class, record.deficiency
                    ),
                ))
            }
            None => {
                if !view.has(superseded) {
                    return Err(refuse_improvement(
                        ImprovementLibraryRefusal::SupersessionUnknown,
                        format!(
                            "lesson '{node_id}' supersedes '{superseded}' the graph does not hold"
                        ),
                    ));
                }
            }
        },
        None if !engine_fills_pointer && head.is_some() => {
            return Err(refuse_improvement(
                ImprovementLibraryRefusal::SupersessionStale,
                format!(
                    "task class '{}' deficiency '{}' is current at lesson '{}'; a new lesson must name it in `supersedes_id`",
                    record.task_class,
                    record.deficiency,
                    head.expect("head is Some")
                ),
            ));
        }
        None => {}
    }
    Ok(Admission::New { head })
}

/// The graph-relative admission of one adopted-harness write (R802.4).
fn adoption_admission(
    view: &HarnessView<'_>,
    node_id: &str,
    props: &Props,
) -> Result<Admission, String> {
    let record = decode_adopted_harness_write(props)
        .map_err(|refusal| refuse_improvement(refusal, "adopted harness record".to_string()))?;
    if record.node_id() != node_id {
        return Err(refuse_improvement(
            ImprovementLibraryRefusal::ContentDigestMismatch,
            format!(
                "node id '{node_id}' does not match the adoption's content address '{}'",
                record.node_id()
            ),
        ));
    }
    record.validate().map_err(|refusal| {
        refuse_improvement(
            refusal,
            format!(
                "agent '{}', task class '{}'",
                record.agent_ref, record.task_class
            ),
        )
    })?;
    match stored_record(
        view,
        node_id,
        is_adopted_harness_write,
        decode_adopted_harness_write,
        |a, b| *a == *b,
        &record,
        "HarnessRecordImmutable",
    )? {
        Some(true) => return Ok(Admission::Replay),
        Some(false) => unreachable!("stored_record refuses a mutable rewrite"),
        None => {}
    }
    // An adoption supersedes the previous adoption of the same agent and task
    // class by edge (the caller composes it); the record itself carries no
    // pointer, so there is nothing to verify head-relative here.
    Ok(Admission::New { head: None })
}

/// The graph-relative admission of one harness-contrast write (R802.5).
fn contrast_admission(
    view: &HarnessView<'_>,
    node_id: &str,
    props: &Props,
) -> Result<Admission, String> {
    let record = decode_harness_contrast_write(props)
        .map_err(|refusal| refuse_improvement(refusal, "harness contrast record".to_string()))?;
    if record.node_id() != node_id {
        return Err(refuse_improvement(
            ImprovementLibraryRefusal::ContentDigestMismatch,
            format!(
                "node id '{node_id}' does not match the contrast's content address '{}'",
                record.node_id()
            ),
        ));
    }
    if record.task_ref.trim().is_empty()
        || record.run_a_trace_id.trim().is_empty()
        || record.run_b_trace_id.trim().is_empty()
    {
        return Err(refuse_improvement(
            ImprovementLibraryRefusal::IdentityMissing,
            format!("contrast '{node_id}' names no task or no probe run"),
        ));
    }
    match stored_record(
        view,
        node_id,
        is_harness_contrast_write,
        decode_harness_contrast_write,
        |a, b| *a == *b,
        &record,
        "HarnessRecordImmutable",
    )? {
        Some(true) => return Ok(Admission::Replay),
        Some(false) => unreachable!("stored_record refuses a mutable rewrite"),
        None => {}
    }
    // The contrast's probe runs must be held — by the graph or by this same
    // write — and agree with it: same trace ids, same conditions digest
    // (R802.5).
    let run_a = view.probe_run(&record.run_a_trace_id).ok_or_else(|| {
        refuse_improvement(
            ImprovementLibraryRefusal::ContrastConditionsMismatch,
            format!(
                "contrast '{node_id}' cites probe run '{}' the graph does not hold",
                record.run_a_trace_id
            ),
        )
    })?;
    let run_b = view.probe_run(&record.run_b_trace_id).ok_or_else(|| {
        refuse_improvement(
            ImprovementLibraryRefusal::ContrastConditionsMismatch,
            format!(
                "contrast '{node_id}' cites probe run '{}' the graph does not hold",
                record.run_b_trace_id
            ),
        )
    })?;
    record.validate_against(&run_a, &run_b).map_err(|refusal| {
        refuse_improvement(
            refusal,
            format!("contrast '{node_id}' disagrees with its probe runs"),
        )
    })?;
    Ok(Admission::New { head: None })
}

/// The graph-relative admission of one harness-probe-run write (R802.5).
fn probe_run_admission(
    view: &HarnessView<'_>,
    node_id: &str,
    props: &Props,
) -> Result<Admission, String> {
    let record = decode_harness_probe_run_write(props)
        .map_err(|refusal| refuse_improvement(refusal, "harness probe run record".to_string()))?;
    if record.node_id() != node_id {
        return Err(refuse_improvement(
            ImprovementLibraryRefusal::RecordMalformed,
            format!(
                "node id '{node_id}' does not match probe run '{}' trace identity '{}'",
                record.trace_id,
                record.node_id()
            ),
        ));
    }
    record.validate().map_err(|refusal| {
        refuse_improvement(refusal, format!("probe run '{node_id}' names no trace"))
    })?;
    match stored_record(
        view,
        node_id,
        is_harness_probe_run_write,
        decode_harness_probe_run_write,
        |a, b| *a == *b,
        &record,
        "HarnessRecordImmutable",
    )? {
        Some(true) => return Ok(Admission::Replay),
        Some(false) => unreachable!("stored_record refuses a mutable rewrite"),
        None => {}
    }
    Ok(Admission::New { head: None })
}

/// The graph-relative admission of one AddNode of ANY harness-record kind.
/// `None` when the node is not a harness record (the write passes through).
fn harness_add_node_admission(
    view: &HarnessView<'_>,
    node_id: &str,
    props: &Props,
    engine_fills_pointer: bool,
) -> Result<Option<Admission>, String> {
    if is_harness_component_write(props) {
        return Ok(Some(component_admission(
            view,
            node_id,
            props,
            engine_fills_pointer,
        )?));
    }
    if is_harness_lesson_write(props) {
        return Ok(Some(lesson_admission(
            view,
            node_id,
            props,
            engine_fills_pointer,
        )?));
    }
    if is_adopted_harness_write(props) {
        return Ok(Some(adoption_admission(view, node_id, props)?));
    }
    if is_harness_contrast_write(props) {
        return Ok(Some(contrast_admission(view, node_id, props)?));
    }
    if is_harness_probe_run_write(props) {
        return Ok(Some(probe_run_admission(view, node_id, props)?));
    }
    Ok(None)
}

/// EG-ORCH-VOCAB-R801.3/R802.4 — the harness-record write attaches to the
/// existing AddNode path at the pre-commit point, beside the owner-id stamp.
/// A non-harness `AddNode` passes through untouched; a harness write comes
/// back stamped with the transaction time and (when the writer left the
/// pointer absent) linked to the current record; an idempotent replay comes
/// back as the stored blob, byte-exact.
pub fn direct_add_node(method: &Method, core: &GraphCore, tx_now: u64) -> Result<Method, String> {
    let Method::AddNode {
        node_id,
        properties_msgpack,
    } = method
    else {
        return Ok(method.clone());
    };
    let props = decode_props(properties_msgpack);
    let empty: BTreeMap<String, Props> = BTreeMap::new();
    let view = HarnessView {
        core,
        pending: &empty,
    };
    let Some(admission) = harness_add_node_admission(&view, node_id, &props, true)? else {
        return Ok(method.clone());
    };
    match admission {
        Admission::Replay => {
            // The stored blob (its original stamps) is the write, byte-exact.
            let stored = core
                .get_node_properties(node_id)
                .expect("admission saw the stored node");
            Ok(Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: stored,
            })
        }
        Admission::New { head } => {
            // Stamp the transaction time and, when the writer left the
            // pointer absent, link the new record to the current one. The
            // record already decoded and validated above.
            let component = is_harness_component_write(&props);
            let lesson = is_harness_lesson_write(&props);
            let mut stamped = props;
            stamped.insert("tx_from".into(), serde_json::json!(tx_now));
            if component || lesson {
                if let Some(pointer_target) = &head {
                    // `tx_from` is an engine-owned key the decoders strip, so
                    // the stamped object decodes to the same record.
                    let pointer_absent = if component {
                        decode_component_write(&stamped)
                            .map(|record| record.supersedes_id.is_none())
                            .unwrap_or(false)
                    } else {
                        decode_lesson_write(&stamped)
                            .map(|record| record.supersedes_id.is_none())
                            .unwrap_or(false)
                    };
                    if pointer_absent {
                        stamped.insert("supersedes_id".into(), serde_json::json!(pointer_target));
                    }
                }
            }
            let blob =
                rmp_serde::to_vec_named(&serde_json::Value::Object(stamped)).map_err(|error| {
                    format!("HarnessRecordMalformed: cannot re-encode stamped record: {error}")
                })?;
            Ok(Method::AddNode {
                node_id: node_id.clone(),
                properties_msgpack: blob,
            })
        }
    }
}

/// The pending state of one harness-record write: the harness nodes its ops
/// have admitted so far, in application order, with the field merges of its
/// CASes applied. One state spans an `ApplyChangeEnvelope`, and the
/// envelopes of an `ApplyChangeEnvelopes` batch in order.
#[derive(Default)]
pub struct HarnessBoundaryState {
    pending: BTreeMap<String, Props>,
}

impl HarnessBoundaryState {
    /// The engine-owned write markers, by record kind — the admission
    /// dispatch key.
    fn is_harness_record(props: &Props) -> bool {
        is_harness_component_write(props)
            || is_harness_lesson_write(props)
            || is_adopted_harness_write(props)
            || is_harness_contrast_write(props)
            || is_harness_probe_run_write(props)
    }

    /// EG-ORCH-VOCAB-R801.3/R802.4 — check one envelope's harness-record
    /// writes op by op, in application order, against the graph as the write
    /// sees it. The envelope bytes are byte-stable for replay, so the engine
    /// verifies instead of mutating: each record decodes and validates,
    /// carries its `tx_from` stamp (the generated client stamps it,
    /// EG-DURABLE-KERNEL-R078/R081), and its supersession pointer names the
    /// current record — which may itself be a PENDING write of this same
    /// batch. Non-harness ops pass through.
    pub fn check_envelope(
        &mut self,
        envelope: &ChangeEnvelope,
        core: &GraphCore,
    ) -> Result<(), String> {
        for operation in &envelope.mutation.operations {
            match &operation.method {
                Method::AddNode {
                    node_id,
                    properties_msgpack,
                } => {
                    let props = decode_props(properties_msgpack);
                    if !Self::is_harness_record(&props) {
                        continue;
                    }
                    let view = HarnessView {
                        core,
                        pending: &self.pending,
                    };
                    let Some(admission) =
                        harness_add_node_admission(&view, node_id, &props, false)?
                    else {
                        continue;
                    };
                    if matches!(admission, Admission::New { .. }) && tx_stamp(&props).is_none() {
                        let code = if is_harness_component_write(&props) {
                            "HarnessVersionWithoutStamp"
                        } else {
                            "HarnessRecordWithoutStamp"
                        };
                        return Err(format!(
                            "{code}: harness record '{node_id}' must carry its transaction-time stamp `tx_from`"
                        ));
                    }
                    self.pending.insert(node_id.clone(), props);
                }
                Method::CompareAndSetNodeFields {
                    node_id,
                    updates_msgpack,
                    ..
                } => {
                    // A head-window-close (or any field merge) on a
                    // harness record must be visible to the supersession
                    // checks of the write's later ops.
                    let base = match self.pending.get(node_id) {
                        Some(props) => props.clone(),
                        None => {
                            let Some(stored) = core.get_node_properties(node_id) else {
                                continue;
                            };
                            let stored = decode_props(&stored);
                            if !Self::is_harness_record(&stored) {
                                continue;
                            }
                            stored
                        }
                    };
                    let mut merged = base;
                    for (key, value) in &decode_props(updates_msgpack) {
                        merged.insert(key.clone(), value.clone());
                    }
                    self.pending.insert(node_id.clone(), merged);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
