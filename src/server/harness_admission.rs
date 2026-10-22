//! EG-ORCH-VOCAB-R801 — engine-side admission of the versioned
//! harness-component writes on BOTH write paths.
//!
//! The two paths differ in one thing only: whether the engine may MUTATE the
//! write before commit.
//!
//! * The direct `AddNode` path (the graph-ops gateway) may mutate: the engine
//!   stamps the transaction time (`tx_from`, EG-DURABLE-KERNEL-R078/R081) and
//!   fills the `supersedes_id` pointer when the writer left it absent (the
//!   version names the current one). The mutation happens before the durable
//!   commit and before the apply closure clones the method, so durable and
//!   serving state cannot diverge.
//! * The `ChangeEnvelope` path is byte-stable for replay (the replay check
//!   compares reserialized bytes) and its digest covers the operations, so the
//!   engine only VERIFIES: the generated client stamps `tx_from` itself
//!   (EG-DURABLE-KERNEL-R081) and composes the real supersession edge and the
//!   head-window-close ops, naming the superseded record in `supersedes_id`.
//!   A supersession that leaves the pointer absent while a current version
//!   exists is refused, because such a version would supersede nothing.
//!
//! The shared rules: the record decodes strictly (the closed kind set, the
//! typed evidence and interval checks), the node id is the record's own
//! content address, a published version never mutates (the same writer fields
//! are an idempotent replay; anything else is `HarnessVersionImmutable`), and
//! a supersession names the CURRENT version (`HarnessSupersessionStale` when
//! it names an older one, `HarnessSupersessionUnknown` when it names nothing
//! the graph holds).
//!
//! The envelope check runs under the graph write lock, before any durable
//! work. A batch commits atomically, so one [`HarnessBoundaryState`] spans
//! the batch's envelopes in order: a supersession may name a version written
//! by an EARLIER envelope of the same write (it is pending, not yet in the
//! graph), and a head-window-close CAS in one op is visible to the
//! supersession check of a later op.

use std::collections::BTreeMap;

use crate::graph::GraphCore;
use crate::protocol::Method;
use eg_types::change_envelope::ChangeEnvelope;
use eg_types::orchestration::{
    HarnessComponentRefusal, decode_component_write, is_harness_component_write,
};

type Props = serde_json::Map<String, serde_json::Value>;

fn decode_props(blob: &[u8]) -> Props {
    eg_types::msgpack::decode_property_object(blob).unwrap_or_default()
}

fn refuse_component(refusal: HarnessComponentRefusal, detail: String) -> String {
    format!("{}: {}", refusal.code(), detail)
}

/// The transaction-time stamp a harness record must carry (R801.3): `tx_from`
/// set, and a positive number.
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
/// itself (so "absent with a live head" would publish a version that
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
    decode: fn(&Props) -> Result<T, String>,
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
            decode_component_write(stored).map_err(|refusal| {
                refuse_component(refusal, "harness component version record".to_string())
            })
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
                ));
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
    Ok(None)
}

/// EG-ORCH-VOCAB-R801.3 — the harness-component write attaches to the
/// existing AddNode path at the pre-commit point, beside the owner-id stamp.
/// A non-harness `AddNode` passes through untouched; a harness write comes
/// back stamped with the transaction time and (when the writer left the
/// pointer absent) linked to the current version; an idempotent replay comes
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
            // pointer absent, link the new version to the current one. The
            // record already decoded and validated above.
            let mut stamped = props;
            stamped.insert("tx_from".into(), serde_json::json!(tx_now));
            if let Some(head) = head {
                // `tx_from` is an engine-owned key the decoders strip, so the
                // stamped object decodes to the same record.
                let pointer_absent = decode_component_write(&stamped)
                    .map(|record| record.supersedes_id.is_none())
                    .unwrap_or(false);
                if pointer_absent {
                    stamped.insert("supersedes_id".into(), serde_json::json!(head));
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
    /// The engine-owned write marker of the harness-component record.
    fn is_harness_record(props: &Props) -> bool {
        is_harness_component_write(props)
    }

    /// EG-ORCH-VOCAB-R801.3 — check one envelope's harness-component writes
    /// op by op, in application order, against the graph as the write sees
    /// it. The envelope bytes are byte-stable for replay, so the engine
    /// verifies instead of mutating: each record decodes and validates,
    /// carries its `tx_from` stamp (the generated client stamps it,
    /// EG-DURABLE-KERNEL-R078/R081), and its supersession pointer names the
    /// current version — which may itself be a PENDING write of this same
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
                        return Err(format!(
                            "HarnessVersionWithoutStamp: harness record '{node_id}' must carry its transaction-time stamp `tx_from`"
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
