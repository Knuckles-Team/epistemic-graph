//! Operation audit: a tenant-scoped, idempotent reservation of one governed
//! effect, and the terminal outcome linked to it.
//!
//! A caller reserves BEFORE the effect (`status = "reserved"`) and closes the
//! reservation afterwards with the effect's outcome. Both are entries of the
//! graph's tamper-evident `audit_chain`; `audit_requests` holds one row per
//! phase, keyed by a digest of the request identity, so a retry of either
//! phase returns the original receipt instead of appending twice.
//!
//! A reservation whose outcome never arrived -- the caller or the engine
//! stopped between the two -- stays durable and stays pending. Nothing closes it
//! on the caller's behalf, because only the caller knows what the effect did.
//! Reconciliation is the caller presenting the same request identity again:
//! replaying the reservation reports that no outcome is linked yet
//! ([`AuditAppendReceipt::outcome_seq`] is absent), and appending the outcome
//! then links it to the original reservation exactly once.
//!
//! Every refusal happens before any row is written, and each has a declared
//! code a caller can match on.
//!
//! [`AuditAppendReceipt::outcome_seq`]: crate::protocol::AuditAppendReceipt

use super::audit::{append_audit_entry_with_line, fresh_attempt_id, verify_audit, AuditTailCache};
use super::shard::{Shard, ShardWrite};
use super::{AUDIT, AUDIT_REQUESTS};
use crate::audit::Hash;
use eg_storage::ScopedOwnerTableMut;
use sha2::{Digest, Sha256};

/// Refused: the request declares no audit class.
pub(crate) const AUDIT_CLASS_REQUIRED: &str = "AUDIT_CLASS_REQUIRED";
/// Refused: the request declares an audit class this engine does not define.
pub(crate) const AUDIT_CLASS_UNKNOWN: &str = "AUDIT_CLASS_UNKNOWN";
/// Refused: no durable audit writer answered the append.
pub(crate) const AUDIT_WRITER_UNAVAILABLE: &str = "AUDIT_WRITER_UNAVAILABLE";
/// Refused: an outcome was presented for a request that never reserved.
pub(crate) const AUDIT_RESERVATION_REQUIRED: &str = "AUDIT_RESERVATION_REQUIRED";
/// Refused: an outcome's request context differs from its reservation's.
pub(crate) const AUDIT_RESERVATION_MISMATCH: &str = "AUDIT_RESERVATION_MISMATCH";
/// Refused: the request identity was already used with different content.
pub(crate) const AUDIT_IDEMPOTENCY_CONFLICT: &str = "AUDIT_IDEMPOTENCY_CONFLICT";

/// The declared refusal for an append no durable writer answered, with its
/// cause. The caller is never told the event was appended.
pub(crate) fn audit_writer_unavailable(cause: impl std::fmt::Display) -> String {
    format!("{AUDIT_WRITER_UNAVAILABLE}: {cause}")
}

/// The audit classes an operation may declare.
const AUDIT_CLASSES: [&str; 2] = ["event", "identity_chain"];
/// How a caller spells "this operation declares no audit class".
const NO_AUDIT_CLASS: &str = "none";

const RESERVED: &str = "reserved";
const RESERVE_PHASE: &str = "reserve";
const OUTCOME_PHASE: &str = "outcome";

type RequestIndex<'w> = ScopedOwnerTableMut<'w, (&'static str, &'static str), &'static [u8]>;
type AuditChain<'w> = ScopedOwnerTableMut<'w, (&'static str, u64), &'static [u8]>;

/// Validated, privacy-safe fields for a served operation outcome. Tenant and
/// principal are derived from the verified carrier, not from method params.
#[derive(Clone, Debug)]
pub(crate) struct OperationAuditEvent {
    pub tenant: String,
    pub principal: String,
    pub op: String,
    pub surface: String,
    pub params_sha256: String,
    pub status: String,
    pub request_id: String,
    pub audit_class: String,
}

fn token(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-/".contains(&b))
}

impl OperationAuditEvent {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.validate_class()?;
        if !token(&self.tenant, 128)
            || !token(&self.principal, 256)
            || !token(&self.op, 128)
            || !token(&self.surface, 32)
            || !token(&self.request_id, 128)
            || !matches!(self.status.as_str(), RESERVED | "ok" | "error" | "denied")
            || self.params_sha256.len() != 64
            || !self.params_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err("invalid privacy-safe audit event fields".to_string());
        }
        Ok(())
    }

    /// An absent class and the explicit "no class" spelling are the same
    /// refusal: an effect with no declared audit class is not audited at all.
    fn validate_class(&self) -> Result<(), String> {
        match self.audit_class.as_str() {
            "" | NO_AUDIT_CLASS => Err(AUDIT_CLASS_REQUIRED.to_string()),
            class if AUDIT_CLASSES.contains(&class) => Ok(()),
            _ => Err(AUDIT_CLASS_UNKNOWN.to_string()),
        }
    }

    fn is_reservation(&self) -> bool {
        self.status == RESERVED
    }

    fn phase(&self) -> &'static str {
        if self.is_reservation() {
            RESERVE_PHASE
        } else {
            OUTCOME_PHASE
        }
    }

    /// The idempotency key of one phase of this request.
    fn request_key(&self, phase: &str) -> String {
        hex::encode(Sha256::digest(format!(
            "{}\0{}\0{}\0{}\0{phase}",
            self.tenant, self.principal, self.request_id, self.op
        )))
    }

    /// Everything an outcome must share with its reservation.
    fn context_fingerprint(&self) -> Hash {
        Sha256::digest(format!(
            "{}\0{}\0{}\0{}\0{}\0{}\0{}",
            self.tenant,
            self.principal,
            self.request_id,
            self.op,
            self.surface,
            self.params_sha256,
            self.audit_class,
        ))
        .into()
    }

    /// The chain line. An outcome names the sequence of the reservation it
    /// closes, so the link is part of the tamper-evident entry itself.
    fn line(&self, reservation: Option<u64>) -> String {
        let link = reservation.map_or_else(String::new, |seq| format!("|reservation={seq}"));
        format!(
            "OP_AUDIT|v1|tenant={}|principal={}|op={}|surface={}|params_sha256={}|status={}|request_id={}|class={}{link}",
            self.tenant,
            self.principal,
            self.op,
            self.surface,
            self.params_sha256,
            self.status,
            self.request_id,
            self.audit_class,
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AuditRequestRecord {
    fingerprint: Hash,
    context_fingerprint: Hash,
    seq: u64,
    entry_hash: Hash,
}

/// What one append resolved to, before it is durable.
struct Applied {
    record: AuditRequestRecord,
    replayed: bool,
    reservation_seq: u64,
    outcome_seq: Option<u64>,
}

/// One graph's audit chain and request index inside one admitted write.
struct AppendScope<'a, 'w> {
    graph: &'a str,
    requests: RequestIndex<'w>,
    audit: AuditChain<'w>,
    tail: &'a mut AuditTailCache,
}

impl AppendScope<'_, '_> {
    fn record(&self, key: &str) -> Result<Option<AuditRequestRecord>, String> {
        self.requests
            .get((self.graph, key))?
            .map(|row| rmp_serde::from_slice(row.value()).map_err(|e| e.to_string()))
            .transpose()
    }

    /// Append `line` under `key` unless that key already holds it: an exact
    /// retry returns the stored record, different content under the key fails.
    fn append_once(
        &mut self,
        key: &str,
        event: &OperationAuditEvent,
        line: &str,
    ) -> Result<(AuditRequestRecord, bool), String> {
        let fingerprint: Hash = Sha256::digest(line.as_bytes()).into();
        if let Some(existing) = self.record(key)? {
            if existing.fingerprint != fingerprint {
                return Err(AUDIT_IDEMPOTENCY_CONFLICT.to_string());
            }
            return Ok((existing, true));
        }
        let (seq, entry_hash) =
            append_audit_entry_with_line(&mut self.audit, self.tail, self.graph, line.as_bytes())?;
        let record = AuditRequestRecord {
            fingerprint,
            context_fingerprint: event.context_fingerprint(),
            seq,
            entry_hash,
        };
        let encoded = rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?;
        self.requests
            .insert((self.graph, key), encoded.as_slice())?;
        Ok((record, false))
    }

    /// Reserve, or replay the reservation and report whether an outcome has
    /// been linked to it since.
    fn reserve(&mut self, event: &OperationAuditEvent) -> Result<Applied, String> {
        let outcome_seq = self
            .record(&event.request_key(OUTCOME_PHASE))?
            .map(|outcome| outcome.seq);
        let (record, replayed) =
            self.append_once(&event.request_key(RESERVE_PHASE), event, &event.line(None))?;
        Ok(Applied {
            reservation_seq: record.seq,
            outcome_seq,
            record,
            replayed,
        })
    }

    /// Link the terminal outcome to its reservation, which must exist and
    /// must have been made for the same request context.
    fn close(&mut self, event: &OperationAuditEvent) -> Result<Applied, String> {
        let reservation = self
            .record(&event.request_key(RESERVE_PHASE))?
            .ok_or_else(|| AUDIT_RESERVATION_REQUIRED.to_string())?;
        if reservation.context_fingerprint != event.context_fingerprint() {
            return Err(AUDIT_RESERVATION_MISMATCH.to_string());
        }
        let (record, replayed) = self.append_once(
            &event.request_key(OUTCOME_PHASE),
            event,
            &event.line(Some(reservation.seq)),
        )?;
        Ok(Applied {
            reservation_seq: reservation.seq,
            outcome_seq: Some(record.seq),
            record,
            replayed,
        })
    }
}

fn apply_event(
    write: &ShardWrite<'_>,
    tail: &mut AuditTailCache,
    graph: &str,
    event: &OperationAuditEvent,
) -> Result<Applied, String> {
    let member = write.graph(graph)?;
    let mut scope = AppendScope {
        graph,
        requests: member.open_scoped_table(AUDIT_REQUESTS)?,
        audit: member.open_scoped_table(AUDIT)?,
        tail,
    };
    if event.is_reservation() {
        scope.reserve(event)
    } else {
        scope.close(event)
    }
}

/// Append the event and its replay index in one admitted graph group. A retry
/// with the same request identity and payload returns its original receipt;
/// changed payload under that identity fails closed.
pub(crate) fn operation_audit_append(
    shard: &Shard,
    tail: &mut AuditTailCache,
    graph: &str,
    event: &OperationAuditEvent,
) -> Result<crate::protocol::AuditAppendReceipt, String> {
    event.validate()?;
    let mut staged_tail = tail.clone();
    let members = shard.graph_members(&[graph])?;
    // Unique per ATTEMPT (see `fresh_attempt_id`): `admit_maintenance`
    // resolves a repeated batch id to a REPLAY and skips the write entirely, so
    // a content-derived id would make the SECOND call of a genuine duplicate
    // request never reach the `audit_requests` idempotency check, which is
    // this function's own, more precise idempotency.
    let op_id = fresh_attempt_id(&format!(
        "operation_audit/{}",
        event.request_key(event.phase())
    ));
    let (group, batches) = shard.admit_maintenance(&members, &op_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = apply_event(&write, &mut staged_tail, graph, event);
    let applied = match (applied, write.finish()) {
        (Ok(applied), Ok(())) => applied,
        (Err(error), _) | (_, Err(error)) => {
            shard.mutations().abort_group(group)?;
            return Err(error);
        }
    };
    shard.commit_drain(group, &batches, 0)?;
    *tail = staged_tail;
    Ok(crate::protocol::AuditAppendReceipt {
        graph: graph.to_string(),
        seq: applied.record.seq,
        entry_sha256: hex::encode(applied.record.entry_hash),
        replayed: applied.replayed,
        reservation_seq: applied.reservation_seq,
        outcome_seq: applied.outcome_seq,
    })
}

/// Fetch one operation event and verify the entire graph chain before giving
/// the caller a proof status. This is an operator read, so the O(history)
/// verification is explicit; appends remain O(log history) on a cold tail.
pub(crate) fn operation_audit_read(
    shard: &Shard,
    graph: &str,
    verified_tenant: &str,
    seq: u64,
) -> Result<crate::protocol::AuditEventProof, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let audit = read.scoped_owner_table(AUDIT)?;
    let entry = audit
        .get((graph, seq))?
        .ok_or_else(|| "AUDIT_EVENT_NOT_FOUND".to_string())?;
    let (previous, hash, line) = crate::audit::decode_entry(entry.value())
        .ok_or_else(|| "AUDIT_EVENT_CORRUPT".to_string())?;
    let event_line = std::str::from_utf8(line).map_err(|_| "AUDIT_EVENT_CORRUPT")?;
    if !event_line.starts_with(&format!("OP_AUDIT|v1|tenant={verified_tenant}|")) {
        return Err("AUDIT_EVENT_NOT_FOUND".to_string());
    }
    let proof = crate::protocol::AuditEventProof {
        graph: graph.to_string(),
        seq,
        entry_sha256: hex::encode(hash),
        previous_sha256: hex::encode(previous),
        event_line: event_line.to_string(),
        chain_verified: false,
        chain_entries: 0,
    };
    drop(entry);
    drop(audit);
    drop(read);
    let report = verify_audit(shard, graph)?;
    Ok(crate::protocol::AuditEventProof {
        chain_verified: report.ok,
        chain_entries: report.entries,
        ..proof
    })
}

#[cfg(test)]
#[path = "operation_audit_tests.rs"]
pub(crate) mod tests;
