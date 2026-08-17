//! GOC-21 — distributed `CapacityCell`/`CapacityLease` wire contract and the pure
//! fencing/admission algorithm the engine dispatch layer enforces.
//!
//! Reuses GOC-03's fencing concept (`crates/eg-types/src/commit_descriptor.rs`,
//! branch `goc/goc-03-commit-currency`, unmerged as of this writing — see this
//! module's own doc note below) rather than inventing a second one:
//! `CommitDescriptorV1` pairs a monotonic `authority_epoch` (the authority's own
//! leadership/term counter) with a random non-zero `fencing_token` minted at
//! commit time, and a projection/reader must reject anything carrying a token
//! that does not match the current authority's record. `CapacityLease` mirrors
//! that exact shape for capacity ownership instead of commit ownership:
//! `lease_epoch` is the cell authority's monotonic epoch (bumped on
//! failover/repartition — a stale scheduler on an old epoch cannot acquire),
//! and `fence_token` is a non-zero, monotonically-increasing-per-cell value
//! minted at acquire time that every renew/release/commit call must present
//! unchanged; a caller presenting a stale `lease_epoch` OR a `fence_token` that
//! does not match the ledger's live record for that `lease_id` is rejected
//! (`CapacityDenial::StaleFence` / `StaleEpoch`), never silently upgraded.
//!
//! ## Premise note (verified against `epistemic-graph` `main` at
//! `02594f7`, 2026-08-16)
//!
//! `CommitDescriptorV1`/`authority_epoch`/`fencing_token` exist ONLY on the
//! unmerged branch `goc/goc-03-commit-currency`
//! (`crates/eg-types/src/commit_descriptor.rs`); they are not present on `main`
//! and this module's branch is cut from `main`, so `CapacityLease` cannot
//! literally import `CommitDescriptorV1` without taking an undeclared
//! cross-lane dependency on GOC-03 landing first (GOC-21's declared hard deps
//! are GOC-18/19/20, not GOC-03). This module therefore VENDORS the identical
//! epoch+fence shape and CAS discipline as its own fields
//! (`lease_epoch`/`fence_token`) rather than a type import, and is written so
//! that swapping to a shared `commit_descriptor::Fencing` type (if GOC-03 lands
//! first and a follow-up lane chooses to unify them) is a mechanical rename,
//! not a redesign. Reported as an open dependency-ordering item, not silently
//! worked around.
//!
//! ## Scope
//!
//! Pure data + pure algorithm, no I/O — this crate is the bottom of the engine
//! DAG (see `lib.rs`). The server-side dispatch/mutation/state-machine wiring
//! (durable persistence of `CapacityCell`/`CapacityLease` rows, the actual
//! fence-token minting counter, RPC handlers) is GOC-21-W04/W05 scope in
//! `eg-server`/`eg-core`, deliberately NOT implemented here; what IS here is
//! the admission/fairness/expiry algorithm those handlers must call so there is
//! exactly one implementation to audit, plus the unit tests that are this
//! lane's "known-bad proof": an expired or wrong-fenced holder is rejected, and
//! a background flood cannot push a reserved-floor request's wait past this
//! module's declared bound (see `try_acquire`'s doc for the exact bound).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Current wire/on-disk schema version for this module's types.
pub const CAPACITY_LEASE_VERSION: u16 = 1;

fn validate_non_empty(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

/// The resource a `CapacityCell` doles out. Mirrors the lane doc's "CPU/LLM/GPU
/// capacity" plus the existing AU gates this composes with
/// (`resource_priority.py`'s shared-LLM gate, `worker_scheduler.py`'s host
/// worker pool, `gpu_group_budget.py`'s per-GPU/model floors).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityResourceClass {
    /// Shared LLM generator admission slots (composes with `resource_priority.py`).
    LlmGenerator,
    /// A separate embedding endpoint's admission slots — never contends with
    /// `LlmGenerator` (mirrors `resource_priority.py`'s separate embedding gate key).
    LlmEmbedding,
    /// GPU compute units (composes with `gpu_group_budget.py`).
    Gpu,
    /// Host worker pool slots (composes with `worker_scheduler.py`).
    Worker,
    /// CPU-bound compute slots.
    Cpu,
    /// Message-broker/engine dispatch admission slots.
    Broker,
}

/// The same four priority tiers `resource_priority.py::PriorityClass` defines,
/// vendored here (not imported — this crate has no Python interop and must not
/// depend on AU) so a `CapacityLease` carries the identical vocabulary the AU
/// scheduler already enforces at the LLM gate. `#[derive(Ord)]` on a fieldless
/// enum compares by DECLARATION ORDER, so listing `Interactive` first and
/// `BackgroundIngestion` last gives `Interactive < Orchestration < Hydration <
/// BackgroundIngestion`, matching `core/cognitive_scheduler.py::SchedulerPriority`'s
/// "lower value = higher priority" convention. `Orchestration`/`Hydration` are
/// NOT equal-priority under this `Ord` (AU's equal LLM-gate WEIGHT for the two
/// is a separate lookup table, `_PRIORITY_WEIGHTS`, not an enum-ordinal fact —
/// see `resource_priority.py`); only `may_spend_reserved_floor` below is a
/// hard cell-admission rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeasePriority {
    Interactive,
    Orchestration,
    Hydration,
    BackgroundIngestion,
}

impl LeasePriority {
    /// True only for `BackgroundIngestion` — the one class a `CapacityCell`'s
    /// `reserved_floor` is never available to (mirrors
    /// `PriorityClass.is_interactive_floor`'s "the one class that yields").
    pub fn may_spend_reserved_floor(self) -> bool {
        !matches!(self, LeasePriority::BackgroundIngestion)
    }
}

/// A durable capacity pool. `capacity` is the cell's total admittable amount at
/// its current `epoch`; `reserved_floor` is the slice never available to
/// `BackgroundIngestion` (invariant: "Interactive and orchestration floors are
/// reserved; background uses spare capacity but cannot starve a tenant
/// indefinitely").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityCell {
    pub cell_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub resource_class: CapacityResourceClass,
    pub capacity: u64,
    pub reserved_floor: u64,
    /// Monotonic authority epoch (GOC-03 fencing concept, vendored — see module
    /// doc). Bumped on cell-authority failover/repartition; an acquire/renew
    /// carrying an epoch below this is a stale-authority rejection, never a
    /// best-effort read of a foreign shape.
    pub epoch: u64,
    /// sha256 digest of the policy/catalog/model snapshot this cell was
    /// provisioned under (checked at every hop per the lane doc's "Security and
    /// privacy" section); opaque here, verified upstream.
    pub policy_digest: String,
    pub updated_at_ms: u64,
}

impl CapacityCell {
    pub fn validate(&self) -> Result<(), String> {
        validate_non_empty("cell_id", &self.cell_id)?;
        validate_non_empty("policy_digest", &self.policy_digest)?;
        if self.reserved_floor > self.capacity {
            return Err("reserved_floor must not exceed capacity".to_string());
        }
        if self.epoch == 0 {
            return Err("epoch must be non-zero (0 is reserved for 'never provisioned')".to_string());
        }
        Ok(())
    }

    /// Capacity available to `priority` right now, given `already_leased`
    /// active-lease amount at this cell. `BackgroundIngestion` can only spend
    /// the spare (non-floor) capacity; every other priority may also draw on
    /// the reserved floor.
    pub fn available_for(&self, priority: LeasePriority, already_leased: u64) -> u64 {
        let ceiling = if priority.may_spend_reserved_floor() {
            self.capacity
        } else {
            self.capacity.saturating_sub(self.reserved_floor)
        };
        ceiling.saturating_sub(already_leased.min(ceiling))
    }
}

/// Lifecycle state of one `CapacityLease` (mirrors `work_item.py`'s closed
/// state vocabulary convention — a plain closed string-like enum, not an
/// open-ended status field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Active,
    Renewed,
    Released,
    /// Expiry reached with no renewal; capacity is reclaimable. A holder that
    /// keeps working past this point is the classic fencing defect this module
    /// exists to prevent — `try_renew`/`try_release` reject a caller presenting
    /// a lease in (or past) this state.
    Expired,
    /// An `ReclaimExpired` sweep already returned this lease's amount to the
    /// cell. Terminal, like `Released`.
    Reclaimed,
}

impl LeaseState {
    fn is_terminal(self) -> bool {
        matches!(self, LeaseState::Released | LeaseState::Reclaimed)
    }
}

/// One tenant's durable claim on a `CapacityCell`. Field set matches the lane
/// doc's frozen schema.
// `Eq` is restored here because `cost_budget_micros` is now an exact integer —
// see its field doc. `Hash` is deliberately NOT derived: nothing keys a map or
// set by a whole lease, and adding it would force `Hash` onto
// `CapacityResourceClass` and `LeasePriority` purely to satisfy a capability no
// caller has asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityLease {
    pub schema_version: u16,
    pub lease_id: String,
    pub work_item_id: String,
    pub tenant_ref: String,
    /// Opaque authenticated actor identity (GOC-15 verified carrier digest).
    pub actor_digest: String,
    pub cell_id: String,
    pub resource_class: CapacityResourceClass,
    pub amount: u64,
    pub priority: LeasePriority,
    /// Non-zero, unguessable-in-practice token minted at acquire time (GOC-03
    /// fencing concept, vendored). Renew/release must present the exact value
    /// last minted for this `lease_id`; a mismatch is `StaleFence`.
    pub fence_token: u64,
    /// The `CapacityCell::epoch` this lease was acquired under. A renew/release
    /// presenting a lease whose `lease_epoch` is behind the cell's CURRENT
    /// epoch is `StaleEpoch` — the cell was repartitioned/failed-over and this
    /// holder's authority no longer applies, independent of whether its
    /// `expires_at_ms` has passed yet.
    pub lease_epoch: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub renewed_count: u32,
    /// Spend ceiling in **micro-units (1e-6) of the deployment's accounting
    /// currency** — exact integer, never a float.
    ///
    /// This was `Option<f64>` and that was wrong three ways, not just
    /// inconvenient for the `Eq` derive:
    ///
    /// 1. **`NaN`/`±Inf`/negative were representable and unvalidated.**
    ///    `validate()` checks neither budget field, so a `NaN` ceiling passed
    ///    silently. A budget is a *control*; a control that admits a value
    ///    which compares false against everything fails open.
    /// 2. **Float accumulation is inexact and non-associative.** A distributed
    ///    cost ledger summing leases must reach the same total on every node
    ///    regardless of summation order. `f64` does not guarantee that.
    /// 3. **`NaN` is not JSON-representable.** It serializes to a bare `NaN`
    ///    token that strict parsers reject, so a non-finite ceiling is an
    ///    interop hazard at the AU boundary, not merely an odd number.
    ///
    /// An unsigned integer makes all three unrepresentable rather than
    /// validated-against, and matches `token_budget` — the sibling ceiling in
    /// this same struct, which was already exact. `None` means unbounded;
    /// `Some(0)` means "may spend nothing", a genuinely different state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_budget_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<u64>,
    pub idempotency_key: String,
    pub state: LeaseState,
}

impl CapacityLease {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CAPACITY_LEASE_VERSION {
            return Err(format!(
                "unsupported capacity lease version {} (expected {})",
                self.schema_version, CAPACITY_LEASE_VERSION
            ));
        }
        for (field, value) in [
            ("lease_id", &self.lease_id),
            ("work_item_id", &self.work_item_id),
            ("tenant_ref", &self.tenant_ref),
            ("actor_digest", &self.actor_digest),
            ("cell_id", &self.cell_id),
            ("idempotency_key", &self.idempotency_key),
        ] {
            validate_non_empty(field, value)?;
        }
        if self.amount == 0 {
            return Err("amount must be non-zero".to_string());
        }
        if self.fence_token == 0 {
            return Err("fence_token must be non-zero".to_string());
        }
        if self.lease_epoch == 0 {
            return Err("lease_epoch must be non-zero".to_string());
        }
        if self.expires_at_ms <= self.issued_at_ms {
            return Err("expires_at_ms must be after issued_at_ms".to_string());
        }
        Ok(())
    }

    /// True once `now_ms` has reached `expires_at_ms` OR the lease is already
    /// terminal-by-fiat (`Released`/`Reclaimed`/`Expired`). A caller must treat
    /// an expired lease as fenced even if it never sees a `Reclaimed`
    /// transition (the reclaim sweep is a cleanup, not the authority — the
    /// authority is the timestamp comparison itself, so two independent
    /// reclaimers can never race on "is it expired").
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.state.is_terminal() || self.state == LeaseState::Expired || now_ms >= self.expires_at_ms
    }
}

/// Why an acquire/renew/release was refused. Every variant is a case the lane
/// doc's acceptance gates require to be an explicit rejection, never a
/// fail-open admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum CapacityDenial {
    /// Not enough spare/floor capacity for this priority right now.
    Exhausted { available: u64, requested: u64 },
    /// The caller's `lease_epoch` is behind the cell's current epoch — the cell
    /// was repartitioned/failed over. THE fencing rejection this lane's
    /// "known-bad proof" targets.
    StaleEpoch { cell_epoch: u64, caller_epoch: u64 },
    /// The caller's `fence_token` does not match the ledger's live record for
    /// this `lease_id`. The other half of the fencing rejection.
    StaleFence,
    /// The lease has reached `expires_at_ms` (or is already terminal); a
    /// renew/release/commit from an expired holder is rejected outright —
    /// nothing "keeps working" past expiry.
    LeaseExpired,
    /// `lease_id` unknown to the ledger.
    NotFound,
    /// Replaying `(tenant_ref, idempotency_key)` against a DIFFERENT demand
    /// shape than the original (mirrors `CommitDescriptorV1`'s idempotency
    /// invariant): same key must mean same request, never a shape mismatch
    /// silently accepted.
    IdempotencyConflict,
}

/// Bounded, in-memory reference ledger for ONE `CapacityCell`. This is the pure
/// algorithm a durable server-side store wraps (GOC-21-W04); it holds no I/O
/// and is deliberately small enough to be exhaustively unit-tested here as this
/// lane's fencing/fairness proof. A production store additionally persists
/// every state transition durably before acking (mirrors `work_item.py`'s
/// "commit-before-ack" discipline) — that persistence is out of this crate's
/// scope (bottom of the DAG, no I/O).
#[derive(Debug, Default)]
pub struct CapacityLedger {
    leases: BTreeMap<String, CapacityLease>,
    /// Monotonic per-cell fence counter; the ONLY minter of `fence_token`, so
    /// two concurrent acquires against the same ledger can never mint the same
    /// token (the in-process analogue of the durable store's atomic counter).
    next_fence: u64,
    /// `(tenant_ref, idempotency_key) -> lease_id` replay table.
    idempotency: BTreeMap<(String, String), String>,
}

impl CapacityLedger {
    pub fn new() -> Self {
        Self {
            leases: BTreeMap::new(),
            next_fence: 1,
            idempotency: BTreeMap::new(),
        }
    }

    fn active_leased_amount(&self, cell: &CapacityCell, now_ms: u64) -> u64 {
        self.leases
            .values()
            .filter(|l| l.cell_id == cell.cell_id && !l.is_expired(now_ms))
            .map(|l| l.amount)
            .sum()
    }

    /// Atomic CAS acquire: admits iff `cell.available_for(priority, already_leased)
    /// >= amount`. Mints a fresh, non-zero, ledger-unique `fence_token` and
    /// stamps `lease_epoch = cell.epoch`. Idempotent: replaying the same
    /// `(tenant_ref, idempotency_key)` returns the SAME lease (never mints a
    /// second one) as long as the demand shape matches; a shape mismatch is
    /// `IdempotencyConflict`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_acquire(
        &mut self,
        cell: &CapacityCell,
        lease_id: String,
        work_item_id: &str,
        tenant_ref: &str,
        actor_digest: &str,
        resource_class: CapacityResourceClass,
        amount: u64,
        priority: LeasePriority,
        idempotency_key: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<CapacityLease, CapacityDenial> {
        let replay_key = (tenant_ref.to_string(), idempotency_key.to_string());
        if let Some(existing_id) = self.idempotency.get(&replay_key) {
            let existing = self.leases.get(existing_id).expect("idempotency index points at a live lease");
            if existing.cell_id == cell.cell_id
                && existing.resource_class == resource_class
                && existing.amount == amount
                && existing.work_item_id == work_item_id
            {
                return Ok(existing.clone());
            }
            return Err(CapacityDenial::IdempotencyConflict);
        }

        let already = self.active_leased_amount(cell, now_ms);
        let available = cell.available_for(priority, already);
        if available < amount {
            return Err(CapacityDenial::Exhausted {
                available,
                requested: amount,
            });
        }

        let fence_token = self.next_fence;
        self.next_fence += 1;
        let lease = CapacityLease {
            schema_version: CAPACITY_LEASE_VERSION,
            lease_id: lease_id.clone(),
            work_item_id: work_item_id.to_string(),
            tenant_ref: tenant_ref.to_string(),
            actor_digest: actor_digest.to_string(),
            cell_id: cell.cell_id.clone(),
            resource_class,
            amount,
            priority,
            fence_token,
            lease_epoch: cell.epoch,
            issued_at_ms: now_ms,
            expires_at_ms: now_ms + ttl_ms,
            renewed_count: 0,
            cost_budget_micros: None,
            token_budget: None,
            idempotency_key: idempotency_key.to_string(),
            state: LeaseState::Active,
        };
        self.leases.insert(lease_id.clone(), lease.clone());
        self.idempotency.insert(replay_key, lease_id);
        Ok(lease)
    }

    /// Renew: rejects a caller whose `fence_token`/`lease_epoch` do not match
    /// the ledger's live record, OR whose lease has already expired — an
    /// expired holder that "keeps working" is exactly the defect this rejects.
    pub fn try_renew(
        &mut self,
        lease_id: &str,
        fence_token: u64,
        lease_epoch: u64,
        cell_epoch: u64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<CapacityLease, CapacityDenial> {
        let lease = self.leases.get_mut(lease_id).ok_or(CapacityDenial::NotFound)?;
        if lease.is_expired(now_ms) {
            lease.state = LeaseState::Expired;
            return Err(CapacityDenial::LeaseExpired);
        }
        if lease_epoch < cell_epoch {
            return Err(CapacityDenial::StaleEpoch {
                cell_epoch,
                caller_epoch: lease_epoch,
            });
        }
        if lease.fence_token != fence_token {
            return Err(CapacityDenial::StaleFence);
        }
        lease.expires_at_ms = now_ms + ttl_ms;
        lease.renewed_count += 1;
        lease.state = LeaseState::Renewed;
        Ok(lease.clone())
    }

    /// Release: same fencing checks as renew, then transitions terminal and
    /// frees capacity for the next admission.
    pub fn try_release(
        &mut self,
        lease_id: &str,
        fence_token: u64,
        lease_epoch: u64,
        cell_epoch: u64,
        now_ms: u64,
    ) -> Result<(), CapacityDenial> {
        let lease = self.leases.get_mut(lease_id).ok_or(CapacityDenial::NotFound)?;
        if lease.state.is_terminal() {
            // Idempotent terminal commit (mirrors CommitDescriptorV1's
            // idempotency invariant): a repeated release on an already-terminal
            // lease resolves to a no-op success, never an error.
            return Ok(());
        }
        if lease.is_expired(now_ms) {
            lease.state = LeaseState::Expired;
            return Err(CapacityDenial::LeaseExpired);
        }
        if lease_epoch < cell_epoch {
            return Err(CapacityDenial::StaleEpoch {
                cell_epoch,
                caller_epoch: lease_epoch,
            });
        }
        if lease.fence_token != fence_token {
            return Err(CapacityDenial::StaleFence);
        }
        lease.state = LeaseState::Released;
        Ok(())
    }

    /// Sweep every non-terminal lease past `expires_at_ms` into `Reclaimed`.
    /// Returns the reclaimed lease ids. Safe to call from multiple reclaimers
    /// concurrently (in the durable store, this is the CAS-guarded sweep); here
    /// it is simply idempotent per lease.
    pub fn reclaim_expired(&mut self, now_ms: u64) -> Vec<String> {
        let mut reclaimed = Vec::new();
        for (id, lease) in self.leases.iter_mut() {
            if !lease.state.is_terminal() && now_ms >= lease.expires_at_ms {
                lease.state = LeaseState::Reclaimed;
                reclaimed.push(id.clone());
            }
        }
        reclaimed
    }

    pub fn get(&self, lease_id: &str) -> Option<&CapacityLease> {
        self.leases.get(lease_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(epoch: u64) -> CapacityCell {
        CapacityCell {
            cell_id: "cell-llm-1".to_string(),
            parent_id: None,
            resource_class: CapacityResourceClass::LlmGenerator,
            capacity: 10,
            reserved_floor: 2,
            epoch,
            policy_digest: "d".repeat(64),
            updated_at_ms: 0,
        }
    }

    /// KNOWN-BAD PROOF 1: an expired lease holder is rejected on renew, not
    /// silently allowed to keep working.
    #[test]
    fn expired_holder_is_rejected_on_renew() {
        let mut ledger = CapacityLedger::new();
        let c = cell(1);
        let lease = ledger
            .try_acquire(
                &c, "lease-1".into(), "wi-1", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-1", 0, 1_000,
            )
            .expect("acquire admits under capacity");

        // Time passes well beyond the 1000ms TTL — the holder never renewed.
        let now_ms = 5_000;
        let err = ledger
            .try_renew(&lease.lease_id, lease.fence_token, lease.lease_epoch, c.epoch, now_ms, 1_000)
            .expect_err("an expired lease must be rejected, never silently renewed");
        assert_eq!(err, CapacityDenial::LeaseExpired);

        // And release, likewise, does not let a stale-but-terminal-by-time
        // holder pretend it still owned the capacity.
        let err2 = ledger
            .try_release(&lease.lease_id, lease.fence_token, lease.lease_epoch, c.epoch, now_ms)
            .expect_err("an expired lease must also be rejected on release");
        assert_eq!(err2, CapacityDenial::LeaseExpired);
    }

    /// KNOWN-BAD PROOF 2: a lease minted under an OLD cell epoch (the cell was
    /// repartitioned/failed over) is rejected even though its timestamp has
    /// not yet expired — the classic "stale worker keeps working" defect.
    #[test]
    fn stale_epoch_holder_is_rejected_even_if_unexpired() {
        let mut ledger = CapacityLedger::new();
        let c_old = cell(1);
        let lease = ledger
            .try_acquire(
                &c_old, "lease-2".into(), "wi-2", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-2", 0, 1_000_000, // huge TTL: NOT a timestamp-expiry story
            )
            .expect("acquire admits under capacity");

        // Cell fails over: epoch bumps from 1 -> 2.
        let cell_epoch_now = 2;
        let err = ledger
            .try_renew(&lease.lease_id, lease.fence_token, lease.lease_epoch, cell_epoch_now, 500, 1_000)
            .expect_err("a stale-epoch holder must be rejected regardless of its timestamp TTL");
        assert_eq!(
            err,
            CapacityDenial::StaleEpoch {
                cell_epoch: 2,
                caller_epoch: 1,
            }
        );
    }

    /// KNOWN-BAD PROOF 2b: a forged/guessed fence token on an otherwise
    /// current-epoch, unexpired lease is rejected.
    #[test]
    fn wrong_fence_token_is_rejected() {
        let mut ledger = CapacityLedger::new();
        let c = cell(1);
        let lease = ledger
            .try_acquire(
                &c, "lease-3".into(), "wi-3", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-3", 0, 10_000,
            )
            .expect("acquire admits under capacity");

        let forged = lease.fence_token + 1;
        let err = ledger
            .try_renew(&lease.lease_id, forged, lease.lease_epoch, c.epoch, 100, 1_000)
            .expect_err("a mismatched fence token must be rejected");
        assert_eq!(err, CapacityDenial::StaleFence);
    }

    /// KNOWN-BAD PROOF 3: reclaim_expired frees capacity so a NEW acquire can
    /// succeed after the old holder's TTL lapses without ever calling release —
    /// proving the ledger doesn't require a cooperating holder to recover.
    #[test]
    fn reclaim_frees_capacity_without_cooperating_holder() {
        let mut ledger = CapacityLedger::new();
        let c = cell(1);
        // Fill the cell to capacity (10) with one greedy, never-releasing lease.
        ledger
            .try_acquire(
                &c, "hog".into(), "wi-hog", "tenant-hog", "actor-hog",
                CapacityResourceClass::LlmGenerator, 10, LeasePriority::BackgroundIngestion,
                "idem-hog", 0, 1_000,
            )
            .expect("fills the cell");

        // A second acquire right away is exhausted — the hog never released.
        let denied = ledger.try_acquire(
            &c, "second".into(), "wi-2", "tenant-b", "actor-b",
            CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
            "idem-2", 500, 1_000,
        );
        assert!(matches!(denied, Err(CapacityDenial::Exhausted { .. })));

        // Past TTL, reclaim_expired sweeps the hog even though it never called
        // release, and the freed capacity is immediately usable.
        let reclaimed = ledger.reclaim_expired(2_000);
        assert_eq!(reclaimed, vec!["hog".to_string()]);
        let admitted = ledger
            .try_acquire(
                &c, "third".into(), "wi-3", "tenant-b", "actor-b",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-3", 2_100, 1_000,
            )
            .expect("capacity was reclaimed, so this now admits");
        assert_eq!(admitted.amount, 1);
    }

    /// A BackgroundIngestion flood can fill every unit of SPARE capacity but can
    /// never touch the reserved_floor (2 of 10 here) — Interactive can always
    /// acquire up to the floor regardless of flood size. This is the
    /// cell-level half of the "flood cannot starve interactive" proof; the
    /// tenant-vs-tenant DRR half lives in the AU scheduler
    /// (`agent_utilities/orchestration/capacity_leases.py`).
    #[test]
    fn background_flood_cannot_touch_reserved_floor() {
        let mut ledger = CapacityLedger::new();
        let c = cell(1);
        // Flood with BackgroundIngestion up to the spare ceiling (10 - 2 = 8).
        for i in 0..8 {
            ledger
                .try_acquire(
                    &c, format!("flood-{i}"), "wi-flood", "tenant-flood", "actor-flood",
                    CapacityResourceClass::LlmGenerator, 1, LeasePriority::BackgroundIngestion,
                    format!("idem-flood-{i}").as_str(), 0, 1_000_000,
                )
                .unwrap_or_else(|e| panic!("flood unit {i} should admit into spare capacity: {e:?}"));
        }
        // A 9th BackgroundIngestion unit is denied — spare is exhausted.
        let ninth = ledger.try_acquire(
            &c, "flood-8".into(), "wi-flood", "tenant-flood", "actor-flood",
            CapacityResourceClass::LlmGenerator, 1, LeasePriority::BackgroundIngestion,
            "idem-flood-8", 0, 1_000_000,
        );
        assert!(matches!(ninth, Err(CapacityDenial::Exhausted { .. })));

        // Yet Interactive can still acquire from the untouched 2-unit floor.
        let interactive = ledger
            .try_acquire(
                &c, "interactive-1".into(), "wi-int", "tenant-int", "actor-int",
                CapacityResourceClass::LlmGenerator, 2, LeasePriority::Interactive,
                "idem-int-1", 0, 1_000,
            )
            .expect("interactive must be able to draw the full reserved floor despite the flood");
        assert_eq!(interactive.amount, 2);
    }

    #[test]
    fn idempotent_replay_returns_same_lease_not_a_second_one() {
        let mut ledger = CapacityLedger::new();
        let c = cell(1);
        let first = ledger
            .try_acquire(
                &c, "lease-idem".into(), "wi-1", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-shared", 0, 1_000,
            )
            .unwrap();
        let second = ledger
            .try_acquire(
                &c, "lease-idem-DIFFERENT-id".into(), "wi-1", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-shared", 1, 1_000,
            )
            .unwrap();
        assert_eq!(first.fence_token, second.fence_token);
        assert_eq!(first.lease_id, second.lease_id);
    }

    #[test]
    fn idempotency_conflict_on_shape_mismatch() {
        let mut ledger = CapacityLedger::new();
        let c = cell(1);
        ledger
            .try_acquire(
                &c, "lease-a".into(), "wi-1", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-shared", 0, 1_000,
            )
            .unwrap();
        let conflict = ledger.try_acquire(
            &c, "lease-b".into(), "wi-1", "tenant-a", "actor-a",
            CapacityResourceClass::LlmGenerator, 999, LeasePriority::Interactive,
            "idem-shared", 1, 1_000,
        );
        assert!(matches!(conflict, Err(CapacityDenial::IdempotencyConflict)));
    }

    #[test]
    fn cell_validate_rejects_floor_exceeding_capacity() {
        let mut c = cell(1);
        c.reserved_floor = c.capacity + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn lease_validate_rejects_zero_fence_token() {
        let mut c = cell(1);
        c.reserved_floor = 0;
        let mut ledger = CapacityLedger::new();
        let mut lease = ledger
            .try_acquire(
                &c, "lease-v".into(), "wi-1", "tenant-a", "actor-a",
                CapacityResourceClass::LlmGenerator, 1, LeasePriority::Interactive,
                "idem-v", 0, 1_000,
            )
            .unwrap();
        lease.fence_token = 0;
        assert!(lease.validate().is_err());
    }
}
