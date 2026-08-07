//! The planner: a pure, deterministic rule engine over [`Estimate`] +
//! `&[BackendDescriptor]`. No I/O, no global state, no randomness — the same
//! `(estimate, available, opts)` triple always yields the same
//! [`PlannerDecision`], which is what makes it unit-testable against stub backends
//! (`tests/planner_acceptance.rs`) without any real simulator.
//!
//! Rule order (first match wins), per PROGRAM.md:
//!
//! - **R5 (checked first, honoured always):** an explicit `backend_id_override` is
//!   honoured unconditionally if the named backend is registered, WITH an audit
//!   entry recording what R0-R4 would otherwise have selected (and a warning if the
//!   override conflicts with a hard constraint). It is evaluated first, not last,
//!   because "always honoured" is meaningless if an earlier rule could preempt it —
//!   see the module-level rationale in `select_backend`.
//! - **R0 hard constraints:** hardware requested → restrict to `Hardware` family;
//!   density matrix required → restrict to backends that
//!   `capabilities.supports_density_matrix`; anything whose footprint exceeds the
//!   caller's memory bound (already reflected in `Estimate::forbidden`, plus a
//!   per-descriptor capability-ceiling check here) is eliminated outright — never
//!   silently substituted.
//! - **R1-R4:** `Estimate::preferred` is already priority-ordered by `estimate()`
//!   (Clifford fast-path first, then noise-driven family, then placement); the
//!   planner walks it and picks the first family with a fitting, non-forbidden,
//!   registered descriptor.
//!
//! **R3 honesty note (updated — register `D-QN-4`):** `estimate()` now ranks
//! `MatrixProductState` AHEAD of statevector in `Estimate::preferred` when its
//! structural `EntanglingConnectivity` classification is `Product` or
//! `NearestNeighborChain`, and omits `MatrixProductState` from `preferred` entirely
//! when it is `Dense` ("reject if bond dim explodes" per PROGRAM.md's R3). This is
//! still NOT a real Schmidt-rank / bond-dimension computation — it is a topology-only
//! proxy over which qubits each multi-qubit gate connects, with no visibility into
//! circuit depth or gate repetition. See `estimate.rs`'s `EntanglingConnectivity` doc
//! for the exact algorithm and, importantly, what it is conservative about (biased
//! toward `Dense`, i.e. toward NOT preferring MPS, in cases of doubt about gate kind
//! or arity) versus its one known blind spot (a very deep nearest-neighbor-only
//! circuit can still need a large bond dimension in practice; this classification has
//! no way to detect that and will still report `NearestNeighborChain`). Proving that
//! case honestly would require simulating or a real entanglement-entropy estimate —
//! out of scope for a `QuantumProgram`-only structural pass, same as before.

use crate::backend::{BackendDescriptor, BackendFamily, BackendId};
use crate::estimate::Estimate;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PlannerOptions {
    /// R0: caller explicitly requires real/cloud hardware.
    pub want_hardware: bool,
    /// R5: explicit backend override, always honoured if the id is registered.
    pub backend_id_override: Option<BackendId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerRule {
    R0HardConstraint,
    R1CliffordStabilizer,
    R2Noise,
    R3Structure,
    R4Placement,
    R5Override,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditEntry {
    pub rule: PlannerRule,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannerDecision {
    pub chosen: BackendId,
    pub family: BackendFamily,
    pub rule: PlannerRule,
    /// Every rule considered, in evaluation order, with a human-readable note —
    /// including, for an R5 override, what the rule engine would otherwise have
    /// picked and any conflict warnings. This is what Q9 observability persists.
    pub audit: Vec<AuditEntry>,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PlannerError {
    #[error("explicit backend override '{0}' is not among the registered/available backends")]
    UnknownOverrideBackend(BackendId),
    #[error("no registered backend satisfies the request: {reason}")]
    NoBackendAvailable { reason: String },
}

/// Whether a specific descriptor can actually take this circuit, beyond the
/// family-level `Estimate::forbidden` bucket: checks the descriptor's OWN qubit
/// ceilings (a specific backend instance may cap lower than the raw byte-memory
/// bound already baked into `forbidden`).
fn descriptor_fits(estimate: &Estimate, d: &BackendDescriptor) -> bool {
    if estimate.forbidden.contains(&d.family) {
        return false;
    }
    if d.family.is_statevector_like() {
        if let Some(max) = d.capabilities.max_qubits_statevector {
            if estimate.n_qubits > max {
                return false;
            }
        }
    }
    if d.family.is_density_matrix_like() {
        if let Some(max) = d.capabilities.max_qubits_density_matrix {
            if estimate.n_qubits > max {
                return false;
            }
        }
        if !d.capabilities.supports_density_matrix {
            return false;
        }
    }
    true
}

pub fn select_backend(
    estimate: &Estimate,
    available: &[BackendDescriptor],
    opts: &PlannerOptions,
) -> Result<PlannerDecision, PlannerError> {
    let mut audit = Vec::new();

    // What R0-R4 would pick, computed unconditionally (cheap — pure logic, no I/O)
    // so an R5 override always has a comparison point for its audit trail, and so
    // the non-override path below can just reuse it.
    let ruled_pick = rule_engine_pick(estimate, available, opts, &mut audit);

    if let Some(override_id) = &opts.backend_id_override {
        let found = available.iter().find(|d| &d.id == override_id);
        match found {
            Some(d) => {
                let mut override_audit = audit.clone();
                match &ruled_pick {
                    Ok((picked_id, _, _)) if picked_id == override_id => {
                        override_audit.push(AuditEntry {
                            rule: PlannerRule::R5Override,
                            note: format!("explicit override '{override_id}' matches what R0-R4 would have selected"),
                        });
                    }
                    Ok((picked_id, _, _)) => {
                        override_audit.push(AuditEntry {
                            rule: PlannerRule::R5Override,
                            note: format!(
                                "explicit override '{override_id}' HONOURED, overriding R0-R4's pick of '{picked_id}'"
                            ),
                        });
                    }
                    Err(e) => {
                        override_audit.push(AuditEntry {
                            rule: PlannerRule::R5Override,
                            note: format!(
                                "explicit override '{override_id}' HONOURED; R0-R4 would have found NO candidate ({e}) — WARNING: overriding a request that R0-R4 could not satisfy on its own"
                            ),
                        });
                    }
                }
                return Ok(PlannerDecision {
                    chosen: d.id.clone(),
                    family: d.family,
                    rule: PlannerRule::R5Override,
                    audit: override_audit,
                });
            }
            None => return Err(PlannerError::UnknownOverrideBackend(override_id.clone())),
        }
    }

    let (chosen, family, rule) = ruled_pick?;
    Ok(PlannerDecision {
        chosen,
        family,
        rule,
        audit,
    })
}

/// R0-R4, no override handling. Returns `(BackendId, BackendFamily, PlannerRule)` of
/// the rule that matched, or a `PlannerError` if nothing fits.
fn rule_engine_pick(
    estimate: &Estimate,
    available: &[BackendDescriptor],
    opts: &PlannerOptions,
    audit: &mut Vec<AuditEntry>,
) -> Result<(BackendId, BackendFamily, PlannerRule), PlannerError> {
    // R0: hard constraints narrow the candidate pool before anything else runs.
    let candidates: Vec<&BackendDescriptor> = available
        .iter()
        .filter(|d| {
            if opts.want_hardware {
                d.family == BackendFamily::Hardware
            } else {
                d.family != BackendFamily::Hardware
            }
        })
        .filter(|d| !estimate.requires_density_matrix || d.capabilities.supports_density_matrix)
        .filter(|d| descriptor_fits(estimate, d))
        .collect();
    audit.push(AuditEntry {
        rule: PlannerRule::R0HardConstraint,
        note: format!(
            "want_hardware={}, requires_density_matrix={}, memory-forbidden families={:?} -> {} candidate(s) remain",
            opts.want_hardware,
            estimate.requires_density_matrix,
            estimate.forbidden,
            candidates.len()
        ),
    });

    if candidates.is_empty() {
        return Err(PlannerError::NoBackendAvailable {
            reason: "no registered backend survives R0 hard constraints (hardware/density-matrix/memory-bound)".to_string(),
        });
    }

    // `want_hardware` is itself the whole decision once R0 has restricted the
    // candidate pool to the `Hardware` family: there is nothing left for R1-R4 to
    // refine among a single family (Q10 may add hardware-specific ranking — queue
    // depth, cost, calibration fidelity — once real hardware providers exist; Q0 has
    // no such data, so it takes the first available candidate deterministically).
    if opts.want_hardware {
        let d = candidates[0];
        audit.push(AuditEntry {
            rule: PlannerRule::R0HardConstraint,
            note: format!("want_hardware=true -> selected hardware backend '{}' (no further ranking available at Q0)", d.id),
        });
        return Ok((d.id.clone(), d.family, PlannerRule::R0HardConstraint));
    }

    // R1: Clifford fast path — never pay O(2^n) for an O(n^2)-representable circuit.
    if estimate.is_clifford && !estimate.has_non_clifford_noise {
        if let Some(d) = candidates
            .iter()
            .find(|d| d.family == BackendFamily::Stabilizer)
            .copied()
        {
            audit.push(AuditEntry {
                rule: PlannerRule::R1CliffordStabilizer,
                note: format!("circuit is Clifford with no non-Clifford noise -> selected stabilizer backend '{}'", d.id),
            });
            return Ok((d.id.clone(), d.family, PlannerRule::R1CliffordStabilizer));
        }
        audit.push(AuditEntry {
            rule: PlannerRule::R1CliffordStabilizer,
            note: "circuit is Clifford but no stabilizer backend is registered/available -> falling through".to_string(),
        });
    }

    // R2-R4 collapse into "walk Estimate::preferred in order, first fitting +
    // registered family wins" — `estimate()` already encodes the R2 (noise-driven),
    // R3 (structural entanglement-connectivity, see module docs), and R4 (placement)
    // priority ordering: MatrixProductState appears ahead of the statevector families
    // in `preferred` exactly when `Estimate::entangling_connectivity` says the
    // circuit is genuinely low-entanglement, and is absent from `preferred` entirely
    // when it is not. The rule LABEL attached to the pick is a semantic
    // classification of the family itself (a noise-only family is always an R2 pick,
    // MPS is always R3, anything else reaching this loop is R4 placement), not a
    // re-derivation of what `estimate()` was thinking — that keeps the audit trail
    // meaningful even though the actual selection logic is one uniform walk.
    for family in &estimate.preferred {
        if *family == BackendFamily::Stabilizer {
            continue; // already tried under R1 above; do not re-attempt here
        }
        if let Some(d) = candidates.iter().find(|d| d.family == *family).copied() {
            let rule = match family {
                BackendFamily::Trajectory => PlannerRule::R2Noise,
                BackendFamily::DensityMatrixCpu | BackendFamily::DensityMatrixGpu => {
                    PlannerRule::R2Noise
                }
                BackendFamily::QuestFfi if estimate.requires_density_matrix => PlannerRule::R2Noise,
                BackendFamily::MatrixProductState => PlannerRule::R3Structure,
                _ => PlannerRule::R4Placement,
            };
            audit.push(AuditEntry {
                rule,
                note: format!("selected '{}' from preferred family {:?}", d.id, family),
            });
            return Ok((d.id.clone(), d.family, rule));
        }
    }

    audit.push(AuditEntry {
        rule: PlannerRule::R4Placement,
        note: "no preferred family had a fitting, registered backend".to_string(),
    });
    Err(PlannerError::NoBackendAvailable {
        reason: "no registered backend among Estimate::preferred fits the circuit under the current constraints".to_string(),
    })
}
