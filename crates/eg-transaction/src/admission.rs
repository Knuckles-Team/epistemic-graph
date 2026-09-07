//! Per-write admission state machine: exactly one admitted batch, exactly one
//! owner-row window inside it, and a poisoned write on any unfinished drop.

use crate::admitted::{is_ledger_only, AdmittedMutation};
use eg_storage::{encode_bounded, MutationClass, OwnerDomain, OwnerLayout};
use eg_types::MutationBatch;

pub(crate) enum AdmissionState {
    Idle,
    Applying {
        batch: Vec<u8>,
        owner: Option<OwnerLayout>,
        class: MutationClass,
    },
    Finished {
        batch: Vec<u8>,
        class: MutationClass,
    },
    /// Admission resolved to a terminal receipt that is ALREADY durable, so
    /// this write applies nothing for it.
    ///
    /// A sole writer aborts on `Begin::Replay` and never reaches commit, but a
    /// group cannot: its members share one transaction, and one member's retry
    /// among N — the shard coalescer's ordinary case — must not discard the
    /// other members' real work. So a replayed member is a first-class
    /// terminal state: it may be committed with, and it may write nothing.
    /// `open_owner_admission` and `finish_batch_admission` both refuse it,
    /// which is what "writes nothing" means mechanically.
    Replayed {
        batch: Vec<u8>,
    },
    Poisoned,
}

impl<D: OwnerDomain> AdmittedMutation<'_, D> {
    pub(crate) fn admit_apply_batch(
        &self,
        batch: &MutationBatch,
        class: MutationClass,
    ) -> Result<(), String> {
        let encoded = encode_bounded(batch, "admitted mutation batch")?;
        let mut state = self.admission.borrow_mut();
        match &*state {
            AdmissionState::Idle | AdmissionState::Finished { .. } => {
                *state = AdmissionState::Applying {
                    batch: encoded,
                    owner: None,
                    class,
                };
                Ok(())
            }
            AdmissionState::Applying { .. } => {
                Err("another mutation batch is already admitted".to_string())
            }
            AdmissionState::Replayed { .. } => {
                Err("a replayed member applies nothing".to_string())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
        }
    }

    /// Record that this member's batch resolved to an already-durable receipt.
    pub(crate) fn admit_replayed_batch(&self, batch: &MutationBatch) -> Result<(), String> {
        let encoded = encode_bounded(batch, "replayed mutation batch")?;
        let mut state = self.admission.borrow_mut();
        match &*state {
            AdmissionState::Idle => {
                *state = AdmissionState::Replayed { batch: encoded };
                Ok(())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("a replayed batch cannot follow an admitted one".to_string()),
        }
    }

    pub(crate) fn admit_prepared_batch(&self, batch: &MutationBatch) -> Result<(), String> {
        self.admit_apply_batch(batch, MutationClass::Operation)
    }

    /// The class of the batch this write is applying, or has just finished.
    ///
    /// A poisoned write reports poison rather than "no class", so an unfinished
    /// owner capability still fails with the poison it caused. Replay evidence
    /// may be recorded either side of `finish`, so `Finished` answers too.
    pub(crate) fn admitted_class(&self) -> Result<MutationClass, String> {
        match &*self.admission.borrow() {
            AdmissionState::Applying { class, .. } | AdmissionState::Finished { class, .. } => {
                Ok(*class)
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            AdmissionState::Replayed { .. } => {
                Err("a replayed member has no admitted class".to_string())
            }
            AdmissionState::Idle => Err("mutation write has no admitted batch".to_string()),
        }
    }

    pub(crate) fn open_owner_admission(
        &self,
        batch: &MutationBatch,
        layout: OwnerLayout,
    ) -> Result<(), String> {
        let encoded = encode_bounded(batch, "owner mutation batch")?;
        let mut state = self.admission.borrow_mut();
        match &mut *state {
            AdmissionState::Applying {
                batch: admitted,
                owner,
                ..
            } if admitted.as_slice() == encoded.as_slice() && owner.is_none() => {
                *owner = Some(layout);
                Ok(())
            }
            AdmissionState::Applying { .. } => {
                Err("owner write does not match the admitted batch".to_string())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("owner write requires an admitted mutation batch".to_string()),
        }
    }

    pub(crate) fn finish_owner_admission(&self, layout: OwnerLayout) -> Result<(), String> {
        let state = self.admission.borrow();
        match &*state {
            AdmissionState::Applying {
                owner: Some(open), ..
            } if *open == layout => Ok(()),
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("owner write completion does not match its admission".to_string()),
        }
    }

    pub(crate) fn poison_owner_admission(&self) {
        *self.admission.borrow_mut() = AdmissionState::Poisoned;
    }

    pub(crate) fn finish_batch_admission(&self, batch: &MutationBatch) -> Result<(), String> {
        let encoded = encode_bounded(batch, "finished mutation batch")?;
        let mut state = self.admission.borrow_mut();
        match &*state {
            AdmissionState::Applying {
                batch: admitted,
                owner,
                class,
            } if admitted.as_slice() == encoded.as_slice()
                && (is_ledger_only(D::LAYOUT) || owner.is_some()) =>
            {
                let class = *class;
                *state = AdmissionState::Finished {
                    batch: encoded,
                    class,
                };
                Ok(())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("mutation batch owner work is unfinished or mismatched".to_string()),
        }
    }

    pub(crate) fn validate_commit_admission(&self, batch: &MutationBatch) -> Result<(), String> {
        let encoded = encode_bounded(batch, "committed mutation batch")?;
        match &*self.admission.borrow() {
            AdmissionState::Finished { batch: finished, .. }
                if finished.as_slice() == encoded.as_slice() =>
            {
                Ok(())
            }
            // A replayed member wrote nothing and needs nothing written; its
            // receipt was already durable before this transaction opened.
            AdmissionState::Replayed { batch: replayed }
                if replayed.as_slice() == encoded.as_slice() =>
            {
                Ok(())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("mutation commit does not match a finished batch".to_string()),
        }
    }
}
