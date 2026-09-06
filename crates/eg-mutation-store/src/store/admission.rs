//! Per-write admission state machine: exactly one admitted batch, exactly one
//! owner-row window inside it, and a poisoned write on any unfinished drop.

use crate::write::{is_ledger_only, MutationWrite};
use eg_storage::{encode_bounded, OwnerDomain, OwnerLayout};
use eg_types::MutationBatch;

pub(crate) enum AdmissionState {
    Idle,
    Applying {
        batch: Vec<u8>,
        owner: Option<OwnerLayout>,
    },
    Finished(Vec<u8>),
    Poisoned,
}

impl<D: OwnerDomain> MutationWrite<'_, D> {
    pub(crate) fn admit_apply_batch(&self, batch: &MutationBatch) -> Result<(), String> {
        let encoded = encode_bounded(batch, "admitted mutation batch")?;
        let mut state = self.admission.borrow_mut();
        match &*state {
            AdmissionState::Idle | AdmissionState::Finished(_) => {
                *state = AdmissionState::Applying {
                    batch: encoded,
                    owner: None,
                };
                Ok(())
            }
            AdmissionState::Applying { .. } => {
                Err("another mutation batch is already admitted".to_string())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
        }
    }

    pub(crate) fn admit_prepared_batch(&self, batch: &MutationBatch) -> Result<(), String> {
        self.admit_apply_batch(batch)
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
            } if admitted.as_slice() == encoded.as_slice()
                && (is_ledger_only(D::LAYOUT) || owner.is_some()) =>
            {
                *state = AdmissionState::Finished(encoded);
                Ok(())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("mutation batch owner work is unfinished or mismatched".to_string()),
        }
    }

    pub(crate) fn validate_commit_admission(&self, batch: &MutationBatch) -> Result<(), String> {
        let encoded = encode_bounded(batch, "committed mutation batch")?;
        match &*self.admission.borrow() {
            AdmissionState::Finished(finished) if finished.as_slice() == encoded.as_slice() => {
                Ok(())
            }
            AdmissionState::Poisoned => Err("mutation write is poisoned".to_string()),
            _ => Err("mutation commit does not match a finished batch".to_string()),
        }
    }
}
