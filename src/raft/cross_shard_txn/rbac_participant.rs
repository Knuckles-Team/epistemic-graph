//! RBAC whole-image participant in local, certificate-bound graph 2PC.
//! This does not grant profile use or implement served task admission.
use super::*;
use eg_core::rbac_persist::{
    RbacPolicyStore, RbacReservationDecisionSource, RbacReservationRequest,
};
use eg_transaction::{CoordinatorDecisionBinding, CoordinatorDecisionCertificate};
use sha2::{Digest, Sha256};

struct DurableDecision<'a>(&'a RedbBackend);
impl RbacReservationDecisionSource for DurableDecision<'_> {
    fn terminal_decision(
        &self,
        request: &RbacReservationRequest,
    ) -> Result<Option<CoordinatorDecisionCertificate>, String> {
        let result = self
            .0
            .xshard_decision_certificate_get(request.transaction_id())?;
        if let Some(certificate) = &result {
            certificate.matches(request.binding(), certificate.commit)?;
        }
        Ok(result)
    }
}
impl CrossShardTxn {
    /// Canonical whole graph intent; the native caller also captures owner authority bounds.
    pub fn rbac_read_request_digest(&self) -> Result<String, String> {
        let mut value = serde_json::to_value(self).map_err(|e| e.to_string())?;
        value.sort_all_objects();
        Ok(hex::encode(Sha256::digest(
            serde_json::to_vec(&value).map_err(|e| e.to_string())?,
        )))
    }
}
impl CrossShardCoordinator {
    /// Native local coordinator entry point. The request holds the caller's captured
    /// owner context, including tenant, physical coordinator scope and earliest deadline.
    /// The durable coordinator clock, never a caller timestamp, decides expiry.
    pub async fn commit_cross_shard_with_rbac_read(
        &self,
        txn: &CrossShardTxn,
        store: &dyn RbacPolicyStore,
        request: RbacReservationRequest,
    ) -> Result<TxnOutcome, String> {
        if request.transaction_id() != txn.txn_id
            || request.request_digest() != txn.rbac_read_request_digest()?
        {
            return Err("RBAC participant does not bind this graph transaction".into());
        }
        if let Some(commit) = store
            .reservation_outcome(&request)
            .map_err(|e| e.to_string())?
        {
            return Ok(if commit {
                TxnOutcome::Committed
            } else {
                TxnOutcome::Aborted
            });
        }
        if self.participants(txn).len() < 2 {
            return Err(
                "RBAC cross-shard read participant requires at least two graph groups".into(),
            );
        }
        self.validate_rbac_prior_decision(store, &request)?;
        let reservation = store
            .prepare_reservation(request)
            .map_err(|e| e.to_string())?;
        // Every error retains the owner fence. No legacy boolean can release it.
        let outcome = self
            .commit_certified_rbac_graph(txn, reservation.request().binding())
            .await?;
        if !store
            .resolve_reservation(&reservation, &DurableDecision(self.redb()?))
            .map_err(|e| e.to_string())?
        {
            return Err("RBAC participant has no bound durable decision certificate".into());
        }
        Ok(outcome)
    }

    fn validate_rbac_prior_decision(
        &self,
        store: &dyn RbacPolicyStore,
        request: &RbacReservationRequest,
    ) -> Result<(), String> {
        let redb = self.redb()?;
        if let Some(certificate) = redb.xshard_decision_certificate_get(request.transaction_id())? {
            certificate.matches(request.binding(), certificate.commit)?;
        } else if store
            .pending_reservation()
            .map_err(|e| e.to_string())?
            .is_none()
        {
            let existing = redb
                .xshard_decision_get(request.transaction_id())?
                .is_some()
                || redb.xshard_decision_retain_get(request.transaction_id())?
                || commit::prepares_by_txn(redb)?.contains_key(request.transaction_id());
            if existing {
                return Err("unbound graph transaction cannot acquire an RBAC reservation".into());
            }
        }
        Ok(())
    }

    async fn commit_certified_rbac_graph(
        &self,
        txn: &CrossShardTxn,
        binding: &CoordinatorDecisionBinding,
    ) -> Result<TxnOutcome, String> {
        binding.validate()?;
        let redb = self.redb()?;
        let mut all_prepared = commit::prepares_by_txn(redb)?;
        let prepared = all_prepared.remove(&txn.txn_id).unwrap_or_default();
        if let Some(certificate) = redb.xshard_decision_certificate_get(&txn.txn_id)? {
            certificate.matches(binding, certificate.commit)?;
            return self.apply_rbac_certificate(&certificate, &prepared).await;
        }
        if redb.xshard_decision_get(&txn.txn_id)?.is_some()
            || redb.xshard_decision_retain_get(&txn.txn_id)?
            || !prepared.is_empty()
        {
            return Err(
                "RBAC graph transaction remains in doubt without a bound certificate".into(),
            );
        }
        redb.xshard_recoverable_pending_put(&txn.txn_id).await?;
        let (writing, read_only) = self.split_participants(self.participants(txn));
        let invalid = self
            .abort_on_invalid_read_only(redb, &txn.txn_id, &read_only, false)
            .await?
            .is_some();
        let (all_yes, prepared_groups) = if invalid {
            (false, Vec::new())
        } else {
            self.run_prepare_phase(redb, &txn.txn_id, &writing).await
        };
        let certificate = redb
            .xshard_certified_decision_put(
                binding.clone(),
                all_yes && prepared_groups.len() == writing.len(),
            )
            .await?;
        certificate.matches(binding, certificate.commit)?;
        self.apply_rbac_certificate(&certificate, &writing).await
    }

    async fn apply_rbac_certificate(
        &self,
        certificate: &CoordinatorDecisionCertificate,
        prepared: &BTreeMap<GroupId, Vec<GraphSlice>>,
    ) -> Result<TxnOutcome, String> {
        certificate.validate()?;
        let redb = self.redb()?;
        let txn_id = &certificate.binding.transaction_id;
        if certificate.commit {
            self.apply_commit(redb, txn_id, prepared, false).await?;
            Ok(TxnOutcome::Committed)
        } else {
            self.apply_abort(
                redb,
                txn_id,
                &prepared.keys().copied().collect::<Vec<_>>(),
                false,
            )
            .await?;
            Ok(TxnOutcome::Aborted)
        }
    }

    /// Missing certificates remain in doubt. Phase two completes before releasing
    /// the RBAC writer fence, including after restart or legacy outcome cleanup.
    pub async fn recover_rbac_read_participant(
        &self,
        store: &dyn RbacPolicyStore,
    ) -> Result<bool, String> {
        let Some(reservation) = store.pending_reservation().map_err(|e| e.to_string())? else {
            return Ok(false);
        };
        let redb = self.redb()?;
        let source = DurableDecision(redb);
        let Some(certificate) = source.terminal_decision(reservation.request())? else {
            return Ok(false);
        };
        let mut all_prepared = commit::prepares_by_txn(redb)?;
        let prepared = all_prepared
            .remove(reservation.request().transaction_id())
            .unwrap_or_default();
        self.apply_rbac_certificate(&certificate, &prepared).await?;
        store
            .resolve_reservation(&reservation, &source)
            .map_err(|e| e.to_string())
    }
}
