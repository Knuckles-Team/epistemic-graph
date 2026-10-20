//! Real-file reservation and ordinary-writer regression coverage.
use super::*;

struct Decision(Option<bool>);
impl RbacReservationDecisionSource for Decision {
    fn terminal_decision(
        &self,
        request: &RbacReservationRequest,
    ) -> Result<Option<eg_transaction::CoordinatorDecisionCertificate>, String> {
        Ok(self
            .0
            .map(|commit| eg_transaction::CoordinatorDecisionCertificate {
                version: 1,
                binding: request.binding().clone(),
                commit,
                decided_at_ms: 1,
            }))
    }
}

fn path(label: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "eg-rbac-reservation-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn request(store: &RbacStore, transaction: &str) -> RbacReservationRequest {
    RbacReservationRequest::new(
        &store.authority_snapshot().unwrap(),
        eg_transaction::CoordinatorDecisionBinding {
            transaction_id: transaction.into(),
            tenant: "tenant-test".into(),
            coordinator_scope: "graph-test".into(),
            request_digest: [0xaa; 32],
            authority_deadline_ms: 100,
        },
    )
    .unwrap()
}

fn save_current(store: &RbacStore) -> Result<u64, RbacPersistError> {
    let image = store.authority_snapshot()?;
    store.compare_and_save(
        image.revision(),
        image.policy(),
        image.identities(),
        image.bootstrap(),
    )
}

#[test]
fn reservation_survives_reopen_and_missing_decision_then_releases_on_abort() {
    let directory = path("restart");
    let reservation;
    {
        let store = test_support::open_test_store(&directory).unwrap();
        let proposed = request(&store, "transaction-a");
        reservation = store.prepare_reservation(proposed.clone()).unwrap();
        assert_eq!(store.prepare_reservation(proposed).unwrap(), reservation);
        assert!(matches!(
            save_current(&store),
            Err(RbacPersistError::Reserved { .. })
        ));
    }
    {
        let store = test_support::open_test_store(&directory).unwrap();
        assert_eq!(
            store.pending_reservation().unwrap(),
            Some(reservation.clone())
        );
        assert!(!store
            .resolve_reservation(&reservation, &Decision(None))
            .unwrap());
        assert!(matches!(
            save_current(&store),
            Err(RbacPersistError::Reserved { .. })
        ));
        assert!(store
            .resolve_reservation(&reservation, &Decision(Some(false)))
            .unwrap());
        assert!(store
            .resolve_reservation(&reservation, &Decision(Some(false)))
            .unwrap());
        assert!(store.pending_reservation().unwrap().is_none());
        save_current(&store).unwrap();
        // The retained receipt replays even after a subsequent authority write.
        assert!(store
            .resolve_reservation(&reservation, &Decision(Some(false)))
            .unwrap());
        assert!(store
            .resolve_reservation(&reservation, &Decision(Some(true)))
            .is_err());
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn conflicting_and_stale_prepares_do_not_replace_the_owner_reservation() {
    let directory = path("conflict");
    {
        let store = test_support::open_test_store(&directory).unwrap();
        let stale = request(&store, "stale");
        save_current(&store).unwrap();
        assert!(store.prepare_reservation(stale).is_err());
        let reservation = store
            .prepare_reservation(request(&store, "winner"))
            .unwrap();
        assert!(store
            .prepare_reservation(request(&store, "competitor"))
            .is_err());
        assert_eq!(
            store.pending_reservation().unwrap(),
            Some(reservation.clone())
        );
        assert!(store
            .resolve_reservation(&reservation, &Decision(Some(true)))
            .unwrap());
        save_current(&store).unwrap();
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn participant_digest_covers_session_state_omitted_from_policy_decision_digest() {
    let session = serde_json::json!({
        "session_hash": "session-hash", "principal_id": "principal-a",
        "created_at_ms": 1, "last_seen_at_ms": 1, "idle_expires_at_ms": 100,
        "absolute_expires_at_ms": 200, "auth_methods": ["pwd"], "mfa_pending": false
    });
    let mut value = serde_json::to_value(RbacPolicy::new()).unwrap();
    value["identity"] = serde_json::json!({"sessions": {"session-hash": session}});
    let before: RbacPolicy = serde_json::from_value(value.clone()).unwrap();
    value["identity"]["sessions"]["session-hash"]["revoked_at_ms"] = serde_json::json!(50);
    let revoked: RbacPolicy = serde_json::from_value(value).unwrap();
    assert_eq!(
        authority_policy_digest(&before).unwrap(),
        authority_policy_digest(&revoked).unwrap()
    );
    assert_ne!(
        reservation::full_image_digest(&before, &BTreeMap::new(), IdentityBootstrapState::Consumed)
            .unwrap(),
        reservation::full_image_digest(
            &revoked,
            &BTreeMap::new(),
            IdentityBootstrapState::Consumed
        )
        .unwrap()
    );
}

#[test]
fn deserialized_malformed_requests_leave_no_reservation_or_version_change() {
    let directory = path("invalid");
    {
        let store = test_support::open_test_store(&directory).unwrap();
        let before = store.current_version().unwrap();
        let good = serde_json::to_value(request(&store, "valid")).unwrap();
        for (field, value) in [
            ("transaction_id", "x".repeat(5000)),
            ("transaction_id", "bad\nidentity".into()),
            ("request_digest", "not-a-digest".into()),
            ("authority_digest", "A".repeat(64)),
        ] {
            let mut malformed = good.clone();
            malformed[field] = serde_json::Value::String(value);
            let request: RbacReservationRequest = serde_json::from_value(malformed).unwrap();
            assert!(store.prepare_reservation(request).is_err());
            assert_eq!(store.current_version().unwrap(), before);
            assert!(store.pending_reservation().unwrap().is_none());
        }
        save_current(&store).unwrap();
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn resolved_transaction_identity_cannot_reserve_a_new_image() {
    let directory = path("transaction-reuse");
    {
        let store = test_support::open_test_store(&directory).unwrap();
        let original = store
            .prepare_reservation(request(&store, "same-id"))
            .unwrap();
        assert!(store
            .resolve_reservation(&original, &Decision(Some(true)))
            .unwrap());
        save_current(&store).unwrap();
        let before = store.current_version().unwrap();
        assert!(store
            .prepare_reservation(request(&store, "same-id"))
            .is_err());
        assert_eq!(store.current_version().unwrap(), before);
        assert!(store.pending_reservation().unwrap().is_none());
        assert!(store
            .resolve_reservation(&original, &Decision(Some(false)))
            .is_err());
        assert!(store
            .resolve_reservation(&original, &Decision(Some(true)))
            .unwrap());
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn certificate_context_or_expiry_mismatch_never_releases_writer_fence() {
    struct Supplied(eg_transaction::CoordinatorDecisionCertificate);
    impl RbacReservationDecisionSource for Supplied {
        fn terminal_decision(
            &self,
            _: &RbacReservationRequest,
        ) -> Result<Option<eg_transaction::CoordinatorDecisionCertificate>, String> {
            Ok(Some(self.0.clone()))
        }
    }
    let directory = path("certificate-binding");
    {
        let store = test_support::open_test_store(&directory).unwrap();
        let record = store.prepare_reservation(request(&store, "bound")).unwrap();
        let good = eg_transaction::CoordinatorDecisionCertificate {
            version: 1,
            binding: record.request().binding().clone(),
            commit: true,
            decided_at_ms: 1,
        };
        let mut wrong_tenant = good.clone();
        wrong_tenant.binding.tenant = "other".into();
        let mut wrong_scope = good.clone();
        wrong_scope.binding.coordinator_scope = "other".into();
        let mut wrong_intent = good.clone();
        wrong_intent.binding.request_digest = [3; 32];
        let mut expired = good.clone();
        expired.decided_at_ms = expired.binding.authority_deadline_ms;
        for certificate in [wrong_tenant, wrong_scope, wrong_intent, expired] {
            assert!(store
                .resolve_reservation(&record, &Supplied(certificate))
                .is_err());
            assert_eq!(store.pending_reservation().unwrap(), Some(record.clone()));
            assert!(matches!(
                save_current(&store),
                Err(RbacPersistError::Reserved { .. })
            ));
        }
        assert!(store.resolve_reservation(&record, &Supplied(good)).unwrap());
    }
    std::fs::remove_dir_all(directory).unwrap();
}
