use super::{
    super::{authority::*, contract::*, *},
    support::*,
};

#[tokio::test]
async fn local_transport_is_registry_affine_and_remote_import_is_explicit() {
    TRANSPORT_CHUNK_READS.with(|counter| counter.store(0, Ordering::Relaxed));
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let first_root = private_tempdir();
    let second_root = private_tempdir();
    let first = make_registry(&authority, flow_providers(first_root.path())).unwrap();
    let second = make_registry(&authority, flow_providers(second_root.path())).unwrap();
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 1,
    };
    let (local, rejected_remote, accepted_remote) = {
        let capture = authority.write().await.unwrap();
        (
            first.capture_all(&capture, scope.clone(), 1024).unwrap(),
            first.capture_all(&capture, scope.clone(), 1024).unwrap(),
            first.capture_all(&capture, scope.clone(), 1024).unwrap(),
        )
    };
    let install = authority.begin_install().await;

    let mut local = local.into_transport();
    count_local_transport_reads(&mut local);
    assert!(second.receive_all(&install, local).is_err());
    assert_eq!(
        TRANSPORT_CHUNK_READS.with(|counter| counter.load(Ordering::Relaxed)),
        0
    );

    let mut rejected_remote = rejected_remote
        .into_transport()
        .into_authenticated_remote_wire();
    let source_generation = rejected_remote.header.source_generation_sha256.clone();
    let remote_scope = rejected_remote.header.scope.clone();
    count_remote_transport_reads(&mut rejected_remote);
    assert!(second
        .receive_authenticated_remote(
            &install,
            &"f".repeat(64),
            &source_generation,
            &remote_scope,
            rejected_remote,
        )
        .is_err());
    assert_eq!(
        TRANSPORT_CHUNK_READS.with(|counter| counter.load(Ordering::Relaxed)),
        0
    );

    let mut accepted_remote = accepted_remote
        .into_transport()
        .into_authenticated_remote_wire();
    let capture_set = accepted_remote.header.capture_set_sha256.clone();
    let source_generation = accepted_remote.header.source_generation_sha256.clone();
    let remote_scope = accepted_remote.header.scope.clone();
    count_remote_transport_reads(&mut accepted_remote);
    let captured = second
        .receive_authenticated_remote(
            &install,
            &capture_set,
            &source_generation,
            &remote_scope,
            accepted_remote,
        )
        .unwrap();
    assert_eq!(
        TRANSPORT_CHUNK_READS.with(|counter| counter.load(Ordering::Relaxed)),
        0
    );
    let validated = second.validate_all(&install, captured, 1024, 1, 1).unwrap();
    assert!(TRANSPORT_CHUNK_READS.with(|counter| counter.load(Ordering::Relaxed)) > 0);
    drop(validated);
}

#[tokio::test]
async fn transported_sections_from_different_capture_sets_cannot_be_mixed() {
    let root = private_tempdir();
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let registry = make_registry(&authority, flow_providers(root.path())).unwrap();
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 1,
    };
    let first = {
        let permit = authority.write().await.unwrap();
        registry.capture_all(&permit, scope.clone(), 1024).unwrap()
    };
    let second = {
        let permit = authority.write().await.unwrap();
        registry.capture_all(&permit, scope, 1024).unwrap()
    };
    let install = authority.begin_install().await;
    let mut first = first.into_transport().into_authenticated_remote_wire();
    let mut second = second.into_transport().into_authenticated_remote_wire();
    std::mem::swap(&mut first.sections[2], &mut second.sections[2]);
    let capture_set = first.header.capture_set_sha256.clone();
    let source_generation = first.header.source_generation_sha256.clone();
    let scope = first.header.scope.clone();
    assert!(registry
        .receive_authenticated_remote(&install, &capture_set, &source_generation, &scope, first,)
        .is_err());
}

#[tokio::test]
async fn invalid_capture_preflight_has_no_provider_effect() {
    PROVIDER_CAPTURE_CALLS.with(|counter| counter.store(0, Ordering::Relaxed));
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let registry = make_registry(&authority, providers()).unwrap();
    let capture = authority.write().await.unwrap();
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 1,
    };
    assert!(registry
        .capture_all(&capture, scope, HARD_MAX_DIRECT_STATE_BYTES + 1,)
        .is_err());
    assert_eq!(
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.load(Ordering::Relaxed)),
        0
    );
    drop(capture);

    let install = authority.begin_install().await;
    let root = private_tempdir();
    let invalid_scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 0,
    };
    assert!(registry
        .capture_initial_generation(
            &install,
            &root.path().join("pending"),
            &root.path().join("current"),
            invalid_scope,
            DEFAULT_MAX_DIRECT_STATE_BYTES,
        )
        .is_err());
    assert_eq!(
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.load(Ordering::Relaxed)),
        0
    );
}
