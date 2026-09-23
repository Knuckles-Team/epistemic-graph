//! EH-374 WASM UDF tenancy proofs through the FULL served dispatch chain
//! (CONCEPT:EG-KG.query.rowset-execution). `RegisterUdf` used to write ONE
//! process-global `id → module` registry, so a tenant could run another tenant's UDF
//! and shadow it by re-registering the same id.

use super::*;

/// Echoes its input bytes (the same identity module the dispatch UDF test uses).
const IDENTITY_WAT: &str = r#"(module
    (memory (export "memory") 1)
    (global $n (mut i32) (i32.const 1024))
    (func (export "alloc") (param $l i32) (result i32)
        (local $p i32) (local.set $p (global.get $n))
        (global.set $n (i32.add (global.get $n) (local.get $l))) (local.get $p))
    (func (export "udf") (param $p i32) (param $l i32) (result i64)
        (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32))
                (i64.extend_i32_u (local.get $l)))))"#;

/// Always returns the single byte `0xbb`, whatever its input.
const CONSTANT_WAT: &str = r#"(module
    (memory (export "memory") 1)
    (data (i32.const 2048) "\bb")
    (func (export "alloc") (param $l i32) (result i32) (i32.const 1024))
    (func (export "udf") (param $p i32) (param $l i32) (result i64)
        (i64.or (i64.shl (i64.const 2048) (i64.const 32)) (i64.const 1))))"#;

fn register(wat_text: &str) -> Method {
    Method::RegisterUdf {
        id: "shared_id".into(),
        wasm: wat::parse_str(wat_text).expect("valid wat"),
    }
}

fn run(input: &[u8]) -> Method {
    Method::RunUdf {
        id: "shared_id".into(),
        input: input.to_vec(),
    }
}

fn raw_output(resp: &crate::protocol::Response) -> Vec<u8> {
    assert_ok(resp);
    match &resp.result {
        Some(ResultPayload::Raw(out)) => out.clone(),
        other => panic!("expected Raw UDF output, got {other:?}"),
    }
}

/// Tenant B can neither run tenant A's UDF (it resolves as unregistered) nor shadow it
/// (B's registration of the same id is B's own module; A still runs A's).
#[tokio::test]
async fn udf_ids_are_tenant_scoped_through_dispatch() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = test_state();
    assert_ok(&dispatch_in_tenant(&state, 930, "tenant-a", register(IDENTITY_WAT)).await);

    let refused = dispatch_in_tenant(&state, 931, "tenant-b", run(b"probe")).await;
    let err = refused.error.expect("tenant B must not run tenant A's UDF");
    assert!(
        err.contains("no UDF registered under id 'shared_id'") && !err.contains("ACCESS_DENIED"),
        "a cross-tenant UDF id must look unregistered, got: {err}"
    );

    assert_ok(&dispatch_in_tenant(&state, 932, "tenant-b", register(CONSTANT_WAT)).await);
    let a_out = raw_output(&dispatch_in_tenant(&state, 933, "tenant-a", run(b"payload")).await);
    assert_eq!(
        a_out,
        b"payload".to_vec(),
        "tenant B must not shadow tenant A's UDF"
    );
    let b_out = raw_output(&dispatch_in_tenant(&state, 934, "tenant-b", run(b"payload")).await);
    assert_eq!(b_out, vec![0xbb], "tenant B runs its own module");
}
