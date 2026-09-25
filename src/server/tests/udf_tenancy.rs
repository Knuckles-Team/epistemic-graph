//! EH-374 WASM UDF ownership proofs through the FULL served dispatch chain
//! (CONCEPT:EG-KG.query.rowset-execution). `RegisterUdf` used to write ONE
//! process-global `id → module` registry, so a principal could run another principal's
//! UDF and shadow it by re-registering the same id. One engine is bound to one tenant,
//! so these run as two principals (`worker1`, `worker2`) of that tenant.

use super::*;

/// Echoes its input bytes (also the dispatch UDF test's identity module).
pub(super) const IDENTITY_WAT: &str = r#"(module
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

/// Principal B can neither run principal A's UDF (it resolves as unregistered) nor
/// shadow it (B's registration of the same id is B's own module; A still runs A's).
#[tokio::test]
async fn udf_ids_are_owner_scoped_through_dispatch() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = multi_tenant_state().await;
    assert_ok(&dispatch_as(&state, 930, "worker1", register(IDENTITY_WAT)).await);

    let refused = dispatch_as(&state, 931, "worker2", run(b"probe")).await;
    let err = refused
        .error_detail
        .expect("principal B must not run principal A's UDF");
    assert_eq!(
        err, "udf ABI error: no UDF registered under id 'shared_id'",
        "a cross-principal UDF id must be the exact not-found (not ACCESS_DENIED, not the \
         envelope tenant-binding refusal)"
    );

    assert_ok(&dispatch_as(&state, 932, "worker2", register(CONSTANT_WAT)).await);
    let a_out = raw_output(&dispatch_as(&state, 933, "worker1", run(b"payload")).await);
    assert_eq!(
        a_out,
        b"payload".to_vec(),
        "principal B must not shadow principal A's UDF"
    );
    let b_out = raw_output(&dispatch_as(&state, 934, "worker2", run(b"payload")).await);
    assert_eq!(b_out, vec![0xbb], "principal B runs its own module");
}
