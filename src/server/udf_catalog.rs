//! Tenant-scoped WASM UDF catalog (CONCEPT:EG-KG.query.rowset-execution, EH-374).
//!
//! `Method::RegisterUdf` compiles an agent-supplied module and `Method::RunUdf` runs it
//! sandboxed. Before EH-374 the server kept ONE process-global `id → module` registry:
//! any tenant could RUN a UDF another tenant registered (its code, and any data baked
//! into it) and could SHADOW it — re-registering the same id replaced the module, so
//! the owner's next `RunUdf` silently executed the other tenant's code.
//!
//! Same shape as the foreign-source fix (`server::foreign_catalog`, EH-373): entries are
//! keyed by `(verified tenant scope, id)`, the tenant scope comes only from a
//! [`CarrierAuthority`] minted from the verified envelope, and [`UdfCatalog::run`] /
//! [`UdfCatalog::register`] are the only ways in. Another tenant's id resolves exactly
//! like an unregistered id (eg-wasm's own "no UDF registered" message), so the refusal
//! does not reveal that the id exists elsewhere. Modules are in memory only
//! (`RegisterUdf`'s durable record is an opaque saga receipt), so there is no stored
//! key shape to migrate. The served `Op::Udf` plan op binds no registry today, so
//! `RunUdf` is the only reader.

use std::sync::Arc;

use eg_wasm::{UdfError, UdfLimits, UdfModule, UdfRegistry};

use super::access::CarrierAuthority;

/// Every tenant's compiled UDFs, partitioned by verified tenant scope.
#[derive(Default)]
pub struct UdfCatalog {
    modules: UdfRegistry,
}

/// The registry key for `(tenant scope, id)`. A tenant scope is an opaque
/// `[a-z0-9:-]` token (see `CarrierAuthority::from_verified`), so it never contains
/// the NUL separator and two distinct pairs never share a key.
fn catalog_key(tenant_scope: &str, id: &str) -> String {
    format!("{tenant_scope}\0{id}")
}

impl UdfCatalog {
    /// Compile + record `owner`'s UDF `id`, replacing only `owner`'s own prior module.
    pub(crate) fn register(
        &self,
        owner: &CarrierAuthority,
        id: &str,
        wasm: &[u8],
        limits: UdfLimits,
    ) -> Result<(), UdfError> {
        self.modules
            .register(&catalog_key(owner.tenant_scope(), id), wasm, limits)
    }

    /// `caller`'s own compiled module `id`, if registered.
    pub(crate) fn module_for(&self, caller: &CarrierAuthority, id: &str) -> Option<Arc<UdfModule>> {
        self.modules.get(&catalog_key(caller.tenant_scope(), id))
    }

    /// Run `caller`'s own UDF `id` over `input`. Another tenant's id is "not
    /// registered", byte-identical to a typo.
    pub(crate) fn run(
        &self,
        caller: &CarrierAuthority,
        id: &str,
        input: &[u8],
    ) -> Result<Vec<u8>, UdfError> {
        let module = self
            .module_for(caller, id)
            .ok_or_else(|| UdfError::Abi(format!("no UDF registered under id '{id}'")))?;
        module.run(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::auth::VerifiedRequestContext;

    fn carrier(tenant: &str) -> CarrierAuthority {
        let context = VerifiedRequestContext::verified_for_test_in_tenant("agent-x", tenant);
        CarrierAuthority::from_verified(&context).expect("verified test carrier")
    }

    /// A UDF whose output is the constant byte `value` (so two modules are
    /// distinguishable by what they return).
    fn constant_udf(value: u8) -> Vec<u8> {
        wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 2048) "\{value:02x}")
                (func (export "alloc") (param $l i32) (result i32) (i32.const 1024))
                (func (export "udf") (param $p i32) (param $l i32) (result i64)
                    (i64.or (i64.shl (i64.const 2048) (i64.const 32)) (i64.const 1))))"#
        ))
        .expect("valid wat")
    }

    #[test]
    fn udf_ids_are_tenant_scoped_for_run_and_shadowing() {
        let (a, b) = (carrier("tenant-a"), carrier("tenant-b"));
        let catalog = UdfCatalog::default();
        catalog
            .register(&a, "f", &constant_udf(0xaa), UdfLimits::default())
            .unwrap();
        let err = catalog.run(&b, "f", b"x").unwrap_err().to_string();
        assert_eq!(err, "udf ABI error: no UDF registered under id 'f'");

        catalog
            .register(&b, "f", &constant_udf(0xbb), UdfLimits::default())
            .unwrap();
        assert_eq!(
            catalog.run(&a, "f", b"x").unwrap(),
            vec![0xaa],
            "no shadowing"
        );
        assert_eq!(
            catalog.run(&b, "f", b"x").unwrap(),
            vec![0xbb],
            "own module"
        );
    }
}
