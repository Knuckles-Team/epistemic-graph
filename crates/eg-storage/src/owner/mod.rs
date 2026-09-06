//! The closed owner-table registry, its typed domains, and the authenticated
//! grants that bind one logical serving scope to one physical owner file.

pub(crate) mod blob_shared;
pub(crate) mod contract;
pub(crate) mod domain;
pub(crate) mod grant;
pub(crate) mod handle;
pub(crate) mod identity;
pub(crate) mod layout;
pub(crate) mod manifest_io;
pub(crate) mod registry;
pub(crate) mod table_api;

pub(crate) use manifest_io::{
    read_current_manifest, validate_manifest_read, validate_manifest_write, write_new_manifest,
};
pub(crate) use registry::{
    copy_declared_owner_tables, hash_declared_owner_tables, open_declared_owner_tables,
    validate_declared_owner_tables, validate_declared_tables_write,
};

#[cfg(test)]
mod blob_shared_tests;
#[cfg(test)]
mod tests;
