//! Physical identity of one durable owner file: its incarnation, its persisted
//! root, its scope bindings, and its read-only validation handle.

pub(crate) mod binding;
pub(crate) mod incarnation;
pub(crate) mod integrity;
pub(crate) mod manifest;
pub(crate) mod read_only;
pub(crate) mod root;

#[cfg(test)]
mod tests;
