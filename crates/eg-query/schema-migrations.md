# Governed user-table schema migrations

`eg-query::tables::TableStore::apply_schema_migration` is the authoritative
schema-transition seam for user tables.  A caller first obtains a
`SchemaSnapshot`, builds a `SchemaMigration::for_schema` plan, and submits the
sealed plan with the same owner scope used to open the store.

Each commit atomically applies the ordered row/catalog operations and writes:

* the monotonic per-`(tenant_scope, table)` schema version;
* the immutable migration record, including its SHA-256 checksum and
  forward-only recovery metadata; and
* the version-to-identity order index used for restart verification.

The CAS fields (`expected_schema_version` and `expected_schema_digest`) reject
stale readers, gaps, out-of-order plans, and concurrent writers.  Retrying the
same identity is a no-op only when the stored bytes and final catalog digest
match exactly.  A changed payload with an existing identity is a checksum/
identity error.

The migration surface is deliberately conservative:

* DROP operations and marked lossy type coercions require explicit policy flags;
* a column participating in a local or child foreign key must be rebound in a
  separate governed operation;
* an affected secondary/ANN index must be explicitly coordinated; this API
  never silently drops or leaves an index stale;
* RLS remains owned by its external authority and requires an opaque binding
  digest when revalidation is required; raw policy material is never stored;
* rollback metadata points to a governed restore checkpoint.  There is no
  inverse/down-migration operation.

Legacy one-shot ALTER/DROP DDL remains available for version-zero tables.  Once
a governed migration commits, those paths fail closed and callers must use the
migration seam.  Existing pre-migration stores are treated as version zero and
are verified on restart before serving.
