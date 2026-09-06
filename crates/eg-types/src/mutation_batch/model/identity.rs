use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use super::request::MutationDomain;

const MAX_TENANT_ID_BYTES: usize = 255;
const MAX_LOGICAL_NAME_BYTES: usize = 1_024;
const MAX_INCARNATION_ID_BYTES: usize = 4_096;
const IDENTITY_DIGEST_DOMAIN: &[u8] = b"eg/mutation-scope-identity/v1\0";
const BINDING_DIGEST_DOMAIN: &[u8] = b"eg/mutation-scope-binding/v1\0";

/// The sole tenant reserved for engine-owned bootstrap and control-plane work.
pub const RESERVED_SYSTEM_TENANT: &str = "__eg_system__";

/// Validated tenant security boundary. Bytes are retained exactly as supplied.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TenantId(String);

impl TenantId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_identifier(&value, MAX_TENANT_ID_BYTES, "mutation tenant")?;
        Ok(Self(value))
    }

    pub fn system() -> Self {
        Self(RESERVED_SYSTEM_TENANT.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_system(&self) -> bool {
        self.0 == RESERVED_SYSTEM_TENANT
    }
}

/// Validated graph or native-resource name with no filesystem semantics.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LogicalName(String);

impl LogicalName {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_identifier(&value, MAX_LOGICAL_NAME_BYTES, "mutation logical name")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque logical-generation identity. It is never derived from a resource name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IncarnationId(String);

impl IncarnationId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_opaque(&value, MAX_INCARNATION_ID_BYTES, "mutation incarnation")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! impl_validated_deserialize {
    ($identity:ty) => {
        impl<'de> Deserialize<'de> for $identity {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

impl_validated_deserialize!(TenantId);
impl_validated_deserialize!(LogicalName);
impl_validated_deserialize!(IncarnationId);

/// Typed logical owner of a durable mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MutationScope {
    Graph {
        graph: LogicalName,
    },
    Native {
        domain: MutationDomain,
        resource: LogicalName,
    },
}

impl MutationScope {
    pub fn graph_name(&self) -> Option<&LogicalName> {
        match self {
            Self::Graph { graph } => Some(graph),
            Self::Native { .. } => None,
        }
    }

    pub fn native_domain(&self) -> Option<MutationDomain> {
        match self {
            Self::Graph { .. } => None,
            Self::Native { domain, .. } => Some(*domain),
        }
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Graph { .. } => Ok(()),
            Self::Native { domain, .. } if domain.may_own_native_scope() => Ok(()),
            Self::Native { domain, .. } => Err(format!(
                "mutation domain '{}' cannot own a native scope",
                domain.canonical_name()
            )),
        }
    }
}

/// Canonical SHA-256 identity digest persisted with every authority-bearing row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationScopeDigest([u8; 32]);

impl MutationScopeDigest {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

/// Event types whose `ApplyMutation` record may carry a sealed private payload.
///
/// This is a CONTRACT, not a convenience list. `eg-mutation-store`'s recovery
/// scan asserts that every sealed payload it finds belongs to a recognized plan
/// shape, and the producers of those plans live in the top crate
/// (`handlers/txn.rs`'s two-phase transaction recovery, `dispatch.rs`'s
/// SPARQL-HTTP update saga and its compensation). Declaring the set in one place
/// both crates can see is what stops the scan and the producers drifting apart --
/// which is exactly how the SPARQL saga came to be rejected by a check that only
/// knew about transaction recovery.
pub const PRIVATE_PAYLOAD_EVENT_TYPES: [&str; 3] = [
    "transaction_recovery_plan",
    "sparql_http_recovery_plan_v1",
    "sparql_http_compensation_v1",
];

/// Incarnation stamped on every `MutationScopeIdentity` built by the top-level
/// mutation-batch compiler (`src/server/mutation_batch.rs`).
///
/// It lives here, beside the type it stamps, because more than one crate must
/// reproduce it byte-for-byte: `eg_mutation_store`'s scope-binding validator
/// rejects ANY incarnation mismatch, so a store that builds its own identity for
/// the same logical scope (eg-tsdb's series store, the KV store) must agree with
/// whatever the compiler stamped. Two copies kept in step by a comment is exactly
/// the drift this program forbids -- one definition, imported by both.
pub const COMPILED_BATCH_INCARNATION: &str = "epistemic-graph:mutation-batch-compiler:v1";

/// Tenant + typed logical owner + exact lifecycle generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationScopeIdentity {
    tenant: TenantId,
    scope: MutationScope,
    incarnation_id: IncarnationId,
    identity_digest: MutationScopeDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationScopeIdentityWire {
    tenant: TenantId,
    scope: MutationScope,
    incarnation_id: IncarnationId,
    identity_digest: MutationScopeDigest,
}

impl MutationScopeIdentity {
    pub fn graph(tenant: TenantId, graph: LogicalName, incarnation_id: IncarnationId) -> Self {
        Self::new(tenant, MutationScope::Graph { graph }, incarnation_id)
            .expect("graph scope construction is infallible after newtype validation")
    }

    /// Build a fixed graph-scoped identity from string literals.
    ///
    /// Every store that owns one logical scope for the life of a file needs the
    /// same five lines: three fallible newtype constructors and one scope
    /// constructor. Left to each consumer, that shape gets copied -- eight
    /// near-identical `*_scope_identity()` helpers appeared across this
    /// workspace before this constructor existed, each with its own private
    /// constants, and one pair was duplicated verbatim between a crate and its
    /// own integration test because the constants were not exported.
    ///
    /// Prefer this over hand-rolling the sequence. It is fallible because the
    /// newtypes validate (see `validate_identifier`), so an invalid literal is
    /// a returned error rather than a panic at startup.
    pub fn fixed_graph(tenant: &str, graph: &str, incarnation_id: &str) -> Result<Self, String> {
        Ok(Self::graph(
            TenantId::new(tenant)?,
            LogicalName::new(graph)?,
            IncarnationId::new(incarnation_id)?,
        ))
    }

    /// Build a fixed native-scoped identity from string literals.
    ///
    /// See [`Self::fixed_graph`] for why this exists. `domain` must equal the
    /// domain of every operation the scope carries; `validate_operations`
    /// enforces that equality for a native scope.
    pub fn fixed_native(
        tenant: &str,
        domain: MutationDomain,
        resource: &str,
        incarnation_id: &str,
    ) -> Result<Self, String> {
        Self::native(
            TenantId::new(tenant)?,
            domain,
            LogicalName::new(resource)?,
            IncarnationId::new(incarnation_id)?,
        )
    }

    pub fn native(
        tenant: TenantId,
        domain: MutationDomain,
        resource: LogicalName,
        incarnation_id: IncarnationId,
    ) -> Result<Self, String> {
        Self::new(
            tenant,
            MutationScope::Native { domain, resource },
            incarnation_id,
        )
    }

    fn new(
        tenant: TenantId,
        scope: MutationScope,
        incarnation_id: IncarnationId,
    ) -> Result<Self, String> {
        scope.validate()?;
        let identity_digest = compute_identity_digest(&tenant, &scope, &incarnation_id);
        Ok(Self {
            tenant,
            scope,
            incarnation_id,
            identity_digest,
        })
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn scope(&self) -> &MutationScope {
        &self.scope
    }

    pub fn incarnation_id(&self) -> &IncarnationId {
        &self.incarnation_id
    }

    pub fn identity_digest(&self) -> MutationScopeDigest {
        self.identity_digest
    }

    /// Digest of tenant + logical owner, excluding the lifecycle generation.
    /// The native store uses this key to reject silent same-name rebinding.
    pub fn binding_digest(&self) -> MutationScopeDigest {
        compute_binding_digest(&self.tenant, &self.scope)
    }

    pub fn validate_digest(&self) -> Result<(), String> {
        self.scope.validate()?;
        let expected = compute_identity_digest(&self.tenant, &self.scope, &self.incarnation_id);
        if self.identity_digest != expected {
            return Err("mutation scope identity digest mismatch".to_string());
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for MutationScopeIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MutationScopeIdentityWire::deserialize(deserializer)?;
        let identity = Self::new(wire.tenant, wire.scope, wire.incarnation_id)
            .map_err(serde::de::Error::custom)?;
        if identity.identity_digest != wire.identity_digest {
            return Err(serde::de::Error::custom(
                "mutation scope identity digest mismatch",
            ));
        }
        Ok(identity)
    }
}

fn validate_identifier(value: &str, max_bytes: usize, label: &str) -> Result<(), String> {
    validate_opaque(value, max_bytes, label)?;
    if matches!(value, "." | "..") || value.contains('/') || value.contains('\\') {
        return Err(format!("{label} must not contain path semantics"));
    }
    // Persistence privacy policy. Before v1 the tenant and graph names were flat
    // `String`s scanned by `ChangeEnvelope::validate_core_text_fields`, which rejected
    // address- and path-shaped inline text. v1 makes them typed, so that scan would
    // otherwise become a second, weaker authority for a rule this constructor should
    // own. The path/backslash forms are already rejected above -- and every remaining
    // filesystem form the scan looked for (`/home/`, `/users/`, `/mnt/`, `file://`)
    // contains a `/` -- so an address-shaped value is the one case left to close here.
    // Enforcing it at the constructor covers every persisted identity, not just the
    // ones that happen to travel inside a ChangeEnvelope.
    if value.contains('@') {
        return Err(format!(
            "{label} must not contain address-shaped text (persistence privacy policy)"
        ));
    }
    Ok(())
}

fn validate_opaque(value: &str, max_bytes: usize, label: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(format!("{label} is invalid"));
    }
    if value.trim() != value {
        return Err(format!(
            "{label} must use exact non-whitespace boundary bytes"
        ));
    }
    Ok(())
}

fn compute_identity_digest(
    tenant: &TenantId,
    scope: &MutationScope,
    incarnation_id: &IncarnationId,
) -> MutationScopeDigest {
    let mut hasher = Sha256::new();
    hasher.update(IDENTITY_DIGEST_DOMAIN);
    encode_scope(&mut hasher, tenant, scope);
    update_lp32(&mut hasher, incarnation_id.as_str().as_bytes());
    MutationScopeDigest(hasher.finalize().into())
}

fn compute_binding_digest(tenant: &TenantId, scope: &MutationScope) -> MutationScopeDigest {
    let mut hasher = Sha256::new();
    hasher.update(BINDING_DIGEST_DOMAIN);
    encode_scope(&mut hasher, tenant, scope);
    MutationScopeDigest(hasher.finalize().into())
}

fn encode_scope(hasher: &mut Sha256, tenant: &TenantId, scope: &MutationScope) {
    match scope {
        MutationScope::Graph { graph } => {
            hasher.update([0]);
            update_lp32(hasher, tenant.as_str().as_bytes());
            update_lp32(hasher, b"graph");
            update_lp32(hasher, graph.as_str().as_bytes());
        }
        MutationScope::Native { domain, resource } => {
            hasher.update([1]);
            update_lp32(hasher, tenant.as_str().as_bytes());
            update_lp32(hasher, domain.canonical_name().as_bytes());
            update_lp32(hasher, resource.as_str().as_bytes());
        }
    }
}

fn update_lp32(hasher: &mut Sha256, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("validated mutation identity field fits LP32");
    hasher.update(length.to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incarnation(value: &str) -> IncarnationId {
        IncarnationId::new(value).unwrap()
    }

    #[test]
    fn malformed_names_are_rejected_without_normalization() {
        for value in ["", " graph", "graph ", ".", "..", "a/b", "a\\b", "a\0b"] {
            assert!(LogicalName::new(value).is_err(), "accepted {value:?}");
        }
        assert!(TenantId::new("tenant\nother").is_err());
        assert!(IncarnationId::new(" incarnation").is_err());
    }

    /// Regression: v0 scanned the flat `tenant`/`graph` strings for address- and
    /// path-shaped inline text in `ChangeEnvelope::validate_core_text_fields`. When v1
    /// made those fields typed, that scan stopped applying to them, and only the
    /// path-shaped half of the policy was reproduced by `validate_identifier` -- so an
    /// address-shaped tenant id would have been silently accepted where it had always
    /// been rejected. This pins the whole policy at the constructor.
    #[test]
    fn address_and_path_shaped_identities_are_rejected_by_construction() {
        for value in [
            "person@example.invalid",
            "/home/person",
            "/users/person",
            "/mnt/data",
            "file://tenant",
        ] {
            assert!(TenantId::new(value).is_err(), "accepted tenant {value:?}");
            assert!(LogicalName::new(value).is_err(), "accepted graph {value:?}");
        }
        // The policy must not over-reach into ordinary identities.
        assert!(TenantId::new("tenant-a").is_ok());
        assert!(LogicalName::new("graph-a").is_ok());
    }

    #[test]
    fn digest_separates_tenant_scope_kind_and_native_domain() {
        let graph = MutationScopeIdentity::graph(
            TenantId::new("tenant-a").unwrap(),
            LogicalName::new("shared").unwrap(),
            incarnation("incarnation-1"),
        );
        let other_tenant = MutationScopeIdentity::graph(
            TenantId::new("tenant-b").unwrap(),
            LogicalName::new("shared").unwrap(),
            incarnation("incarnation-1"),
        );
        let blob = MutationScopeIdentity::native(
            TenantId::new("tenant-a").unwrap(),
            MutationDomain::BlobStore,
            LogicalName::new("shared").unwrap(),
            incarnation("incarnation-1"),
        )
        .unwrap();
        let jobs = MutationScopeIdentity::native(
            TenantId::new("tenant-a").unwrap(),
            MutationDomain::AnalyticsJob,
            LogicalName::new("shared").unwrap(),
            incarnation("incarnation-1"),
        )
        .unwrap();
        let semantic = MutationScopeIdentity::native(
            TenantId::new("tenant-a").unwrap(),
            MutationDomain::SemanticIndex,
            LogicalName::new("shared").unwrap(),
            incarnation("incarnation-1"),
        )
        .unwrap();
        assert_ne!(graph.identity_digest(), other_tenant.identity_digest());
        assert_ne!(graph.identity_digest(), blob.identity_digest());
        assert_ne!(blob.identity_digest(), jobs.identity_digest());
        assert_ne!(jobs.identity_digest(), semantic.identity_digest());
    }

    #[test]
    fn persisted_digest_tampering_is_rejected() {
        let identity = MutationScopeIdentity::graph(
            TenantId::new("tenant-a").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            incarnation("incarnation-1"),
        );
        let mut value = serde_json::to_value(identity).unwrap();
        value["identity_digest"][0] = serde_json::json!(255);
        assert!(serde_json::from_value::<MutationScopeIdentity>(value).is_err());
    }

    #[test]
    fn graph_domains_cannot_be_smuggled_into_native_scope() {
        let error = MutationScopeIdentity::native(
            TenantId::new("tenant-a").unwrap(),
            MutationDomain::GraphRows,
            LogicalName::new("graph-shaped-native").unwrap(),
            incarnation("incarnation-1"),
        )
        .unwrap_err();
        assert!(error.contains("cannot own a native scope"));
    }
}
