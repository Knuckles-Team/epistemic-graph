//! The identity store as standard SQL relations (operator ruling: "standard
//! SQL tables to store users as a web application would").
//!
//! Every relation is REDACTED by construction: its columns are listed here,
//! and none holds a password hash, a token or session hash, a sealed secret
//! or a recovery code. The engine serves them read-only, to identity readers
//! only, inside its one authorized SQL projection, and the admin dump
//! (`export_sql`) renders the same relations as Postgres DDL + INSERTs.

use serde_json::{json, Value};

use super::store::IdentityStore;
use super::views::SessionView;

/// A column's SQL type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Text,
    Integer,
    Boolean,
}

impl SqlType {
    pub fn postgres(self) -> &'static str {
        match self {
            Self::Text => "TEXT",
            Self::Integer => "BIGINT",
            Self::Boolean => "BOOLEAN",
        }
    }
}

use SqlType::{Boolean, Integer, Text};

/// One relation: its name (unqualified), its columns (the first is the key
/// column), and its rows in column order.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlRelation {
    pub name: &'static str,
    pub columns: &'static [(&'static str, SqlType)],
    pub rows: Vec<Vec<Value>>,
}

pub const USERS: &[(&str, SqlType)] = &[
    ("principal_id", Text),
    ("username", Text),
    ("display_name", Text),
    ("email", Text),
    ("kind", Text),
    ("status", Text),
    ("is_bootstrap", Boolean),
    ("source", Text),
    ("created_at_ms", Integer),
    ("disabled_at_ms", Integer),
    ("last_login_at_ms", Integer),
    ("has_password", Boolean),
    ("totp_enrolled", Boolean),
];
pub const ROLES: &[(&str, SqlType)] = &[
    ("role_id", Text),
    ("name", Text),
    ("description", Text),
    ("builtin", Boolean),
];
pub const ROLE_SCOPES: &[(&str, SqlType)] = &[("role_id", Text), ("scope", Text)];
pub const ROLE_GRANTS: &[(&str, SqlType)] = &[
    ("role_id", Text),
    ("resource", Text),
    ("action", Text),
    ("effect", Text),
];
pub const GROUPS: &[(&str, SqlType)] = &[
    ("group_id", Text),
    ("name", Text),
    ("source", Text),
    ("builtin", Boolean),
    ("mfa_required", Boolean),
];
pub const GROUP_MEMBERS: &[(&str, SqlType)] =
    &[("group_id", Text), ("principal_id", Text), ("source", Text)];
pub const GROUP_ROLES: &[(&str, SqlType)] = &[("group_id", Text), ("role_id", Text)];
pub const USER_ROLES: &[(&str, SqlType)] = &[("principal_id", Text), ("role_id", Text)];
pub const SESSIONS: &[(&str, SqlType)] = &[
    ("handle", Text),
    ("principal_id", Text),
    ("created_at_ms", Integer),
    ("last_seen_at_ms", Integer),
    ("idle_expires_at_ms", Integer),
    ("absolute_expires_at_ms", Integer),
    ("auth_methods", Text),
    ("mfa_pending", Boolean),
    ("ip_prefix", Text),
    ("revoked", Boolean),
];
pub const API_KEYS: &[(&str, SqlType)] = &[
    ("key_id", Text),
    ("principal_id", Text),
    ("scopes", Text),
    ("created_at_ms", Integer),
    ("expires_at_ms", Integer),
    ("last_used_at_ms", Integer),
    ("revoked_at_ms", Integer),
];
pub const IDPS: &[(&str, SqlType)] = &[
    ("idp_id", Text),
    ("kind", Text),
    ("display_name", Text),
    ("enabled", Boolean),
    ("config_json", Text),
    ("secret_ref", Text),
    ("jit_policy", Text),
    ("email_domains", Text),
    ("sort_order", Integer),
];
pub const IDP_RULES: &[(&str, SqlType)] = &[
    ("idp_id", Text),
    ("rule_id", Text),
    ("claim_path", Text),
    ("match_kind", Text),
    ("value", Text),
    ("target", Text),
    ("privileged", Boolean),
];
pub const LINKS: &[(&str, SqlType)] = &[
    ("idp_id", Text),
    ("subject", Text),
    ("principal_id", Text),
    ("linked_at_ms", Integer),
    ("linked_by", Text),
];
pub const AUDIT: &[(&str, SqlType)] = &[
    ("seq", Integer),
    ("at_ms", Integer),
    ("actor", Text),
    ("event", Text),
    ("target", Text),
    ("ip_prefix", Text),
    ("detail", Text),
    ("prev", Text),
    ("chain", Text),
];
pub const THROTTLE: &[(&str, SqlType)] = &[
    ("key", Text),
    ("failures", Integer),
    ("window_start_ms", Integer),
    ("next_allowed_at_ms", Integer),
];
pub const CONFIG: &[(&str, SqlType)] = &[
    ("mode", Text),
    ("local_fallback", Text),
    ("registration_policy", Text),
    ("password_min_chars", Integer),
    ("epoch", Integer),
    ("issuer_kid_current", Text),
];

/// The serde name of a unit enum value (`"active"`, `"human"`, …).
pub(crate) fn tag<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn relation(
    name: &'static str,
    columns: &'static [(&str, SqlType)],
    rows: Vec<Vec<Value>>,
) -> SqlRelation {
    SqlRelation {
        name,
        columns,
        rows,
    }
}

impl IdentityStore {
    /// Every relation, redacted, in a stable order.
    pub fn sql_relations(&self) -> Vec<SqlRelation> {
        vec![
            relation("users", USERS, self.user_rows()),
            relation("roles", ROLES, self.role_rows()),
            relation("role_scopes", ROLE_SCOPES, self.role_scope_rows()),
            relation("role_grants", ROLE_GRANTS, self.role_grant_rows()),
            relation("groups", GROUPS, self.group_rows()),
            relation("group_members", GROUP_MEMBERS, self.member_rows()),
            relation("group_roles", GROUP_ROLES, self.group_role_rows()),
            relation("user_roles", USER_ROLES, self.user_role_rows()),
            relation("sessions", SESSIONS, self.session_rows()),
            relation("api_keys", API_KEYS, self.api_key_rows()),
            relation("idps", IDPS, self.idp_rows()),
            relation("idp_rules", IDP_RULES, self.idp_rule_rows()),
            relation("links", LINKS, self.link_rows()),
            relation("audit", AUDIT, self.audit_rows()),
            relation("throttle", THROTTLE, self.throttle_rows()),
            relation("config", CONFIG, self.config_rows()),
        ]
    }

    fn user_rows(&self) -> Vec<Vec<Value>> {
        self.users
            .values()
            .map(|user| {
                vec![
                    json!(user.principal_id),
                    json!(user.username),
                    json!(user.display_name),
                    json!(user.email),
                    tag(&user.kind),
                    tag(&user.status),
                    json!(user.is_bootstrap),
                    json!(user.source),
                    json!(user.created_at_ms),
                    json!(user.disabled_at_ms),
                    json!(user.last_login_at_ms),
                    json!(self.passwords.contains_key(&user.principal_id)),
                    json!(self.mfa_enrolled(&user.principal_id)),
                ]
            })
            .collect()
    }

    fn role_rows(&self) -> Vec<Vec<Value>> {
        self.roles
            .values()
            .map(|role| {
                vec![
                    json!(role.role_id),
                    json!(role.name),
                    json!(role.description),
                    json!(role.builtin),
                ]
            })
            .collect()
    }

    fn role_scope_rows(&self) -> Vec<Vec<Value>> {
        self.roles
            .values()
            .flat_map(|role| {
                role.scopes
                    .iter()
                    .map(move |scope| vec![json!(role.role_id), json!(scope)])
            })
            .collect()
    }

    fn role_grant_rows(&self) -> Vec<Vec<Value>> {
        self.roles
            .values()
            .flat_map(|role| {
                role.graph_grants.iter().map(move |grant| {
                    vec![
                        json!(role.role_id),
                        Value::String(serde_json::to_string(&grant.resource).unwrap_or_default()),
                        tag(&grant.action),
                        tag(&grant.effect),
                    ]
                })
            })
            .collect()
    }

    fn group_rows(&self) -> Vec<Vec<Value>> {
        self.groups
            .values()
            .map(|group| {
                vec![
                    json!(group.group_id),
                    json!(group.name),
                    json!(group.source),
                    json!(group.builtin),
                    json!(group.mfa_required),
                ]
            })
            .collect()
    }

    fn member_rows(&self) -> Vec<Vec<Value>> {
        self.groups
            .values()
            .flat_map(|group| {
                group.members.iter().map(move |(principal, source)| {
                    vec![json!(group.group_id), json!(principal), json!(source)]
                })
            })
            .collect()
    }

    fn group_role_rows(&self) -> Vec<Vec<Value>> {
        self.groups
            .values()
            .flat_map(|group| {
                group
                    .roles
                    .iter()
                    .map(move |role| vec![json!(group.group_id), json!(role)])
            })
            .collect()
    }

    fn user_role_rows(&self) -> Vec<Vec<Value>> {
        self.users
            .values()
            .flat_map(|user| {
                user.roles
                    .iter()
                    .map(move |role| vec![json!(user.principal_id), json!(role)])
            })
            .collect()
    }

    fn session_rows(&self) -> Vec<Vec<Value>> {
        self.sessions
            .values()
            .map(|session| {
                let view = SessionView::of(session);
                vec![
                    json!(view.handle),
                    json!(view.principal_id),
                    json!(view.created_at_ms),
                    json!(view.last_seen_at_ms),
                    json!(view.idle_expires_at_ms),
                    json!(view.absolute_expires_at_ms),
                    json!(view.auth_methods.join(" ")),
                    json!(view.mfa_pending),
                    json!(view.ip_prefix),
                    json!(view.revoked),
                ]
            })
            .collect()
    }

    fn api_key_rows(&self) -> Vec<Vec<Value>> {
        self.api_keys
            .values()
            .map(|key| {
                let scopes: Vec<&str> = key.scopes.iter().map(String::as_str).collect();
                vec![
                    json!(key.key_id),
                    json!(key.principal_id),
                    json!(scopes.join(" ")),
                    json!(key.created_at_ms),
                    json!(key.expires_at_ms),
                    json!(key.last_used_at_ms),
                    json!(key.revoked_at_ms),
                ]
            })
            .collect()
    }

    fn idp_rows(&self) -> Vec<Vec<Value>> {
        self.idps
            .values()
            .map(|idp| {
                vec![
                    json!(idp.idp_id),
                    tag(&idp.kind),
                    json!(idp.display_name),
                    json!(idp.enabled),
                    json!(idp.config_json),
                    json!(idp.secret_ref),
                    tag(&idp.jit_policy),
                    json!(idp.email_domains.join(" ")),
                    json!(idp.order),
                ]
            })
            .collect()
    }

    fn idp_rule_rows(&self) -> Vec<Vec<Value>> {
        self.idps
            .values()
            .flat_map(|idp| {
                idp.rules.iter().map(move |rule| {
                    vec![
                        json!(idp.idp_id),
                        json!(rule.rule_id),
                        json!(rule.claim_path),
                        json!(rule.match_kind),
                        json!(rule.value),
                        json!(rule.target),
                        json!(rule.privileged),
                    ]
                })
            })
            .collect()
    }

    fn link_rows(&self) -> Vec<Vec<Value>> {
        self.links
            .values()
            .map(|link| {
                vec![
                    json!(link.idp_id),
                    json!(link.subject),
                    json!(link.principal_id),
                    json!(link.linked_at_ms),
                    json!(link.linked_by),
                ]
            })
            .collect()
    }

    fn audit_rows(&self) -> Vec<Vec<Value>> {
        self.audit
            .entries()
            .map(|entry| {
                vec![
                    json!(entry.seq),
                    json!(entry.at_ms),
                    json!(entry.actor),
                    tag(&entry.event),
                    json!(entry.target),
                    json!(entry.ip_prefix),
                    json!(entry.detail),
                    json!(entry.prev),
                    json!(entry.chain),
                ]
            })
            .collect()
    }

    fn throttle_rows(&self) -> Vec<Vec<Value>> {
        self.throttle
            .iter()
            .map(|(key, entry)| {
                vec![
                    json!(key),
                    json!(entry.failures),
                    json!(entry.window_start_ms),
                    json!(entry.next_allowed_at_ms),
                ]
            })
            .collect()
    }

    fn config_rows(&self) -> Vec<Vec<Value>> {
        self.config
            .iter()
            .map(|config| {
                vec![
                    tag(&config.mode),
                    tag(&config.local_fallback),
                    tag(&config.registration_policy),
                    json!(config.password_min_chars),
                    json!(config.epoch),
                    json!(config.issuer_kid_current),
                ]
            })
            .collect()
    }
}
