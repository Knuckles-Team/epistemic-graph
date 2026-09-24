//! Restore from an administrator's SQL dump (`import_sql`).
//!
//! A MERGE: rows whose key already exists are skipped (a built-in, the
//! bootstrap principal, a role restored earlier); a new username that
//! collides with another principal's is refused. Dumps carry no credentials,
//! so a restored active human is `pending_reset` until an administrator
//! issues a reset. Class invariants are re-checked over the whole store
//! after the import, so a dump cannot smuggle in a forbidden binding.

use std::collections::{BTreeMap, BTreeSet};

use super::super::access::{GroupRecord, IdpConfig, MappingRule, RoleRecord};
use super::super::model::{ExternalIdentity, UserKind, UserRecord, UserStatus};
use super::super::requests_admin::RoleGraphGrant;
use super::super::sql_dump::{parse_dump, DumpRow};
use super::super::{normalize_username, validate_principal_id, IdentityRefusal};
use super::{link_key, IdentityStore};

type Importer = fn(&mut IdentityStore, &DumpRow, u64) -> Result<bool, IdentityRefusal>;

/// Importers in dependency order.
const IMPORTERS: [(&str, Importer); 11] = [
    ("roles", IdentityStore::import_role),
    ("role_scopes", IdentityStore::import_role_scope),
    ("role_grants", IdentityStore::import_role_grant),
    ("groups", IdentityStore::import_group),
    ("group_roles", IdentityStore::import_group_role),
    ("users", IdentityStore::import_user),
    ("user_roles", IdentityStore::import_user_role),
    ("group_members", IdentityStore::import_member),
    ("idps", IdentityStore::import_idp),
    ("idp_rules", IdentityStore::import_idp_rule),
    ("links", IdentityStore::import_link),
];

/// Relations a dump carries that a restore deliberately does not replay.
const NOT_RESTORED: [&str; 3] = ["api_keys", "audit", "config"];

fn required(row: &DumpRow, column: &str) -> Result<String, IdentityRefusal> {
    row.text(column).ok_or(IdentityRefusal::InvalidRequest)
}

fn parsed<T: serde::de::DeserializeOwned>(row: &DumpRow, column: &str) -> Result<T, IdentityRefusal> {
    serde_json::from_value(row.get(column).clone()).map_err(|_| IdentityRefusal::InvalidRequest)
}

fn flag(row: &DumpRow, column: &str) -> bool {
    row.get(column).as_bool().unwrap_or(false)
}

impl IdentityStore {
    /// Merge every row of `dump`. Answers how many rows were new.
    pub(super) fn import_dump(&mut self, dump: &str, now_ms: u64) -> Result<usize, IdentityRefusal> {
        let rows = parse_dump(dump)?;
        let known = |relation: &str| {
            NOT_RESTORED.contains(&relation) || IMPORTERS.iter().any(|(name, _)| *name == relation)
        };
        if rows.iter().any(|row| !known(&row.relation)) {
            return Err(IdentityRefusal::InvalidRequest);
        }
        let mut imported = 0;
        for (relation, importer) in IMPORTERS {
            for row in rows.iter().filter(|row| row.relation == relation) {
                imported += usize::from(importer(self, row, now_ms)?);
            }
        }
        Ok(imported)
    }

    fn import_role(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let role_id = required(row, "role_id")?;
        super::super::text::identifier(&role_id)?;
        if self.roles.contains_key(&role_id) {
            return Ok(false);
        }
        let role = RoleRecord {
            role_id: role_id.clone(),
            name: required(row, "name")?,
            description: row.text("description"),
            builtin: false,
            scopes: BTreeSet::new(),
            graph_grants: Vec::new(),
        };
        self.roles.insert(role_id, role);
        Ok(true)
    }

    /// The restorable (non-built-in) role a binding row names.
    fn restorable_role(&mut self, row: &DumpRow) -> Result<Option<&mut RoleRecord>, IdentityRefusal> {
        let role = self
            .roles
            .get_mut(&required(row, "role_id")?)
            .ok_or(IdentityRefusal::NotFound)?;
        Ok((!role.builtin).then_some(role))
    }

    fn import_role_scope(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let scope = required(row, "scope")?;
        Ok(self
            .restorable_role(row)?
            .is_some_and(|role| role.scopes.insert(scope)))
    }

    fn import_role_grant(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let resource = serde_json::from_str(&required(row, "resource")?)
            .map_err(|_| IdentityRefusal::InvalidRequest)?;
        let grant = RoleGraphGrant {
            resource,
            action: parsed(row, "action")?,
            effect: parsed(row, "effect")?,
        };
        let Some(role) = self.restorable_role(row)? else {
            return Ok(false);
        };
        let new = !role.graph_grants.contains(&grant);
        if new {
            role.graph_grants.push(grant);
        }
        Ok(new)
    }

    fn import_group(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let group_id = required(row, "group_id")?;
        super::super::text::identifier(&group_id)?;
        if self.groups.contains_key(&group_id) {
            return Ok(false);
        }
        let group = GroupRecord {
            group_id: group_id.clone(),
            name: required(row, "name")?,
            source: required(row, "source")?,
            builtin: false,
            members: BTreeMap::new(),
            roles: BTreeSet::new(),
            mfa_required: flag(row, "mfa_required"),
        };
        self.groups.insert(group_id, group);
        Ok(true)
    }

    fn import_group_role(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let role_id = required(row, "role_id")?;
        if !self.roles.contains_key(&role_id) {
            return Err(IdentityRefusal::NotFound);
        }
        let group = self
            .groups
            .get_mut(&required(row, "group_id")?)
            .ok_or(IdentityRefusal::NotFound)?;
        Ok(!group.builtin && group.roles.insert(role_id))
    }

    fn import_user(&mut self, row: &DumpRow, now_ms: u64) -> Result<bool, IdentityRefusal> {
        let principal_id = required(row, "principal_id")?;
        validate_principal_id(&principal_id)?;
        if self.users.contains_key(&principal_id) {
            return Ok(false);
        }
        let username = normalize_username(&required(row, "username")?)?;
        if self.usernames.contains_key(&username) {
            return Err(IdentityRefusal::Collision);
        }
        let kind: UserKind = parsed(row, "kind")?;
        let dumped: UserStatus = parsed(row, "status")?;
        let status = match (kind, dumped) {
            (UserKind::Human, UserStatus::Active) => UserStatus::PendingReset,
            (_, status) => status,
        };
        let user = UserRecord {
            principal_id,
            username,
            display_name: row.text("display_name"),
            email: row.text("email"),
            kind,
            status,
            is_bootstrap: false,
            source: required(row, "source")?,
            roles: BTreeSet::new(),
            created_at_ms: row.get("created_at_ms").as_u64().unwrap_or(now_ms),
            disabled_at_ms: row.get("disabled_at_ms").as_u64(),
            last_login_at_ms: row.get("last_login_at_ms").as_u64(),
        };
        self.insert_user(user, &BTreeSet::new(), "local");
        Ok(true)
    }

    fn import_user_role(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let role_id = required(row, "role_id")?;
        if !self.roles.contains_key(&role_id) {
            return Err(IdentityRefusal::NotFound);
        }
        let user = self
            .users
            .get_mut(&required(row, "principal_id")?)
            .ok_or(IdentityRefusal::NotFound)?;
        Ok(user.roles.insert(role_id))
    }

    fn import_member(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let principal = required(row, "principal_id")?;
        if !self.users.contains_key(&principal) {
            return Err(IdentityRefusal::NotFound);
        }
        let group = self
            .groups
            .get_mut(&required(row, "group_id")?)
            .ok_or(IdentityRefusal::NotFound)?;
        let source = row.text("source").unwrap_or_else(|| "local".to_string());
        Ok(group.members.insert(principal, source).is_none())
    }

    fn import_idp(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let idp_id = required(row, "idp_id")?;
        super::super::text::identifier(&idp_id)?;
        if self.idps.contains_key(&idp_id) {
            return Ok(false);
        }
        let domains = row.text("email_domains").unwrap_or_default();
        let idp = IdpConfig {
            idp_id: idp_id.clone(),
            kind: parsed(row, "kind")?,
            display_name: required(row, "display_name")?,
            enabled: flag(row, "enabled"),
            config_json: required(row, "config_json")?,
            secret_ref: row.text("secret_ref"),
            jit_policy: parsed(row, "jit_policy")?,
            email_domains: domains.split_whitespace().map(str::to_string).collect(),
            order: row.get("sort_order").as_u64().unwrap_or(0) as u32,
            rules: Vec::new(),
        };
        self.idps.insert(idp_id, idp);
        Ok(true)
    }

    fn import_idp_rule(&mut self, row: &DumpRow, _now_ms: u64) -> Result<bool, IdentityRefusal> {
        let rule = MappingRule {
            rule_id: required(row, "rule_id")?,
            claim_path: required(row, "claim_path")?,
            match_kind: required(row, "match_kind")?,
            value: required(row, "value")?,
            target: required(row, "target")?,
            privileged: flag(row, "privileged"),
        };
        let idp = self
            .idps
            .get_mut(&required(row, "idp_id")?)
            .ok_or(IdentityRefusal::NotFound)?;
        let new = !idp.rules.iter().any(|existing| existing.rule_id == rule.rule_id);
        if new {
            idp.rules.push(rule);
        }
        Ok(new)
    }

    fn import_link(&mut self, row: &DumpRow, now_ms: u64) -> Result<bool, IdentityRefusal> {
        let (idp_id, subject) = (required(row, "idp_id")?, required(row, "subject")?);
        let principal_id = required(row, "principal_id")?;
        let known = self.idps.contains_key(&idp_id) && self.users.contains_key(&principal_id);
        if !known {
            return Err(IdentityRefusal::NotFound);
        }
        let key = link_key(&idp_id, &subject);
        if self.links.contains_key(&key) {
            return Ok(false);
        }
        let link = ExternalIdentity {
            idp_id,
            subject,
            principal_id,
            linked_at_ms: row.get("linked_at_ms").as_u64().unwrap_or(now_ms),
            linked_by: row.text("linked_by").unwrap_or_else(|| "import".to_string()),
        };
        self.links.insert(key, link);
        Ok(true)
    }
}
