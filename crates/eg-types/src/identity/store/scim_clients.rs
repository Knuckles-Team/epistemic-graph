//! Typed SCIM provisioner administration over the existing IdP binding.
//! A bearer secret is never stored here: API keys are issued separately.

use serde_json::{Map, Value};

use super::super::access::IdpKind;
use super::super::audit::IdentityEvent;
use super::super::model::UserKind;
use super::super::requests_admin::ScimClientBinding;
use super::super::stamp::IdentityStamp;
use super::super::text::{bounded, MAX_TEXT_BYTES};
use super::super::views::{IdentityReply, ScimClientView};
use super::super::IdentityRefusal;
use super::IdentityStore;

fn config_object(raw: &str) -> Result<Map<String, Value>, IdentityRefusal> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .ok_or(IdentityRefusal::InvalidRequest)
}

fn provisioner(config: &str) -> Option<String> {
    config_object(config)
        .ok()?
        .get("provisioner")?
        .as_str()
        .map(str::to_string)
}

impl IdentityStore {
    pub(super) fn scim_client(&self, idp_id: &str) -> Result<ScimClientView, IdentityRefusal> {
        let idp = self.idps.get(idp_id).ok_or(IdentityRefusal::NotFound)?;
        if idp.kind != IdpKind::Scim {
            return Err(IdentityRefusal::KindMismatch);
        }
        let principal_id = provisioner(&idp.config_json).ok_or(IdentityRefusal::NotFound)?;
        Ok(ScimClientView {
            idp_id: idp_id.to_string(),
            principal_id,
            enabled: idp.enabled,
        })
    }

    pub(super) fn scim_clients(&self) -> Vec<ScimClientView> {
        self.idps
            .values()
            .filter(|idp| idp.kind == IdpKind::Scim)
            .filter_map(|idp| self.scim_client(&idp.idp_id).ok())
            .collect()
    }

    pub(super) fn upsert_scim_client(
        &mut self,
        request: &ScimClientBinding,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let service = self
            .users
            .get(&request.principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        if service.kind != UserKind::Service || !service.status.is_active() {
            return Err(IdentityRefusal::KindMismatch);
        }
        let idp = self
            .idps
            .get_mut(&request.idp_id)
            .ok_or(IdentityRefusal::NotFound)?;
        if idp.kind != IdpKind::Scim {
            return Err(IdentityRefusal::KindMismatch);
        }
        let mut config = config_object(&idp.config_json)?;
        let changed = config.get("provisioner").and_then(Value::as_str)
            != Some(request.principal_id.as_str());
        config.insert(
            "provisioner".to_string(),
            Value::String(request.principal_id.clone()),
        );
        let serialized = Value::Object(config).to_string();
        bounded(&serialized, MAX_TEXT_BYTES)?;
        idp.config_json = serialized;
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::IdpChanged,
            Some(&request.idp_id),
        );
        Ok(IdentityReply::Done { changed })
    }

    pub(super) fn remove_scim_client(
        &mut self,
        idp_id: &str,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let idp = self.idps.get_mut(idp_id).ok_or(IdentityRefusal::NotFound)?;
        if idp.kind != IdpKind::Scim {
            return Err(IdentityRefusal::KindMismatch);
        }
        let mut config = config_object(&idp.config_json)?;
        let changed = config.remove("provisioner").is_some();
        if changed {
            idp.config_json = Value::Object(config).to_string();
            self.audit_event(stamp, now_ms, IdentityEvent::IdpChanged, Some(idp_id));
        }
        Ok(IdentityReply::Done { changed })
    }
}
