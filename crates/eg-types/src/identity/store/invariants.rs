//! Class invariants (IDM-05), checked over the whole store after every op
//! that can change who holds which scope. A violation refuses the op as a
//! whole; the engine then keeps the previous store.

use std::collections::BTreeSet;

use super::super::scope::{ScopeClass, ScopeClassifier};
use super::super::IdentityRefusal;
use super::IdentityStore;

/// Where a role is bound.
enum Binding<'a> {
    User,
    Group(&'a str),
    Link,
}

impl IdentityStore {
    /// Every class rule, or the first violation.
    pub(super) fn validate(&self, classifier: &dyn ScopeClassifier) -> Result<(), IdentityRefusal> {
        self.validate_role_scopes(classifier)?;
        self.validate_approver_bindings(classifier)?;
        self.validate_principal_kinds(classifier)
    }

    /// Every role scope is registered.
    fn validate_role_scopes(
        &self,
        classifier: &dyn ScopeClassifier,
    ) -> Result<(), IdentityRefusal> {
        let unknown = self
            .roles
            .values()
            .flat_map(|role| role.scopes.iter())
            .any(|scope| classifier.class_of(scope).is_none());
        if unknown {
            Err(IdentityRefusal::UnknownScope)
        } else {
            Ok(())
        }
    }

    /// The one approver group a role's approver scopes allow it to be bound
    /// to: `Ok(None)` when it carries none, an error when they disagree.
    fn approver_group_of_role(
        &self,
        role_id: &str,
        classifier: &dyn ScopeClassifier,
    ) -> Result<Option<&'static str>, IdentityRefusal> {
        let Some(role) = self.roles.get(role_id) else {
            return Ok(None);
        };
        let groups: BTreeSet<&'static str> = role
            .scopes
            .iter()
            .filter(|scope| classifier.class_of(scope) == Some(ScopeClass::Approver))
            .map(|scope| {
                classifier
                    .approver_group_of(scope)
                    .ok_or(IdentityRefusal::ClassViolation)
            })
            .collect::<Result<_, _>>()?;
        match groups.len() {
            0 => Ok(None),
            1 => Ok(groups.into_iter().next()),
            _ => Err(IdentityRefusal::ClassViolation),
        }
    }

    /// A role carrying an approver-class scope is bound ONLY to that scope's
    /// built-in approver group: never directly to a principal, never to any
    /// other group, never through an IdP mapping outside that group.
    fn validate_approver_bindings(
        &self,
        classifier: &dyn ScopeClassifier,
    ) -> Result<(), IdentityRefusal> {
        let user_bindings = self
            .users
            .values()
            .flat_map(|user| user.roles.iter().map(|role| (role.as_str(), Binding::User)));
        let group_bindings = self.groups.values().flat_map(|group| {
            group
                .roles
                .iter()
                .map(move |role| (role.as_str(), Binding::Group(group.group_id.as_str())))
        });
        let link_bindings = self
            .link_roles
            .values()
            .flat_map(|roles| roles.iter().map(|role| (role.as_str(), Binding::Link)));
        for (role, binding) in user_bindings.chain(group_bindings).chain(link_bindings) {
            let Some(required) = self.approver_group_of_role(role, classifier)? else {
                continue;
            };
            let allowed = matches!(binding, Binding::Group(group) if group == required);
            if !allowed {
                return Err(IdentityRefusal::ClassViolation);
            }
        }
        Ok(())
    }

    /// Every principal's effective scopes are allowed for its kind: no human
    /// reaches a service-only scope; no service reaches an approver or
    /// administrator scope.
    fn validate_principal_kinds(
        &self,
        classifier: &dyn ScopeClassifier,
    ) -> Result<(), IdentityRefusal> {
        for user in self.users.values() {
            let (roles, _) = self.effective_roles(&user.principal_id);
            let violates = self
                .scopes_of(&roles, classifier)
                .iter()
                .filter_map(|scope| classifier.class_of(scope))
                .any(|class| !class.allows_kind(user.kind));
            if violates {
                return Err(IdentityRefusal::ClassViolation);
            }
        }
        Ok(())
    }
}
