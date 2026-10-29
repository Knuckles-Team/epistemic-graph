//! Served agent-bus records (EG-DURABLE-KERNEL-R095.1000).
//!
//! A [`BusParticipant`] is one session's row on a tenant's bus; a [`HubPeer`] is
//! a federated hub with the scope it may receive. Registration, roster, send,
//! dispatch and federation behavior are EG-DURABLE-KERNEL-R095.1001 onwards.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusParticipant {
    pub session_ref: String,
    pub tenant: String,
    pub host_ref: String,
    pub capabilities: BTreeSet<String>,
    pub last_seen_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubPeer {
    pub hub_ref: String,
    /// Reference to the peer hub's URL in configuration; never a raw credentialed URL.
    pub url_ref: String,
    pub scope: BTreeSet<String>,
}
