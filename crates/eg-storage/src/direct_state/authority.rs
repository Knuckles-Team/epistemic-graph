use super::{contract::*, generation::*, *};

/// Process-local state-image gate.  All writes to captured authorities hold a read
/// permit; capture/install/recovery holds the exclusive write permit.  Neither
/// permit is Clone, so authority cannot be silently detached from its lexical scope.
#[derive(Clone)]
pub struct StateImageAuthority {
    pub(super) identity: Arc<StateImageAuthorityIdentity>,
    pub(super) gate: Arc<TokioRwLock<()>>,
    pub(super) current: Arc<SyncRwLock<Option<Arc<DirectStateGeneration>>>>,
    pub(super) opened_current: Arc<SyncRwLock<Option<OpenedCurrentFence>>>,
    pub(super) ready: Arc<AtomicBool>,
    pub(super) authority_epoch: Arc<AtomicU64>,
}

pub(super) struct StateImageAuthorityIdentity;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OpenedCurrentFence {
    pub(super) sha256: String,
    pub(super) scope: DirectStateScope,
}

impl OpenedCurrentFence {
    pub(super) fn control_applied_index(&self) -> u64 {
        match self.scope {
            DirectStateScope::DefaultGlobal {
                control_applied_index,
                ..
            } => control_applied_index,
        }
    }

    pub(super) fn authority_epoch(&self) -> u64 {
        self.scope.authority_epoch()
    }

    pub(super) fn validate_successor_of(&self, previous: &Self) -> Result<(), String> {
        if self.authority_epoch() < previous.authority_epoch() {
            return Err("direct-state Current would roll back the authority epoch".into());
        }
        if self.control_applied_index() < previous.control_applied_index() {
            return Err("direct-state Current would roll back the control applied index".into());
        }
        if self.authority_epoch() == previous.authority_epoch()
            && self.control_applied_index() == previous.control_applied_index()
        {
            return Err("different direct-state Current reuses the same authority fence".into());
        }
        Ok(())
    }
}

/// Non-Clone request/stream session. It pins both the readiness read permit and
/// the exact whole generation, so no operation can cross an install boundary.
pub struct DirectStateReadSession {
    pub(super) _permit: StateImageReadPermit,
    pub(super) generation: Arc<DirectStateGeneration>,
}

impl DirectStateReadSession {
    pub fn get<T: DirectStateDomainValue>(&self) -> Result<&T, String> {
        self.generation.get::<T>()
    }

    pub fn scope(&self) -> &DirectStateScope {
        &self.generation.scope
    }

    /// Build an owned operation/cursor while retaining this exact whole-generation
    /// session. The operation value is deliberately not removable and is dropped
    /// before the session, so an owned MVCC read/write transaction cannot escape
    /// the readiness fence between multi-RPC calls.
    pub fn pin_operation<D, R>(
        self,
        build: impl FnOnce(&D) -> Result<R, String>,
    ) -> Result<DirectStatePinnedOperation<R>, String>
    where
        D: DirectStateDomainValue,
        R: DirectStatePinnedValue,
    {
        let operation = build(self.get::<D>()?)?;
        Ok(DirectStatePinnedOperation {
            domain: D::DOMAIN,
            operation,
            _session: self,
        })
    }
}

/// Non-Clone operation/cursor paired with its exact whole-generation read session.
/// Field order is intentional: the operation is dropped before its generation.
pub struct DirectStatePinnedOperation<T> {
    pub(super) domain: DirectStateDomain,
    pub(super) operation: T,
    pub(super) _session: DirectStateReadSession,
}

impl<T> DirectStatePinnedOperation<T> {
    pub fn operation(&self) -> &T {
        &self.operation
    }

    pub fn operation_mut(&mut self) -> &mut T {
        &mut self.operation
    }

    /// Continue a multi-RPC operation against the same domain generation that
    /// created it. The closure may return response data, but the unsafe domain and
    /// operation contracts forbid returning owned store/transaction authority.
    pub fn with_domain<D, R>(
        &mut self,
        apply: impl FnOnce(&D, &mut T) -> Result<R, String>,
    ) -> Result<R, String>
    where
        D: DirectStateDomainValue,
    {
        if D::DOMAIN != self.domain {
            return Err("pinned direct-state operation requested a different domain".into());
        }
        let domain = self._session.get::<D>()?;
        apply(domain, &mut self.operation)
    }
}

impl StateImageAuthority {
    pub fn new() -> Self {
        Self {
            identity: Arc::new(StateImageAuthorityIdentity),
            gate: Arc::new(TokioRwLock::new(())),
            current: Arc::new(SyncRwLock::new(None)),
            opened_current: Arc::new(SyncRwLock::new(None)),
            ready: Arc::new(AtomicBool::new(false)),
            authority_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(super) fn validate_identity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.identity, expected) {
            Ok(())
        } else {
            Err("state-image service belongs to a different authority".into())
        }
    }

    pub async fn read_session(&self) -> Result<DirectStateReadSession, String> {
        let permit = self.read_ready().await?;
        let generation = self
            .current
            .read()
            .map_err(|_| "direct-state generation slot is poisoned".to_string())?
            .clone()
            .ok_or_else(|| "direct-state generation is not initialized".to_string())?;
        permit.validate_affinity(&generation.authority_identity)?;
        Ok(DirectStateReadSession {
            _permit: permit,
            generation,
        })
    }

    pub(super) async fn read_ready(&self) -> Result<StateImageReadPermit, String> {
        let permit = StateImageReadPermit {
            authority_identity: self.identity.clone(),
            _guard: self.gate.clone().read_owned().await,
        };
        if !self.ready.load(Ordering::Acquire) {
            return Err("direct-state authority is not ready".into());
        }
        Ok(permit)
    }

    pub async fn write(&self) -> Result<StateImageWritePermit, String> {
        let guard = self.gate.clone().write_owned().await;
        if !self.ready.load(Ordering::Acquire) {
            return Err("direct-state authority is not ready for image capture".into());
        }
        let generation = self
            .current
            .read()
            .map_err(|_| "direct-state generation slot is poisoned".to_string())?
            .clone()
            .ok_or_else(|| "direct-state generation is not initialized".to_string())?;
        self.validate_identity(&generation.authority_identity)?;
        Ok(StateImageWritePermit {
            authority_identity: self.identity.clone(),
            generation: Some(generation),
            _guard: guard,
        })
    }

    /// Enter a fail-closed aggregate install/recovery epoch. Readiness is closed
    /// before this non-Clone permit becomes observable and remains closed if it is
    /// dropped on any failure path.
    pub async fn begin_install(&self) -> StateImageInstallPermit {
        let write = StateImageWritePermit {
            authority_identity: self.identity.clone(),
            generation: None,
            _guard: self.gate.clone().write_owned().await,
        };
        self.ready.store(false, Ordering::Release);
        StateImageInstallPermit { write }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn authority_epoch(&self) -> Option<u64> {
        self.is_ready()
            .then(|| self.authority_epoch.load(Ordering::Acquire))
    }

    pub(super) fn validate_current_successor(
        &self,
        candidate: &OpenedCurrentFence,
    ) -> Result<(), String> {
        let opened = self
            .opened_current
            .read()
            .map_err(|_| "direct-state opened Current fence is poisoned".to_string())?;
        let Some(previous) = opened.as_ref() else {
            return Ok(());
        };
        if candidate == previous {
            return Ok(());
        }
        candidate.validate_successor_of(previous)
    }
}

impl Default for StateImageAuthority {
    fn default() -> Self {
        Self::new()
    }
}

pub(super) struct StateImageReadPermit {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    pub(super) _guard: OwnedRwLockReadGuard<()>,
}

pub struct StateImageWritePermit {
    pub(super) authority_identity: Arc<StateImageAuthorityIdentity>,
    /// Exact Current generation pinned while this capture permit owns the gate.
    /// Install permits deliberately carry `None`: recovery must use journal-bound
    /// provider state, never a live Current value.
    pub(super) generation: Option<Arc<DirectStateGeneration>>,
    pub(super) _guard: OwnedRwLockWriteGuard<()>,
}

pub struct StateImageInstallPermit {
    pub(super) write: StateImageWritePermit,
}

impl std::ops::Deref for StateImageInstallPermit {
    type Target = StateImageWritePermit;

    fn deref(&self) -> &Self::Target {
        &self.write
    }
}

impl StateImageReadPermit {
    pub(super) fn validate_affinity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.authority_identity, expected) {
            Ok(())
        } else {
            Err("state-image read permit belongs to a different authority".into())
        }
    }
}

impl StateImageWritePermit {
    pub(super) fn validate_affinity(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<(), String> {
        if Arc::ptr_eq(&self.authority_identity, expected) {
            Ok(())
        } else {
            Err("state-image write permit belongs to a different authority".into())
        }
    }

    pub(super) fn current_generation(
        &self,
        expected: &Arc<StateImageAuthorityIdentity>,
    ) -> Result<&DirectStateGeneration, String> {
        self.validate_affinity(expected)?;
        let generation = self
            .generation
            .as_deref()
            .ok_or_else(|| "install permit has no capturable Current generation".to_string())?;
        if !Arc::ptr_eq(&generation.authority_identity, expected) {
            return Err("capture generation belongs to another authority".into());
        }
        Ok(generation)
    }
}
