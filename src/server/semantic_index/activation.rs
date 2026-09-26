//! RF-019 prestart handoff. The root-owned helper verifies signed claims and
//! EG's offline migration observation before the server takes engine.lock.
//! After acquiring the lock the server rechecks owner bytes and installs only
//! an in-memory, exact-tenant grant. A missing helper/receipt/grant never
//! opens the v3 route.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eg_core::compute::semantic_index_service::SemanticIndexService;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{
    semantic_owner_principal, semantic_registry, semantic_server_secrets,
    tenant_semantic_owner_file, upgrade, TenantScopedSemanticVerifier,
};

const ROOT: &str = "/etc/agent-utilities/rf019";
const MANIFEST: &str = "/etc/agent-utilities/rf019/activation-manifest.json";
const AUTHORITY: &str = "/etc/agent-utilities/rf019/server-preflight-authority.json";
const HELPER: &str = "/usr/local/bin/rf019-activation-preflight";
const MAX_HELPER_BYTES: u64 = 16 * 1024 * 1024;
const MAX_HANDOFF_BYTES: usize = 128 * 1024;
const MAX_OWNER_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperAuthority {
    schema: String,
    helper_sha256: String,
    persist_dir: PathBuf,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    tenant_id: String,
    binding_ids: Vec<String>,
    owner_path: PathBuf,
    owner_device: u64,
    owner_inode: u64,
    owner_size: u64,
    owner_sha256: String,
    owner_layout_sha256: String,
    source_census_sha256: String,
    migration_proof_sha256: String,
    claim_payload_digest: String,
    expires_at_unix: u64,
    implementation_receipt_sha256: String,
    runtime_receipt_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Handoff {
    schema: String,
    grants: Vec<Grant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptPins {
    schema: String,
    implementation_receipt_sha256: String,
    runtime_receipt_sha256: String,
}

/// A startup-only value created from the fixed helper's successful output.
/// Its fields are private so no request or caller can create a serving grant.
pub struct PendingRf019Activation {
    grants: Vec<Grant>,
    manifest_sha256: Option<[u8; 32]>,
}

static ACTIVATED: OnceLock<Mutex<HashMap<String, Grant>>> = OnceLock::new();
static OPENED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();

fn activated() -> &'static Mutex<HashMap<String, Grant>> {
    ACTIVATED.get_or_init(|| Mutex::new(HashMap::new()))
}

fn opened() -> &'static Mutex<HashSet<(String, String)>> {
    OPENED.get_or_init(|| Mutex::new(HashSet::new()))
}

fn digest_file(path: &Path, max_bytes: u64) -> Result<([u8; 32], u64, u64, u64), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        const O_NOFOLLOW: i32 = 0o400000;
        let named = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
        let mut file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW)
            .open(path)
            .map_err(|error| error.to_string())?;
        let before = file.metadata().map_err(|error| error.to_string())?;
        if !named.file_type().is_file()
            || !before.is_file()
            || before.nlink() != 1
            || named.nlink() != 1
            || before.len() > max_bytes
            || (named.dev(), named.ino()) != (before.dev(), before.ino())
        {
            return Err("RF-019 owner or helper file identity refused".to_string());
        }
        let mut sha = Sha256::new();
        let mut bytes = 0_u64;
        let mut block = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut block).map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            bytes = bytes
                .checked_add(read as u64)
                .ok_or_else(|| "RF-019 byte budget overflowed".to_string())?;
            if bytes > max_bytes {
                return Err("RF-019 file exceeded its byte budget".to_string());
            }
            sha.update(&block[..read]);
        }
        let after = file.metadata().map_err(|error| error.to_string())?;
        let named_after = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
        if bytes != before.len()
            || (
                before.dev(),
                before.ino(),
                before.len(),
                before.mtime(),
                before.mtime_nsec(),
            ) != (
                after.dev(),
                after.ino(),
                after.len(),
                after.mtime(),
                after.mtime_nsec(),
            )
            || (after.dev(), after.ino(), after.len(), after.nlink())
                != (
                    named_after.dev(),
                    named_after.ino(),
                    named_after.len(),
                    named_after.nlink(),
                )
        {
            return Err("RF-019 file changed during observation".to_string());
        }
        Ok((sha.finalize().into(), before.dev(), before.ino(), bytes))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, max_bytes);
        Err("RF-019 activation requires no-follow Unix files".to_string())
    }
}

fn fixed_authority(persist_dir: &Path) -> Result<[u8; 32], String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        for directory in [
            "/etc",
            "/etc/agent-utilities",
            ROOT,
            "/usr",
            "/usr/local",
            "/usr/local/bin",
        ] {
            let metadata = fs::symlink_metadata(directory).map_err(|error| error.to_string())?;
            if !metadata.file_type().is_dir()
                || metadata.uid() != 0
                || metadata.permissions().mode() & 0o022 != 0
                || fs::canonicalize(directory).map_err(|error| error.to_string())?
                    != Path::new(directory)
            {
                return Err("RF-019 fixed trust path is not root-owned and canonical".to_string());
            }
        }
        let root = fs::symlink_metadata(ROOT).map_err(|error| error.to_string())?;
        let config = fs::symlink_metadata(AUTHORITY).map_err(|error| error.to_string())?;
        let manifest = fs::symlink_metadata(MANIFEST).map_err(|error| error.to_string())?;
        let helper = fs::symlink_metadata(HELPER).map_err(|error| error.to_string())?;
        let helper_parent =
            fs::symlink_metadata("/usr/local/bin").map_err(|error| error.to_string())?;
        if !root.file_type().is_dir()
            || root.permissions().mode() & 0o777 != 0o700
            || root.uid() != 0
            || !config.file_type().is_file()
            || config.permissions().mode() & 0o777 != 0o600
            || config.uid() != 0
            || config.nlink() != 1
            || !manifest.file_type().is_file()
            || manifest.permissions().mode() & 0o777 != 0o600
            || manifest.uid() != 0
            || manifest.nlink() != 1
            || !helper.file_type().is_file()
            || helper.uid() != 0
            || helper.permissions().mode() & 0o111 == 0
            || helper.permissions().mode() & 0o022 != 0
            || !helper_parent.file_type().is_dir()
            || helper_parent.uid() != 0
            || helper_parent.permissions().mode() & 0o022 != 0
        {
            return Err("RF-019 fixed preflight authority ownership refused".to_string());
        }
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0o400000;
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW)
            .open(AUTHORITY)
            .map_err(|error| error.to_string())?;
        let mut raw = Vec::new();
        file.take(4097)
            .read_to_end(&mut raw)
            .map_err(|error| error.to_string())?;
        if raw.len() > 4096 {
            return Err("RF-019 fixed preflight authority exceeds bound".to_string());
        }
        let authority: HelperAuthority = serde_json::from_slice(&raw)
            .map_err(|_| "RF-019 fixed preflight authority is invalid".to_string())?;
        if authority.schema != "rf019-server-preflight-authority/v1"
            || authority.persist_dir != persist_dir
            || !hex_digest(&authority.helper_sha256)
        {
            return Err("RF-019 fixed preflight authority does not bind this store".to_string());
        }
        let (digest, _, _, _) = digest_file(Path::new(HELPER), MAX_HELPER_BYTES)?;
        if hex::encode(digest) != authority.helper_sha256 {
            return Err("RF-019 fixed preflight helper digest changed".to_string());
        }
        Ok(digest)
    }
    #[cfg(not(unix))]
    {
        let _ = persist_dir;
        Err("RF-019 activation requires Unix fixed authority".to_string())
    }
}

fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn token(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(byte))
}

fn run_fixed_helper(args: &[&str]) -> Result<Vec<u8>, String> {
    fn bounded_pipe<R: Read + Send + 'static>(
        mut pipe: R,
        limit: usize,
        overflow: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<Result<Vec<u8>, String>> {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            pipe.by_ref()
                .take((limit + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(|error| error.to_string())?;
            if bytes.len() > limit {
                overflow.store(true, Ordering::Release);
            }
            Ok(bytes)
        })
    }

    let mut child = Command::new(HELPER)
        .args(args)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout = bounded_pipe(
        child
            .stdout
            .take()
            .ok_or("RF-019 helper stdout unavailable")?,
        MAX_HANDOFF_BYTES,
        Arc::clone(&overflow),
    );
    let stderr = bounded_pipe(
        child
            .stderr
            .take()
            .ok_or("RF-019 helper stderr unavailable")?,
        1024,
        Arc::clone(&overflow),
    );
    let started = Instant::now();
    let status = loop {
        if overflow.load(Ordering::Acquire) || started.elapsed() >= PREFLIGHT_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            // Do not join a pipe reader held open by an orphaned descendant.
            // The startup error exits this process without publishing a grant.
            return Err("RF-019 fixed preflight helper exceeded resource limits".to_string());
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("RF-019 fixed preflight helper wait failed".to_string());
            }
        }
    };
    while !stdout.is_finished() || !stderr.is_finished() {
        if started.elapsed() >= PREFLIGHT_TIMEOUT {
            return Err("RF-019 fixed preflight pipe remained open".to_string());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let stdout = stdout
        .join()
        .map_err(|_| "RF-019 helper stdout reader failed".to_string())??;
    let stderr = stderr
        .join()
        .map_err(|_| "RF-019 helper stderr reader failed".to_string())??;
    if !status.success() || !stderr.is_empty() || stdout.len() > MAX_HANDOFF_BYTES {
        return Err("RF-019 fixed preflight helper refused".to_string());
    }
    Ok(stdout)
}

/// Run before the process acquires engine.lock. With no fixed activation
/// manifest, normal v2 startup continues and all v3 owners stay fenced.
pub fn preflight(persist_dir: &Path) -> Result<PendingRf019Activation, String> {
    match fs::symlink_metadata(MANIFEST) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PendingRf019Activation {
                grants: Vec::new(),
                manifest_sha256: None,
            });
        }
        Err(error) => return Err(error.to_string()),
        Ok(_) => {}
    }
    let canonical = fs::canonicalize(persist_dir).map_err(|error| error.to_string())?;
    if canonical != persist_dir {
        return Err("RF-019 persist root is not canonical".to_string());
    }
    let manifest_sha256 = digest_file(Path::new(MANIFEST), 8192)?.0;
    let helper_sha256 = fixed_authority(persist_dir)?;
    let output = run_fixed_helper(&[])?;
    if digest_file(Path::new(HELPER), MAX_HELPER_BYTES)?.0 != helper_sha256
        || digest_file(Path::new(MANIFEST), 8192)?.0 != manifest_sha256
    {
        return Err("RF-019 preflight authority changed during verification".to_string());
    }
    let handoff: Handoff = serde_json::from_slice(&output)
        .map_err(|_| "RF-019 preflight handoff is malformed".to_string())?;
    if handoff.schema != "rf019-server-activation-handoff/v1"
        || handoff.grants.is_empty()
        || handoff.grants.len() > 32
    {
        return Err("RF-019 preflight handoff has invalid cardinality".to_string());
    }
    Ok(PendingRf019Activation {
        grants: handoff.grants,
        manifest_sha256: Some(manifest_sha256),
    })
}

/// Recheck exact owner facts after startup owns engine.lock, then install only
/// these in-memory tenant grants. No caller-provided observation is accepted.
pub fn install(pending: PendingRf019Activation, persist_dir: &Path) -> Result<(), String> {
    if pending.grants.is_empty() {
        return Ok(());
    }
    if digest_file(Path::new(MANIFEST), 8192)?.0 != pending.manifest_sha256.unwrap_or_default() {
        return Err("RF-019 activation manifest changed before lock".to_string());
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_secs();
    let mut checked = HashMap::new();
    let mut receipt_pair: Option<(String, String)> = None;
    let mut claim_digests = HashSet::new();
    for grant in pending.grants {
        if !token(&grant.tenant_id)
            || grant.binding_ids.is_empty()
            || grant.binding_ids.len() > 32
            || grant.binding_ids.iter().any(|binding| !token(binding))
            || grant.binding_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || grant.owner_path != tenant_semantic_owner_file(persist_dir, &grant.tenant_id)
            || grant.expires_at_unix < now
            || !hex_digest(&grant.owner_sha256)
            || !hex_digest(&grant.owner_layout_sha256)
            || !hex_digest(&grant.source_census_sha256)
            || !hex_digest(&grant.migration_proof_sha256)
            || !hex_digest(&grant.claim_payload_digest)
            || !hex_digest(&grant.implementation_receipt_sha256)
            || !hex_digest(&grant.runtime_receipt_sha256)
        {
            return Err("RF-019 activation grant identity or freshness refused".to_string());
        }
        let pair = (
            grant.implementation_receipt_sha256.clone(),
            grant.runtime_receipt_sha256.clone(),
        );
        if receipt_pair
            .as_ref()
            .is_some_and(|expected| expected != &pair)
            || !claim_digests.insert(grant.claim_payload_digest.clone())
        {
            return Err("RF-019 activation grants disagree on signed authority".to_string());
        }
        receipt_pair.get_or_insert(pair);
        let native = upgrade::observe_promoted_tenant_owner_under_existing_lease(
            persist_dir,
            &grant.tenant_id,
            &grant.binding_ids,
            MAX_OWNER_BYTES,
            MAX_OWNER_BYTES,
        )?;
        if native.owner_path != grant.owner_path
            || native.owner_device != grant.owner_device
            || native.owner_inode != grant.owner_inode
            || native.owner_size != grant.owner_size
            || native.owner_sha256 != grant.owner_sha256
            || native.owner_layout_sha256 != grant.owner_layout_sha256
            || native.source_census_sha256 != grant.source_census_sha256
            || native.migration_proof_sha256 != grant.migration_proof_sha256
        {
            return Err("RF-019 source or migration proof changed before serving lock".to_string());
        }
        let (digest, device, inode, size) = digest_file(&grant.owner_path, MAX_OWNER_BYTES)?;
        if (device, inode, size, hex::encode(digest))
            != (
                grant.owner_device,
                grant.owner_inode,
                grant.owner_size,
                grant.owner_sha256.clone(),
            )
        {
            return Err("RF-019 v3 owner changed before serving lock".to_string());
        }
        if checked.insert(grant.tenant_id.clone(), grant).is_some() {
            return Err("RF-019 activation tenant is duplicated".to_string());
        }
    }
    // This helper mode reruns only the fixed global checker, never EG's
    // offline observer, so it can run while the server owns engine.lock.
    let helper_digest = fixed_authority(persist_dir)?;
    let raw_pins = run_fixed_helper(&["--receipt-pins-only"])?;
    if digest_file(Path::new(HELPER), MAX_HELPER_BYTES)?.0 != helper_digest {
        return Err("RF-019 receipt checker changed under engine lock".to_string());
    }
    let pins: ReceiptPins = serde_json::from_slice(&raw_pins)
        .map_err(|_| "RF-019 current receipt pins are malformed".to_string())?;
    if pins.schema != "rf019-current-receipt-pins/v1"
        || Some((
            pins.implementation_receipt_sha256,
            pins.runtime_receipt_sha256,
        )) != receipt_pair
    {
        return Err("RF-019 global signed receipts changed before serving lock".to_string());
    }
    let mut active = activated()
        .lock()
        .map_err(|_| "RF-019 activation registry unavailable")?;
    if !active.is_empty() {
        return Err("RF-019 activation registry already installed".to_string());
    }
    *active = checked;
    Ok(())
}

pub(super) fn has_grant(tenant: &str) -> Result<bool, String> {
    let active = activated()
        .lock()
        .map_err(|_| "RF-019 activation registry unavailable")?;
    Ok(active.contains_key(tenant))
}

pub(super) fn open_service(
    persist_dir: &Path,
    tenant: &str,
    binding_id: &str,
) -> Result<Arc<SemanticIndexService>, String> {
    let grant = {
        let active = activated()
            .lock()
            .map_err(|_| "RF-019 activation registry unavailable")?;
        active.get(tenant).cloned()
    }
    .ok_or_else(|| "RF-019 v3 tenant has no verified activation grant".to_string())?;
    if !grant
        .binding_ids
        .iter()
        .any(|binding| binding == binding_id)
        || grant.owner_path != tenant_semantic_owner_file(persist_dir, tenant)
    {
        return Err("RF-019 v3 binding is outside its verified activation".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata =
            fs::symlink_metadata(&grant.owner_path).map_err(|error| error.to_string())?;
        if !metadata.file_type().is_file()
            || metadata.nlink() != 1
            || (metadata.dev(), metadata.ino(), metadata.len())
                != (grant.owner_device, grant.owner_inode, grant.owner_size)
        {
            return Err("RF-019 v3 owner identity changed after activation".to_string());
        }
    }
    let key = (tenant.to_string(), binding_id.to_string());
    let mut registry = semantic_registry()
        .lock()
        .map_err(|_| "semantic index owner registry is unavailable".to_string())?;
    if let Some(existing) = registry.get(&key) {
        let opened = opened()
            .lock()
            .map_err(|_| "RF-019 activation registry unavailable")?;
        return if opened.contains(&key) {
            Ok(Arc::clone(existing))
        } else {
            Err("RF-019 v2 handle is already open for this binding".to_string())
        };
    }
    let (proof, _) = *semantic_server_secrets();
    let service = Arc::new(
        SemanticIndexService::open_tenant(
            &grant.owner_path,
            Arc::new(TenantScopedSemanticVerifier {
                tenant: tenant.to_string(),
                proof,
            }),
            semantic_owner_principal(),
            &proof,
            tenant,
            binding_id,
        )
        .map_err(|error| error.to_string())?,
    );
    registry.insert(key.clone(), Arc::clone(&service));
    opened()
        .lock()
        .map_err(|_| "RF-019 activation registry unavailable")?
        .insert(key);
    Ok(service)
}
