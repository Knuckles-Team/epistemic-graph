//! Descriptor-owned filesystem lifecycle for SQLite file transfer.

use eg_sqlite_format::Reader;
#[cfg(target_os = "linux")]
use rustix::fs::{self, AtFlags, FileType, FlockOperation, Mode, OFlags};
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};

const TRANSFER_ROOT_ENV: &str = "EPISTEMIC_GRAPH_SQLITE_TRANSFER_ROOT";

struct PinnedRoot {
    file: std::fs::File,
}

pub(super) struct ExportDestination {
    root: PinnedRoot,
    name: String,
}

pub(super) fn open_import(name: &str, max_bytes: u64) -> Result<Reader, String> {
    #[cfg(target_os = "linux")]
    return open_import_at(&transfer_root()?, logical_filename(name)?, max_bytes);
    #[cfg(not(target_os = "linux"))]
    {
        let _name = name;
        let _max_bytes = max_bytes;
        Err("SQLite file import requires descriptor containment support".to_string())
    }
}

pub(super) fn export_destination(name: &str) -> Result<ExportDestination, String> {
    Ok(ExportDestination {
        root: transfer_root()?,
        name: logical_filename(name)?.to_string(),
    })
}

pub(super) fn write_export<T>(
    destination: &ExportDestination,
    max_bytes: u64,
    write: impl FnOnce(&Path) -> Result<T, String>,
) -> Result<T, String> {
    let temp = create_private_export_temp(&destination.root)?;
    let result = write(&temp.writer_path()?)?;
    if pinned_file_len(&temp.file)? > max_bytes {
        return Err("SQLite export exceeds the configured size limit".to_string());
    }
    install_export(&temp, destination)?;
    Ok(result)
}

fn transfer_root() -> Result<PinnedRoot, String> {
    let configured = std::env::var_os(TRANSFER_ROOT_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            format!("SQLite file transfer is disabled; configure {TRANSFER_ROOT_ENV}")
        })?;
    open_root_path(&configured)
}

fn open_root_path(configured: &Path) -> Result<PinnedRoot, String> {
    open_root_path_after_check(configured, || {})
}

#[cfg(target_os = "linux")]
fn open_root_path_after_check(
    configured: &Path,
    after_check: impl FnOnce(),
) -> Result<PinnedRoot, String> {
    let before = fs::statat(fs::CWD, configured, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| "configured SQLite transfer root is unavailable".to_string())?;
    if FileType::from_raw_mode(before.st_mode) != FileType::Directory {
        return Err("configured SQLite transfer root must be a real directory".to_string());
    }
    after_check();
    let file = fs::open(
        configured,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(std::fs::File::from)
    .map_err(|_| "configured SQLite transfer root is unavailable".to_string())?;
    let opened = fs::fstat(&file)
        .map_err(|_| "configured SQLite transfer root is unavailable".to_string())?;
    let after = fs::statat(fs::CWD, configured, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| "configured SQLite transfer root changed during validation".to_string())?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::Directory
        || opened.st_mode & 0o077 != 0
        || (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        || (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
    {
        return Err("configured SQLite transfer root changed during validation".to_string());
    }
    Ok(PinnedRoot { file })
}

#[cfg(not(target_os = "linux"))]
fn open_root_path_after_check(
    _configured: &Path,
    _after_check: impl FnOnce(),
) -> Result<PinnedRoot, String> {
    Err("SQLite file transfer requires descriptor containment support".to_string())
}

fn logical_filename(value: &str) -> Result<&str, String> {
    let mut chars = value.chars();
    if value.is_empty()
        || value.len() > 255
        || chars.next().is_none_or(|c| !c.is_ascii_alphanumeric())
        || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        || !value.to_ascii_lowercase().ends_with(".db")
    {
        return Err("SQLite transfer name must be a bounded .db filename".to_string());
    }
    let mut components = Path::new(value).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err("SQLite transfer name must not contain a path".to_string());
    }
    Ok(value)
}

#[cfg(target_os = "linux")]
fn open_import_at(root: &PinnedRoot, name: &str, max_bytes: u64) -> Result<Reader, String> {
    open_import_at_after_metadata(root, name, max_bytes, || {})
}

#[cfg(target_os = "linux")]
fn open_import_at_after_metadata(
    root: &PinnedRoot,
    name: &str,
    max_bytes: u64,
    after_metadata: impl FnOnce(),
) -> Result<Reader, String> {
    let mut file = open_pinned_import(root, name)?;
    let metadata =
        fs::fstat(&file).map_err(|_| "SQLite import source is unavailable".to_string())?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        return Err("SQLite import source must be a regular file".to_string());
    }
    let initial_len = u64::try_from(metadata.st_size)
        .map_err(|_| "SQLite import source has an invalid size".to_string())?;
    if initial_len > max_bytes {
        return Err("SQLite import source exceeds the configured size limit".to_string());
    }
    after_metadata();
    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| "read SQLite import source failed".to_string())?;
    if bytes.len() as u64 > max_bytes {
        return Err("SQLite import source exceeds the configured size limit".to_string());
    }
    Reader::from_bytes(bytes).map_err(|_| "open SQLite import source failed".to_string())
}

#[cfg(target_os = "linux")]
fn open_pinned_import(root: &PinnedRoot, name: &str) -> Result<std::fs::File, String> {
    fs::openat(
        &root.file,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(std::fs::File::from)
    .map_err(|_| "SQLite import source does not exist".to_string())
}

#[cfg(target_os = "linux")]
fn pinned_file_len(file: &std::fs::File) -> Result<u64, String> {
    let size = fs::fstat(file)
        .map_err(|_| "inspect SQLite export failed".to_string())?
        .st_size;
    u64::try_from(size).map_err(|_| "SQLite export has an invalid size".to_string())
}

#[cfg(not(target_os = "linux"))]
fn pinned_file_len(_: &std::fs::File) -> Result<u64, String> {
    Err("SQLite file export requires descriptor containment support".to_string())
}

struct ExportTempGuard {
    file: std::fs::File,
}

impl ExportTempGuard {
    fn writer_path(&self) -> Result<PathBuf, String> {
        #[cfg(target_os = "linux")]
        return Ok(PathBuf::from(format!(
            "/proc/self/fd/{}",
            std::os::fd::AsRawFd::as_raw_fd(&self.file)
        )));
        #[cfg(not(target_os = "linux"))]
        Err("SQLite file export requires descriptor containment support".to_string())
    }
}

fn create_private_export_temp(root: &PinnedRoot) -> Result<ExportTempGuard, String> {
    #[cfg(target_os = "linux")]
    {
        let file = fs::openat(
            &root.file,
            ".",
            OFlags::RDWR | OFlags::TMPFILE | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )
        .map(std::fs::File::from)
        .map_err(|_| "create private SQLite export failed".to_string())?;
        Ok(ExportTempGuard { file })
    }
    #[cfg(not(target_os = "linux"))]
    Err("SQLite file export requires descriptor containment support".to_string())
}

#[cfg(target_os = "linux")]
fn install_export(temp: &ExportTempGuard, destination: &ExportDestination) -> Result<(), String> {
    install_export_after_sidecar_check(temp, destination, || {})
}

#[cfg(target_os = "linux")]
fn install_export_after_sidecar_check(
    temp: &ExportTempGuard,
    destination: &ExportDestination,
    after_sidecar_check: impl FnOnce(),
) -> Result<(), String> {
    fs::fchmod(&temp.file, Mode::from_raw_mode(0o600))
        .map_err(|_| "secure SQLite export permissions failed".to_string())?;
    // The pinned root descriptor owns this advisory lock until the destination
    // drops, including every error path and process termination. Cooperating
    // exporters therefore cannot interleave sidecar checks and publication.
    fs::flock(
        &destination.root.file,
        FlockOperation::NonBlockingLockExclusive,
    )
    .map_err(|_| "another SQLite export is already in progress".to_string())?;
    for suffix in ["-wal", "-shm", "-journal"] {
        match fs::statat(
            &destination.root.file,
            format!("{}{suffix}", destination.name),
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Err(rustix::io::Errno::NOENT) => continue,
            Err(_) => return Err("inspect existing SQLite export failed".to_string()),
            Ok(_) => {
                return Err("existing SQLite export has active sidecar files".to_string());
            }
        }
    }
    after_sidecar_check();
    link_pinned_file(&temp.file, &destination.root.file, &destination.name)
}

#[cfg(not(target_os = "linux"))]
fn install_export(_: &ExportTempGuard, _: &ExportDestination) -> Result<(), String> {
    Err("SQLite file export requires descriptor containment support".to_string())
}

#[cfg(target_os = "linux")]
fn link_pinned_file(file: &std::fs::File, root: &std::fs::File, name: &str) -> Result<(), String> {
    let source = format!("/proc/self/fd/{}", file.as_raw_fd());
    match fs::linkat(fs::CWD, source, root, name, AtFlags::SYMLINK_FOLLOW) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::EXIST) => {
            Err("SQLite export destination already exists".to_string())
        }
        _ => Err("install SQLite export failed".to_string()),
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(super) fn test_destination(path: &Path) -> ExportDestination {
    let parent = path.parent().unwrap();
    ExportDestination {
        root: PinnedRoot {
            file: std::fs::File::from(
                fs::open(
                    parent,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .unwrap(),
            ),
        },
        name: path.file_name().unwrap().to_str().unwrap().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_rendezvous::{join_bounded, meet};
    #[cfg(target_os = "linux")]
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(target_os = "linux")]
    fn unique_test_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let root = std::env::temp_dir().join(format!("eg_sqlite_fs_{pid}_{nonce}"));
        create_dir(&root, 0o700);
        root
    }

    #[cfg(target_os = "linux")]
    fn create_dir(path: &Path, mode: u32) {
        fs::mkdir(path, Mode::from_raw_mode(mode)).unwrap();
    }

    #[cfg(target_os = "linux")]
    fn write_bytes(path: &Path, bytes: &[u8]) {
        let mut file = std::fs::File::from(
            fs::open(
                path,
                OFlags::WRONLY | OFlags::CREATE | OFlags::TRUNC | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )
            .unwrap(),
        );
        file.write_all(bytes).unwrap();
    }

    #[cfg(target_os = "linux")]
    fn read_bytes(path: &Path) -> Vec<u8> {
        let mut file = std::fs::File::from(
            fs::open(path, OFlags::RDONLY | OFlags::CLOEXEC, Mode::empty()).unwrap(),
        );
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        bytes
    }

    #[cfg(target_os = "linux")]
    fn expect_import_error(result: Result<Reader, String>) -> String {
        match result {
            Ok(_) => panic!("SQLite import unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_no_sidecars(destination: &Path) {
        for suffix in ["-wal", "-shm", "-journal"] {
            let sidecar = PathBuf::from(format!("{}{suffix}", destination.display()));
            assert!(matches!(
                fs::statat(fs::CWD, sidecar, AtFlags::SYMLINK_NOFOLLOW),
                Err(rustix::io::Errno::NOENT)
            ));
        }
    }

    #[test]
    fn transfer_name_rejects_paths_and_non_database_files() {
        assert_eq!(logical_filename("snapshot.db").unwrap(), "snapshot.db");
        for invalid in [
            "../snapshot.db",
            "nested/snapshot.db",
            "/tmp/snapshot.db",
            "C:\\temp\\snapshot.db",
            ".hidden.db",
            "snapshot.sqlite",
            "snapshot.db\n",
            "",
        ] {
            assert!(logical_filename(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn root_replacement_between_check_and_open_fails_closed() {
        use std::sync::{Arc, Barrier};

        let test_root = unique_test_root();
        let root = test_root.join("root");
        let moved = test_root.join("moved-root");
        create_dir(&root, 0o700);
        let barrier = Arc::new(Barrier::new(2));
        let (other_barrier, other_root, other_moved) =
            (Arc::clone(&barrier), root.clone(), moved.clone());
        let attacker = std::thread::spawn(move || {
            meet(
                &other_barrier,
                "root-replacement race: attacker at the swap point",
            );
            fs::renameat(fs::CWD, &other_root, fs::CWD, &other_moved).unwrap();
            create_dir(&other_root, 0o777);
            meet(
                &other_barrier,
                "root-replacement race: attacker finished the swap",
            );
        });
        let result = open_root_path_after_check(&root, || {
            meet(
                &barrier,
                "root-replacement race: checked root, swap may begin",
            );
            meet(
                &barrier,
                "root-replacement race: waiting for the swap to land",
            );
        });
        join_bounded(attacker, "the root-replacement attacker thread");
        assert!(result.is_err());
        fs::rmdir(&root).unwrap();
        fs::rmdir(&moved).unwrap();
        fs::rmdir(&test_root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn import_rejects_symlink_escape_and_enforces_pinned_read_bound() {
        let test_root = unique_test_root();
        let outside = test_root.join("outside.db");
        let root = test_root.join("transfer-root");
        create_dir(&root, 0o700);
        let pinned_root = open_root_path(&root).unwrap();
        write_bytes(&outside, &[0_u8; 32]);
        let link = root.join("escape.db");
        fs::symlinkat(&outside, &pinned_root.file, "escape.db").unwrap();
        assert!(open_import_at(&pinned_root, "escape.db", 64).is_err());
        let bounded = root.join("bounded.db");
        write_bytes(&bounded, &[0_u8; 9]);
        assert_eq!(
            expect_import_error(open_import_at(&pinned_root, "bounded.db", 8)),
            "SQLite import source exceeds the configured size limit"
        );
        let growing = root.join("growing.db");
        write_bytes(&growing, &[0_u8; 8]);
        assert_eq!(
            expect_import_error(open_import_at_after_metadata(
                &pinned_root,
                "growing.db",
                8,
                || write_bytes(&growing, &[0_u8; 9]),
            )),
            "SQLite import source exceeds the configured size limit"
        );
        let fifo = root.join("fifo.db");
        fs::mkfifoat(
            &pinned_root.file,
            fifo.file_name().unwrap(),
            Mode::from_raw_mode(0o600),
        )
        .unwrap();
        assert!(open_import_at(&pinned_root, "fifo.db", 8).is_err());
        for path in [&link, &bounded, &growing, &fifo, &outside] {
            fs::unlink(path).unwrap();
        }
        fs::rmdir(&root).unwrap();
        fs::rmdir(&test_root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_install_preserves_existing_database_and_temp() {
        let test_root = unique_test_root();
        let destination = test_root.join("destination.db");
        let sidecar = PathBuf::from(format!("{}-wal", destination.display()));
        write_bytes(&destination, b"old database");
        write_bytes(&sidecar, b"active wal");
        let target = test_destination(&destination);
        let temp = create_private_export_temp(&target.root).unwrap();
        write_bytes(&temp.writer_path().unwrap(), b"new database");
        assert_eq!(
            install_export(&temp, &target).unwrap_err(),
            "existing SQLite export has active sidecar files"
        );
        assert_eq!(read_bytes(&destination), b"old database");
        assert_eq!(read_bytes(&sidecar), b"active wal");
        assert_eq!(read_bytes(&temp.writer_path().unwrap()), b"new database");
        fs::unlink(&destination).unwrap();
        fs::unlink(&sidecar).unwrap();
        assert_no_sidecars(&destination);
        fs::rmdir(&test_root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn root_descriptor_lock_serializes_cooperating_exporters() {
        let test_root = unique_test_root();
        let destination = test_root.join("destination.db");
        let first = test_destination(&destination);
        let second = test_destination(&destination.with_extension("other.db"));
        fs::flock(&first.root.file, FlockOperation::NonBlockingLockExclusive).unwrap();
        let temp = create_private_export_temp(&second.root).unwrap();
        assert_eq!(
            install_export(&temp, &second).unwrap_err(),
            "another SQLite export is already in progress"
        );
        drop(temp);
        drop(second);
        drop(first);
        fs::rmdir(&test_root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn existing_main_is_never_surrounded_by_synthetic_sidecars() {
        let test_root = unique_test_root();
        let destination = test_root.join("destination.db");
        write_bytes(&destination, b"healthy");
        let target = test_destination(&destination);
        let temp = create_private_export_temp(&target.root).unwrap();
        write_bytes(&temp.writer_path().unwrap(), b"replacement");

        assert_eq!(
            install_export(&temp, &target).unwrap_err(),
            "SQLite export destination already exists"
        );
        assert_eq!(read_bytes(&destination), b"healthy");
        assert_no_sidecars(&destination);
        fs::unlink(&destination).unwrap();
        fs::rmdir(&test_root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn install_atomically_refuses_a_destination_created_at_the_barrier() {
        use std::sync::{Arc, Barrier};

        let test_root = unique_test_root();
        let destination = test_root.join("destination.db");
        let target = test_destination(&destination);
        let temp = create_private_export_temp(&target.root).unwrap();
        write_bytes(&temp.writer_path().unwrap(), b"pinned");
        let barrier = Arc::new(Barrier::new(2));
        let other_barrier = Arc::clone(&barrier);
        let other_path = destination.clone();
        let attacker = std::thread::spawn(move || {
            meet(
                &other_barrier,
                "destination-creation race: attacker at the write point",
            );
            write_bytes(&other_path, b"other");
            meet(
                &other_barrier,
                "destination-creation race: attacker finished the write",
            );
        });
        let result = install_export_after_sidecar_check(&temp, &target, || {
            meet(
                &barrier,
                "destination-creation race: sidecar checked, write may begin",
            );
            meet(
                &barrier,
                "destination-creation race: waiting for the write to land",
            );
        });
        join_bounded(attacker, "the destination-creation attacker thread");
        assert_eq!(
            result.unwrap_err(),
            "SQLite export destination already exists"
        );
        assert_eq!(read_bytes(&destination), b"other");
        assert_eq!(read_bytes(&temp.writer_path().unwrap()), b"pinned");
        assert_no_sidecars(&destination);
        fs::unlink(&destination).unwrap();
        fs::rmdir(&test_root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn successful_install_links_the_exact_anonymous_inode_at_mode_0600() {
        let test_root = unique_test_root();
        let destination = test_root.join("destination.db");
        let target = test_destination(&destination);
        let temp = create_private_export_temp(&target.root).unwrap();
        let before = fs::fstat(&temp.file).unwrap();
        assert_eq!(before.st_nlink, 0, "temporary inode must be anonymous");
        write_bytes(&temp.writer_path().unwrap(), b"published");
        install_export(&temp, &target).unwrap();
        let installed = fs::statat(fs::CWD, &destination, AtFlags::SYMLINK_NOFOLLOW).unwrap();
        assert_eq!(
            (installed.st_dev, installed.st_ino),
            (before.st_dev, before.st_ino)
        );
        assert_eq!(installed.st_mode & 0o777, 0o600);
        assert_eq!(read_bytes(&destination), b"published");
        assert_no_sidecars(&destination);
        fs::unlink(&destination).unwrap();
        fs::rmdir(&test_root).unwrap();
    }
}
