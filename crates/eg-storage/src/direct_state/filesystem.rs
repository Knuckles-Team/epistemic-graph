use super::{contract::*, *};

pub(super) struct PinnedPrivateDirectory {
    pub(super) path: PathBuf,
    pub(super) authority: File,
}

impl PinnedPrivateDirectory {
    pub(super) fn open(path: &Path) -> Result<Self, String> {
        ensure_private_directory(path)?;
        let authority = open_directory_nofollow(path, "open direct-state private root")?;
        let pinned = Self {
            path: path.to_path_buf(),
            authority,
        };
        pinned.validate_live("direct-state private root")?;
        Ok(pinned)
    }

    pub(super) fn validate_live(&self, label: &str) -> Result<(), String> {
        validate_same_file_identity(
            &self.authority,
            &open_directory_nofollow(&self.path, &format!("open {label}"))?,
            label,
        )
    }

    pub(super) fn try_clone_token(&self) -> Result<Self, String> {
        Ok(Self {
            path: self.path.clone(),
            authority: self
                .authority
                .try_clone()
                .map_err(|error| format!("clone direct-state private root: {error}"))?,
        })
    }

    pub(super) fn validate_path<'a>(&self, path: &'a Path, label: &str) -> Result<&'a str, String> {
        let parent = path
            .parent()
            .ok_or_else(|| format!("{label} has no parent directory"))?;
        if parent != self.path {
            return Err(format!("{label} is outside its registered private root"));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("{label} name is not portable"))?;
        validate_relative_basename(name)?;
        Ok(name)
    }

    pub(super) fn validate_file(
        &self,
        path: &Path,
        expected: &File,
        label: &str,
    ) -> Result<(), String> {
        let name = self.validate_path(path, label)?;
        let actual = self.reader().open_regular(name, &format!("open {label}"))?;
        validate_same_file_identity(expected, &actual, label)
    }

    pub(super) fn reader(&self) -> PinnedDirectoryRead<'_> {
        PinnedDirectoryRead { root: self }
    }

    pub(super) fn mutations(&self) -> PinnedDirectoryMutations<'_> {
        PinnedDirectoryMutations { root: self }
    }

    pub(super) fn collector(&self) -> PinnedDirectoryCollector<'_> {
        PinnedDirectoryCollector { root: self }
    }

    pub(super) fn sync(&self) -> Result<(), String> {
        self.authority
            .sync_all()
            .map_err(|error| format!("fsync direct-state private root: {error}"))
    }
}

pub(super) struct PinnedDirectoryRead<'a> {
    root: &'a PinnedPrivateDirectory,
}

impl PinnedDirectoryRead<'_> {
    pub(super) fn descriptor_root_path(&self) -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", self.root.authority.as_raw_fd()))
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            use std::os::fd::AsRawFd;
            return PathBuf::from(format!("/dev/fd/{}", self.root.authority.as_raw_fd()));
        }
        #[cfg(not(unix))]
        {
            PathBuf::new()
        }
    }

    pub(super) fn descriptor_path(&self, name: &str) -> Result<PathBuf, String> {
        validate_relative_basename(name)?;
        #[cfg(unix)]
        return Ok(self.descriptor_root_path().join(name));
        #[cfg(not(unix))]
        Err("direct-state descriptor paths are unsupported on this platform".into())
    }

    pub(super) fn open_regular(&self, name: &str, operation: &str) -> Result<File, String> {
        #[cfg(unix)]
        {
            validate_relative_basename(name)?;
            let fd = openat(
                &self.root.authority,
                name,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|error| format!("{operation}: {error}"))?;
            let file = File::from(fd);
            if !file
                .metadata()
                .map_err(|error| format!("{operation} metadata: {error}"))?
                .is_file()
            {
                return Err(format!("{operation}: source is not a regular file"));
            }
            Ok(file)
        }
        #[cfg(not(unix))]
        {
            let _ = (name, operation);
            Err("direct-state private files are unsupported on this platform".into())
        }
    }

    pub(super) fn open_optional_regular(
        &self,
        name: &str,
        operation: &str,
    ) -> Result<Option<File>, String> {
        #[cfg(unix)]
        {
            validate_relative_basename(name)?;
            match openat(
                &self.root.authority,
                name,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => {
                    let file = File::from(fd);
                    if !file
                        .metadata()
                        .map_err(|error| format!("{operation} metadata: {error}"))?
                        .is_file()
                    {
                        return Err(format!("{operation}: source is not a regular file"));
                    }
                    Ok(Some(file))
                }
                Err(rustix::io::Errno::NOENT) => Ok(None),
                Err(error) => Err(format!("{operation}: {error}")),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (name, operation);
            Err("direct-state private files are unsupported on this platform".into())
        }
    }
}

pub(super) struct PinnedDirectoryMutations<'a> {
    root: &'a PinnedPrivateDirectory,
}

impl PinnedDirectoryMutations<'_> {
    pub(super) fn create_new(&self, name: &str, operation: &str) -> Result<File, String> {
        #[cfg(unix)]
        {
            validate_relative_basename(name)?;
            let fd = openat(
                &self.root.authority,
                name,
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|error| format!("{operation}: {error}"))?;
            Ok(File::from(fd))
        }
        #[cfg(not(unix))]
        {
            let _ = (name, operation);
            Err("direct-state private files are unsupported on this platform".into())
        }
    }

    pub(super) fn create_new_optional(
        &self,
        name: &str,
        operation: &str,
    ) -> Result<Option<File>, String> {
        #[cfg(unix)]
        {
            validate_relative_basename(name)?;
            match openat(
                &self.root.authority,
                name,
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(fd) => Ok(Some(File::from(fd))),
                Err(rustix::io::Errno::EXIST) => Ok(None),
                Err(error) => Err(format!("{operation}: {error}")),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (name, operation);
            Err("direct-state private files are unsupported on this platform".into())
        }
    }

    pub(super) fn link_no_replace(
        &self,
        source_name: &str,
        target_root: &PinnedPrivateDirectory,
        target_name: &str,
        operation: &str,
    ) -> Result<(), String> {
        #[cfg(unix)]
        {
            validate_relative_basename(source_name)?;
            validate_relative_basename(target_name)?;
            linkat(
                &self.root.authority,
                source_name,
                &target_root.authority,
                target_name,
                AtFlags::empty(),
            )
            .map_err(|error| format!("{operation}: {error}"))?;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (source_name, target_root, target_name, operation);
            Err("direct-state private files are unsupported on this platform".into())
        }
    }

    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    pub(super) fn exchange(
        &self,
        first_name: &str,
        second_name: &str,
        operation: &str,
    ) -> Result<(), String> {
        validate_relative_basename(first_name)?;
        validate_relative_basename(second_name)?;
        renameat_with(
            &self.root.authority,
            first_name,
            &self.root.authority,
            second_name,
            RenameFlags::EXCHANGE,
        )
        .map_err(|error| format!("{operation}: {error}"))
    }

    #[cfg(not(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    )))]
    pub(super) fn exchange(
        &self,
        _first_name: &str,
        _second_name: &str,
        _operation: &str,
    ) -> Result<(), String> {
        Err("direct-state private files are unsupported on this platform".into())
    }

    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    pub(super) fn rename_no_replace(
        &self,
        source_name: &str,
        target_name: &str,
        operation: &str,
    ) -> Result<(), String> {
        validate_relative_basename(source_name)?;
        validate_relative_basename(target_name)?;
        renameat_with(
            &self.root.authority,
            source_name,
            &self.root.authority,
            target_name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| format!("{operation}: {error}"))
    }

    #[cfg(not(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    )))]
    pub(super) fn rename_no_replace(
        &self,
        _source_name: &str,
        _target_name: &str,
        _operation: &str,
    ) -> Result<(), String> {
        Err("direct-state private files are unsupported on this platform".into())
    }

    pub(super) fn unlink(&self, name: &str, operation: &str) -> Result<(), String> {
        #[cfg(unix)]
        {
            validate_relative_basename(name)?;
            unlinkat(&self.root.authority, name, AtFlags::empty())
                .map_err(|error| format!("{operation}: {error}"))?;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (name, operation);
            Err("direct-state private files are unsupported on this platform".into())
        }
    }

    /// Remove an unjournaled managed artifact without ever unlinking a pathname
    /// that may have been ABA-reused. The public name is atomically moved to a
    /// unique quarantine, the moved inode is compared with the pinned descriptor,
    /// and only that quarantine name is then removed.
    pub(super) fn retire_unjournaled_exact(
        &self,
        name: &str,
        expected: &File,
        label: &str,
    ) -> Result<(), String> {
        validate_relative_basename(name)?;
        let live = self
            .root
            .reader()
            .open_regular(name, &format!("authenticate live {label}"))?;
        validate_same_file_identity(expected, &live, label)?;
        let quarantine = format!(
            ".{name}.retired.{}.{}",
            std::process::id(),
            NEXT_DIRECT_STATE_TEMP.fetch_add(1, Ordering::Relaxed)
        );
        self.rename_no_replace(name, &quarantine, &format!("quarantine {label}"))?;
        let moved = self
            .root
            .reader()
            .open_regular(&quarantine, &format!("pin quarantined {label}"))?;
        validate_same_file_identity(expected, &moved, label)?;
        self.root.sync()?;
        self.unlink(&quarantine, &format!("remove quarantined {label}"))?;
        self.root.sync()
    }

    pub(super) fn write_temporary(
        &self,
        target_name: &str,
        purpose: &str,
        bytes: &[u8],
    ) -> Result<(File, String), String> {
        validate_relative_basename(target_name)?;
        let mut last_error = None;
        for _ in 0..32 {
            let ordinal = NEXT_DIRECT_STATE_TEMP.fetch_add(1, Ordering::Relaxed);
            let temporary = format!(
                ".{target_name}.{purpose}.{}.{}.tmp",
                std::process::id(),
                ordinal
            );
            match self.create_new_optional(&temporary, "create direct-state durable temporary")? {
                Some(mut file) => {
                    let result = file
                        .write_all(bytes)
                        .map_err(|error| format!("write direct-state temporary: {error}"))
                        .and_then(|_| {
                            file.sync_all()
                                .map_err(|error| format!("fsync direct-state temporary: {error}"))
                        });
                    if let Err(error) = result {
                        let _ = self.retire_unjournaled_exact(
                            &temporary,
                            &file,
                            "failed direct-state temporary",
                        );
                        return Err(error);
                    }
                    let authority = self
                        .root
                        .reader()
                        .open_regular(&temporary, "reopen direct-state temporary read-only")?;
                    validate_same_file_identity(
                        &file,
                        &authority,
                        "reopened direct-state temporary",
                    )?;
                    drop(file);
                    return Ok((authority, temporary));
                }
                None => last_error = Some("temporary already exists".to_string()),
            }
        }
        Err(format!(
            "allocate direct-state durable temporary: {}",
            last_error.unwrap_or_else(|| "exhausted unique names".into())
        ))
    }
}

pub(super) struct PinnedDirectoryCollector<'a> {
    root: &'a PinnedPrivateDirectory,
}

impl PinnedDirectoryCollector<'_> {
    pub(super) fn collect_temporaries(&self, target_name: &str) -> Result<(), String> {
        validate_relative_basename(target_name)?;
        self.collect_retired_residue(target_name)?;
        let prefix = format!(".{target_name}.");
        for (ordinal, entry) in std::fs::read_dir(self.root.reader().descriptor_root_path())
            .map_err(|error| format!("scan direct-state durable temporaries: {error}"))?
            .enumerate()
        {
            if ordinal >= MAX_DIRECT_STATE_GC_ENTRIES {
                return Err("direct-state durable directory exceeds its GC entry bound".into());
            }
            let entry =
                entry.map_err(|error| format!("read direct-state temporary entry: {error}"))?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "direct-state temporary name is not UTF-8".to_string())?;
            if !name.starts_with(&prefix) || !name.ends_with(".tmp") {
                continue;
            }
            let authority = self
                .root
                .reader()
                .open_regular(&name, "pin direct-state temporary residue")?;
            self.root.mutations().retire_unjournaled_exact(
                &name,
                &authority,
                "direct-state temporary residue",
            )?;
        }
        Ok(())
    }

    pub(super) fn collect_retired_residue(&self, target_name: &str) -> Result<(), String> {
        validate_relative_basename(target_name)?;
        let prefix = format!(".{target_name}.");
        for (ordinal, entry) in std::fs::read_dir(self.root.reader().descriptor_root_path())
            .map_err(|error| format!("scan direct-state retired residue: {error}"))?
            .enumerate()
        {
            if ordinal >= MAX_DIRECT_STATE_GC_ENTRIES {
                return Err("direct-state durable directory exceeds its GC entry bound".into());
            }
            let entry =
                entry.map_err(|error| format!("read direct-state retired residue: {error}"))?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "direct-state retired residue name is not UTF-8".to_string())?;
            if !name.starts_with(&prefix)
                || !(name.contains(".retired.") || name.contains(".abandoned."))
            {
                continue;
            }
            let expected = self
                .root
                .reader()
                .open_regular(&name, "pin direct-state retired residue")?;
            let mut retirement = ExactRetirement {
                root: self.root.try_clone_token()?,
                expected,
                live_name: target_name.to_string(),
                quarantine_name: name,
                label: "direct-state retired residue",
                phase: ExactRetirementPhase::Quarantined,
            };
            retirement.retry()?;
        }
        Ok(())
    }
}

pub fn ensure_private_directory(path: &Path) -> Result<(), String> {
    ensure_direct_state_platform_supported()?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err("direct-state staging root is not a real directory".into())
        }
        Ok(metadata) => validate_private_directory_metadata(&metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                let parent = path.parent().ok_or_else(|| {
                    "direct-state staging root has no parent directory".to_string()
                })?;
                let name = path
                    .file_name()
                    .ok_or_else(|| "direct-state staging root has no leaf name".to_string())?;
                let parent_authority =
                    open_directory_nofollow(parent, "open direct-state staging parent")?;
                mkdirat(
                    &parent_authority,
                    name,
                    Mode::RUSR | Mode::WUSR | Mode::XUSR,
                )
                .map_err(|error| format!("create direct-state staging root: {error}"))?;
                parent_authority
                    .sync_all()
                    .map_err(|error| format!("fsync direct-state staging parent: {error}"))?;
            }
            #[cfg(not(unix))]
            return Err("direct-state private staging is unsupported on this platform".into());
            let metadata = std::fs::symlink_metadata(path)
                .map_err(|error| format!("restat direct-state staging root: {error}"))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err("direct-state staging root changed during creation".into());
            }
            validate_private_directory_metadata(&metadata)?;
            open_directory_nofollow(path, "pin created direct-state staging root").map(|_| ())
        }
        Err(error) => Err(format!("stat direct-state staging root: {error}")),
    }
}

#[cfg(all(
    unix,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
pub(super) fn ensure_direct_state_platform_supported() -> Result<(), String> {
    Ok(())
}

#[cfg(not(all(
    unix,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
)))]
pub(super) fn ensure_direct_state_platform_supported() -> Result<(), String> {
    Err("direct-state atomic private-file publication is unsupported on this platform".into())
}

#[cfg(unix)]
pub(super) fn validate_private_directory_metadata(
    metadata: &std::fs::Metadata,
) -> Result<(), String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err("direct-state staging root must have mode 0700".into());
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err("direct-state staging root is owned by another user".into());
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn validate_private_directory_metadata(
    _metadata: &std::fs::Metadata,
) -> Result<(), String> {
    Err("direct-state private staging is unsupported on this platform".into())
}

pub(super) fn open_directory_nofollow(path: &Path, operation: &str) -> Result<File, String> {
    #[cfg(unix)]
    let directory = {
        let start = if path.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        };
        let mut current = File::from(
            rustix_open(
                start,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|error| format!("{operation}: {error}"))?,
        );
        for component in path.components() {
            let name = match component {
                Component::RootDir | Component::CurDir => continue,
                Component::Normal(name) => name,
                Component::ParentDir | Component::Prefix(_) => {
                    return Err(format!(
                        "{operation}: direct-state private root must not contain parent/prefix components"
                    ));
                }
            };
            let next = openat(
                &current,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(|error| format!("{operation}: {error}"))?;
            current = File::from(next);
        }
        current
    };
    #[cfg(not(unix))]
    return Err(format!(
        "{operation}: direct-state private directories are unsupported on this platform"
    ));
    let metadata = directory
        .metadata()
        .map_err(|error| format!("{operation} metadata: {error}"))?;
    if !metadata.is_dir() {
        return Err(format!("{operation}: source is not a directory"));
    }
    validate_private_directory_metadata(&metadata)?;
    Ok(directory)
}

pub(super) fn validate_same_file_identity(
    expected: &File,
    actual: &File,
    label: &str,
) -> Result<(), String> {
    let (expected, actual) = (
        expected
            .metadata()
            .map_err(|error| format!("stat {label} authority: {error}"))?,
        actual
            .metadata()
            .map_err(|error| format!("stat {label} path: {error}"))?,
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if expected.dev() != actual.dev() || expected.ino() != actual.ino() {
            return Err(format!("{label} path identity changed"));
        }
    }
    Ok(())
}

pub(super) fn validate_file_content(
    source: &File,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<(), String> {
    if expected_bytes > HARD_MAX_DIRECT_STATE_BYTES {
        return Err("direct-state pinned input exceeds its hard bound".into());
    }
    validate_sha256("direct-state pinned input", expected_sha256)?;
    let mut input = source
        .try_clone()
        .map_err(|error| format!("clone direct-state pinned input: {error}"))?;
    input
        .seek(SeekFrom::Start(0))
        .map_err(|error| format!("rewind direct-state pinned input: {error}"))?;
    if input
        .metadata()
        .map_err(|error| format!("stat direct-state pinned input: {error}"))?
        .len()
        != expected_bytes
    {
        return Err("direct-state pinned input length differs from its manifest".into());
    }
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; MAX_DIRECT_STATE_CHUNK_BYTES];
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|error| format!("read direct-state pinned input: {error}"))?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| "direct-state pinned input length overflow".to_string())?;
        if bytes > expected_bytes {
            return Err("direct-state pinned input grew during validation".into());
        }
        hasher.update(&buffer[..count]);
    }
    if bytes != expected_bytes || format!("{:x}", hasher.finalize()) != expected_sha256 {
        return Err("direct-state pinned input differs from its manifest".into());
    }
    Ok(())
}

pub(super) fn copy_regular_file_exact(
    source: &File,
    target_root: &PinnedPrivateDirectory,
    target_name: &str,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<File, String> {
    if expected_bytes > HARD_MAX_DIRECT_STATE_BYTES {
        return Err("direct-state mutable copy exceeds its hard bound".into());
    }
    validate_sha256("direct-state immutable input", expected_sha256)?;
    let mut input = source
        .try_clone()
        .map_err(|error| format!("clone direct-state immutable input: {error}"))?;
    input
        .seek(SeekFrom::Start(0))
        .map_err(|error| format!("rewind direct-state immutable input: {error}"))?;
    if input
        .metadata()
        .map_err(|error| format!("stat direct-state immutable input: {error}"))?
        .len()
        != expected_bytes
    {
        return Err("direct-state immutable input length changed before copy".into());
    }
    let mut output = target_root
        .mutations()
        .create_new(target_name, "create direct-state mutable copy")?;
    let result = (|| {
        let mut bytes = 0_u64;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; MAX_DIRECT_STATE_CHUNK_BYTES];
        loop {
            let count = input
                .read(&mut buffer)
                .map_err(|error| format!("read direct-state immutable input: {error}"))?;
            if count == 0 {
                break;
            }
            bytes = bytes
                .checked_add(count as u64)
                .ok_or_else(|| "direct-state mutable copy length overflow".to_string())?;
            if bytes > expected_bytes {
                return Err("direct-state immutable input grew during copy".into());
            }
            output
                .write_all(&buffer[..count])
                .map_err(|error| format!("write direct-state mutable copy: {error}"))?;
            hasher.update(&buffer[..count]);
        }
        if bytes != expected_bytes {
            return Err("direct-state immutable input shrank during copy".into());
        }
        if format!("{:x}", hasher.finalize()) != expected_sha256 {
            return Err("direct-state immutable input changed before copy".into());
        }
        output
            .sync_all()
            .map_err(|error| format!("fsync direct-state mutable copy: {error}"))
    })();
    match result {
        Ok(()) => Ok(output),
        Err(error) => {
            drop(output);
            let _ = target_root
                .mutations()
                .unlink(target_name, "remove failed direct-state mutable copy");
            let _ = target_root.sync();
            Err(error)
        }
    }
}

pub(super) static NEXT_DIRECT_STATE_TEMP: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExactRetirementPhase {
    Live,
    Quarantined,
    Unlinked,
}

/// Non-Clone cleanup debt for one exact inode. Every descriptor/root clone is
/// acquired before the first visibility change. Once the public name moves, all
/// validation or durability failures remain retryable through this value.
pub(super) struct ExactRetirement {
    pub(super) root: PinnedPrivateDirectory,
    pub(super) expected: File,
    pub(super) live_name: String,
    pub(super) quarantine_name: String,
    pub(super) label: &'static str,
    pub(super) phase: ExactRetirementPhase,
}

impl ExactRetirement {
    pub(super) fn prepare(
        root: &PinnedPrivateDirectory,
        expected: &File,
        live_name: &str,
        label: &'static str,
    ) -> Result<Self, String> {
        validate_relative_basename(live_name)?;
        Ok(Self {
            root: root.try_clone_token()?,
            expected: expected
                .try_clone()
                .map_err(|error| format!("clone {label} authority: {error}"))?,
            live_name: live_name.to_string(),
            quarantine_name: format!(
                ".{live_name}.retired.{}.{}",
                std::process::id(),
                NEXT_DIRECT_STATE_TEMP.fetch_add(1, Ordering::Relaxed)
            ),
            label,
            phase: ExactRetirementPhase::Live,
        })
    }

    pub(super) fn retry(&mut self) -> Result<(), String> {
        if self.phase == ExactRetirementPhase::Live {
            let live = self.root.reader().open_regular(
                &self.live_name,
                &format!("authenticate live {}", self.label),
            )?;
            validate_same_file_identity(&self.expected, &live, self.label)?;
            self.root.mutations().rename_no_replace(
                &self.live_name,
                &self.quarantine_name,
                &format!("quarantine {}", self.label),
            )?;
            self.phase = ExactRetirementPhase::Quarantined;
        }
        if self.phase == ExactRetirementPhase::Quarantined {
            let moved = self.root.reader().open_regular(
                &self.quarantine_name,
                &format!("pin quarantined {}", self.label),
            )?;
            validate_same_file_identity(&self.expected, &moved, self.label)?;
            self.root.sync()?;
            self.root.mutations().unlink(
                &self.quarantine_name,
                &format!("remove quarantined {}", self.label),
            )?;
            self.phase = ExactRetirementPhase::Unlinked;
        }
        self.root.sync()
    }
}

#[cfg(test)]
pub(super) fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("fsync direct-state directory: {error}"))
}

pub(super) fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
