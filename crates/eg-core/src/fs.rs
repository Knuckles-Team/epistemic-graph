//! Secure create-new files used for private transient and durable records.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Create a new writable file without following an existing final path.
/// On Unix the new inode is private from creation, before callers write data.
pub fn create_private_new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_file_is_private_and_cannot_replace_an_existing_entry() {
        let path = std::env::temp_dir().join(format!(
            "eg-core-private-new-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = create_private_new_file(&path).unwrap();
        assert_eq!(
            create_private_new_file(&path).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        }
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
