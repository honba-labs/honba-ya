//! Shared mechanics of the per-user JSON document stores (`profiles`, `portfolios`).
//!
//! Both stores keep one JSON file per user id under a directory, with the same operational
//! shape: an untrusted user id is validated before it touches a path, a write goes to a
//! uniquely-named temp file beside the target and is renamed over it (fsync'd file and
//! directory), and a file that no longer parses is moved aside so one bad document cannot make
//! every request for that user fail forever. The in-process mutex each store holds makes its
//! read-check-write atomic within one process only; there is no cross-process file lock, so run
//! **one `honba serve` per directory**.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::warn;

/// The user id used when authentication is off.
pub(crate) const DEFAULT_USER_ID: &str = "default";

/// Longest accepted user id, in bytes.
pub(crate) const MAX_USER_ID_LEN: usize = 64;

/// Accept only `[A-Za-z0-9_-]`, 1..=64 bytes.
pub(crate) fn is_valid_user_id(user_id: &str) -> bool {
    (1..=MAX_USER_ID_LEN).contains(&user_id.len())
        && user_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// The file `dir/<user_id>.json`, or an error when `user_id` is not a valid user id.
pub(crate) fn path_for(dir: &Path, user_id: &str) -> Option<PathBuf> {
    is_valid_user_id(user_id).then(|| dir.join(format!("{user_id}.json")))
}

/// Distinguishes the temp files of concurrent writes within one process.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write to a uniquely named temp file beside `path`, sync it, then rename over `path`. The temp
/// name carries the pid and a counter so concurrent writers never share a file; it is removed
/// if any step fails.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(
        ".{}-{}.tmp",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let tmp = PathBuf::from(name);
    let result = (|| {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        // The rename is only durable once the directory entry is.
        path.parent().map_or(Ok(()), sync_dir)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

/// Create `dir` (and parents) if missing, syncing it and its parent so the new directory
/// survives a crash.
pub(crate) fn create_dir_durable(dir: &Path) -> std::io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    fs::create_dir_all(dir)?;
    sync_dir(dir)?;
    match dir.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => sync_dir(parent),
        _ => Ok(()),
    }
}

/// `fsync` a directory so renames and creations inside it are durable. A no-op off unix, where
/// directories cannot be opened for syncing.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Move a document that no longer parses aside to `<name>.corrupt-<unix_ts>`, so the user starts
/// over on the next read; warn either way, naming `kind` ("profile", "portfolio").
pub(crate) fn quarantine_corrupt(path: &Path, kind: &str, error: &serde_json::Error) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut aside = path.as_os_str().to_owned();
    aside.push(format!(".corrupt-{ts}"));
    let aside = PathBuf::from(aside);
    match fs::rename(path, &aside) {
        Ok(()) => warn!(
            file = %path.display(), moved_to = %aside.display(), error = %error,
            "corrupt {kind} file moved aside; starting fresh"
        ),
        Err(rename_err) => warn!(
            file = %path.display(), error = %error, rename_error = %rename_err,
            "corrupt {kind} file could not be moved aside; starting fresh"
        ),
    }
}
