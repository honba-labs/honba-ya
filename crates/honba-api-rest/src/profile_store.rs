//! Per-user profile store: one JSON file per user id in a directory.
//!
//! A profile holds the workbench, layouts, modules, watchlists and preferences of one user.
//! Every profile carries a `revision`; [`ProfileStore::put`] is a compare-and-swap on it, so two
//! tabs of the same user cannot silently overwrite each other. User ids come from bearer
//! `token_id`s and are therefore untrusted: they are validated before they touch a path.
//!
//! The in-process mutex makes each read-check-write atomic within one process only. There is no
//! cross-process file lock (none is available in the workspace), so run **one `honba serve` per
//! profiles directory**; two servers sharing one could lose updates.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub use honba_api::{Module, Profile, Source};

use crate::user_doc;

/// The user id used when authentication is off.
pub const DEFAULT_USER_ID: &str = crate::user_doc::DEFAULT_USER_ID;

/// Largest serialized profile a user may store, in bytes.
pub const MAX_PROFILE_BYTES: usize = 2 * 1024 * 1024;

/// Most named layouts one profile may hold.
pub const MAX_LAYOUTS: usize = 200;

/// Most modules one profile may hold.
pub const MAX_MODULES: usize = 100;

/// Most watchlists one profile may hold.
pub const MAX_WATCHLISTS: usize = 100;

/// Most symbols in one watchlist, and in the shortlist.
pub const MAX_SYMBOLS: usize = 5000;

/// Why a profile operation failed.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// The user id is empty, too long, or has characters outside `[A-Za-z0-9_-]`.
    #[error("invalid user id")]
    InvalidUserId,
    /// The caller's expected revision is not the stored one.
    #[error("revision conflict: expected {expected}, current {current}")]
    RevisionConflict {
        /// Revision the caller sent.
        expected: u64,
        /// Revision currently stored.
        current: u64,
    },
    /// The profile exceeds a size or count limit; `what` names the limit.
    #[error("profile too large: {what}")]
    TooLarge {
        /// Which limit was exceeded.
        what: String,
    },
    /// Reading or writing the profile file failed.
    #[error("profile io: {0}")]
    Io(#[from] std::io::Error),
    /// A profile could not be serialized.
    #[error("profile json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result alias for profile operations.
pub type Result<T> = std::result::Result<T, ProfileError>;

/// Directory of per-user profile files.
#[derive(Debug)]
pub struct ProfileStore {
    dir: PathBuf,
    /// Serializes writers so the read-check-write of the revision is atomic in-process.
    write_lock: Mutex<()>,
}

impl ProfileStore {
    /// A store rooted at `dir`; the directory is created on first write.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            write_lock: Mutex::new(()),
        }
    }

    /// The user's profile, or a default one (revision 0) when none is stored.
    pub fn get(&self, user_id: &str) -> Result<Profile> {
        let path = self.path_for(user_id)?;
        read_profile(&path)
    }

    /// Replace the user's profile if `expected_revision` is the stored revision. The stored
    /// profile gets `expected_revision + 1`, whatever revision `new_profile` carries. If
    /// `new_profile` equals the stored one apart from the revision, nothing is written and the
    /// stored profile (same revision) is returned.
    pub fn put(
        &self,
        user_id: &str,
        expected_revision: u64,
        mut new_profile: Profile,
    ) -> Result<Profile> {
        let path = self.path_for(user_id)?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stored = read_profile(&path)?;
        let current = stored.revision;
        if current != expected_revision {
            return Err(ProfileError::RevisionConflict {
                expected: expected_revision,
                current,
            });
        }
        // Saving what is already stored (a debounced client re-sending) writes nothing.
        new_profile.revision = current;
        if new_profile == stored {
            return Ok(stored);
        }
        new_profile.revision = current + 1;
        let bytes = encode_within_limits(&new_profile)?;
        user_doc::create_dir_durable(&self.dir)?;
        user_doc::write_atomic(&path, &bytes)?;
        Ok(new_profile)
    }

    /// Read-modify-write the user's profile under the store lock. `change` returns whether it
    /// altered the profile; if so the revision is bumped once and the file rewritten atomically,
    /// otherwise nothing is written. Returns the profile as stored afterwards.
    pub fn update(
        &self,
        user_id: &str,
        change: impl FnOnce(&mut Profile) -> bool,
    ) -> Result<Profile> {
        let path = self.path_for(user_id)?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut profile = read_profile(&path)?;
        if !change(&mut profile) {
            return Ok(profile);
        }
        profile.revision += 1;
        let bytes = encode_within_limits(&profile)?;
        user_doc::create_dir_durable(&self.dir)?;
        user_doc::write_atomic(&path, &bytes)?;
        Ok(profile)
    }

    fn path_for(&self, user_id: &str) -> Result<PathBuf> {
        user_doc::path_for(&self.dir, user_id).ok_or(ProfileError::InvalidUserId)
    }
}

/// Check the count limits, then serialize and check [`MAX_PROFILE_BYTES`].
fn encode_within_limits(profile: &Profile) -> Result<Vec<u8>> {
    let too_large = |what: String| Err(ProfileError::TooLarge { what });
    if profile.layouts.len() > MAX_LAYOUTS {
        return too_large(format!("more than {MAX_LAYOUTS} layouts"));
    }
    if profile.modules.len() > MAX_MODULES {
        return too_large(format!("more than {MAX_MODULES} modules"));
    }
    if profile.watchlists.len() > MAX_WATCHLISTS {
        return too_large(format!("more than {MAX_WATCHLISTS} watchlists"));
    }
    if profile.shortlist.len() > MAX_SYMBOLS
        || profile.watchlists.values().any(|w| w.len() > MAX_SYMBOLS)
    {
        return too_large(format!(
            "more than {MAX_SYMBOLS} symbols in a watchlist or the shortlist"
        ));
    }
    let bytes = serde_json::to_vec_pretty(profile)?;
    if bytes.len() > MAX_PROFILE_BYTES {
        return too_large(format!("more than {MAX_PROFILE_BYTES} bytes"));
    }
    Ok(bytes)
}

/// Read the profile at `path`; a missing file is the default profile. A file that is not valid
/// JSON for a profile is moved aside to `<name>.corrupt-<unix_ts>` and the user starts over at
/// revision 0, so one bad file cannot make every request for that user fail forever.
fn read_profile(path: &Path) -> Result<Profile> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Profile::default()),
        Err(e) => return Err(e.into()),
    };
    match serde_json::from_slice(&bytes) {
        Ok(profile) => Ok(profile),
        Err(e) => {
            user_doc::quarantine_corrupt(path, "profile", &e);
            Ok(Profile::default())
        }
    }
}
