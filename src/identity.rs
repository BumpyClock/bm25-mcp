//! Canonical project identity and persisted cwd ownership associations.
//!
//! Code collections are per worktree, while session ownership is shared by a
//! Git repository's canonical common directory. Plain folders are owned by
//! the explicitly registered canonical root. The registry is a small,
//! replaceable JSON file used only after an identity has been verified from a
//! live path; it never guesses ownership for an unknown deleted directory.

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_FILE_NAME: &str = "identity-registry.json";

/// The kind of root that was verified when a registry entry was recorded.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RootKind {
    GitWorktree,
    Plain,
}

/// Canonical identities for a configured project root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectIdentity {
    /// The actual worktree root for Git projects, or the canonical explicit
    /// root for a plain folder.
    pub root: PathBuf,
    /// Repository-family identity for Git projects, or the canonical root
    /// identity for a plain folder.
    pub owner_key: String,
    /// Per-worktree code collection identity.
    pub collection: String,
    /// The verified Git common directory, when this is a Git worktree.
    pub git_common_dir: Option<PathBuf>,
    /// Whether the identity was obtained from Git or from a plain root.
    pub kind: RootKind,
}

/// A cwd association returned by [`IdentityRegistry::associate_cwd`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CwdAssociation {
    pub root: PathBuf,
    pub owner_key: String,
    pub collection: String,
    pub kind: RootKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RegistryFile {
    version: u32,
    entries: Vec<RegistryEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RegistryEntry {
    root: String,
    owner_key: String,
    collection: String,
    kind: RootKind,
    git_common_dir: Option<String>,
    last_verified_unix: u64,
}

/// A bounded, persistable set of identities verified from live roots.
#[derive(Clone, Debug)]
pub struct IdentityRegistry {
    path: PathBuf,
    entries: Vec<RegistryEntry>,
}

impl IdentityRegistry {
    /// Open an existing registry or create an empty in-memory registry. The
    /// file is written only by [`IdentityRegistry::remember_verified`].
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        if !path.exists() {
            return Ok(Self {
                path,
                entries: Vec::new(),
            });
        }
        let bytes = fs::read(&path)
            .with_context(|| format!("reading identity registry {}", path.display()))?;
        let file: RegistryFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("decoding identity registry {}", path.display()))?;
        if file.version != REGISTRY_VERSION {
            bail!(
                "unsupported identity registry version {}; expected {}",
                file.version,
                REGISTRY_VERSION
            );
        }
        let mut entries = file.entries;
        entries.retain(|entry| {
            !entry.root.is_empty() && !entry.owner_key.is_empty() && !entry.collection.is_empty()
        });
        Ok(Self { path, entries })
    }

    /// The registry file convention used by an owner cache directory.
    pub fn for_cache_dir(cache_dir: &Path) -> Result<Self> {
        Self::open(&cache_dir.join(REGISTRY_FILE_NAME))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Record an identity that was verified from a currently existing root.
    /// Re-recording a root replaces its old association and is atomic with
    /// respect to readers of the registry file.
    pub fn remember_verified(&mut self, identity: &ProjectIdentity) -> Result<()> {
        if identity.root.as_os_str().is_empty()
            || identity.owner_key.is_empty()
            || identity.collection.is_empty()
        {
            bail!("cannot persist an incomplete project identity")
        }
        // Owners can be started concurrently for different worktrees. Reload
        // after taking the cross-platform create-new lock so one writer does
        // not erase another writer's verified mapping.
        let _lock = RegistryLock::acquire(&self.path)?;
        if self.path.exists() {
            let latest = read_registry(&self.path)?;
            self.entries = latest.entries;
        }
        // Persist normalized containment paths as well as live identities.
        // This keeps a caller-supplied removed path consistent with the
        // canonical temporary-directory prefix used during lookup.
        let root = canonical_or_normalized(&identity.root)
            .to_string_lossy()
            .into_owned();
        let common_dir = identity
            .git_common_dir
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned());
        if self.entries.iter().any(|entry| {
            entry.root == root
                && entry.owner_key == identity.owner_key
                && entry.collection == identity.collection
                && entry.kind == identity.kind
                && entry.git_common_dir == common_dir
        }) {
            return Ok(());
        }
        self.entries.retain(|entry| entry.root != root);
        self.entries.push(RegistryEntry {
            root,
            owner_key: identity.owner_key.clone(),
            collection: identity.collection.clone(),
            kind: identity.kind,
            git_common_dir: common_dir,
            last_verified_unix: now_unix(),
        });
        self.persist()
    }

    /// Resolve a session cwd against verified identities.
    ///
    /// A live Git cwd is resolved by Git itself. For a deleted cwd, an exact
    /// or descendant path match is accepted only from this registry. Plain
    /// roots use the most-specific registered path, so a nested registration
    /// wins over its parent. `active_owner` is an optional caller-side filter;
    /// passing it does not make unknown paths match.
    pub fn associate_cwd(
        &self,
        cwd: &Path,
        active_owner: Option<&str>,
    ) -> Result<Option<CwdAssociation>> {
        // A live Git path is authoritative even when a registered plain root
        // happens to contain it. Resolve it before registry containment so a
        // nested repository keeps its common-directory ownership identity.
        // The registry remains the source of truth for removed worktrees,
        // where this probe necessarily fails.
        if let Ok(identity) = resolve_project(cwd)
            && identity.kind == RootKind::GitWorktree
        {
            if active_owner.is_some_and(|owner| owner != identity.owner_key) {
                return Ok(None);
            }
            return Ok(Some(CwdAssociation {
                root: identity.root,
                owner_key: identity.owner_key,
                collection: identity.collection,
                kind: identity.kind,
            }));
        }
        let candidate = canonical_or_normalized(cwd);
        let mut best: Option<&RegistryEntry> = None;
        for entry in &self.entries {
            let root = Path::new(&entry.root);
            if !candidate.starts_with(root) {
                continue;
            }
            if entry.kind == RootKind::GitWorktree {
                // A persisted Git worktree match is exact or a descendant of
                // that worktree root; no name/remotes based inference.
                if best.is_none_or(|current| {
                    root.components().count() > Path::new(&current.root).components().count()
                }) {
                    best = Some(entry);
                }
            } else if best.is_none_or(|current| {
                root.components().count() > Path::new(&current.root).components().count()
            }) {
                best = Some(entry);
            }
        }
        if let Some(entry) = best {
            // Apply the owner filter only after choosing the most-specific
            // root. A nested root registered to another owner must not fall
            // back to a broader parent and leak events across projects.
            if active_owner.is_some_and(|owner| owner != entry.owner_key) {
                return Ok(None);
            }
            return Ok(Some(CwdAssociation {
                root: PathBuf::from(&entry.root),
                owner_key: entry.owner_key.clone(),
                collection: entry.collection.clone(),
                kind: entry.kind,
            }));
        }

        // A live plain cwd with no registered ancestor is safe only when the
        // caller did not ask us to prove an existing project association.
        // Deleted or otherwise unverified roots never reach this branch.
        if let Ok(identity) = resolve_project(cwd)
            && identity.kind == RootKind::Plain
            && active_owner.is_none_or(|owner| owner == identity.owner_key)
        {
            return Ok(Some(CwdAssociation {
                root: identity.root,
                owner_key: identity.owner_key,
                collection: identity.collection,
                kind: identity.kind,
            }));
        }
        Ok(None)
    }

    fn persist(&self) -> Result<()> {
        let Some(parent) = self.path.parent() else {
            bail!("identity registry has no parent directory")
        };
        fs::create_dir_all(parent)
            .with_context(|| format!("create identity registry directory {}", parent.display()))?;
        static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
        let temp_id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp = parent.join(format!(
            ".{}-tmp-{}-{stamp}-{temp_id}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(REGISTRY_FILE_NAME),
            std::process::id(),
        ));
        let bytes = serde_json::to_vec_pretty(&RegistryFile {
            version: REGISTRY_VERSION,
            entries: self.entries.clone(),
        })?;
        {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(&temp)
                .with_context(|| format!("create identity registry temp {}", temp.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
            }
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        if let Err(error) = fs::rename(&temp, &self.path) {
            let _ = fs::remove_file(&temp);
            return Err(error)
                .with_context(|| format!("replace identity registry {}", self.path.display()));
        }
        if let Ok(parent_file) = File::open(parent) {
            let _ = parent_file.sync_all();
        }
        Ok(())
    }
}

fn read_registry(path: &Path) -> Result<RegistryFile> {
    let bytes =
        fs::read(path).with_context(|| format!("reading identity registry {}", path.display()))?;
    let file: RegistryFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("decoding identity registry {}", path.display()))?;
    if file.version != REGISTRY_VERSION {
        bail!(
            "unsupported identity registry version {}; expected {}",
            file.version,
            REGISTRY_VERSION
        );
    }
    Ok(file)
}

struct RegistryLock {
    file: File,
}

impl RegistryLock {
    fn acquire(registry: &Path) -> Result<Self> {
        let lock_path = registry.with_extension("json.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&lock_path)?;
        for _ in 0..500 {
            match file.try_lock() {
                Ok(()) => {
                    return Ok(Self { file });
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            }
        }
        Err(anyhow!("timed out acquiring identity registry lock"))
    }
}

impl Drop for RegistryLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Resolve the identity of an existing project root.
pub fn resolve_project(root: &Path) -> Result<ProjectIdentity> {
    let requested = fs::canonicalize(root)
        .with_context(|| format!("canonicalizing project root {}", root.display()))?;
    if !fs::metadata(&requested)
        .with_context(|| format!("reading project root {}", requested.display()))?
        .is_dir()
    {
        return Err(anyhow!(
            "project root is not a directory: {}",
            requested.display()
        ));
    }

    if let Some(worktree_root) = git_toplevel(&requested)
        && let Some(common_dir) = git_common_dir(&worktree_root)
    {
        return Ok(ProjectIdentity {
            root: worktree_root.clone(),
            owner_key: digest_path(&common_dir),
            collection: digest_path(&worktree_root),
            git_common_dir: Some(common_dir),
            kind: RootKind::GitWorktree,
        });
    }

    Ok(ProjectIdentity {
        root: requested.clone(),
        owner_key: digest_path(&requested),
        collection: digest_path(&requested),
        git_common_dir: None,
        kind: RootKind::Plain,
    })
}

/// Resolve and persist a live project identity in one operation.
pub fn resolve_and_remember(root: &Path, registry_path: &Path) -> Result<ProjectIdentity> {
    let identity = resolve_project(root)?;
    let mut registry = IdentityRegistry::open(registry_path)?;
    registry.remember_verified(&identity)?;
    Ok(identity)
}

/// Register path containment for an explicit plain root without requiring a
/// caller to construct the identity manually.
pub fn register_plain_root(root: &Path, registry_path: &Path) -> Result<ProjectIdentity> {
    let identity = resolve_project(root)?;
    if identity.kind != RootKind::Plain {
        bail!("root is a Git worktree, not a plain folder")
    }
    let mut registry = IdentityRegistry::open(registry_path)?;
    registry.remember_verified(&identity)?;
    Ok(identity)
}

fn git_toplevel(root: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "--show-toplevel"])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    (!value.is_empty())
        .then(|| fs::canonicalize(value).ok())
        .flatten()
}

fn git_common_dir(worktree_root: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree_root)
        .args(["rev-parse", "--git-common-dir"])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let path = Path::new(value);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        worktree_root.join(path)
    };
    fs::canonicalize(path).ok()
}

fn digest_path(path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(path.to_string_lossy().as_bytes());
    let mut text = String::with_capacity(64);
    for byte in digest.finalize() {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}

fn canonical_or_normalized(path: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    // Resolve the deepest existing ancestor so temporary-directory symlinks
    // (for example `/var` -> `/private/var` on macOS) still compare equal to
    // persisted canonical roots when the cwd itself has been removed.
    let mut suffix = Vec::new();
    let mut probe = path.to_path_buf();
    let canonical_base = loop {
        if let Ok(canonical) = fs::canonicalize(&probe) {
            break canonical;
        }
        let Some(name) = probe.file_name().map(|name| name.to_os_string()) else {
            break probe.clone();
        };
        suffix.push(name);
        if !probe.pop() {
            break probe.clone();
        }
    };
    let mut normalized = canonical_base;
    for component in suffix.iter().rev() {
        normalized.push(component);
    }
    normalized
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("git is installed for identity tests");
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn nested_git_cwd_uses_worktree_root() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        git(root, &["init", "-q"]);
        fs::create_dir(root.join("src")).unwrap();
        let identity = resolve_project(&root.join("src")).unwrap();
        assert_eq!(identity.root, fs::canonicalize(root).unwrap());
        assert_eq!(identity.kind, RootKind::GitWorktree);
    }

    #[test]
    fn nested_plain_registration_wins_and_deleted_unknown_stays_unassociated() {
        let temp = TempDir::new().unwrap();
        let outer = temp.path().join("outer");
        let nested = outer.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let registry_path = temp.path().join("cache").join("identities.json");
        let outer_id = register_plain_root(&outer, &registry_path).unwrap();
        let nested_id = register_plain_root(&nested, &registry_path).unwrap();
        let registry = IdentityRegistry::open(&registry_path).unwrap();
        let child = nested.join("missing").join("cwd");
        assert_eq!(
            registry
                .associate_cwd(&child, None)
                .unwrap()
                .unwrap()
                .owner_key,
            nested_id.owner_key
        );
        assert!(
            registry
                .associate_cwd(&temp.path().join("never-observed"), None)
                .unwrap()
                .is_none()
        );
        assert_ne!(outer_id.owner_key, nested_id.owner_key);
    }

    #[test]
    fn live_git_identity_precedes_registered_plain_ancestor() {
        let temp = TempDir::new().unwrap();
        let outer = temp.path().join("outer");
        let nested = outer.join("nested-repo");
        fs::create_dir_all(&nested).unwrap();
        git(&nested, &["init", "-q"]);
        let registry_path = temp.path().join("cache").join("identities.json");
        register_plain_root(&outer, &registry_path).unwrap();
        let registry = IdentityRegistry::open(&registry_path).unwrap();
        let identity = resolve_project(&nested).unwrap();
        let association = registry.associate_cwd(&nested, None).unwrap().unwrap();
        assert_eq!(association.kind, RootKind::GitWorktree);
        assert_eq!(association.owner_key, identity.owner_key);
    }

    #[test]
    fn persisted_git_worktree_can_be_associated_after_removal() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("repo");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.email", "test@example.invalid"]);
        git(&root, &["config", "user.name", "Identity Test"]);
        fs::write(root.join("file.txt"), "identity").unwrap();
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "root"]);
        let registry_path = temp.path().join("cache").join("identities.json");
        let identity = resolve_and_remember(&root, &registry_path).unwrap();
        let removed = temp.path().join("removed-worktree");
        // The removed path is a known verified root; it is intentionally not
        // created. An unknown sibling below the same temp dir remains absent.
        let mut registry = IdentityRegistry::open(&registry_path).unwrap();
        let mut stale = identity.clone();
        stale.root = removed.clone();
        registry.remember_verified(&stale).unwrap();
        let loaded = IdentityRegistry::open(&registry_path).unwrap();
        assert_eq!(
            loaded
                .associate_cwd(&removed.join("child"), Some(&identity.owner_key))
                .unwrap()
                .unwrap()
                .owner_key,
            identity.owner_key
        );
        assert!(
            loaded
                .associate_cwd(&temp.path().join("unknown"), Some(&identity.owner_key))
                .unwrap()
                .is_none()
        );
    }
}
