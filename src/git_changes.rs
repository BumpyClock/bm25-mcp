//! Git narrows reconciliation candidates; working-tree bytes remain authoritative.
use std::collections::HashSet;
use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Clone)]
pub(crate) struct Snapshot {
    head: String,
    dirty: HashSet<PathBuf>,
}
impl Snapshot {
    pub fn same_revision(&self, other: &Self) -> bool {
        self.head == other.head
    }
    pub fn capture(root: &Path) -> Option<Self> {
        let head = String::from_utf8(git(root, &["rev-parse", "--verify", "HEAD"])?)
            .ok()?
            .trim()
            .to_owned();
        if head.is_empty() {
            return None;
        }
        let bytes = git(
            root,
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )?;
        let mut fields = bytes.split(|b| *b == 0);
        let mut dirty = HashSet::new();
        while let Some(field) = fields.next() {
            if field.is_empty() {
                continue;
            }
            if field.len() < 4 || field[2] != b' ' {
                return None;
            }
            dirty.insert(root.join(path(&field[3..])?));
            if field[..2].iter().any(|b| matches!(b, b'R' | b'C')) {
                dirty.insert(root.join(path(fields.next()?)?));
            }
        }
        Some(Self { head, dirty })
    }
    pub fn changed_paths(&self, root: &Path, next: &Self) -> Option<HashSet<PathBuf>> {
        let mut paths = self
            .dirty
            .union(&next.dirty)
            .cloned()
            .collect::<HashSet<_>>();
        if self.head != next.head {
            let bytes = git(
                root,
                &[
                    "diff",
                    "--name-only",
                    "--no-renames",
                    "-z",
                    &self.head,
                    &next.head,
                    "--",
                ],
            )?;
            for name in bytes.split(|b| *b == 0).filter(|s| !s.is_empty()) {
                paths.insert(root.join(path(name)?));
            }
        }
        if paths.iter().any(|p| {
            matches!(
                p.file_name().and_then(|n| n.to_str()),
                Some(".gitignore" | ".ignore")
            )
        }) {
            return None;
        }
        Some(paths)
    }
}

fn git(root: &Path, args: &[&str]) -> Option<Vec<u8>> {
    const MAX_BYTES: u64 = 16 * 1024 * 1024;
    let mut child = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut bytes = Vec::new();
    let result = child
        .stdout
        .take()?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes);
    if result.is_err() || bytes.len() as u64 > MAX_BYTES {
        let _ = child.kill();
        let _ = child.wait();
        return None;
    }
    if !child.wait().ok()?.success() {
        return None;
    }
    Some(bytes)
}
fn path(bytes: &[u8]) -> Option<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        Some(PathBuf::from(OsString::from_vec(bytes.to_vec())))
    }
    #[cfg(not(unix))]
    {
        Some(PathBuf::from(OsString::from(
            String::from_utf8(bytes.to_vec()).ok()?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    fn run(root: &Path, args: &[&str]) {
        assert!(git(root, args).is_some(), "git {args:?}");
    }
    #[test]
    fn revision_changes_include_prior_dirty_current_dirty_and_untracked_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        run(root, &["init", "-q"]);
        run(root, &["config", "user.email", "test@example.test"]);
        run(root, &["config", "user.name", "Test"]);
        fs::write(root.join("tracked.txt"), "initial").unwrap();
        fs::write(root.join("dirty.txt"), "initial").unwrap();
        run(root, &["add", "."]);
        run(root, &["commit", "-qm", "initial"]);
        fs::write(root.join("dirty.txt"), "dirty").unwrap();
        let before = Snapshot::capture(root).unwrap();
        run(root, &["restore", "dirty.txt"]);
        fs::write(root.join("tracked.txt"), "next").unwrap();
        run(root, &["commit", "-qam", "next"]);
        fs::write(root.join("new.txt"), "untracked").unwrap();
        let after = Snapshot::capture(root).unwrap();
        let paths = before.changed_paths(root, &after).unwrap();
        for name in ["dirty.txt", "tracked.txt", "new.txt"] {
            assert!(paths.contains(&root.join(name)));
        }
        fs::write(root.join(".gitignore"), "new.txt\n").unwrap();
        assert!(
            after
                .changed_paths(root, &Snapshot::capture(root).unwrap())
                .is_none()
        );
    }
}
