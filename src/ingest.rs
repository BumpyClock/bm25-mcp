//! Project identity and bounded, ignore-aware source ingestion.
//!
//! The scanner deliberately keeps the source bytes out of memory.  It streams
//! a digest and decoded text into a small JSON-lines spool, then hands an
//! iterator over that spool to the transactional store.  A source is only
//! published after the file's metadata is the same before and after reading.

use crate::content_cache::{CachedChunks, ContentCache};
pub use crate::identity::ProjectIdentity;
use crate::model::{Chunk, ScanReport, Source};
use crate::progress::{ProgressPhase, ProgressReporter, WorkKind};
use crate::store::Store;
use crate::text::StreamingTokenizer;
use anyhow::{Context, Result, anyhow, ensure};
use ignore::WalkBuilder;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// The source kind used by project-file ingestion.
pub const PROJECT_SOURCE_KIND: &str = "project";

const MAX_CHUNK_BYTES: usize = 16 * 1024;
const APPROX_LINES_PER_CHUNK: u64 = 60;
const READ_BUFFER_BYTES: usize = 16 * 1024;
const MAX_RETRIES: usize = 3;
const MAX_REPORTED_ERRORS: usize = 64;
const MAX_ERROR_MESSAGE_BYTES: usize = 512;

/// Resolve canonical project, repository-family, and collection identities.
///
/// Git worktrees use the canonical `--git-common-dir` as their shared owner
/// identity, so separate worktrees have shared session ownership but distinct
/// code collections.  A non-Git root uses its canonical path as the owner.
pub fn project_identity(root: &Path) -> Result<ProjectIdentity> {
    crate::identity::resolve_project(root)
}

/// Invalidate the project sources represented by an exact set of canonical
/// paths, or the whole collection for a full/ambiguous reconciliation.
///
/// The observed scanner repeats this operation idempotently so direct callers
/// cannot publish a changed source without first suppressing its old rows.
pub fn invalidate_project_scope(
    root: &Path,
    store: &Store,
    collection: &str,
    changes: Option<&HashSet<PathBuf>>,
) -> Result<()> {
    let root = fs::canonicalize(root)
        .with_context(|| format!("canonicalizing project root {}", root.display()))?;
    let Some(changes) = changes else {
        return store.invalidate_collection(collection, PROJECT_SOURCE_KIND);
    };
    let candidates = changes
        .iter()
        .map(|path| canonical_or_normalized(&resolve_project_candidate(&root, path)))
        .collect::<HashSet<_>>();
    for source in store.sources(collection, PROJECT_SOURCE_KIND)? {
        if candidates.contains(&canonical_or_normalized(&root.join(&source.path))) {
            store.invalidate_source(&source.key)?;
        }
    }
    Ok(())
}

/// Reconcile eligible project files into `store`.
///
/// The walker applies nested `.gitignore` rules to tracked and untracked files
/// alike.  Symlinks are not followed, which prevents a project scan from
/// escaping its configured root.  Existing sources are invalidated before
/// reconciliation; unchanged sources are verified and changed sources are
/// replaced transactionally by the store.
pub fn scan_project(root: &Path, store: &Store, collection: &str) -> Result<ScanReport> {
    scan_project_observed(
        root,
        store,
        collection,
        None,
        &|| true,
        None,
        &ProgressReporter::noop(),
    )
}

/// Reconcile project files while reusing verified, tokenized chunks from a
/// bounded disk cache. The cache is an optimization and can be discarded at
/// any time without changing search correctness.
pub fn scan_project_with_cache(
    root: &Path,
    store: &Store,
    collection: &str,
    cache: &mut ContentCache,
) -> Result<ScanReport> {
    scan_project_observed(
        root,
        store,
        collection,
        None,
        &|| true,
        Some(cache),
        &ProgressReporter::noop(),
    )
}

/// Reconcile watcher candidates while retaining verified, unaffected sources.
/// Membership is still walked so nested ignore rules have their normal semantics.
pub fn scan_project_changes(
    root: &Path,
    store: &Store,
    collection: &str,
    paths: &HashSet<PathBuf>,
) -> Result<ScanReport> {
    scan_project_observed(
        root,
        store,
        collection,
        Some(paths),
        &|| true,
        None,
        &ProgressReporter::noop(),
    )
}

pub fn scan_project_controlled(
    root: &Path,
    store: &Store,
    collection: &str,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
) -> Result<ScanReport> {
    scan_project_observed(
        root,
        store,
        collection,
        changes,
        should_continue,
        None,
        &ProgressReporter::noop(),
    )
}

/// Controlled project reconciliation with an optional bounded content cache.
pub fn scan_project_controlled_with_cache(
    root: &Path,
    store: &Store,
    collection: &str,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    cache: Option<&mut ContentCache>,
) -> Result<ScanReport> {
    scan_project_observed(
        root,
        store,
        collection,
        changes,
        should_continue,
        cache,
        &ProgressReporter::noop(),
    )
}

/// Reconcile project sources while publishing bounded, path-free progress.
///
/// `changes` is an exact set of source-file candidates. `None` retains the
/// conservative full-reconcile behavior used for topology or watcher
/// uncertainty; a precise set never invalidates or walks unrelated sources.
pub fn scan_project_observed(
    root: &Path,
    store: &Store,
    collection: &str,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    cache: Option<&mut ContentCache>,
    progress: &ProgressReporter,
) -> Result<ScanReport> {
    scan_project_publishing(
        root,
        store,
        collection,
        changes,
        should_continue,
        cache,
        progress,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn scan_project_publishing(
    root: &Path,
    store: &Store,
    collection: &str,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    cache: Option<&mut ContentCache>,
    progress: &ProgressReporter,
    publisher: Option<crate::reconciliation::Publisher>,
) -> Result<ScanReport> {
    progress.begin_run();
    let result = scan_project_observed_inner(
        root,
        store,
        collection,
        changes,
        should_continue,
        cache,
        progress,
        publisher,
    );
    match &result {
        Ok(report) if report.cancelled => progress.finish_run(ProgressPhase::Cancelled),
        Ok(report) if !report.coverage.discovery_complete => {
            progress.finish_run(ProgressPhase::Failed)
        }
        Ok(_) => progress.finish_run(ProgressPhase::Complete),
        Err(error) if error.to_string().contains("indexing_cancelled") => {
            progress.record_cancellation();
            progress.finish_run(ProgressPhase::Cancelled);
        }
        Err(_) => progress.finish_run(ProgressPhase::Failed),
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn scan_project_observed_inner(
    root: &Path,
    store: &Store,
    collection: &str,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    mut cache: Option<&mut ContentCache>,
    progress: &ProgressReporter,
    publisher: Option<crate::reconciliation::Publisher>,
) -> Result<ScanReport> {
    let root = fs::canonicalize(root)
        .with_context(|| format!("canonicalizing project root {}", root.display()))?;
    let root_metadata =
        fs::metadata(&root).with_context(|| format!("reading project root {}", root.display()))?;
    if !root_metadata.is_dir() {
        return Err(anyhow!(
            "project root is not a directory: {}",
            root.display()
        ));
    }

    let all_existing = store
        .sources(collection, PROJECT_SOURCE_KIND)
        .context("listing existing project sources")?;
    let precise_paths = changes.map(|paths| {
        paths
            .iter()
            .map(|path| canonical_or_normalized(&resolve_project_candidate(&root, path)))
            .collect::<HashSet<_>>()
    });
    invalidate_project_scope(&root, store, collection, changes)?;
    let existing = all_existing
        .iter()
        .filter(|source| {
            precise_paths.as_ref().is_none_or(|paths| {
                paths.contains(&canonical_or_normalized(&root.join(&source.path)))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let existing_versions: HashMap<_, _> = existing
        .iter()
        .map(|source| (source.key.as_str(), source.version.as_str()))
        .collect();
    let mut report = ScanReport::default();
    report.coverage.publisher = publisher;
    report.coverage.full = changes.is_none();
    let mut seen = HashSet::new();
    let mut candidate_walk_failed = false;
    let precise_candidates = if let Some(paths) = precise_paths.as_ref() {
        let mut candidates = HashSet::new();
        for path in paths {
            if !path.starts_with(&root) {
                candidate_walk_failed = true;
                report.excluded_count = report.excluded_count.saturating_add(1);
                bump_diagnostic(&mut report, "outside_root");
                continue;
            }
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_file() => {
                    candidates.insert(path.clone());
                }
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    record_discovered_exclusion(
                        &mut report,
                        store,
                        collection,
                        &root,
                        path,
                        "symlink_excluded",
                    )?;
                }
                Ok(_) => {
                    record_discovered_exclusion(
                        &mut report,
                        store,
                        collection,
                        &root,
                        path,
                        "non_file_change",
                    )?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // Preserve deleted candidates so the final reconciliation
                    // removes their old source membership.
                    candidates.insert(path.clone());
                }
                Err(error) => {
                    let relative = normalize_relative(path.strip_prefix(&root)?);
                    let key = source_key(collection, &relative);
                    let mut source_report = ScanReport::default();
                    record_error(&mut source_report, format!("reading changed path: {error}"));
                    bump_diagnostic(&mut source_report, "source_read");
                    if existing.iter().any(|source| source.key == key) {
                        store.remove_source(&key)?;
                    }
                    seen.insert(key.clone());
                    report.record_source(store, key, None, source_report)?;
                }
            }
        }
        Some(candidates)
    } else {
        None
    };
    let (files, mut walk_failed) = discover_project_files(
        &root,
        store,
        collection,
        precise_candidates.as_ref(),
        should_continue,
        &mut report,
        progress,
    )?;
    walk_failed |= candidate_walk_failed;
    report.coverage.discovery_complete = !walk_failed;
    report.coverage.discovery = crate::coverage::SourceOutcome::from_report(&report);
    // Discovery may already have attributed exclusions to individual sources.
    for outcome in report.coverage.sources.values().flatten() {
        report.coverage.discovery.excluded -= outcome.excluded;
        report.coverage.discovery.errors -= outcome.errors;
        report.coverage.discovery.pending -= outcome.pending;
        for (category, count) in &outcome.diagnostics {
            if let Some(total) = report.coverage.discovery.diagnostics.get_mut(category) {
                *total -= count;
            }
        }
    }
    report
        .coverage
        .discovery
        .diagnostics
        .retain(|_, count| *count > 0);
    progress.record_discovered(files.len() as u64);

    for path in &files {
        ensure!(should_continue(), "indexing_cancelled");
        if is_git_metadata_path(path, &root) {
            continue;
        }
        let relative = match path.strip_prefix(&root) {
            Ok(relative) if !relative.as_os_str().is_empty() => normalize_relative(relative),
            _ => continue,
        };
        let key = source_key(collection, &relative);
        let mut source_report = ScanReport::default();
        let mut published_version = None;
        progress.record_current_source_bytes(0);
        progress.set_phase(ProgressPhase::Normalization);
        match read_source(
            path,
            existing_versions.get(key.as_str()).copied(),
            should_continue,
            cache.as_deref_mut(),
            progress,
        ) {
            Ok(ReadOutcome::Unchanged { version }) => {
                seen.insert(key.clone());
                published_version = Some(version.clone());
                let source = Source {
                    key: key.clone(),
                    collection: collection.to_owned(),
                    path: relative,
                    version,
                    kind: PROJECT_SOURCE_KIND.to_owned(),
                };
                let chunk_count = store
                    .mark_source_verified(&key)
                    .with_context(|| format!("verifying unchanged source {}", source.path))?;
                source_report.sources = 1;
                source_report.chunks = chunk_count;
                bump_diagnostic(&mut source_report, "unchanged_index_reuse");
                progress.record_file_completed();
            }
            Ok(ReadOutcome::Cached {
                version,
                chunks,
                chunk_count,
            }) => {
                bump_diagnostic(&mut source_report, "content_cache_hit");
                seen.insert(key.clone());
                published_version = Some(version.clone());
                let source = Source {
                    key: key.clone(),
                    collection: collection.to_owned(),
                    path: relative,
                    version,
                    kind: PROJECT_SOURCE_KIND.to_owned(),
                };
                source_report.chunks = chunk_count;
                progress.record_prepared_chunks(chunk_count);
                if existing_versions.get(key.as_str()).copied() == Some(source.version.as_str()) {
                    store
                        .mark_source_verified(&key)
                        .with_context(|| format!("verifying unchanged source {}", source.path))?;
                } else {
                    let commit_started = Instant::now();
                    store
                        .replace_source(
                            &source,
                            chunks.map(|chunk| {
                                ensure!(should_continue(), "indexing_cancelled");
                                chunk
                            }),
                        )
                        .with_context(|| format!("replacing source {}", source.path))?;
                    progress.record_work(WorkKind::DurableTxn, commit_started.elapsed());
                    progress.record_committed_chunks(chunk_count);
                }
                source_report.sources = 1;
                progress.record_file_completed();
            }
            Ok(ReadOutcome::Included {
                version,
                spool,
                chunks,
            }) => {
                if cache.is_some() {
                    bump_diagnostic(&mut source_report, "content_cache_miss");
                }
                seen.insert(key.clone());
                published_version = Some(version.clone());
                let source = Source {
                    key: key.clone(),
                    collection: collection.to_owned(),
                    path: relative,
                    version: version.clone(),
                    kind: PROJECT_SOURCE_KIND.to_owned(),
                };
                source_report.chunks = chunks;
                let cache_copy = cache.as_ref().map(|_| spool.reopen()).transpose()?;
                if existing_versions.get(key.as_str()).copied() == Some(version.as_str()) {
                    drop(spool);
                    store
                        .mark_source_verified(&key)
                        .with_context(|| format!("verifying unchanged source {}", source.path))?;
                } else {
                    let commit_started = Instant::now();
                    store
                        .replace_source(
                            &source,
                            spool.map(|chunk| {
                                ensure!(should_continue(), "indexing_cancelled");
                                chunk
                            }),
                        )
                        .with_context(|| format!("replacing source {}", source.path))?;
                    progress.record_work(WorkKind::DurableTxn, commit_started.elapsed());
                    progress.record_committed_chunks(chunks);
                }
                if let (Some(content_cache), Some(cache_copy)) = (cache.as_deref_mut(), cache_copy)
                {
                    // SQLite is authoritative. A full cache volume or a
                    // transient cache-file failure must not turn a committed
                    // source replacement into a failed reconciliation.
                    let cache_started = Instant::now();
                    if let Err(error) = content_cache.put(&version, cache_copy) {
                        bump_diagnostic(&mut source_report, "content_cache_write_error");
                        record_error(&mut source_report, format!("content cache: {error}"));
                    }
                    progress.record_work(WorkKind::TempFileOps, cache_started.elapsed());
                }
                source_report.sources = 1;
                progress.record_file_completed();
            }
            Ok(ReadOutcome::Excluded { reason }) => {
                // An ineligible replacement must remove its old searchable
                // membership.  It is intentionally absent from `seen`.
                if cache.is_some() {
                    bump_diagnostic(&mut source_report, "content_cache_miss");
                }
                source_report.excluded_count = 1;
                if reason == "binary content" {
                    bump_diagnostic(&mut source_report, "binary_excluded");
                } else if reason == "unsupported or malformed text encoding" {
                    bump_diagnostic(&mut source_report, "unsupported_encoding");
                }
                if reason != "binary content" {
                    record_error(&mut source_report, format!("excluded {relative}: {reason}"));
                }
                if let Some(source) = existing.iter().find(|source| source.key == key) {
                    store.remove_source(&source.key)?;
                }
                seen.insert(key.clone());
                progress.record_file_completed();
            }
            Err(error) => {
                record_error(&mut source_report, format!("{relative}: {error}"));
                progress.record_file_completed();
            }
        }
        report.record_source(store, key, published_version.as_deref(), source_report)?;
        ensure!(should_continue(), "indexing_cancelled");
    }

    // A precise change set only reconciles the requested source keys. A full
    // walk may remove missing sources once traversal completed successfully.
    if !walk_failed {
        for source in &existing {
            if !seen.contains(&source.key) {
                store
                    .remove_source(&source.key)
                    .with_context(|| format!("removing stale source {}", source.key))?;
                if !report.coverage.sources.contains_key(&source.key) {
                    report.record_removal(store, source.key.clone())?;
                }
                progress.record_file_completed();
            }
        }
        if let Some(paths) = precise_paths {
            for path in paths {
                if let Ok(relative) = path.strip_prefix(&root) {
                    let key = source_key(collection, &normalize_relative(relative));
                    if !report.coverage.sources.contains_key(&key) {
                        report.record_removal(store, key)?;
                    }
                }
            }
        }
    }

    Ok(report)
}

fn discover_project_files(
    root: &Path,
    store: &Store,
    collection: &str,
    candidates: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    report: &mut ScanReport,
    progress: &ProgressReporter,
) -> Result<(Vec<PathBuf>, bool)> {
    let require_git_rules = project_identity(root)
        .map(|identity| identity.kind == crate::identity::RootKind::GitWorktree)
        .unwrap_or(false);
    let mut builder = WalkBuilder::new(root);
    builder
        .hidden(false)
        .git_global(true)
        .git_ignore(true)
        .git_exclude(true)
        // The ignore crate needs `require_git(true)` to parse a linked
        // worktree's `.git` file and follow its common-dir `info/exclude`.
        // Plain roots retain ordinary/global ignore behavior without Git
        // metadata requirements.
        .require_git(require_git_rules)
        .parents(true)
        .follow_links(false);
    if let Some(candidates) = candidates {
        let candidates = candidates.clone();
        builder.filter_entry(move |entry| {
            let entry_path = canonical_or_normalized(entry.path());
            candidates
                .iter()
                .any(|candidate| candidate == &entry_path || candidate.starts_with(&entry_path))
        });
    }

    let discovery_started = Instant::now();
    let mut files = Vec::new();
    let mut walk_failed = false;
    for entry in builder.build() {
        ensure!(should_continue(), "indexing_cancelled");
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                walk_failed = true;
                record_error(report, format!("walking project: {error}"));
                continue;
            }
        };
        let path = entry.path();
        if is_git_metadata_path(path, root) {
            if path.is_file() {
                record_discovered_exclusion(
                    report,
                    store,
                    collection,
                    root,
                    path,
                    "git_metadata_excluded",
                )?;
            }
            continue;
        }
        let file_type = match entry.file_type() {
            Some(file_type) => file_type,
            None => {
                record_discovered_exclusion(
                    report,
                    store,
                    collection,
                    root,
                    path,
                    "non_file_change",
                )?;
                continue;
            }
        };
        if !file_type.is_file() {
            if file_type.is_symlink() {
                record_discovered_exclusion(
                    report,
                    store,
                    collection,
                    root,
                    path,
                    "symlink_excluded",
                )?;
            }
            continue;
        }
        if let Some(candidates) = candidates
            && !candidates.contains(&canonical_or_normalized(path))
        {
            continue;
        }
        files.push(path.to_path_buf());
    }
    progress.record_work(WorkKind::Discovery, discovery_started.elapsed());
    Ok((files, walk_failed))
}

fn record_discovered_exclusion(
    report: &mut ScanReport,
    store: &Store,
    collection: &str,
    root: &Path,
    path: &Path,
    category: &str,
) -> Result<()> {
    let mut outcome = ScanReport {
        excluded_count: 1,
        ..ScanReport::default()
    };
    bump_diagnostic(&mut outcome, category);
    let relative = path
        .strip_prefix(root)
        .expect("discovery stays within root");
    report.record_source(
        store,
        source_key(collection, &normalize_relative(relative)),
        None,
        outcome,
    )
}

fn canonical_or_normalized(path: &Path) -> PathBuf {
    if let Ok(path) = fs::canonicalize(path) {
        return path;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        use std::path::Component;
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    crate::identity::canonical_or_normalized(&normalized)
}

fn resolve_project_candidate(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

/// A stable source key derived from a collection and normalized relative path.
fn source_key(collection: &str, path: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(collection.as_bytes());
    digest.update([0]);
    digest.update(path.as_bytes());
    hex_digest(digest.finalize())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}

fn normalize_relative(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn is_git_metadata_path(path: &Path, root: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    relative
        .components()
        .any(|component| component.as_os_str() == OsStr::new(".git"))
}

fn record_error(report: &mut ScanReport, message: String) {
    report.error_count = report.error_count.saturating_add(1);
    if report.errors.len() < MAX_REPORTED_ERRORS {
        report.errors.push(truncate_error(message));
    }
}

fn bump_diagnostic(report: &mut ScanReport, key: &str) {
    let value = report.diagnostics.entry(key.to_owned()).or_default();
    *value = value.saturating_add(1);
}

fn truncate_error(mut message: String) -> String {
    if message.len() <= MAX_ERROR_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_ERROR_MESSAGE_BYTES.saturating_sub("…".len());
    while !message.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    message.truncate(end);
    message.push('…');
    message
}

#[derive(Debug)]
enum ReadOutcome {
    Unchanged {
        version: String,
    },
    Cached {
        version: String,
        chunks: CachedChunks,
        chunk_count: u64,
    },
    Included {
        version: String,
        spool: SpoolChunks,
        chunks: u64,
    },
    Excluded {
        reason: String,
    },
}

fn read_source(
    path: &Path,
    known_version: Option<&str>,
    should_continue: &dyn Fn() -> bool,
    mut cache: Option<&mut ContentCache>,
    progress: &ProgressReporter,
) -> Result<ReadOutcome> {
    let mut last_error = None;
    for attempt in 0..MAX_RETRIES {
        if attempt != 0 {
            progress.record_retry();
        }
        match read_source_once(
            path,
            known_version,
            should_continue,
            cache.as_deref_mut(),
            progress,
        ) {
            Ok(outcome) => return Ok(outcome),
            Err(error) if error.downcast_ref::<UnstableRead>().is_some() => {
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("source changed while reading")))
}

#[derive(Debug)]
struct UnstableRead;

impl std::fmt::Display for UnstableRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("source changed while reading")
    }
}

impl std::error::Error for UnstableRead {}

fn read_source_once(
    path: &Path,
    known_version: Option<&str>,
    should_continue: &dyn Fn() -> bool,
    cache: Option<&mut ContentCache>,
    progress: &ProgressReporter,
) -> Result<ReadOutcome> {
    let before = file_fingerprint(path)?;
    progress.set_phase(ProgressPhase::PrefixVerification);
    let version = raw_digest(path, should_continue, progress)?;
    if before != file_fingerprint(path)? {
        return Err(UnstableRead.into());
    }
    if known_version == Some(version.as_str()) {
        return Ok(ReadOutcome::Unchanged { version });
    }
    if let Some(content_cache) = cache
        && let Some(chunks) = content_cache.get(&version)?
    {
        // A cache hit still needs the same before/after identity check as a
        // decode. Otherwise a file changed during lookup could publish
        // chunks for the earlier digest.
        if before != file_fingerprint(path)? {
            return Err(UnstableRead.into());
        }
        let chunk_count = chunks.chunk_count();
        return Ok(ReadOutcome::Cached {
            version,
            chunks,
            chunk_count,
        });
    }

    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    if !file
        .metadata()
        .with_context(|| format!("reading metadata for {}", path.display()))?
        .is_file()
    {
        return Ok(ReadOutcome::Excluded {
            reason: "not a regular file".to_owned(),
        });
    }

    let spool = ChunkSpool::new()?;
    let mut spool = spool;
    let mut chunker = Chunker {
        progress: Some(progress.clone()),
        ..Chunker::default()
    };
    let mut reader = BufReader::with_capacity(READ_BUFFER_BYTES, file);
    let mut prefix = Vec::with_capacity(3);
    let mut prefix_buf = [0u8; 3];
    while prefix.len() < 3 {
        let count = reader.read(&mut prefix_buf[..3 - prefix.len()])?;
        if count == 0 {
            break;
        }
        progress.record_source_bytes(count as u64, 0);
        prefix.extend_from_slice(&prefix_buf[..count]);
    }

    let (encoding, bom_len) = detect_encoding(&prefix);
    progress.set_phase(ProgressPhase::Tokenization);
    let mut decoder = Decoder::new(encoding);
    let mut process = |bytes: &[u8], offset: u64| -> std::result::Result<(), DecodeIssue> {
        progress.record_work_bytes(WorkKind::Tokenization, bytes.len() as u64);
        decoder.feed(bytes, offset, &mut |character| {
            chunker.push(character, &mut spool)
        })
    };
    if prefix.len() > bom_len
        && let Err(issue) = process(&prefix[bom_len..], bom_len as u64)
    {
        if issue.excluded {
            return Ok(ReadOutcome::Excluded {
                reason: issue.message,
            });
        }
        return Err(anyhow!(issue.message));
    }

    let mut offset = prefix.len() as u64;
    let mut buffer = [0u8; READ_BUFFER_BYTES];
    loop {
        ensure!(should_continue(), "indexing_cancelled");
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if let Err(issue) = process(&buffer[..count], offset) {
            if issue.excluded {
                return Ok(ReadOutcome::Excluded {
                    reason: issue.message,
                });
            }
            return Err(anyhow!(issue.message));
        }
        progress.record_source_bytes(count as u64, 0);
        offset = offset.saturating_add(count as u64);
    }
    if let Err(issue) = decoder.finish(&mut |character| chunker.push(character, &mut spool)) {
        if issue.excluded {
            return Ok(ReadOutcome::Excluded {
                reason: issue.message,
            });
        }
        return Err(anyhow!(issue.message));
    }
    chunker.finish(&mut spool)?;
    let spool = spool.finish()?;
    let after = file_fingerprint(path)?;
    if before != after {
        return Err(UnstableRead.into());
    }
    Ok(ReadOutcome::Included {
        version,
        spool,
        chunks: chunker.chunks,
    })
}

fn raw_digest(
    path: &Path,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<String> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::with_capacity(READ_BUFFER_BYTES, file);
    let mut digest = Sha256::new();
    let mut buffer = [0u8; READ_BUFFER_BYTES];
    loop {
        ensure!(should_continue(), "indexing_cancelled");
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        progress.record_source_bytes(count as u64, count as u64);
    }
    Ok(hex_digest(digest.finalize()))
}

#[derive(Clone, Copy, Debug)]
enum Encoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

fn detect_encoding(prefix: &[u8]) -> (Encoding, usize) {
    if prefix.starts_with(&[0xef, 0xbb, 0xbf]) {
        (Encoding::Utf8, 3)
    } else if prefix.starts_with(&[0xff, 0xfe]) {
        (Encoding::Utf16Le, 2)
    } else if prefix.starts_with(&[0xfe, 0xff]) {
        (Encoding::Utf16Be, 2)
    } else {
        (Encoding::Utf8, 0)
    }
}

#[derive(Clone, Copy, Debug)]
struct DecodedChar {
    character: char,
    start_byte: u64,
    end_byte: u64,
}

#[derive(Debug)]
struct DecodeIssue {
    message: String,
    excluded: bool,
}

impl DecodeIssue {
    fn invalid_encoding() -> Self {
        Self {
            message: "unsupported or malformed text encoding".to_owned(),
            excluded: true,
        }
    }

    fn binary() -> Self {
        Self {
            message: "binary content".to_owned(),
            excluded: true,
        }
    }

    fn callback(error: anyhow::Error) -> Self {
        if error.downcast_ref::<BinaryContent>().is_some() {
            Self::binary()
        } else {
            Self {
                message: error.to_string(),
                excluded: false,
            }
        }
    }
}

#[derive(Debug)]
struct BinaryContent;

impl std::fmt::Display for BinaryContent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("binary content")
    }
}

impl std::error::Error for BinaryContent {}

struct Decoder {
    kind: Encoding,
    utf8: Utf8Decoder,
    utf16: Utf16Decoder,
}

impl Decoder {
    fn new(kind: Encoding) -> Self {
        Self {
            kind,
            utf8: Utf8Decoder::default(),
            utf16: Utf16Decoder::new(matches!(kind, Encoding::Utf16Be)),
        }
    }

    fn feed<F>(
        &mut self,
        bytes: &[u8],
        offset: u64,
        callback: &mut F,
    ) -> std::result::Result<(), DecodeIssue>
    where
        F: FnMut(DecodedChar) -> Result<()>,
    {
        match self.kind {
            Encoding::Utf8 => self.utf8.feed(bytes, offset, callback),
            Encoding::Utf16Le | Encoding::Utf16Be => self.utf16.feed(bytes, offset, callback),
        }
    }

    fn finish<F>(&mut self, callback: &mut F) -> std::result::Result<(), DecodeIssue>
    where
        F: FnMut(DecodedChar) -> Result<()>,
    {
        match self.kind {
            Encoding::Utf8 => self.utf8.finish(),
            Encoding::Utf16Le | Encoding::Utf16Be => self.utf16.finish(callback),
        }
    }
}

#[derive(Default)]
struct Utf8Decoder {
    pending: Vec<(u8, u64)>,
}

impl Utf8Decoder {
    fn feed<F>(
        &mut self,
        bytes: &[u8],
        offset: u64,
        callback: &mut F,
    ) -> std::result::Result<(), DecodeIssue>
    where
        F: FnMut(DecodedChar) -> Result<()>,
    {
        for (index, byte) in bytes.iter().copied().enumerate() {
            let byte_offset = offset.saturating_add(index as u64);
            if self.pending.is_empty() {
                if byte.is_ascii() {
                    callback(DecodedChar {
                        character: byte as char,
                        start_byte: byte_offset,
                        end_byte: byte_offset + 1,
                    })
                    .map_err(DecodeIssue::callback)?;
                } else {
                    let expected: usize = match byte {
                        0xc2..=0xdf => 2,
                        0xe0..=0xef => 3,
                        0xf0..=0xf4 => 4,
                        _ => return Err(DecodeIssue::invalid_encoding()),
                    };
                    self.pending.push((byte, byte_offset));
                    self.pending.reserve(expected.saturating_sub(1));
                }
                continue;
            }

            if !(0x80..=0xbf).contains(&byte) {
                return Err(DecodeIssue::invalid_encoding());
            }
            self.pending.push((byte, byte_offset));
            let expected: usize = match self.pending[0].0 {
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => return Err(DecodeIssue::invalid_encoding()),
            };
            if self.pending.len() == expected {
                let raw = self
                    .pending
                    .iter()
                    .map(|(byte, _)| *byte)
                    .collect::<Vec<_>>();
                let text =
                    std::str::from_utf8(&raw).map_err(|_| DecodeIssue::invalid_encoding())?;
                let character = text
                    .chars()
                    .next()
                    .ok_or_else(DecodeIssue::invalid_encoding)?;
                let start_byte = self.pending[0].1;
                let end_byte = self.pending[expected - 1].1 + 1;
                self.pending.clear();
                callback(DecodedChar {
                    character,
                    start_byte,
                    end_byte,
                })
                .map_err(DecodeIssue::callback)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> std::result::Result<(), DecodeIssue> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(DecodeIssue::invalid_encoding())
        }
    }
}

struct Utf16Decoder {
    big_endian: bool,
    pending_byte: Option<(u8, u64)>,
    high_surrogate: Option<(u16, u64, u64)>,
}

impl Utf16Decoder {
    fn new(big_endian: bool) -> Self {
        Self {
            big_endian,
            pending_byte: None,
            high_surrogate: None,
        }
    }

    fn feed<F>(
        &mut self,
        bytes: &[u8],
        offset: u64,
        callback: &mut F,
    ) -> std::result::Result<(), DecodeIssue>
    where
        F: FnMut(DecodedChar) -> Result<()>,
    {
        for (index, byte) in bytes.iter().copied().enumerate() {
            let byte_offset = offset.saturating_add(index as u64);
            let Some((first, first_offset)) = self.pending_byte.take() else {
                self.pending_byte = Some((byte, byte_offset));
                continue;
            };
            let unit = if self.big_endian {
                u16::from_be_bytes([first, byte])
            } else {
                u16::from_le_bytes([first, byte])
            };
            self.push_unit(unit, first_offset, byte_offset + 1, callback)?;
        }
        Ok(())
    }

    fn push_unit<F>(
        &mut self,
        unit: u16,
        start: u64,
        end: u64,
        callback: &mut F,
    ) -> std::result::Result<(), DecodeIssue>
    where
        F: FnMut(DecodedChar) -> Result<()>,
    {
        if let Some((high, high_start, _high_end)) = self.high_surrogate.take() {
            if !(0xdc00..=0xdfff).contains(&unit) {
                return Err(DecodeIssue::invalid_encoding());
            }
            let codepoint = 0x1_0000 + (((high as u32) - 0xd800) << 10) + (unit as u32 - 0xdc00);
            let character = char::from_u32(codepoint).ok_or_else(DecodeIssue::invalid_encoding)?;
            callback(DecodedChar {
                character,
                start_byte: high_start,
                end_byte: end,
            })
            .map_err(DecodeIssue::callback)?;
            return Ok(());
        }
        if (0xd800..=0xdbff).contains(&unit) {
            self.high_surrogate = Some((unit, start, end));
        } else if (0xdc00..=0xdfff).contains(&unit) {
            return Err(DecodeIssue::invalid_encoding());
        } else {
            let character =
                char::from_u32(unit as u32).ok_or_else(DecodeIssue::invalid_encoding)?;
            callback(DecodedChar {
                character,
                start_byte: start,
                end_byte: end,
            })
            .map_err(DecodeIssue::callback)?;
        }
        Ok(())
    }

    fn finish<F>(&mut self, _callback: &mut F) -> std::result::Result<(), DecodeIssue>
    where
        F: FnMut(DecodedChar) -> Result<()>,
    {
        if self.pending_byte.is_some() || self.high_surrogate.is_some() {
            Err(DecodeIssue::invalid_encoding())
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct Chunker {
    text: String,
    tokens: Vec<String>,
    tokenizer: StreamingTokenizer,
    decoded_chars: u64,
    disallowed_controls: u64,
    start_line: u64,
    end_line: u64,
    start_byte: u64,
    end_byte: u64,
    current_line: u64,
    lines: u64,
    chunks: u64,
    progress: Option<ProgressReporter>,
}

impl Chunker {
    fn push(&mut self, decoded: DecodedChar, spool: &mut ChunkSpool) -> Result<()> {
        if decoded.character == '\0' {
            return Err(BinaryContent.into());
        }
        self.decoded_chars = self.decoded_chars.saturating_add(1);
        if decoded.character.is_control()
            && !matches!(
                decoded.character,
                '\n' | '\r' | '\t' | '\u{000b}' | '\u{000c}'
            )
        {
            self.disallowed_controls = self.disallowed_controls.saturating_add(1);
            // Valid UTF-8 is not automatically human-readable text. Reject a
            // control-heavy stream once enough evidence exists, while
            // allowing an isolated control in otherwise useful source.
            if self.disallowed_controls >= 8
                && self.disallowed_controls.saturating_mul(5) >= self.decoded_chars
            {
                return Err(BinaryContent.into());
            }
        }
        if !self.text.is_empty()
            && self.text.len().saturating_add(decoded.character.len_utf8()) > MAX_CHUNK_BYTES
        {
            self.flush(spool)?;
        }
        if self.text.is_empty() {
            if self.current_line == 0 {
                self.current_line = 1;
            }
            self.start_line = self.current_line;
            self.start_byte = decoded.start_byte;
        }
        self.text.push(decoded.character);
        self.end_line = self.current_line;
        self.end_byte = decoded.end_byte;
        self.tokenizer
            .try_push(decoded.character, &mut |term| self.tokens.push(term))
            .map_err(|error| anyhow!("tokenizer spill: {error}"))?;
        if decoded.character == '\n' {
            self.lines = self.lines.saturating_add(1);
            self.current_line = self.current_line.saturating_add(1);
        }
        if self.text.len() >= MAX_CHUNK_BYTES
            || (decoded.character == '\n' && self.lines >= APPROX_LINES_PER_CHUNK)
        {
            self.flush(spool)?;
        }
        Ok(())
    }

    fn finish(&mut self, spool: &mut ChunkSpool) -> Result<()> {
        self.tokenizer
            .try_finish(&mut |term| self.tokens.push(term))
            .map_err(|error| anyhow!("tokenizer spill: {error}"))?;
        self.flush(spool)
    }

    fn flush(&mut self, spool: &mut ChunkSpool) -> Result<()> {
        if self.text.is_empty() {
            self.lines = 0;
            return Ok(());
        }
        let chunk = Chunk {
            field_kind: None,
            text: std::mem::take(&mut self.text),
            tokens: (!self.tokens.is_empty()).then(|| std::mem::take(&mut self.tokens)),
            start_line: self.start_line,
            end_line: self.end_line,
            start_byte: self.start_byte,
            end_byte: self.end_byte,
            ..Chunk::default()
        };
        spool.push(&chunk)?;
        if let Some(progress) = &self.progress {
            progress.record_work_bytes(WorkKind::TempFileOps, chunk.text.len() as u64);
        }
        self.chunks = self.chunks.saturating_add(1);
        if let Some(progress) = &self.progress {
            progress.record_prepared_chunks(1);
        }
        self.lines = 0;
        Ok(())
    }
}

struct ChunkSpool {
    path: Option<PathBuf>,
    writer: Option<BufWriter<File>>,
}

impl std::fmt::Debug for ChunkSpool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChunkSpool")
            .field("path", &self.path)
            .finish()
    }
}

impl ChunkSpool {
    fn new() -> Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let temp_dir = std::env::temp_dir();
        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = temp_dir.join(format!(
                "bm25-mcp-spool-{}-{timestamp}-{id}.jsonl",
                std::process::id()
            ));
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(0o600));
                    }
                    return Ok(Self {
                        path: Some(path),
                        writer: Some(BufWriter::new(file)),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(anyhow!("could not create a unique ingestion spool"))
    }

    fn push(&mut self, chunk: &Chunk) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("spool already finished"))?;
        serde_json::to_writer(&mut *writer, chunk)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn finish(mut self) -> Result<SpoolChunks> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
        }
        let path = self
            .path
            .take()
            .ok_or_else(|| anyhow!("spool already finished"))?;
        let reader = match File::open(&path) {
            Ok(file) => BufReader::with_capacity(READ_BUFFER_BYTES, file),
            Err(error) => {
                let _ = fs::remove_file(&path);
                return Err(error.into());
            }
        };
        Ok(SpoolChunks {
            path: Arc::new(path),
            reader: Some(reader),
            line: String::new(),
        })
    }
}

impl Drop for ChunkSpool {
    fn drop(&mut self) {
        self.writer.take();
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[derive(Debug)]
struct SpoolChunks {
    path: Arc<PathBuf>,
    reader: Option<BufReader<File>>,
    line: String,
}

impl SpoolChunks {
    fn reopen(&self) -> Result<Self> {
        let reader = File::open(self.path.as_path())
            .with_context(|| format!("reopen ingestion spool {}", self.path.display()))?;
        Ok(Self {
            path: self.path.clone(),
            reader: Some(BufReader::with_capacity(READ_BUFFER_BYTES, reader)),
            line: String::new(),
        })
    }
}

impl Iterator for SpoolChunks {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.line.clear();
        let reader = self.reader.as_mut()?;
        match reader.read_line(&mut self.line) {
            Ok(0) => None,
            Ok(_) => Some(serde_json::from_str(self.line.trim_end()).map_err(Into::into)),
            Err(error) => Some(Err(error.into())),
        }
    }
}

impl Drop for SpoolChunks {
    fn drop(&mut self) {
        self.reader.take();
        if Arc::strong_count(&self.path) == 1 {
            let _ = fs::remove_file(self.path.as_path());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

fn file_fingerprint(path: &Path) -> Result<FileFingerprint> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("reading metadata for {}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(anyhow!("source is not a regular file"));
    }
    Ok(fingerprint(&metadata))
}

fn fingerprint(metadata: &Metadata) -> FileFingerprint {
    FileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().ok(),
        created: metadata.created().ok(),
        #[cfg(unix)]
        device: {
            use std::os::unix::fs::MetadataExt;
            metadata.dev()
        },
        #[cfg(unix)]
        inode: {
            use std::os::unix::fs::MetadataExt;
            metadata.ino()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SearchFilter;
    use std::process::Command;
    use tempfile::TempDir;

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("git is installed for ingestion tests");
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn write_bytes(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write fixture");
    }

    fn store_for(temp: &TempDir) -> Store {
        static STORE_ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "bm25-mcp-ingest-test-{}-{}.sqlite3",
            std::process::id(),
            STORE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = temp;
        Store::open(&path).expect("open test store")
    }

    #[test]
    fn project_identity_is_canonical_and_hash_backed() {
        let temp = TempDir::new().expect("temp root");
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).expect("nested root");
        let identity = project_identity(&nested.join("..").join("nested")).expect("identity");
        assert_eq!(identity.root, fs::canonicalize(&nested).unwrap());
        assert_eq!(identity.collection.len(), 64);
        assert_eq!(identity.owner_key.len(), 64);
        assert_eq!(
            identity.collection,
            project_identity(&nested).unwrap().collection
        );
    }

    #[test]
    fn scan_honors_gitignore_for_tracked_files_and_reconciles_binary_deletion() {
        let temp = TempDir::new().expect("temp root");
        let root = temp.path();
        git(root, &["init", "-q"]);
        git(root, &["config", "user.email", "test@example.invalid"]);
        git(root, &["config", "user.name", "Ingestion Test"]);
        write_bytes(&root.join(".gitignore"), b"ignored.txt\nignored-dir/\n");
        write_bytes(&root.join("visible.txt"), b"visible needle\n");
        write_bytes(&root.join("ignored.txt"), b"should not be indexed\n");
        fs::create_dir(root.join("ignored-dir")).unwrap();
        write_bytes(&root.join("ignored-dir/file.txt"), b"ignored nested\n");
        write_bytes(&root.join("binary.dat"), &[0, 1, 2, 3]);
        git(root, &["add", ".gitignore", "visible.txt"]);
        git(root, &["add", "-f", "ignored-dir/file.txt"]);
        git(root, &["add", "-f", "ignored.txt"]);
        git(root, &["commit", "-qm", "fixtures"]);

        let store = store_for(&temp);
        let identity = project_identity(root).unwrap();
        let first = scan_project(root, &store, &identity.collection).expect("first scan");
        assert!(first.sources >= 2);
        let sources = store
            .sources(&identity.collection, PROJECT_SOURCE_KIND)
            .unwrap();
        let paths: HashSet<_> = sources.iter().map(|source| source.path.as_str()).collect();
        assert!(paths.contains("visible.txt"));
        assert!(paths.contains(".gitignore"));
        assert!(!paths.contains("ignored.txt"));
        assert!(!paths.contains("ignored-dir/file.txt"));
        assert!(!paths.contains("binary.dat"));

        // A previously searchable text source becoming binary is removed on
        // the next reconciliation rather than leaving stale postings behind.
        write_bytes(&root.join("visible.txt"), &[0, 9, 0, 8]);
        let second = scan_project(root, &store, &identity.collection).expect("binary scan");
        assert!(second.excluded_count > 0);
        assert!(
            !store
                .sources(&identity.collection, PROJECT_SOURCE_KIND)
                .unwrap()
                .iter()
                .any(|source| source.path == "visible.txt")
        );

        fs::remove_file(root.join(".gitignore")).unwrap();
        let _third = scan_project(root, &store, &identity.collection).expect("deletion scan");
        assert!(
            !store
                .sources(&identity.collection, PROJECT_SOURCE_KIND)
                .unwrap()
                .iter()
                .any(|source| source.path == ".gitignore")
        );
    }

    #[test]
    fn unchanged_index_reuse_survives_missing_and_evicted_content_cache() {
        let temp = TempDir::new().expect("temp root");
        let root = temp.path().join("project");
        fs::create_dir(&root).unwrap();
        let source_path = root.join("source.txt");
        write_bytes(&source_path, b"reuse_marker alpha\n");

        let store = store_for(&temp);
        let identity = project_identity(&root).unwrap();
        let filter = SearchFilter {
            collection: identity.collection.clone(),
            kind: PROJECT_SOURCE_KIND.to_owned(),
            ..SearchFilter::default()
        };
        let cache_path = temp.path().join("content-cache");

        // The first pass has no cache at all. A later pass must reuse the
        // authoritative indexed chunks after hashing, without decoding again.
        let first = scan_project(&root, &store, &identity.collection).unwrap();
        let (_, first_hits) = store.search("reuse_marker", &filter, 10).unwrap();
        assert_eq!(first_hits.len(), 1);
        let first_signature: Vec<_> = first_hits
            .iter()
            .map(|hit| (hit.source.path.clone(), hit.score))
            .collect();

        let mut cache = ContentCache::open(&cache_path).unwrap();
        let second =
            scan_project_with_cache(&root, &store, &identity.collection, &mut cache).unwrap();
        assert_eq!(second.diagnostics.get("unchanged_index_reuse"), Some(&1));
        assert_eq!(second.diagnostics.get("content_cache_miss"), None);
        assert_eq!(second.chunks, first.chunks);
        let (_, second_hits) = store.search("reuse_marker", &filter, 10).unwrap();
        let second_signature: Vec<_> = second_hits
            .iter()
            .map(|hit| (hit.source.path.clone(), hit.score))
            .collect();
        assert_eq!(second_signature, first_signature);

        // Populate the cache for a changed version, then evict that entry.
        write_bytes(&source_path, b"rewrite_marker beta\n");
        let rewritten =
            scan_project_with_cache(&root, &store, &identity.collection, &mut cache).unwrap();
        assert_eq!(rewritten.diagnostics.get("unchanged_index_reuse"), None);
        assert_eq!(rewritten.diagnostics.get("content_cache_miss"), Some(&1));
        assert!(
            fs::read_dir(&cache_path)
                .unwrap()
                .flatten()
                .any(|entry| entry.path().extension().and_then(OsStr::to_str) == Some("jsonl"))
        );
        drop(cache);
        let mut evicting_cache = ContentCache::open_with_limits(&cache_path, 1, 1).unwrap();
        assert!(
            !fs::read_dir(&cache_path)
                .unwrap()
                .flatten()
                .any(|entry| entry.path().extension().and_then(OsStr::to_str) == Some("jsonl"))
        );
        let (_, rewritten_hits) = store.search("rewrite_marker", &filter, 10).unwrap();
        assert_eq!(rewritten_hits.len(), 1);
        let rewritten_signature: Vec<_> = rewritten_hits
            .iter()
            .map(|hit| (hit.source.path.clone(), hit.score))
            .collect();
        let reused_after_eviction =
            scan_project_with_cache(&root, &store, &identity.collection, &mut evicting_cache)
                .unwrap();
        assert_eq!(
            reused_after_eviction
                .diagnostics
                .get("unchanged_index_reuse"),
            Some(&1)
        );
        assert_eq!(
            reused_after_eviction.diagnostics.get("content_cache_miss"),
            None
        );
        assert_eq!(reused_after_eviction.chunks, rewritten.chunks);
        let (_, reused_hits) = store.search("rewrite_marker", &filter, 10).unwrap();
        let reused_signature: Vec<_> = reused_hits
            .iter()
            .map(|hit| (hit.source.path.clone(), hit.score))
            .collect();
        assert_eq!(reused_signature, rewritten_signature);

        // A changed digest must bypass the fast path. Binary replacement is
        // excluded and cannot leave the old searchable postings behind.
        write_bytes(&source_path, &[0, 1, 2, 3]);
        let binary =
            scan_project_with_cache(&root, &store, &identity.collection, &mut evicting_cache)
                .unwrap();
        assert_eq!(binary.diagnostics.get("unchanged_index_reuse"), None);
        assert!(binary.diagnostics.contains_key("binary_excluded"));
        let (_, stale_hits) = store.search("rewrite_marker", &filter, 10).unwrap();
        assert!(stale_hits.is_empty());
    }

    #[test]
    fn scan_decodes_supported_boms_and_splits_giant_lines() {
        let temp = TempDir::new().expect("temp root");
        let root = temp.path();
        write_bytes(&root.join("utf8-bom.txt"), b"\xef\xbb\xbfUTF8 needle\n");
        let utf16le: Vec<u8> = "UTF16LE needle\n"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let mut utf16le_bom = vec![0xff, 0xfe];
        utf16le_bom.extend(utf16le);
        write_bytes(&root.join("utf16-le.txt"), &utf16le_bom);
        let utf16be: Vec<u8> = "UTF16BE needle\n"
            .encode_utf16()
            .flat_map(u16::to_be_bytes)
            .collect();
        let mut utf16be_bom = vec![0xfe, 0xff];
        utf16be_bom.extend(utf16be);
        write_bytes(&root.join("utf16-be.txt"), &utf16be_bom);
        let giant = format!("{}GIANT_NEEDLE{}\n", "x".repeat(20_000), "y".repeat(20_000));
        write_bytes(&root.join("giant.txt"), giant.as_bytes());

        let store = store_for(&temp);
        let identity = project_identity(root).unwrap();
        let report = scan_project(root, &store, &identity.collection).expect("encoding scan");
        assert_eq!(report.sources, 4);
        assert!(report.chunks >= 4);
        assert_eq!(report.error_count, 0);
        let sources = store
            .sources(&identity.collection, PROJECT_SOURCE_KIND)
            .unwrap();
        assert_eq!(sources.len(), 4);
        let giant_source = sources
            .iter()
            .find(|source| source.path == "giant.txt")
            .unwrap();
        let filter = SearchFilter {
            collection: identity.collection,
            kind: PROJECT_SOURCE_KIND.to_owned(),
            ..SearchFilter::default()
        };
        let (_generation, hits) = store.search("GIANT_NEEDLE", &filter, 10).unwrap();
        assert!(hits.iter().any(|hit| hit.source.key == giant_source.key));
        assert!(
            hits.iter()
                .all(|hit| hit.chunk.text.len() <= MAX_CHUNK_BYTES)
        );
    }

    #[test]
    fn identifiers_crossing_chunk_boundaries_are_searchable_without_partial_terms() {
        let temp = TempDir::new().expect("temp root");
        let root = temp.path();
        let identifier = format!("{}NeedleToken", "Boundary");
        // The delimiter leaves only three bytes of the identifier in the
        // first 16 KiB chunk. The tokenizer must retain the unfinished run
        // and attach the completed whole form to the following chunk.
        let contents = format!("{} {}\n", "x".repeat(MAX_CHUNK_BYTES - 3), identifier);
        write_bytes(&root.join("boundary.txt"), contents.as_bytes());

        let store = store_for(&temp);
        let identity = project_identity(root).unwrap();
        let report = scan_project(root, &store, &identity.collection).unwrap();
        assert_eq!(report.sources, 1);
        let filter = SearchFilter {
            collection: identity.collection.clone(),
            kind: PROJECT_SOURCE_KIND.to_owned(),
            ..SearchFilter::default()
        };
        let (_, full_hits) = store.search(&identifier, &filter, 10).unwrap();
        assert_eq!(full_hits.len(), 1);
        assert!(full_hits[0].chunk.text.contains("NeedleToken"));

        // A prefix that only exists as a physical chunk fragment is not a
        // complete lexical occurrence and must not become searchable.
        let (_, partial_hits) = store.search("Bound", &filter, 10).unwrap();
        assert!(partial_hits.is_empty());
    }

    #[test]
    fn control_heavy_valid_utf8_is_excluded_as_binary() {
        let temp = TempDir::new().expect("temp root");
        let root = temp.path();
        let mut bytes = Vec::new();
        bytes.extend(std::iter::repeat_n(1u8, 12));
        bytes.extend_from_slice(b"visible-looking-bytes");
        write_bytes(&root.join("control-heavy.txt"), &bytes);

        let store = store_for(&temp);
        let identity = project_identity(root).unwrap();
        let report = scan_project(root, &store, &identity.collection).unwrap();
        assert_eq!(report.sources, 0);
        assert!(report.excluded_count >= 1);
    }

    #[test]
    fn linked_worktree_uses_common_info_exclude() {
        let temp = TempDir::new().expect("temp root");
        let main = temp.path().join("main");
        let linked = temp.path().join("linked");
        fs::create_dir(&main).unwrap();
        git(&main, &["init", "-q"]);
        git(&main, &["config", "user.email", "test@example.invalid"]);
        git(&main, &["config", "user.name", "Ingestion Test"]);
        write_bytes(&main.join("tracked.txt"), b"tracked needle\n");
        git(&main, &["add", "tracked.txt"]);
        git(&main, &["commit", "-qm", "root"]);
        git(
            &main,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "linked",
                linked.to_str().unwrap(),
            ],
        );

        // This file lives in the repository common directory, while the
        // linked worktree itself has only a .git indirection file.
        write_bytes(&main.join(".git/info/exclude"), b"common-excluded.txt\n");
        write_bytes(&linked.join("common-excluded.txt"), b"should be excluded\n");
        write_bytes(&linked.join("visible.txt"), b"visible linked needle\n");

        let store = store_for(&temp);
        let identity = project_identity(&linked).unwrap();
        let report = scan_project(&linked, &store, &identity.collection).unwrap();
        assert_eq!(report.error_count, 0);
        let paths: HashSet<_> = store
            .sources(&identity.collection, PROJECT_SOURCE_KIND)
            .unwrap()
            .into_iter()
            .map(|source| source.path)
            .collect();
        assert!(paths.contains("visible.txt"));
        assert!(!paths.contains("common-excluded.txt"));
    }
}
