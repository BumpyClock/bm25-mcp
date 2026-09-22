//! Provider-neutral session-file streaming and durable incremental parsing.
//!
//! This module intentionally does not build a `serde_json::Value` for a
//! record.  Physical records are spooled to disk, selected strings are
//! decoded by `session_json`, and only bounded chunks enter SQLite.  The
//! checkpoint is committed in the same transaction as those chunks, so an
//! incomplete tail or an interrupted append is replayed safely.

use super::session_json::{CaptureFile, Fragment, MAX_FRAGMENT_BYTES, ParseCancelled, PathPart};
use super::{SESSION_KIND, SessionConfig};
use crate::identity::IdentityRegistry;
use crate::ingest::{ProjectIdentity, project_identity};
use crate::model::{Chunk, ScanReport, Source};
use crate::progress::{ProgressPhase, ProgressReporter, ScratchBatchMetrics, WorkKind};
use crate::record_spool::{FinalizeError, RecordSpool, Records};
use crate::store::{SessionCheckpoint, SessionStateReader, Store};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_CHUNK_BYTES: usize = 16 * 1024;
const MAX_REPORTED_ERRORS: usize = 64;
const MAX_ERROR_BYTES: usize = 512;
const MAX_METADATA_VALUE_BYTES: usize = 4096;
const MAX_BLOCK_TYPES_IN_MEMORY: usize = 256;
const MAX_CWD_CACHE_ENTRIES: usize = 4096;
const MAX_INLINE_DEDUP_BYTES: usize = 64 * 1024;
const SCRATCH_BATCH_WRITES: usize = 512;
// Version 7 also retains inspection diagnostics in the existing checkpoint
// counts. Older prefixes must be replayed to reconstruct truthful coverage.
const CHECKPOINT_SCHEMA: u32 = 7;
const STATE_KIND_OWN_CALL: &str = "own_call";
const STATE_KIND_TOOL_CALL: &str = "tool_call";
const STATE_KIND_SEEN_EVENT: &str = "seen_event";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Provider {
    Codex,
    Claude,
    Copilot,
}

impl Provider {
    fn name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Copilot => "copilot",
        }
    }
}

#[derive(Clone, Debug)]
struct SessionFile {
    path: PathBuf,
    provider: Provider,
}

#[derive(Debug)]
struct ScanCancelled;

impl std::fmt::Display for ScanCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("session scan cancelled")
    }
}

impl std::error::Error for ScanCancelled {}

#[derive(Debug)]
struct RecordSummary {
    fields: HashMap<String, String>,
    block_types: BlockTypeIndex,
    kind: Option<String>,
    cwd: Option<String>,
    timestamp_raw: Option<String>,
    session_id: Option<String>,
    event_id: Option<String>,
    role: Option<String>,
    channel: Option<String>,
    name: Option<String>,
    call_id: Option<String>,
    tool_use_id: Option<String>,
    metadata_truncated: bool,
}

struct BlockTypeIndex {
    memory: HashMap<String, String>,
    path: Option<PathBuf>,
    connection: Option<rusqlite::Connection>,
}

impl std::fmt::Debug for BlockTypeIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlockTypeIndex")
            .field("memory_len", &self.memory.len())
            .field("path", &self.path)
            .finish()
    }
}

impl BlockTypeIndex {
    fn new() -> Result<Self> {
        Ok(Self {
            memory: HashMap::new(),
            path: None,
            connection: None,
        })
    }

    fn insert(&mut self, path: &str, value: &str) -> Result<()> {
        if let Some(connection) = self.connection.as_ref() {
            connection.execute(
                "INSERT OR IGNORE INTO block_types(path,value) VALUES (?1,?2)",
                rusqlite::params![path, value],
            )?;
            return Ok(());
        }
        if self.memory.contains_key(path) {
            return Ok(());
        }
        if self.memory.len() < MAX_BLOCK_TYPES_IN_MEMORY {
            self.memory.insert(path.to_owned(), value.to_owned());
            return Ok(());
        }
        self.spill_to_disk()?;
        self.connection
            .as_ref()
            .ok_or_else(|| anyhow!("block type index spill did not open"))?
            .execute(
                "INSERT OR IGNORE INTO block_types(path,value) VALUES (?1,?2)",
                rusqlite::params![path, value],
            )?;
        Ok(())
    }

    fn get(&self, path: &str) -> Result<Option<String>> {
        if let Some(value) = self.memory.get(path) {
            return Ok(Some(value.clone()));
        }
        let Some(connection) = self.connection.as_ref() else {
            return Ok(None);
        };
        connection
            .query_row(
                "SELECT value FROM block_types WHERE path=?1",
                [path],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn has_value(&self, value: &str) -> Result<bool> {
        if self.memory.values().any(|item| item == value) {
            return Ok(true);
        }
        let Some(connection) = self.connection.as_ref() else {
            return Ok(false);
        };
        let found: i64 = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM block_types WHERE value=?1)",
            [value],
            |row| row.get(0),
        )?;
        Ok(found != 0)
    }

    fn spill_to_disk(&mut self) -> Result<()> {
        let (path, mut connection) = open_sqlite_scratch("block-types")?;
        connection.execute(
            "CREATE TABLE block_types(path TEXT PRIMARY KEY, value TEXT NOT NULL)",
            [],
        )?;
        {
            let transaction = connection.transaction()?;
            for (path_key, value) in &self.memory {
                transaction.execute(
                    "INSERT INTO block_types(path,value) VALUES (?1,?2)",
                    rusqlite::params![path_key, value],
                )?;
            }
            transaction.commit()?;
        }
        self.memory.clear();
        self.path = Some(path);
        self.connection = Some(connection);
        Ok(())
    }
}

impl Drop for BlockTypeIndex {
    fn drop(&mut self) {
        self.connection.take();
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[derive(Debug)]
struct Inspection {
    version: String,
    cwds: StringSpool,
    session_hint: Option<String>,
    malformed: bool,
}

const MAX_SUMMARY_METADATA: usize = 4096;

/// A bounded-memory sequence of metadata strings.  CWD fields are needed for
/// ownership checks but a history can contain an arbitrary number of them;
/// keeping those values in a private spool avoids turning metadata cardinality
/// into a process-memory limit.
struct StringSpool {
    path: Option<PathBuf>,
    writer: Option<BufWriter<File>>,
}

impl std::fmt::Debug for StringSpool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StringSpool")
            .field("path", &self.path)
            .finish()
    }
}

impl StringSpool {
    fn new(prefix: &str) -> Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bm25-mcp-session-{prefix}-{}-{stamp}-{id}.bin",
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
                    return Ok(Self {
                        path: Some(path),
                        writer: Some(BufWriter::new(file)),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(anyhow!("could not create session metadata spool"))
    }

    fn push(&mut self, value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let length = u32::try_from(bytes.len()).context("session metadata value too long")?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("session metadata spool finished"))?;
        writer.write_all(&length.to_le_bytes())?;
        writer.write_all(bytes)?;
        Ok(())
    }

    fn iter(&mut self) -> Result<StringSpoolIter> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| anyhow!("session metadata spool path missing"))?;
        Ok(StringSpoolIter {
            reader: BufReader::new(File::open(path)?),
            done: false,
        })
    }
}

struct StringSpoolIter {
    reader: BufReader<File>,
    done: bool,
}

impl Iterator for StringSpoolIter {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut length = [0_u8; 4];
        if let Err(error) = self.reader.read_exact(&mut length) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                self.done = true;
                return None;
            }
            self.done = true;
            return Some(Err(error.into()));
        }
        let length = u32::from_le_bytes(length) as usize;
        if length > MAX_METADATA_VALUE_BYTES {
            self.done = true;
            return Some(Err(anyhow!("session metadata value exceeds bound")));
        }
        let mut bytes = vec![0_u8; length];
        if let Err(error) = self.reader.read_exact(&mut bytes) {
            self.done = true;
            return Some(Err(error.into()));
        }
        Some(String::from_utf8(bytes).map_err(Into::into))
    }
}

impl Drop for StringSpool {
    fn drop(&mut self) {
        self.writer.take();
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedState {
    schema: u32,
    provider: String,
    identity: String,
    prefix_digest: String,
    next_line: u64,
    owner_key: String,
    #[serde(default)]
    registry_revision: Option<String>,
    session_id: Option<String>,
    #[serde(default)]
    diagnostics: HashMap<String, u64>,
}

#[derive(Debug)]
struct Parsed {
    source: Source,
    chunks: Records<Chunk>,
    chunk_count: u64,
    checkpoint: SessionCheckpoint,
    append: bool,
    state_updates: Records<(String, String, Option<String>)>,
}

#[derive(Clone, Debug)]
struct RecordRef {
    path: PathBuf,
    start_byte: u64,
    end_byte: u64,
    line: u64,
}

#[derive(Clone, Debug, Default)]
struct StreamResult {
    version: String,
    complete_offset: u64,
    next_line: u64,
    pending_tail: bool,
}

/// Invalidate the source keys represented by a precise provider-file change
/// set, or the whole collection for an ambiguous/full reconciliation.
pub(super) fn invalidate_scope(
    owner_key: &str,
    store: &Store,
    config: &SessionConfig,
    changes: Option<&HashSet<PathBuf>>,
) -> Result<()> {
    let Some(changes) = changes else {
        return store.invalidate_collection(owner_key, SESSION_KIND);
    };
    let candidates = changes
        .iter()
        .map(|path| canonical_or_normalized(path))
        .collect::<HashSet<_>>();
    if candidates.is_empty() {
        return Ok(());
    }
    for source in store.sources(owner_key, SESSION_KIND)? {
        let Some(path) = source_absolute_path(&source.path, config) else {
            continue;
        };
        if candidates.contains(&path) {
            store.invalidate_source(&source.key)?;
        }
    }
    Ok(())
}

/// Compatibility entry point used by the existing session unit tests.
#[allow(dead_code)]
pub(super) fn scan(
    root: &Path,
    owner_key: &str,
    store: &Store,
    config: &SessionConfig,
    should_continue: &dyn Fn() -> bool,
) -> Result<ScanReport> {
    scan_observed(
        root,
        owner_key,
        store,
        config,
        None,
        should_continue,
        &ProgressReporter::noop(),
        None,
    )
}

/// Reconcile provider files while publishing bounded progress.
#[allow(clippy::too_many_arguments)]
pub(super) fn scan_observed(
    root: &Path,
    owner_key: &str,
    store: &Store,
    config: &SessionConfig,
    changes: Option<&HashSet<PathBuf>>,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
    publisher: Option<crate::reconciliation::Publisher>,
) -> Result<ScanReport> {
    super::validate_own_tool_names_env()?;
    let root = fs::canonicalize(root)
        .with_context(|| format!("canonicalizing session project root {}", root.display()))?;
    let root_identity = project_identity(&root)?;
    let root_is_git = git_common_dir(&root).is_some();
    let mut registry = config
        .identity_registry_path
        .as_deref()
        .map(IdentityRegistry::open)
        .transpose()?;
    invalidate_scope(owner_key, store, config, changes)?;
    let existing = store.sources(owner_key, SESSION_KIND)?;
    let affected_existing = changes.map(|changes| {
        let candidates = changes
            .iter()
            .map(|path| canonical_or_normalized(path))
            .collect::<HashSet<_>>();
        existing
            .iter()
            .filter(|source| {
                source_absolute_path(&source.path, config)
                    .is_some_and(|path| candidates.contains(&path))
            })
            .map(|source| source.key.clone())
            .collect::<HashSet<_>>()
    });

    let mut report = ScanReport::default();
    report.coverage.publisher = publisher;
    report.coverage.full = changes.is_none();
    report.coverage.discovery_complete = true;
    progress.set_phase(ProgressPhase::Discovery);
    let discovery_started = Instant::now();
    let mut files = Vec::new();
    if let Some(changes) = changes {
        for path in changes {
            if let Some(provider) = provider_for_path(path, config) {
                files.push(SessionFile {
                    path: canonical_or_normalized(path),
                    provider,
                });
            } else {
                report.coverage.discovery_complete = false;
                report.excluded_count = report.excluded_count.saturating_add(1);
                diagnostic(
                    &mut report,
                    "ambiguous_change",
                    "changed path is not a provider file",
                );
            }
        }
    } else {
        report.coverage.discovery_complete &= discover_files(
            &config.codex_home.join("sessions"),
            Provider::Codex,
            &mut files,
            &mut report,
            should_continue,
        );
        report.coverage.discovery_complete &= discover_files(
            &config.codex_home.join("archived_sessions"),
            Provider::Codex,
            &mut files,
            &mut report,
            should_continue,
        );
        report.coverage.discovery_complete &= discover_files(
            &config.claude_config_dir.join("projects"),
            Provider::Claude,
            &mut files,
            &mut report,
            should_continue,
        );
        report.coverage.discovery_complete &= discover_files(
            &config.copilot_home.join("session-state"),
            Provider::Copilot,
            &mut files,
            &mut report,
            should_continue,
        );
    }
    let mut unique = HashSet::new();
    files.retain(|file| unique.insert(file.path.clone()));
    progress.record_work(WorkKind::Discovery, discovery_started.elapsed());
    progress.record_discovered(files.len() as u64);
    report.coverage.discovery = crate::coverage::SourceOutcome::from_report(&report);
    report.coverage.scope = files
        .iter()
        .map(|file| source_key(owner_key, &source_path(file.provider, &file.path, config)))
        .collect();

    let mut seen_sources = HashSet::new();
    let mut cwd_cache = HashMap::<String, Option<String>>::new();
    let mut cancelled = false;
    for (index, file) in files.iter().enumerate() {
        if !should_continue() {
            report.pending_count = report
                .pending_count
                .saturating_add((files.len() - index) as u64);
            cancelled = true;
            progress.record_cancellation();
            break;
        }
        let relative = source_path(file.provider, &file.path, config);
        let key = source_key(owner_key, &relative);
        let old = existing.iter().find(|source| source.key == key);
        let mut source_report = ScanReport::default();
        progress.record_current_source_bytes(0);
        match fs::symlink_metadata(&file.path) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                diagnostic(&mut source_report, "source_read", &error.to_string());
                if let Some(old) = old {
                    store.remove_source(&old.key)?;
                }
                seen_sources.insert(key.clone());
                report.record_source(store, key, None, source_report)?;
                progress.record_file_completed();
                continue;
            }
            metadata => {
                if let Some(old) = old {
                    store.remove_source(&old.key)?;
                }
                seen_sources.insert(key.clone());
                if metadata.is_ok() {
                    source_report.excluded_count = 1;
                    source_report
                        .diagnostics
                        .insert("non_file_change".into(), 1);
                    report.record_source(store, key, None, source_report)?;
                } else {
                    report.record_removal(store, key)?;
                }
                progress.record_file_completed();
                continue;
            }
        }
        let checkpoint = store.session_checkpoint(&key)?;
        let parsed = match read_file_with_retries(
            &file.path,
            file.provider,
            &root,
            &root_identity,
            root_is_git,
            owner_key,
            registry.as_mut(),
            config,
            store,
            old,
            checkpoint.as_ref(),
            &mut source_report,
            &mut cwd_cache,
            should_continue,
            progress,
        ) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => {
                source_report.excluded_count = source_report.excluded_count.saturating_add(1);
                if let Some(old) = old {
                    store.remove_source(&old.key)?;
                }
                seen_sources.insert(key.clone());
                report.record_source(store, key, None, source_report)?;
                progress.record_file_completed();
                continue;
            }
            Err(error) if error.downcast_ref::<FinalizeError>().is_some() => return Err(error),
            Err(error) if error.downcast_ref::<ScanCancelled>().is_some() => {
                report.pending_count = report
                    .pending_count
                    .saturating_add((files.len() - index) as u64);
                cancelled = true;
                progress.record_cancellation();
                break;
            }
            Err(error) => {
                diagnostic(
                    &mut source_report,
                    "source_read",
                    &format!("{}: {error}", file.path.display()),
                );
                if let Some(old) = old {
                    store.remove_source(&old.key)?;
                }
                seen_sources.insert(key.clone());
                report.record_source(store, key, None, source_report)?;
                progress.record_file_completed();
                continue;
            }
        };
        seen_sources.insert(key.clone());
        let prepared_chunk_count = parsed.chunk_count;
        source_report.sources = 1;
        source_report.chunks = prepared_chunk_count;
        let Parsed {
            source,
            chunks,
            checkpoint,
            append,
            state_updates,
            ..
        } = parsed;
        if !should_continue() {
            report.pending_count = report
                .pending_count
                .saturating_add((files.len() - index) as u64);
            cancelled = true;
            progress.record_cancellation();
            break;
        }
        let chunks = chunks.map(|item| {
            if !should_continue() {
                Err(ScanCancelled.into())
            } else {
                item
            }
        });
        let state_updates = state_updates.map(|item| {
            if !should_continue() {
                Err(ScanCancelled.into())
            } else {
                item
            }
        });
        let commit_started = Instant::now();
        let commit = if append {
            if let Some(old) = old {
                progress.set_phase(ProgressPhase::DurableTransaction);
                store.append_session_with_state(
                    &source,
                    &old.version,
                    chunks,
                    &checkpoint,
                    state_updates,
                )
            } else {
                progress.set_phase(ProgressPhase::DurableTransaction);
                store.replace_session_with_state(&source, chunks, &checkpoint, state_updates)
            }
        } else {
            progress.set_phase(ProgressPhase::DurableTransaction);
            store.replace_session_with_state(&source, chunks, &checkpoint, state_updates)
        };
        if let Err(error) = commit {
            if error.downcast_ref::<ScanCancelled>().is_some() {
                report.pending_count = report
                    .pending_count
                    .saturating_add((files.len() - index) as u64);
                cancelled = true;
                progress.record_cancellation();
                break;
            }
            return Err(error);
        }
        progress.record_work(WorkKind::DurableTxn, commit_started.elapsed());
        progress.record_committed_chunks(prepared_chunk_count);
        report.record_source(store, key, Some(&source.version), source_report)?;
        progress.record_file_completed();
    }

    if !cancelled && report.coverage.discovery_complete {
        for source in &existing {
            if affected_existing
                .as_ref()
                .is_none_or(|keys| keys.contains(&source.key))
                && !seen_sources.contains(&source.key)
            {
                store.remove_source(&source.key)?;
                report.record_removal(store, source.key.clone())?;
                progress.record_file_completed();
            }
        }
    }
    report.cancelled |= cancelled || !should_continue();
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
fn read_file_with_retries(
    path: &Path,
    provider: Provider,
    root: &Path,
    root_identity: &ProjectIdentity,
    root_is_git: bool,
    owner_key: &str,
    mut registry: Option<&mut IdentityRegistry>,
    config: &SessionConfig,
    store: &Store,
    old: Option<&Source>,
    checkpoint: Option<&SessionCheckpoint>,
    report: &mut ScanReport,
    cwd_cache: &mut HashMap<String, Option<String>>,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<Option<Parsed>> {
    let mut last = None;
    for attempt in 0..3 {
        if attempt != 0 {
            progress.record_retry();
        }
        let mut attempt_report = ScanReport::default();
        let result = read_file(
            path,
            provider,
            root,
            root_identity,
            root_is_git,
            owner_key,
            registry.as_deref_mut(),
            config,
            store,
            old,
            checkpoint,
            &mut attempt_report,
            cwd_cache,
            should_continue,
            progress,
        );
        match result {
            Ok(value) => {
                *report = attempt_report;
                return Ok(value);
            }
            Err(error) if error.to_string().contains("unstable_source") && attempt < 2 => {
                last = Some(error);
            }
            Err(error) => {
                *report = attempt_report;
                return Err(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("unstable_source: source changed while reading")))
}

#[allow(clippy::too_many_arguments)]
fn read_file(
    path: &Path,
    provider: Provider,
    root: &Path,
    root_identity: &ProjectIdentity,
    root_is_git: bool,
    owner_key: &str,
    mut registry: Option<&mut IdentityRegistry>,
    config: &SessionConfig,
    store: &Store,
    old: Option<&Source>,
    checkpoint: Option<&SessionCheckpoint>,
    report: &mut ScanReport,
    cwd_cache: &mut HashMap<String, Option<String>>,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<Option<Parsed>> {
    let before = file_identity(path)?;
    let identity = before.token.clone();
    let relative = source_path(provider, path, config);
    let source_key = source_key(owner_key, &relative);
    let registry_revision = identity_registry_revision(registry.as_deref(), should_continue)?;
    let mut append = false;
    let mut state = PersistedState {
        schema: CHECKPOINT_SCHEMA,
        provider: provider.name().to_owned(),
        identity: identity.clone(),
        prefix_digest: String::new(),
        next_line: 1,
        owner_key: owner_key.to_owned(),
        registry_revision: registry_revision.clone(),
        session_id: None,
        diagnostics: HashMap::new(),
    };
    let mut start_offset = 0_u64;
    if let (Some(_old_source), Some(checkpoint)) = (old, checkpoint)
        && let Ok(previous) = serde_json::from_str::<PersistedState>(&checkpoint.state)
        && previous.schema == CHECKPOINT_SCHEMA
        && previous.provider == provider.name()
        && previous.identity == identity
        && previous.owner_key == owner_key
        && previous.registry_revision == registry_revision
        && checkpoint.offset <= before.len
        && {
            progress.set_phase(ProgressPhase::PrefixVerification);
            let started = Instant::now();
            let result = hash_prefix(path, checkpoint.offset, should_continue, progress)?;
            progress.record_work(WorkKind::PrefixVerification, started.elapsed());
            result == previous.prefix_digest
        }
    {
        append = true;
        start_offset = checkpoint.offset;
        state = previous;
    }

    progress.set_phase(ProgressPhase::JsonInspection);
    let inspection_started = Instant::now();
    let mut inspection = inspect(
        path,
        provider,
        start_offset,
        if append { state.next_line } else { 1 },
        report,
        should_continue,
        progress,
    )?;
    progress.record_work(WorkKind::JsonInspection, inspection_started.elapsed());
    let mut saw_cwd = false;
    // A previously committed source has already passed ownership checks. An
    // append suffix without another cwd inherits that verified ownership.
    let mut current_cwd = append;
    let mut known_other_cwd = false;
    let mut unknown_cwd = false;
    for cwd in inspection.cwds.iter()? {
        let cwd = cwd?;
        let owner = if let Some(owner) = cwd_cache.get(&cwd) {
            owner.clone()
        } else {
            progress.set_phase(ProgressPhase::Ownership);
            let ownership_started = Instant::now();
            let owner = associate_cwd(
                &cwd,
                root,
                root_identity,
                root_is_git,
                registry.as_deref_mut(),
            )?;
            progress.record_work(WorkKind::Ownership, ownership_started.elapsed());
            if cwd_cache.len() >= MAX_CWD_CACHE_ENTRIES {
                cwd_cache.clear();
            }
            cwd_cache.insert(cwd.clone(), owner.clone());
            owner
        };
        saw_cwd = true;
        match owner.as_deref() {
            Some(owner) if owner == owner_key => current_cwd = true,
            Some(_) => known_other_cwd = true,
            None => unknown_cwd = true,
        }
    }
    if !saw_cwd && !append {
        diagnostic(
            report,
            "ownership",
            &format!("{} has no verified cwd", path.display()),
        );
        return Ok(None);
    }
    if known_other_cwd && !current_cwd && !unknown_cwd {
        *report
            .diagnostics
            .entry("ownership_excluded".to_owned())
            .or_default() += 1;
        return Ok(None);
    }
    if known_other_cwd || unknown_cwd || !current_cwd {
        diagnostic(
            report,
            "ownership",
            &format!("{} contains an unrelated or ambiguous cwd", path.display()),
        );
        return Ok(None);
    }

    if append {
        for (category, count) in &state.diagnostics {
            let total = report.diagnostics.entry(category.clone()).or_default();
            *total = total.saturating_add(*count);
            report.error_count = report.error_count.saturating_add(*count);
            if report.errors.len() < MAX_REPORTED_ERRORS {
                report
                    .errors
                    .push(format!("historical_coverage: {category} count {count}"));
            }
        }
    }

    let mut parser_state = ParseState {
        ids: {
            let started = Instant::now();
            let ids = DurableIdSet::new(&source_key, append, store, progress)?;
            progress.record_work(WorkKind::TempFileOps, started.elapsed());
            ids
        },
        session_id: state.session_id.clone().or(inspection.session_hint.clone()),
    };
    let mut chunks = {
        let started = Instant::now();
        let chunks = RecordSpool::new()?;
        progress.record_work(WorkKind::TempFileOps, started.elapsed());
        chunks
    };
    let mut chunk_count = 0_u64;
    let stream = stream_records(
        path,
        start_offset,
        if append { state.next_line } else { 1 },
        should_continue,
        progress,
        |record| {
            progress.record_record();
            let capture = match CaptureFile::parse_controlled_with_progress(
                &record.path,
                should_continue,
                progress,
            ) {
                Err(error) if error.downcast_ref::<ParseCancelled>().is_some() => {
                    return Err(ScanCancelled.into());
                }
                Ok(capture) => capture,
                Err(error) => {
                    diagnostic(
                        report,
                        "malformed_record",
                        &format!("{} line {}: {error}", path.display(), record.line),
                    );
                    return Ok(());
                }
            };
            let summary = summarize(provider, &capture)?;
            let digest = digest_file(&record.path)?;
            if let Some(event_id) = &summary.event_id {
                let dedup_key = format!("{event_id}:{digest}");
                if !parser_state.ids.insert(STATE_KIND_SEEN_EVENT, &dedup_key)? {
                    return Ok(());
                }
            }
            if let Some(session_id) = &summary.session_id {
                parser_state.session_id = Some(session_id.clone());
            }
            let mut appender = SessionChunkAppender::new(&mut chunks, progress);
            let mut field_deduper = VisibleFieldDeduper::new();
            {
                let mut append_item = |item: Normalized| {
                    if !should_continue() {
                        return Err(ScanCancelled.into());
                    }
                    appender.push(item, record)
                };
                progress.set_phase(ProgressPhase::Normalization);
                let normalization_started = Instant::now();
                let tokenization_started = Instant::now();
                normalize(
                    provider,
                    &capture,
                    &summary,
                    &mut parser_state.ids,
                    &config.own_tool_names,
                    parser_state.session_id.as_deref(),
                    &mut |item| field_deduper.push(item, &mut append_item),
                    report,
                )?;
                progress.record_work(WorkKind::Normalization, normalization_started.elapsed());
                progress.record_work(WorkKind::Tokenization, tokenization_started.elapsed());
                field_deduper.finish(&mut append_item)?;
            }
            chunk_count = chunk_count.saturating_add(appender.finish()?);
            Ok(())
        },
    )?;
    if !should_continue() {
        return Err(ScanCancelled.into());
    }
    let offset = stream.complete_offset;
    let (final_version, final_suffix, prefix_digest, previous_prefix_digest) =
        digest_file_controlled(path, start_offset, offset, should_continue, progress)?;
    let after = file_identity(path)?;
    if before.token != after.token
        || before.len != after.len
        || before.modified != after.modified
        || stream.version != inspection.version
        || final_suffix != stream.version
        || (!append && final_version != stream.version)
        || (append && previous_prefix_digest != state.prefix_digest)
    {
        bail!("unstable_source: session file changed while reading")
    }
    state.identity = after.token;
    state.prefix_digest = prefix_digest;
    state.next_line = stream.next_line;
    state.session_id = parser_state.session_id;
    state.diagnostics = report.diagnostics.clone().into_iter().collect();
    let state_json = serde_json::to_string(&state)?;
    let source = Source {
        key: source_key,
        collection: owner_key.to_owned(),
        path: relative,
        version: final_version,
        kind: SESSION_KIND.to_owned(),
    };
    Ok(Some(Parsed {
        source,
        chunks: chunks.finish()?,
        chunk_count,
        checkpoint: SessionCheckpoint {
            offset,
            state: state_json,
        },
        append,
        state_updates: parser_state.ids.finish()?,
    }))
}

fn inspect(
    path: &Path,
    provider: Provider,
    start_offset: u64,
    start_line: u64,
    report: &mut ScanReport,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<Inspection> {
    let mut inspection = Inspection {
        version: String::new(),
        cwds: StringSpool::new("cwds")?,
        session_hint: None,
        malformed: false,
    };
    let stream = stream_records(
        path,
        start_offset,
        start_line,
        should_continue,
        progress,
        |record| {
            progress.record_record_inspected();
            let capture = match CaptureFile::parse_metadata_controlled_with_progress(
                &record.path,
                should_continue,
                progress,
            ) {
                Err(error) if error.downcast_ref::<ParseCancelled>().is_some() => {
                    return Err(ScanCancelled.into());
                }
                Ok(capture) => capture,
                Err(error) => {
                    inspection.malformed = true;
                    diagnostic(
                        report,
                        "malformed_record",
                        &format!("{} line {}: {error}", path.display(), record.line),
                    );
                    return Ok(());
                }
            };
            let summary = summarize(provider, &capture)?;
            if summary.metadata_truncated {
                diagnostic(
                    report,
                    "metadata",
                    &format!(
                        "{} line {} metadata fields exceeded bounded summary",
                        path.display(),
                        record.line
                    ),
                );
            }
            if let Some(cwd) = summary.cwd {
                inspection.cwds.push(&cwd)?;
            }
            if inspection.session_hint.is_none() {
                inspection.session_hint = summary.session_id;
            }
            Ok(())
        },
    )?;
    if stream.pending_tail {
        report.pending_count = report.pending_count.saturating_add(1);
    }
    inspection.version = stream.version;
    Ok(inspection)
}

fn summarize(provider: Provider, capture: &CaptureFile) -> Result<RecordSummary> {
    let mut summary = RecordSummary {
        fields: HashMap::new(),
        block_types: BlockTypeIndex::new()?,
        kind: None,
        cwd: None,
        timestamp_raw: None,
        session_id: None,
        event_id: None,
        role: None,
        channel: None,
        name: None,
        call_id: None,
        tool_use_id: None,
        metadata_truncated: false,
    };
    for item in capture.iter()? {
        let fragment = item?;
        let fragment_key = path_key(&fragment.path);
        let visible = fragment
            .path
            .last()
            .is_some_and(|part| matches!(part, PathPart::Key(key) if is_visible_key(key)));
        if !visible {
            if fragment.text.len() <= MAX_METADATA_VALUE_BYTES {
                if summary.fields.contains_key(&fragment_key)
                    || summary.fields.len() < MAX_SUMMARY_METADATA
                {
                    summary
                        .fields
                        .entry(fragment_key.clone())
                        .or_insert_with(|| fragment.text.clone());
                } else {
                    summary.metadata_truncated = true;
                }
            } else {
                summary.metadata_truncated = true;
            }
        }
        if let Some(PathPart::Key(key)) = fragment.path.last()
            && key == "type"
        {
            let parent = path_key(&fragment.path[..fragment.path.len() - 1]);
            summary.block_types.insert(&parent, &fragment.text)?;
        }
    }
    summary.kind = field_exact(&summary.fields, "type");
    summary.role = match provider {
        Provider::Codex => field_at_keys(&summary.fields, &["payload", "role"]),
        Provider::Claude => field_at_keys(&summary.fields, &["message", "role"]),
        Provider::Copilot => field_at_keys(&summary.fields, &["data", "role"]),
    };
    summary.channel = match provider {
        Provider::Codex => field_at_keys(&summary.fields, &["payload", "channel"]),
        _ => field_exact(&summary.fields, "channel"),
    };
    summary.name = match provider {
        Provider::Codex => field_at_keys(&summary.fields, &["payload", "name"]),
        Provider::Claude => None,
        Provider::Copilot => field_at_keys(&summary.fields, &["data", "name"])
            .or_else(|| field_at_keys(&summary.fields, &["data", "toolName"]))
            .or_else(|| field_at_keys(&summary.fields, &["data", "mcpToolName"])),
    };
    summary.call_id = match provider {
        Provider::Codex => field_at_keys(&summary.fields, &["payload", "call_id"]),
        Provider::Claude => None,
        Provider::Copilot => field_at_keys(&summary.fields, &["data", "toolCallId"])
            .or_else(|| field_at_keys(&summary.fields, &["data", "callId"])),
    };
    summary.tool_use_id = match provider {
        Provider::Codex => field_at_keys(&summary.fields, &["payload", "tool_use_id"]),
        Provider::Claude => field_at_keys(&summary.fields, &["tool_use_id"]),
        Provider::Copilot => field_at_keys(&summary.fields, &["data", "toolUseId"]),
    };
    summary.event_id = event_id(provider, summary.kind.as_deref(), &summary.fields)
        .or_else(|| summary.call_id.clone());
    summary.timestamp_raw = field_exact(&summary.fields, "timestamp")
        .or_else(|| field_exact(&summary.fields, "created_at"))
        .or_else(|| field_exact(&summary.fields, "createdAt"))
        .or_else(|| field_exact(&summary.fields, "time"));
    summary.session_id = session_id(provider, summary.kind.as_deref(), &summary.fields);
    summary.cwd = ownership_cwd(provider, summary.kind.as_deref(), &summary.fields);
    Ok(summary)
}

fn is_visible_key(key: &str) -> bool {
    matches!(
        key,
        "content"
            | "text"
            | "output"
            | "arguments"
            | "input"
            | "result"
            | "detailedContent"
            | "message"
    )
}

fn ownership_cwd(
    provider: Provider,
    kind: Option<&str>,
    fields: &HashMap<String, String>,
) -> Option<String> {
    match provider {
        Provider::Codex if matches!(kind, Some("session_meta" | "turn_context")) => {
            field_at_keys(fields, &["payload", "cwd"])
        }
        Provider::Claude => field_at_keys(fields, &["cwd"]),
        Provider::Copilot if matches!(kind, Some("session.start" | "session.resume")) => {
            field_at_keys(fields, &["data", "context", "cwd"])
        }
        _ => None,
    }
}

fn event_id(
    provider: Provider,
    kind: Option<&str>,
    fields: &HashMap<String, String>,
) -> Option<String> {
    field_exact(fields, "uuid")
        .or_else(|| field_exact(fields, "id"))
        .or_else(|| match provider {
            Provider::Codex if kind == Some("response_item") => {
                field_at_keys(fields, &["payload", "id"])
            }
            Provider::Copilot => field_at_keys(fields, &["data", "eventId"]),
            _ => None,
        })
}

fn session_id(
    provider: Provider,
    kind: Option<&str>,
    fields: &HashMap<String, String>,
) -> Option<String> {
    field_exact(fields, "session_id")
        .or_else(|| field_exact(fields, "sessionId"))
        .or_else(|| match provider {
            // `payload.id` is the session identifier only on the initial
            // Codex metadata record. Response-item payload IDs identify
            // individual events and must not replace the session context.
            Provider::Codex if kind == Some("session_meta") => {
                field_at_keys(fields, &["payload", "session_id"])
                    .or_else(|| field_at_keys(fields, &["payload", "id"]))
            }
            Provider::Copilot if matches!(kind, Some("session.start" | "session.resume")) => {
                field_at_keys(fields, &["data", "sessionId"])
            }
            _ => None,
        })
}

struct ParseState {
    ids: DurableIdSet,
    session_id: Option<String>,
}

const STATE_ID_TABLE: &str = "session_ids";

/// Exact membership for a source's incremental parser state.  The committed
/// prefix lives in Store's session_state table and is queried by key; IDs
/// discovered while parsing the current suffix live in this private SQLite
/// file.  Both sides stay off the process heap even when a history contains
/// millions of calls/events.
struct DurableIdSet {
    source_key: String,
    scratch_path: Option<PathBuf>,
    connection: Option<rusqlite::Connection>,
    reader: Option<SessionStateReader>,
    updates: Option<RecordSpool>,
    pending_writes: usize,
    batch_started: Option<Instant>,
    lookup_elapsed: Duration,
    lookup_count: u64,
    begin_elapsed: Duration,
    begin_count: u64,
    insert_elapsed: Duration,
    insert_count: u64,
    progress: ProgressReporter,
}

impl DurableIdSet {
    fn new(
        source_key: &str,
        append: bool,
        store: &Store,
        progress: &ProgressReporter,
    ) -> Result<Self> {
        let (path, connection) = open_state_scratch()?;
        connection.execute(
            &format!(
                "CREATE TABLE {STATE_ID_TABLE}(kind TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(kind,key))"
            ),
            [],
        )?;
        Ok(Self {
            source_key: source_key.to_owned(),
            scratch_path: Some(path),
            connection: Some(connection),
            reader: append.then(|| store.session_state_reader()).transpose()?,
            updates: Some(RecordSpool::new()?),
            pending_writes: 0,
            batch_started: None,
            lookup_elapsed: Duration::ZERO,
            lookup_count: 0,
            begin_elapsed: Duration::ZERO,
            begin_count: 0,
            insert_elapsed: Duration::ZERO,
            insert_count: 0,
            progress: progress.clone(),
        })
    }

    /// Insert one ID and return whether it was new relative to the committed
    /// prefix and the current suffix.
    fn insert(&mut self, kind: &str, key: &str) -> Result<bool> {
        self.insert_value(kind, key, "1")
    }

    fn insert_value(&mut self, kind: &str, key: &str, value: &str) -> Result<bool> {
        let lookup_started = Instant::now();
        let local: Option<i64> = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("session ID scratch database closed"))?
            .query_row(
                &format!("SELECT 1 FROM {STATE_ID_TABLE} WHERE kind=?1 AND key=?2"),
                rusqlite::params![kind, key],
                |row| row.get(0),
            )
            .optional()?;
        if local.is_some() {
            self.lookup_elapsed = self.lookup_elapsed.saturating_add(lookup_started.elapsed());
            self.lookup_count = self.lookup_count.saturating_add(1);
            return Ok(false);
        }
        let committed = self
            .reader
            .as_ref()
            .map(|reader| reader.get(&self.source_key, kind, key))
            .transpose()?
            .flatten();
        self.lookup_elapsed = self.lookup_elapsed.saturating_add(lookup_started.elapsed());
        self.lookup_count = self.lookup_count.saturating_add(1);
        if committed.is_some() {
            return Ok(false);
        }
        if self.pending_writes == 0 {
            let begin_started = Instant::now();
            self.connection
                .as_ref()
                .ok_or_else(|| anyhow!("session ID scratch database closed"))?
                .execute_batch("BEGIN IMMEDIATE")?;
            self.begin_elapsed = self.begin_elapsed.saturating_add(begin_started.elapsed());
            self.begin_count = self.begin_count.saturating_add(1);
            self.batch_started = Some(Instant::now());
        }
        let insert_started = Instant::now();
        self.connection
            .as_ref()
            .ok_or_else(|| anyhow!("session ID scratch database closed"))?
            .execute(
                &format!("INSERT INTO {STATE_ID_TABLE}(kind,key,value) VALUES (?1,?2,?3)"),
                rusqlite::params![kind, key, value],
            )?;
        self.insert_elapsed = self.insert_elapsed.saturating_add(insert_started.elapsed());
        self.insert_count = self.insert_count.saturating_add(1);
        self.progress
            .record_scratch_state_write((kind.len() + key.len() + value.len()) as u64);
        self.pending_writes = self.pending_writes.saturating_add(1);
        if self.pending_writes >= SCRATCH_BATCH_WRITES {
            self.commit_batch()?;
            self.pending_writes = 0;
        }
        self.updates
            .as_mut()
            .ok_or_else(|| anyhow!("session state update spool finished"))?
            .push(&(kind, key, Some(value)))?;
        Ok(true)
    }

    fn contains(&self, kind: &str, key: &str) -> Result<bool> {
        Ok(self.value(kind, key)?.is_some())
    }

    fn value(&self, kind: &str, key: &str) -> Result<Option<String>> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("session ID scratch database closed"))?;
        let local: Option<String> = connection
            .query_row(
                &format!("SELECT value FROM {STATE_ID_TABLE} WHERE kind=?1 AND key=?2"),
                rusqlite::params![kind, key],
                |row| row.get(0),
            )
            .optional()?;
        if local.is_some() {
            return Ok(local);
        }
        Ok(self
            .reader
            .as_ref()
            .map(|reader| reader.get(&self.source_key, kind, key))
            .transpose()?
            .flatten())
    }

    fn finish(mut self) -> Result<Records<(String, String, Option<String>)>> {
        if self.pending_writes != 0 {
            self.commit_batch()?;
            self.pending_writes = 0;
        }
        self.updates
            .take()
            .ok_or_else(|| anyhow!("session state update spool finished"))?
            .finish()
            .map_err(Into::into)
    }

    fn commit_batch(&mut self) -> Result<()> {
        let commit_started = Instant::now();
        self.connection
            .as_ref()
            .ok_or_else(|| anyhow!("session ID scratch database closed"))?
            .execute_batch("COMMIT")?;
        let transaction_lifetime = self
            .batch_started
            .take()
            .map(|started| started.elapsed())
            .unwrap_or_default();
        self.progress.record_scratch_batch(ScratchBatchMetrics {
            lookup: self.lookup_elapsed,
            lookup_count: self.lookup_count,
            begin: self.begin_elapsed,
            begin_count: self.begin_count,
            insert: self.insert_elapsed,
            insert_count: self.insert_count,
            commit: commit_started.elapsed(),
            transaction_lifetime,
        });
        self.progress.record_scratch_state_transaction();
        self.lookup_elapsed = Duration::ZERO;
        self.lookup_count = 0;
        self.begin_elapsed = Duration::ZERO;
        self.begin_count = 0;
        self.insert_elapsed = Duration::ZERO;
        self.insert_count = 0;
        Ok(())
    }
}

impl Drop for DurableIdSet {
    fn drop(&mut self) {
        self.connection.take();
        if let Some(path) = self.scratch_path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

fn open_state_scratch() -> Result<(PathBuf, rusqlite::Connection)> {
    open_sqlite_scratch("state")
}

fn open_field_value_scratch() -> Result<(PathBuf, File)> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    for _ in 0..32 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bm25-mcp-session-field-values-{}-{stamp}-{id}.bin",
            std::process::id(),
        ));
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(anyhow!("could not create visible-field value spool"))
}

fn open_sqlite_scratch(prefix: &str) -> Result<(PathBuf, rusqlite::Connection)> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    for _ in 0..32 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bm25-mcp-session-{prefix}-{}-{stamp}-{id}.sqlite3",
            std::process::id(),
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
                drop(file);
                match rusqlite::Connection::open(&path) {
                    Ok(connection) => return Ok((path, connection)),
                    Err(error) => {
                        let _ = fs::remove_file(&path);
                        return Err(error.into());
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(anyhow!("could not create session state scratch database"))
}

#[derive(Clone, Debug)]
struct Normalized {
    text: String,
    agent: String,
    session_id: String,
    event_id: Option<String>,
    timestamp: Option<String>,
    role: Option<String>,
    tool: Option<String>,
    field_kind: Option<String>,
    logical_id: Option<String>,
    dedup_key: Option<String>,
}

/// Suppresses exact repeats of visible fields within one physical event.
///
/// Normalization receives a selected JSON string in bounded fragments.  A
/// fragment-level set would either miss a repeat split at a different
/// boundary or incorrectly drop a legitimately repeated substring.  This
/// collector therefore spools one logical field at a time, records its total
/// length and digest, and compares matching candidates byte-for-byte before
/// replaying the first distinct value.  The value bytes and the digest index
/// live on disk, so a large field or a record with many sibling fields does
/// not grow process memory with its payload.
struct VisibleFieldDeduper {
    storage: Option<FieldDedupStorage>,
    active: Option<PendingField>,
}

struct PendingField {
    template: Normalized,
    start: u64,
    length: u64,
    digest: Sha256,
    dedup_key: String,
    inline: Vec<u8>,
}

struct FieldDedupStorage {
    value_path: PathBuf,
    value_writer: Option<BufWriter<File>>,
    index_path: PathBuf,
    connection: Option<rusqlite::Connection>,
    next_offset: u64,
}

impl VisibleFieldDeduper {
    fn new() -> Self {
        Self {
            storage: None,
            active: None,
        }
    }

    fn push(
        &mut self,
        mut item: Normalized,
        output: &mut dyn FnMut(Normalized) -> Result<()>,
    ) -> Result<()> {
        let Some(dedup_key) = item.dedup_key.as_ref() else {
            self.finish_active(output)?;
            output(item)?;
            return Ok(());
        };
        let same_field = self.active.as_ref().is_some_and(|active| {
            active.template.logical_id == item.logical_id
                && active.dedup_key == *dedup_key
                && normalized_context_equal(&active.template, &item)
        });
        if !same_field {
            if self.active.is_some() && self.storage.is_none() {
                self.promote_active()?;
            }
            self.finish_active(output)?;
            self.start_field(item)?;
        } else {
            let text = std::mem::take(&mut item.text);
            self.append_active(&text)?;
        }
        Ok(())
    }

    fn start_field(&mut self, mut item: Normalized) -> Result<()> {
        let text = std::mem::take(&mut item.text);
        let dedup_key = item
            .dedup_key
            .clone()
            .ok_or_else(|| anyhow!("visible-field dedup key missing"))?;
        let start = self
            .storage
            .as_ref()
            .map_or(0, |storage| storage.next_offset);
        self.active = Some(PendingField {
            template: Normalized {
                text: String::new(),
                ..item
            },
            start,
            length: 0,
            digest: Sha256::new(),
            dedup_key,
            inline: Vec::new(),
        });
        self.append_active(&text)
    }

    fn append_active(&mut self, text: &str) -> Result<()> {
        let bytes = text.as_bytes();
        let needs_promotion = self.active.as_ref().is_some_and(|active| {
            self.storage.is_none()
                && active.inline.len().saturating_add(bytes.len()) > MAX_INLINE_DEDUP_BYTES
        });
        if needs_promotion {
            self.promote_active()?;
        }
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| anyhow!("visible-field dedup field missing"))?;
        if self.storage.is_some() {
            let storage = self
                .storage
                .as_mut()
                .ok_or_else(|| anyhow!("visible-field dedup storage missing"))?;
            storage
                .value_writer
                .as_mut()
                .ok_or_else(|| anyhow!("visible-field dedup spool closed"))?
                .write_all(bytes)?;
            storage.next_offset = storage.next_offset.saturating_add(bytes.len() as u64);
        } else {
            active.inline.extend_from_slice(bytes);
        }
        active.length = active.length.saturating_add(bytes.len() as u64);
        active.digest.update(bytes);
        Ok(())
    }

    fn promote_active(&mut self) -> Result<()> {
        self.ensure_storage()?;
        let bytes = self
            .active
            .as_mut()
            .ok_or_else(|| anyhow!("visible-field dedup field missing"))?
            .inline
            .split_off(0);
        let storage = self
            .storage
            .as_mut()
            .ok_or_else(|| anyhow!("visible-field dedup storage missing"))?;
        let start = storage.next_offset;
        storage
            .value_writer
            .as_mut()
            .ok_or_else(|| anyhow!("visible-field dedup spool closed"))?
            .write_all(&bytes)?;
        storage.next_offset = storage.next_offset.saturating_add(bytes.len() as u64);
        self.active
            .as_mut()
            .ok_or_else(|| anyhow!("visible-field dedup field missing"))?
            .start = start;
        Ok(())
    }

    fn finish_active(&mut self, output: &mut dyn FnMut(Normalized) -> Result<()>) -> Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        if self.storage.is_none() {
            if !active.inline.is_empty() {
                let mut item = active.template;
                item.text = String::from_utf8(active.inline)?;
                output(item)?;
            }
            return Ok(());
        }
        let storage = self
            .storage
            .as_mut()
            .ok_or_else(|| anyhow!("visible-field dedup storage missing"))?;
        if let Some(writer) = storage.value_writer.as_mut() {
            writer.flush()?;
        }
        let digest = active.digest.clone().finalize();
        let length = i64::try_from(active.length).context("visible field length overflow")?;
        let value_path = storage.value_path.clone();
        let start = active.start;
        let end = start.saturating_add(active.length);
        let duplicate = {
            let connection = storage
                .connection
                .as_ref()
                .ok_or_else(|| anyhow!("visible-field dedup index closed"))?;
            let mut statement = connection.prepare(
                "SELECT start_offset,end_offset FROM visible_fields
                 WHERE dedup_key=?1 AND length=?2 AND digest=?3",
            )?;
            let mut rows = statement.query(rusqlite::params![
                active.dedup_key,
                length,
                digest.as_slice()
            ])?;
            let mut found = false;
            while let Some(row) = rows.next()? {
                let prior_start = u64::try_from(row.get::<_, i64>(0)?)
                    .context("visible field start offset is negative")?;
                let prior_end = u64::try_from(row.get::<_, i64>(1)?)
                    .context("visible field end offset is negative")?;
                if byte_ranges_equal(&value_path, start, end, prior_start, prior_end)? {
                    found = true;
                    break;
                }
            }
            found
        };
        if duplicate {
            return Ok(());
        }

        replay_field(&value_path, &active, output)?;
        let start = i64::try_from(start).context("visible field start offset overflow")?;
        let end = i64::try_from(end).context("visible field end offset overflow")?;
        storage
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("visible-field dedup index closed"))?
            .execute(
                "INSERT INTO visible_fields(dedup_key,length,digest,start_offset,end_offset)
                 VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![active.dedup_key, length, digest.as_slice(), start, end],
            )?;
        Ok(())
    }

    fn ensure_storage(&mut self) -> Result<()> {
        if self.storage.is_some() {
            return Ok(());
        }
        self.storage = Some(FieldDedupStorage::new()?);
        Ok(())
    }

    fn finish(mut self, output: &mut dyn FnMut(Normalized) -> Result<()>) -> Result<()> {
        self.finish_active(output)
    }
}

impl FieldDedupStorage {
    fn new() -> Result<Self> {
        let (index_path, connection) = open_sqlite_scratch("field-dedup")?;
        if let Err(error) = connection.execute(
            "CREATE TABLE visible_fields(
                dedup_key TEXT NOT NULL,
                length INTEGER NOT NULL,
                digest BLOB NOT NULL,
                start_offset INTEGER NOT NULL,
                end_offset INTEGER NOT NULL
            )",
            [],
        ) {
            drop(connection);
            let _ = fs::remove_file(index_path);
            return Err(error.into());
        }
        let (value_path, value_file) = match open_field_value_scratch() {
            Ok(value) => value,
            Err(error) => {
                drop(connection);
                let _ = fs::remove_file(index_path);
                return Err(error);
            }
        };
        Ok(Self {
            value_path,
            value_writer: Some(BufWriter::new(value_file)),
            index_path,
            connection: Some(connection),
            next_offset: 0,
        })
    }
}

impl Drop for FieldDedupStorage {
    fn drop(&mut self) {
        if let Some(mut writer) = self.value_writer.take() {
            let _ = writer.flush();
        }
        self.connection.take();
        let _ = fs::remove_file(&self.value_path);
        let _ = fs::remove_file(&self.index_path);
    }
}

fn normalized_context_equal(left: &Normalized, right: &Normalized) -> bool {
    left.agent == right.agent
        && left.session_id == right.session_id
        && left.event_id == right.event_id
        && left.timestamp == right.timestamp
        && left.role == right.role
        && left.tool == right.tool
        && left.field_kind == right.field_kind
        && left.dedup_key == right.dedup_key
}

fn byte_ranges_equal(
    path: &Path,
    left_start: u64,
    left_end: u64,
    right_start: u64,
    right_end: u64,
) -> Result<bool> {
    if left_end.saturating_sub(left_start) != right_end.saturating_sub(right_start) {
        return Ok(false);
    }
    let mut left = File::open(path)?;
    let mut right = File::open(path)?;
    left.seek(SeekFrom::Start(left_start))?;
    right.seek(SeekFrom::Start(right_start))?;
    let mut left_buffer = [0_u8; MAX_FRAGMENT_BYTES];
    let mut right_buffer = [0_u8; MAX_FRAGMENT_BYTES];
    let mut remaining = left_end.saturating_sub(left_start);
    while remaining != 0 {
        let count = remaining.min(left_buffer.len() as u64) as usize;
        left.read_exact(&mut left_buffer[..count])?;
        right.read_exact(&mut right_buffer[..count])?;
        if left_buffer[..count] != right_buffer[..count] {
            return Ok(false);
        }
        remaining -= count as u64;
    }
    Ok(true)
}

fn replay_field(
    path: &Path,
    active: &PendingField,
    output: &mut dyn FnMut(Normalized) -> Result<()>,
) -> Result<()> {
    let mut reader = File::open(path)?;
    reader.seek(SeekFrom::Start(active.start))?;
    let mut remaining = active.length;
    let mut buffer = [0_u8; MAX_FRAGMENT_BYTES];
    let mut pending = Vec::new();
    while remaining != 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        reader.read_exact(&mut buffer[..count])?;
        pending.extend_from_slice(&buffer[..count]);
        remaining -= count as u64;
        replay_valid_utf8(&mut pending, &active.template, output)?;
    }
    if !pending.is_empty() {
        if std::str::from_utf8(&pending).is_err() {
            bail!("visible-field dedup spool contains invalid UTF-8")
        }
        replay_valid_utf8(&mut pending, &active.template, output)?;
    }
    Ok(())
}

fn replay_valid_utf8(
    pending: &mut Vec<u8>,
    template: &Normalized,
    output: &mut dyn FnMut(Normalized) -> Result<()>,
) -> Result<()> {
    loop {
        match std::str::from_utf8(pending) {
            Ok(_) => {
                if pending.is_empty() {
                    return Ok(());
                }
                let text = String::from_utf8(std::mem::take(pending))?;
                let mut item = template.clone();
                item.text = text;
                output(item)?;
                return Ok(());
            }
            Err(error) => {
                if error.error_len().is_some() {
                    bail!("visible-field dedup spool contains invalid UTF-8")
                }
                let valid = error.valid_up_to();
                if valid == 0 {
                    return Ok(());
                }
                let tail = pending.split_off(valid);
                let text = String::from_utf8(std::mem::take(pending))?;
                let mut item = template.clone();
                item.text = text;
                output(item)?;
                *pending = tail;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize(
    provider: Provider,
    capture: &CaptureFile,
    summary: &RecordSummary,
    own_calls: &mut DurableIdSet,
    own_tools: &[String],
    inherited_session_id: Option<&str>,
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    report: &mut ScanReport,
) -> Result<()> {
    let kind = summary.kind.as_deref().unwrap_or("");
    let session_id = summary
        .session_id
        .clone()
        .or_else(|| inherited_session_id.map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned());
    let timestamp = summary
        .timestamp_raw
        .as_deref()
        .and_then(normalize_timestamp);
    if summary.timestamp_raw.is_some() && timestamp.is_none() {
        diagnostic(report, "timestamp", "record has an invalid timestamp");
    }
    let event_id = summary.event_id.clone();
    match provider {
        Provider::Codex => normalize_codex(
            capture,
            summary,
            kind,
            &session_id,
            event_id,
            timestamp,
            own_calls,
            own_tools,
            output,
            report,
        )?,
        Provider::Claude => normalize_claude(
            capture,
            summary,
            kind,
            &session_id,
            event_id,
            timestamp,
            own_calls,
            own_tools,
            output,
            report,
        )?,
        Provider::Copilot => normalize_copilot(
            capture,
            summary,
            kind,
            &session_id,
            event_id,
            timestamp,
            own_calls,
            own_tools,
            output,
            report,
        )?,
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_codex(
    capture: &CaptureFile,
    summary: &RecordSummary,
    kind: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    own_calls: &mut DurableIdSet,
    own_tools: &[String],
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    report: &mut ScanReport,
) -> Result<()> {
    if kind == "session_meta" || kind == "turn_context" {
        return Ok(());
    }
    if kind != "response_item" {
        if !kind.is_empty() {
            diagnostic(
                report,
                "unsupported_record",
                &format!("codex event type {kind}"),
            );
        }
        return Ok(());
    }
    let payload_kind = field_at_keys(&summary.fields, &["payload", "type"]).unwrap_or_default();
    match payload_kind.as_str() {
        "message" => {
            if !matches!(summary.role.as_deref(), Some("user") | Some("assistant"))
                || summary.channel.as_deref() == Some("analysis")
            {
                return Ok(());
            }
            for fragment in capture.iter()? {
                let fragment = fragment?;
                let keys = key_strings(&fragment.path);
                let text = if keys == ["payload", "content"] {
                    true
                } else if keys.len() >= 3
                    && keys[0..2] == ["payload", "content"]
                    && keys[keys.len() - 1] == "text"
                {
                    let parent = path_key(&fragment.path[..fragment.path.len() - 1]);
                    matches!(
                        summary.block_types.get(&parent)?.as_deref(),
                        Some("text" | "input_text" | "output_text")
                    )
                } else {
                    false
                };
                if text {
                    emit_fragment(
                        output,
                        &fragment,
                        "codex",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        summary.role.clone(),
                        None,
                    )?;
                }
            }
        }
        "function_call" | "custom_tool_call" => {
            let name = summary.name.clone().unwrap_or_default();
            let call_id = summary.call_id.clone().unwrap_or_default();
            if !call_id.is_empty() {
                own_calls.insert_value(STATE_KIND_TOOL_CALL, &call_id, &name)?;
                if is_own(&name, own_tools) {
                    own_calls.insert_value(STATE_KIND_OWN_CALL, &call_id, &name)?;
                }
            }
            emit_text(
                output,
                &name,
                "codex",
                session_id,
                event_id.clone(),
                timestamp.clone(),
                Some("tool"),
                Some(name.clone()),
            )?;
            for fragment in capture.iter()? {
                let fragment = fragment?;
                let keys = key_strings(&fragment.path);
                if starts_keys(&keys, &["payload", "arguments"])
                    || starts_keys(&keys, &["payload", "input"])
                {
                    emit_fragment(
                        output,
                        &fragment,
                        "codex",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        Some("tool".to_owned()),
                        Some(name.clone()),
                    )?;
                }
            }
        }
        "function_call_output" | "custom_tool_call_output" => {
            if let Some(call_id) = summary.call_id.as_deref()
                && own_calls.contains(STATE_KIND_OWN_CALL, call_id)?
            {
                return Ok(());
            }
            let result_tool = summary
                .call_id
                .as_deref()
                .map(|id| own_calls.value(STATE_KIND_TOOL_CALL, id))
                .transpose()?
                .flatten();
            for fragment in capture.iter()? {
                let fragment = fragment?;
                let keys = key_strings(&fragment.path);
                if starts_keys(&keys, &["payload", "output"]) {
                    emit_tool_result_fragment(
                        output,
                        &fragment,
                        "codex",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        Some("tool".to_owned()),
                        result_tool.clone(),
                    )?;
                }
            }
        }
        other => diagnostic(
            report,
            "unsupported_record",
            &format!("codex response item {other}"),
        ),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_claude(
    capture: &CaptureFile,
    summary: &RecordSummary,
    kind: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    own_calls: &mut DurableIdSet,
    own_tools: &[String],
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    report: &mut ScanReport,
) -> Result<()> {
    if !matches!(kind, "user" | "assistant") {
        if matches!(
            kind,
            "system" | "summary" | "progress" | "file-history-snapshot" | ""
        ) {
            return Ok(());
        }
        diagnostic(
            report,
            "unsupported_record",
            &format!("claude event type {kind}"),
        );
        return Ok(());
    }
    let role = summary.role.clone();
    let tool_use_name = block_scalar(capture, summary, "tool_use", "name")?;
    let tool_use_id = block_scalar(capture, summary, "tool_use", "id")?;
    let result_tool_use_id = block_scalar(capture, summary, "tool_result", "tool_use_id")?
        .or_else(|| summary.tool_use_id.clone());
    for fragment in capture.iter()? {
        let fragment = fragment?;
        let keys = key_strings(&fragment.path);
        if keys == ["message", "content"] && role.as_deref() != Some("tool") {
            emit_fragment(
                output,
                &fragment,
                "claude",
                session_id,
                event_id.clone(),
                timestamp.clone(),
                role.clone(),
                None,
            )?;
            continue;
        }
        if keys.len() >= 3 && starts_keys(&keys, &["message", "content"]) {
            let block_kind = block_type_for_fragment(summary, &fragment.path)?.unwrap_or_default();
            match block_kind.as_str() {
                "text" => emit_fragment(
                    output,
                    &fragment,
                    "claude",
                    session_id,
                    event_id.clone(),
                    timestamp.clone(),
                    role.clone(),
                    None,
                )?,
                "tool_use" if keys.iter().any(|key| key == "input") => {
                    let name = tool_use_name.clone().unwrap_or_default();
                    emit_fragment(
                        output,
                        &fragment,
                        "claude",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        Some("tool".to_owned()),
                        Some(name),
                    )?;
                }
                "tool_result" if keys.iter().any(|key| key == "content") => {
                    let own_result = if let Some(id) = result_tool_use_id.as_deref() {
                        own_calls.contains(STATE_KIND_OWN_CALL, id)?
                    } else {
                        false
                    };
                    if !own_result {
                        let result_tool = result_tool_use_id
                            .as_deref()
                            .map(|id| own_calls.value(STATE_KIND_TOOL_CALL, id))
                            .transpose()?
                            .flatten();
                        emit_tool_result_fragment(
                            output,
                            &fragment,
                            "claude",
                            session_id,
                            event_id.clone(),
                            timestamp.clone(),
                            Some("tool".to_owned()),
                            result_tool,
                        )?;
                    }
                }
                _ => {}
            }
        }
    }
    // A tool_use block's name/id are scalars and were captured in `summary`.
    if summary.block_types.has_value("tool_use")? {
        let name = tool_use_name.unwrap_or_default();
        let id = tool_use_id.unwrap_or_default();
        if !id.is_empty() {
            own_calls.insert_value(STATE_KIND_TOOL_CALL, &id, &name)?;
            if is_own(&name, own_tools) {
                own_calls.insert_value(STATE_KIND_OWN_CALL, &id, &name)?;
            }
        }
        emit_text(
            output,
            &name,
            "claude",
            session_id,
            event_id,
            timestamp,
            Some("tool"),
            Some(name.clone()),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn normalize_copilot(
    capture: &CaptureFile,
    summary: &RecordSummary,
    kind: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    own_calls: &mut DurableIdSet,
    own_tools: &[String],
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    report: &mut ScanReport,
) -> Result<()> {
    match kind {
        "session.start" | "session.resume" | "session.info" => return Ok(()),
        "user.message" | "assistant.message" => {
            for fragment in capture.iter()? {
                let fragment = fragment?;
                let keys = key_strings(&fragment.path);
                if starts_keys(&keys, &["data", "content"]) || starts_keys(&keys, &["content"]) {
                    emit_fragment(
                        output,
                        &fragment,
                        "copilot",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        Some(
                            if kind.starts_with("user") {
                                "user"
                            } else {
                                "assistant"
                            }
                            .to_owned(),
                        ),
                        None,
                    )?;
                }
            }
        }
        "tool.execution_start" | "tool.start" | "tool_use" | "tool.call" => {
            let name = summary
                .name
                .clone()
                .or_else(|| field_at_keys(&summary.fields, &["data", "mcpToolName"]))
                .or_else(|| field_at_keys(&summary.fields, &["data", "toolName"]))
                .unwrap_or_default();
            let call_id = summary
                .call_id
                .clone()
                .or_else(|| field_at_keys(&summary.fields, &["data", "toolCallId"]))
                .or_else(|| field_at_keys(&summary.fields, &["data", "callId"]))
                .unwrap_or_default();
            if !call_id.is_empty() {
                own_calls.insert_value(STATE_KIND_TOOL_CALL, &call_id, &name)?;
                if is_own(&name, own_tools) {
                    own_calls.insert_value(STATE_KIND_OWN_CALL, &call_id, &name)?;
                }
            }
            emit_text(
                output,
                &name,
                "copilot",
                session_id,
                event_id.clone(),
                timestamp.clone(),
                Some("tool"),
                Some(name.clone()),
            )?;
            for fragment in capture.iter()? {
                let fragment = fragment?;
                let keys = key_strings(&fragment.path);
                if starts_keys(&keys, &["data", "arguments"])
                    || starts_keys(&keys, &["data", "input"])
                {
                    emit_fragment(
                        output,
                        &fragment,
                        "copilot",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        Some("tool".to_owned()),
                        Some(name.clone()),
                    )?;
                }
            }
        }
        "tool.execution_complete" | "tool.complete" | "tool_result" | "tool.result" => {
            if let Some(call_id) = summary.call_id.as_deref()
                && own_calls.contains(STATE_KIND_OWN_CALL, call_id)?
            {
                return Ok(());
            }
            let result_tool = summary
                .call_id
                .as_deref()
                .map(|id| own_calls.value(STATE_KIND_TOOL_CALL, id))
                .transpose()?
                .flatten();
            for fragment in capture.iter()? {
                let fragment = fragment?;
                let keys = key_strings(&fragment.path);
                if starts_keys(&keys, &["data", "result", "content"])
                    || starts_keys(&keys, &["data", "result", "detailedContent"])
                    || starts_keys(&keys, &["result", "content"])
                {
                    emit_tool_result_fragment(
                        output,
                        &fragment,
                        "copilot",
                        session_id,
                        event_id.clone(),
                        timestamp.clone(),
                        Some("tool".to_owned()),
                        result_tool.clone(),
                    )?;
                }
            }
        }
        other => diagnostic(
            report,
            "unsupported_record",
            &format!("copilot event type {other}"),
        ),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_fragment(
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    fragment: &Fragment,
    agent: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    role: Option<String>,
    tool: Option<String>,
) -> Result<()> {
    emit_fragment_with_kind(
        output, fragment, agent, session_id, event_id, timestamp, role, tool, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_tool_result_fragment(
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    fragment: &Fragment,
    agent: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    role: Option<String>,
    tool: Option<String>,
) -> Result<()> {
    emit_fragment_with_kind(
        output,
        fragment,
        agent,
        session_id,
        event_id,
        timestamp,
        role,
        tool,
        Some("tool_result"),
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_fragment_with_kind(
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    fragment: &Fragment,
    agent: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    role: Option<String>,
    tool: Option<String>,
    field_kind: Option<&str>,
) -> Result<()> {
    emit_text_with_id(
        output,
        &fragment.text,
        agent,
        session_id,
        event_id,
        timestamp,
        role.as_deref(),
        tool,
        Some(path_key(&fragment.path)),
        field_kind,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_text(
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    text: &str,
    agent: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    role: Option<&str>,
    tool: Option<String>,
) -> Result<()> {
    emit_text_with_id(
        output, text, agent, session_id, event_id, timestamp, role, tool, None, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_text_with_id(
    output: &mut dyn FnMut(Normalized) -> Result<()>,
    text: &str,
    agent: &str,
    session_id: &str,
    event_id: Option<String>,
    timestamp: Option<String>,
    role: Option<&str>,
    tool: Option<String>,
    logical_id: Option<String>,
    field_kind_override: Option<&str>,
) -> Result<()> {
    if text.trim().is_empty() {
        return Ok(());
    }
    let field_kind = if let Some(field_kind) = field_kind_override {
        Some(field_kind.to_owned())
    } else if tool.is_some() {
        Some("tool_call".to_owned())
    } else if role == Some("tool") {
        Some("tool_result".to_owned())
    } else {
        Some("message".to_owned())
    };
    let dedup_key = field_kind.as_deref().and_then(|field_kind| {
        if !matches!(field_kind, "message" | "tool_result") {
            return None;
        }
        let mut key = String::new();
        for value in [
            Some(agent),
            Some(field_kind),
            Some(session_id),
            event_id.as_deref(),
            timestamp.as_deref(),
            role,
            tool.as_deref(),
        ] {
            let value = value.unwrap_or_default();
            key.push_str(&value.len().to_string());
            key.push(':');
            key.push_str(value);
            key.push(';');
        }
        Some(key)
    });
    output(Normalized {
        text: text.to_owned(),
        agent: agent.to_owned(),
        session_id: session_id.to_owned(),
        event_id,
        timestamp,
        role: role.map(str::to_owned),
        tool,
        field_kind,
        logical_id,
        dedup_key,
    })
}

struct SessionChunkAppender<'a> {
    output: &'a mut RecordSpool,
    progress: ProgressReporter,
    record: Option<RecordRef>,
    active_key: Option<String>,
    active: Option<Normalized>,
    text: String,
    tokens: Vec<String>,
    tokenizer: crate::text::StreamingTokenizer,
    count: u64,
}

impl<'a> SessionChunkAppender<'a> {
    fn new(output: &'a mut RecordSpool, progress: &ProgressReporter) -> Self {
        Self {
            output,
            progress: progress.clone(),
            record: None,
            active_key: None,
            active: None,
            text: String::new(),
            tokens: Vec::new(),
            tokenizer: crate::text::StreamingTokenizer::new(),
            count: 0,
        }
    }

    fn push(&mut self, normalized: Normalized, record: &RecordRef) -> Result<()> {
        self.progress
            .record_work_bytes(WorkKind::Tokenization, normalized.text.len() as u64);
        let standalone = normalized.logical_id.is_none();
        if standalone || self.active_key.as_deref() != normalized.logical_id.as_deref() {
            self.finish_active()?;
            self.record = Some(record.clone());
            self.active_key = normalized.logical_id.clone();
            self.active = Some(normalized.clone());
        }
        for character in normalized.text.chars() {
            if !self.text.is_empty()
                && self.text.len().saturating_add(character.len_utf8()) > MAX_CHUNK_BYTES
            {
                self.flush_current()?;
            }
            self.text.push(character);
            self.tokenizer
                .try_push(character, &mut |term| self.tokens.push(term))
                .map_err(|error| anyhow!("tokenizer spill: {error}"))?;
            if self.text.len() >= MAX_CHUNK_BYTES {
                // Keep a full final chunk until the logical field ends so a
                // term emitted by finish() is attached to actual text.
                self.flush_current_if_followed(normalized.logical_id.as_deref())?;
            }
        }
        if standalone {
            self.finish_active()?;
        }
        Ok(())
    }

    fn flush_current_if_followed(&mut self, _logical_id: Option<&str>) -> Result<()> {
        // The next character or field boundary will flush this full chunk;
        // retaining it here keeps end-of-field tokenizer terms searchable.
        Ok(())
    }

    fn finish_active(&mut self) -> Result<()> {
        if self.active.is_none() {
            return Ok(());
        }
        self.tokenizer
            .try_finish(&mut |term| self.tokens.push(term))
            .map_err(|error| anyhow!("tokenizer spill: {error}"))?;
        self.flush_current()?;
        self.active = None;
        self.active_key = None;
        self.record = None;
        self.tokenizer = crate::text::StreamingTokenizer::new();
        Ok(())
    }

    fn flush_current(&mut self) -> Result<()> {
        if self.text.is_empty() {
            self.tokens.clear();
            return Ok(());
        }
        let normalized = self
            .active
            .as_ref()
            .ok_or_else(|| anyhow!("session chunk has no metadata"))?;
        let record = self
            .record
            .as_ref()
            .ok_or_else(|| anyhow!("session chunk has no record"))?;
        let tokens = (!self.tokens.is_empty()).then(|| std::mem::take(&mut self.tokens));
        let chunk = make_chunk(&self.text, tokens, normalized, record);
        self.output.push(&chunk)?;
        self.progress
            .record_work_bytes(WorkKind::TempFileOps, chunk.text.len() as u64);
        self.progress.record_prepared_chunks(1);
        self.text.clear();
        self.count = self.count.saturating_add(1);
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        self.finish_active()?;
        Ok(self.count)
    }
}

fn make_chunk(
    text: &str,
    tokens: Option<Vec<String>>,
    normalized: &Normalized,
    record: &RecordRef,
) -> Chunk {
    Chunk {
        field_kind: normalized.field_kind.clone(),
        text: text.to_owned(),
        tokens,
        start_line: record.line,
        end_line: record.line,
        start_byte: record.start_byte,
        end_byte: record.end_byte,
        agent: Some(normalized.agent.clone()),
        session_id: Some(normalized.session_id.clone()),
        event_id: normalized.event_id.clone(),
        timestamp: normalized.timestamp.clone(),
        role: normalized.role.clone(),
        tool: normalized.tool.clone(),
    }
}

fn stream_records<F>(
    path: &Path,
    start_offset: u64,
    start_line: u64,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
    mut callback: F,
) -> Result<StreamResult>
where
    F: FnMut(&RecordRef) -> Result<()>,
{
    let mut input = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    input.seek(SeekFrom::Start(start_offset))?;
    let mut spool = RecordByteSpool::new()?;
    let mut hasher = Sha256::new();
    if start_offset > 0 {
        // The caller validates the prefix separately; the suffix stream only
        // needs a version hash when it is a full scan.  Keeping this branch
        // explicit prevents accidental hash-of-suffix version identifiers.
    }
    let mut buffer = [0_u8; 16 * 1024];
    let mut absolute = start_offset;
    let mut record_start = start_offset;
    let mut record_len = 0_u64;
    let mut line = start_line;
    let mut complete_offset = start_offset;
    loop {
        if !should_continue() {
            return Err(ScanCancelled.into());
        }
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        progress.record_source_bytes(count as u64, count as u64);
        hasher.update(&buffer[..count]);
        let mut segment = 0_usize;
        for (index, byte) in buffer[..count].iter().enumerate() {
            if *byte != b'\n' {
                continue;
            }
            if index + 1 > segment {
                spool.write_all(&buffer[segment..=index])?;
            }
            spool.flush()?;
            let record = RecordRef {
                path: spool.path.clone(),
                start_byte: record_start,
                end_byte: absolute.saturating_add(index as u64).saturating_add(1),
                line,
            };
            callback(&record)?;
            spool.reset()?;
            record_start = record.end_byte;
            complete_offset = record.end_byte;
            line = line.saturating_add(1);
            record_len = 0;
            segment = index + 1;
        }
        if segment < count {
            spool.write_all(&buffer[segment..count])?;
            record_len = record_len.saturating_add((count - segment) as u64);
        }
        absolute = absolute.saturating_add(count as u64);
    }
    let pending_tail = record_len != 0;
    Ok(StreamResult {
        version: hex_digest(hasher.finalize()),
        complete_offset,
        next_line: line,
        pending_tail,
    })
}

struct RecordByteSpool {
    path: PathBuf,
    file: Option<File>,
}

impl RecordByteSpool {
    fn new() -> Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "bm25-mcp-session-record-{}-{stamp}-{id}.json",
                std::process::id()
            ));
            let mut options = OpenOptions::new();
            options.create_new(true).read(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file: Some(file),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(anyhow!("could not create session record spool"))
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| anyhow!("session record spool closed"))?
            .write_all(bytes)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.file
            .as_mut()
            .ok_or_else(|| anyhow!("session record spool closed"))?
            .flush()?;
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| anyhow!("session record spool closed"))?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

impl Drop for RecordByteSpool {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

fn file_identity(path: &Path) -> Result<FileIdentityWithLength> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        bail!("session source is not a regular file")
    }
    let mut digest = Sha256::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        digest.update(MetadataExt::dev(&metadata).to_le_bytes());
        digest.update(MetadataExt::ino(&metadata).to_le_bytes());
    }
    if let Ok(created) = metadata.created()
        && let Ok(duration) = created.duration_since(UNIX_EPOCH)
    {
        digest.update(duration.as_nanos().to_le_bytes());
    }
    Ok(FileIdentityWithLength {
        len: metadata.len(),
        token: hex_digest(digest.finalize()),
        modified: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |duration| duration.as_nanos()),
    })
}

#[derive(Clone, Debug)]
struct FileIdentityWithLength {
    len: u64,
    token: String,
    modified: u128,
}

fn hash_prefix(
    path: &Path,
    offset: u64,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<String> {
    let mut file = File::open(path)?;
    let mut remaining = offset;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    while remaining > 0 {
        if !should_continue() {
            return Err(ScanCancelled.into());
        }
        let wanted = remaining.min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..wanted])?;
        if count == 0 {
            bail!("session prefix shorter than checkpoint")
        }
        hasher.update(&buffer[..count]);
        progress.record_source_bytes(count as u64, count as u64);
        remaining -= count as u64;
    }
    Ok(hex_digest(hasher.finalize()))
}

fn digest_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex_digest(hasher.finalize()))
}

fn digest_file_controlled(
    path: &Path,
    suffix_offset: u64,
    checkpoint_offset: u64,
    should_continue: &dyn Fn() -> bool,
    progress: &ProgressReporter,
) -> Result<(String, String, String, String)> {
    if suffix_offset > checkpoint_offset {
        bail!("session checkpoint precedes append offset")
    }
    let mut file = File::open(path)?;
    let mut full_hasher = Sha256::new();
    let mut suffix_hasher = Sha256::new();
    let mut checkpoint_prefix_digest = None;
    let mut previous_prefix_digest = None;
    let mut buffer = [0_u8; 16 * 1024];
    let mut offset = 0_u64;
    loop {
        if !should_continue() {
            return Err(ScanCancelled.into());
        }
        let count = file.read(&mut buffer)?;
        let end_offset = offset.saturating_add(count as u64);
        let bytes = &buffer[..count];
        let mut start = 0;
        // Snapshot the running hash at exact boundaries, including zero and EOF.
        for (boundary, digest) in [
            (suffix_offset, &mut previous_prefix_digest),
            (checkpoint_offset, &mut checkpoint_prefix_digest),
        ] {
            if digest.is_none() && boundary <= end_offset {
                let end = (boundary - offset) as usize;
                full_hasher.update(&bytes[start..end]);
                *digest = Some(hex_digest(full_hasher.clone().finalize()));
                start = end;
            }
        }
        if count == 0 {
            break;
        }
        progress.record_source_bytes(count as u64, count as u64);
        full_hasher.update(&bytes[start..]);
        if suffix_offset < end_offset {
            let start = suffix_offset.saturating_sub(offset) as usize;
            suffix_hasher.update(&bytes[start..]);
        }
        offset = end_offset;
    }
    Ok((
        hex_digest(full_hasher.finalize()),
        hex_digest(suffix_hasher.finalize()),
        checkpoint_prefix_digest
            .ok_or_else(|| anyhow!("unstable_source: session prefix shorter than checkpoint"))?,
        previous_prefix_digest
            .ok_or_else(|| anyhow!("unstable_source: session prefix shorter than append offset"))?,
    ))
}

fn discover_files(
    root: &Path,
    provider: Provider,
    files: &mut Vec<SessionFile>,
    report: &mut ScanReport,
    should_continue: &dyn Fn() -> bool,
) -> bool {
    match fs::metadata(root) {
        Ok(metadata) if metadata.is_dir() => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        result => {
            diagnostic(
                report,
                "discovery",
                &format!(
                    "{}: not an accessible directory: {result:?}",
                    root.display()
                ),
            );
            return false;
        }
    }
    let mut complete = true;
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        if !should_continue() {
            report.cancelled = true;
            report.pending_count += 1;
            return false;
        }
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) => {
                complete = false;
                diagnostic(
                    report,
                    "discovery",
                    &format!("{}: {error}", directory.display()),
                );
                continue;
            }
        };
        for entry in entries {
            if !should_continue() {
                report.cancelled = true;
                report.pending_count += 1;
                return false;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    complete = false;
                    diagnostic(
                        report,
                        "discovery",
                        &format!("{}: {error}", directory.display()),
                    );
                    continue;
                }
            };
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    complete = false;
                    diagnostic(report, "discovery", &format!("{}: {error}", path.display()));
                    continue;
                }
            };
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && (path
                    .extension()
                    .is_some_and(|extension| extension == "jsonl")
                    || path.file_name().is_some_and(|name| name == "events.jsonl"))
            {
                files.push(SessionFile { path, provider });
            }
        }
    }
    complete
}

fn provider_for_path(path: &Path, config: &SessionConfig) -> Option<Provider> {
    let path = canonical_or_normalized(path);
    let discoverable = path
        .extension()
        .is_some_and(|extension| extension == "jsonl")
        || path.file_name().is_some_and(|name| name == "events.jsonl");
    if !discoverable {
        return None;
    }
    let codex_sessions = canonical_or_normalized(&config.codex_home.join("sessions"));
    let codex_archived = canonical_or_normalized(&config.codex_home.join("archived_sessions"));
    let claude_projects = canonical_or_normalized(&config.claude_config_dir.join("projects"));
    let copilot_sessions = canonical_or_normalized(&config.copilot_home.join("session-state"));
    if path.starts_with(codex_sessions) || path.starts_with(codex_archived) {
        Some(Provider::Codex)
    } else if path.starts_with(claude_projects) {
        Some(Provider::Claude)
    } else if path.starts_with(copilot_sessions) {
        Some(Provider::Copilot)
    } else {
        None
    }
}

fn source_absolute_path(source: &str, config: &SessionConfig) -> Option<PathBuf> {
    let (provider, relative) = source.split_once(':')?;
    let base = match provider {
        "codex" => &config.codex_home,
        "claude" => &config.claude_config_dir,
        "copilot" => &config.copilot_home,
        _ => return None,
    };
    Some(canonical_or_normalized(&base.join(relative)))
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

fn source_path(provider: Provider, path: &Path, config: &SessionConfig) -> String {
    let base = match provider {
        Provider::Codex => &config.codex_home,
        Provider::Claude => &config.claude_config_dir,
        Provider::Copilot => &config.copilot_home,
    };
    let relative = path
        .strip_prefix(base)
        .map(|value| value.to_string_lossy().replace('\\', "/"))
        .or_else(|_| {
            let canonical_path = canonical_or_normalized(path);
            let canonical_base = canonical_or_normalized(base);
            canonical_path
                .strip_prefix(&canonical_base)
                .map(|value| value.to_string_lossy().replace('\\', "/"))
                .map_err(|_| ())
        })
        .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"));
    format!("{}:{relative}", provider.name())
}

fn source_key(owner_key: &str, path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(owner_key.as_bytes());
    hasher.update([0]);
    hasher.update(path.as_bytes());
    hex_digest(hasher.finalize())
}

fn identity_registry_revision(
    registry: Option<&IdentityRegistry>,
    should_continue: &dyn Fn() -> bool,
) -> Result<Option<String>> {
    let Some(registry) = registry else {
        return Ok(None);
    };
    let mut input = match File::open(registry.path()) {
        Ok(file) => BufReader::with_capacity(16 * 1024, file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Some("missing".to_owned()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        if !should_continue() {
            return Err(ScanCancelled.into());
        }
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(Some(hex_digest(digest.finalize())))
}

fn git_common_dir(path: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let common = PathBuf::from(value.trim());
    common.is_dir().then_some(common)
}

fn associate_cwd(
    cwd: &str,
    root: &Path,
    root_identity: &ProjectIdentity,
    root_is_git: bool,
    mut registry: Option<&mut IdentityRegistry>,
) -> Result<Option<String>> {
    let cwd_path = Path::new(cwd);
    // Live Git metadata is authoritative and should be persisted as soon as
    // parsing verifies it, even when this worktree has not yet been attached
    // as a code collection. Removed paths fall through to the registry below.
    if let Ok(identity) = project_identity(cwd_path)
        && identity.kind == crate::identity::RootKind::GitWorktree
    {
        if let Some(registry) = registry.as_deref_mut() {
            registry.remember_verified(&identity)?;
        }
        return Ok(Some(identity.owner_key));
    }
    if let Some(registry) = registry
        && let Some(association) = registry.associate_cwd(cwd_path, None)?
    {
        return Ok(Some(association.owner_key));
    }
    if !root_is_git
        && let Ok(canonical) = fs::canonicalize(cwd_path)
        && canonical.starts_with(root)
        && git_common_dir(&canonical).is_none()
    {
        return Ok(Some(root_identity.owner_key.clone()));
    }
    // A live Git path is handled above because the registry may not yet
    // contain a newly-created worktree. Deleted paths can only resolve from
    // the persisted registry.
    if let Ok(identity) = project_identity(cwd_path) {
        return Ok(Some(identity.owner_key));
    }
    if root_is_git {
        return Ok(None);
    }
    let canonical = match fs::canonicalize(cwd_path) {
        Ok(path) => path,
        Err(_) => return Ok(None),
    };
    if canonical.starts_with(root) && git_common_dir(&canonical).is_none() {
        return Ok(Some(root_identity.owner_key.clone()));
    }
    Ok(None)
}

fn path_key(path: &[PathPart]) -> String {
    let mut key = String::new();
    for part in path {
        match part {
            PathPart::Key(value) => {
                key.push_str("k:");
                key.push_str(&value.len().to_string());
                key.push(':');
                key.push_str(value);
                key.push(';');
            }
            PathPart::Index(value) => {
                key.push_str("i:");
                key.push_str(&value.to_string());
                key.push(';');
            }
        }
    }
    key
}

fn key_strings(path: &[PathPart]) -> Vec<String> {
    path.iter()
        .filter_map(|part| match part {
            PathPart::Key(key) => Some(key.clone()),
            PathPart::Index(_) => None,
        })
        .collect()
}

fn starts_keys(keys: &[String], expected: &[&str]) -> bool {
    keys.len() >= expected.len()
        && keys
            .iter()
            .zip(expected)
            .all(|(actual, wanted)| actual == wanted)
}

fn block_type_for_fragment(summary: &RecordSummary, path: &[PathPart]) -> Result<Option<String>> {
    // `type` is indexed at its containing object path.  For a nested tool
    // input/result value, walk up the path until that block object is found.
    // This keeps routing streamed structured fragments correct without
    // retaining a parsed JSON tree.
    for end in (0..path.len()).rev() {
        let candidate = path_key(&path[..end]);
        if let Some(block_kind) = summary.block_types.get(&candidate)? {
            return Ok(Some(block_kind));
        }
    }
    Ok(None)
}

fn block_scalar(
    capture: &CaptureFile,
    summary: &RecordSummary,
    block_kind: &str,
    key: &str,
) -> Result<Option<String>> {
    for item in capture.iter()? {
        let fragment = item?;
        if !matches!(fragment.path.last(), Some(PathPart::Key(value)) if value == key) {
            continue;
        }
        let Some(parent) = fragment.path.get(..fragment.path.len().saturating_sub(1)) else {
            continue;
        };
        if summary.block_types.get(&path_key(parent))?.as_deref() == Some(block_kind) {
            return Ok(Some(fragment.text));
        }
    }
    Ok(None)
}

fn field_exact(fields: &HashMap<String, String>, name: &str) -> Option<String> {
    fields
        .get(&path_key(&[PathPart::Key(name.to_owned())]))
        .cloned()
}

fn field_at_keys(fields: &HashMap<String, String>, names: &[&str]) -> Option<String> {
    let path = names
        .iter()
        .map(|name| PathPart::Key((*name).to_owned()))
        .collect::<Vec<_>>();
    fields.get(&path_key(&path)).cloned()
}

fn normalize_timestamp(value: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| {
            timestamp
                .with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        })
}

fn is_own(name: &str, names: &[String]) -> bool {
    names.iter().any(|configured| configured == name)
}

fn diagnostic(report: &mut ScanReport, category: &str, message: &str) {
    report.error_count = report.error_count.saturating_add(1);
    let count = report.diagnostics.entry(category.to_owned()).or_default();
    *count = count.saturating_add(1);
    if report.errors.len() < MAX_REPORTED_ERRORS {
        report
            .errors
            .push(format!("{category}: {}", truncate(message.to_owned())));
    }
}

fn truncate(mut value: String) -> String {
    if value.len() <= MAX_ERROR_BYTES {
        return value;
    }
    let mut end = MAX_ERROR_BYTES.saturating_sub(3);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push_str("...");
    value
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::register_plain_root;
    use crate::ingest::project_identity;
    use crate::model::SearchFilter;
    use std::process::Command;
    use tempfile::TempDir;

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("git installed");
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn setup() -> (TempDir, TempDir, SessionConfig, Store, String) {
        let project = TempDir::new().unwrap();
        git(project.path(), &["init", "-q"]);
        let home = TempDir::new().unwrap();
        let codex = home.path().join("codex");
        fs::create_dir_all(codex.join("sessions")).unwrap();
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: home.path().join("claude"),
            copilot_home: home.path().join("copilot"),
            ..SessionConfig::default()
        };
        let store = Store::open(&home.path().join("index.sqlite3")).unwrap();
        let identity = project_identity(project.path()).unwrap();
        (project, home, config, store, identity.owner_key)
    }

    fn line(value: serde_json::Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn session_filter(owner: &str) -> SearchFilter {
        SearchFilter {
            collection: owner.to_owned(),
            kind: SESSION_KIND.to_owned(),
            ..SearchFilter::default()
        }
    }

    #[test]
    fn verification_digests_match_independent_ranges() -> Result<()> {
        let directory = TempDir::new()?;
        let path = directory.path().join("ranges");
        let data: Vec<u8> = (0..49_153)
            .map(|i| ((i * 31 + i / 251) % 256) as u8)
            .collect();
        let boundaries = [0, 1, 63, 64, 65, 16_383, 16_384, 16_385, 32_768, 49_153];
        for len in boundaries {
            fs::write(&path, &data[..len])?;
            for previous in boundaries.into_iter().filter(|&offset| offset <= len) {
                for checkpoint in boundaries
                    .into_iter()
                    .filter(|&offset| previous <= offset && offset <= len)
                {
                    let actual = digest_file_controlled(
                        &path,
                        previous as u64,
                        checkpoint as u64,
                        &|| true,
                        &ProgressReporter::noop(),
                    )?;
                    let hash = |bytes: &[u8]| hex_digest(Sha256::digest(bytes));
                    assert_eq!(
                        actual,
                        (
                            hash(&data[..len]),
                            hash(&data[previous..len]),
                            hash(&data[..checkpoint]),
                            hash(&data[..previous]),
                        ),
                        "length {len}, previous {previous}, checkpoint {checkpoint}"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn verification_rejects_invalid_boundaries_and_cancellation() -> Result<()> {
        let directory = TempDir::new()?;
        let path = directory.path().join("ranges");
        fs::write(&path, b"abc")?;
        for (previous, checkpoint) in [(2, 1), (0, 4), (4, 4), (u64::MAX, u64::MAX)] {
            assert!(
                digest_file_controlled(
                    &path,
                    previous,
                    checkpoint,
                    &|| true,
                    &ProgressReporter::noop(),
                )
                .is_err(),
                "previous {previous}, checkpoint {checkpoint}"
            );
        }
        let error =
            digest_file_controlled(&path, 0, 3, &|| false, &ProgressReporter::noop()).unwrap_err();
        assert!(error.downcast_ref::<ScanCancelled>().is_some());
        Ok(())
    }

    #[test]
    #[ignore = "release-mode verification microbenchmark"]
    fn benchmark_verification() -> Result<()> {
        let directory = TempDir::new()?;
        let path = directory.path().join("benchmark");
        let block: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        for (prefix_mib, suffix_bytes) in [
            (64, 1024),
            (256, 1024),
            (64, 1024 * 1024),
            (0, 64 * 1024 * 1024),
        ] {
            let mut file = File::create(&path)?;
            for _ in 0..prefix_mib {
                file.write_all(&block)?;
            }
            for _ in 0..suffix_bytes / block.len() {
                file.write_all(&block)?;
            }
            file.write_all(&block[..suffix_bytes % block.len()])?;
            drop(file);
            let previous = prefix_mib * 1024 * 1024;
            let checkpoint = previous + suffix_bytes as u64;
            for trial in 0..6 {
                let started = Instant::now();
                std::hint::black_box(digest_file_controlled(
                    &path,
                    previous,
                    checkpoint,
                    &|| true,
                    &ProgressReporter::noop(),
                )?);
                if trial != 0 {
                    println!(
                        "prefix={previous} suffix={suffix_bytes} trial={trial} elapsed_us={}",
                        started.elapsed().as_micros()
                    );
                }
            }
        }
        Ok(())
    }

    fn test_normalized_field(path: &str, text: &str, event_id: &str) -> Normalized {
        Normalized {
            text: text.to_owned(),
            agent: "copilot".to_owned(),
            session_id: "test-session".to_owned(),
            event_id: Some(event_id.to_owned()),
            timestamp: None,
            role: Some("tool".to_owned()),
            tool: Some("external_tool".to_owned()),
            field_kind: Some("tool_result".to_owned()),
            logical_id: Some(path.to_owned()),
            dedup_key: Some("copilot:tool_result".to_owned()),
        }
    }

    #[test]
    fn visible_field_deduper_suppresses_exact_sibling_fields() -> Result<()> {
        let mut deduper = VisibleFieldDeduper::new();
        let mut output = Vec::new();
        let mut append = |item| {
            output.push(item);
            Ok(())
        };
        deduper.push(
            test_normalized_field("result/content", "same sibling text", "event-1"),
            &mut append,
        )?;
        deduper.push(
            test_normalized_field("result/detailedContent", "same sibling text", "event-1"),
            &mut append,
        )?;
        deduper.finish(&mut append)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].text, "same sibling text");
        assert_eq!(output[0].logical_id.as_deref(), Some("result/content"));
        Ok(())
    }

    #[test]
    fn visible_field_deduper_keeps_distinct_fields_and_events() -> Result<()> {
        let mut deduper = VisibleFieldDeduper::new();
        let mut output = Vec::new();
        let mut append = |item| {
            output.push(item);
            Ok(())
        };
        deduper.push(
            test_normalized_field("result/content", "first field", "event-1"),
            &mut append,
        )?;
        deduper.push(
            test_normalized_field("result/detailedContent", "second field", "event-1"),
            &mut append,
        )?;
        deduper.finish(&mut append)?;
        assert_eq!(output.len(), 2);

        let mut event_two = VisibleFieldDeduper::new();
        let mut event_two_output = Vec::new();
        let mut append_event_two = |item| {
            event_two_output.push(item);
            Ok(())
        };
        event_two.push(
            test_normalized_field("result/content", "same event text", "event-2"),
            &mut append_event_two,
        )?;
        event_two.finish(&mut append_event_two)?;

        let mut event_three = VisibleFieldDeduper::new();
        let mut event_three_output = Vec::new();
        let mut append_event_three = |item| {
            event_three_output.push(item);
            Ok(())
        };
        event_three.push(
            test_normalized_field("result/content", "same event text", "event-3"),
            &mut append_event_three,
        )?;
        event_three.finish(&mut append_event_three)?;
        assert_eq!(event_two_output.len(), 1);
        assert_eq!(event_three_output.len(), 1);
        assert_eq!(event_two_output[0].event_id.as_deref(), Some("event-2"));
        assert_eq!(event_three_output[0].event_id.as_deref(), Some("event-3"));

        // Tool arguments are separate semantic fields even when their values
        // happen to match; the result/message namespace is the only one that
        // participates in exact sibling suppression.
        let mut arguments = VisibleFieldDeduper::new();
        let mut argument_output = Vec::new();
        let mut append_argument = |item| {
            argument_output.push(item);
            Ok(())
        };
        for path in ["arguments/first", "arguments/second"] {
            let mut argument = test_normalized_field(path, "same argument", "event-4");
            argument.field_kind = Some("tool_call".to_owned());
            argument.tool = Some("external_tool".to_owned());
            argument.dedup_key = None;
            arguments.push(argument, &mut append_argument)?;
        }
        arguments.finish(&mut append_argument)?;
        assert_eq!(argument_output.len(), 2);
        Ok(())
    }

    #[test]
    fn visible_field_deduper_compares_huge_values_across_fragment_boundaries() -> Result<()> {
        let value = format!(
            "{}boundary-marker{}",
            "a".repeat(MAX_FRAGMENT_BYTES * 2 + 13),
            "b".repeat(MAX_FRAGMENT_BYTES + 7)
        );
        let first_split = MAX_FRAGMENT_BYTES - 1;
        let second_split = MAX_FRAGMENT_BYTES + 5;
        let mut deduper = VisibleFieldDeduper::new();
        let mut output = Vec::new();
        let mut append = |item| {
            output.push(item);
            Ok(())
        };
        deduper.push(
            test_normalized_field("result/content", &value[..first_split], "event-1"),
            &mut append,
        )?;
        deduper.push(
            test_normalized_field(
                "result/content",
                &value[first_split..second_split],
                "event-1",
            ),
            &mut append,
        )?;
        deduper.push(
            test_normalized_field("result/content", &value[second_split..], "event-1"),
            &mut append,
        )?;
        deduper.push(
            test_normalized_field("result/detailedContent", &value, "event-1"),
            &mut append,
        )?;
        deduper.finish(&mut append)?;
        assert!(output.len() > 1);
        assert_eq!(
            output.iter().map(|item| item.text.len()).sum::<usize>(),
            value.len()
        );
        assert_eq!(
            output
                .iter()
                .map(|item| item.text.as_str())
                .collect::<String>(),
            value
        );
        Ok(())
    }

    #[test]
    fn copilot_duplicate_result_siblings_are_indexed_once() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        fs::create_dir_all(config.copilot_home.join("session-state"))?;
        let path = config.copilot_home.join("session-state/events.jsonl");
        let huge = format!(
            "copilotduplicateprefix {} copilotduplicateboundarymarker",
            "x".repeat(MAX_FRAGMENT_BYTES * 2 + 11)
        );
        let values = [
            serde_json::json!({
                "type": "session.start",
                "data": {"context": {"cwd": project.path()}, "sessionId": "copilot-dedup"}
            }),
            serde_json::json!({
                "type": "tool.execution_complete",
                "id": "copilot-event-1",
                "data": {"toolCallId": "call-1", "result": {
                    "content": huge,
                    "detailedContent": huge
                }}
            }),
            serde_json::json!({
                "type": "tool.execution_complete",
                "id": "copilot-event-2",
                "data": {"toolCallId": "call-2", "result": {
                    "content": "copilotdistinctfirst",
                    "detailedContent": "copilotdistinctsecond"
                }}
            }),
            serde_json::json!({
                "type": "tool.execution_complete",
                "id": "copilot-event-3",
                "data": {"toolCallId": "call-3", "result": {
                    "content": "copilotseparateeventmarker"
                }}
            }),
            serde_json::json!({
                "type": "tool.execution_complete",
                "id": "copilot-event-4",
                "data": {"toolCallId": "call-4", "result": {
                    "content": "copilotseparateeventmarker"
                }}
            }),
        ];
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend(line(value));
        }
        fs::write(path, bytes)?;

        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(report.error_count, 0, "{report:?}");
        let filter = session_filter(&owner);
        let (_, duplicate_hits) = store.search("copilotduplicateboundarymarker", &filter, 20)?;
        assert_eq!(duplicate_hits.len(), 1);
        assert_eq!(
            duplicate_hits[0].chunk.event_id.as_deref(),
            Some("copilot-event-1")
        );
        let (_, first_hits) = store.search("copilotdistinctfirst", &filter, 20)?;
        assert_eq!(first_hits.len(), 1);
        let (_, second_hits) = store.search("copilotdistinctsecond", &filter, 20)?;
        assert_eq!(second_hits.len(), 1);
        let (_, separate_hits) = store.search("copilotseparateeventmarker", &filter, 20)?;
        assert_eq!(separate_hits.len(), 2);
        assert_ne!(
            separate_hits[0].chunk.event_id,
            separate_hits[1].chunk.event_id
        );
        Ok(())
    }

    #[test]
    fn huge_record_searches_complete_tail_without_value_allocation() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/huge.jsonl");
        let huge = format!(
            "{} tail_marker_huge_record",
            "x".repeat(MAX_CHUNK_BYTES * 8)
        );
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": project.path(), "id": "huge"}
        }));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "huge-message",
                "role": "user",
                "content": [{"type": "text", "text": huge}]
            }
        })));
        fs::write(&path, bytes).unwrap();
        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(report.error_count, 0, "{report:?}");
        let (_, hits) = store.search("tail_marker_huge_record", &session_filter(&owner), 10)?;
        assert_eq!(hits.len(), 1);
        assert!(hits[0].chunk.text.contains("tail_marker_huge_record"));
        Ok(())
    }

    #[test]
    fn append_reuses_checkpoint_and_persists_own_call_state() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/append.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": project.path(), "id": "append"}
        }));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "id": "call-event",
                "name": "mcp__bm25-mcp__search_project",
                "call_id": "own-call",
                "arguments": "needle"
            }
        })));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "id": "external-call-event",
                "name": "external_lookup",
                "call_id": "external-call",
                "arguments": "needle"
            }
        })));
        fs::write(&path, bytes).unwrap();
        scan(project.path(), &owner, &store, &config, &|| true)?;
        let source = store.sources(&owner, SESSION_KIND)?.pop().unwrap();
        let first_checkpoint = store.session_checkpoint(&source.key)?.unwrap();
        let mut append = OpenOptions::new().append(true).open(&path)?;
        append.write_all(&line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "id": "output-event",
                "call_id": "own-call",
                "output": "zzownexcludedmarker"
            }
        })))?;
        append.write_all(&line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "id": "external-output-event",
                "call_id": "external-call",
                "output": "zzexternalincludedmarker"
            }
        })))?;
        append.write_all(&line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "new-event",
                "role": "assistant",
                "content": [{"type": "text", "text": "append_visible_marker"}]
            }
        })))?;
        drop(append);
        scan(project.path(), &owner, &store, &config, &|| true)?;
        let (_, old_hits) = store.search("zzownexcludedmarker", &session_filter(&owner), 10)?;
        assert!(old_hits.is_empty());
        let (_, new_hits) = store.search("append_visible_marker", &session_filter(&owner), 10)?;
        assert_eq!(new_hits.len(), 1);
        let (_, tool_hits) =
            store.search("zzexternalincludedmarker", &session_filter(&owner), 10)?;
        assert_eq!(tool_hits.len(), 1);
        assert_eq!(tool_hits[0].chunk.tool.as_deref(), Some("external_lookup"));
        assert_eq!(
            tool_hits[0].chunk.field_kind.as_deref(),
            Some("tool_result")
        );
        let checkpoint = store.session_checkpoint(&source.key)?.unwrap();
        assert!(checkpoint.offset > first_checkpoint.offset);
        assert_eq!(checkpoint.offset, fs::metadata(&path)?.len());
        Ok(())
    }

    #[test]
    fn structured_arguments_and_results_keep_keys_and_scalars() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/structured.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": project.path(), "id": "structured"}
        }));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "id": "structured-call-event",
                "name": "external_structured_lookup",
                "call_id": "structured-call",
                "arguments": {
                    "port": 8080,
                    "nested": {"enabled": true, "label": "structuredargmarkerxyz"}
                }
            }
        })));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "id": "structured-output-event",
                "call_id": "structured-call",
                "output": {
                    "nested": {"status": "structuredresultmarkerxyz", "count": 2},
                    "ok": false
                }
            }
        })));
        fs::write(&path, bytes).unwrap();

        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(report.error_count, 0, "{report:?}");
        let filter = session_filter(&owner);
        let (_, numeric_hits) = store.search("8080", &filter, 10)?;
        assert_eq!(numeric_hits.len(), 1);
        assert!(numeric_hits[0].chunk.text.contains("8080"));
        let (_, argument_hits) = store.search("structuredargmarkerxyz", &filter, 10)?;
        assert_eq!(argument_hits.len(), 1);
        let (_, result_hits) = store.search("structuredresultmarkerxyz", &filter, 10)?;
        assert_eq!(result_hits.len(), 1);
        assert_eq!(
            result_hits[0].chunk.field_kind.as_deref(),
            Some("tool_result")
        );
        assert_eq!(
            result_hits[0].chunk.tool.as_deref(),
            Some("external_structured_lookup")
        );
        Ok(())
    }

    #[test]
    fn rewrite_invalidates_old_chunks_and_rebuilds_checkpoint() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/rewrite.jsonl");
        let initial = [
            serde_json::json!({"type":"session_meta","payload":{"cwd":project.path(),"id":"rewrite"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","id":"old","role":"user","content":[{"type":"text","text":"olduniquealpha"}]}}),
        ];
        let mut bytes = Vec::new();
        for value in initial {
            bytes.extend(line(value));
        }
        fs::write(&path, bytes).unwrap();
        scan(project.path(), &owner, &store, &config, &|| true)?;
        fs::write(
            &path,
            [
                line(serde_json::json!({"type":"session_meta","payload":{"cwd":project.path(),"id":"rewrite"}})),
                line(serde_json::json!({"type":"response_item","payload":{"type":"message","id":"new","role":"user","content":[{"type":"text","text":"newuniquebeta"}]}})),
            ].concat(),
        )?;
        scan(project.path(), &owner, &store, &config, &|| true)?;
        let (_, old_hits) = store.search("olduniquealpha", &session_filter(&owner), 10)?;
        assert!(old_hits.is_empty());
        let (_, new_hits) = store.search("newuniquebeta", &session_filter(&owner), 10)?;
        assert_eq!(new_hits.len(), 1);
        Ok(())
    }

    #[test]
    fn known_other_project_history_is_excluded_without_error() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let other = TempDir::new().unwrap();
        git(other.path(), &["init", "-q"]);
        let path = config.codex_home.join("sessions/other.jsonl");
        let bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": other.path(), "id": "other"}
        }));
        fs::write(&path, bytes).unwrap();

        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(report.error_count, 0, "{report:?}");
        assert_eq!(report.excluded_count, 1);
        assert_eq!(report.diagnostics.get("ownership_excluded"), Some(&1));
        assert!(store.sources(&owner, SESSION_KIND)?.is_empty());
        Ok(())
    }

    #[test]
    fn registry_revision_rechecks_append_ownership_for_nested_plain_roots() -> Result<()> {
        let parent = TempDir::new()?;
        let child = parent.path().join("nested");
        fs::create_dir(&child)?;
        let home = TempDir::new()?;
        let codex = home.path().join("codex");
        fs::create_dir_all(codex.join("sessions"))?;
        let registry_path = home.path().join("cache/identity-registry.json");
        let config = SessionConfig {
            codex_home: codex,
            claude_config_dir: home.path().join("claude"),
            copilot_home: home.path().join("copilot"),
            identity_registry_path: Some(registry_path.clone()),
            ..SessionConfig::default()
        };
        let store = Store::open(&home.path().join("index.sqlite3"))?;
        let parent_identity = project_identity(parent.path())?;
        let child_identity = project_identity(&child)?;
        let path = config.codex_home.join("sessions/nested-owner.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": child, "id": "nested-owner"}
        }));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "nested-owner-message",
                "role": "user",
                "content": [{"type": "text", "text": "nested_owner_marker"}]
            }
        })));
        fs::write(&path, bytes)?;
        register_plain_root(parent.path(), &registry_path)?;

        scan(
            parent.path(),
            &parent_identity.owner_key,
            &store,
            &config,
            &|| true,
        )?;
        let parent_filter = session_filter(&parent_identity.owner_key);
        let (_, parent_hits) = store.search("nested_owner_marker", &parent_filter, 10)?;
        assert_eq!(parent_hits.len(), 1);

        register_plain_root(&child, &registry_path)?;
        let report = scan(
            parent.path(),
            &parent_identity.owner_key,
            &store,
            &config,
            &|| true,
        )?;
        assert_eq!(report.error_count, 0, "{report:?}");
        assert_eq!(report.excluded_count, 1);
        let (_, parent_hits) = store.search("nested_owner_marker", &parent_filter, 10)?;
        assert!(parent_hits.is_empty());

        scan(&child, &child_identity.owner_key, &store, &config, &|| true)?;
        let child_filter = session_filter(&child_identity.owner_key);
        let (_, child_hits) = store.search("nested_owner_marker", &child_filter, 10)?;
        assert_eq!(child_hits.len(), 1);
        Ok(())
    }

    #[test]
    fn ownership_uses_provider_routing_fields_only() -> Result<()> {
        let project = TempDir::new()?;
        let owner_cwd = project.path().to_string_lossy().into_owned();
        let cases = [
            (
                Provider::Codex,
                serde_json::json!({
                    "type": "session_meta",
                    "payload": {"cwd": owner_cwd}
                }),
                Some(owner_cwd.clone()),
            ),
            (
                Provider::Codex,
                serde_json::json!({
                    "type": "turn_context",
                    "payload": {"cwd": owner_cwd}
                }),
                Some(owner_cwd.clone()),
            ),
            (
                Provider::Codex,
                serde_json::json!({
                    "type": "world_state",
                    "payload": {
                        "state": {"environments": {"environments": {"local": {"cwd": owner_cwd}}}}
                    }
                }),
                None,
            ),
            (
                Provider::Codex,
                serde_json::json!({
                    "type": "event_msg",
                    "payload": {
                        "thread_settings": {"cwd": owner_cwd},
                        "item": {"cwd": "file:///unrelated/project"}
                    }
                }),
                None,
            ),
            (
                Provider::Codex,
                serde_json::json!({
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "content": [{"type": "text", "cwd": "file:///unrelated/project"}]
                    }
                }),
                None,
            ),
            (
                Provider::Claude,
                serde_json::json!({"type": "user", "cwd": owner_cwd}),
                Some(owner_cwd.clone()),
            ),
            (
                Provider::Claude,
                serde_json::json!({
                    "type": "user",
                    "message": {"cwd": "file:///unrelated/project"}
                }),
                None,
            ),
            (
                Provider::Copilot,
                serde_json::json!({
                    "type": "session.start",
                    "data": {"context": {"cwd": owner_cwd}}
                }),
                Some(owner_cwd.clone()),
            ),
            (
                Provider::Copilot,
                serde_json::json!({
                    "type": "tool.execution_start",
                    "data": {"context": {"cwd": "file:///unrelated/project"}}
                }),
                None,
            ),
        ];

        for (provider, value, expected) in cases {
            let mut record = tempfile::NamedTempFile::new()?;
            record.write_all(&line(value))?;
            let capture = CaptureFile::parse(record.path())?;
            let summary = summarize(provider, &capture)?;
            assert_eq!(summary.cwd, expected);
        }
        Ok(())
    }

    #[test]
    fn nested_codex_tool_cwd_does_not_exclude_history() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/nested-cwd.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": project.path(), "id": "nested-cwd"}
        }));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "nested-cwd-message",
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "nested_cwd_marker",
                    "cwd": "file:///unrelated/project"
                }]
            }
        })));
        fs::write(&path, bytes)?;

        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(report.error_count, 0, "{report:?}");
        let (_, hits) = store.search("nested_cwd_marker", &session_filter(&owner), 10)?;
        assert_eq!(hits.len(), 1);
        Ok(())
    }

    #[test]
    fn diagnostics_persist_beyond_bounded_error_samples() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/diagnostics.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": project.path(), "id": "diagnostics"}
        }));
        for index in 0..80 {
            bytes.extend(line(serde_json::json!({
                "type": "response_item",
                "payload": {
                    "type": format!("future_variant_{index}"),
                    "id": format!("future-event-{index}")
                }
            })));
        }
        fs::write(&path, bytes)?;

        let first = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(first.diagnostics.get("unsupported_record"), Some(&80));
        assert_eq!(first.errors.len(), MAX_REPORTED_ERRORS);

        let mut append = OpenOptions::new().append(true).open(&path)?;
        append.write_all(&line(serde_json::json!({
            "type": "response_item",
            "payload": {"type": "future_variant_append", "id": "future-event-append"}
        })))?;
        drop(append);

        let second = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(second.diagnostics.get("unsupported_record"), Some(&81));
        let source = store.sources(&owner, SESSION_KIND)?.pop().unwrap();
        let checkpoint = store.session_checkpoint(&source.key)?.unwrap();
        let state = serde_json::from_str::<PersistedState>(&checkpoint.state)?;
        assert_eq!(state.diagnostics.get("unsupported_record"), Some(&81));
        Ok(())
    }

    #[test]
    fn codex_event_ids_do_not_replace_session_id_or_tool_metadata() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/stable-session.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {
                "cwd": project.path(),
                "id": "meta-event",
                "session_id": "stable-session"
            }
        }));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "id": "call-event",
                "name": "external_collision_tool",
                "call_id": "call-stable",
                "arguments": {
                    "name": "zzargumentnamecollision",
                    "id": "argument_id_collision",
                    "cwd": "file:///unrelated/project"
                }
            }
        })));
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "message-event",
                "role": "user",
                "content": [{"type": "text", "text": "zzstablesessionmarker"}]
            }
        })));
        fs::write(&path, bytes)?;

        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert_eq!(report.error_count, 0, "{report:?}");
        let filter = session_filter(&owner);
        let (_, marker_hits) = store.search("zzstablesessionmarker", &filter, 10)?;
        assert_eq!(marker_hits.len(), 1);
        assert_eq!(
            marker_hits[0].chunk.session_id.as_deref(),
            Some("stable-session")
        );
        let (_, argument_hits) = store.search("zzargumentnamecollision", &filter, 10)?;
        assert_eq!(argument_hits.len(), 1);
        assert_eq!(
            argument_hits[0].chunk.tool.as_deref(),
            Some("external_collision_tool")
        );
        Ok(())
    }

    #[test]
    fn malformed_complete_record_is_resynchronized_with_category() -> Result<()> {
        let (project, _home, config, store, owner) = setup();
        let path = config.codex_home.join("sessions/malformed.jsonl");
        let mut bytes = line(serde_json::json!({
            "type": "session_meta",
            "payload": {"cwd": project.path(), "id": "malformed"}
        }));
        bytes.extend(b"{this is malformed}\n");
        bytes.extend(line(serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "after-malformed",
                "role": "user",
                "content": [{"type": "text", "text": "after_malformed_marker"}]
            }
        })));
        fs::write(&path, bytes).unwrap();
        let report = scan(project.path(), &owner, &store, &config, &|| true)?;
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.starts_with("malformed_record:"))
        );
        let (_, hits) = store.search("after_malformed_marker", &session_filter(&owner), 10)?;
        assert_eq!(hits.len(), 1);
        Ok(())
    }
}
