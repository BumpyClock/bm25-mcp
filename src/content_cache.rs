//! Bounded disk cache for verified, tokenized source chunks.
//!
//! The cache is keyed by the raw source digest and tokenizer version. It is
//! a recomputable optimization: SQLite remains authoritative and a missing or
//! corrupt cache entry simply causes ingestion to use the source spool. Cache
//! entries are JSONL so a hit is streamed to the store without materializing a
//! whole source in memory.

use crate::model::Chunk;
use crate::text::normalization_version;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const MANIFEST_VERSION: u32 = 1;
const MANIFEST_NAME: &str = "manifest.json";
const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_ENTRIES: usize = 1024;
const MAX_CACHE_LINE_BYTES: usize = 16 * 1024 * 1024;
/// Bump when decoded source chunking or binary eligibility changes.
pub const INGEST_CACHE_VERSION: &str = "bm25-mcp-ingest-v1";

#[derive(Clone, Debug)]
pub struct ContentCache {
    root: PathBuf,
    max_bytes: u64,
    max_entries: usize,
    manifest: CacheManifest,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct CacheManifest {
    version: u32,
    entries: Vec<CacheEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CacheEntry {
    key: String,
    bytes: u64,
    #[serde(default)]
    chunks: u64,
    #[serde(default)]
    checksum: String,
    last_used_unix: u64,
}

/// A line-at-a-time cache hit iterator.
#[derive(Debug)]
pub struct CachedChunks {
    reader: Option<BufReader<File>>,
    line: Vec<u8>,
    chunk_count: u64,
}

impl ContentCache {
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_limits(root, DEFAULT_MAX_BYTES, DEFAULT_MAX_ENTRIES)
    }

    pub fn open_with_limits(root: &Path, max_bytes: u64, max_entries: usize) -> Result<Self> {
        if max_bytes == 0 || max_entries == 0 {
            return Err(anyhow!("content cache limits must be positive"));
        }
        fs::create_dir_all(root)
            .with_context(|| format!("create content cache {}", root.display()))?;
        let manifest_path = root.join(MANIFEST_NAME);
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path).with_context(|| {
                format!("read content cache manifest {}", manifest_path.display())
            })?;
            let manifest: CacheManifest = serde_json::from_slice(&bytes).with_context(|| {
                format!("decode content cache manifest {}", manifest_path.display())
            })?;
            if manifest.version != MANIFEST_VERSION {
                CacheManifest {
                    version: MANIFEST_VERSION,
                    entries: Vec::new(),
                }
            } else {
                manifest
            }
        } else {
            CacheManifest {
                version: MANIFEST_VERSION,
                entries: Vec::new(),
            }
        };
        let mut cache = Self {
            root: root.to_path_buf(),
            max_bytes,
            max_entries,
            manifest,
        };
        cache.clean_missing()?;
        cache.clean_orphans()?;
        cache.prune()?;
        Ok(cache)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Look up a digest and return a stream over immutable cached chunks.
    pub fn get(&mut self, source_version: &str) -> Result<Option<CachedChunks>> {
        let key = cache_key(source_version);
        let Some(index) = self
            .manifest
            .entries
            .iter()
            .position(|entry| entry.key == key)
        else {
            return Ok(None);
        };
        let path = self.path_for(&key);
        let expected_checksum = self.manifest.entries[index].checksum.clone();
        let validated = match validate_cache_file(&path) {
            Ok(validated) => validated,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.manifest.entries.remove(index);
                let _ = self.persist_manifest();
                return Ok(None);
            }
            Err(_) => {
                self.manifest.entries.remove(index);
                let _ = fs::remove_file(&path);
                let _ = self.persist_manifest();
                return Ok(None);
            }
        };
        let Some((chunk_count, checksum)) = validated else {
            self.manifest.entries.remove(index);
            let _ = fs::remove_file(&path);
            let _ = self.persist_manifest();
            return Ok(None);
        };
        if !expected_checksum.is_empty() && expected_checksum != checksum {
            self.manifest.entries.remove(index);
            let _ = fs::remove_file(&path);
            let _ = self.persist_manifest();
            return Ok(None);
        }
        self.manifest.entries[index].last_used_unix = now_unix();
        self.manifest.entries[index].chunks = chunk_count;
        self.manifest.entries[index].checksum = checksum;
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.manifest.entries.remove(index);
                let _ = self.persist_manifest();
                return Ok(None);
            }
            Err(_) => {
                self.manifest.entries.remove(index);
                let _ = self.persist_manifest();
                return Ok(None);
            }
        };
        // Manifest bookkeeping is best effort: cache metadata failure must
        // never prevent the authoritative source from being decoded.
        let _ = self.persist_manifest();
        Ok(Some(CachedChunks {
            reader: Some(BufReader::new(file)),
            line: Vec::new(),
            chunk_count,
        }))
    }

    /// Materialize a validated chunk stream in the cache. The source digest
    /// is supplied by the caller only after its before/after identity check.
    pub fn put<I>(&mut self, source_version: &str, chunks: I) -> Result<()>
    where
        I: IntoIterator<Item = Result<Chunk>>,
    {
        let key = cache_key(source_version);
        let path = self.path_for(&key);
        let temp = self.temp_path(&key);
        let result = (|| -> Result<(u64, u64, String)> {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let file = options
                .open(&temp)
                .with_context(|| format!("create content cache temp {}", temp.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
            }
            let mut writer = BufWriter::new(file);
            let mut chunk_count = 0u64;
            for chunk in chunks {
                serde_json::to_writer(&mut writer, &chunk?)?;
                writer.write_all(b"\n")?;
                chunk_count = chunk_count.saturating_add(1);
            }
            writer.flush()?;
            let file = writer.into_inner().map_err(|error| error.into_error())?;
            file.sync_all()?;
            let bytes = file.metadata()?.len();
            drop(file);
            fs::rename(&temp, &path)?;
            let checksum = checksum_file(&path)?;
            Ok((bytes, chunk_count, checksum))
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        let (bytes, chunk_count, checksum) = result?;
        self.manifest.entries.retain(|entry| entry.key != key);
        self.manifest.entries.push(CacheEntry {
            key,
            bytes,
            chunks: chunk_count,
            checksum,
            last_used_unix: now_unix(),
        });
        self.prune()
    }

    /// Remove stale entries and enforce both disk and entry budgets.
    pub fn prune(&mut self) -> Result<()> {
        self.manifest
            .entries
            .sort_by_key(|entry| Reverse(entry.last_used_unix));
        let mut total = 0u64;
        let mut kept = Vec::with_capacity(self.manifest.entries.len());
        let mut seen_keys = HashSet::new();
        let root = self.root.clone();
        for entry in self.manifest.entries.drain(..) {
            if !seen_keys.insert(entry.key.clone()) {
                // A duplicate manifest row refers to the same file as the
                // retained row; dropping the row must not remove that file.
                continue;
            }
            let keep = kept.len() < self.max_entries
                && total.saturating_add(entry.bytes) <= self.max_bytes;
            if keep {
                total = total.saturating_add(entry.bytes);
                kept.push(entry);
            } else {
                let _ = fs::remove_file(root.join(format!("{}.jsonl", entry.key)));
            }
        }
        self.manifest.entries = kept;
        self.persist_manifest()
    }

    fn clean_missing(&mut self) -> Result<()> {
        let root = self.root.clone();
        self.manifest
            .entries
            .retain(|entry| root.join(format!("{}.jsonl", entry.key)).is_file());
        self.persist_manifest()
    }

    fn clean_orphans(&self) -> Result<()> {
        let referenced: HashSet<&str> = self
            .manifest
            .entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect();
        for item in fs::read_dir(&self.root)? {
            let item = item?;
            let path = item.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(key) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            if !referenced.contains(key) {
                let _ = fs::remove_file(path);
            }
        }
        Ok(())
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.jsonl"))
    }

    fn temp_path(&self, key: &str) -> PathBuf {
        static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        self.root
            .join(format!(".{key}.tmp-{}-{stamp}-{id}", std::process::id()))
    }

    fn persist_manifest(&self) -> Result<()> {
        let path = self.root.join(MANIFEST_NAME);
        static NEXT_MANIFEST_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_MANIFEST_ID.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp = self.root.join(format!(
            ".{MANIFEST_NAME}.tmp-{}-{stamp}-{id}",
            std::process::id()
        ));
        let bytes = serde_json::to_vec_pretty(&self.manifest)?;
        let result = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
            }
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temp, &path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

impl Iterator for CachedChunks {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.line.clear();
        let result = {
            let reader = self.reader.as_mut()?;
            read_bounded_line(reader, &mut self.line)
        };
        match result {
            Ok(None) => None,
            Ok(Some(terminated)) => {
                let text = match std::str::from_utf8(&self.line) {
                    Ok(text) => text,
                    Err(error) => {
                        self.reader.take();
                        return Some(Err(error.into()));
                    }
                };
                if !terminated {
                    self.reader.take();
                }
                Some(serde_json::from_str(text.trim_end()).map_err(Into::into))
            }
            Err(error) => {
                self.reader.take();
                Some(Err(error.into()))
            }
        }
    }
}

impl CachedChunks {
    pub fn chunk_count(&self) -> u64 {
        self.chunk_count
    }
}

impl Drop for CachedChunks {
    fn drop(&mut self) {
        self.reader.take();
    }
}

fn validate_cache_file(path: &Path) -> std::io::Result<Option<(u64, String)>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut digest = sha2::Sha256::new();
    let mut chunks = 0u64;
    loop {
        match read_bounded_line(&mut reader, &mut line) {
            Ok(None) => {
                let checksum = digest
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                return Ok(Some((chunks, checksum)));
            }
            Ok(Some(terminated)) => {
                digest.update(line.as_slice());
                let Ok(text) = std::str::from_utf8(&line) else {
                    return Ok(None);
                };
                if serde_json::from_str::<Chunk>(text.trim_end()).is_err() {
                    return Ok(None);
                }
                chunks = chunks.saturating_add(1);
                if !terminated {
                    let checksum = digest
                        .finalize()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect();
                    return Ok(Some((chunks, checksum)));
                }
            }
            Err(_) => return Ok(None),
        }
    }
}

/// Read one cache JSONL record while retaining at most the configured line
/// bound. The boolean says whether a newline terminated the record. A final
/// unterminated record is returned once, then the next call returns `None`.
/// `BufRead::read_line` can otherwise allocate the entire corrupt line before
/// its length is checked.
fn read_bounded_line(
    reader: &mut BufReader<File>,
    output: &mut Vec<u8>,
) -> std::io::Result<Option<bool>> {
    output.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if output.is_empty() {
                Ok(None)
            } else {
                Ok(Some(false))
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index.saturating_add(1));
        if output.len().saturating_add(take) > MAX_CACHE_LINE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "content cache chunk exceeds bounded line size",
            ));
        }
        let terminated = available[..take].contains(&b'\n');
        output.extend_from_slice(&available[..take]);
        reader.consume(take);
        if terminated {
            return Ok(Some(true));
        }
    }
}

fn checksum_file(path: &Path) -> Result<String> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut digest = sha2::Sha256::new();
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let count = std::io::Read::read(&mut reader, &mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn cache_key(source_version: &str) -> String {
    let mut digest = sha2::Sha256::new();
    let tokenizer_version = normalization_version();
    digest.update(INGEST_CACHE_VERSION.as_bytes());
    digest.update([0]);
    digest.update(tokenizer_version.as_bytes());
    digest.update([0]);
    digest.update(source_version.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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

    #[test]
    fn round_trips_chunks_and_enforces_disk_budget() {
        let temp = TempDir::new().unwrap();
        let mut cache = ContentCache::open_with_limits(temp.path(), 1024, 1).unwrap();
        let chunk = Chunk {
            text: "cached needle".into(),
            ..Chunk::default()
        };
        cache.put("version-a", [Ok(chunk.clone())]).unwrap();
        let mut hit = cache.get("version-a").unwrap().unwrap();
        assert_eq!(hit.next().unwrap().unwrap().text, chunk.text);
        assert!(hit.next().is_none());
        let key = cache.manifest.entries[0].key.clone();
        let path = cache.path_for(&key);
        let tampered = Chunk {
            text: "tampered but still valid JSON".into(),
            ..Chunk::default()
        };
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&tampered).unwrap()),
        )
        .unwrap();
        assert!(cache.get("version-a").unwrap().is_none());
        cache
            .put(
                "version-b",
                [Ok(Chunk {
                    text: "x".repeat(512),
                    ..Chunk::default()
                })],
            )
            .unwrap();
        assert!(cache.get("version-a").unwrap().is_none());

        let orphan = temp.path().join("orphan.jsonl");
        fs::write(&orphan, b"orphaned cache bytes\n").unwrap();
        drop(cache);
        let _reopened = ContentCache::open_with_limits(temp.path(), 1024, 1).unwrap();
        assert!(!orphan.exists());
    }
}
