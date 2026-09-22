//! Private temporary JSONL records with consuming finalization and replay.

use crate::progress::ProgressReporter;
use anyhow::{Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

const READ_BUFFER_BYTES: usize = 16 * 1024;
pub(crate) const INLINE_BYTES: usize = 64 * 1024;

/// Bounded bytes sharing the same private-file ownership as typed records.
#[derive(Debug)]
pub(crate) struct ByteSpool {
    memory: Vec<u8>,
    writer: Option<BufWriter<SpoolFile>>,
    file: Option<Arc<TemporaryFile>>,
    len: u64,
    progress: ProgressReporter,
}

impl Default for ByteSpool {
    fn default() -> Self {
        Self::observed(&ProgressReporter::noop())
    }
}

#[derive(Clone, Debug)]
enum ByteStorage {
    Memory(Arc<[u8]>),
    File(Arc<TemporaryFile>),
}

#[derive(Clone, Debug)]
pub(crate) struct ByteRange {
    storage: ByteStorage,
    start: u64,
    len: u64,
    progress: ProgressReporter,
}

impl ByteSpool {
    pub(crate) fn observed(progress: &ProgressReporter) -> Self {
        Self {
            memory: Vec::new(),
            writer: None,
            file: None,
            len: 0,
            progress: progress.clone(),
        }
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn range(&mut self, start: u64, end: u64) -> Result<ByteRange> {
        anyhow::ensure!(
            start <= end && end <= self.len,
            "invalid temporary byte range"
        );
        if let Some(writer) = &mut self.writer {
            writer.flush().map_err(FinalizeError)?;
            Ok(ByteRange {
                storage: ByteStorage::File(self.file.as_ref().unwrap().clone()),
                start,
                len: end - start,
                progress: self.progress.clone(),
            })
        } else {
            Ok(ByteRange {
                storage: ByteStorage::Memory(Arc::from(&self.memory[start as usize..end as usize])),
                start: 0,
                len: end - start,
                progress: self.progress.clone(),
            })
        }
    }

    pub(crate) fn finish(mut self) -> Result<ByteRange> {
        if self.writer.is_some() {
            self.range(0, self.len)
        } else {
            Ok(ByteRange {
                storage: ByteStorage::Memory(self.memory.into()),
                start: 0,
                len: self.len,
                progress: self.progress,
            })
        }
    }

    pub(crate) fn reset(&mut self) {
        self.writer.take();
        self.file.take();
        self.memory.clear();
        self.len = 0;
    }
}

impl Write for ByteSpool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.writer.is_none() && self.memory.len().saturating_add(bytes.len()) > INLINE_BYTES {
            let (file, owner) = temporary_file(&self.progress).map_err(io::Error::other)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(&self.memory)?;
            self.memory.clear();
            self.writer = Some(writer);
            self.file = Some(owner);
        }
        if let Some(writer) = &mut self.writer {
            writer.write_all(bytes)?;
        } else {
            if self.memory.capacity() == 0 {
                self.memory.reserve_exact(INLINE_BYTES);
            }
            self.memory.extend_from_slice(bytes);
        }
        self.len += bytes.len() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush()?;
        }
        Ok(())
    }
}

impl ByteRange {
    pub(crate) fn range(&self, start: u64, end: u64) -> Result<Self> {
        anyhow::ensure!(
            start <= end && end <= self.len,
            "invalid captured byte range"
        );
        Ok(Self {
            storage: self.storage.clone(),
            start: self.start + start,
            len: end - start,
            progress: self.progress.clone(),
        })
    }

    pub(crate) fn reader(&self) -> Result<BufReader<Box<dyn Read>>> {
        let reader: Box<dyn Read> = match &self.storage {
            ByteStorage::Memory(bytes) => {
                let mut reader = Cursor::new(bytes.clone());
                reader.set_position(self.start);
                Box::new(reader.take(self.len))
            }
            ByteStorage::File(owner) => {
                let mut file = File::open(&owner.0).map_err(FinalizeError)?;
                file.seek(SeekFrom::Start(self.start))
                    .map_err(FinalizeError)?;
                Box::new(OwnedByteReader {
                    reader: SpoolFile {
                        file,
                        progress: self.progress.clone(),
                    }
                    .take(self.len),
                    _owner: owner.clone(),
                })
            }
        };
        Ok(BufReader::with_capacity(READ_BUFFER_BYTES, reader))
    }
}

struct OwnedByteReader {
    reader: io::Take<SpoolFile>,
    _owner: Arc<TemporaryFile>,
}

impl Read for OwnedByteReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.reader.read(bytes)
    }
}

#[derive(Debug)]
struct SpoolFile {
    file: File,
    progress: ProgressReporter,
}

impl Read for SpoolFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.file.read(bytes)?;
        self.progress.record_spool_io(count as u64, 0);
        Ok(count)
    }
}

impl Write for SpoolFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.file.write(bytes)?;
        self.progress.record_spool_io(0, count as u64);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[derive(Debug)]
struct TemporaryFile(PathBuf);

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn temporary_file(progress: &ProgressReporter) -> Result<(SpoolFile, Arc<TemporaryFile>)> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    for _ in 0..32 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bm25-mcp-records-{}-{timestamp}-{id}.jsonl",
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
                return Ok((
                    SpoolFile {
                        file,
                        progress: progress.clone(),
                    },
                    Arc::new(TemporaryFile(path)),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(anyhow!("could not create a unique record spool"))
}

#[derive(Debug)]
pub(crate) struct RecordSpool {
    // Handles must close before the last path owner removes the file on Windows.
    writer: BufWriter<SpoolFile>,
    file: Arc<TemporaryFile>,
}

/// Finalization failures abort reconciliation rather than reject one source.
#[derive(Debug)]
pub(crate) struct FinalizeError(io::Error);

impl std::fmt::Display for FinalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "finalizing temporary records: {}", self.0)
    }
}

impl std::error::Error for FinalizeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl RecordSpool {
    pub(crate) fn new() -> Result<Self> {
        Self::observed(&ProgressReporter::noop())
    }

    pub(crate) fn observed(progress: &ProgressReporter) -> Result<Self> {
        let (writer, file) = temporary_file(progress)?;
        Ok(Self {
            writer: BufWriter::new(writer),
            file,
        })
    }

    pub(crate) fn push(&mut self, record: &impl Serialize) -> Result<()> {
        serde_json::to_writer(&mut self.writer, record)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub(crate) fn finish<T: DeserializeOwned>(self) -> Result<Records<T>, FinalizeError> {
        let Self { mut writer, file } = self;
        #[cfg(test)]
        let failure = testing::take_failure::<T>(&mut writer, &file.0);
        // Close before propagating a flush error so cleanup also works on Windows.
        let progress = writer.get_ref().progress.clone();
        let flushed = writer.flush();
        drop(writer);
        flushed.map_err(FinalizeError)?;
        #[cfg(test)]
        if failure == Some(testing::Failure::Open) {
            fs::remove_file(&file.0).unwrap();
        }
        Records::open(file, progress).map_err(FinalizeError)
    }
}

#[derive(Debug)]
pub(crate) struct Records<T> {
    reader: Option<BufReader<SpoolFile>>,
    file: Arc<TemporaryFile>,
    line: Vec<u8>,
    progress: ProgressReporter,
    record: PhantomData<fn() -> T>,
}

impl<T> Records<T> {
    fn open(file: Arc<TemporaryFile>, progress: ProgressReporter) -> io::Result<Self> {
        let reader = BufReader::with_capacity(
            READ_BUFFER_BYTES,
            SpoolFile {
                file: File::open(&file.0)?,
                progress: progress.clone(),
            },
        );
        Ok(Self {
            reader: Some(reader),
            file,
            line: Vec::new(),
            progress,
            record: PhantomData,
        })
    }

    pub(crate) fn reopen(&self) -> io::Result<Self> {
        Self::open(self.file.clone(), self.progress.clone())
    }
}

impl<T: DeserializeOwned> Iterator for Records<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        self.line.clear();
        let result = match reader.read_until(b'\n', &mut self.line) {
            Ok(0) => {
                self.reader = None;
                return None;
            }
            Ok(_) => serde_json::from_slice(&self.line).map_err(Into::into),
            Err(error) => Err(error.into()),
        };
        if result.is_err() {
            self.reader = None;
        }
        Some(result)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::{any::type_name, cell::Cell, path::Path};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Failure {
        Flush,
        Open,
    }

    thread_local! {
        static FAILURE: Cell<Option<(&'static str, Failure)>> = const { Cell::new(None) };
    }

    pub(crate) struct FaultGuard;

    pub(crate) fn fail_next_finish<T>(failure: Failure) -> FaultGuard {
        FAILURE.with(|slot| assert!(slot.replace(Some((type_name::<T>(), failure))).is_none()));
        FaultGuard
    }

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            FAILURE.with(|slot| slot.set(None));
        }
    }

    pub(super) fn take_failure<T>(
        writer: &mut BufWriter<SpoolFile>,
        path: &Path,
    ) -> Option<Failure> {
        let failure = FAILURE.with(|slot| match slot.get() {
            Some((record, failure)) if record == type_name::<T>() => {
                slot.set(None);
                Some(failure)
            }
            _ => None,
        });
        if failure == Some(Failure::Flush) {
            // Keep pending bytes but make the real flush target unwritable.
            writer.get_mut().file = File::open(path).unwrap();
        }
        failure
    }
}

#[cfg(test)]
#[path = "record_spool_tests.rs"]
mod tests;
