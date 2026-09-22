//! Private temporary JSONL records with consuming finalization and replay.

use anyhow::{Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

const READ_BUFFER_BYTES: usize = 16 * 1024;

#[derive(Debug)]
struct TemporaryFile(PathBuf);

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[derive(Debug)]
pub(crate) struct RecordSpool {
    // Handles must close before the last path owner removes the file on Windows.
    writer: BufWriter<File>,
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
                    return Ok(Self {
                        writer: BufWriter::new(file),
                        file: Arc::new(TemporaryFile(path)),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(anyhow!("could not create a unique record spool"))
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
        let flushed = writer.flush();
        drop(writer);
        flushed.map_err(FinalizeError)?;
        #[cfg(test)]
        if failure == Some(testing::Failure::Open) {
            fs::remove_file(&file.0).unwrap();
        }
        Records::open(file).map_err(FinalizeError)
    }
}

#[derive(Debug)]
pub(crate) struct Records<T> {
    reader: Option<BufReader<File>>,
    file: Arc<TemporaryFile>,
    line: Vec<u8>,
    record: PhantomData<fn() -> T>,
}

impl<T> Records<T> {
    fn open(file: Arc<TemporaryFile>) -> io::Result<Self> {
        let reader = BufReader::with_capacity(READ_BUFFER_BYTES, File::open(&file.0)?);
        Ok(Self {
            reader: Some(reader),
            file,
            line: Vec::new(),
            record: PhantomData,
        })
    }

    pub(crate) fn reopen(&self) -> io::Result<Self> {
        Self::open(self.file.clone())
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

    pub(super) fn take_failure<T>(writer: &mut BufWriter<File>, path: &Path) -> Option<Failure> {
        let failure = FAILURE.with(|slot| match slot.get() {
            Some((record, failure)) if record == type_name::<T>() => {
                slot.set(None);
                Some(failure)
            }
            _ => None,
        });
        if failure == Some(Failure::Flush) {
            // Keep pending bytes but make the real flush target unwritable.
            *writer.get_mut() = File::open(path).unwrap();
        }
        failure
    }
}

#[cfg(test)]
#[path = "record_spool_tests.rs"]
mod tests;
