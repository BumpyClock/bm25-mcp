//! Bounded JSONL extraction for session adapters.
//!
//! `serde_json::Value` is a convenient shape for small records, but it makes
//! one large JSON string an allocation boundary.  Session histories contain
//! tool output and can contain records much larger than the normal indexing
//! chunk.  This module is a small streaming JSON walker: it decodes selected
//! strings a bounded fragment at a time and spools those fragments to a
//! private file.  Unknown values are skipped lexically, so their size does not
//! affect the process heap.

use crate::progress::ProgressReporter;
use crate::progress::WorkKind;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_FRAGMENT_BYTES: usize = 16 * 1024;
const JSON_READ_BUFFER_BYTES: usize = 64 * 1024;
const JSON_CANCEL_CHECK_BYTES: u64 = 64 * 1024;
const MAX_KEY_BYTES: usize = 4096;
const MAX_DEPTH: usize = 256;
const MAX_PATH_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum PathPart {
    Key(String),
    Index(usize),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Fragment {
    pub path: Vec<PathPart>,
    pub text: String,
}

/// A disk-backed sequence of decoded string fragments from one JSON record.
/// The file is removed when this value is dropped.
pub struct CaptureFile {
    path: PathBuf,
}

impl std::fmt::Debug for CaptureFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CaptureFile")
            .field("path", &self.path)
            .finish()
    }
}

impl CaptureFile {
    #[allow(dead_code)]
    pub fn parse(record_path: &Path) -> Result<Self> {
        Self::parse_with_visible_fields(record_path, true, None, None)
    }

    /// Parse a record while retaining only metadata fields needed for
    /// ownership and routing. Visible message/tool payloads are still
    /// validated lexically, but their strings and structured scalar values
    /// are not spooled during an inspection pass.
    #[allow(dead_code)]
    pub fn parse_metadata(record_path: &Path) -> Result<Self> {
        Self::parse_with_visible_fields(record_path, false, None, None)
    }

    #[allow(dead_code)]
    pub fn parse_controlled(
        record_path: &Path,
        should_continue: &dyn Fn() -> bool,
    ) -> Result<Self> {
        Self::parse_with_visible_fields(record_path, true, Some(should_continue), None)
    }

    pub fn parse_controlled_with_progress(
        record_path: &Path,
        should_continue: &dyn Fn() -> bool,
        progress: &ProgressReporter,
    ) -> Result<Self> {
        Self::parse_with_visible_fields(record_path, true, Some(should_continue), Some(progress))
    }

    #[allow(dead_code)]
    pub fn parse_metadata_controlled(
        record_path: &Path,
        should_continue: &dyn Fn() -> bool,
    ) -> Result<Self> {
        Self::parse_with_visible_fields(record_path, false, Some(should_continue), None)
    }

    pub fn parse_metadata_controlled_with_progress(
        record_path: &Path,
        should_continue: &dyn Fn() -> bool,
        progress: &ProgressReporter,
    ) -> Result<Self> {
        Self::parse_with_visible_fields(record_path, false, Some(should_continue), Some(progress))
    }

    fn parse_with_visible_fields(
        record_path: &Path,
        capture_visible_fields: bool,
        should_continue: Option<&dyn Fn() -> bool>,
        progress: Option<&ProgressReporter>,
    ) -> Result<Self> {
        let spool = CaptureSpool::new()?;
        let reader = BufReader::with_capacity(JSON_READ_BUFFER_BYTES, File::open(record_path)?);
        let mut parser = JsonParser::new(
            reader,
            spool,
            capture_visible_fields,
            should_continue,
            progress,
        );
        parser.parse_root()?;
        let spool = parser.finish()?;
        spool.finish()
    }

    pub fn iter(&self) -> Result<CaptureIter> {
        Ok(CaptureIter {
            reader: BufReader::with_capacity(JSON_READ_BUFFER_BYTES, File::open(&self.path)?),
            done: false,
        })
    }
}

impl Drop for CaptureFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub struct CaptureIter {
    reader: BufReader<File>,
    done: bool,
}

#[derive(Debug)]
pub struct ParseCancelled;

impl std::fmt::Display for ParseCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("session JSON parse cancelled")
    }
}

impl std::error::Error for ParseCancelled {}

impl Iterator for CaptureIter {
    type Item = Result<Fragment>;

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
        let path_len = u32::from_le_bytes(length) as usize;
        if path_len > MAX_PATH_BYTES {
            self.done = true;
            return Some(Err(anyhow!("captured JSON path exceeds bound")));
        }
        let mut path_bytes = vec![0_u8; path_len];
        if let Err(error) = self.reader.read_exact(&mut path_bytes) {
            self.done = true;
            return Some(Err(error.into()));
        }
        let path = match serde_json::from_slice::<Vec<PathPart>>(&path_bytes) {
            Ok(path) => path,
            Err(error) => {
                self.done = true;
                return Some(Err(error.into()));
            }
        };
        if let Err(error) = self.reader.read_exact(&mut length) {
            self.done = true;
            return Some(Err(error.into()));
        }
        let text_len = u32::from_le_bytes(length) as usize;
        if text_len > MAX_FRAGMENT_BYTES {
            self.done = true;
            return Some(Err(anyhow!("captured JSON fragment exceeds bound")));
        }
        let mut text_bytes = vec![0_u8; text_len];
        if let Err(error) = self.reader.read_exact(&mut text_bytes) {
            self.done = true;
            return Some(Err(error.into()));
        }
        Some(
            String::from_utf8(text_bytes)
                .map(|text| Fragment { path, text })
                .map_err(|error| error.into()),
        )
    }
}

struct CaptureSpool {
    path: Option<PathBuf>,
    file: Option<BufWriter<File>>,
}

impl CaptureSpool {
    fn new() -> Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let temp_dir = std::env::temp_dir();
        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = temp_dir.join(format!(
                "bm25-mcp-json-capture-{}-{stamp}-{id}.bin",
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
                        file: Some(BufWriter::new(file)),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(anyhow!("could not create JSON capture spool"))
    }

    fn push(&mut self, path: &[PathPart], text: &str) -> Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        if text.len() > MAX_FRAGMENT_BYTES {
            bail!("JSON capture fragment exceeds bound")
        }
        let path_bytes = serde_json::to_vec(path)?;
        if path_bytes.len() > MAX_PATH_BYTES {
            bail!("JSON capture path exceeds bound")
        }
        let path_len = u32::try_from(path_bytes.len()).context("JSON path length overflow")?;
        let text_len = u32::try_from(text.len()).context("JSON fragment length overflow")?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| anyhow!("JSON capture spool already finished"))?;
        file.write_all(&path_len.to_le_bytes())?;
        file.write_all(&path_bytes)?;
        file.write_all(&text_len.to_le_bytes())?;
        file.write_all(text.as_bytes())?;
        Ok(())
    }

    fn finish(mut self) -> Result<CaptureFile> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| anyhow!("JSON capture spool already finished"))?;
        file.flush()?;
        Ok(CaptureFile {
            path: self
                .path
                .take()
                .ok_or_else(|| anyhow!("JSON capture spool path missing"))?,
        })
    }
}

impl Drop for CaptureSpool {
    fn drop(&mut self) {
        self.file.take();
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

struct JsonParser<'a, R> {
    reader: R,
    offset: u64,
    spool: CaptureSpool,
    pending: Option<u8>,
    capture_visible_fields: bool,
    should_continue: Option<&'a dyn Fn() -> bool>,
    progress: Option<&'a ProgressReporter>,
    bytes_since_check: u64,
}

impl<'a, R: Read> JsonParser<'a, R> {
    fn new(
        reader: R,
        spool: CaptureSpool,
        capture_visible_fields: bool,
        should_continue: Option<&'a dyn Fn() -> bool>,
        progress: Option<&'a ProgressReporter>,
    ) -> Self {
        Self {
            reader,
            offset: 0,
            spool,
            pending: None,
            capture_visible_fields,
            should_continue,
            progress,
            bytes_since_check: 0,
        }
    }

    fn parse_root(&mut self) -> Result<()> {
        self.skip_ws()?;
        let mut path = Vec::new();
        self.parse_value(&mut path, 0)?;
        self.skip_ws()?;
        if self.read_byte()?.is_some() {
            bail!("trailing bytes after JSON value")
        }
        Ok(())
    }

    fn finish(self) -> Result<CaptureSpool> {
        if self.bytes_since_check != 0
            && let Some(progress) = self.progress
        {
            progress.record_work_bytes(WorkKind::JsonInspection, self.bytes_since_check);
        }
        Ok(self.spool)
    }

    fn parse_value(&mut self, path: &mut Vec<PathPart>, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            bail!("JSON nesting exceeds bound")
        }
        self.skip_ws()?;
        let Some(first) = self.read_byte()? else {
            bail!("unexpected end of JSON value")
        };
        match first {
            b'"' => self.parse_string(path, self.capture_path(path)),
            b'{' => self.parse_object(path, depth + 1),
            b'[' => self.parse_array(path, depth + 1),
            b't' | b'f' | b'n' => self.parse_literal(first, path),
            b'-' | b'0'..=b'9' => self.parse_number_value(first, path),
            other => bail!("unexpected JSON byte 0x{other:02x}"),
        }
    }

    fn parse_object(&mut self, path: &mut Vec<PathPart>, depth: usize) -> Result<()> {
        self.skip_ws()?;
        if self.consume_if(b'}')? {
            return Ok(());
        }
        loop {
            self.expect_byte(b'"')?;
            let key = self.parse_key()?;
            self.skip_ws()?;
            self.expect_byte(b':')?;
            if let Some(key) = key {
                // Object keys are part of a structured tool argument/result.
                // Keep them at the containing path so the normalizer can
                // index names alongside their scalar values without building
                // an in-memory JSON object.
                if self.capture_visible_fields && structured_path(path) {
                    self.spool.push(path, &key)?;
                }
                path.push(PathPart::Key(key));
                self.parse_value(path, depth)?;
                path.pop();
            } else {
                // An overlarge key is structurally skipped. It cannot be a
                // provider field and does not need to consume heap
                // proportional to the key.
                self.skip_value(path, depth)?;
            }
            self.skip_ws()?;
            if self.consume_if(b'}')? {
                return Ok(());
            }
            self.expect_byte(b',')?;
            self.skip_ws()?;
        }
    }

    fn parse_array(&mut self, path: &mut Vec<PathPart>, depth: usize) -> Result<()> {
        self.skip_ws()?;
        if self.consume_if(b']')? {
            return Ok(());
        }
        let mut index = 0_usize;
        loop {
            path.push(PathPart::Index(index));
            self.parse_value(path, depth)?;
            path.pop();
            index = index.saturating_add(1);
            self.skip_ws()?;
            if self.consume_if(b']')? {
                return Ok(());
            }
            self.expect_byte(b',')?;
            self.skip_ws()?;
        }
    }

    fn skip_value(&mut self, path: &[PathPart], depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            bail!("JSON nesting exceeds bound")
        }
        self.skip_ws()?;
        let Some(first) = self.read_byte()? else {
            bail!("unexpected end of JSON value")
        };
        match first {
            b'"' => self.parse_string_discard(),
            b'{' => self.skip_object(path, depth + 1),
            b'[' => self.skip_array(path, depth + 1),
            b't' => self.expect_literal(b"rue"),
            b'f' => self.expect_literal(b"alse"),
            b'n' => self.expect_literal(b"ull"),
            b'-' | b'0'..=b'9' => self.skip_number(first),
            other => bail!("unexpected JSON byte 0x{other:02x}"),
        }
    }

    fn skip_object(&mut self, path: &[PathPart], depth: usize) -> Result<()> {
        self.skip_ws()?;
        if self.consume_if(b'}')? {
            return Ok(());
        }
        loop {
            self.expect_byte(b'"')?;
            self.parse_key_discard()?;
            self.skip_ws()?;
            self.expect_byte(b':')?;
            self.skip_value(path, depth)?;
            self.skip_ws()?;
            if self.consume_if(b'}')? {
                return Ok(());
            }
            self.expect_byte(b',')?;
        }
    }

    fn skip_array(&mut self, path: &[PathPart], depth: usize) -> Result<()> {
        self.skip_ws()?;
        if self.consume_if(b']')? {
            return Ok(());
        }
        loop {
            self.skip_value(path, depth)?;
            self.skip_ws()?;
            if self.consume_if(b']')? {
                return Ok(());
            }
            self.expect_byte(b',')?;
        }
    }

    fn parse_key(&mut self) -> Result<Option<String>> {
        let mut value = String::new();
        let mut overflow = false;
        self.decode_string(|fragment| {
            if !overflow {
                if value.len().saturating_add(fragment.len()) <= MAX_KEY_BYTES {
                    value.push_str(fragment);
                } else {
                    overflow = true;
                    value.clear();
                }
            }
            Ok(())
        })?;
        if overflow { Ok(None) } else { Ok(Some(value)) }
    }

    fn parse_key_discard(&mut self) -> Result<()> {
        self.parse_string_discard()
    }

    fn parse_string(&mut self, path: &[PathPart], capture: bool) -> Result<()> {
        if capture && should_capture(path) {
            self.parse_string_capture(path)
        } else {
            self.parse_string_discard()
        }
    }

    fn parse_string_capture(&mut self, path: &[PathPart]) -> Result<()> {
        let mut fragment = String::new();
        loop {
            let Some(byte) = self.read_byte()? else {
                bail!("unterminated JSON string")
            };
            match byte {
                b'"' => {
                    if !fragment.is_empty() {
                        self.spool.push(path, &fragment)?;
                    }
                    return Ok(());
                }
                b'\\' => {
                    let Some(escape) = self.read_byte()? else {
                        bail!("unterminated JSON escape")
                    };
                    let character = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{0008}',
                        b'f' => '\u{000c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.decode_unicode_escape()?,
                        _ => bail!("invalid JSON escape"),
                    };
                    if fragment.len().saturating_add(character.len_utf8()) > MAX_FRAGMENT_BYTES {
                        self.spool.push(path, &fragment)?;
                        fragment.clear();
                    }
                    fragment.push(character);
                }
                byte if byte < 0x20 => bail!("control byte in JSON string"),
                byte if byte < 0x80 => {
                    if fragment.len() == MAX_FRAGMENT_BYTES {
                        self.spool.push(path, &fragment)?;
                        fragment.clear();
                    }
                    fragment.push(byte as char);
                }
                byte => {
                    let width = utf8_width(byte)?;
                    let mut bytes = [0_u8; 4];
                    bytes[0] = byte;
                    for slot in bytes.iter_mut().take(width).skip(1) {
                        let Some(next) = self.read_byte()? else {
                            bail!("truncated UTF-8 in JSON string")
                        };
                        *slot = next;
                    }
                    let value = std::str::from_utf8(&bytes[..width])
                        .map_err(|_| anyhow!("invalid UTF-8 in JSON string"))?;
                    if fragment.len().saturating_add(value.len()) > MAX_FRAGMENT_BYTES {
                        self.spool.push(path, &fragment)?;
                        fragment.clear();
                    }
                    fragment.push_str(value);
                }
            }
            if fragment.len() == MAX_FRAGMENT_BYTES {
                self.spool.push(path, &fragment)?;
                fragment.clear();
            }
        }
    }

    fn parse_string_discard(&mut self) -> Result<()> {
        loop {
            let Some(byte) = self.read_byte()? else {
                bail!("unterminated JSON string")
            };
            match byte {
                b'"' => return Ok(()),
                b'\\' => {
                    let Some(escape) = self.read_byte()? else {
                        bail!("unterminated JSON escape")
                    };
                    if escape == b'u' {
                        for _ in 0..4 {
                            let Some(hex) = self.read_byte()? else {
                                bail!("unterminated JSON unicode escape")
                            };
                            if !hex.is_ascii_hexdigit() {
                                bail!("invalid JSON unicode escape")
                            }
                        }
                    }
                }
                byte if byte < 0x20 => bail!("control byte in JSON string"),
                _ => {}
            }
        }
    }

    fn decode_string<F>(&mut self, mut sink: F) -> Result<()>
    where
        F: FnMut(&str) -> Result<()>,
    {
        let mut fragment = String::new();
        loop {
            let Some(byte) = self.read_byte()? else {
                bail!("unterminated JSON string")
            };
            match byte {
                b'"' => {
                    if !fragment.is_empty() {
                        sink(&fragment)?;
                    }
                    return Ok(());
                }
                b'\\' => {
                    let Some(escape) = self.read_byte()? else {
                        bail!("unterminated JSON escape")
                    };
                    let character = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{0008}',
                        b'f' => '\u{000c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.decode_unicode_escape()?,
                        _ => bail!("invalid JSON escape"),
                    };
                    append_char(&mut fragment, character, &mut sink)?;
                }
                byte if byte < 0x20 => bail!("control byte in JSON string"),
                byte if byte < 0x80 => append_char(&mut fragment, byte as char, &mut sink)?,
                byte => {
                    let width = utf8_width(byte)?;
                    let mut bytes = [0_u8; 4];
                    bytes[0] = byte;
                    for slot in bytes.iter_mut().take(width).skip(1) {
                        let Some(next) = self.read_byte()? else {
                            bail!("truncated UTF-8 in JSON string")
                        };
                        *slot = next;
                    }
                    let value = std::str::from_utf8(&bytes[..width])
                        .map_err(|_| anyhow!("invalid UTF-8 in JSON string"))?;
                    if fragment.len().saturating_add(value.len()) > MAX_FRAGMENT_BYTES {
                        sink(&fragment)?;
                        fragment.clear();
                    }
                    fragment.push_str(value);
                }
            }
            if fragment.len() >= MAX_FRAGMENT_BYTES {
                sink(&fragment)?;
                fragment.clear();
            }
        }
    }

    fn decode_unicode_escape(&mut self) -> Result<char> {
        let high = self.read_hex_quad()?;
        if (0xd800..=0xdbff).contains(&high) {
            self.expect_byte(b'\\')?;
            self.expect_byte(b'u')?;
            let low = self.read_hex_quad()?;
            if !(0xdc00..=0xdfff).contains(&low) {
                bail!("invalid JSON surrogate pair")
            }
            let scalar = 0x1_0000 + ((high - 0xd800) << 10) + (low - 0xdc00);
            return char::from_u32(scalar).ok_or_else(|| anyhow!("invalid JSON unicode scalar"));
        }
        if (0xdc00..=0xdfff).contains(&high) {
            bail!("unpaired JSON low surrogate")
        }
        char::from_u32(high).ok_or_else(|| anyhow!("invalid JSON unicode scalar"))
    }

    fn read_hex_quad(&mut self) -> Result<u32> {
        let mut value = 0_u32;
        for _ in 0..4 {
            let Some(byte) = self.read_byte()? else {
                bail!("unterminated JSON unicode escape")
            };
            let digit = (byte as char)
                .to_digit(16)
                .ok_or_else(|| anyhow!("invalid JSON unicode escape"))?;
            value = (value << 4) | digit;
        }
        Ok(value)
    }

    fn skip_number(&mut self, first: u8) -> Result<()> {
        self.parse_number(first, None)
    }

    fn capture_number(&mut self, first: u8, path: &[PathPart]) -> Result<()> {
        self.parse_number(first, Some(path))
    }

    fn parse_number_value(&mut self, first: u8, path: &[PathPart]) -> Result<()> {
        if self.capture_path(path) {
            self.capture_number(first, path)
        } else {
            self.skip_number(first)
        }
    }

    fn parse_number(&mut self, first: u8, path: Option<&[PathPart]>) -> Result<()> {
        // JSON numbers have a deliberately narrow grammar.  Accepting a
        // loose set of number characters here would let malformed values
        // such as `--1`, `01`, `1e`, or `1+2` desynchronize the enclosing
        // object and potentially hide a later valid event.
        let mut fragment = String::new();
        let mut byte = first;
        if byte == b'-' {
            byte = self
                .read_byte()?
                .ok_or_else(|| anyhow!("truncated JSON number"))?;
            if !byte.is_ascii_digit() {
                bail!("invalid JSON number")
            }
            self.capture_number_byte(path, &mut fragment, b'-')?;
            self.capture_number_byte(path, &mut fragment, byte)?;
        } else {
            self.capture_number_byte(path, &mut fragment, byte)?;
        }

        if byte == b'0' {
            byte = match self.read_byte()? {
                Some(byte) => byte,
                None => {
                    self.finish_number_capture(path, &mut fragment)?;
                    return Ok(());
                }
            };
            if byte.is_ascii_digit() {
                bail!("invalid JSON number with leading zero")
            }
        } else if (b'1'..=b'9').contains(&byte) {
            loop {
                byte = match self.read_byte()? {
                    Some(byte) => byte,
                    None => {
                        self.finish_number_capture(path, &mut fragment)?;
                        return Ok(());
                    }
                };
                if !byte.is_ascii_digit() {
                    break;
                }
                self.capture_number_byte(path, &mut fragment, byte)?;
            }
        } else {
            bail!("invalid JSON number")
        }

        if byte == b'.' {
            self.capture_number_byte(path, &mut fragment, byte)?;
            byte = self
                .read_byte()?
                .ok_or_else(|| anyhow!("truncated JSON fraction"))?;
            if !byte.is_ascii_digit() {
                bail!("invalid JSON fraction")
            }
            self.capture_number_byte(path, &mut fragment, byte)?;
            loop {
                byte = match self.read_byte()? {
                    Some(byte) => byte,
                    None => {
                        self.finish_number_capture(path, &mut fragment)?;
                        return Ok(());
                    }
                };
                if !byte.is_ascii_digit() {
                    break;
                }
                self.capture_number_byte(path, &mut fragment, byte)?;
            }
        }

        if matches!(byte, b'e' | b'E') {
            self.capture_number_byte(path, &mut fragment, byte)?;
            byte = self
                .read_byte()?
                .ok_or_else(|| anyhow!("truncated JSON exponent"))?;
            if matches!(byte, b'+' | b'-') {
                self.capture_number_byte(path, &mut fragment, byte)?;
                byte = self
                    .read_byte()?
                    .ok_or_else(|| anyhow!("truncated JSON exponent"))?;
            }
            if !byte.is_ascii_digit() {
                bail!("invalid JSON exponent")
            }
            self.capture_number_byte(path, &mut fragment, byte)?;
            loop {
                byte = match self.read_byte()? {
                    Some(byte) => byte,
                    None => {
                        self.finish_number_capture(path, &mut fragment)?;
                        return Ok(());
                    }
                };
                if !byte.is_ascii_digit() {
                    break;
                }
                self.capture_number_byte(path, &mut fragment, byte)?;
            }
        }

        if matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}') {
            self.unread_byte(byte);
            self.finish_number_capture(path, &mut fragment)?;
            return Ok(());
        }
        bail!("invalid JSON number terminator")
    }

    fn capture_number_byte(
        &mut self,
        path: Option<&[PathPart]>,
        fragment: &mut String,
        byte: u8,
    ) -> Result<()> {
        let Some(path) = path else {
            return Ok(());
        };
        if fragment.len() == MAX_FRAGMENT_BYTES {
            self.spool.push(path, fragment)?;
            fragment.clear();
        }
        fragment.push(byte as char);
        Ok(())
    }

    fn finish_number_capture(
        &mut self,
        path: Option<&[PathPart]>,
        fragment: &mut String,
    ) -> Result<()> {
        if let Some(path) = path
            && !fragment.is_empty()
        {
            self.spool.push(path, fragment)?;
            fragment.clear();
        }
        Ok(())
    }

    fn expect_literal(&mut self, rest: &[u8]) -> Result<()> {
        for expected in rest {
            let Some(actual) = self.read_byte()? else {
                bail!("truncated JSON literal")
            };
            if actual != *expected {
                bail!("invalid JSON literal")
            }
        }
        Ok(())
    }

    fn expect_literal_capture(
        &mut self,
        rest: &[u8],
        path: &[PathPart],
        literal: &str,
    ) -> Result<()> {
        self.expect_literal(rest)?;
        if should_capture(path) {
            self.spool.push(path, literal)?;
        }
        Ok(())
    }

    fn parse_literal(&mut self, first: u8, path: &[PathPart]) -> Result<()> {
        let (rest, literal) = match first {
            b't' => (&b"rue"[..], "true"),
            b'f' => (&b"alse"[..], "false"),
            b'n' => (&b"ull"[..], "null"),
            _ => bail!("unexpected JSON literal byte 0x{first:02x}"),
        };
        if self.capture_path(path) {
            self.expect_literal_capture(rest, path, literal)
        } else {
            self.expect_literal(rest)
        }
    }

    fn capture_path(&self, path: &[PathPart]) -> bool {
        should_capture(path)
            && (self.capture_visible_fields
                || !path
                    .iter()
                    .any(|part| matches!(part, PathPart::Key(key) if is_visible_field_key(key))))
    }

    fn skip_ws(&mut self) -> Result<()> {
        loop {
            let Some(byte) = self.read_byte()? else {
                return Ok(());
            };
            if !byte.is_ascii_whitespace() {
                self.unread_byte(byte);
                return Ok(());
            }
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<()> {
        let actual = self
            .read_byte()?
            .ok_or_else(|| anyhow!("unexpected end of JSON"))?;
        if actual != expected {
            bail!("expected JSON byte 0x{expected:02x}, found 0x{actual:02x}")
        }
        Ok(())
    }

    fn consume_if(&mut self, expected: u8) -> Result<bool> {
        let Some(actual) = self.read_byte()? else {
            return Ok(false);
        };
        if actual == expected {
            Ok(true)
        } else {
            self.unread_byte(actual);
            Ok(false)
        }
    }

    fn unread_byte(&mut self, byte: u8) {
        debug_assert!(self.pending.is_none());
        self.pending = Some(byte);
    }

    fn read_byte(&mut self) -> Result<Option<u8>> {
        if let Some(byte) = self.pending.take() {
            return Ok(Some(byte));
        }
        let mut byte = [0_u8; 1];
        let count = self.reader.read(&mut byte)?;
        if count == 0 {
            Ok(None)
        } else {
            self.offset = self.offset.saturating_add(1);
            self.bytes_since_check = self.bytes_since_check.saturating_add(count as u64);
            if self.bytes_since_check >= JSON_CANCEL_CHECK_BYTES {
                if let Some(progress) = self.progress {
                    progress.record_work_bytes(WorkKind::JsonInspection, self.bytes_since_check);
                }
                self.bytes_since_check = 0;
                if self.should_continue.is_some_and(|check| !check()) {
                    return Err(anyhow!(ParseCancelled));
                }
            }
            Ok(Some(byte[0]))
        }
    }
}

fn append_char<F>(fragment: &mut String, character: char, sink: &mut F) -> Result<()>
where
    F: FnMut(&str) -> Result<()>,
{
    let width = character.len_utf8();
    if fragment.len().saturating_add(width) > MAX_FRAGMENT_BYTES {
        sink(fragment)?;
        fragment.clear();
    }
    fragment.push(character);
    Ok(())
}

fn utf8_width(first: u8) -> Result<usize> {
    let width = match first {
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return Err(anyhow!("invalid UTF-8 leading byte")),
    };
    Ok(width)
}

fn should_capture(path: &[PathPart]) -> bool {
    if structured_path(path) {
        return true;
    }
    let Some(PathPart::Key(key)) = path.last() else {
        return false;
    };
    matches!(
        key.as_str(),
        "type"
            | "role"
            | "channel"
            | "name"
            | "id"
            | "uuid"
            | "call_id"
            | "callId"
            | "tool_use_id"
            | "toolUseId"
            | "toolCallId"
            | "session_id"
            | "sessionId"
            | "conversation_id"
            | "conversationId"
            | "cwd"
            | "timestamp"
            | "created_at"
            | "createdAt"
            | "time"
            | "content"
            | "text"
            | "output"
            | "arguments"
            | "input"
            | "result"
            | "detailedContent"
            | "toolName"
            | "mcpToolName"
            | "message"
    )
}

fn structured_path(path: &[PathPart]) -> bool {
    path.iter().any(|part| {
        matches!(
            part,
            PathPart::Key(key)
                if matches!(
                    key.as_str(),
                    "arguments" | "input" | "output" | "result" | "detailedContent"
                )
        )
    })
}

fn is_visible_field_key(key: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn huge_selected_string_is_fragmented_on_disk() -> Result<()> {
        let mut record = NamedTempFile::new()?;
        let text = "x".repeat(MAX_FRAGMENT_BYTES * 3 + 17);
        writeln!(
            record,
            "{{\"type\":\"message\",\"content\":{}}}",
            serde_json::to_string(&text)?
        )?;
        let capture = CaptureFile::parse(record.path())?;
        let fragments = capture.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            fragments
                .iter()
                .filter(|item| item.path == vec![PathPart::Key("content".into())])
                .map(|item| item.text.len())
                .sum::<usize>(),
            text.len()
        );
        assert!(
            fragments
                .iter()
                .all(|item| item.text.len() <= MAX_FRAGMENT_BYTES)
        );
        Ok(())
    }

    #[test]
    fn escaped_unicode_and_surrogates_decode() -> Result<()> {
        let mut record = NamedTempFile::new()?;
        record.write_all(br#"{"type":"message","text":"a\n\uD83D\uDE00"}"#)?;
        let capture = CaptureFile::parse(record.path())?;
        let text = capture
            .iter()?
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .find(|fragment| fragment.path == vec![PathPart::Key("text".into())])
            .map(|fragment| fragment.text)
            .unwrap();
        assert_eq!(text, "a\n😀");
        Ok(())
    }

    #[test]
    fn numbers_follow_json_grammar() -> Result<()> {
        for source in [
            "0", "-0", "12", "-12", "1.5", "-0.25", "1e3", "-1E+3", "2.5e-4",
        ] {
            let mut record = NamedTempFile::new()?;
            write!(record, "{{\"n\":{source}}}")?;
            CaptureFile::parse(record.path())?;
        }
        for source in ["--1", "01", "1.", "1e", "1e+", "1+2", "-", ".1"] {
            let mut record = NamedTempFile::new()?;
            write!(record, "{{\"n\":{source}}}")?;
            assert!(
                CaptureFile::parse(record.path()).is_err(),
                "accepted {source}"
            );
        }
        Ok(())
    }

    #[test]
    fn structured_keys_and_scalars_are_captured() -> Result<()> {
        let mut record = NamedTempFile::new()?;
        record.write_all(
            br#"{"arguments":{"port":8080,"enabled":false,"missing":null,"nested":{"label":"marker"}}}"#,
        )?;
        let capture = CaptureFile::parse(record.path())?;
        let fragments = capture.iter()?.collect::<Result<Vec<_>>>()?;
        let path = |parts: &[&str]| {
            parts
                .iter()
                .map(|part| PathPart::Key((*part).to_owned()))
                .collect::<Vec<_>>()
        };
        assert!(
            fragments
                .iter()
                .any(|fragment| fragment.path == path(&["arguments"]) && fragment.text == "port")
        );
        assert!(fragments.iter().any(|fragment| {
            fragment.path == path(&["arguments", "port"]) && fragment.text == "8080"
        }));
        assert!(fragments.iter().any(|fragment| {
            fragment.path == path(&["arguments", "enabled"]) && fragment.text == "false"
        }));
        assert!(fragments.iter().any(|fragment| {
            fragment.path == path(&["arguments", "missing"]) && fragment.text == "null"
        }));
        assert!(fragments.iter().any(|fragment| {
            fragment.path == path(&["arguments"]) && fragment.text == "nested"
        }));
        assert!(fragments.iter().any(|fragment| {
            fragment.path == path(&["arguments", "nested", "label"]) && fragment.text == "marker"
        }));
        Ok(())
    }
}
