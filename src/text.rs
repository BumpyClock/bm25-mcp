//! Versioned lexical normalization shared by index writers and readers.
//!
//! The tokenizer is deliberately incremental. A caller may feed one decoded
//! character at a time and terms are emitted only after their lexical
//! occurrence is complete. This matters for project ingestion: chunks are
//! limited to 16 KiB for storage, but an identifier is allowed to cross any
//! number of chunk boundaries. Long occurrences are represented by a
//! domain-separated digest while they are being consumed, so their spelling
//! is never retained in memory.

use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use unicode_casefold::UnicodeCaseFold;

/// Bump this when changing emitted terms. Persisting it with the schema makes
/// a tokenizer change an explicit migration instead of silently mixing terms.
pub const TOKENIZER_VERSION: &str = "bm25-mcp-tokenizer-v4";

/// Version of the complete lexical normalizer, including the Unicode tables
/// used by Rust's character classification and the pinned full case-folding
/// table from `unicode-casefold`.
pub fn normalization_version() -> String {
    let (major, minor, patch) = std::char::UNICODE_VERSION;
    let (fold_major, fold_minor, fold_patch) = unicode_casefold::UNICODE_VERSION;
    format!(
        "{TOKENIZER_VERSION}-unicode-{major}.{minor}.{patch}-casefold-{fold_major}.{fold_minor}.{fold_patch}"
    )
}

/// Normalized terms at or below this UTF-8 size retain their spelling. Longer
/// normalized forms are represented by [`LONG_TERM_PREFIX`] plus a SHA-256
/// digest of the complete normalized spelling.
pub const MAX_TERM_BYTES: usize = 256;

const LONG_TERM_DOMAIN: &[u8] = b"bm25-mcp-long-term-v2\0";
/// This prefix contains a NUL, which the lexical scanner never emits.
pub const LONG_TERM_PREFIX: &str = "\0bm25_long_v2:";

// A pathological identifier can contain millions of distinct underscore or
// camel-case components. Keep a bounded hot set and spill exact form bytes to
// a short-lived disk set once it fills. This keeps memory bounded without
// relaxing per-occurrence de-duplication.
const MAX_DEDUP_FORMS: usize = 4096;

/// Normalize all lexical terms in `text`.
///
/// This is the query-facing convenience wrapper. Project and session
/// ingestion use [`StreamingTokenizer`] directly so terms can be attached to
/// the bounded storage chunk that contains the end of an occurrence.
pub fn tokenize(text: &str) -> Vec<String> {
    tokenize_checked(text).expect("tokenizer temporary spill failed")
}

/// Fallible form of [`tokenize`]. Ingestion uses this form so an unavailable
/// exact-dedup spill backend rejects the source instead of silently dropping
/// or duplicating terms.
pub fn tokenize_checked(text: &str) -> std::io::Result<Vec<String>> {
    let mut tokenizer = StreamingTokenizer::new();
    let mut terms = Vec::new();
    let mut occurrence = Vec::new();
    for character in text.chars() {
        tokenizer.try_push(character, &mut |term| occurrence.push(term))?;
        if !character.is_alphanumeric() && character != '_' && !is_surface_separator(character) {
            append_query_occurrence(&mut terms, &mut occurrence);
        }
    }
    tokenizer.try_finish(&mut |term| occurrence.push(term))?;
    append_query_occurrence(&mut terms, &mut occurrence);
    Ok(terms)
}

fn append_query_occurrence(output: &mut Vec<String>, occurrence: &mut Vec<String>) {
    if let Some(whole) = occurrence.pop() {
        // Streaming ingestion emits component forms as soon as their boundary
        // is known. The historical query API emits the whole identifier first;
        // retain that order while sharing the exact same normalizer.
        output.push(whole);
        output.append(occurrence);
    }
}

/// Full case folding shared by structural identities and lexical normalization.
pub fn fold(text: &str) -> String {
    text.chars().flat_map(|c| c.case_fold()).collect()
}

/// Complete tokenizer output used by ranking fields. Compound forms are also
/// persisted in postings, so index and query paths share exact parity.
pub fn surface_tokens(text: &str) -> std::io::Result<Vec<String>> {
    tokenize_checked(text)
}

/// An incremental document/query tokenizer.
///
/// `push` accepts decoded Unicode scalar values, not raw bytes. Callers must
/// invoke [`StreamingTokenizer::finish`] at the end of the logical text.
#[derive(Debug, Default)]
pub struct StreamingTokenizer {
    run: Option<RunTokenizer>,
    surface: Option<SurfaceTokenizer>,
}

impl StreamingTokenizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one decoded character and emit every term whose occurrence ends
    /// at this character. Terms are emitted in the same order as
    /// [`tokenize`].
    pub fn push(&mut self, character: char, emit: &mut impl FnMut(String)) {
        self.try_push(character, emit)
            .expect("tokenizer temporary spill failed");
    }

    /// Fallible counterpart to [`StreamingTokenizer::push`].
    pub fn try_push(
        &mut self,
        character: char,
        emit: &mut impl FnMut(String),
    ) -> std::io::Result<()> {
        if character.is_alphanumeric() || character == '_' {
            self.run
                .get_or_insert_with(RunTokenizer::default)
                .push(character, emit)?;
            self.surface
                .get_or_insert_with(SurfaceTokenizer::default)
                .push(character);
        } else if is_surface_separator(character) {
            self.try_finish_run(emit)?;
            self.surface
                .get_or_insert_with(SurfaceTokenizer::default)
                .push_separator(character);
        } else {
            self.try_finish(emit)?;
        }
        Ok(())
    }

    /// Feed a string without allocating a whole-token intermediate.
    pub fn push_str(&mut self, text: &str, emit: &mut impl FnMut(String)) {
        self.try_push_str(text, emit)
            .expect("tokenizer temporary spill failed");
    }

    /// Fallible counterpart to [`StreamingTokenizer::push_str`].
    pub fn try_push_str(
        &mut self,
        text: &str,
        emit: &mut impl FnMut(String),
    ) -> std::io::Result<()> {
        for character in text.chars() {
            self.try_push(character, emit)?;
        }
        Ok(())
    }

    /// Finish the current lexical occurrence, if any.
    pub fn finish(&mut self, emit: &mut impl FnMut(String)) {
        self.try_finish(emit)
            .expect("tokenizer temporary spill failed");
    }

    /// Fallible counterpart to [`StreamingTokenizer::finish`].
    pub fn try_finish(&mut self, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        self.try_finish_run(emit)?;
        if let Some(mut surface) = self.surface.take() {
            surface.finish(emit)?;
        }
        Ok(())
    }

    fn try_finish_run(&mut self, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        if let Some(mut run) = self.run.take() {
            run.finish(emit)?;
        }
        Ok(())
    }
}

fn is_surface_separator(character: char) -> bool {
    matches!(character, '-' | '.' | ':' | '/' | '\\')
}

#[derive(Debug, Default)]
struct SurfaceTokenizer {
    form: FormAccumulator,
    trimmed: FormAccumulator,
    saw_alphanumeric: bool,
    saw_separator: bool,
    trimmed_has_separator: bool,
    pending_separator: Option<char>,
    dedup: FormDeduper,
}

impl SurfaceTokenizer {
    fn push(&mut self, character: char) {
        self.form.push_char(character);
        if let Some(separator) = self.pending_separator.take() {
            self.trimmed.push_char(separator);
            self.trimmed_has_separator = true;
        }
        self.trimmed.push_char(character);
        self.saw_alphanumeric |= character.is_alphanumeric();
    }

    fn push_separator(&mut self, character: char) {
        self.form.push_char(character);
        if let Some(separator) = self.pending_separator.replace(character) {
            self.trimmed.push_char(separator);
            self.trimmed_has_separator = true;
        }
        self.saw_separator = true;
    }

    fn finish(&mut self, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        if self.saw_alphanumeric && self.saw_separator {
            if self.pending_separator.is_some() && self.trimmed_has_separator {
                let trimmed = std::mem::take(&mut self.trimmed).finish();
                if self.dedup.insert(&trimmed)? {
                    emit(trimmed);
                }
            } else {
                let form = std::mem::take(&mut self.form).finish();
                if self.dedup.insert(&form)? {
                    emit(form);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RunTokenizer {
    whole: FormAccumulator,
    component: FormAccumulator,
    component_has_alphanumeric: bool,
    /// The final character stays pending until the next character so an
    /// acronym boundary (`HTTPResponse`) can be recognized without buffering
    /// the complete identifier.
    pending: Option<char>,
    committed_last_upper: bool,
    saw_alphanumeric: bool,
    dedup: FormDeduper,
}

impl RunTokenizer {
    fn push(&mut self, character: char, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        if character == '_' {
            self.whole.push_char(character);
            self.commit_pending();
            self.finish_component(emit)?;
            return Ok(());
        }

        self.whole.push_char(character);
        self.saw_alphanumeric = true;
        let Some(previous) = self.pending.replace(character) else {
            return Ok(());
        };

        if previous.is_lowercase() && character.is_uppercase() {
            // lowerCase -> UpperCase: the new character starts a component.
            self.commit_char(previous);
            self.finish_component(emit)?;
        } else if previous.is_uppercase() && character.is_uppercase() {
            // Keep one uppercase character pending. If the next character is
            // lowercase, this gives us the acronym-to-word boundary.
            self.commit_char(previous);
        } else if previous.is_uppercase() && character.is_lowercase() && self.committed_last_upper {
            // ...ABCWord: the pending C begins the next component; the prior
            // uppercase run is already committed in the preceding component.
            self.finish_component(emit)?;
            self.commit_char(previous);
        } else {
            self.commit_char(previous);
        }
        Ok(())
    }

    fn finish(&mut self, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        self.commit_pending();
        self.finish_component(emit)?;
        if self.saw_alphanumeric {
            let whole = std::mem::take(&mut self.whole).finish();
            self.emit_form(whole, emit)?;
        }
        Ok(())
    }

    fn commit_pending(&mut self) {
        if let Some(character) = self.pending.take() {
            self.commit_char(character);
        }
    }

    fn commit_char(&mut self, character: char) {
        self.component.push_char(character);
        self.component_has_alphanumeric = true;
        self.committed_last_upper = character.is_uppercase();
    }

    fn finish_component(&mut self, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        if self.component_has_alphanumeric {
            let component = std::mem::take(&mut self.component).finish();
            self.emit_form(component, emit)?;
        }
        self.component_has_alphanumeric = false;
        self.committed_last_upper = false;
        Ok(())
    }

    fn emit_form(&mut self, form: String, emit: &mut impl FnMut(String)) -> std::io::Result<()> {
        if self.dedup.insert(&form)? {
            emit(form);
        }
        Ok(())
    }
}

/// A normalized term that is kept inline while small and converted to a
/// streaming digest when it crosses [`MAX_TERM_BYTES`].
#[derive(Debug)]
enum FormAccumulator {
    Inline(Vec<u8>),
    Digest(Sha256),
}

impl Default for FormAccumulator {
    fn default() -> Self {
        Self::Inline(Vec::with_capacity(MAX_TERM_BYTES + 1))
    }
}

impl FormAccumulator {
    fn push_char(&mut self, character: char) {
        let mut lower = [0u8; 4];
        // Full Unicode case folding can expand a scalar (for example, ß to
        // `ss` or U+0130 to `i` plus a combining dot). Both document and
        // query paths use this iterator, so expansions remain symmetric while
        // the accumulator still retains at most MAX_TERM_BYTES of spelling.
        for lowered in character.case_fold() {
            let encoded = lowered.encode_utf8(&mut lower);
            self.push_bytes(encoded.as_bytes());
        }
    }

    fn push_bytes(&mut self, bytes: &[u8]) {
        match self {
            Self::Inline(existing)
                if existing.len().saturating_add(bytes.len()) <= MAX_TERM_BYTES =>
            {
                existing.extend_from_slice(bytes);
            }
            Self::Inline(existing) => {
                let mut digest = Sha256::new();
                digest.update(LONG_TERM_DOMAIN);
                digest.update(&*existing);
                digest.update(bytes);
                *self = Self::Digest(digest);
            }
            Self::Digest(digest) => digest.update(bytes),
        }
    }

    fn finish(self) -> String {
        match self {
            Self::Inline(bytes) => {
                String::from_utf8(bytes).expect("lowercase Unicode scalar values are valid UTF-8")
            }
            Self::Digest(digest) => digest_term(digest.finalize()),
        }
    }
}

fn digest_term(digest: impl AsRef<[u8]>) -> String {
    let bytes = digest.as_ref();
    let mut term = String::with_capacity(LONG_TERM_PREFIX.len() + bytes.len() * 2);
    term.push_str(LONG_TERM_PREFIX);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(term, "{byte:02x}");
    }
    term
}

/// Bounded de-duplication of equivalent forms in one lexical occurrence.
#[derive(Debug, Default)]
struct FormDeduper {
    forms: Vec<String>,
    spill: Option<DiskFormSet>,
}

impl FormDeduper {
    fn insert(&mut self, form: &str) -> std::io::Result<bool> {
        if let Some(spill) = self.spill.as_mut() {
            return spill.insert(form);
        }
        if self.forms.iter().any(|existing| existing == form) {
            return Ok(false);
        }
        if self.forms.len() >= MAX_DEDUP_FORMS {
            let mut spill = DiskFormSet::new(&self.forms)?;
            self.forms.clear();
            let inserted = spill.insert(form)?;
            self.spill = Some(spill);
            return Ok(inserted);
        }
        self.forms.push(form.to_owned());
        Ok(true)
    }
}

#[derive(Debug)]
struct DiskFormSet {
    path: PathBuf,
    connection: Option<Connection>,
}

impl DiskFormSet {
    fn new(forms: &[String]) -> std::io::Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let temp_dir = std::env::temp_dir();
        for _ in 0..32 {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = temp_dir.join(format!(
                "bm25-mcp-token-set-{}-{timestamp}-{id}.bin",
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
                    drop(file);
                    let result = (|| -> std::io::Result<Self> {
                        let connection = Connection::open(&path).map_err(io_error)?;
                        connection
                            .execute_batch(
                                "PRAGMA journal_mode=OFF;
                                 PRAGMA synchronous=OFF;
                                 PRAGMA temp_store=FILE;
                                 PRAGMA cache_size=-512;
                                 CREATE TABLE forms(form TEXT PRIMARY KEY) WITHOUT ROWID;",
                            )
                            .map_err(io_error)?;
                        let mut set = Self {
                            path: path.clone(),
                            connection: Some(connection),
                        };
                        let connection = set.connection.as_mut().expect("connection just set");
                        let transaction = connection.transaction().map_err(io_error)?;
                        for form in forms {
                            transaction
                                .execute(
                                    "INSERT OR IGNORE INTO forms(form) VALUES (?1)",
                                    params![form],
                                )
                                .map_err(io_error)?;
                        }
                        transaction.commit().map_err(io_error)?;
                        Ok(set)
                    })();
                    match result {
                        Ok(set) => return Ok(set),
                        Err(error) => {
                            // The pre-created path is private temporary state;
                            // clean it when SQLite setup fails so repeated
                            // pathological identifiers cannot leak files.
                            let _ = fs::remove_file(&path);
                            return Err(error);
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not create temporary token set",
        ))
    }

    fn insert(&mut self, form: &str) -> std::io::Result<bool> {
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| std::io::Error::other("token set connection is closed"))?;
        let changed = connection
            .execute(
                "INSERT OR IGNORE INTO forms(form) VALUES (?1)",
                params![form],
            )
            .map_err(io_error)?;
        Ok(changed != 0)
    }
}

impl Drop for DiskFormSet {
    fn drop(&mut self) {
        self.connection.take();
        let _ = fs::remove_file(&self.path);
    }
}

fn io_error(error: rusqlite::Error) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_case_camel_and_snake_identifiers() {
        assert_eq!(
            tokenize("parseHTTPResponse parse_http_response"),
            vec![
                "parsehttpresponse",
                "parse",
                "http",
                "response",
                "parse_http_response",
                "parse",
                "http",
                "response"
            ]
        );
    }

    #[test]
    fn preserves_repeated_occurrences_but_dedupes_forms() {
        assert_eq!(
            tokenize("foo foo_bar"),
            vec!["foo", "foo_bar", "foo", "bar"]
        );
    }

    #[test]
    fn boundary_spanning_occurrence_has_no_partial_terms() {
        let mut tokenizer = StreamingTokenizer::new();
        let mut terms = Vec::new();
        tokenizer.push_str("prefixlongident", &mut |term| terms.push(term));
        assert!(terms.is_empty());
        tokenizer.push_str("ifier", &mut |term| terms.push(term));
        tokenizer.finish(&mut |term| terms.push(term));
        assert_eq!(terms, vec!["prefixlongidentifier"]);
    }

    #[test]
    fn long_tokens_are_bounded_and_symmetric() {
        let long = "A".repeat(100_000);
        let first = tokenize(&long);
        let second = tokenize(&long.to_lowercase());
        assert_eq!(first, second);
        assert_eq!(first.len(), 1);
        assert!(first[0].len() < 100);
        assert!(first[0].starts_with(LONG_TERM_PREFIX));
    }

    #[test]
    fn acronym_boundaries_are_streaming() {
        assert_eq!(
            tokenize("parseHTTPResponse"),
            vec!["parsehttpresponse", "parse", "http", "response"]
        );
    }

    #[test]
    fn uses_full_unicode_case_folding_for_query_and_document_terms() {
        assert_eq!(tokenize("Straße"), vec!["strasse"]);
        assert_eq!(tokenize("STRASSE"), vec!["strasse"]);
    }

    #[test]
    fn pathological_component_occurrence_deduplicates_with_disk_spill() {
        let input = (0..5000)
            .map(|index| format!("piece{index}"))
            .collect::<Vec<_>>()
            .join("_");
        let terms = tokenize(&input);
        // The whole identifier plus one unique component for every piece.
        assert_eq!(terms.len(), 5001);
        let mut sorted = terms.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), terms.len());
    }

    #[test]
    fn surface_tokens_keep_exact_code_forms_and_lexical_parts() {
        let terms = surface_tokens("/src/HTTPServer.rs Namespace::HTTPServer foo-bar").unwrap();
        for expected in [
            "src",
            "httpserver",
            "http",
            "server",
            "rs",
            "namespace",
            "foo",
            "bar",
            "/src/httpserver.rs",
            "namespace::httpserver",
            "foo-bar",
        ] {
            assert!(
                terms.iter().any(|term| term == expected),
                "missing {expected:?}"
            );
        }
    }

    #[test]
    fn surface_tokens_bound_casefold_expansion() {
        let token = format!("{}ß", "a".repeat(MAX_TERM_BYTES));
        assert!(
            surface_tokens(&token)
                .unwrap()
                .iter()
                .all(|term| term.len() <= MAX_TERM_BYTES)
        );
    }

    #[test]
    fn compound_forms_survive_streaming_boundaries_and_sentence_delimiters() {
        let mut tokenizer = StreamingTokenizer::new();
        let mut terms = Vec::new();
        tokenizer.push_str("Namespace::HTTP", &mut |term| terms.push(term));
        tokenizer.push_str("Server.rs", &mut |term| terms.push(term));
        tokenizer.finish(&mut |term| terms.push(term));
        assert!(terms.iter().any(|term| term == "namespace::httpserver.rs"));
        assert!(terms.iter().any(|term| term == "namespace"));
        assert!(terms.iter().any(|term| term == "httpserver"));
        assert!(terms.iter().any(|term| term == "rs"));

        let sentence = tokenize("foo.bar, next");
        assert!(sentence.contains(&"foo.bar".to_owned()));
        assert!(!sentence.iter().any(|term| term.contains(',')));
        assert_eq!(
            tokenize("foo.")
                .iter()
                .filter(|term| *term == "foo")
                .count(),
            1
        );

        let qualified_sentence = tokenize("Foo::bar.");
        assert!(qualified_sentence.contains(&"foo::bar".to_owned()));
    }

    #[test]
    fn huge_compound_form_uses_digest_storage() {
        let input = format!("/{}", "A".repeat(100_000));
        let terms = tokenize(&input);
        assert!(terms.iter().any(|term| term.starts_with(LONG_TERM_PREFIX)));
        assert!(terms.iter().all(|term| term.len() < 100));
    }
}
