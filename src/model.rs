use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Source {
    pub key: String,
    pub collection: String,
    pub path: String,
    pub version: String,
    pub kind: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Chunk {
    #[serde(default)]
    pub field_kind: Option<String>,
    pub text: String,
    /// Precomputed terms for streaming ingestion. `None` asks the store to
    /// normalize `text`; `Some` avoids re-tokenizing a lexical occurrence
    /// that crossed a 16 KiB chunk boundary. This is an internal spool field
    /// and is omitted from wire responses when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<Vec<String>>,
    pub start_line: u64,
    pub end_line: u64,
    pub start_byte: u64,
    pub end_byte: u64,
    pub agent: Option<String>,
    pub session_id: Option<String>,
    pub event_id: Option<String>,
    pub timestamp: Option<String>,
    pub role: Option<String>,
    pub tool: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SearchFilter {
    pub collection: String,
    pub kind: String,
    pub path_glob: Option<String>,
    pub agent: Option<String>,
    pub session_id: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hit {
    #[serde(default = "one_copy")]
    pub copy_count: usize,
    pub match_id: String,
    pub verified_at: Option<String>,
    pub source: Source,
    pub chunk: Chunk,
    pub score: f32,
}

fn one_copy() -> usize {
    1
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScanReport {
    #[serde(skip)]
    pub coverage: crate::coverage::CoverageUpdate,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub diagnostics: std::collections::BTreeMap<String, u64>,
    pub sources: u64,
    pub chunks: u64,
    pub excluded_count: u64,
    pub error_count: u64,
    pub pending_count: u64,
    pub errors: Vec<String>,
}

impl ScanReport {
    pub(crate) fn record_source(
        &mut self,
        store: &crate::store::Store,
        key: String,
        version: Option<&str>,
        report: Self,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            report.sources == 0 || version.is_some(),
            "successful outcome requires a source version"
        );
        let publication = store.confirm_source_publication(&key, version)?;
        let (key, outcome) =
            publication.outcome(crate::coverage::SourceOutcome::from_report(&report));
        if let Some(publisher) = &self.coverage.publisher {
            publisher.source(&publication, Some(&outcome));
        }
        self.coverage.sources.insert(key, Some(outcome));
        self.sources += report.sources;
        self.chunks += report.chunks;
        self.excluded_count += report.excluded_count;
        self.error_count += report.error_count;
        self.pending_count += report.pending_count;
        for (category, count) in report.diagnostics {
            *self.diagnostics.entry(category).or_default() += count;
        }
        let remaining = 64usize.saturating_sub(self.errors.len());
        self.errors
            .extend(report.errors.into_iter().take(remaining));
        Ok(())
    }

    pub(crate) fn record_removal(
        &mut self,
        store: &crate::store::Store,
        key: String,
    ) -> anyhow::Result<()> {
        let publication = store.confirm_source_removal(&key)?;
        if let Some(publisher) = &self.coverage.publisher {
            publisher.source(&publication, None);
        }
        self.coverage.sources.insert(key, None);
        Ok(())
    }
}
